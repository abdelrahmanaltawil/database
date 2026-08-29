from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from research_store.access.api import connect, load
from research_store.foundation.catalog import Catalog
from research_store.foundation.partitioning import entity_bucket
from research_store.foundation.paths import (
    STORE_ENV,
    looks_cloud_synced,
    resolve_store_paths,
)
from research_store.foundation.registry import DEFAULT_REGISTRY
from research_store.ingestion import (
    fixed_width_hourly,
    hydrometric_sqlite,
    inventory_csv,
    reanalysis_netcdf,
    scada_wide,
)

INGESTERS = {
    "fixed_width_hourly": fixed_width_hourly.ingest,
    "hydrometric_sqlite": hydrometric_sqlite.ingest,
    "inventory_csv": inventory_csv.ingest,
    "reanalysis_netcdf": reanalysis_netcdf.ingest,
    "scada_wide": scada_wide.ingest,
}


def _paths(args: argparse.Namespace, *, for_write: bool = False):
    return resolve_store_paths(args.store, for_write=for_write)


def _init(args: argparse.Namespace) -> int:
    paths = _paths(args, for_write=True)
    Catalog(paths).initialize(DEFAULT_REGISTRY)
    print(f"Initialized store at {paths.root}")
    return 0


def _datasets(args: argparse.Namespace) -> int:
    for spec in DEFAULT_REGISTRY:
        print(
            f"{spec.dataset_id}\t{spec.readiness.value}\t{spec.storage_model.value}\t"
            f"{spec.producer}"
        )
        for decision in spec.unresolved_decisions:
            print(f"  unresolved: {decision}")
    return 0


def _ingest(args: argparse.Namespace) -> int:
    paths = _paths(args, for_write=True)
    spec = DEFAULT_REGISTRY.get(args.dataset)
    ingester = INGESTERS[spec.producer]
    snapshot = ingester(
        args.dataset,
        args.source,
        registry=DEFAULT_REGISTRY,
        paths=paths,
        source_uri=args.source_uri,
        publisher_vintage=args.publisher_vintage,
        fetched_at=args.fetched_at,
    )
    print(snapshot)
    return 0


def _provenance(args: argparse.Namespace) -> int:
    records = Catalog(_paths(args)).provenance(args.dataset, args.snapshot)
    print(json.dumps(records, indent=2, default=str))
    return 0


def _sql(args: argparse.Namespace) -> int:
    connection = connect(store=args.store)
    try:
        if args.query:
            print(connection.execute(args.query).fetchdf().to_string(index=False))
        else:
            print("SQL query is required in non-interactive mode", file=sys.stderr)
            return 2
    finally:
        connection.close()
    return 0


def _doctor(args: argparse.Namespace) -> int:
    paths = _paths(args)
    problems: list[str] = []
    print(f"resolved_root: {paths.root}")
    print(f"environment_variable: {STORE_ENV}")
    print(f"cloud_synced_path: {looks_cloud_synced(paths.root)}")
    print(f"catalog_exists: {paths.catalog.exists()}")
    print(f"registry_sha256: {DEFAULT_REGISTRY.digest}")
    provisional = [
        spec.dataset_id
        for spec in DEFAULT_REGISTRY
        if spec.readiness.value == "provisional"
    ]
    print(f"provisional_datasets: {', '.join(provisional) if provisional else 'none'}")
    if looks_cloud_synced(paths.root):
        problems.append("move the store out of a cloud-synced directory")
    if not paths.catalog.exists():
        problems.append("run research-store init")
    if problems:
        for problem in problems:
            print(f"problem: {problem}")
        return 1
    return 0


def _benchmark(args: argparse.Namespace) -> int:
    paths = _paths(args)
    spec = DEFAULT_REGISTRY.get(args.dataset)
    spec.require_ready()
    partition_filter = {
        "year": {args.year},
        "entity_bucket": {entity_bucket(args.entity, spec.entity_buckets)},
    }
    snapshot, fragments = Catalog(paths).committed_fragments(
        args.dataset, args.snapshot, partition_filter=partition_filter
    )
    candidate_bytes = sum(Path(path).stat().st_size for path in fragments)
    started = time.perf_counter()
    frame = load(
        args.dataset,
        entity=args.entity,
        variable=args.variable,
        start=str(args.year),
        end=str(args.year + 1),
        snapshot=snapshot,
        store=paths.root,
    )
    elapsed = time.perf_counter() - started
    print(
        json.dumps(
            {
                "dataset": args.dataset,
                "snapshot": snapshot,
                "entity": args.entity,
                "year": args.year,
                "rows": len(frame),
                "candidate_fragments": len(fragments),
                "candidate_compressed_bytes": candidate_bytes,
                "elapsed_seconds": elapsed,
            },
            indent=2,
        )
    )
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="research-store")
    root.add_argument("--store", type=Path, help=f"override {STORE_ENV}")
    subparsers = root.add_subparsers(dest="command", required=True)
    init = subparsers.add_parser("init", help="initialize directories and catalogue")
    init.set_defaults(handler=_init)
    datasets = subparsers.add_parser("datasets", help="list registry declarations")
    datasets.set_defaults(handler=_datasets)
    ingest = subparsers.add_parser("ingest", help="ingest one immutable source file")
    ingest.add_argument("dataset")
    ingest.add_argument("source", type=Path)
    ingest.add_argument("--source-uri")
    ingest.add_argument("--publisher-vintage")
    ingest.add_argument("--fetched-at")
    ingest.set_defaults(handler=_ingest)
    provenance = subparsers.add_parser(
        "provenance", help="resolve sources for a snapshot"
    )
    provenance.add_argument("dataset")
    provenance.add_argument("--snapshot")
    provenance.set_defaults(handler=_provenance)
    sql = subparsers.add_parser("sql", help="run read-only DuckDB SQL")
    sql.add_argument("query", nargs="?")
    sql.set_defaults(handler=_sql)
    doctor = subparsers.add_parser(
        "doctor", help="check configuration and store health"
    )
    doctor.set_defaults(handler=_doctor)
    benchmark = subparsers.add_parser(
        "benchmark", help="measure one-entity/year read cost on real data"
    )
    benchmark.add_argument("dataset")
    benchmark.add_argument("--entity", required=True)
    benchmark.add_argument("--year", required=True, type=int)
    benchmark.add_argument("--variable", required=True)
    benchmark.add_argument("--snapshot")
    benchmark.set_defaults(handler=_benchmark)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (
        KeyError,
        ValueError,
        RuntimeError,
        FileNotFoundError,
        LookupError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
