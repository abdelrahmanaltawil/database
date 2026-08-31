from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from research_store.access.api import connect, load
from research_store.foundation.catalog import Catalog
from research_store.foundation.maintenance import (
    collect_garbage,
    diagnose,
    migrate_store,
)
from research_store.foundation.partitioning import entity_bucket
from research_store.foundation.paths import (
    STORE_ENV,
    looks_cloud_synced,
    resolve_store_paths,
)
from research_store.foundation.registry import DEFAULT_REGISTRY
from research_store.foundation.writer import StoreWriteLock
from research_store.ingestion import (
    fixed_width_daily,
    fixed_width_hourly,
    hydrometric_sqlite,
    inventory_csv,
    inventory_sqlite,
    reanalysis_netcdf,
    scada_wide,
    unit_value_corrected,
)

INGESTERS = {
    "fixed_width_daily": fixed_width_daily.ingest,
    "fixed_width_hourly": fixed_width_hourly.ingest,
    "hydrometric_sqlite": hydrometric_sqlite.ingest,
    "inventory_csv": inventory_csv.ingest,
    "inventory_sqlite": inventory_sqlite.ingest,
    "reanalysis_netcdf": reanalysis_netcdf.ingest,
    "scada_wide": scada_wide.ingest,
    "unit_value_corrected": unit_value_corrected.ingest,
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
    extra: dict[str, object] = {}
    if spec.producer == "unit_value_corrected":
        extra.update(
            station_metadata=args.station_metadata,
            max_drainage_area_km2=args.max_drainage_area_km2,
        )
    elif args.station_metadata is not None or args.max_drainage_area_km2 is not None:
        raise ValueError(
            "--station-metadata and --max-drainage-area-km2 apply only to "
            "corrected unit-value ingestion"
        )
    snapshot = ingester(
        args.dataset,
        args.source,
        registry=DEFAULT_REGISTRY,
        paths=paths,
        source_uri=args.source_uri,
        publisher_vintage=args.publisher_vintage,
        fetched_at=args.fetched_at,
        **extra,
    )
    print(snapshot)
    return 0


def _reingest(args: argparse.Namespace) -> int:
    """Re-present every archived source of a dataset to the ingester.

    Used after a correction, and to rebuild a warehouse from a restored backup.
    The archived object is hard-linked under its original publisher filename so
    the ingest records the same alias it did the first time, rather than a
    content digest.
    """

    import os
    import shutil
    import tempfile

    paths = _paths(args, for_write=not args.dry_run)
    sources = Catalog(paths).archived_sources(args.dataset)
    if not sources:
        print(f"No archived sources are recorded for {args.dataset!r}", file=sys.stderr)
        return 1
    print(f"{len(sources)} archived source(s) for {args.dataset}")
    if args.dry_run:
        for raw_path, original_name, vintage, *_ in sources:
            print(f"  {original_name}\t{vintage or ''}")
        print("note: nothing was ingested; re-run without --dry-run")
        return 0

    spec = DEFAULT_REGISTRY.get(args.dataset)
    ingester = INGESTERS[spec.producer]
    failures = 0
    for index, (raw_path, original_name, vintage, uri, fetched, _version) in enumerate(
        sources, 1
    ):
        archived = Path(raw_path)
        if not archived.is_file():
            print(f"[{index}/{len(sources)}] {original_name}: archived bytes missing")
            failures += 1
            continue
        with tempfile.TemporaryDirectory() as directory:
            presented = Path(directory) / (original_name or archived.name)
            try:
                os.link(archived, presented)
            except OSError:
                shutil.copy2(archived, presented)
            try:
                snapshot = ingester(
                    args.dataset,
                    presented,
                    registry=DEFAULT_REGISTRY,
                    paths=paths,
                    source_uri=uri,
                    publisher_vintage=vintage,
                    fetched_at=fetched,
                )
            except Exception as error:  # noqa: BLE001 - report and continue
                failures += 1
                print(f"[{index}/{len(sources)}] {original_name}: FAILED {error}")
                if args.stop_on_error:
                    return 1
                continue
        print(f"[{index}/{len(sources)}] {original_name}: {snapshot}")
    if failures:
        print(f"{failures} source(s) failed; re-run to resume", file=sys.stderr)
    return 1 if failures else 0


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
    findings, facts = diagnose(
        paths,
        DEFAULT_REGISTRY,
        verify_sample=args.verify_sample,
        verify_all=args.verify_all,
    )
    facts["environment_variable"] = STORE_ENV
    print(json.dumps(facts, indent=2, default=str))
    for finding in findings:
        print(finding.render())
    if not findings:
        print("note: no problems found")
    return 1 if any(item.severity == "problem" for item in findings) else 0


def _gc(args: argparse.Namespace) -> int:
    paths = _paths(args, for_write=args.apply)
    lock = StoreWriteLock(paths.write_lock) if args.apply else None
    if lock is not None:
        lock.acquire()
    try:
        plan = collect_garbage(paths, DEFAULT_REGISTRY, apply=args.apply)
    finally:
        if lock is not None:
            lock.release()
    print(
        json.dumps(
            {
                "applied": plan["applied"],
                "reclaimable_bytes": plan["bytes"],
                "staging_runs": len(plan["staging"]),
                "unreferenced_fragments": len(plan["fragments"]),
                "legacy_failed_runs": plan.get("legacy_failed_runs"),
            },
            indent=2,
            default=str,
        )
    )
    if not args.apply:
        print("note: nothing was deleted; re-run with --apply")
    return 0


def _migrate(args: argparse.Namespace) -> int:
    paths = _paths(args, for_write=True)
    lock = StoreWriteLock(paths.write_lock)
    lock.acquire()
    try:
        Catalog(paths).initialize(DEFAULT_REGISTRY)
        report = migrate_store(
            paths,
            DEFAULT_REGISTRY,
            apply=args.apply,
            log=sys.stdout,
            budget_seconds=args.budget_seconds,
        )
    finally:
        lock.release()
    print(json.dumps(report, indent=2, default=str))
    if not args.apply:
        print("note: nothing was changed; re-run with --apply")
    return 0


def _supersede(args: argparse.Namespace) -> int:
    paths = _paths(args, for_write=True)
    lock = StoreWriteLock(paths.write_lock)
    lock.acquire()
    try:
        catalog = Catalog(paths)
        catalog.initialize(DEFAULT_REGISTRY)
        affected = catalog.supersede_dataset(args.dataset, args.reason)
    finally:
        lock.release()
    print(
        json.dumps(
            {
                "dataset": args.dataset,
                "superseded_snapshots": affected,
                "reason": args.reason,
            },
            indent=2,
        )
    )
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
    ingest.add_argument(
        "--station-metadata",
        type=Path,
        help="HYDAT SQLite station metadata used for optional station selection",
    )
    ingest.add_argument(
        "--max-drainage-area-km2",
        type=float,
        help="optional maximum gross drainage area for this ingestion run",
    )
    ingest.set_defaults(handler=_ingest)
    reingest = subparsers.add_parser(
        "reingest",
        help="re-present every archived source of a dataset to its ingester",
    )
    reingest.add_argument("dataset")
    reingest.add_argument("--dry-run", action="store_true", help="list, do not ingest")
    reingest.add_argument(
        "--stop-on-error",
        action="store_true",
        help="stop at the first failure instead of continuing",
    )
    reingest.set_defaults(handler=_reingest)
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
        "doctor", help="verify catalogue, fragments and checksums"
    )
    doctor.add_argument(
        "--verify-sample",
        type=int,
        default=0,
        help="re-hash this many committed fragments against their recorded SHA-256",
    )
    doctor.add_argument(
        "--verify-all",
        action="store_true",
        help="re-hash every committed fragment (slow)",
    )
    doctor.set_defaults(handler=_doctor)
    gc_parser = subparsers.add_parser(
        "gc", help="reclaim staging and fragments no snapshot references"
    )
    gc_parser.add_argument("--apply", action="store_true", help="actually delete")
    gc_parser.set_defaults(handler=_gc)
    migrate = subparsers.add_parser(
        "migrate", help="move an older store onto the current layout"
    )
    migrate.add_argument("--apply", action="store_true", help="actually migrate")
    migrate.add_argument(
        "--budget-seconds",
        type=float,
        default=None,
        help="stop cleanly after this long and report complete=false; re-run to resume",
    )
    migrate.set_defaults(handler=_migrate)
    supersede = subparsers.add_parser(
        "supersede",
        help="retire every committed snapshot of a dataset after a correction",
    )
    supersede.add_argument("dataset")
    supersede.add_argument(
        "--reason", required=True, help="why the published values are being withdrawn"
    )
    supersede.set_defaults(handler=_supersede)
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
