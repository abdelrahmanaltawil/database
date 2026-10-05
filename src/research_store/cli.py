from __future__ import annotations

import argparse
import json
import sys
import textwrap
import time
from datetime import date
from pathlib import Path

from research_store.access.api import connect, describe, load
from research_store.acquisition import geomet_climate_hourly as geomet_acquisition
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
from research_store.foundation.models import TemporalKind
from research_store.foundation.registry import DEFAULT_REGISTRY
from research_store.foundation.writer import StoreWriteLock
from research_store.ingestion import (
    fixed_width_daily,
    fixed_width_hourly,
    geomet_climate_hourly,
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
    "geomet_climate_hourly": geomet_climate_hourly.ingest,
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


def _producer_options(spec, args: argparse.Namespace) -> dict[str, object]:
    """Producer-specific ingest options, refused where they do not apply."""

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
    if spec.producer == "geomet_climate_hourly":
        extra["allow_selection_shrink"] = args.allow_selection_shrink
    elif args.allow_selection_shrink:
        raise ValueError(
            "--allow-selection-shrink applies only to replacement collections "
            "fetched from MSC GeoMet"
        )
    return extra


def _ingest(args: argparse.Namespace) -> int:
    paths = _paths(args, for_write=True)
    spec = DEFAULT_REGISTRY.get(args.dataset)
    ingester = INGESTERS[spec.producer]
    extra = _producer_options(spec, args)
    print(f"ingesting {args.source} ...", file=sys.stderr, flush=True)
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


def _ingest_directory(args: argparse.Namespace) -> int:
    paths = _paths(args, for_write=True)
    spec = DEFAULT_REGISTRY.get(args.dataset)
    ingester = INGESTERS[spec.producer]
    extra = _producer_options(spec, args)
    directory = args.directory.expanduser().resolve(strict=True)
    if not directory.is_dir():
        raise ValueError(f"Source is not a directory: {directory}")
    sources = sorted(path for path in directory.glob(args.pattern) if path.is_file())
    if not sources:
        raise FileNotFoundError(
            f"No files in {directory} match pattern {args.pattern!r}"
        )
    total = len(sources)
    for number, source in enumerate(sources, start=1):
        print(
            f"[{number}/{total}] ingesting {source.name} ...",
            file=sys.stderr,
            flush=True,
        )
        snapshot = ingester(
            args.dataset,
            source,
            registry=DEFAULT_REGISTRY,
            paths=paths,
            source_uri=None,
            publisher_vintage=args.publisher_vintage,
            fetched_at=None,
            **extra,
        )
        print(f"{source.name}\t{snapshot}", flush=True)
    return 0


def _fetch(args: argparse.Namespace) -> int:
    """Download a publisher selection into the store's download cache.

    This writes only below ``downloads/`` (atomically, one file at a time) and
    never touches the catalogue, so it takes no write lock. The manifest it
    prints is what `research-store ingest` then reads.
    """

    spec = DEFAULT_REGISTRY.get(args.dataset)
    # One acquisition module exists, so it is called directly; a second API
    # source would add its own branch here, as `_ingest` does for options.
    if spec.producer != "geomet_climate_hourly":
        raise ValueError(
            f"{args.dataset!r} is ingested from delivered files; `fetch` applies "
            "only to datasets acquired from the MSC GeoMet API"
        )
    if len(args.start) != len(args.end):
        raise ValueError("Give one --end for every --start")
    ranges = [
        (date.fromisoformat(start), date.fromisoformat(end))
        for start, end in zip(args.start, args.end, strict=True)
    ]
    downloads = args.downloads
    if downloads is None:
        downloads = _paths(args).downloads / args.dataset
    # Climate IDs and ranges are validated where the windows are built.
    result = geomet_acquisition.fetch_collection(
        args.dataset,
        climate_ids=args.station,
        ranges=ranges,
        registry=DEFAULT_REGISTRY,
        downloads=downloads,
        refresh=args.refresh,
        min_interval_seconds=args.delay_seconds,
    )
    print(result.manifest)
    print(json.dumps(result.summary(), indent=2))
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
    # A replacement collection's manifests are replayed oldest first; the
    # ingester must not measure an earlier, published selection against the
    # newer snapshot it precedes (a refused one is still refused).
    replay = {"replay": True} if spec.producer == "geomet_climate_hourly" else {}
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
                    **replay,
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


def _field(label: str, value: object, *, indent: str = "    ", width: int = 20) -> list[str]:
    """One label/value row, wrapping long prose under a hanging indent."""

    text = "-" if value in (None, "") else str(value)
    head = f"{indent}{label:<{width}}"
    return textwrap.wrap(
        text, width=96, initial_indent=head, subsequent_indent=" " * len(head)
    ) or [head.rstrip()]


def _format_spec(spec, *, verbose: bool = False) -> str:
    """Render one dataset declaration for a person reading a terminal."""

    out: list[str] = [spec.dataset_id]
    out += textwrap.wrap(spec.description, width=96,
                         initial_indent="  ", subsequent_indent="  ")
    out.append(
        f"  {spec.kind.value} | {spec.storage_model.value} storage | "
        f"{spec.temporal_kind.value} | producer {spec.producer} | {spec.readiness.value}"
    )
    for decision in spec.unresolved_decisions:
        out += textwrap.wrap(f"unresolved: {decision}", width=96,
                             initial_indent="  ", subsequent_indent="    ")

    if spec.variables:
        out += ["", "  variables"]
        name_w = max(len(item.name) for item in spec.variables)
        qty_w = max(len(item.quantity) for item in spec.variables)
        unit_w = max(len(item.unit or "-") for item in spec.variables)
        for variable in spec.variables:
            row = (
                f"    {variable.name:<{name_w}}  {variable.quantity:<{qty_w}}  "
                f"{(variable.unit or '-'):<{unit_w}}  {variable.dtype}"
            )
            if variable.quality_field:
                row += f"  quality={variable.quality_field}"
            if not variable.nullable:
                row += "  not-null"
            out.append(row)
            if variable.plausible_band is not None:
                out.append(
                    f"    {'':<{name_w}}  plausible: {variable.plausible_band.describe()}"
                )

    if spec.annotations:
        out += ["", "  annotations"]
        name_w = max(len(item.name) for item in spec.annotations)
        for annotation in spec.annotations:
            out += _field(
                f"{annotation.name:<{name_w}}  {annotation.dtype:<7}",
                annotation.meaning,
                width=name_w + 11,
            )

    out += ["", "  keys"]
    out += _field("entity", spec.entity_field)
    if spec.temporal_kind is not TemporalKind.REFERENCE:
        span = spec.time_start_field or "-"
        if spec.time_end_field:
            span += f" .. {spec.time_end_field}"
        out += _field("time", span)
    partitions = ", ".join(spec.partition_keys) or "-"
    if "entity_bucket" in spec.partition_keys:
        partitions += f"  (entity_buckets={spec.entity_buckets})"
    out += _field("partitions", partitions)
    out += _field("snapshots", spec.snapshot_mode)
    if spec.coordinate_convention:
        out += _field("coordinates", spec.coordinate_convention)

    if spec.temporal_kind is not TemporalKind.REFERENCE:
        out += ["", "  time"]
        out += _field("native frequency", spec.native_frequency)
        out += _field("source timezone", spec.source_timezone)
        out += _field("canonical timezone", spec.canonical_timezone)
        out += _field("semantics", spec.timestamp_semantics)

    if spec.sentinel_rules:
        out += ["", "  sentinels"]
        for rule in spec.sentinel_rules:
            target = "null" if rule.replacement is None else f"{rule.replacement:g}"
            window = ""
            if rule.start or rule.end:
                window = f"  [{rule.start or '...'}, {rule.end or '...'})"
            # A blank marker (an empty field) would otherwise print as nothing.
            marker = rule.marker if rule.marker.strip() else repr(rule.marker)
            scope = ""
            scoped = spec.ingest_options.get("sentinel_variables", {}).get(rule.marker)
            if scoped is not None and set(scoped) != set(spec.variable_names):
                scope = f"  (only {', '.join(scoped)})"
            out += _field(
                marker, f"{rule.meaning} -> {target}{window}{scope}", width=10
            )
            if verbose and rule.evidence:
                out += _field("", rule.evidence, indent="      ", width=8)

    if spec.documentation is not None:
        doc = spec.documentation
        out += ["", "  documentation"]
        out += _field("source format", doc.source_format)
        out += _field("quality control", doc.quality_control)
        if doc.quality_flags:
            out.append("    quality flags")
            code_w = max(len(code) for code in doc.quality_flags)
            for code, meaning in doc.quality_flags.items():
                out += _field(code, meaning, indent="      ", width=code_w + 2)
        sections = (
            ("limitations", doc.limitations),
            ("notes", doc.notes),
            ("references", doc.references),
        )
        if verbose:
            for label, items in sections:
                if items:
                    out.append(f"    {label}")
                    for item in items:
                        out += textwrap.wrap(item, width=96,
                                             initial_indent="      - ",
                                             subsequent_indent="        ")
        else:
            counts = ", ".join(f"{len(items)} {label}" for label, items in sections if items)
            if counts:
                out.append(f"    ({counts}; --verbose to print)")

    out += ["", f"  identity  {spec.identity_digest}"]
    return "\n".join(out)


def _describe(args: argparse.Namespace) -> int:
    spec = describe(args.dataset)
    if args.json:
        print(json.dumps(spec.serializable(), indent=2, default=str))
    else:
        print(_format_spec(spec, verbose=args.verbose))
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="research-store")
    root.add_argument("--store", type=Path, help=f"override {STORE_ENV}")
    subparsers = root.add_subparsers(dest="command", required=True)
    init = subparsers.add_parser("init", help="initialize directories and catalogue")
    init.set_defaults(handler=_init)
    datasets = subparsers.add_parser("datasets", help="list registry declarations")
    datasets.set_defaults(handler=_datasets)
    describe_parser = subparsers.add_parser(
        "describe", help="show one dataset's declared schema and semantics"
    )
    describe_parser.add_argument("dataset")
    describe_parser.add_argument(
        "--json", action="store_true", help="emit the declaration as JSON"
    )
    describe_parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="print references, limitations and notes in full",
    )
    describe_parser.set_defaults(handler=_describe)
    ingest = subparsers.add_parser("ingest", help="ingest one immutable source file")
    ingest.add_argument("dataset")
    ingest.add_argument("source", type=Path)
    ingest.add_argument("--source-uri")
    ingest.add_argument("--publisher-vintage")
    ingest.add_argument("--fetched-at")
    ingest.set_defaults(handler=_ingest)
    ingest_directory = subparsers.add_parser(
        "ingest-directory",
        help="ingest matching immutable source files in filename order",
    )
    ingest_directory.add_argument("dataset")
    ingest_directory.add_argument("directory", type=Path)
    ingest_directory.add_argument("--pattern", required=True)
    ingest_directory.add_argument("--publisher-vintage")
    ingest_directory.set_defaults(handler=_ingest_directory)
    for command in (ingest, ingest_directory):
        command.add_argument(
            "--station-metadata",
            type=Path,
            help="HYDAT SQLite station metadata used for optional station selection",
        )
        command.add_argument(
            "--max-drainage-area-km2",
            type=float,
            help="optional maximum gross drainage area for this ingestion run",
        )
        command.add_argument(
            "--allow-selection-shrink",
            action="store_true",
            help=(
                "publish a GeoMet replacement snapshot that covers fewer stations "
                "or hours than the published one"
            ),
        )
    fetch = subparsers.add_parser(
        "fetch",
        help="download a publisher API selection and write its manifest",
    )
    fetch.add_argument("dataset")
    fetch.add_argument(
        "--station",
        action="append",
        required=True,
        help="ECCC Climate ID; repeat for several stations",
    )
    fetch.add_argument(
        "--start",
        action="append",
        required=True,
        help="first local-standard-time day, YYYY-MM-DD (inclusive); repeatable",
    )
    fetch.add_argument(
        "--end",
        action="append",
        required=True,
        help="local-standard-time day after the last, YYYY-MM-DD (exclusive); "
        "pairs with the --start in the same position",
    )
    fetch.add_argument(
        "--refresh",
        action="store_true",
        help=(
            "re-request every response; without it only windows that could still "
            "gain hours, or are not cached, are requested"
        ),
    )
    fetch.add_argument(
        "--delay-seconds",
        type=float,
        default=geomet_acquisition.MIN_REQUEST_INTERVAL_SECONDS,
        help=(
            "pause between requests; at least "
            f"{geomet_acquisition.MIN_REQUEST_INTERVAL_SECONDS:g} (the default)"
        ),
    )
    fetch.add_argument(
        "--downloads",
        type=Path,
        help="download cache directory (default <store>/downloads/<dataset>)",
    )
    fetch.set_defaults(handler=_fetch)
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
