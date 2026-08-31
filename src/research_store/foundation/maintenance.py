"""Verification, garbage collection and migration for a physical store.

The catalogue records a SHA-256 for every archived source and every published
fragment. Recording evidence is not the same as checking it, so this module
exists to actually check it, to reclaim space that no snapshot references, and
to move an older store onto the current layout.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from research_store.foundation.catalog import SCHEMA_VERSION, Catalog
from research_store.foundation.hashing import sha256_file
from research_store.foundation.models import DatasetKind, Registry
from research_store.foundation.paths import StorePaths, looks_cloud_synced

LEGACY_FAILED_RUNS = "failed-runs"


def resolve_stored_path(paths: StorePaths, stored: str) -> Path | None:
    """Find the file a catalogue row names, even if the store has moved.

    Older catalogues recorded absolute paths, so a store that was moved,
    restored elsewhere, or mounted at a different point has rows pointing into
    a directory that no longer exists. The tail of such a path from `warehouse`
    or `raw` onward is still meaningful relative to wherever the store lives
    now, which is what makes those stores recoverable rather than lost.
    """

    candidate = paths.root / stored
    if candidate.exists():
        return candidate
    text = str(stored)
    for anchor in ("/warehouse/", "/raw/", "/staging/"):
        index = text.rfind(anchor)
        if index != -1:
            relocated = paths.root / text[index + 1 :]
            if relocated.exists():
                return relocated
    return None


@dataclass(frozen=True, slots=True)
class Finding:
    severity: str
    code: str
    detail: str

    def render(self) -> str:
        return f"{self.severity}: {self.code}: {self.detail}"


def _tier_of(registry: Registry, dataset_id: str) -> str:
    try:
        spec = registry.get(dataset_id)
    except KeyError:
        return "external"
    return "derived" if spec.kind is DatasetKind.DERIVED else "external"


def _partition_components(
    registry: Registry, dataset_id: str, partition: dict[str, Any]
) -> list[str]:
    try:
        keys: tuple[str, ...] = registry.get(dataset_id).partition_keys
    except KeyError:
        keys = tuple(sorted(partition))
    return [f"{key}={partition[key]}" for key in keys if key in partition]


def target_relative_path(
    registry: Registry,
    paths: StorePaths,
    dataset_id: str,
    partition: dict[str, Any],
    content_sha256: str,
) -> str:
    directory = paths.data_dir(_tier_of(registry, dataset_id), dataset_id).joinpath(
        *_partition_components(registry, dataset_id, partition)
    )
    return paths.relative(directory / f"part-{content_sha256[:24]}.parquet")


def _data_files(paths: StorePaths) -> list[Path]:
    found: list[Path] = []
    for tier in ("external", "derived"):
        root = paths.warehouse / tier
        if not root.is_dir():
            continue
        for dataset_dir in root.iterdir():
            data = dataset_dir / "data"
            if data.is_dir():
                found.extend(item for item in data.rglob("*.parquet") if item.is_file())
    return found


def _legacy_snapshot_dirs(paths: StorePaths) -> list[Path]:
    found: list[Path] = []
    for tier in ("external", "derived"):
        root = paths.warehouse / tier
        if not root.is_dir():
            continue
        for dataset_dir in root.iterdir():
            snapshots = dataset_dir / "snapshots"
            if snapshots.is_dir():
                found.extend(item for item in snapshots.iterdir() if item.is_dir())
    return found


def _directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


# ----------------------------------------------------------------------
# doctor
# ----------------------------------------------------------------------


def diagnose(
    paths: StorePaths,
    registry: Registry,
    *,
    verify_sample: int = 0,
    verify_all: bool = False,
) -> tuple[list[Finding], dict[str, Any]]:
    findings: list[Finding] = []
    facts: dict[str, Any] = {
        "resolved_root": str(paths.root),
        "cloud_synced_path": looks_cloud_synced(paths.root),
        "catalog_exists": paths.catalog.exists(),
        "registry_sha256": registry.digest,
    }
    if facts["cloud_synced_path"]:
        findings.append(
            Finding(
                "problem",
                "cloud_synced_store",
                "move the store out of a cloud-synced directory: a large mutable "
                "catalogue can be evicted or endlessly re-uploaded",
            )
        )
    if not paths.catalog.exists():
        findings.append(
            Finding("problem", "no_catalogue", "run research-store init")
        )
        return findings, facts

    catalog = Catalog(paths)
    facts["schema_version"] = catalog.get_meta("schema_version", "1")
    if facts["schema_version"] != str(SCHEMA_VERSION):
        findings.append(
            Finding(
                "problem",
                "schema_behind",
                f"catalogue schema is {facts['schema_version']}, expected "
                f"{SCHEMA_VERSION}; run research-store init",
            )
        )

    facts["provisional_datasets"] = [
        spec.dataset_id for spec in registry if spec.readiness.value == "provisional"
    ]
    overview = catalog.dataset_overview()
    facts["datasets"] = [
        {
            "dataset_id": row[0],
            "readiness": row[1],
            "committed_snapshots": row[2],
            "superseded_snapshots": row[3],
        }
        for row in overview
    ]
    for row in overview:
        if row[1] == "retired":
            findings.append(
                Finding(
                    "note",
                    "retired_dataset",
                    f"{row[0]} still holds {row[2]} committed snapshot(s) but is no "
                    f"longer declared in the registry",
                )
            )
        if row[1] == "ready" and row[2] == 0 and row[3] == 0:
            findings.append(
                Finding(
                    "note",
                    "declared_but_empty",
                    f"{row[0]} is declared ready but has never been ingested, so "
                    f"load() will raise LookupError",
                )
            )

    absolute = catalog.absolute_path_count()
    facts["absolute_fragment_paths"] = absolute
    if absolute:
        findings.append(
            Finding(
                "problem",
                "absolute_paths",
                f"{absolute:,} fragment rows store absolute paths, so this store "
                f"cannot be moved or restored elsewhere; run research-store migrate",
            )
        )

    legacy = _legacy_snapshot_dirs(paths)
    facts["legacy_snapshot_directories"] = len(legacy)
    if legacy:
        findings.append(
            Finding(
                "problem",
                "legacy_layout",
                f"{len(legacy)} snapshot directories remain from the pre-manifest "
                f"layout; they are not self-contained and must not be deleted by "
                f"hand. Run research-store migrate",
            )
        )

    inventory = catalog.fragment_inventory()
    facts["committed_fragments"] = len(inventory)
    located = {row[0]: resolve_stored_path(paths, row[0]) for row in inventory}
    missing = [row for row in inventory if located[row[0]] is None]
    facts["missing_fragments"] = len(missing)
    if missing:
        findings.append(
            Finding(
                "problem",
                "missing_fragments",
                f"{len(missing)} committed fragments are absent from disk, e.g. "
                f"{missing[0][0]}. Re-run the ingest for the affected sources to "
                f"rebuild them from archived bytes",
            )
        )

    checked = [row for row in inventory if located[row[0]] is not None]
    if verify_all:
        sample = checked
    elif verify_sample > 0:
        sample = random.sample(checked, min(verify_sample, len(checked)))
    else:
        sample = []
    corrupt: list[str] = []
    for relative, expected, *_ in sample:
        found = located[relative]
        if found is None or sha256_file(found) != expected:
            corrupt.append(relative)
    facts["checksums_verified"] = len(sample)
    facts["checksums_failed"] = len(corrupt)
    if corrupt:
        findings.append(
            Finding(
                "problem",
                "fragment_corrupt",
                f"{len(corrupt)} fragments no longer match their recorded "
                f"SHA-256, e.g. {corrupt[0]}",
            )
        )

    referenced = catalog.referenced_relative_paths()
    unreferenced = [
        item for item in _data_files(paths) if paths.relative(item) not in referenced
    ]
    facts["unreferenced_fragments"] = len(unreferenced)
    if unreferenced:
        findings.append(
            Finding(
                "note",
                "unreferenced_fragments",
                f"{len(unreferenced)} Parquet files under data/ are referenced by "
                f"no snapshot ({sum(item.stat().st_size for item in unreferenced) / 1e6:.0f} MB); "
                f"run research-store gc",
            )
        )

    staging_runs = paths.staging / "runs"
    staged_dirs = (
        [item for item in staging_runs.iterdir() if item.is_dir()]
        if staging_runs.is_dir()
        else []
    )
    live = {row[0] for row in catalog.runs_in_state("running")}
    abandoned = [item for item in staged_dirs if item.name not in live]
    facts["staging_runs"] = len(staged_dirs)
    facts["abandoned_staging_runs"] = len(abandoned)
    if abandoned:
        size = sum(_directory_size(item) for item in abandoned)
        findings.append(
            Finding(
                "note",
                "abandoned_staging",
                f"{len(abandoned)} staged run directories belong to no running run "
                f"({size / 1e6:.0f} MB); run research-store gc",
            )
        )

    running = catalog.runs_in_state("running")
    facts["running_runs"] = len(running)
    if running:
        findings.append(
            Finding(
                "note",
                "running_runs",
                f"{len(running)} runs are still marked running: either an ingest is "
                f"in progress, or a process died without recording failure "
                f"({', '.join(row[0] for row in running[:3])})",
            )
        )

    legacy_failed = paths.root / LEGACY_FAILED_RUNS
    if legacy_failed.is_dir():
        findings.append(
            Finding(
                "note",
                "unmanaged_directory",
                f"{legacy_failed} is not referenced by any code path; it was "
                f"created by hand. research-store gc will report it",
            )
        )

    return findings, facts


# ----------------------------------------------------------------------
# gc
# ----------------------------------------------------------------------


def collect_garbage(
    paths: StorePaths, registry: Registry, *, apply: bool = False
) -> dict[str, Any]:
    """Reclaim what no snapshot and no resumable run refers to.

    Superseded snapshots still reference their fragments, so a correction never
    silently destroys the record of what used to be published.
    """

    catalog = Catalog(paths)
    plan: dict[str, Any] = {"applied": apply, "staging": [], "fragments": [], "bytes": 0}

    live = {row[0] for row in catalog.runs_in_state("running")}
    staging_runs = paths.staging / "runs"
    if staging_runs.is_dir():
        for item in sorted(staging_runs.iterdir()):
            if not item.is_dir() or item.name in live:
                continue
            size = _directory_size(item)
            plan["staging"].append({"run_id": item.name, "bytes": size})
            plan["bytes"] += size
            if apply:
                shutil.rmtree(item, ignore_errors=True)
                catalog.forget_run(item.name)

    referenced = catalog.referenced_relative_paths()
    for item in sorted(_data_files(paths)):
        relative = paths.relative(item)
        if relative in referenced:
            continue
        size = item.stat().st_size
        plan["fragments"].append({"path": relative, "bytes": size})
        plan["bytes"] += size
        if apply:
            item.unlink()

    legacy_failed = paths.root / LEGACY_FAILED_RUNS
    if legacy_failed.is_dir():
        size = _directory_size(legacy_failed)
        plan["legacy_failed_runs"] = {"path": str(legacy_failed), "bytes": size}
        plan["bytes"] += size
        if apply:
            shutil.rmtree(legacy_failed, ignore_errors=True)

    if apply:
        _prune_empty_directories(paths)
    return plan


def _prune_empty_directories(paths: StorePaths) -> None:
    for tier in ("external", "derived"):
        root = paths.warehouse / tier
        if not root.is_dir():
            continue
        for directory in sorted(
            (item for item in root.rglob("*") if item.is_dir()),
            key=lambda item: len(item.parts),
            reverse=True,
        ):
            try:
                next(directory.iterdir())
            except StopIteration:
                directory.rmdir()
            except OSError:
                continue


# ----------------------------------------------------------------------
# migrate
# ----------------------------------------------------------------------


def _statistics_bounds(path: Path, columns: list[str]) -> tuple[Any, Any]:
    """Read time bounds from Parquet footer statistics without scanning rows."""

    import pyarrow.parquet as pq

    try:
        metadata = pq.ParquetFile(path).metadata
    except Exception:  # noqa: BLE001 - an unreadable footer is reported, not fatal
        return None, None
    names = [metadata.schema.column(i).name for i in range(metadata.num_columns)]
    indices = [names.index(name) for name in columns if name in names]
    if not indices:
        return None, None
    earliest: Any = None
    latest: Any = None
    for group in range(metadata.num_row_groups):
        row_group = metadata.row_group(group)
        for index in indices:
            statistics = row_group.column(index).statistics
            if statistics is None or not statistics.has_min_max:
                continue
            low, high = statistics.min, statistics.max
            if low is not None and (earliest is None or low < earliest):
                earliest = low
            if high is not None and (latest is None or high > latest):
                latest = high
    return _as_utc(earliest), _as_utc(latest)


def _as_utc(value: Any) -> Any:
    if value is None:
        return None
    if getattr(value, "tzinfo", None) is None:
        from datetime import UTC

        return value.replace(tzinfo=UTC)
    return value


def migrate_store(
    paths: StorePaths,
    registry: Registry,
    *,
    apply: bool = False,
    log: Any = None,
    budget_seconds: float | None = None,
    batch_size: int = 4000,
) -> dict[str, Any]:
    """Move an older store onto the current layout.

    Three things change together, because they are one idea: fragments belong
    to a dataset rather than to a snapshot, their catalogue paths are relative
    to the store root so the store can be moved, and their time bounds are
    recorded so a read can prune on them.

    The work is batched and each batch commits its own catalogue update, so a
    migration of a large store can be stopped and resumed without leaving the
    catalogue disagreeing with the disk. Pass `budget_seconds` to return early
    with `complete: False`; call again to carry on.
    """

    started = time.monotonic()

    def exhausted() -> bool:
        return budget_seconds is not None and time.monotonic() - started > budget_seconds

    def say(message: str) -> None:
        if log is not None:
            print(message, file=log, flush=True)

    catalog = Catalog(paths)
    report: dict[str, Any] = {
        "applied": apply,
        "complete": True,
        "relocated": 0,
        "already_placed": 0,
        "missing": [],
        "fragment_rows_repathed": 0,
        "chunk_rows_repathed": 0,
        "time_bounds_filled": 0,
    }

    # ---------------- fragments: relocate and re-path ----------------
    with catalog.open(read_only=True) as connection:
        fragments = connection.execute(
            "SELECT DISTINCT relative_path, content_sha256, dataset_id, partition_json "
            "FROM fragments"
        ).fetchall()

    outstanding: list[tuple[str, str]] = []
    for stored, checksum, dataset_id, partition_json in fragments:
        target_relative = target_relative_path(
            registry, paths, dataset_id, json.loads(partition_json), checksum
        )
        if stored == target_relative:
            report["already_placed"] += 1
        else:
            outstanding.append((stored, target_relative))
    say(
        f"physical fragments: {len(fragments):,}; already placed "
        f"{report['already_placed']:,}; to migrate {len(outstanding):,}"
    )

    for offset in range(0, len(outstanding), batch_size):
        if exhausted():
            report["complete"] = False
            say("time budget reached; re-run to continue")
            break
        batch = outstanding[offset : offset + batch_size]
        mapping: list[tuple[str, str]] = []
        for stored, target_relative in batch:
            target = paths.root / target_relative
            if target.is_file():
                mapping.append((stored, target_relative))
                continue
            current = resolve_stored_path(paths, stored)
            if current is None or not current.is_file():
                report["missing"].append(stored)
                continue
            if apply:
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(current, target)
                try:
                    target.chmod(0o444)
                except OSError:
                    pass
            mapping.append((stored, target_relative))
            report["relocated"] += 1
        if apply and mapping:
            with catalog.transaction() as connection:
                connection.execute(
                    "CREATE OR REPLACE TEMP TABLE path_map(old VARCHAR, new VARCHAR)"
                )
                connection.executemany(
                    "INSERT INTO path_map VALUES (?, ?)",
                    [list(pair) for pair in mapping],
                )
                connection.execute(
                    "UPDATE fragments SET relative_path = path_map.new "
                    "FROM path_map WHERE fragments.relative_path = path_map.old"
                )
                report["fragment_rows_repathed"] += len(mapping)
        say(
            f"  migrated {min(offset + batch_size, len(outstanding)):,}/"
            f"{len(outstanding):,} (relocated {report['relocated']:,})"
        )

    # ---------------- staged chunks point at the same places ----------------
    if report["complete"]:
        with catalog.open(read_only=True) as connection:
            chunks = connection.execute(
                "SELECT chunk.run_id, chunk.chunk_key, chunk.relative_path, "
                "       chunk.content_sha256, chunk.partition_json, run.dataset_id "
                "FROM ingestion_chunks AS chunk "
                "JOIN ingestion_runs AS run USING (run_id)"
            ).fetchall()
        chunk_updates = []
        for run_id, chunk_key, stored, checksum, partition_json, dataset_id in chunks:
            target_relative = target_relative_path(
                registry, paths, dataset_id, json.loads(partition_json), checksum
            )
            if stored != target_relative:
                chunk_updates.append((target_relative, run_id, chunk_key))
        if apply and chunk_updates:
            # Joined against a temp table rather than one statement per row:
            # tens of thousands of individual UPDATEs would each rescan.
            with catalog.transaction() as connection:
                connection.execute(
                    "CREATE OR REPLACE TEMP TABLE chunk_map"
                    "(new_path VARCHAR, run_id VARCHAR, chunk_key VARCHAR)"
                )
                connection.executemany(
                    "INSERT INTO chunk_map VALUES (?, ?, ?)",
                    [list(row) for row in chunk_updates],
                )
                connection.execute(
                    "UPDATE ingestion_chunks SET relative_path = chunk_map.new_path "
                    "FROM chunk_map "
                    "WHERE ingestion_chunks.run_id = chunk_map.run_id "
                    "  AND ingestion_chunks.chunk_key = chunk_map.chunk_key"
                )
        report["chunk_rows_repathed"] = len(chunk_updates)
        say(f"staged-chunk rows re-pathed: {len(chunk_updates):,}")

    # ---------------- time bounds from Parquet footers ----------------
    if report["complete"]:
        with catalog.open(read_only=True) as connection:
            pending = connection.execute(
                "SELECT DISTINCT relative_path, dataset_id FROM fragments "
                "WHERE min_time IS NULL OR max_time IS NULL"
            ).fetchall()
        say(f"fragments needing time bounds: {len(pending):,}")
        for offset in range(0, len(pending), batch_size):
            if exhausted():
                report["complete"] = False
                say("time budget reached during time-bound backfill; re-run to continue")
                break
            bounds: list[tuple[Any, Any, str]] = []
            for relative, dataset_id in pending[offset : offset + batch_size]:
                try:
                    spec = registry.get(dataset_id)
                except KeyError:
                    continue
                columns = [
                    name
                    for name in (spec.time_start_field, spec.time_end_field)
                    if name is not None
                ]
                if not columns:
                    continue
                path = resolve_stored_path(paths, relative)
                if path is None or not path.is_file():
                    continue
                earliest, latest = _statistics_bounds(path, columns)
                if earliest is not None or latest is not None:
                    bounds.append((earliest, latest, relative))
            if apply and bounds:
                with catalog.transaction() as connection:
                    connection.execute(
                        "CREATE OR REPLACE TEMP TABLE bound_map"
                        "(min_time TIMESTAMPTZ, max_time TIMESTAMPTZ, relative_path VARCHAR)"
                    )
                    connection.executemany(
                        "INSERT INTO bound_map VALUES (?, ?, ?)",
                        [list(row) for row in bounds],
                    )
                    connection.execute(
                        "UPDATE fragments SET min_time = bound_map.min_time, "
                        "max_time = bound_map.max_time FROM bound_map "
                        "WHERE fragments.relative_path = bound_map.relative_path"
                    )
                    connection.execute(
                        "UPDATE ingestion_chunks SET min_time = bound_map.min_time, "
                        "max_time = bound_map.max_time FROM bound_map "
                        "WHERE ingestion_chunks.relative_path = bound_map.relative_path"
                    )
            report["time_bounds_filled"] += len(bounds)
            say(
                f"  time bounds {min(offset + batch_size, len(pending)):,}/"
                f"{len(pending):,}"
            )

    if apply and report["complete"]:
        for directory in _legacy_snapshot_dirs(paths):
            shutil.rmtree(directory, ignore_errors=True)
        _prune_empty_directories(paths)
        say("removed emptied snapshot directories")
    return report
