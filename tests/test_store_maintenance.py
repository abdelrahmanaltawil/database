"""Tests for the properties an audit found missing: portability, recovery,
verification, correction, concurrency and unit sanity."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
from dataclasses import replace
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from research_store import load
from research_store.foundation.catalog import SCHEMA_VERSION, Catalog
from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.maintenance import (
    collect_garbage,
    diagnose,
    migrate_store,
)
from research_store.foundation.models import (
    DatasetKind,
    DatasetSpec,
    PlausibleBand,
    Registry,
    StorageModel,
    TemporalKind,
    VariableSpec,
)
from research_store.foundation.paths import StorePaths
from research_store.foundation.pipeline import ingest_file
from research_store.foundation.writer import StoreWriteLock


def _long_frame(rows: list[tuple[str, str, str, float]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=["entity_id", "time_start", "variable", "value"])
    frame["entity_id"] = frame["entity_id"].astype("string")
    frame["time_start"] = pd.to_datetime(frame["time_start"], utc=True)
    frame["time_end"] = frame["time_start"] + pd.Timedelta(hours=1)
    frame["variable"] = frame["variable"].astype("string")
    frame["value"] = frame["value"].astype("float64")
    return frame[["entity_id", "time_start", "time_end", "variable", "value"]]


def _ingest(spec, registry, paths, source: Path, frame, *, version="test-1") -> str:
    def parser(path, dataset_spec, completed):
        yield from chunks_from_frame(
            frame, dataset_spec, key_prefix=source.stem, completed=completed
        )

    return ingest_file(
        dataset_id=spec.dataset_id,
        source_path=source,
        parser=parser,
        ingester_version=version,
        registry=registry,
        paths=paths,
    )


@pytest.fixture
def seeded(tmp_path, store_paths, registry, long_spec):
    source = tmp_path / "observations.csv"
    source.write_text("source bytes\n")
    frame = _long_frame(
        [
            ("0100001", "2024-01-01T00:00:00Z", "rain", 1.0),
            ("0100001", "2024-01-01T01:00:00Z", "rain", 2.0),
            ("702S006", "2024-01-01T00:00:00Z", "temperature", -5.0),
        ]
    )
    snapshot = _ingest(long_spec, registry, store_paths, source, frame)
    return snapshot, source, frame


# ----------------------------------------------------------------------
# Portability
# ----------------------------------------------------------------------


def test_fragments_are_catalogued_relative_to_the_store_root(seeded, store_paths):
    catalog = Catalog(store_paths)
    assert catalog.absolute_path_count() == 0
    with catalog.open(read_only=True) as connection:
        stored = [
            row[0]
            for row in connection.execute("SELECT relative_path FROM fragments").fetchall()
        ]
    assert stored, "the fixture must publish fragments"
    assert all(not path.startswith("/") for path in stored)
    assert all(path.startswith("warehouse/external/test_long/data/") for path in stored)


def test_a_moved_store_still_reads(seeded, store_paths, tmp_path, long_spec):
    relocated = tmp_path / "relocated-store"
    shutil.move(str(store_paths.root), str(relocated))
    frame = load(long_spec.dataset_id, store=relocated, registry=_registry_of(long_spec))
    assert len(frame) == 3


def test_no_snapshot_directory_is_created(seeded, store_paths):
    for tier in ("external", "derived"):
        assert not list((store_paths.warehouse / tier).glob("*/snapshots"))


def _registry_of(*specs) -> Registry:
    return Registry(list(specs))


# ----------------------------------------------------------------------
# Recovery
# ----------------------------------------------------------------------


def test_missing_fragments_rebuild_into_the_same_snapshot(
    seeded, store_paths, registry, long_spec
):
    snapshot, source, frame = seeded
    catalog = Catalog(store_paths)
    published = catalog.snapshot_fragment_paths(snapshot)
    assert published
    for path in published:
        Path(path).unlink()

    rebuilt = _ingest(long_spec, registry, store_paths, source, frame)
    assert rebuilt == snapshot, "a restore must not mint a new snapshot identity"
    assert all(Path(path).is_file() for path in published)
    assert len(load(long_spec.dataset_id, store=store_paths.root, registry=registry)) == 3


def test_repeat_ingest_of_an_intact_store_is_a_no_op(seeded, store_paths, registry, long_spec):
    snapshot, source, frame = seeded
    again = _ingest(long_spec, registry, store_paths, source, frame)
    assert again == snapshot
    with Catalog(store_paths).open(read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM ingestion_runs").fetchone()[0] == 1


def test_declaring_another_dataset_does_not_re_key_finished_runs(
    seeded, store_paths, registry, long_spec, wide_spec
):
    snapshot, source, frame = seeded
    extra = DatasetSpec(
        dataset_id="unrelated_dataset",
        description="A dataset declared later, touching nothing that exists",
        kind=DatasetKind.EXTERNAL,
        producer="synthetic",
        storage_model=StorageModel.WIDE,
        temporal_kind=TemporalKind.INTERVAL,
        variables=(VariableSpec("value", "a quantity", "m"),),
    )
    widened = Registry([long_spec, wide_spec, extra])
    assert widened.digest != registry.digest
    again = _ingest(long_spec, widened, store_paths, source, frame)
    assert again == snapshot, "run identity must depend on its own dataset only"


# ----------------------------------------------------------------------
# Verification
# ----------------------------------------------------------------------


def test_doctor_is_clean_on_a_healthy_store(seeded, store_paths, registry):
    findings, facts = diagnose(store_paths, registry, verify_all=True)
    assert facts["checksums_verified"] > 0
    assert facts["checksums_failed"] == 0
    assert facts["absolute_fragment_paths"] == 0
    assert facts["missing_fragments"] == 0
    assert [item for item in findings if item.severity == "problem"] == []


def test_doctor_detects_a_tampered_fragment(seeded, store_paths, registry):
    catalog = Catalog(store_paths)
    target = Path(catalog.fragment_inventory()[0][0])
    absolute = store_paths.root / target
    absolute.chmod(0o644)
    absolute.write_bytes(absolute.read_bytes() + b"\0")
    findings, facts = diagnose(store_paths, registry, verify_all=True)
    assert facts["checksums_failed"] == 1
    assert any(item.code == "fragment_corrupt" for item in findings)


def test_doctor_detects_a_deleted_fragment(seeded, store_paths, registry):
    catalog = Catalog(store_paths)
    (store_paths.root / catalog.fragment_inventory()[0][0]).unlink()
    findings, facts = diagnose(store_paths, registry)
    assert facts["missing_fragments"] == 1
    assert any(item.code == "missing_fragments" for item in findings)


# ----------------------------------------------------------------------
# Correction and garbage collection
# ----------------------------------------------------------------------


def test_supersede_hides_data_without_deleting_the_record(
    seeded, store_paths, registry, long_spec
):
    snapshot, _, _ = seeded
    catalog = Catalog(store_paths)
    affected = catalog.supersede_dataset(long_spec.dataset_id, "scale factor corrected")
    assert affected == [snapshot]
    with pytest.raises(LookupError):
        load(long_spec.dataset_id, store=store_paths.root, registry=registry)
    with catalog.open(read_only=True) as connection:
        state, reason = connection.execute(
            "SELECT state, supersede_reason FROM snapshots WHERE snapshot_id = ?",
            [snapshot],
        ).fetchone()
    assert state == "superseded"
    assert reason == "scale factor corrected"
    # The fragments survive, so the record of what was published survives.
    assert all(Path(path).is_file() for path in catalog.snapshot_fragment_paths(snapshot))


def test_supersede_requires_a_reason(seeded, store_paths, long_spec):
    with pytest.raises(ValueError):
        Catalog(store_paths).supersede_dataset(long_spec.dataset_id, "   ")


def test_gc_keeps_superseded_fragments_and_reclaims_abandoned_staging(
    seeded, store_paths, registry, long_spec
):
    snapshot, _, _ = seeded
    catalog = Catalog(store_paths)
    catalog.supersede_dataset(long_spec.dataset_id, "corrected")
    abandoned = store_paths.staging / "runs" / "run_abandoned"
    abandoned.mkdir(parents=True)
    (abandoned / "part-x.parquet").write_bytes(b"debris")

    plan = collect_garbage(store_paths, registry, apply=True)
    assert [item["run_id"] for item in plan["staging"]] == ["run_abandoned"]
    assert not abandoned.exists()
    assert plan["fragments"] == [], "superseded snapshots still reference their data"
    assert all(Path(path).is_file() for path in catalog.snapshot_fragment_paths(snapshot))


def test_gc_reclaims_fragments_nothing_references(seeded, store_paths, registry):
    orphan = (
        store_paths.warehouse / "external" / "test_long" / "data" / "part-orphan.parquet"
    )
    orphan.write_bytes(b"unreferenced")
    plan = collect_garbage(store_paths, registry, apply=True)
    assert not orphan.exists()
    assert any(item["path"].endswith("part-orphan.parquet") for item in plan["fragments"])


# ----------------------------------------------------------------------
# Concurrency
# ----------------------------------------------------------------------


def test_a_second_process_cannot_write_to_the_store(store_paths):
    store_paths.create()
    lock = StoreWriteLock(store_paths.write_lock)
    lock.acquire()
    try:
        script = textwrap.dedent(
            f"""
            from pathlib import Path
            from research_store.foundation.paths import StorePaths
            from research_store.foundation.writer import StoreWriteLock
            paths = StorePaths(Path({str(store_paths.root)!r}))
            try:
                StoreWriteLock(paths.write_lock).acquire()
            except RuntimeError:
                raise SystemExit(3)
            raise SystemExit(0)
            """
        )
        root = Path(__file__).resolve().parents[1]
        environment = dict(os.environ)
        environment["PYTHONPATH"] = os.pathsep.join(
            [str(root / "src"), environment.get("PYTHONPATH", "")]
        ).rstrip(os.pathsep)
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            cwd=str(root),
            env=environment,
        )
        assert result.returncode == 3, result.stderr
    finally:
        lock.release()


def test_the_lock_is_reentrant_within_one_process(store_paths):
    store_paths.create()
    first = StoreWriteLock(store_paths.write_lock)
    second = StoreWriteLock(store_paths.write_lock)
    first.acquire()
    second.acquire()
    second.release()
    first.release()


# ----------------------------------------------------------------------
# Unit sanity and the read path
# ----------------------------------------------------------------------


def _banded_spec(minimum: float, maximum: float) -> DatasetSpec:
    return DatasetSpec(
        dataset_id="banded_dataset",
        description="A dataset that declares what its values can physically be",
        kind=DatasetKind.EXTERNAL,
        producer="synthetic",
        storage_model=StorageModel.LONG,
        temporal_kind=TemporalKind.INTERVAL,
        variables=(
            VariableSpec(
                "snow_depth",
                "snow depth",
                "cm",
                plausible_band=PlausibleBand(
                    quantile=0.5,
                    minimum=minimum,
                    maximum=maximum,
                    evidence="deepest snow ever measured is about 1146 cm",
                ),
            ),
        ),
        entity_buckets=4,
    )


def test_a_hundredfold_scale_error_is_refused_at_publication(tmp_path, store_paths):
    spec = _banded_spec(0.0, 300.0)
    registry = Registry([spec])
    source = tmp_path / "snow.csv"
    source.write_text("bytes\n")
    wrong = _long_frame(
        [
            ("0100001", f"2024-01-01T{hour:02d}:00:00Z", "snow_depth", 100.0 * hour)
            for hour in range(1, 24)
        ]
    )
    with pytest.raises(ValueError, match="plausibility band"):
        _ingest(spec, registry, store_paths, source, wrong)


def test_a_correctly_scaled_publication_passes_the_band(tmp_path, store_paths):
    spec = _banded_spec(0.0, 1200.0)
    registry = Registry([spec])
    source = tmp_path / "snow.csv"
    source.write_text("bytes\n")
    right = _long_frame(
        [
            ("0100001", f"2024-01-01T{hour:02d}:00:00Z", "snow_depth", float(hour))
            for hour in range(1, 24)
        ]
    )
    _ingest(spec, registry, store_paths, source, right)
    frame = load("banded_dataset", store=store_paths.root, registry=registry)
    assert frame["snow_depth"].max() == 23.0


def test_an_extreme_single_reading_does_not_fail_the_band(tmp_path, store_paths):
    spec = _banded_spec(0.0, 1200.0)
    registry = Registry([spec])
    source = tmp_path / "snow.csv"
    source.write_text("bytes\n")
    rows = [
        ("0100001", f"2024-01-01T{hour:02d}:00:00Z", "snow_depth", float(hour))
        for hour in range(1, 23)
    ]
    rows.append(("0100001", "2024-01-01T23:00:00Z", "snow_depth", 99999.0))
    _ingest(spec, registry, store_paths, source, _long_frame(rows))
    frame = load("banded_dataset", store=store_paths.root, registry=registry)
    assert frame["snow_depth"].max() == 99999.0, "outliers are preserved, not clipped"


def test_load_refuses_to_choose_between_duplicate_values(
    seeded, store_paths, registry, long_spec
):
    # Forge a fragment holding two values for one observation key. The writer
    # would refuse this, so the read path is what is under test.
    catalog = Catalog(store_paths)
    relative = catalog.fragment_inventory()[0][0]
    target = store_paths.root / relative
    target.chmod(0o644)
    with duckdb.connect() as connection:
        connection.execute(
            f"COPY (SELECT * FROM read_parquet('{target}') UNION ALL "
            f"SELECT * REPLACE (value + 1 AS value) FROM read_parquet('{target}')) "
            f"TO '{target}' (FORMAT PARQUET)"
        )
    with pytest.raises(duckdb.InvalidInputException, match="more than one value"):
        load(long_spec.dataset_id, store=store_paths.root, registry=registry)


def test_provenance_does_not_change_the_number_of_rows(
    seeded, store_paths, registry, long_spec
):
    plain = load(long_spec.dataset_id, store=store_paths.root, registry=registry)
    traced = load(
        long_spec.dataset_id,
        store=store_paths.root,
        registry=registry,
        include_provenance=True,
    )
    assert len(plain) == len(traced)
    assert traced["_source_id"].notna().all()


def test_load_refuses_a_read_wider_than_its_guard(seeded, store_paths, registry, long_spec):
    with pytest.raises(ValueError, match="row guard"):
        load(long_spec.dataset_id, store=store_paths.root, registry=registry, max_rows=1)
    sampled = load(
        long_spec.dataset_id, store=store_paths.root, registry=registry, limit=1
    )
    assert len(sampled) == 1


def test_fragment_time_bounds_are_recorded_and_prune_reads(
    seeded, store_paths, long_spec
):
    catalog = Catalog(store_paths)
    with catalog.open(read_only=True) as connection:
        unbounded = connection.execute(
            "SELECT count(*) FROM fragments WHERE min_time IS NULL OR max_time IS NULL"
        ).fetchone()[0]
    assert unbounded == 0, "time bounds must be captured when a chunk is written"

    _, everything = catalog.committed_fragments(long_spec.dataset_id)
    assert everything

    # A window after every observation prunes every fragment away.
    with pytest.raises(LookupError):
        catalog.committed_fragments(
            long_spec.dataset_id,
            start=pd.Timestamp("2030-01-01", tz="UTC").to_pydatetime(),
        )
    # A window covering them keeps them.
    _, kept = catalog.committed_fragments(
        long_spec.dataset_id,
        start=pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
        end=pd.Timestamp("2025-01-01", tz="UTC").to_pydatetime(),
    )
    assert sorted(kept) == sorted(everything)


def test_undeclared_datasets_are_dropped_from_the_catalogue(
    seeded, store_paths, long_spec, wide_spec
):
    catalog = Catalog(store_paths)
    with catalog.open() as connection:
        connection.execute(
            "INSERT INTO datasets (dataset_id, description, kind, storage_model, "
            "readiness, producer, registry_hash, spec_json) VALUES "
            "('ghost_dataset', 'left over', 'external', 'long', 'provisional', "
            "'synthetic', 'x', '{}')"
        )
    catalog.initialize(Registry([long_spec, wide_spec]))
    with catalog.open(read_only=True) as connection:
        remaining = {
            row[0]
            for row in connection.execute("SELECT dataset_id FROM datasets").fetchall()
        }
    assert "ghost_dataset" not in remaining


def test_a_dataset_with_data_is_retired_rather_than_forgotten(
    seeded, store_paths, wide_spec
):
    catalog = Catalog(store_paths)
    catalog.initialize(Registry([wide_spec]))
    with catalog.open(read_only=True) as connection:
        readiness = connection.execute(
            "SELECT readiness FROM datasets WHERE dataset_id = 'test_long'"
        ).fetchone()
    assert readiness == ("retired",)


# ----------------------------------------------------------------------
# Migration from the pre-manifest layout
# ----------------------------------------------------------------------


def _demigrate(store_paths: StorePaths) -> list[Path]:
    """Rebuild the old layout from a current store, to migrate it back.

    The old store kept fragments under `snapshots/<id>/` and catalogued them by
    absolute path with no time bounds. Reproducing that exactly is the only
    honest way to test the migration before running it on real data.
    """

    import hashlib

    catalog = Catalog(store_paths)
    legacy: list[Path] = []
    with catalog.open() as connection:
        rows = connection.execute(
            "SELECT fragment.relative_path, fragment.snapshot_id, fragment.dataset_id, "
            "       fragment.partition_json, chunk.chunk_key "
            "FROM fragments AS fragment "
            "LEFT JOIN ingestion_chunks AS chunk "
            "  ON chunk.relative_path = fragment.relative_path"
        ).fetchall()
        for relative, snapshot_id, dataset_id, partition_json, chunk_key in rows:
            partition = json.loads(partition_json)
            components = [f"{key}={partition[key]}" for key in sorted(partition)]
            digest = hashlib.sha256((chunk_key or relative).encode()).hexdigest()[:20]
            inner = Path(*components) / f"part-{digest}.parquet"
            destination = (
                store_paths.warehouse
                / "external"
                / dataset_id
                / "snapshots"
                / snapshot_id
                / inner
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            current = store_paths.root / relative
            if current.is_file():
                current.chmod(0o644)
                shutil.move(str(current), str(destination))
            legacy.append(destination)
            connection.execute(
                "UPDATE fragments SET relative_path = ?, min_time = NULL, "
                "max_time = NULL WHERE relative_path = ?",
                [str(destination), relative],
            )
            connection.execute(
                "UPDATE ingestion_chunks SET relative_path = ?, min_time = NULL, "
                "max_time = NULL WHERE relative_path = ?",
                [str(inner), relative],
            )
    return legacy


def test_migrate_moves_a_legacy_store_onto_the_current_layout(
    seeded, store_paths, registry, long_spec
):
    legacy = _demigrate(store_paths)
    assert legacy and all(path.is_file() for path in legacy)
    catalog = Catalog(store_paths)
    assert catalog.absolute_path_count() == len(legacy)

    findings, facts = diagnose(store_paths, registry)
    assert any(item.code == "absolute_paths" for item in findings)
    assert any(item.code == "legacy_layout" for item in findings)

    report = migrate_store(store_paths, registry, apply=True)
    assert report["relocated"] == len(set(str(path) for path in legacy))
    assert report["missing"] == []
    assert report["time_bounds_filled"] > 0

    assert catalog.absolute_path_count() == 0
    assert not list((store_paths.warehouse / "external").glob("*/snapshots"))
    with catalog.open(read_only=True) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM fragments WHERE min_time IS NULL"
            ).fetchone()[0]
            == 0
        )

    frame = load(long_spec.dataset_id, store=store_paths.root, registry=registry)
    assert len(frame) == 3
    findings, facts = diagnose(store_paths, registry, verify_all=True)
    assert facts["checksums_failed"] == 0
    assert [item for item in findings if item.severity == "problem"] == []


def test_migration_is_idempotent(seeded, store_paths, registry):
    _demigrate(store_paths)
    first = migrate_store(store_paths, registry, apply=True)
    second = migrate_store(store_paths, registry, apply=True)
    assert first["relocated"] > 0
    assert second["relocated"] == 0
    assert second["missing"] == []
    findings, _ = diagnose(store_paths, registry, verify_all=True)
    assert [item for item in findings if item.severity == "problem"] == []


def test_a_rebuild_after_migration_still_finds_its_fragments(
    seeded, store_paths, registry, long_spec
):
    snapshot, source, frame = seeded
    _demigrate(store_paths)
    migrate_store(store_paths, registry, apply=True)
    catalog = Catalog(store_paths)
    for path in catalog.snapshot_fragment_paths(snapshot):
        Path(path).unlink()
    rebuilt = _ingest(long_spec, registry, store_paths, source, frame)
    assert rebuilt == snapshot
    assert len(load(long_spec.dataset_id, store=store_paths.root, registry=registry)) == 3


def test_migration_recovers_a_store_that_was_moved(
    seeded, store_paths, registry, long_spec, tmp_path
):
    """The reason absolute paths are a bug: the store no longer resolves."""

    _demigrate(store_paths)
    relocated = tmp_path / "moved-store"
    shutil.move(str(store_paths.root), str(relocated))
    moved = StorePaths(relocated)

    findings, facts = diagnose(moved, registry)
    assert facts["absolute_fragment_paths"] > 0
    assert facts["missing_fragments"] == 0, (
        "the fragments are present, only their recorded paths are stale"
    )

    report = migrate_store(moved, registry, apply=True)
    assert report["missing"] == []
    assert Catalog(moved).absolute_path_count() == 0
    frame = load(long_spec.dataset_id, store=relocated, registry=registry)
    assert len(frame) == 3


LEGACY_RUNS_SQL = """
CREATE TABLE ingestion_runs (
    run_id VARCHAR PRIMARY KEY,
    dataset_id VARCHAR NOT NULL,
    source_id VARCHAR NOT NULL,
    ingester_version VARCHAR NOT NULL,
    registry_hash VARCHAR NOT NULL,
    snapshot_id VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    completed_at TIMESTAMPTZ,
    error VARCHAR,
    UNIQUE (dataset_id, source_id, ingester_version, registry_hash)
);
"""


def test_schema_migration_keeps_repeated_identities_apart(
    seeded, store_paths, registry, long_spec
):
    """A source can appear twice under one ingester version.

    The old key included the whole-registry hash, so an unrelated registry edit
    between a failed attempt and a later success produced two rows. Collapsing
    them onto one per-dataset identity must not violate the new key or drop
    history.
    """

    catalog = Catalog(store_paths)
    with catalog.open() as connection:
        connection.execute("DROP TABLE ingestion_runs")
        connection.execute(LEGACY_RUNS_SQL)
        for run_id, registry_hash, state in (
            ("run_failed", "hash_before_edit", "failed"),
            ("run_committed", "hash_after_edit", "committed"),
        ):
            connection.execute(
                "INSERT INTO ingestion_runs (run_id, dataset_id, source_id, "
                "ingester_version, registry_hash, snapshot_id, state) "
                "VALUES (?, ?, 'src_same', '4', ?, ?, ?)",
                [run_id, long_spec.dataset_id, registry_hash, f"snap_{run_id}", state],
            )
        connection.execute("DELETE FROM store_meta WHERE key = 'schema_version'")

    catalog.initialize(registry)

    with catalog.open(read_only=True) as connection:
        rows = dict(
            connection.execute(
                "SELECT run_id, dataset_digest FROM ingestion_runs"
            ).fetchall()
        )
    assert set(rows) == {"run_failed", "run_committed"}, "no history may be dropped"
    assert rows["run_committed"] == long_spec.identity_digest, (
        "the surviving publication claims the real identity"
    )
    assert rows["run_failed"].startswith("superseded-identity:")
    assert catalog.get_meta("schema_version") == str(SCHEMA_VERSION)


def test_an_interrupted_schema_migration_is_undone_and_retried(
    seeded, store_paths, registry
):
    catalog = Catalog(store_paths)
    with catalog.open() as connection:
        connection.execute("ALTER TABLE ingestion_runs RENAME TO ingestion_runs_old")
        connection.execute(LEGACY_RUNS_SQL)  # a partial, half-copied rebuild
        connection.execute("DELETE FROM store_meta WHERE key = 'schema_version'")

    catalog.initialize(registry)

    with catalog.open(read_only=True) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        surviving = connection.execute(
            "SELECT count(*) FROM ingestion_runs"
        ).fetchone()[0]
    assert "ingestion_runs_old" not in tables
    assert surviving == 1, "the original rows must come back, not the partial copy"


def test_archived_sources_prefers_a_publisher_name_over_a_digest(
    seeded, store_paths, long_spec
):
    """A restore must re-present bytes under the name they arrived with.

    An ingest pointed straight at the archived object records the content
    digest as the filename. That alias is real history and is kept, but it is
    not the name to replay.
    """

    catalog = Catalog(store_paths)
    rows = catalog.archived_sources(long_spec.dataset_id)
    assert len(rows) == 1
    assert rows[0][1] == "observations.csv"

    with catalog.open() as connection:
        source_id = connection.execute("SELECT source_id FROM source_files").fetchone()[0]
        connection.execute(
            "INSERT INTO source_aliases (alias_id, source_id, original_name, "
            "recorded_at) VALUES ('alias_digest', ?, ?, current_timestamp)",
            [source_id, "b" * 64],
        )

    rows = catalog.archived_sources(long_spec.dataset_id)
    assert len(rows) == 1, "one row per archived source, whatever its alias history"
    assert rows[0][1] == "observations.csv"


def test_a_band_can_ignore_zeros_for_a_zero_inflated_variable(tmp_path, store_paths):
    """Most precipitation slots record no rain.

    A quantile of the whole distribution is then either zero, which cannot
    discriminate any scale, or sits inside whatever junk the archive carries in
    its tail. The quantile of the values that recorded something is the
    distribution of actual measurements.
    """

    spec = DatasetSpec(
        dataset_id="zero_inflated",
        description="A mostly-zero variable with a contaminated upper tail",
        kind=DatasetKind.EXTERNAL,
        producer="synthetic",
        storage_model=StorageModel.LONG,
        temporal_kind=TemporalKind.INTERVAL,
        variables=(
            VariableSpec(
                "rain",
                "precipitation amount",
                "mm",
                plausible_band=PlausibleBand(
                    quantile=0.5,
                    minimum=0.02,
                    maximum=20.0,
                    ignore_zeros=True,
                    evidence="median non-zero hourly rainfall is under a millimetre",
                ),
            ),
        ),
        entity_buckets=4,
    )
    registry = Registry([spec])
    source = tmp_path / "rain.csv"
    source.write_text("bytes\n")

    def series(scale: float) -> list[tuple[str, str, str, float]]:
        rows = []
        for hour in range(24):
            for minute in (0, 30):
                index = hour * 2 + minute // 30
                # 90% zeros, a little real rain, and a junk tail like the archive's
                if index < 42:
                    value = 0.0
                elif index < 46:
                    value = 0.8 * scale
                else:
                    value = 900.0
                rows.append(
                    (
                        "0100001",
                        f"2024-01-01T{hour:02d}:{minute:02d}:00Z",
                        "rain",
                        value,
                    )
                )
        return rows

    def frame(rows):
        import pandas as pd

        built = pd.DataFrame(rows, columns=["entity_id", "time_start", "variable", "value"])
        built["entity_id"] = built["entity_id"].astype("string")
        built["time_start"] = pd.to_datetime(built["time_start"], utc=True)
        built["time_end"] = built["time_start"] + pd.Timedelta(minutes=30)
        built["variable"] = built["variable"].astype("string")
        built["value"] = built["value"].astype("float64")
        return built[["entity_id", "time_start", "time_end", "variable", "value"]]

    # Correct scale passes even though the tail is absurd and most slots are zero.
    _ingest(spec, registry, store_paths, source, frame(series(1.0)))
    published = load("zero_inflated", store=store_paths.root, registry=registry)
    assert published["rain"].max() == 900.0, "the junk tail is preserved, not clipped"

    # A hundredfold error moves the median of what was measured, and is caught.
    other = tmp_path / "rain2.csv"
    other.write_text("other bytes\n")
    with pytest.raises(ValueError, match="plausibility band"):
        _ingest(spec, registry, store_paths, other, frame(series(100.0)))


def test_an_append_only_rescans_partitions_it_could_collide_with(
    seeded, store_paths, registry, long_spec, tmp_path
):
    """Duplicate checking must not grow with the size of the archive.

    An observation key determines its partition, so an append can only collide
    inside a partition it writes to. Checking it against every published
    fragment would make each annual file cost a scan of the whole dataset.
    """

    published = [
        path
        for path, _ in Catalog(store_paths).latest_committed_fragments(
            long_spec.dataset_id
        )[1]
    ]
    assert published

    from research_store.foundation.writer import StoreWriter

    prior = Catalog(store_paths).latest_committed_fragments(long_spec.dataset_id)[1]

    # An append landing in a year nothing has been published for rescans nothing.
    untouched = StoreWriter._prior_fragments_that_could_collide(
        prior, [{"year": 2099, "entity_bucket": 0}], long_spec
    )
    assert untouched == []

    # An append landing in a published partition rescans exactly that partition.
    existing = prior[0][1]
    overlapping = StoreWriter._prior_fragments_that_could_collide(
        prior, [existing], long_spec
    )
    assert len(overlapping) == 1
    assert len(overlapping) < len(prior) or len(prior) == 1

    # And the duplicate it exists to catch is still caught.
    source = tmp_path / "again.csv"
    source.write_text("different bytes, same observations\n")
    duplicate = _long_frame(
        [("0100001", "2024-01-01T00:00:00Z", "rain", 1.0)]
    )
    with pytest.raises(ValueError, match="Duplicate observation keys"):
        _ingest(long_spec, registry, store_paths, source, duplicate)
