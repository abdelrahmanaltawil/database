from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from research_store import load
from research_store.access.api import connect
from research_store.derived import materialize
from research_store.foundation.catalog import Catalog
from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.models import (
    DatasetKind,
    DatasetSpec,
    Registry,
    StorageModel,
    TemporalKind,
    VariableSpec,
)
from research_store.foundation.pipeline import ingest_file


def _wide_frame(rows: list[tuple[str, str, float, float]]) -> pd.DataFrame:
    frame = pd.DataFrame(
        rows, columns=["entity_id", "time_start", "power", "wind_speed"]
    )
    frame["entity_id"] = frame["entity_id"].astype("string")
    frame["time_start"] = pd.to_datetime(frame["time_start"], utc=True)
    frame["time_end"] = frame["time_start"] + pd.Timedelta(minutes=10)
    frame["power"] = frame["power"].astype("float64")
    frame["wind_speed"] = frame["wind_speed"].astype("float64")
    frame["power_quality"] = pd.Series(["A"] * len(frame), dtype="string")
    return frame[
        [
            "entity_id",
            "time_start",
            "time_end",
            "power",
            "power_quality",
            "wind_speed",
        ]
    ]


def test_ingest_load_sql_and_provenance(
    tmp_path: Path, store_paths, registry, wide_spec
) -> None:
    source = tmp_path / "sensor.csv"
    source.write_text("immutable source bytes\n")
    data = _wide_frame(
        [
            ("0100001", "2024-01-01T00:00:00Z", 10.0, 8.0),
            ("702S006", "2024-01-01T00:00:00Z", 20.0, 9.0),
            ("0100001", "2025-01-01T00:00:00Z", 30.0, 10.0),
        ]
    )

    def parser(path, spec, completed):
        assert path.read_text() == "immutable source bytes\n"
        yield from chunks_from_frame(
            data, spec, key_prefix="fixture", completed=completed
        )

    snapshot = ingest_file(
        dataset_id=wide_spec.dataset_id,
        source_path=source,
        parser=parser,
        ingester_version="test-1",
        registry=registry,
        paths=store_paths,
        publisher_vintage="2024-Q1",
    )

    frame = load(
        wide_spec.dataset_id,
        entity="0100001",
        variable=["power", "wind_speed"],
        start="2024",
        end="2025",
        snapshot=snapshot,
        store=store_paths.root,
        registry=registry,
    )
    assert frame["entity_id"].tolist() == ["0100001"]
    assert frame["power"].tolist() == [10.0]
    assert "value" not in frame.columns
    assert frame.attrs["units"] == {"power": "kW", "wind_speed": "m/s"}

    chosen, selected_paths = Catalog(store_paths).committed_fragments(
        wide_spec.dataset_id,
        snapshot,
        partition_filter={
            "year": {2024},
            "entity_bucket": {
                next(
                    chunk.partition["entity_bucket"]
                    for chunk in chunks_from_frame(
                        data.iloc[[0]], wide_spec, key_prefix="probe"
                    )
                )
            },
        },
    )
    assert chosen == snapshot
    assert selected_paths
    assert all("year=2024" in path for path in selected_paths)

    with connect(store=store_paths.root, registry=registry) as sql:
        assert sql.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"
        result = sql.execute(
            "SELECT entity_id, power FROM test_sensor WHERE entity_id = ?", ["0100001"]
        ).fetchall()
        source_count = sql.execute(
            "SELECT count(*) FROM catalog.main.source_files"
        ).fetchone()[0]
    assert result == [("0100001", 10.0), ("0100001", 30.0)]
    assert source_count == 1

    provenance = Catalog(store_paths).provenance(wide_spec.dataset_id, snapshot)
    assert provenance[0]["original_name"] == "sensor.csv"
    assert provenance[0]["publisher_vintage"] == "2024-Q1"


def test_long_storage_has_one_unit_safe_logical_api(
    tmp_path: Path, store_paths, registry, long_spec
) -> None:
    source = tmp_path / "long.txt"
    source.write_text("source")
    start = pd.Timestamp("2024-01-01", tz="UTC")
    data = pd.DataFrame(
        {
            "entity_id": pd.Series(["0100001", "0100001"], dtype="string"),
            "time_start": [start, start],
            "time_end": [start + pd.Timedelta(hours=1)] * 2,
            "variable": pd.Series(["rain", "temperature"], dtype="string"),
            "value": pd.Series([1.25, -2.0], dtype="float64"),
            "quality_flag": pd.Series(["A", "A"], dtype="string"),
        }
    )

    def parser(path, spec, completed):
        yield from chunks_from_frame(data, spec, key_prefix="long", completed=completed)

    ingest_file(
        dataset_id=long_spec.dataset_id,
        source_path=source,
        parser=parser,
        ingester_version="test-1",
        registry=registry,
        paths=store_paths,
    )
    frame = load(
        long_spec.dataset_id,
        entity="0100001",
        variable=["rain", "temperature"],
        store=store_paths.root,
        registry=registry,
    )
    assert list(frame[["rain", "temperature"]].iloc[0]) == [1.25, -2.0]
    assert "value" not in frame
    assert frame.attrs["units"] == {"rain": "mm", "temperature": "degC"}


def test_interrupted_ingest_is_invisible_and_resumes_without_rewriting(
    tmp_path: Path, store_paths, registry, wide_spec
) -> None:
    source = tmp_path / "restart.dat"
    source.write_text("restartable")
    data = _wide_frame(
        [
            ("0100001", "2023-01-01T00:00:00Z", 1.0, 2.0),
            ("0100001", "2024-01-01T00:00:00Z", 3.0, 4.0),
        ]
    )
    all_chunks = list(chunks_from_frame(data, wide_spec, key_prefix="restart"))
    assert len(all_chunks) == 2
    attempts = 0

    def parser(path, spec, completed):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            yield all_chunks[0]
            raise OSError("simulated interruption")
        assert all_chunks[0].chunk_key in completed
        yield all_chunks[1]

    with pytest.raises(OSError, match="simulated interruption"):
        ingest_file(
            dataset_id=wide_spec.dataset_id,
            source_path=source,
            parser=parser,
            ingester_version="restart-1",
            registry=registry,
            paths=store_paths,
        )
    with pytest.raises(LookupError):
        Catalog(store_paths).committed_fragments(wide_spec.dataset_id)

    snapshot = ingest_file(
        dataset_id=wide_spec.dataset_id,
        source_path=source,
        parser=parser,
        ingester_version="restart-1",
        registry=registry,
        paths=store_paths,
    )
    assert snapshot.startswith("snap_")
    frame = load(
        wide_spec.dataset_id,
        entity="0100001",
        variable="power",
        store=store_paths.root,
        registry=registry,
    )
    assert frame["power"].tolist() == [1.0, 3.0]

    def must_not_run(path, spec, completed):
        raise AssertionError("idempotent re-ingestion reparsed a committed source")
        yield

    same_snapshot = ingest_file(
        dataset_id=wide_spec.dataset_id,
        source_path=source,
        parser=must_not_run,
        ingester_version="restart-1",
        registry=registry,
        paths=store_paths,
    )
    assert same_snapshot == snapshot


def test_append_snapshots_are_cumulative_and_replace_snapshots_preserve_vintages(
    tmp_path: Path, store_paths, wide_spec
) -> None:
    append_spec = wide_spec
    replace_spec = replace(
        wide_spec, dataset_id="test_replace", snapshot_mode="replace"
    )
    registry = Registry([append_spec, replace_spec])

    def ingest_rows(spec, filename, rows, version):
        source = tmp_path / filename
        source.write_text(filename)
        frame = _wide_frame(rows)

        def parser(path, declared, completed):
            yield from chunks_from_frame(
                frame, declared, key_prefix=filename, completed=completed
            )

        return ingest_file(
            dataset_id=spec.dataset_id,
            source_path=source,
            parser=parser,
            ingester_version=version,
            registry=registry,
            paths=store_paths,
        )

    append_first = ingest_rows(
        append_spec, "annual-2023", [("A", "2023-01-01T00:00:00Z", 1.0, 2.0)], "v1"
    )
    append_second = ingest_rows(
        append_spec, "annual-2024", [("A", "2024-01-01T00:00:00Z", 3.0, 4.0)], "v1"
    )
    assert append_first != append_second
    current = load(
        append_spec.dataset_id,
        variable="power",
        store=store_paths.root,
        registry=registry,
    )
    assert current["power"].tolist() == [1.0, 3.0]

    replace_first = ingest_rows(
        replace_spec, "quarterly-old", [("A", "2024-01-01T00:00:00Z", 5.0, 6.0)], "v1"
    )
    replace_second = ingest_rows(
        replace_spec, "quarterly-new", [("A", "2024-01-01T00:00:00Z", 7.0, 8.0)], "v1"
    )
    latest = load(
        replace_spec.dataset_id,
        variable="power",
        store=store_paths.root,
        registry=registry,
    )
    old = load(
        replace_spec.dataset_id,
        variable="power",
        snapshot=replace_first,
        store=store_paths.root,
        registry=registry,
    )
    assert latest.attrs["snapshot_id"] == replace_second
    assert latest["power"].tolist() == [7.0]
    assert old["power"].tolist() == [5.0]


def test_derived_snapshot_has_transitive_source_provenance(
    tmp_path: Path, store_paths, wide_spec
) -> None:
    derived_spec = DatasetSpec(
        dataset_id="mean_power",
        description="Synthetic derived result",
        kind=DatasetKind.DERIVED,
        producer="test_mean",
        storage_model=StorageModel.WIDE,
        temporal_kind=TemporalKind.INTERVAL,
        native_frequency="1 day",
        variables=(VariableSpec("mean_power", "mean active power", "kW"),),
        snapshot_mode="replace",
        entity_buckets=8,
    )
    registry = Registry([wide_spec, derived_spec])
    source = tmp_path / "parent.csv"
    source.write_text("parent source")
    parent_frame = _wide_frame([("A", "2024-01-01T00:00:00Z", 2.0, 3.0)])

    def parent_parser(path, spec, completed):
        yield from chunks_from_frame(
            parent_frame, spec, key_prefix="parent", completed=completed
        )

    parent = ingest_file(
        dataset_id=wide_spec.dataset_id,
        source_path=source,
        parser=parent_parser,
        ingester_version="v1",
        registry=registry,
        paths=store_paths,
    )
    derived_frame = pd.DataFrame(
        {
            "entity_id": pd.Series(["A"], dtype="string"),
            "time_start": [pd.Timestamp("2024-01-01", tz="UTC")],
            "time_end": [pd.Timestamp("2024-01-02", tz="UTC")],
            "mean_power": pd.Series([2.0], dtype="float64"),
        }
    )

    def derived_chunks(completed):
        yield from chunks_from_frame(
            derived_frame, derived_spec, key_prefix="mean", completed=completed
        )

    derived = materialize(
        dataset_id=derived_spec.dataset_id,
        parent_snapshot_ids=[parent],
        query={"operation": "daily_mean", "variable": "power"},
        producer_version="test-1",
        chunks=derived_chunks,
        registry=registry,
        paths=store_paths,
    )
    result = load(
        derived_spec.dataset_id,
        variable="mean_power",
        snapshot=derived,
        store=store_paths.root,
        registry=registry,
    )
    assert result["mean_power"].tolist() == [2.0]
    provenance = Catalog(store_paths).provenance(derived_spec.dataset_id, derived)
    assert [record["original_name"] for record in provenance] == ["parent.csv"]
