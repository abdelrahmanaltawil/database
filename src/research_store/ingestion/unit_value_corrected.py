from __future__ import annotations

import csv
import hashlib
import json
import lzma
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable

import pandas as pd

from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.models import DatasetSpec, Registry, StorageModel, TemporalKind
from research_store.foundation.paths import StorePaths
from research_store.foundation.pipeline import ParsedChunk
from research_store.foundation.writer import SourceAsset, StoreWriter

VERSION = "2"
FILENAME = re.compile(
    r"^Discharge\.Working@(?P<station>[0-9A-Z]+)\."
    r"(?P<start>[0-9]{8})_corrected\.csv\.xz$"
)
EXPECTED_COLUMNS = (
    "ISO 8601 UTC",
    "Timestamp",
    "Value",
    "Approval Level",
    "Grade",
    "Qualifiers",
)


@dataclass(frozen=True, slots=True)
class UnitValueSource:
    region: str
    filename: str
    station_id: str
    path: Path
    source_uri: str
    publisher_modified: str
    publisher_listed_size: str
    drainage_area_gross_km2: float | None


@dataclass(frozen=True, slots=True)
class Selection:
    sources: tuple[UnitValueSource, ...]
    manifest_count: int
    unmatched_station_count: int
    missing_drainage_area_count: int
    excluded_by_area_count: int


def _quoted_identifier(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError("Invalid SQLite identifier")
    return '"' + value.replace('"', '""') + '"'


def _station_areas(path: Path, spec: DatasetSpec) -> dict[str, float | None]:
    options = spec.ingest_options
    table = str(options["station_table"])
    station = str(options["station_id_column"])
    area = str(options["drainage_area_column"])
    query = (
        f"SELECT {_quoted_identifier(station)}, {_quoted_identifier(area)} "
        f"FROM {_quoted_identifier(table)}"
    )
    uri = f"file:{path.expanduser().resolve(strict=True)}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        rows = connection.execute(query).fetchall()
    result: dict[str, float | None] = {}
    for station_id, gross_area in rows:
        if not isinstance(station_id, str):
            raise TypeError(
                f"HYDAT station id {station_id!r} is not stored as text"
            )
        result[station_id] = None if gross_area is None else float(gross_area)
    return result


def select_sources(
    manifest_path: str | Path,
    station_metadata_path: str | Path,
    spec: DatasetSpec,
    *,
    max_drainage_area_km2: float | None,
) -> Selection:
    """Resolve publisher files and apply an optional gross-drainage-area limit."""

    manifest_path = Path(manifest_path).expanduser().resolve(strict=True)
    station_metadata_path = (
        Path(station_metadata_path).expanduser().resolve(strict=True)
    )
    if max_drainage_area_km2 is not None and max_drainage_area_km2 < 0:
        raise ValueError("max_drainage_area_km2 must be non-negative")
    areas = _station_areas(station_metadata_path, spec)
    expected = tuple(spec.ingest_options["manifest_columns"])
    delimiter = str(spec.ingest_options["manifest_delimiter"])
    root = manifest_path.parent.resolve()
    selected: list[UnitValueSource] = []
    unmatched = 0
    missing_area = 0
    excluded = 0
    manifest_count = 0
    seen_paths: set[Path] = set()
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, delimiter=delimiter)
        if tuple(reader.fieldnames or ()) != expected:
            raise ValueError(
                f"Unexpected corrected unit-value manifest columns: {reader.fieldnames}"
            )
        for row_number, row in enumerate(reader, start=2):
            manifest_count += 1
            region = (row["region"] or "").strip()
            filename = (row["filename"] or "").strip()
            match = FILENAME.fullmatch(filename)
            if not match:
                raise ValueError(
                    f"Manifest row {row_number} has an unexpected filename: {filename!r}"
                )
            station_id = match.group("station")
            candidate = (root / region / filename).resolve(strict=True)
            if root not in candidate.parents or not candidate.is_file():
                raise ValueError(
                    f"Manifest row {row_number} resolves outside the collection: {candidate}"
                )
            if candidate in seen_paths:
                raise ValueError(f"Manifest repeats source path: {candidate}")
            seen_paths.add(candidate)
            gross_area = areas.get(station_id)
            if station_id not in areas:
                unmatched += 1
            elif gross_area is None:
                missing_area += 1
            if max_drainage_area_km2 is not None:
                if station_id not in areas or gross_area is None:
                    continue
                if gross_area > max_drainage_area_km2:
                    excluded += 1
                    continue
            selected.append(
                UnitValueSource(
                    region=region,
                    filename=filename,
                    station_id=station_id,
                    path=candidate,
                    source_uri=(row["source_url"] or "").strip(),
                    publisher_modified=(row["publisher_modified"] or "").strip(),
                    publisher_listed_size=(
                        row["publisher_listed_size"] or ""
                    ).strip(),
                    drainage_area_gross_km2=gross_area,
                )
            )
    if not selected:
        raise ValueError("The corrected unit-value selection is empty")
    return Selection(
        sources=tuple(selected),
        manifest_count=manifest_count,
        unmatched_station_count=unmatched,
        missing_drainage_area_count=missing_area,
        excluded_by_area_count=excluded,
    )


def _metadata_and_reader(stream, station_id: str):
    metadata: dict[str, str] = {}
    header: list[str] | None = None
    for line_number, line in enumerate(stream, start=1):
        if line.startswith("#"):
            content = line[1:].strip()
            if ":" in content:
                key, value = content.split(":", 1)
                metadata[key.strip()] = value.strip()
            continue
        if not line.strip():
            continue
        header = next(csv.reader([line]))
        break
    if header is None:
        raise ValueError("Corrected unit-value source has no CSV header")
    normalized = list(header)
    if len(normalized) != len(EXPECTED_COLUMNS):
        raise ValueError(f"Unexpected corrected unit-value columns: {header}")
    if not normalized[1].startswith("Timestamp (UTC"):
        raise ValueError(f"Unexpected local timestamp heading: {normalized[1]!r}")
    normalized[1] = "Timestamp"
    if tuple(normalized) != EXPECTED_COLUMNS:
        raise ValueError(f"Unexpected corrected unit-value columns: {header}")
    required_metadata = {
        "Time-series identifier": f"Discharge.Working@{station_id}",
        "Value units": "m^3/s",
        "Value parameter": "Discharge",
        "Interpolation type": "Instantaneous Values",
    }
    for key, expected in required_metadata.items():
        if metadata.get(key) != expected:
            raise ValueError(
                f"Unexpected {key} for station {station_id}: {metadata.get(key)!r}"
            )
    if not metadata.get("Export options", "").startswith("Corrected signal"):
        raise ValueError(f"Station {station_id} is not a corrected-signal export")
    return metadata, csv.DictReader(stream, fieldnames=header), line_number


def _frame(records: list[dict[str, str]], station_id: str) -> pd.DataFrame:
    if any(None in record for record in records):
        raise ValueError(f"Station {station_id} contains a malformed CSV row")
    timestamps = pd.to_datetime(
        [record["ISO 8601 UTC"] for record in records],
        utc=True,
        errors="raise",
        format="ISO8601",
    )
    values = pd.to_numeric(
        [record["Value"] for record in records], errors="raise"
    ).astype("float64")
    return pd.DataFrame(
        {
            "entity_id": pd.Series([station_id] * len(records), dtype="string"),
            "time_start": timestamps,
            "discharge": values,
            "approval_level": pd.Series(
                [record["Approval Level"] for record in records], dtype="string"
            ),
            "grade": pd.Series(
                [record["Grade"] for record in records], dtype="string"
            ),
            "qualifiers": pd.Series(
                [record["Qualifiers"] for record in records], dtype="string"
            ),
        }
    )


def parse(
    path: Path,
    spec: DatasetSpec,
    completed: set[str],
    *,
    station_id: str,
    source_key: str,
    rows_per_batch: int = 250_000,
) -> Iterable[ParsedChunk]:
    """Parse one compressed station export without materializing its CSV."""

    if spec.storage_model is not StorageModel.WIDE:
        raise ValueError("Corrected unit values require wide storage")
    if spec.temporal_kind is not TemporalKind.INSTANT:
        raise ValueError("Corrected unit values require instant timestamps")
    if rows_per_batch < 1:
        raise ValueError("rows_per_batch must be positive")
    row_count = 0
    batch_number = 0
    with lzma.open(path, mode="rt", encoding="utf-8-sig", newline="") as stream:
        _, reader, _ = _metadata_and_reader(stream, station_id)
        records: list[dict[str, str]] = []
        for record in reader:
            extra_qualifiers = record.pop(None, [])
            if not any((value or "").strip() for value in record.values()):
                continue
            qualifier_values = [record["Qualifiers"], *extra_qualifiers]
            record["Qualifiers"] = json.dumps(
                [value for value in qualifier_values if value != ""],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            records.append(record)
            row_count += 1
            if len(records) >= rows_per_batch:
                batch_number += 1
                yield from chunks_from_frame(
                    _frame(records, station_id),
                    spec,
                    key_prefix=f"unit={source_key}:batch={batch_number}",
                    completed=completed,
                )
                records = []
        if records:
            batch_number += 1
            yield from chunks_from_frame(
                _frame(records, station_id),
                spec,
                key_prefix=f"unit={source_key}:batch={batch_number}",
                completed=completed,
            )
    if row_count == 0:
        raise ValueError(f"Corrected unit-value source for {station_id} has no rows")


def _selection_fingerprint(
    *,
    manifest: SourceAsset,
    station_metadata: SourceAsset,
    max_drainage_area_km2: float | None,
    members: list[tuple[UnitValueSource, SourceAsset]],
) -> str:
    payload = {
        "manifest_sha256": manifest.sha256,
        "station_metadata_sha256": station_metadata.sha256,
        "max_drainage_area_km2": max_drainage_area_km2,
        "members": [
            {
                "region": source.region,
                "filename": source.filename,
                "station_id": source.station_id,
                "drainage_area_gross_km2": source.drainage_area_gross_km2,
                "sha256": asset.sha256,
            }
            for source, asset in members
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def ingest(
    dataset_id: str,
    source_path: str | Path,
    *,
    station_metadata: str | Path | None = None,
    max_drainage_area_km2: float | None = None,
    registry: Registry,
    paths: StorePaths,
    source_uri: str | None = None,
    publisher_vintage: str | None = None,
    fetched_at: str | None = None,
) -> str:
    if station_metadata is None:
        raise ValueError(
            "Corrected unit-value ingestion requires --station-metadata HYDAT.sqlite3"
        )
    spec = registry.get(dataset_id)
    spec.require_ready()
    writer = StoreWriter(paths, registry)
    selection = select_sources(
        source_path,
        station_metadata,
        spec,
        max_drainage_area_km2=max_drainage_area_km2,
    )
    manifest_asset = writer.archive_source(
        Path(source_path),
        source_uri=source_uri,
        publisher_vintage=publisher_vintage,
        fetched_at=fetched_at,
    )
    metadata_asset = writer.archive_source(
        Path(station_metadata),
        source_uri=str(spec.ingest_options["station_metadata_uri"]),
        publisher_vintage=publisher_vintage,
        fetched_at=fetched_at,
    )
    members: list[tuple[UnitValueSource, SourceAsset]] = []
    for source in selection.sources:
        members.append(
            (
                source,
                writer.archive_source(
                    source.path,
                    source_uri=source.source_uri,
                    publisher_vintage=publisher_vintage,
                    fetched_at=fetched_at,
                ),
            )
        )
    fingerprint = _selection_fingerprint(
        manifest=manifest_asset,
        station_metadata=metadata_asset,
        max_drainage_area_km2=max_drainage_area_km2,
        members=members,
    )
    run = writer.begin(
        spec,
        manifest_asset,
        ingester_version=f"{VERSION}+selection.{fingerprint}",
    )
    if run.state == "committed":
        return run.snapshot_id
    writer.catalog.record_ingestion_input(
        run_id=run.run_id,
        input_key="publisher_manifest",
        source_id=manifest_asset.source_id,
        input_role="publisher_manifest",
        details={
            "manifest_count": selection.manifest_count,
            "selected_count": len(selection.sources),
            "unmatched_station_count": selection.unmatched_station_count,
            "missing_drainage_area_count": selection.missing_drainage_area_count,
            "excluded_by_area_count": selection.excluded_by_area_count,
        },
    )
    writer.catalog.record_ingestion_input(
        run_id=run.run_id,
        input_key="station_metadata",
        source_id=metadata_asset.source_id,
        input_role="selection_metadata",
        details={
            "drainage_area_field": spec.ingest_options["drainage_area_column"],
            "max_drainage_area_km2": max_drainage_area_km2,
        },
    )
    for source, asset in members:
        writer.catalog.record_ingestion_input(
            run_id=run.run_id,
            input_key=f"member:{source.region}/{source.filename}",
            source_id=asset.source_id,
            input_role="observation_source",
            details={
                "station_id": source.station_id,
                "drainage_area_gross_km2": source.drainage_area_gross_km2,
                "publisher_modified": source.publisher_modified,
                "publisher_listed_size": source.publisher_listed_size,
            },
        )
    completed = writer.catalog.completed_chunk_keys(run.run_id)
    try:
        for source, asset in members:
            for chunk in parse(
                asset.raw_path,
                spec,
                completed,
                station_id=source.station_id,
                source_key=asset.sha256[:20],
            ):
                writer.write_chunk(
                    run=run,
                    spec=spec,
                    source=asset,
                    chunk_key=chunk.chunk_key,
                    table=chunk.table,
                    partition=chunk.partition,
                )
        return writer.publish(run=run, spec=spec, source=manifest_asset)
    except BaseException as error:
        writer.catalog.mark_run_failed(run.run_id, repr(error))
        raise
