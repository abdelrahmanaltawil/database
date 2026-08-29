from __future__ import annotations

import csv
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.conventions import signed_longitude
from research_store.foundation.models import DatasetSpec
from research_store.foundation.pipeline import ParsedChunk, ingest_file

VERSION = "1"


def parse(path: Path, spec: DatasetSpec, completed: set[str]) -> Iterable[ParsedChunk]:
    options = spec.ingest_options
    header_identifier = options.get("header_identifier")
    column_map = dict(options.get("column_map") or {})
    longitude_convention = options.get("source_longitude_convention")
    if not header_identifier or not column_map or not longitude_convention:
        raise ValueError(
            "Inventory header, column map, and coordinate convention are unresolved"
        )
    with path.open(
        "rt", encoding=str(options.get("encoding", "utf-8-sig")), newline=""
    ) as stream:
        lines = stream.readlines()
    header_index = next(
        (index for index, line in enumerate(lines) if str(header_identifier) in line),
        None,
    )
    if header_index is None:
        raise ValueError(
            f"Could not find real CSV header containing {header_identifier!r}"
        )
    reader = csv.DictReader(lines[header_index:])
    rows: list[dict[str, Any]] = []
    for source_row in reader:
        canonical = {
            target: source_row[source] for target, source in column_map.items()
        }
        entity = canonical.get(spec.entity_field)
        if not isinstance(entity, str) or not entity:
            raise TypeError("Inventory entity identifiers must be non-empty strings")
        canonical["latitude"] = float(canonical["latitude"])
        canonical["longitude"] = signed_longitude(
            float(canonical["longitude"]), str(longitude_convention)
        )
        canonical["elevation"] = (
            None
            if canonical.get("elevation") in {None, ""}
            else float(canonical["elevation"])
        )
        rows.append(canonical)
    frame = pd.DataFrame(rows)
    frame[spec.entity_field] = frame[spec.entity_field].astype("string")
    frame["station_name"] = frame["station_name"].astype("string")
    for name in ("latitude", "longitude", "elevation"):
        frame[name] = pd.to_numeric(frame[name], errors="raise").astype("float64")
    yield from chunks_from_frame(
        frame,
        spec,
        key_prefix="inventory",
        completed=completed,
    )


def ingest(dataset_id: str, source_path: str | Path, **kwargs: Any) -> str:
    return ingest_file(
        dataset_id=dataset_id,
        source_path=source_path,
        parser=parse,
        ingester_version=VERSION,
        **kwargs,
    )
