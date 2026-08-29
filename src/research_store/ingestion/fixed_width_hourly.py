from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.conventions import apply_sentinel
from research_store.foundation.models import DatasetSpec
from research_store.foundation.pipeline import ParsedChunk, ingest_file

VERSION = "2"


def _slice(value: Any, name: str) -> slice:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError(
            f"{name} must be a two-integer [start, end] slice in the registry"
        )
    return slice(int(value[0]), int(value[1]))


def _canonical_frame(rows: list[dict[str, Any]], spec: DatasetSpec) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame[spec.entity_field] = frame[spec.entity_field].astype("string")
    frame["variable"] = frame["variable"].astype("string")
    frame["value"] = pd.to_numeric(frame["value"], errors="raise").astype("float64")
    frame["quality_flag"] = frame["quality_flag"].astype("string")
    return frame


def parse(
    path: Path,
    spec: DatasetSpec,
    completed: set[str],
    *,
    lines_per_batch: int = 10_000,
) -> Iterable[ParsedChunk]:
    options = spec.ingest_options
    station_slice = _slice(options.get("station_slice"), "station_slice")
    date_slice = _slice(options.get("date_slice"), "date_slice")
    element_slice = _slice(options.get("element_slice"), "element_slice")
    values_start = options.get("values_start")
    scale = options.get("scale")
    element_map = dict(options.get("element_map") or {})
    if values_start is None or scale is None or not element_map:
        raise ValueError(
            "values_start, scale, and element_map must be resolved in the registry"
        )
    if spec.source_timezone is None:
        raise ValueError("source_timezone must be resolved in the registry")
    field_width = int(options.get("field_width", 7))
    value_width = int(options.get("value_width", 6))
    expected_width = int(values_start) + 24 * field_width
    configured_entities = options.get("entity_allowlist", [])
    if not isinstance(configured_entities, (list, tuple, set)):
        raise TypeError("entity_allowlist must be a list of station identifiers")
    entity_allowlist = {str(value) for value in configured_entities}

    rows: list[dict[str, Any]] = []
    batch_start = 1
    record_count = 0
    with path.open(
        "rt", encoding=str(options.get("encoding", "ascii")), newline=""
    ) as stream:
        for line_number, line in enumerate(stream, start=1):
            record = line.rstrip("\r\n")
            if not record:
                continue
            if len(record) == expected_width - 1:
                # Some exports omit the final blank quality flag.
                record += " "
            elif len(record) < expected_width:
                raise ValueError(
                    f"Truncated fixed-width record on line {line_number}: "
                    f"expected {expected_width} characters, got {len(record)}"
                )
            elif len(record) > expected_width:
                overflow = record[expected_width:]
                if overflow.strip():
                    raise ValueError(
                        f"Unexpected data after position {expected_width} "
                        f"on line {line_number}"
                    )
                record = record[:expected_width]

            entity = record[station_slice]
            if entity != entity.strip():
                entity = entity.strip()
            if not entity:
                raise ValueError(f"Missing station identifier on line {line_number}")
            if entity_allowlist and entity not in entity_allowlist:
                continue
            record_count += 1
            day = pd.to_datetime(
                record[date_slice],
                format=str(options.get("date_format", "%Y%m%d")),
                errors="raise",
            )
            element = record[element_slice].strip()
            try:
                variable = element_map[element]
            except KeyError as error:
                raise ValueError(
                    f"Undeclared element code {element!r} on line {line_number}"
                ) from error
            for hour in range(24):
                offset = int(values_start) + hour * field_width
                raw_value = record[offset : offset + value_width]
                quality = record[offset + value_width : offset + field_width].strip()
                local_start = day + pd.Timedelta(hours=hour)
                aware_start = local_start.tz_localize(
                    spec.source_timezone, ambiguous="raise", nonexistent="raise"
                ).tz_convert("UTC")
                interpreted = apply_sentinel(
                    raw_value, aware_start, spec.sentinel_rules
                )
                value = None if interpreted is None else interpreted * float(scale)
                rows.append(
                    {
                        spec.entity_field: entity,
                        spec.time_start_field: aware_start,
                        spec.time_end_field: aware_start + timedelta(hours=1),
                        "variable": variable,
                        "value": value,
                        "quality_flag": quality,
                    }
                )
            if record_count % lines_per_batch == 0:
                frame = _canonical_frame(rows, spec)
                yield from chunks_from_frame(
                    frame,
                    spec,
                    key_prefix=f"records={batch_start}-{record_count}",
                    completed=completed,
                )
                rows.clear()
                batch_start = record_count + 1
        if rows:
            frame = _canonical_frame(rows, spec)
            yield from chunks_from_frame(
                frame,
                spec,
                key_prefix=f"records={batch_start}-{record_count}",
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
