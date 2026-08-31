"""ECCC daily fixed-width archive adapter.

One physical record holds one station, one month and one element, followed by
31 day slots. Day slots that the calendar cannot contain are dropped; every
retained slot is stamped with the climatological day declared by the registry's
era rules. The boundary is a fixed UTC hour, so this adapter needs no station
timezone lookup.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.conventions import apply_sentinel, valid_calendar_day
from research_store.foundation.models import DatasetSpec, Registry
from research_store.foundation.paths import StorePaths, resolve_store_paths
from research_store.foundation.pipeline import ParsedChunk, ingest_file

VERSION = "1"

# Boundaries for stations reporting at morning and afternoon observation times
# are station-specific and recoverable only from historical inspection reports,
# so this adapter refuses them rather than assuming a national convention.
SUPPORTED_DAY_CLASSES = frozenset({"synoptic_24h"})


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
    frame["source_element"] = frame["source_element"].astype("string")
    return frame


def _element_specs(spec: DatasetSpec) -> dict[str, dict[str, Any]]:
    configured = spec.ingest_options.get("elements")
    if not isinstance(configured, Mapping) or not configured:
        raise ValueError("elements must be a non-empty mapping in the registry")
    result: dict[str, dict[str, Any]] = {}
    for code, raw in configured.items():
        if not isinstance(raw, Mapping):
            raise TypeError(f"Element {code!r} declaration must be a mapping")
        item = dict(raw)
        variable = item.get("variable")
        if variable not in spec.variable_names:
            raise ValueError(
                f"Element {code!r} maps to undeclared variable {variable!r}"
            )
        if "scale" not in item:
            raise ValueError(f"Element {code!r} is missing its scale")
        float(item["scale"])
        result[str(code)] = item
    return result


def _day_class(options: Mapping[str, Any]) -> str:
    declared = options.get("station_day_class")
    if declared not in SUPPORTED_DAY_CLASSES:
        raise ValueError(
            "station_day_class must be declared in the registry as one of "
            f"{sorted(SUPPORTED_DAY_CLASSES)}; got {declared!r}. Stations that "
            "report at morning and afternoon observation times close their "
            "climatological day at station-specific times that this archive "
            "does not carry."
        )
    return str(declared)


def _day_boundary_rules(
    options: Mapping[str, Any],
) -> tuple[tuple[date | None, date | None, int], ...]:
    """Parse inclusive-start, exclusive-end climatological-day era rules."""

    configured = options.get("climatological_day_rules")
    if not isinstance(configured, (list, tuple)) or not configured:
        raise ValueError(
            "climatological_day_rules must be declared in the registry; the "
            "observation-day boundary is not carried by the source file"
        )
    rules: list[tuple[date | None, date | None, int]] = []
    for raw in configured:
        if not isinstance(raw, Mapping):
            raise TypeError("Each climatological-day rule must be a mapping")
        if "utc_end_hour" not in raw:
            raise ValueError("Each climatological-day rule needs utc_end_hour")
        hour = int(raw["utc_end_hour"])
        if not 0 <= hour < 24:
            raise ValueError(f"utc_end_hour must be an hour of day, got {hour}")
        start = None if raw.get("start") is None else pd.Timestamp(raw["start"]).date()
        end = None if raw.get("end") is None else pd.Timestamp(raw["end"]).date()
        if start is not None and end is not None and end <= start:
            raise ValueError("A climatological-day era must end after it starts")
        rules.append((start, end, hour))
    return tuple(rules)


def _utc_end_hour(
    day: date, rules: tuple[tuple[date | None, date | None, int], ...]
) -> int:
    matching = [
        hour
        for start, end, hour in rules
        if (start is None or day >= start) and (end is None or day < end)
    ]
    if len(matching) > 1:
        raise ValueError(f"Overlapping climatological-day rules cover {day}")
    if not matching:
        raise ValueError(
            f"No climatological-day rule covers {day}; refusing to guess the "
            "observation-day boundary for this era"
        )
    return matching[0]


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
    if values_start is None:
        raise ValueError("values_start must be resolved in the registry")
    elements = _element_specs(spec)
    _day_class(options)
    rules = _day_boundary_rules(options)
    field_width = int(options.get("field_width", 7))
    value_width = int(options.get("value_width", 6))
    day_slots = int(options.get("day_slots", 31))
    if day_slots < 28 or day_slots > 31:
        raise ValueError(f"day_slots must cover a calendar month, got {day_slots}")
    expected_width = int(values_start) + day_slots * field_width
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

            entity = record[station_slice].strip()
            if not entity:
                raise ValueError(f"Missing station identifier on line {line_number}")
            if entity_allowlist and entity not in entity_allowlist:
                continue
            record_count += 1
            month_start = pd.to_datetime(
                record[date_slice],
                format=str(options.get("date_format", "%Y%m")),
                errors="raise",
            )
            year = int(month_start.year)
            month = int(month_start.month)
            element = record[element_slice].strip()
            try:
                element_spec = elements[element]
            except KeyError as error:
                raise ValueError(
                    f"Undeclared element code {element!r} on line {line_number}"
                ) from error
            variable = str(element_spec["variable"])
            scale = float(element_spec["scale"])
            for slot in range(day_slots):
                day_of_month = slot + 1
                if not valid_calendar_day(year, month, day_of_month):
                    continue
                observation_day = date(year, month, day_of_month)
                end_hour = _utc_end_hour(observation_day, rules)
                start = pd.Timestamp(
                    year=year,
                    month=month,
                    day=day_of_month,
                    hour=end_hour,
                    tz="UTC",
                )
                end = start + pd.Timedelta(days=1)
                offset = int(values_start) + slot * field_width
                raw_value = record[offset : offset + value_width]
                quality = record[offset + value_width : offset + field_width].strip()
                interpreted = apply_sentinel(raw_value, start, spec.sentinel_rules)
                value = None if interpreted is None else interpreted * scale
                rows.append(
                    {
                        spec.entity_field: entity,
                        spec.time_start_field: start,
                        spec.time_end_field: end,
                        "variable": variable,
                        "value": value,
                        "quality_flag": quality,
                        "source_element": element,
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
    registry = kwargs.get("registry")
    if not isinstance(registry, Registry):
        raise TypeError("registry is required for ECCC ingestion")
    paths = kwargs.get("paths")
    if paths is None:
        paths = resolve_store_paths(for_write=True)
        kwargs["paths"] = paths
    if not isinstance(paths, StorePaths):
        raise TypeError("paths must be StorePaths")

    def parser(
        path: Path, declared: DatasetSpec, completed: set[str]
    ) -> Iterable[ParsedChunk]:
        return parse(path, declared, completed)

    return ingest_file(
        dataset_id=dataset_id,
        source_path=source_path,
        parser=parser,
        ingester_version=VERSION,
        **kwargs,
    )
