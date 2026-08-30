from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfoNotFoundError

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_store.foundation.catalog import Catalog
from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.conventions import apply_sentinel
from research_store.foundation.models import DatasetSpec, Registry
from research_store.foundation.paths import StorePaths, resolve_store_paths
from research_store.foundation.pipeline import ParsedChunk, ingest_file
from research_store.foundation.timezones import standard_offset_history

VERSION = "4"


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
        if "scale" not in item or "duration_minutes" not in item:
            raise ValueError(f"Element {code!r} is missing scale or duration")
        positions = {"start_minute", "end_minute"} & set(item)
        if len(positions) != 1:
            raise ValueError(
                f"Element {code!r} must declare exactly one interval position"
            )
        duration = int(item["duration_minutes"])
        if duration <= 0:
            raise ValueError(f"Element {code!r} duration must be positive")
        position_name = positions.pop()
        minute = int(item[position_name])
        before_duration = int(item.get("duration_minutes_before", duration))
        if before_duration <= 0:
            raise ValueError(f"Element {code!r} historical duration must be positive")
        if position_name == "start_minute":
            if minute < 0 or minute + max(duration, before_duration) > 60:
                raise ValueError(f"Element {code!r} interval exceeds its source hour")
        elif minute > 60 or minute - max(duration, before_duration) < 0:
            raise ValueError(f"Element {code!r} interval exceeds its source hour")
        result[str(code)] = item
    return result


def _local_interval(
    day: pd.Timestamp, hour: int, element: Mapping[str, Any]
) -> tuple[pd.Timestamp, pd.Timestamp]:
    slot = day + pd.Timedelta(hours=hour)
    duration = int(element["duration_minutes"])
    before_year = element.get("before_year")
    if before_year is not None and day.year < int(before_year):
        duration = int(element.get("duration_minutes_before", duration))
    if "start_minute" in element:
        start = slot + pd.Timedelta(minutes=int(element["start_minute"]))
        return start, start + pd.Timedelta(minutes=duration)
    end = slot + pd.Timedelta(minutes=int(element["end_minute"]))
    return end - pd.Timedelta(minutes=duration), end


def _standard_offset(
    local: pd.Timestamp,
    timezone_name: str,
    cache: dict[tuple[str, object], timedelta],
) -> timedelta:
    try:
        history = standard_offset_history(timezone_name)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"Unknown station timezone {timezone_name!r}") from error
    local_datetime = local.to_pydatetime()
    cache_component: object = local.date()
    if local.date() in history.transition_dates:
        cache_component = local_datetime
    key = (timezone_name, cache_component)
    if key in cache:
        return cache[key]
    standard = history.offset_at(local_datetime)
    cache[key] = standard
    return standard


def _utc_interval(
    local_start: pd.Timestamp,
    local_end: pd.Timestamp,
    timezone_name: str,
    cache: dict[tuple[str, object], timedelta],
) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = (
        local_start - _standard_offset(local_start, timezone_name, cache)
    ).tz_localize("UTC")
    end = (local_end - _standard_offset(local_end, timezone_name, cache)).tz_localize(
        "UTC"
    )
    if end <= start:
        raise ValueError(
            f"Station timezone policy produced a non-positive interval for "
            f"{timezone_name!r}: {local_start} to {local_end}"
        )
    return start, end


def _timezone_map(
    paths: StorePaths, registry: Registry, station_dataset_id: str
) -> dict[str, str]:
    registry.get(station_dataset_id).require_ready()
    try:
        _, fragments = Catalog(paths).committed_fragments(station_dataset_id)
    except LookupError as error:
        raise RuntimeError(
            f"Ingest {station_dataset_id!r} before ingesting ECCC observations"
        ) from error
    tables = [
        pq.read_table(path, columns=["entity_id", "timezone_name"])
        for path in fragments
    ]
    frame = pa.concat_tables(tables).to_pandas()
    if frame["entity_id"].duplicated().any():
        duplicated = frame.loc[frame["entity_id"].duplicated(), "entity_id"].head(3)
        raise ValueError(
            f"Station timezone lookup contains duplicate Climate IDs: "
            f"{duplicated.tolist()}"
        )
    available = frame.dropna(subset=["timezone_name"])
    return dict(
        zip(
            available["entity_id"].astype(str),
            available["timezone_name"].astype(str),
            strict=True,
        )
    )


def parse(
    path: Path,
    spec: DatasetSpec,
    completed: set[str],
    *,
    lines_per_batch: int = 10_000,
    timezone_by_entity: Mapping[str, str] | None = None,
) -> Iterable[ParsedChunk]:
    options = spec.ingest_options
    station_slice = _slice(options.get("station_slice"), "station_slice")
    date_slice = _slice(options.get("date_slice"), "date_slice")
    element_slice = _slice(options.get("element_slice"), "element_slice")
    values_start = options.get("values_start")
    elements = _element_specs(spec)
    if values_start is None:
        raise ValueError("values_start must be resolved in the registry")
    timezone_policy = options.get("timezone_policy", "fixed")
    if timezone_policy == "station_inventory" and timezone_by_entity is None:
        raise ValueError("Station timezone mapping is required for this dataset")
    if timezone_policy == "fixed" and spec.source_timezone is None:
        raise ValueError("source_timezone must be resolved in the registry")
    if timezone_policy not in {"fixed", "station_inventory"}:
        raise ValueError(f"Unsupported timezone policy: {timezone_policy!r}")
    field_width = int(options.get("field_width", 7))
    value_width = int(options.get("value_width", 6))
    expected_width = int(values_start) + 24 * field_width
    configured_entities = options.get("entity_allowlist", [])
    if not isinstance(configured_entities, (list, tuple, set)):
        raise TypeError("entity_allowlist must be a list of station identifiers")
    entity_allowlist = {str(value) for value in configured_entities}

    rows: list[dict[str, Any]] = []
    offset_cache: dict[tuple[str, object], timedelta] = {}
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
                element_spec = elements[element]
            except KeyError as error:
                raise ValueError(
                    f"Undeclared element code {element!r} on line {line_number}"
                ) from error
            variable = str(element_spec["variable"])
            if timezone_policy == "station_inventory":
                assert timezone_by_entity is not None
                try:
                    timezone_name = timezone_by_entity[entity]
                except KeyError as error:
                    raise ValueError(
                        f"No station timezone is available for Climate ID {entity!r}"
                    ) from error
            else:
                assert spec.source_timezone is not None
                timezone_name = spec.source_timezone
            for hour in range(24):
                offset = int(values_start) + hour * field_width
                raw_value = record[offset : offset + value_width]
                quality = record[offset + value_width : offset + field_width].strip()
                local_start, local_end = _local_interval(day, hour, element_spec)
                aware_start, aware_end = _utc_interval(
                    local_start, local_end, timezone_name, offset_cache
                )
                interpreted = apply_sentinel(
                    raw_value, aware_start, spec.sentinel_rules
                )
                value = (
                    None
                    if interpreted is None
                    else interpreted * float(element_spec["scale"])
                )
                rows.append(
                    {
                        spec.entity_field: entity,
                        spec.time_start_field: aware_start,
                        spec.time_end_field: aware_end,
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
    spec = registry.get(dataset_id)
    timezone_by_entity: Mapping[str, str] | None = None
    if spec.ingest_options.get("timezone_policy") == "station_inventory":
        station_dataset_id = str(spec.ingest_options.get("station_dataset_id") or "")
        if not station_dataset_id:
            raise ValueError("station_dataset_id is required by timezone policy")
        timezone_by_entity = _timezone_map(paths, registry, station_dataset_id)

    def parser(
        path: Path, declared: DatasetSpec, completed: set[str]
    ) -> Iterable[ParsedChunk]:
        return parse(
            path,
            declared,
            completed,
            timezone_by_entity=timezone_by_entity,
        )

    return ingest_file(
        dataset_id=dataset_id,
        source_path=source_path,
        parser=parser,
        ingester_version=VERSION,
        **kwargs,
    )
