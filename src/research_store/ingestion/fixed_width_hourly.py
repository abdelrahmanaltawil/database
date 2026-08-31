from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
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
from research_store.foundation.pipeline import (
    ParsedEvent,
    RejectedRecord,
    ingest_file,
)
from research_store.foundation.timezones import pinned_zoneinfo

# 10: snow-depth elements 275-278 rescaled from 1.0 to 0.01 after the
# delivered RCS bytes were found to contradict the public unit table.
VERSION = "10"


class StandardTimeTransitionError(ValueError):
    """A source day or interval crosses a change in standard UTC offset."""


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


def _standard_offset_at(local: pd.Timestamp, timezone_name: str) -> timedelta:
    """Resolve the standard (non-daylight) offset at one local instant."""

    try:
        zone = pinned_zoneinfo(timezone_name)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"Unknown station timezone {timezone_name!r}") from error
    aware = local.to_pydatetime().replace(tzinfo=zone, fold=0)
    utc_offset = aware.utcoffset()
    if utc_offset is None:
        raise ValueError(f"Timezone {timezone_name!r} has no UTC offset")
    daylight = aware.dst() or timedelta(0)
    if daylight == timedelta(0):
        return utc_offset

    # The ordinary IANA representation is authoritative here: civil UTC
    # offset minus a one-hour daylight adjustment gives local standard time.
    # This is especially important for Yukon in 2020, when America/Dawson's
    # standard offset changed from UTC-8 to UTC-7. Searching for the nearest
    # non-DST month would incorrectly move that change into July.
    if abs(daylight) <= timedelta(hours=1):
        return utc_offset - daylight

    # A few IANA histories expose a non-standard daylight delta. For example,
    # Python reports two hours for America/Inuvik in 2004 even though its
    # MDT/MST offsets differ by one hour. Resolve only those exceptional cases
    # from the closest non-DST month, using the same pinned timezone data.
    candidates: list[tuple[int, timedelta]] = []
    # Some jurisdictions observed daylight time continuously for several
    # years during wartime.  Search far enough across the pinned IANA
    # history to find the nearest explicitly non-DST offset while keeping
    # a finite bound so an all-DST history still fails closed.
    for month_distance in range(1, 121):
        for direction in (-1, 1):
            probe = local + pd.DateOffset(months=direction * month_distance)
            probe = probe.replace(day=15, hour=12, minute=0, second=0, microsecond=0)
            probe_aware = probe.to_pydatetime().replace(tzinfo=zone, fold=0)
            probe_offset = probe_aware.utcoffset()
            if probe_offset is not None and not (probe_aware.dst() or timedelta(0)):
                candidates.append((month_distance, probe_offset))
        if candidates:
            break
    if not candidates:
        raise ValueError(
            f"No nearby non-DST offset is available for {timezone_name!r} "
            f"at {local}; refusing to guess local standard time"
        )
    return candidates[0][1]


def _standard_offset(
    local: pd.Timestamp,
    timezone_name: str,
    cache: dict[tuple[str, object], timedelta | None],
) -> timedelta:
    key = (timezone_name, local.date())
    if key in cache:
        cached = cache[key]
        return (
            _standard_offset_at(local, timezone_name)
            if cached is None
            else cached
        )

    day_start = local.normalize()
    next_day_start = day_start + pd.Timedelta(days=1)
    start_offset = _standard_offset_at(day_start, timezone_name)
    end_offset = _standard_offset_at(next_day_start, timezone_name)
    if start_offset == end_offset:
        cache[key] = start_offset
        return start_offset
    # None marks the rare day on which the jurisdiction changed standard
    # offset. Resolve each boundary separately so the transition is detected.
    cache[key] = None
    return _standard_offset_at(local, timezone_name)


def _utc_interval(
    local_start: pd.Timestamp,
    local_end: pd.Timestamp,
    timezone_name: str,
    cache: dict[tuple[str, object], timedelta | None],
) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = (
        local_start - _standard_offset(local_start, timezone_name, cache)
    ).tz_localize("UTC")
    end = (local_end - _standard_offset(local_end, timezone_name, cache)).tz_localize(
        "UTC"
    )
    local_duration = local_end - local_start
    utc_duration = end - start
    if utc_duration != local_duration:
        raise StandardTimeTransitionError(
            f"Station standard UTC offset changes inside the source interval for "
            f"{timezone_name!r}: {local_start} to {local_end} becomes "
            f"{utc_duration} instead of {local_duration}"
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


def _timezone_overrides(spec: DatasetSpec) -> dict[str, str]:
    configured = spec.ingest_options.get("timezone_overrides", {})
    if not isinstance(configured, Mapping):
        raise TypeError("timezone_overrides must be a mapping by Climate ID")
    result: dict[str, str] = {}
    for entity, raw in configured.items():
        if not isinstance(raw, Mapping):
            raise TypeError(f"Timezone override for {entity!r} must be a mapping")
        timezone_name = str(raw.get("timezone_name") or "")
        evidence = str(raw.get("evidence") or "")
        if not timezone_name or not evidence:
            raise ValueError(
                f"Timezone override for {entity!r} needs timezone_name and evidence"
            )
        try:
            pinned_zoneinfo(timezone_name)
        except ZoneInfoNotFoundError as error:
            raise ValueError(
                f"Unknown timezone override {timezone_name!r} for {entity!r}"
            ) from error
        result[str(entity)] = timezone_name
    return result


def _raw_sha256(value: str, encoding: str) -> str:
    return hashlib.sha256(value.encode(encoding)).hexdigest()


def _rejection(
    *,
    line_number: int,
    rejection_key: str,
    record_locator: str,
    reason: str,
    raw: str,
    encoding: str,
    recovered_record_count: int = 0,
    details: dict[str, Any] | None = None,
) -> RejectedRecord:
    return RejectedRecord(
        rejection_key=f"line={line_number}:{rejection_key}",
        record_locator=record_locator,
        reason=reason,
        raw_sha256=_raw_sha256(raw, encoding),
        raw_length=len(raw),
        recovered_record_count=recovered_record_count,
        details=details,
    )


def _structural_error(
    record: str,
    *,
    expected_width: int,
    station_slice: slice,
    date_slice: slice,
    date_format: str,
    element_slice: slice,
    elements: Mapping[str, Mapping[str, Any]],
    values_start: int,
    field_width: int,
    value_width: int,
    sentinel_markers: set[str],
) -> str | None:
    """Return why a candidate is invalid without applying timezone semantics."""

    if len(record) != expected_width:
        return f"expected {expected_width} characters, got {len(record)}"
    if not record[station_slice].strip():
        return "missing station identifier"
    try:
        datetime.strptime(record[date_slice], date_format)
    except ValueError as error:
        return f"invalid date field: {error}"
    element = record[element_slice].strip()
    if element not in elements:
        return f"undeclared element code {element!r}"
    for hour in range(24):
        offset = values_start + hour * field_width
        raw_value = record[offset : offset + value_width]
        if raw_value in sentinel_markers:
            continue
        try:
            float(raw_value)
        except ValueError:
            return f"invalid numeric field for hour {hour + 1}: {raw_value!r}"
    return None


def _recover_records(
    raw: str,
    *,
    expected_width: int,
    structural_options: dict[str, Any],
) -> list[tuple[int, str, str]]:
    """Find non-overlapping valid records inside one malformed physical line."""

    recovered: list[tuple[int, str, str]] = []
    offset = 0
    final_offset = len(raw) - expected_width
    while offset <= final_offset:
        candidate = raw[offset : offset + expected_width]
        if _structural_error(
            candidate, expected_width=expected_width, **structural_options
        ) is None:
            recovered.append((offset, candidate, candidate))
            offset += expected_width
        else:
            offset += 1
    return recovered


def _records_from_line(
    raw: str,
    *,
    line_number: int,
    expected_width: int,
    malformed_record_policy: str,
    encoding: str,
    structural_options: dict[str, Any],
) -> tuple[list[tuple[int, str, str]], RejectedRecord | None]:
    """Normalize one physical line and optionally describe its quarantined bytes."""

    if len(raw) == expected_width - 1:
        normalized = raw + " "
        if malformed_record_policy == "salvage_valid_fixed_width_records":
            error = _structural_error(
                normalized, expected_width=expected_width, **structural_options
            )
            if error is not None:
                return [], _rejection(
                    line_number=line_number,
                    rejection_key="invalid-record",
                    record_locator=f"line:{line_number}",
                    reason="invalid_fixed_width_record",
                    raw=raw,
                    encoding=encoding,
                    details={"structural_error": error},
                )
        return [(0, normalized, raw)], None
    if len(raw) == expected_width:
        if malformed_record_policy == "salvage_valid_fixed_width_records":
            error = _structural_error(
                raw, expected_width=expected_width, **structural_options
            )
            if error is not None:
                return [], _rejection(
                    line_number=line_number,
                    rejection_key="invalid-record",
                    record_locator=f"line:{line_number}",
                    reason="invalid_fixed_width_record",
                    raw=raw,
                    encoding=encoding,
                    details={"structural_error": error},
                )
        return [(0, raw, raw)], None
    if len(raw) < expected_width - 1:
        if malformed_record_policy != "salvage_valid_fixed_width_records":
            raise ValueError(
                f"Truncated fixed-width record on line {line_number}: "
                f"expected {expected_width} characters, got {len(raw)}"
            )
        return [], _rejection(
            line_number=line_number,
            rejection_key="truncated-record",
            record_locator=f"line:{line_number}",
            reason="truncated_fixed_width_record",
            raw=raw,
            encoding=encoding,
            details={"expected_width": expected_width},
        )

    overflow = raw[expected_width:]
    if not overflow.strip():
        return [(0, raw[:expected_width], raw[:expected_width])], None
    if malformed_record_policy != "salvage_valid_fixed_width_records":
        raise ValueError(
            f"Unexpected data after position {expected_width} on line {line_number}"
        )
    recovered = _recover_records(
        raw,
        expected_width=expected_width,
        structural_options=structural_options,
    )
    offsets = [offset for offset, _, _ in recovered]
    reason = (
        "extra_bytes_around_valid_fixed_width_records"
        if recovered
        else "invalid_fixed_width_record"
    )
    return recovered, _rejection(
        line_number=line_number,
        rejection_key="malformed-physical-line",
        record_locator=f"line:{line_number}",
        reason=reason,
        raw=raw,
        encoding=encoding,
        recovered_record_count=len(recovered),
        details={
            "expected_width": expected_width,
            "recovered_offsets": offsets,
            "discarded_character_count": len(raw) - len(recovered) * expected_width,
        },
    )


def parse(
    path: Path,
    spec: DatasetSpec,
    completed: set[str],
    *,
    lines_per_batch: int = 10_000,
    timezone_by_entity: Mapping[str, str] | None = None,
) -> Iterable[ParsedEvent]:
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
    encoding = str(options.get("encoding", "ascii"))
    malformed_record_policy = str(options.get("malformed_record_policy", "error"))
    if malformed_record_policy not in {"error", "salvage_valid_fixed_width_records"}:
        raise ValueError(
            f"Unsupported malformed record policy: {malformed_record_policy!r}"
        )
    missing_timezone_policy = str(options.get("missing_timezone_policy", "error"))
    if missing_timezone_policy not in {"error", "quarantine_record"}:
        raise ValueError(
            f"Unsupported missing timezone policy: {missing_timezone_policy!r}"
        )
    standard_time_transition_policy = str(
        options.get("standard_time_transition_policy", "error")
    )
    if standard_time_transition_policy not in {"error", "quarantine_record"}:
        raise ValueError(
            "Unsupported standard-time transition policy: "
            f"{standard_time_transition_policy!r}"
        )
    configured_entities = options.get("entity_allowlist", [])
    if not isinstance(configured_entities, (list, tuple, set)):
        raise TypeError("entity_allowlist must be a list of station identifiers")
    entity_allowlist = {str(value) for value in configured_entities}

    rows: list[dict[str, Any]] = []
    offset_cache: dict[tuple[str, object], timedelta | None] = {}
    batch_start = 1
    record_count = 0
    structural_options = {
        "station_slice": station_slice,
        "date_slice": date_slice,
        "date_format": str(options.get("date_format", "%Y%m%d")),
        "element_slice": element_slice,
        "elements": elements,
        "values_start": int(values_start),
        "field_width": field_width,
        "value_width": value_width,
        "sentinel_markers": {rule.marker for rule in spec.sentinel_rules},
    }
    with path.open("rt", encoding=encoding, newline="") as stream:
        for line_number, line in enumerate(stream, start=1):
            raw_line = line.rstrip("\r\n")
            if not raw_line:
                continue
            records, malformed = _records_from_line(
                raw_line,
                line_number=line_number,
                expected_width=expected_width,
                malformed_record_policy=malformed_record_policy,
                encoding=encoding,
                structural_options=structural_options,
            )
            if malformed is not None:
                yield malformed
            for recovered_index, (record_offset, record, source_record) in enumerate(
                records, start=1
            ):
                entity = record[station_slice].strip()
                if not entity:
                    raise ValueError(f"Missing station identifier on line {line_number}")
                if entity_allowlist and entity not in entity_allowlist:
                    continue
                day = datetime.strptime(
                    record[date_slice],
                    str(options.get("date_format", "%Y%m%d")),
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
                    timezone_name = timezone_by_entity.get(entity)
                    if timezone_name is None:
                        if missing_timezone_policy == "quarantine_record":
                            start = record_offset + 1
                            end = record_offset + len(source_record)
                            yield _rejection(
                                line_number=line_number,
                                rejection_key=(
                                    f"record={recovered_index}:missing-timezone"
                                ),
                                record_locator=(
                                    f"line:{line_number}:chars:{start}-{end}"
                                ),
                                reason="missing_station_timezone",
                                raw=source_record,
                                encoding=encoding,
                                details={
                                    "entity_id": entity,
                                    "source_date": record[date_slice],
                                    "source_element": element,
                                },
                            )
                            continue
                        raise ValueError(
                            f"No station timezone is available for Climate ID {entity!r}"
                        )
                else:
                    assert spec.source_timezone is not None
                    timezone_name = spec.source_timezone
                duration_minutes = int(element_spec["duration_minutes"])
                before_year = element_spec.get("before_year")
                if before_year is not None and day.year < int(before_year):
                    duration_minutes = int(
                        element_spec.get("duration_minutes_before", duration_minutes)
                    )
                if "start_minute" in element_spec:
                    interval_start_minute = int(element_spec["start_minute"])
                else:
                    interval_start_minute = (
                        int(element_spec["end_minute"]) - duration_minutes
                    )
                day_timestamp = pd.Timestamp(day)
                standard_offset = _standard_offset(
                    day_timestamp, timezone_name, offset_cache
                )
                transition_day = (
                    offset_cache[(timezone_name, day.date())] is None
                )
                if transition_day:
                    error = StandardTimeTransitionError(
                        "Station standard UTC offset changes between source-day "
                        f"boundaries for {timezone_name!r} on {day.date()}"
                    )
                    if standard_time_transition_policy != "quarantine_record":
                        raise error
                    start = record_offset + 1
                    end = record_offset + len(source_record)
                    yield _rejection(
                        line_number=line_number,
                        rejection_key=(
                            f"record={recovered_index}:standard-time-transition"
                        ),
                        record_locator=f"line:{line_number}:chars:{start}-{end}",
                        reason="standard_timezone_transition",
                        raw=source_record,
                        encoding=encoding,
                        details={
                            "entity_id": entity,
                            "source_date": record[date_slice],
                            "source_element": element,
                            "timezone_name": timezone_name,
                            "error": str(error),
                        },
                    )
                    continue
                base_utc = (day - standard_offset).replace(tzinfo=timezone.utc)
                record_rows: list[dict[str, Any]] = []
                try:
                    for hour in range(24):
                        offset = int(values_start) + hour * field_width
                        raw_value = record[offset : offset + value_width]
                        quality = record[
                            offset + value_width : offset + field_width
                        ].strip()
                        aware_start = base_utc + timedelta(
                            hours=hour, minutes=interval_start_minute
                        )
                        aware_end = aware_start + timedelta(
                            minutes=duration_minutes
                        )
                        if raw_value in structural_options["sentinel_markers"]:
                            interpreted = apply_sentinel(
                                raw_value, aware_start, spec.sentinel_rules
                            )
                        else:
                            interpreted = float(raw_value)
                        value = (
                            None
                            if interpreted is None
                            else interpreted * float(element_spec["scale"])
                        )
                        record_rows.append(
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
                except StandardTimeTransitionError as error:
                    if standard_time_transition_policy != "quarantine_record":
                        raise
                    start = record_offset + 1
                    end = record_offset + len(source_record)
                    yield _rejection(
                        line_number=line_number,
                        rejection_key=(
                            f"record={recovered_index}:standard-time-transition"
                        ),
                        record_locator=f"line:{line_number}:chars:{start}-{end}",
                        reason="standard_timezone_transition",
                        raw=source_record,
                        encoding=encoding,
                        details={
                            "entity_id": entity,
                            "source_date": record[date_slice],
                            "source_element": element,
                            "timezone_name": timezone_name,
                            "error": str(error),
                        },
                    )
                    continue
                rows.extend(record_rows)
                record_count += 1
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
        for entity, timezone_name in _timezone_overrides(spec).items():
            inventoried = timezone_by_entity.get(entity)
            if inventoried is not None and inventoried != timezone_name:
                raise ValueError(
                    f"Timezone override for {entity!r} conflicts with the station "
                    f"inventory: {timezone_name!r} != {inventoried!r}"
                )
            timezone_by_entity[entity] = timezone_name

    def parser(
        path: Path, declared: DatasetSpec, completed: set[str]
    ) -> Iterable[ParsedEvent]:
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
