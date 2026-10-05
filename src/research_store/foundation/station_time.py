"""Station-local standard time and its conversion to UTC.

ECCC climate products timestamp observations in each station's local standard
time (LST) all year: daylight saving is never applied. Two producers need the
same placement of an LST interval on the UTC timeline, the fixed-width HLY
archive and the MSC GeoMet climate-hourly API; which interval a source slot
denotes is each dataset's own registry declaration. The rules therefore live
here, once, rather than in either ingestion module (ingestion modules may not
import each other).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from zoneinfo import ZoneInfoNotFoundError

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_store.foundation.catalog import Catalog
from research_store.foundation.models import DatasetSpec, Registry
from research_store.foundation.paths import StorePaths
from research_store.foundation.timezones import pinned_zoneinfo

OffsetCache = dict[tuple[str, object], timedelta | None]


class StandardTimeTransitionError(ValueError):
    """A source day or interval crosses a change in standard UTC offset."""


def standard_offset_at(local: pd.Timestamp, timezone_name: str) -> timedelta:
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


def standard_offset(
    local: pd.Timestamp,
    timezone_name: str,
    cache: OffsetCache,
) -> timedelta:
    """The standard offset at `local`, cached per station timezone and day.

    A day on which the jurisdiction changed its standard offset is cached as
    ``None`` so callers can detect it; its instants are resolved one by one.
    """

    key = (timezone_name, local.date())
    if key in cache:
        cached = cache[key]
        return (
            standard_offset_at(local, timezone_name)
            if cached is None
            else cached
        )

    day_start = local.normalize()
    next_day_start = day_start + pd.Timedelta(days=1)
    start_offset = standard_offset_at(day_start, timezone_name)
    end_offset = standard_offset_at(next_day_start, timezone_name)
    if start_offset == end_offset:
        cache[key] = start_offset
        return start_offset
    # None marks the rare day on which the jurisdiction changed standard
    # offset. Resolve each boundary separately so the transition is detected.
    cache[key] = None
    return standard_offset_at(local, timezone_name)


def local_standard_interval_to_utc(
    local_start: pd.Timestamp,
    local_end: pd.Timestamp,
    timezone_name: str,
    cache: OffsetCache,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Place a local-standard-time interval on the UTC timeline.

    Raises :class:`StandardTimeTransitionError` when the station's standard
    offset changes inside the interval, because the interval would then have a
    different length in UTC than the publisher's local slot says it has.
    """

    start = (
        local_start - standard_offset(local_start, timezone_name, cache)
    ).tz_localize("UTC")
    end = (local_end - standard_offset(local_end, timezone_name, cache)).tz_localize(
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


def timezone_overrides(spec: DatasetSpec) -> dict[str, str]:
    """Evidence-backed Climate ID timezones declared in the registry."""

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


def station_timezone_map(
    paths: StorePaths, registry: Registry, station_dataset_id: str
) -> tuple[dict[str, str], str]:
    """Climate ID to IANA timezone from the latest committed station inventory.

    Returns the mapping and the inventory snapshot it was read from, so a
    producer can record which inventory placed its timestamps.
    """

    registry.get(station_dataset_id).require_ready()
    try:
        snapshot_id, fragments = Catalog(paths).committed_fragments(station_dataset_id)
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
    mapping = dict(
        zip(
            available["entity_id"].astype(str),
            available["timezone_name"].astype(str),
            strict=True,
        )
    )
    return mapping, snapshot_id


def resolve_station_timezones(
    paths: StorePaths, registry: Registry, spec: DatasetSpec
) -> tuple[dict[str, str], str]:
    """The station inventory's timezones plus the registry's overrides.

    An override may only fill a Climate ID the inventory cannot place; one that
    disagrees with an inventoried station stops ingestion.
    """

    station_dataset_id = str(spec.ingest_options.get("station_dataset_id") or "")
    if not station_dataset_id:
        raise ValueError("station_dataset_id is required by timezone policy")
    timezone_by_entity, snapshot_id = station_timezone_map(
        paths, registry, station_dataset_id
    )
    for entity, timezone_name in timezone_overrides(spec).items():
        inventoried = timezone_by_entity.get(entity)
        if inventoried is not None and inventoried != timezone_name:
            raise ValueError(
                f"Timezone override for {entity!r} conflicts with the station "
                f"inventory: {timezone_name!r} != {inventoried!r}"
            )
        timezone_by_entity[entity] = timezone_name
    return timezone_by_entity, snapshot_id
