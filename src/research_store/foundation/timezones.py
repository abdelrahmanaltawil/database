from __future__ import annotations

import struct
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import cache
from importlib.metadata import version
from importlib.resources import files
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@cache
def pinned_zoneinfo(name: str) -> ZoneInfo:
    """Load an IANA zone from the project's pinned tzdata distribution."""

    resource = files("tzdata.zoneinfo")
    for component in name.split("/"):
        resource = resource.joinpath(component)
    if not resource.is_file():
        raise ZoneInfoNotFoundError(name)
    with resource.open("rb") as stream:
        return ZoneInfo.from_file(stream, key=name)


def timezone_data_version() -> str:
    return version("tzdata")


@dataclass(frozen=True, slots=True)
class StandardOffsetHistory:
    """IANA non-DST UTC offsets indexed by local-standard transition time."""

    initial: timedelta
    transition_times: tuple[datetime, ...]
    transition_offsets: tuple[timedelta, ...]
    transition_dates: frozenset[date]

    def offset_at(self, local_standard_time: datetime) -> timedelta:
        position = bisect_right(self.transition_times, local_standard_time)
        if position == 0:
            return self.initial
        return self.transition_offsets[position - 1]


def _read_tzif_types(
    stream,
) -> tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...], tuple[int, ...]]:
    """Read the transition and type fields defined by RFC 8536."""

    def header() -> tuple[int, tuple[int, int, int, int, int, int]]:
        if stream.read(4) != b"TZif":
            raise ValueError("Invalid TZif file: magic not found")
        raw_version = stream.read(1)
        tzif_version = 1 if raw_version == b"\x00" else int(raw_version)
        stream.read(15)
        counts = struct.unpack(">6l", stream.read(24))
        return tzif_version, counts

    tzif_version, counts = header()
    isutcnt, isstdcnt, leapcnt, timecnt, typecnt, charcnt = counts
    time_size = 4
    if tzif_version >= 2:
        first_block_size = (
            timecnt * 5
            + typecnt * 6
            + charcnt
            + leapcnt * 8
            + isstdcnt
            + isutcnt
        )
        stream.seek(first_block_size, 1)
        _, counts = header()
        isutcnt, isstdcnt, leapcnt, timecnt, typecnt, charcnt = counts
        time_size = 8

    transition_format = "q" if time_size == 8 else "l"
    transition_times = (
        struct.unpack(
            f">{timecnt}{transition_format}", stream.read(timecnt * time_size)
        )
        if timecnt
        else ()
    )
    transition_types = (
        struct.unpack(f">{timecnt}B", stream.read(timecnt)) if timecnt else ()
    )
    records = [struct.unpack(">lbb", stream.read(6)) for _ in range(typecnt)]
    offsets = tuple(record[0] for record in records)
    daylight = tuple(record[1] for record in records)
    return transition_times, transition_types, offsets, daylight


@cache
def standard_offset_history(name: str) -> StandardOffsetHistory:
    """Load explicit standard offsets without relying on ``datetime.dst()``.

    Some valid IANA histories, including ``America/Inuvik``, cause Python's
    inferred ``dst()`` value to be two hours even though the civil DST shift is
    one hour. TZif records whether each transition type is standard or DST, so
    those publisher-independent flags are the reliable basis for ECCC local
    standard time.
    """

    resource = files("tzdata.zoneinfo")
    for component in name.split("/"):
        resource = resource.joinpath(component)
    if not resource.is_file():
        raise ZoneInfoNotFoundError(name)
    with resource.open("rb") as stream:
        transition_utc, transition_types, offsets, daylight = _read_tzif_types(
            stream
        )

    standard_types = [index for index, flag in enumerate(daylight) if flag == 0]
    if not standard_types:
        raise ValueError(f"Timezone {name!r} declares no standard-time offset")
    initial_seconds = offsets[standard_types[0]]
    current_seconds = initial_seconds
    local_transitions: list[datetime] = []
    standard_offsets: list[timedelta] = []
    epoch = datetime(1970, 1, 1)
    for utc_seconds, type_index in zip(
        transition_utc, transition_types, strict=True
    ):
        if daylight[type_index] or offsets[type_index] == current_seconds:
            continue
        current_seconds = offsets[type_index]
        local_transition = epoch + timedelta(
            seconds=utc_seconds + current_seconds
        )
        local_transitions.append(local_transition)
        standard_offsets.append(timedelta(seconds=current_seconds))

    return StandardOffsetHistory(
        initial=timedelta(seconds=initial_seconds),
        transition_times=tuple(local_transitions),
        transition_offsets=tuple(standard_offsets),
        transition_dates=frozenset(item.date() for item in local_transitions),
    )


def standard_utc_offset(name: str, local_standard_time: datetime) -> timedelta:
    """Return the IANA standard UTC offset applicable to a naive local time."""

    if local_standard_time.tzinfo is not None:
        raise ValueError("local_standard_time must be timezone-naive")
    return standard_offset_history(name).offset_at(local_standard_time)
