"""ECCC hourly climate observations from the MSC GeoMet OGC API: offline ingestion.

Acquisition and ingestion are separate steps. Downloading belongs to
:mod:`research_store.acquisition.geomet_climate_hourly`, the only module in the
store allowed to open a socket (``tests/test_architecture.py`` enforces that).
This module never touches the network. It owns what both steps share, the
declared query and the selection-manifest format, and it ingests:

* :func:`read_manifest` proves a manifest asks exactly the registry's declared
  query: one Climate ID and one local-standard-time calendar year per window,
  a ``resulttype=hits`` count and its ``f=csv`` pages.
* :func:`ingest` archives the manifest and every response into ``raw/``,
  reconciles each window's rows with its published count, and publishes one
  replacement snapshot. Because members are resolved from ``raw/`` when the
  download cache is gone, ``research-store reingest`` and a restore replay the
  exact bytes that were first ingested.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import tempfile
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import pandas as pd

from research_store.foundation.catalog import Catalog
from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.conventions import apply_sentinel
from research_store.foundation.hashing import sha256_file
from research_store.foundation.models import (
    DatasetSpec,
    Registry,
    SentinelRule,
    StorageModel,
    TemporalKind,
)
from research_store.foundation.paths import StorePaths
from research_store.foundation.pipeline import RejectedRecord
from research_store.foundation.station_time import (
    OffsetCache,
    StandardTimeTransitionError,
    local_standard_interval_to_utc,
    resolve_station_timezones,
)
from research_store.foundation.writer import SourceAsset, StoreWriter

VERSION = "1"

CLIMATE_ID = re.compile(r"^[0-9A-Z]{7}$")
ROLE_HITS = "hits"
ROLE_PAGE = "page"
MANIFEST_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
RETRIEVED_AT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# Input roles recorded for a run. The selection manifest is written by this
# store's own `fetch`, so it is not a publisher-supplied manifest.
INPUT_SELECTION_MANIFEST = "selection_manifest"
INPUT_COMPLETENESS = "completeness_evidence"
INPUT_OBSERVATIONS = "observation_source"
# How the replacement guard was applied to a run, recorded with the run.
GUARD_CHECKED = "checked"
GUARD_ALLOWED = "allowed_by_operator"
GUARD_REBUILD = "rebuild_of_committed_run"
GUARD_REPLAY = "replay_of_published_selection"
_NUMBER = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_INTEGER = re.compile(r"^[+-]?\d+$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def atomic_write(path: Path, data: bytes) -> None:
    """Write a whole file or nothing; shared with the acquisition cache."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".incoming-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


# ----------------------------------------------------------------------
# Declared query and windows
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ApiOptions:
    collection_url: str
    items_url: str
    format: str
    limit: int
    sortby: str
    window: str
    settle_days: int


def api_options(spec: DatasetSpec) -> ApiOptions:
    """The registry's declared query; unsupported declarations are refused."""

    raw = spec.ingest_options.get("api")
    if not isinstance(raw, Mapping):
        raise ValueError(f"{spec.dataset_id} declares no GeoMet api options")
    options = ApiOptions(
        collection_url=str(raw["collection_url"]),
        items_url=str(raw["items_url"]),
        format=str(raw["format"]),
        limit=int(raw["limit"]),
        sortby=str(raw["sortby"]),
        window=str(raw["window"]),
        settle_days=int(raw["settle_days"]),
    )
    # These are declarations of the one query this producer implements, in
    # the same way `timezone_policy` is: the request URLs, and so every
    # archived response, depend on them, and a different value is refused
    # rather than half-honoured.
    if options.format != "csv":
        raise ValueError(f"Unsupported GeoMet response format: {options.format!r}")
    if options.window != "local_calendar_year":
        raise ValueError(f"Unsupported GeoMet request window: {options.window!r}")
    if options.sortby != "LOCAL_DATE":
        raise ValueError("GeoMet pages must be sorted by LOCAL_DATE to be reproducible")
    if not 1 <= options.limit <= 10_000:
        raise ValueError("GeoMet limit must lie in [1, 10000], the server's maximum")
    if options.settle_days < 1:
        raise ValueError("settle_days must be at least one day")
    return options


@dataclass(frozen=True, slots=True, order=True)
class Window:
    """One Climate ID over a half-open local-standard-time range of LOCAL_DATE."""

    climate_id: str
    start: datetime
    end: datetime

    @property
    def request_end(self) -> datetime:
        """The inclusive end the API's ``datetime`` filter is given.

        The filter includes both ends, so one second before `end` makes the
        request half-open. Stopping at the last whole hour instead would lose
        the last record of every day window at a station whose LOCAL_DATE
        falls on the half hour (Newfoundland, UTC-3:30, reports 23:30).
        """

        return self.end - timedelta(seconds=1)

    @property
    def label(self) -> str:
        return f"{self.start:%Y%m%dT%H}-{self.end:%Y%m%dT%H}"


def _as_hour(value: date | datetime | str, name: str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise ValueError(f"{name} is local standard time and must be naive")
        result = value
    elif isinstance(value, date):
        result = datetime(value.year, value.month, value.day)
    else:
        raise TypeError(f"{name} must be a date or datetime")
    if result.minute or result.second or result.microsecond:
        raise ValueError(f"{name} must fall on a whole hour: {result}")
    return result


def windows(
    climate_ids: Iterable[str],
    ranges: Sequence[tuple[date | datetime | str, date | datetime | str]],
) -> list[Window]:
    """Split each LST range ``[start, end)`` into calendar years, per station."""

    identifiers = list(dict.fromkeys(climate_ids))
    if not identifiers:
        raise ValueError("At least one Climate ID is required")
    for identifier in identifiers:
        if not CLIMATE_ID.fullmatch(identifier):
            raise ValueError(f"Not an ECCC Climate ID: {identifier!r}")
    spans = sorted(
        (_as_hour(start, "start"), _as_hour(end, "end")) for start, end in ranges
    )
    if not spans:
        raise ValueError("At least one start/end range is required")
    for start, end in spans:
        if end <= start:
            raise ValueError(f"end must be later than start: {start} >= {end}")
    for (_, previous_end), (next_start, _) in zip(spans, spans[1:], strict=False):
        if next_start < previous_end:
            raise ValueError("Requested ranges overlap")
    result: list[Window] = []
    for identifier in identifiers:
        for start, end in spans:
            cursor = start
            while cursor < end:
                year_end = datetime(cursor.year + 1, 1, 1)
                stop = min(year_end, end)
                result.append(Window(identifier, cursor, stop))
                cursor = stop
    return sorted(result)


def _datetime_parameter(window: Window) -> str:
    return (
        f"{window.start:{MANIFEST_TIME_FORMAT}}/"
        f"{window.request_end:{MANIFEST_TIME_FORMAT}}"
    )


def page_url(api: ApiOptions, window: Window, offset: int) -> str:
    """The exact URL of one CSV page; the manifest must repeat it verbatim."""

    url = (
        f"{api.items_url}?f={api.format}&CLIMATE_IDENTIFIER={window.climate_id}"
        f"&datetime={_datetime_parameter(window)}&sortby={api.sortby}"
        f"&limit={api.limit}"
    )
    if offset:
        url += f"&offset={offset}"
    return url


def hits_url(api: ApiOptions, window: Window) -> str:
    """The count request that evidences how many rows a window must hold."""

    return (
        f"{api.items_url}?f=json&CLIMATE_IDENTIFIER={window.climate_id}"
        f"&datetime={_datetime_parameter(window)}&resulttype=hits"
    )


def parse_retrieved_at(value: str) -> datetime:
    """A manifest or cache retrieval time, as naive UTC."""

    return datetime.strptime(value, RETRIEVED_AT_FORMAT)


def window_settled(api: ApiOptions, window: Window, retrieved_at: datetime) -> bool:
    """Whether a response retrieved then can no longer gain hours.

    The collection publishes an hour a day or more after it ends and can still
    fill gaps later, so a window counts as settled only `settle_days` after its
    LST end. That margin also absorbs the few hours between LST and the UTC
    retrieval time.
    """

    return retrieved_at >= window.end + timedelta(days=api.settle_days)


# ----------------------------------------------------------------------
# Response bodies
# ----------------------------------------------------------------------


def _source_columns(spec: DatasetSpec) -> tuple[str, ...]:
    columns = tuple(str(item) for item in spec.ingest_options.get("source_columns", ()))
    if not columns or len(columns) != len(set(columns)):
        raise ValueError("source_columns must be a non-empty list of unique names")
    return columns


def _lines(body: bytes, encoding: str) -> list[str]:
    """Physical CRLF lines of a CSV page, without their terminators."""

    text = body.decode(encoding)
    if not text.endswith("\r\n"):
        raise ValueError("GeoMet CSV page does not end with CRLF; transfer truncated?")
    lines = text.split("\r\n")
    lines.pop()
    return lines


def _check_header(line: str, columns: Sequence[str]) -> None:
    header = next(csv.reader([line]))
    if tuple(header) != tuple(columns):
        raise ValueError(
            "GeoMet climate-hourly CSV header differs from the declared "
            f"{len(columns)} source columns: {header}"
        )


def page_row_count(body: bytes, spec: DatasetSpec) -> int:
    """Data rows in a page after checking its header and terminators."""

    lines = _lines(body, str(spec.ingest_options.get("encoding", "utf-8")))
    if not lines:
        raise ValueError("GeoMet CSV page has no header")
    _check_header(lines[0], _source_columns(spec))
    return len(lines) - 1


def hits_count(body: bytes) -> int:
    """`numberMatched` of a ``resulttype=hits`` response, checked for shape."""

    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("type") != "FeatureCollection":
        raise ValueError("GeoMet hits response is not a FeatureCollection")
    if payload.get("features") != []:
        raise ValueError("GeoMet hits response unexpectedly carries features")
    matched = payload.get("numberMatched")
    if isinstance(matched, bool) or not isinstance(matched, int) or matched < 0:
        raise ValueError(f"GeoMet hits response has no valid numberMatched: {matched!r}")
    return matched


# ----------------------------------------------------------------------
# Selection manifest
# ----------------------------------------------------------------------


def _manifest_sort_key(row: Mapping[str, str]) -> tuple[Any, ...]:
    offset = row["page_offset"]
    return (
        row["climate_id"],
        row["window_start_lst"],
        row["role"],
        int(offset) if offset else -1,
    )


def write_manifest(root: Path, spec: DatasetSpec, rows: list[dict[str, str]]) -> Path:
    """Write a selection manifest named so that name order is retrieval order.

    `reingest` replays a replacement dataset's manifests by name, so the one
    fetched last is replayed last and its snapshot ends up live.
    """

    columns = [str(item) for item in spec.ingest_options["manifest_columns"]]
    delimiter = str(spec.ingest_options["manifest_delimiter"])
    lines = [delimiter.join(columns)]
    for row in sorted(rows, key=_manifest_sort_key):
        values = [row[name] for name in columns]
        if any(delimiter in value or "\n" in value for value in values):
            raise ValueError(f"Manifest value contains a delimiter: {values}")
        lines.append(delimiter.join(values))
    body = ("\n".join(lines) + "\n").encode("utf-8")
    newest = max(row["retrieved_at"] for row in rows)
    stamp = parse_retrieved_at(newest).strftime("%Y%m%dT%H%M%SZ")
    path = root / f"{stamp}_{hashlib.sha256(body).hexdigest()[:12]}.tsv"
    if not path.is_file():
        atomic_write(path, body)
    return path


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    climate_id: str
    window: Window
    role: str
    page_offset: int | None
    filename: str
    size_bytes: int
    sha256: str
    request_url: str
    retrieved_at: str
    http_date: str
    content_type: str
    server: str
    number_matched: int

    def fingerprint_row(self) -> dict[str, Any]:
        return {
            "climate_id": self.climate_id,
            "window_start_lst": f"{self.window.start:{MANIFEST_TIME_FORMAT}}",
            "window_end_lst": f"{self.window.end:{MANIFEST_TIME_FORMAT}}",
            "role": self.role,
            "page_offset": self.page_offset,
            "sha256": self.sha256,
            "number_matched": self.number_matched,
        }


@dataclass(frozen=True, slots=True)
class ManifestWindow:
    window: Window
    hits: ManifestEntry
    pages: tuple[ManifestEntry, ...]

    @property
    def number_matched(self) -> int:
        return self.hits.number_matched


@dataclass(frozen=True, slots=True)
class Selection:
    windows: tuple[ManifestWindow, ...]

    @property
    def entries(self) -> list[ManifestEntry]:
        result: list[ManifestEntry] = []
        for item in self.windows:
            result.append(item.hits)
            result.extend(item.pages)
        return result

    @property
    def climate_ids(self) -> list[str]:
        return sorted({item.window.climate_id for item in self.windows})

    @property
    def fingerprint(self) -> str:
        payload = [entry.fingerprint_row() for entry in self.entries]
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def read_manifest(path: str | Path, spec: DatasetSpec) -> Selection:
    """Parse a selection manifest and prove it asks exactly the declared query."""

    path = Path(path)
    api = api_options(spec)
    columns = tuple(str(item) for item in spec.ingest_options["manifest_columns"])
    delimiter = str(spec.ingest_options["manifest_delimiter"])
    text = path.read_bytes().decode("utf-8")
    if not text.endswith("\n"):
        raise ValueError(f"Selection manifest is truncated: {path}")
    lines = text[:-1].split("\n")
    if tuple(lines[0].split(delimiter)) != columns:
        raise ValueError(f"Unexpected GeoMet selection manifest columns: {lines[0]!r}")
    entries: list[ManifestEntry] = []
    filenames: set[str] = set()
    for number, line in enumerate(lines[1:], start=2):
        values = line.split(delimiter)
        if len(values) != len(columns):
            raise ValueError(f"Manifest line {number} has {len(values)} fields")
        row = dict(zip(columns, values, strict=True))
        climate_id = row["climate_id"]
        if not CLIMATE_ID.fullmatch(climate_id):
            raise ValueError(f"Manifest line {number} has Climate ID {climate_id!r}")
        window = Window(
            climate_id,
            datetime.strptime(row["window_start_lst"], MANIFEST_TIME_FORMAT),
            datetime.strptime(row["window_end_lst"], MANIFEST_TIME_FORMAT),
        )
        if window.end <= window.start or window.start.year != window.request_end.year:
            raise ValueError(
                f"Manifest line {number} window is not within one LST calendar year"
            )
        role = row["role"]
        if role == ROLE_HITS:
            if row["page_offset"] != "":
                raise ValueError(f"Manifest line {number}: a count has no page offset")
            offset = None
            expected_url = hits_url(api, window)
        elif role == ROLE_PAGE:
            if not row["page_offset"].isdigit():
                raise ValueError(f"Manifest line {number} has no page offset")
            offset = int(row["page_offset"])
            expected_url = page_url(api, window, offset)
        else:
            raise ValueError(f"Manifest line {number} has unknown role {role!r}")
        if row["request_url"] != expected_url:
            raise ValueError(
                f"Manifest line {number} request URL does not match the declared "
                f"query: {row['request_url']!r} != {expected_url!r}"
            )
        filename = row["filename"]
        pure = PurePosixPath(filename)
        if pure.is_absolute() or ".." in pure.parts or not filename:
            raise ValueError(f"Manifest line {number} has unsafe filename {filename!r}")
        if filename in filenames:
            raise ValueError(f"Manifest repeats filename {filename!r}")
        filenames.add(filename)
        if not _SHA256.fullmatch(row["sha256"]):
            raise ValueError(f"Manifest line {number} has no SHA-256")
        if not row["size_bytes"].isdigit() or not row["number_matched"].isdigit():
            raise ValueError(f"Manifest line {number} has a non-integer count")
        parse_retrieved_at(row["retrieved_at"])
        entries.append(
            ManifestEntry(
                climate_id=climate_id,
                window=window,
                role=role,
                page_offset=offset,
                filename=filename,
                size_bytes=int(row["size_bytes"]),
                sha256=row["sha256"],
                request_url=row["request_url"],
                retrieved_at=row["retrieved_at"],
                http_date=row["http_date"],
                content_type=row["content_type"],
                server=row["server"],
                number_matched=int(row["number_matched"]),
            )
        )
    grouped: dict[Window, list[ManifestEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.window, []).append(entry)
    result: list[ManifestWindow] = []
    for window in sorted(grouped):
        members = grouped[window]
        counts = [entry for entry in members if entry.role == ROLE_HITS]
        if len(counts) != 1:
            raise ValueError(f"Window {window.climate_id} {window.label} needs one count")
        matched = counts[0].number_matched
        if any(entry.number_matched != matched for entry in members):
            raise ValueError(f"Window {window.climate_id} {window.label} disagrees on its count")
        pages = sorted(
            (entry for entry in members if entry.role == ROLE_PAGE),
            key=lambda entry: entry.page_offset or 0,
        )
        if [entry.page_offset for entry in pages] != list(range(0, matched, api.limit)):
            raise ValueError(
                f"Window {window.climate_id} {window.label} does not page its "
                f"{matched} records at limit {api.limit}"
            )
        result.append(ManifestWindow(window, counts[0], tuple(pages)))
    if not result:
        raise ValueError("The GeoMet selection manifest is empty")
    for previous, following in zip(result, result[1:], strict=False):
        if (
            previous.window.climate_id == following.window.climate_id
            and following.window.start < previous.window.end
        ):
            raise ValueError(
                f"Manifest windows overlap for {following.window.climate_id}: "
                f"{previous.window.label} and {following.window.label}"
            )
    return Selection(tuple(result))


# ----------------------------------------------------------------------
# Parsing one page
# ----------------------------------------------------------------------


_SLOT_LABELS = {"slot_start", "slot_end"}


def _duration_choices(value: Any, name: str) -> tuple[int, ...]:
    choices = tuple(value) if isinstance(value, (list, tuple)) else (value,)
    if not choices or any(
        isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in choices
    ):
        raise ValueError(f"variable_timing for {name!r} needs non-negative minutes")
    return choices


def _check_variable_timing(spec: DatasetSpec) -> None:
    """Every variable declares where it sits inside the row's one-hour slot.

    A wide row has one key, so these placements do not move rows; they are the
    declaration protocol section 1 asks for, which a reader needs to tell a
    slot total from an instant observed at the slot's end.
    """

    timing = spec.ingest_options.get("variable_timing")
    if not isinstance(timing, Mapping) or set(timing) != set(spec.variable_names):
        raise ValueError("variable_timing must place exactly the declared variables")
    for name, raw in timing.items():
        if not isinstance(raw, Mapping):
            raise TypeError(f"variable_timing for {name!r} must be a mapping")
        positions = {"start_minute", "end_minute"} & set(raw)
        if len(positions) != 1:
            raise ValueError(f"variable_timing for {name!r} needs one position")
        position = positions.pop()
        minute = raw[position]
        if isinstance(minute, bool) or not isinstance(minute, int) or not 0 <= minute <= 60:
            raise ValueError(f"variable_timing for {name!r} lies outside the slot")
        longest = max(_duration_choices(raw.get("duration_minutes"), name))
        if (position == "start_minute" and minute + longest > 60) or (
            position == "end_minute" and minute - longest < 0
        ):
            raise ValueError(f"variable_timing for {name!r} exceeds its slot")


def _sentinel_scope(spec: DatasetSpec) -> dict[str, tuple[SentinelRule, ...]]:
    """The sentinel rules that apply to each variable's value field.

    A marker is evidence about particular fields: 'NA' is observed only in the
    present-weather text, and '0' means calm only in wind direction. Applying
    either to every field would turn a numeric 'NA' into a silent null.
    """

    scope = spec.ingest_options.get("sentinel_variables")
    if not isinstance(scope, Mapping):
        raise ValueError("sentinel_variables must map every marker to its variables")
    markers = {rule.marker for rule in spec.sentinel_rules}
    if set(scope) != markers:
        raise ValueError("sentinel_variables must scope exactly the declared markers")
    result: dict[str, list[SentinelRule]] = {name: [] for name in spec.variable_names}
    for marker, names in scope.items():
        names = list(names)
        if not names or not set(names) <= set(spec.variable_names):
            raise ValueError(f"Sentinel {marker!r} is scoped to undeclared variables")
        for name in names:
            result[name].extend(rule for rule in spec.sentinel_rules if rule.marker == marker)
    return {name: tuple(rules) for name, rules in result.items()}


@dataclass(frozen=True, slots=True)
class _ParseConfig:
    columns: tuple[str, ...]
    index: Mapping[str, int]
    encoding: str
    entity_column: str
    local_column: str
    local_format: str
    local_labels: str
    local_components: tuple[int, ...]
    utc_column: str
    utc_format: str
    utc_components: tuple[int, ...]
    record_id: int
    record_id_format: str
    page_constants: tuple[tuple[str, int], ...]
    coordinate_pairs: tuple[tuple[str, int, str, int], ...]
    numeric: tuple[tuple[str, int, float], ...]
    text: tuple[tuple[str, int], ...]
    quality: tuple[tuple[str, int], ...]
    annotations: tuple[tuple[str, int], ...]
    rules: Mapping[str, tuple[SentinelRule, ...]]
    markers: Mapping[str, Mapping[str, float | None] | None]
    integer_ranges: Mapping[str, tuple[int, int]]
    missing_timezone_policy: str
    transition_policy: str
    utc_mismatch_policy: str

    @classmethod
    def from_spec(cls, spec: DatasetSpec) -> _ParseConfig:
        if spec.storage_model is not StorageModel.WIDE:
            raise ValueError("GeoMet climate-hourly requires wide storage")
        if spec.temporal_kind is not TemporalKind.INTERVAL:
            raise ValueError("GeoMet climate-hourly rows are LST hour slots")
        options = spec.ingest_options
        if options.get("timezone_policy") != "station_inventory":
            raise ValueError("GeoMet climate-hourly needs the station_inventory policy")
        columns = _source_columns(spec)
        index = {name: position for position, name in enumerate(columns)}

        def column(name: Any, purpose: str) -> int:
            if name not in index:
                raise ValueError(f"{purpose} names undeclared source column {name!r}")
            return index[name]

        def columns_of(key: str, count: int | None = None) -> tuple[int, ...]:
            names = list(options.get(key) or ())
            if not names or (count is not None and len(names) != count):
                raise ValueError(f"{key} must name {count or 'some'} source columns")
            return tuple(column(name, key) for name in names)

        column_map = dict(options.get("column_map") or {})
        scales = dict(options.get("scales") or {})
        numeric: list[tuple[str, int, float]] = []
        text: list[tuple[str, int]] = []
        quality: list[tuple[str, int]] = []
        for variable in spec.variables:
            position = column(column_map.get(variable.name), variable.name)
            if variable.dtype == "float64":
                if variable.name not in scales:
                    raise ValueError(f"No publisher scale declared for {variable.name!r}")
                numeric.append((variable.name, position, float(scales[variable.name])))
            elif variable.dtype == "string":
                if variable.name in scales:
                    raise ValueError(f"Text variable {variable.name!r} cannot be scaled")
                text.append((variable.name, position))
            else:
                raise ValueError(f"Unsupported variable dtype {variable.dtype!r}")
            if variable.quality_field:
                quality.append(
                    (
                        variable.quality_field,
                        column(
                            column_map.get(variable.quality_field),
                            variable.quality_field,
                        ),
                    )
                )
        mapped = set(spec.variable_names) | {name for name, _ in quality}
        if set(column_map) != mapped or set(scales) - set(spec.variable_names):
            raise ValueError("column_map and scales must name exactly the declared fields")
        annotation_map = dict(options.get("annotation_map") or {})
        if set(annotation_map) != {item.name for item in spec.annotations}:
            raise ValueError("annotation_map must name exactly the declared annotations")
        annotations = tuple(
            (item.name, column(annotation_map[item.name], item.name))
            for item in spec.annotations
        )
        policies = {
            key: str(options.get(key, "error"))
            for key in (
                "missing_timezone_policy",
                "standard_time_transition_policy",
                "publisher_utc_mismatch_policy",
            )
        }
        for key, value in policies.items():
            if value not in {"error", "quarantine_record"}:
                raise ValueError(f"Unsupported {key}: {value!r}")
        _check_variable_timing(spec)
        local_labels = str(options.get("local_time_labels"))
        if local_labels not in _SLOT_LABELS:
            raise ValueError(f"local_time_labels must be one of {sorted(_SLOT_LABELS)}")
        rules = _sentinel_scope(spec)
        markers = {
            name: (
                {rule.marker: rule.replacement for rule in scoped}
                if all(rule.start is None and rule.end is None for rule in scoped)
                else None
            )
            for name, scoped in rules.items()
        }
        integer_ranges: dict[str, tuple[int, int]] = {}
        numeric_names = {name for name, *_ in numeric}
        for name, bounds in dict(options.get("source_integer_ranges") or {}).items():
            low, high = (int(item) for item in bounds)
            if name not in numeric_names or low >= high:
                raise ValueError(f"source_integer_ranges for {name!r} is not usable")
            integer_ranges[name] = (low, high)
        record_id_format = str(options["record_id_format"])
        record_id_format.format(entity="", year=0, month=0, day=0, hour=0)
        coordinate_pairs = tuple(
            (
                name,
                column(name, "coordinate_columns"),
                other,
                column(other, "coordinate_columns"),
            )
            for name, other in dict(options.get("coordinate_columns") or {}).items()
        )
        page_constants = tuple(
            (name, column(name, "page_constant_columns"))
            for name in options.get("page_constant_columns") or ()
        )
        for name in (
            options["entity_column"],
            options["local_time_column"],
            options["publisher_utc_column"],
        ):
            column(name, "a key column")
        return cls(
            columns=columns,
            index=index,
            encoding=str(options.get("encoding", "utf-8")),
            entity_column=str(options["entity_column"]),
            local_column=str(options["local_time_column"]),
            local_format=str(options["local_time_format"]),
            local_labels=local_labels,
            local_components=columns_of("local_component_columns", 4),
            utc_column=str(options["publisher_utc_column"]),
            utc_format=str(options["publisher_utc_format"]),
            utc_components=columns_of("utc_component_columns", 3),
            record_id=column(options.get("record_id_column"), "record_id_column"),
            record_id_format=record_id_format,
            page_constants=page_constants,
            coordinate_pairs=coordinate_pairs,
            numeric=tuple(numeric),
            text=tuple(text),
            quality=tuple(quality),
            annotations=annotations,
            rules=rules,
            markers=markers,
            integer_ranges=integer_ranges,
            missing_timezone_policy=policies["missing_timezone_policy"],
            transition_policy=policies["standard_time_transition_policy"],
            utc_mismatch_policy=policies["publisher_utc_mismatch_policy"],
        )

    def marker(
        self, name: str, raw: str, observed_at: pd.Timestamp
    ) -> tuple[bool, float | None]:
        """(is_marker, replacement) for one raw value field of one variable."""

        markers = self.markers[name]
        if markers is not None:
            if raw in markers:
                return True, markers[raw]
            return False, None
        scoped = self.rules[name]
        if any(rule.marker == raw for rule in scoped):
            return True, apply_sentinel(raw, observed_at, scoped)
        return False, None


@dataclass(slots=True)
class PageResult:
    """One page parsed into canonical rows plus the rows it quarantined."""

    frame: pd.DataFrame
    rejections: list[RejectedRecord]
    physical_rows: int
    local_dates: list[datetime]
    details: dict[str, Any] = field(default_factory=dict)


def _page_rejection(
    *, key: str, number: int, raw: bytes, reason: str, details: dict[str, Any]
) -> RejectedRecord:
    return RejectedRecord(
        rejection_key=f"page={key}:line={number}:{reason}",
        record_locator=f"line:{number}",
        reason=reason,
        raw_sha256=hashlib.sha256(raw).hexdigest(),
        raw_length=len(raw),
        details=details,
    )


def _same_coordinate(left: str, right: str) -> bool:
    try:
        return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=1e-9)
    except ValueError:
        return False


def parse_page(
    body: bytes,
    spec: DatasetSpec,
    *,
    climate_id: str,
    window: Window,
    timezone_by_entity: Mapping[str, str],
    key: str,
    offset_cache: OffsetCache | None = None,
) -> PageResult:
    """Parse one CSV page into canonical wide rows, failing closed on drift.

    Structural surprises raise: a changed header, a short row, a record for
    another station or outside the requested window, a repeated hour, date
    components or ID that disagree with LOCAL_DATE, station metadata that is
    not constant over the page, an unparseable number, or a value outside its
    publisher's declared integer domain. Records whose placement on the UTC
    timeline cannot be defended are quarantined with a line locator instead.
    """

    config = _ParseConfig.from_spec(spec)
    cache: OffsetCache = offset_cache if offset_cache is not None else {}
    lines = _lines(body, config.encoding)
    if not lines:
        raise ValueError("GeoMet CSV page has no header")
    _check_header(lines[0], config.columns)
    index = config.index
    width = len(config.columns)
    one_hour = pd.Timedelta(hours=1)
    labels_end = config.local_labels == "slot_end"

    entities: list[str] = []
    starts: list[pd.Timestamp] = []
    ends: list[pd.Timestamp] = []
    numeric_values: dict[str, list[float | None]] = {name: [] for name, *_ in config.numeric}
    text_values: dict[str, list[str | None]] = {name: [] for name, _ in config.text}
    quality_values: dict[str, list[str]] = {name: [] for name, _ in config.quality}
    annotation_values: dict[str, list[str]] = {name: [] for name, _ in config.annotations}
    rejections: list[RejectedRecord] = []
    local_dates: list[datetime] = []
    seen: set[datetime] = set()
    constants: dict[str, set[str]] = {name: set() for name, _ in config.page_constants}
    constant_positions = {position for _, position in config.page_constants}
    nonblank_flags: dict[str, Counter[str]] = {}
    nonblank_annotations: dict[str, Counter[str]] = {}
    reasons: Counter[str] = Counter()

    for number, line in enumerate(lines[1:], start=2):
        fields = next(csv.reader([line]))
        if len(fields) != width:
            raise ValueError(f"Line {number} has {len(fields)} fields, expected {width}")
        entity = fields[index[config.entity_column]]
        if entity != climate_id:
            raise ValueError(
                f"Line {number} is Climate ID {entity!r}, but the page was "
                f"requested for {climate_id!r}"
            )
        raw_local = fields[index[config.local_column]]
        local = datetime.strptime(raw_local, config.local_format)
        # LOCAL_DATE is on the hour at most stations and on the half hour at
        # UTC-3:30 (Newfoundland) ones; the publisher UTC cross-check below
        # decides whether the station's offset explains it.
        if local.second or local.microsecond:
            raise ValueError(f"Line {number} LOCAL_DATE has seconds: {raw_local}")
        if not window.start <= local < window.end:
            raise ValueError(
                f"Line {number} LOCAL_DATE {raw_local} is outside the requested "
                f"window {window.label}; the API's datetime semantics may have changed"
            )
        if local in seen:
            raise ValueError(f"Line {number} repeats LOCAL_DATE {raw_local}")
        seen.add(local)
        expected_components = tuple(
            str(value) for value in (local.year, local.month, local.day, local.hour)
        )
        expected_id = config.record_id_format.format(
            entity=entity, year=local.year, month=local.month, day=local.day, hour=local.hour
        )
        if (
            tuple(fields[position] for position in config.local_components)
            != expected_components
            or fields[config.record_id] != expected_id
        ):
            raise ValueError(f"Line {number} LOCAL_* or ID disagrees with LOCAL_DATE")
        raw_utc = fields[index[config.utc_column]]
        publisher_utc = datetime.strptime(raw_utc, config.utc_format)
        if tuple(fields[position] for position in config.utc_components) != tuple(
            str(value)
            for value in (publisher_utc.year, publisher_utc.month, publisher_utc.day)
        ):
            raise ValueError(f"Line {number} UTC_* components disagree with UTC_DATE")
        for name, position in config.page_constants:
            constants[name].add(fields[position])
        for name, position, other, other_position in config.coordinate_pairs:
            if not _same_coordinate(fields[position], fields[other_position]):
                raise ValueError(f"Line {number} {name} disagrees with {other}")
        local_dates.append(local)

        timezone_name = timezone_by_entity.get(entity)
        details: dict[str, Any] = {
            "entity_id": entity,
            "local_date": raw_local,
            "publisher_utc": raw_utc,
            "derived_utc": None,
            "timezone_name": timezone_name,
        }

        problem: tuple[str, str, str] | None = None
        start = end = None
        if timezone_name is None:
            problem = (
                "missing_station_timezone",
                config.missing_timezone_policy,
                f"no station timezone is available for Climate ID {entity!r}",
            )
        else:
            labelled = pd.Timestamp(local)
            local_start, local_end = (
                (labelled - one_hour, labelled) if labels_end else (labelled, labelled + one_hour)
            )
            try:
                start, end = local_standard_interval_to_utc(
                    local_start, local_end, timezone_name, cache
                )
            except StandardTimeTransitionError as error:
                problem = (
                    "standard_timezone_transition",
                    config.transition_policy,
                    str(error),
                )
            else:
                # UTC_DATE is the observation time, the instant LOCAL_DATE labels.
                observed = end if labels_end else start
                details["derived_utc"] = observed.strftime(RETRIEVED_AT_FORMAT)
                if pd.Timestamp(publisher_utc, tz="UTC") != observed:
                    problem = (
                        "publisher_utc_mismatch",
                        config.utc_mismatch_policy,
                        f"UTC_DATE {raw_utc} disagrees with the station-inventory "
                        f"conversion {details['derived_utc']}",
                    )
        if problem is not None:
            reason, policy, message = problem
            if policy != "quarantine_record":
                raise ValueError(f"Line {number}: {message}")
            rejections.append(
                _page_rejection(
                    key=key,
                    number=number,
                    raw=line.encode(config.encoding),
                    reason=reason,
                    details=dict(details, error=message),
                )
            )
            reasons[reason] += 1
            continue

        for name, position, scale in config.numeric:
            raw = fields[position]
            is_marker, replacement = config.marker(name, raw, start)
            if is_marker:
                value = replacement
            else:
                if not _NUMBER.fullmatch(raw):
                    raise ValueError(f"Line {number} {name} is not a number: {raw!r}")
                value = float(raw)
                if not math.isfinite(value):
                    raise ValueError(f"Line {number} {name} is not finite: {raw!r}")
                bounds = config.integer_ranges.get(name)
                if bounds is not None and not (
                    _INTEGER.fullmatch(raw) and bounds[0] <= int(raw) <= bounds[1]
                ):
                    raise ValueError(
                        f"Line {number} {name} {raw!r} is outside the publisher's "
                        f"declared domain, integers {bounds[0]}-{bounds[1]}; its "
                        "encoding may have changed"
                    )
            numeric_values[name].append(None if value is None else value * scale)
        for name, position in config.text:
            raw = fields[position]
            is_marker, replacement = config.marker(name, raw, start)
            if is_marker and replacement is not None:
                raise ValueError(f"Sentinel {raw!r} cannot give text {name!r} a number")
            text_values[name].append(None if is_marker else raw)
        for name, position in config.quality:
            flag = fields[position]
            quality_values[name].append(flag)
            if flag:
                nonblank_flags.setdefault(name, Counter())[flag] += 1
        for name, position in config.annotations:
            value = fields[position]
            annotation_values[name].append(value)
            if value and position not in constant_positions:
                nonblank_annotations.setdefault(name, Counter())[value] += 1
        entities.append(entity)
        starts.append(start)
        ends.append(end)

    varying = {
        name: sorted(values) for name, values in sorted(constants.items()) if len(values) > 1
    }
    if varying:
        raise ValueError(
            f"Page for {climate_id} mixes values of station metadata that must be "
            f"constant: {varying}"
        )
    columns: dict[str, Any] = {
        spec.entity_field: pd.Series(entities, dtype="string"),
        spec.time_start_field: pd.Series(starts, dtype="datetime64[ns, UTC]"),
        spec.time_end_field: pd.Series(ends, dtype="datetime64[ns, UTC]"),
    }
    quality_for = {
        variable.name: variable.quality_field for variable in spec.variables
    }
    for variable in spec.variables:
        if variable.name in numeric_values:
            columns[variable.name] = pd.Series(
                numeric_values[variable.name], dtype="float64"
            )
        else:
            columns[variable.name] = pd.Series(text_values[variable.name], dtype="string")
        quality_name = quality_for[variable.name]
        if quality_name:
            columns[quality_name] = pd.Series(quality_values[quality_name], dtype="string")
    for name, values in annotation_values.items():
        columns[name] = pd.Series(values, dtype="string")
    frame = pd.DataFrame(columns)
    physical = len(lines) - 1
    if len(frame) + len(rejections) != physical:
        raise AssertionError("page reconciliation failed")
    return PageResult(
        frame=frame,
        rejections=rejections,
        physical_rows=physical,
        local_dates=local_dates,
        details={
            "rows": physical,
            "rows_published": len(frame),
            "rows_quarantined": len(rejections),
            "quarantined_by_reason": dict(sorted(reasons.items())),
            "page_constants": {
                name: next(iter(values)) for name, values in sorted(constants.items()) if values
            },
            "first_local_date": min(local_dates).strftime(MANIFEST_TIME_FORMAT)
            if local_dates
            else None,
            "last_local_date": max(local_dates).strftime(MANIFEST_TIME_FORMAT)
            if local_dates
            else None,
            "nonblank_flags": {
                name: dict(sorted(counter.items()))
                for name, counter in sorted(nonblank_flags.items())
            },
            "nonblank_annotations": {
                name: dict(sorted(counter.items()))
                for name, counter in sorted(nonblank_annotations.items())
            },
        },
    )


# ----------------------------------------------------------------------
# Offline ingestion
# ----------------------------------------------------------------------


def _resolve_members(
    selection: Selection, manifest_path: Path, paths: StorePaths
) -> dict[str, Path]:
    """Locate every response beside the manifest, else in the raw archive."""

    root = manifest_path.parent.resolve()
    resolved: dict[str, Path] = {}
    for entry in selection.entries:
        candidate = (root / entry.filename).resolve()
        if root not in candidate.parents:
            raise ValueError(f"Manifest member escapes its directory: {entry.filename}")
        if not candidate.is_file():
            candidate = paths.raw_object(entry.sha256)
        if not candidate.is_file():
            raise FileNotFoundError(
                f"GeoMet response {entry.filename} is neither beside the manifest "
                "nor in the raw archive"
            )
        size = candidate.stat().st_size
        digest = sha256_file(candidate)
        if size != entry.size_bytes or digest != entry.sha256:
            raise ValueError(
                f"GeoMet response {entry.filename} does not match its manifest "
                f"SHA-256/size ({digest}, {size} bytes)"
            )
        resolved[entry.filename] = candidate
    return resolved


def _verify_counts(
    selection: Selection, members: Mapping[str, Path], spec: DatasetSpec
) -> None:
    """Every window's pages must hold exactly its published count."""

    for item in selection.windows:
        matched = hits_count(members[item.hits.filename].read_bytes())
        if matched != item.number_matched:
            raise ValueError(
                f"Count response for {item.window.climate_id} {item.window.label} "
                f"says {matched}, the manifest {item.number_matched}"
            )
        rows = sum(
            page_row_count(members[page.filename].read_bytes(), spec)
            for page in item.pages
        )
        if rows != matched:
            raise ValueError(
                f"Pages for {item.window.climate_id} {item.window.label} hold {rows} "
                f"rows but numberMatched is {matched}; refusing a partial window"
            )


def _merged_spans(spans: Iterable[tuple[datetime, datetime]]) -> list[list[datetime]]:
    merged: list[list[datetime]] = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def _refuse_shrinking_selection(
    catalog: Catalog, spec: DatasetSpec, selection: Selection
) -> None:
    """A replacement must not silently drop hours that readers can see.

    The published snapshot's coverage is the LOCAL_DATE range each of its
    windows actually held, not the range that was requested: a selection that
    starts at a station's first record, or ends after its last one, drops
    nothing, and a window that held no records imposes nothing.
    """

    _, previous = catalog.latest_snapshot_inputs(spec.dataset_id, INPUT_COMPLETENESS)
    covered: dict[str, list[tuple[datetime, datetime]]] = {}
    for item in selection.windows:
        covered.setdefault(item.window.climate_id, []).append(
            (item.window.start, item.window.end)
        )
    merged = {key: _merged_spans(value) for key, value in covered.items()}
    dropped: list[str] = []
    for _, _, details in previous:
        climate_id = str(details.get("climate_id"))
        if "first_local_date" in details:
            if details["first_local_date"] is None:
                continue
            first = datetime.strptime(details["first_local_date"], MANIFEST_TIME_FORMAT)
            last = datetime.strptime(details["last_local_date"], MANIFEST_TIME_FORMAT)
        else:
            first = datetime.strptime(details["window_start_lst"], MANIFEST_TIME_FORMAT)
            last = datetime.strptime(details["window_end_lst"], MANIFEST_TIME_FORMAT)
            last -= timedelta(seconds=1)
        spans = merged.get(climate_id, [])
        if not any(low <= first and last < high for low, high in spans):
            dropped.append(f"{climate_id} {first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M}")
    if dropped:
        raise ValueError(
            "This selection would replace the published snapshot without hours "
            f"it shows in {len(dropped)} window(s), e.g. {dropped[:3]}. Fetch the "
            "full range, or pass --allow-selection-shrink to publish a smaller "
            "replacement deliberately."
        )


def ingest(
    dataset_id: str,
    source_path: str | Path,
    *,
    registry: Registry,
    paths: StorePaths,
    source_uri: str | None = None,
    publisher_vintage: str | None = None,
    fetched_at: str | None = None,
    allow_selection_shrink: bool = False,
    replay: bool = False,
) -> str:
    """Publish one replacement snapshot from a GeoMet selection manifest.

    `replay` is set by `research-store reingest`, which re-presents every
    archived manifest oldest first. A manifest that was published before, under
    any ingester version, is then not measured against the newer snapshot it
    precedes; one that was refused is checked, and refused, again.
    """

    spec = registry.get(dataset_id)
    spec.require_ready()
    api = api_options(spec)
    _ParseConfig.from_spec(spec)
    manifest_path = Path(source_path).expanduser().resolve(strict=True)
    selection = read_manifest(manifest_path, spec)
    members = _resolve_members(selection, manifest_path, paths)
    _verify_counts(selection, members, spec)
    timezone_by_entity, inventory_snapshot = resolve_station_timezones(
        paths, registry, spec
    )
    entries = selection.entries
    newest = max(entry.retrieved_at for entry in entries)

    with StoreWriter(paths, registry) as writer:
        manifest_asset = writer.archive_source(
            manifest_path,
            source_uri=source_uri or api.collection_url,
            publisher_vintage=publisher_vintage,
            fetched_at=fetched_at or newest,
        )
        assets: dict[str, SourceAsset] = {}
        for entry in entries:
            assets[entry.filename] = writer.archive_source(
                members[entry.filename],
                source_uri=entry.request_url,
                publisher_vintage=publisher_vintage,
                fetched_at=entry.retrieved_at,
                original_name=PurePosixPath(entry.filename).name,
            )
        begun = writer.begin(
            spec,
            manifest_asset,
            ingester_version=f"{VERSION}+selection.{selection.fingerprint}",
        )
        rebuilding = begun.state == "committed"
        run = writer.resume_or_rebuild(begun)
        if run.state == "committed":
            return run.snapshot_id
        try:
            if rebuilding:
                guard = GUARD_REBUILD
            elif allow_selection_shrink:
                guard = GUARD_ALLOWED
            elif replay and writer.catalog.source_was_published(
                spec.dataset_id, manifest_asset.source_id
            ):
                guard = GUARD_REPLAY
            else:
                guard = GUARD_CHECKED
                _refuse_shrinking_selection(writer.catalog, spec, selection)
            manifest_details: dict[str, Any] = {
                "stations": selection.climate_ids,
                "lst_start": min(item.window.start for item in selection.windows).strftime(
                    MANIFEST_TIME_FORMAT
                ),
                "lst_end": max(item.window.end for item in selection.windows).strftime(
                    MANIFEST_TIME_FORMAT
                ),
                "windows": len(selection.windows),
                "pages": sum(len(item.pages) for item in selection.windows),
                "number_matched_total": sum(
                    item.number_matched for item in selection.windows
                ),
                "timezone_inventory_snapshot_id": inventory_snapshot,
                "allow_selection_shrink": allow_selection_shrink,
                "selection_guard": guard,
            }
            writer.catalog.record_ingestion_input(
                run_id=run.run_id,
                input_key=INPUT_SELECTION_MANIFEST,
                source_id=manifest_asset.source_id,
                input_role=INPUT_SELECTION_MANIFEST,
                details=manifest_details,
            )
            completed = writer.catalog.completed_chunk_keys(run.run_id)
            offset_cache: OffsetCache = {}
            published = 0
            open_windows = 0
            quarantined: Counter[str] = Counter()
            for item in selection.windows:
                window_dates: set[datetime] = set()
                window_rows = 0
                for page in item.pages:
                    asset = assets[page.filename]
                    result = parse_page(
                        asset.raw_path.read_bytes(),
                        spec,
                        climate_id=item.window.climate_id,
                        window=item.window,
                        timezone_by_entity=timezone_by_entity,
                        key=asset.sha256[:20],
                        offset_cache=offset_cache,
                    )
                    repeated = window_dates.intersection(result.local_dates)
                    if repeated:
                        raise ValueError(
                            f"Pages of {item.window.climate_id} {item.window.label} "
                            f"repeat {len(repeated)} hour(s), e.g. {min(repeated)}; "
                            "the data changed between page requests, fetch again"
                        )
                    window_dates.update(result.local_dates)
                    window_rows += result.physical_rows
                    if result.rejections:
                        writer.record_rejections(
                            run=run,
                            spec=spec,
                            source=asset,
                            rejections=result.rejections,
                        )
                    for chunk in chunks_from_frame(
                        result.frame,
                        spec,
                        key_prefix=f"page={asset.sha256[:20]}",
                        completed=completed,
                    ):
                        writer.write_chunk(
                            run=run,
                            spec=spec,
                            source=asset,
                            chunk_key=chunk.chunk_key,
                            table=chunk.table,
                            partition=chunk.partition,
                        )
                    published += len(result.frame)
                    quarantined.update(
                        rejection.reason for rejection in result.rejections
                    )
                    writer.catalog.record_ingestion_input(
                        run_id=run.run_id,
                        input_key=f"page:{page.filename}",
                        source_id=asset.source_id,
                        input_role=INPUT_OBSERVATIONS,
                        details={
                            "climate_id": item.window.climate_id,
                            "window_start_lst": item.window.start.strftime(
                                MANIFEST_TIME_FORMAT
                            ),
                            "window_end_lst": item.window.end.strftime(
                                MANIFEST_TIME_FORMAT
                            ),
                            "page_offset": page.page_offset,
                            "request_url": page.request_url,
                            "retrieved_at": page.retrieved_at,
                            **result.details,
                        },
                    )
                if window_rows != item.number_matched:
                    raise AssertionError("window reconciliation failed after parsing")
                settled = window_settled(
                    api, item.window, parse_retrieved_at(item.hits.retrieved_at)
                )
                open_windows += not settled
                writer.catalog.record_ingestion_input(
                    run_id=run.run_id,
                    input_key=f"hits:{item.hits.filename}",
                    source_id=assets[item.hits.filename].source_id,
                    input_role=INPUT_COMPLETENESS,
                    details={
                        "climate_id": item.window.climate_id,
                        "window_start_lst": item.window.start.strftime(
                            MANIFEST_TIME_FORMAT
                        ),
                        "window_end_lst": item.window.end.strftime(MANIFEST_TIME_FORMAT),
                        "number_matched": item.number_matched,
                        "request_url": item.hits.request_url,
                        "retrieved_at": item.hits.retrieved_at,
                        # The hours readers can see, which a later replacement
                        # must keep; and whether the window could still grow.
                        "first_local_date": min(window_dates).strftime(
                            MANIFEST_TIME_FORMAT
                        )
                        if window_dates
                        else None,
                        "last_local_date": max(window_dates).strftime(
                            MANIFEST_TIME_FORMAT
                        )
                        if window_dates
                        else None,
                        "settled_at_retrieval": settled,
                    },
                )
            manifest_details.update(
                rows_published=published,
                rows_quarantined=sum(quarantined.values()),
                quarantined_by_reason=dict(sorted(quarantined.items())),
                open_windows=open_windows,
            )
            writer.catalog.record_ingestion_input(
                run_id=run.run_id,
                input_key=INPUT_SELECTION_MANIFEST,
                source_id=manifest_asset.source_id,
                input_role=INPUT_SELECTION_MANIFEST,
                details=manifest_details,
            )
            return writer.publish(run=run, spec=spec, source=manifest_asset)
        except BaseException as error:
            writer.catalog.mark_run_failed(run.run_id, repr(error))
            raise
