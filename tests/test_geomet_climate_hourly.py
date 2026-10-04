"""MSC GeoMet climate-hourly: acquisition, parsing and replacement ingestion.

No test opens a socket: every response comes from a fake opener or from the
fixtures in ``tests/fixtures/geomet_climate_hourly``. Both fixtures keep the
recorded byte layout of a publisher response with synthetic content, because
raw publisher responses are never committed (docs/ingestion-protocol.md,
section 2): ``hits_response.json`` is a ``resulttype=hits`` count response
(the layout of one retrieved from api.weather.gc.ca on 2026-09-25, with a
synthetic time stamp), and ``page_response.csv`` an ``f=csv`` page (header,
CRLF, coordinate and ID formatting, blank nulls, ``NA``/``ND`` weather).
"""

from __future__ import annotations

import email.message
import hashlib
import json
import shutil
import urllib.error
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import Workbook

from research_store import load
from research_store.acquisition import geomet_climate_hourly as geomet_acquisition
from research_store.cli import main
from research_store.foundation.catalog import Catalog
from research_store.foundation.models import Registry
from research_store.foundation.paths import StorePaths
from research_store.foundation.registry import DEFAULT_REGISTRY
from research_store.foundation.writer import _HELD_LOCKS
from research_store.ingestion import (
    fixed_width_hourly,
    geomet_climate_hourly,
    inventory_csv,
)

DATASET = "eccc_climate_hourly_observations"
FIXTURES = Path(__file__).parent / "fixtures" / "geomet_climate_hourly"
SPEC = DEFAULT_REGISTRY.get(DATASET)
COLUMNS = tuple(SPEC.ingest_options["source_columns"])
TORONTO = {"6153301": "America/Toronto"}
JULY = geomet_climate_hourly.Window("6153301", datetime(2018, 7, 1), datetime(2018, 8, 1))


@pytest.fixture(autouse=True)
def _no_network(monkeypatch) -> None:
    def refuse(*_args, **_kwargs):
        raise AssertionError("a test tried to reach the network")

    monkeypatch.setattr(geomet_acquisition.urllib.request, "urlopen", refuse)


# ----------------------------------------------------------------------
# Synthetic publisher responses
# ----------------------------------------------------------------------


def _row(
    local: str,
    *,
    climate_id: str = "6153301",
    offset_hours: int = 5,
    utc: str | None = None,
    **values: str,
) -> dict[str, str]:
    """One record; UTC_DATE is the observation time, LOCAL_DATE plus the offset."""

    moment = datetime.strptime(local, "%Y-%m-%d %H:%M:%S")
    publisher_utc = (
        datetime.strptime(utc, "%Y-%m-%dT%H:%M:%S")
        if utc
        else moment + timedelta(hours=offset_hours)
    )
    row = {name: "" for name in COLUMNS}
    row.update(
        x="-79.90833333333333",
        y="43.291666666666664",
        STATION_NAME="SYNTHETIC STATION",
        CLIMATE_IDENTIFIER=climate_id,
        ID=f"{climate_id}.{moment.year}.{moment.month}.{moment.day}.{moment.hour}",
        LOCAL_DATE=local,
        PROVINCE_CODE="ON",
        LOCAL_YEAR=str(moment.year),
        LOCAL_MONTH=str(moment.month),
        LOCAL_DAY=str(moment.day),
        LOCAL_HOUR=str(moment.hour),
        UTC_DATE=publisher_utc.strftime("%Y-%m-%dT%H:%M:%S"),
        UTC_YEAR=str(publisher_utc.year),
        UTC_MONTH=str(publisher_utc.month),
        UTC_DAY=str(publisher_utc.day),
        TEMP="20.5",
        DEW_POINT_TEMP="15.0",
        PRECIP_AMOUNT="0",
        RELATIVE_HUMIDITY="70",
        STATION_PRESSURE="100.5",
        WEATHER_ENG_DESC="NA",
        WEATHER_FRE_DESC="ND",
        WIND_DIRECTION="26",
        WIND_SPEED="8",
        STN_ID="27529",
        LONGITUDE_DECIMAL_DEGREES="-79.90833333333333",
        LATITUDE_DECIMAL_DEGREES="43.291666666666664",
    )
    row.update(values)
    return row


def _hours(start: str, count: int, **values: str) -> list[dict[str, str]]:
    first = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    return [
        _row((first + timedelta(hours=hour)).strftime("%Y-%m-%d %H:%M:%S"), **values)
        for hour in range(count)
    ]


def _page_csv(rows: list[dict[str, str]]) -> bytes:
    lines = [",".join(COLUMNS)]
    for row in rows:
        lines.append(
            ",".join(
                f'"{row[name]}"' if "," in row[name] else row[name] for name in COLUMNS
            )
        )
    return ("\r\n".join(lines) + "\r\n").encode("utf-8")


def _hits(
    count: int, stamp: str = "2026-09-25T06:37:02.554680Z", url: str | None = None
) -> bytes:
    """The count fixture, re-pointed at another request."""

    payload = json.loads((FIXTURES / "hits_response.json").read_text())
    payload["numberMatched"] = count
    payload["timeStamp"] = stamp
    if url is not None:
        for link in payload["links"]:
            if link["rel"] == "self":
                link["href"] = url
    return json.dumps(payload).encode()


@dataclass
class _Reply:
    body: bytes
    content_type: str
    status: int = 200
    content_length: int | None = None

    @property
    def headers(self) -> dict[str, str]:
        length = len(self.body) if self.content_length is None else self.content_length
        return {
            "Content-Type": self.content_type,
            "Content-Length": str(length),
            "Date": "Fri, 25 Sep 2026 06:37:00 GMT",
            "X-Powered-By": "pygeoapi 0.20.0",
        }

    def read(self) -> bytes:
        return self.body


class _Opened:
    """What the opener hands back: a context manager that marks the request done."""

    def __init__(self, server: _Server, reply: _Reply):
        self._server = server
        self.status = reply.status
        self.headers = reply.headers
        self._body = reply.body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self._server.active -= 1
        return False


def _csv_reply(body: bytes, **kwargs) -> _Reply:
    return _Reply(body, "text/csv; charset=utf-8", **kwargs)


def _json_reply(body: bytes) -> _Reply:
    return _Reply(body, "application/json")


def _http_error(url: str, code: int, retry_after: str | None = None):
    headers = email.message.Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return urllib.error.HTTPError(url, code, "synthetic", headers, None)


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class _Server:
    """A fake opener: URL -> queue of replies; the last reply repeats."""

    def __init__(self, clock: _Clock, replies: dict[str, list]):
        self.clock = clock
        self.replies = {url: list(items) for url, items in replies.items()}
        self.calls: list[tuple[str, float]] = []
        self.active = 0

    def __call__(self, request, timeout):
        assert timeout == geomet_acquisition.TIMEOUT_SECONDS
        assert request.get_header("User-agent").startswith("research-store/")
        assert request.get_header("Accept-encoding") is None
        assert self.active == 0, "requests must be sequential"
        url = request.full_url
        self.calls.append((url, self.clock.now))
        queue = self.replies[url]
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        self.clock.now += 0.25  # transfer time
        if isinstance(item, Exception):
            raise item
        self.active += 1
        return _Opened(self, item)


def _serve(
    clock: _Clock,
    pages: dict[geomet_climate_hourly.Window, list[bytes]],
    *,
    spec=SPEC,
    stamp: str = "2026-09-25T06:37:02.554680Z",
) -> _Server:
    api = geomet_climate_hourly.api_options(spec)
    replies: dict[str, list] = {}
    for window, bodies in pages.items():
        count = sum(body.count(b"\r\n") - 1 for body in bodies)
        count_url = geomet_climate_hourly.hits_url(api, window)
        replies[count_url] = [_json_reply(_hits(count, stamp, count_url))]
        for index, body in enumerate(bodies):
            url = geomet_climate_hourly.page_url(api, window, index * api.limit)
            replies[url] = [_csv_reply(body)]
    return _Server(clock, replies)


def _fetch(
    tmp_path: Path,
    server: _Server,
    clock: _Clock,
    ranges,
    *,
    registry: Registry = DEFAULT_REGISTRY,
    climate_ids=("6153301",),
    refresh: bool = False,
    now: datetime = datetime(2026, 9, 25, 6, 37, 0),
    **options,
) -> geomet_acquisition.FetchResult:
    return geomet_acquisition.fetch_collection(
        DATASET,
        climate_ids=climate_ids,
        ranges=ranges,
        registry=registry,
        downloads=tmp_path / "store" / "downloads" / DATASET,
        refresh=refresh,
        opener=server,
        sleep=clock.sleep,
        clock=clock,
        now=lambda: now.replace(tzinfo=geomet_acquisition.UTC),
        **options,
    )


def _inventory(paths: StorePaths, tmp_path: Path, stations) -> None:
    spec = DEFAULT_REGISTRY.get("eccc_station_inventory")
    headers = list(spec.ingest_options["column_map"].values())
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(headers)
    for climate_id, station_id, latitude, longitude in stations:
        row = {name: None for name in headers}
        row.update(
            {
                "Climate ID": climate_id,
                "Name": f"SYNTHETIC {climate_id}",
                "Province": "ONTARIO",
                "Station ID": station_id,
                "Latitude (Decimal Degrees)": latitude,
                "Longitude (Decimal Degrees)": longitude,
                "Elevation (m)": 100.0,
                "First Year": 2000,
                "Last Year": 2025,
                "HLY First Year": 2000,
                "HLY Last Year": 2025,
            }
        )
        sheet.append([row[name] for name in headers])
    source = tmp_path / "Station Inventory EN.xlsx"
    workbook.save(source)
    inventory_csv.ingest(
        "eccc_station_inventory", source, registry=DEFAULT_REGISTRY, paths=paths
    )


JULY_2018 = (datetime(2018, 7, 1), datetime(2018, 8, 1))
JANUARY_2021 = (datetime(2021, 1, 1), datetime(2021, 2, 1))
JAN = geomet_climate_hourly.Window("6153301", *JANUARY_2021)


def _collection(
    tmp_path: Path,
    windows=(JULY, JAN),
    hours: int = 3,
    *,
    precipitation: list[str] | None = None,
    rows: dict | None = None,
    refresh: bool = False,
    now: datetime = datetime(2026, 9, 25, 6, 37, 0),
):
    """A fetched selection plus the store it will be ingested into.

    Each window holds `hours` synthetic hours from its start, unless `rows`
    gives a window its own records.
    """

    clock = _Clock()
    pages = {}
    for window in windows:
        if rows and window in rows:
            records = rows[window]
        else:
            records = _hours(f"{window.start:%Y-%m-%d %H:%M:%S}", hours)
            for row, amount in zip(records, precipitation or [], strict=False):
                row["PRECIP_AMOUNT"] = amount
        pages[window] = [_page_csv(records)]
    server = _serve(clock, pages)
    result = _fetch(
        tmp_path,
        server,
        clock,
        [(window.start, window.end) for window in windows],
        refresh=refresh,
        now=now,
    )
    paths = StorePaths(tmp_path / "store")
    if not paths.catalog.exists():
        _inventory(paths, tmp_path, [(6153301, 27529, 43.2917, -79.9083)])
    return result.manifest, paths


def _parse(rows, *, window=JULY, climate_id="6153301", timezones=TORONTO, key="t"):
    return geomet_climate_hourly.parse_page(
        _page_csv(rows),
        SPEC,
        climate_id=climate_id,
        window=window,
        timezone_by_entity=timezones,
        key=key,
    )


# ----------------------------------------------------------------------
# Parsing
# ----------------------------------------------------------------------


def test_recorded_response_layout_parses_to_canonical_rows() -> None:
    body = (FIXTURES / "page_response.csv").read_bytes()
    assert geomet_climate_hourly.page_row_count(body, SPEC) == 3
    assert geomet_climate_hourly.hits_count(
        (FIXTURES / "hits_response.json").read_bytes()
    ) == 744
    result = geomet_climate_hourly.parse_page(
        body,
        SPEC,
        climate_id="6153301",
        window=JULY,
        timezone_by_entity=TORONTO,
        key="fixture",
    )
    frame = result.frame
    assert result.physical_rows == 3 and not result.rejections
    # LOCAL_DATE H is the observation time: the row is the hour ending then,
    # and its end is the publisher's UTC_DATE (LOCAL_DATE + 5 h in Ontario).
    assert frame["time_start"].tolist() == [
        pd.Timestamp(f"2018-07-01T0{hour}:00:00Z") for hour in (4, 5, 6)
    ]
    assert frame["time_end"].tolist() == [
        pd.Timestamp(f"2018-07-01T0{hour}:00:00Z") for hour in (5, 6, 7)
    ]
    # Tens of degrees times ten; the calm 0 has no direction.
    assert frame["wind_direction"].tolist()[0::2] == [260.0, 350.0]
    assert pd.isna(frame.loc[1, "wind_direction"])
    assert frame["wind_speed"].tolist() == [8.0, 0.0, 13.0]
    assert pd.isna(frame.loc[1, "precipitation_amount_1h"])
    assert frame.loc[1, "precipitation_amount_1h_quality"] == "M"
    assert frame.loc[0, "precipitation_amount_1h_quality"] == ""
    assert frame.loc[2, "precipitation_amount_1h"] == 0.6
    assert frame["humidex"].isna().tolist() == [True, True, False]
    assert frame["weather_description"].isna().all()
    assert frame["visibility"].isna().all()
    assert frame["source_station_id"].tolist() == ["27529"] * 3
    assert frame["record_flag"].tolist() == [""] * 3
    for variable in SPEC.variables:
        expected = "float64" if variable.dtype == "float64" else "string"
        assert str(frame[variable.name].dtype).startswith(expected), variable.name
    constants = result.details["page_constants"]
    assert constants["STATION_NAME"] == "HAMILTON RBG CS"
    assert constants["STN_ID"] == "27529"
    assert result.details["nonblank_flags"] == {
        "precipitation_amount_1h_quality": {"M": 1}
    }
    assert result.details["first_local_date"] == "2018-07-01T00:00:00"
    assert result.details["last_local_date"] == "2018-07-01T02:00:00"


def test_parser_keeps_text_quoted_commas_and_alphanumeric_ids() -> None:
    window = geomet_climate_hourly.Window("702S006", datetime(2019, 1, 1), datetime(2020, 1, 1))
    rows = [
        _row(
            "2019-01-15 06:00:00",
            climate_id="702S006",
            WEATHER_ENG_DESC="Rain,Fog",
            VISIBILITY="4.8",
            WINDCHILL="-7",
            TEMP="-1.5",
        )
    ]
    frame = _parse(
        rows, window=window, climate_id="702S006", timezones={"702S006": "America/Toronto"}
    ).frame
    assert frame["entity_id"].tolist() == ["702S006"]
    assert frame["weather_description"].tolist() == ["Rain,Fog"]
    assert frame["visibility"].tolist() == [4.8]
    assert frame["wind_chill"].tolist() == [-7.0]
    assert frame["time_start"].tolist() == [pd.Timestamp("2019-01-15T10:00:00Z")]
    assert frame["time_end"].tolist() == [pd.Timestamp("2019-01-15T11:00:00Z")]


def test_markers_apply_only_to_the_fields_they_were_observed_in() -> None:
    frame = _parse(
        [
            _row("2018-07-01 00:00:00", WIND_DIRECTION="0", WIND_SPEED="0"),
            _row("2018-07-01 01:00:00", WIND_DIRECTION="0", WIND_SPEED="2"),
            _row("2018-07-01 02:00:00", WIND_DIRECTION="36", TEMP="", WEATHER_ENG_DESC=""),
        ]
    ).frame
    # Calm: no direction, but a measured zero speed.
    assert frame["wind_direction"].isna().tolist() == [True, True, False]
    assert frame.loc[2, "wind_direction"] == 360.0
    assert frame["wind_speed"].tolist() == [0.0, 2.0, 8.0]
    assert pd.isna(frame.loc[2, "air_temperature"])
    assert frame["weather_description"].isna().all()
    # 'NA' is a missing marker only in the present-weather text.
    for field in ("TEMP", "WIND_SPEED", "STATION_PRESSURE", "PRECIP_AMOUNT"):
        with pytest.raises(ValueError, match="not a number: 'NA'"):
            _parse([_row("2018-07-01 00:00:00", **{field: "NA"})])
    # '0' is a calm marker only in wind direction.
    assert _parse([_row("2018-07-01 00:00:00", TEMP="0")]).frame["air_temperature"].tolist() == [0.0]


def test_a_wind_direction_outside_the_tens_of_degrees_encoding_stops_parsing() -> None:
    for raw in ("37", "260", "26.5", "-1"):
        with pytest.raises(ValueError, match="declared domain, integers 0-36"):
            _parse([_row("2018-07-01 00:00:00", WIND_DIRECTION=raw)])


def test_structural_drift_fails_closed() -> None:
    renamed = _page_csv(_hours("2018-07-01 00:00:00", 1)).replace(
        b",TEMP,", b",TEMPERATURE,", 1
    )
    with pytest.raises(ValueError, match="header differs"):
        geomet_climate_hourly.page_row_count(renamed, SPEC)
    with pytest.raises(ValueError, match="outside the requested window"):
        _parse([_row("2018-08-01 00:00:00")])
    with pytest.raises(ValueError, match="repeats LOCAL_DATE"):
        _parse([_row("2018-07-01 00:00:00"), _row("2018-07-01 00:00:00")])
    with pytest.raises(ValueError, match="requested for"):
        _parse([_row("2018-07-01 00:00:00", climate_id="6153302")])
    with pytest.raises(ValueError, match="LOCAL_\\* or ID"):
        _parse([_row("2018-07-01 00:00:00", ID="6153301.2018.07.01.0")])
    with pytest.raises(ValueError, match="LOCAL_DATE has seconds"):
        _parse([_row("2018-07-01 00:00:30")])
    with pytest.raises(ValueError, match="not a number"):
        _parse([_row("2018-07-01 00:00:00", TEMP="nan")])
    with pytest.raises(ValueError, match="station metadata that must be constant"):
        _parse([_row("2018-07-01 00:00:00"), _row("2018-07-01 01:00:00", STN_ID="1")])
    with pytest.raises(ValueError, match="station metadata that must be constant"):
        _parse(
            [
                _row("2018-07-01 00:00:00"),
                _row("2018-07-01 01:00:00", STATION_NAME="ANOTHER STATION"),
            ]
        )
    with pytest.raises(ValueError, match="x disagrees with LONGITUDE_DECIMAL_DEGREES"):
        _parse([_row("2018-07-01 00:00:00", x="-79.5")])
    truncated = _page_csv(_hours("2018-07-01 00:00:00", 2))[:-2]
    with pytest.raises(ValueError, match="CRLF"):
        geomet_climate_hourly.page_row_count(truncated, SPEC)


def test_unplaceable_records_are_quarantined_with_line_locators() -> None:
    rows = [
        _row("2018-07-01 00:00:00"),
        _row("2018-07-01 01:00:00", utc="2018-07-01T05:00:00"),
    ]
    result = _parse(rows, key="k")
    assert len(result.frame) == 1
    [rejection] = result.rejections
    assert rejection.reason == "publisher_utc_mismatch"
    assert rejection.record_locator == "line:3"
    assert rejection.rejection_key == "page=k:line=3:publisher_utc_mismatch"
    assert rejection.details["derived_utc"] == "2018-07-01T06:00:00Z"
    assert rejection.details["publisher_utc"] == "2018-07-01T05:00:00"

    missing = _parse(rows[:1], timezones={}, key="k")
    assert missing.frame.empty
    assert [item.reason for item in missing.rejections] == ["missing_station_timezone"]


def test_only_the_hour_ending_at_the_yukon_2020_change_is_quarantined() -> None:
    # America/Dawson's standard offset moved from UTC-8 to UTC-7 at 2020-11-01
    # 00:00 local; the hour ending then spans the change.
    window = geomet_climate_hourly.Window("2100184", datetime(2020, 1, 1), datetime(2021, 1, 1))
    rows = [
        _row("2020-10-31 22:00:00", climate_id="2100184", offset_hours=8),
        _row("2020-10-31 23:00:00", climate_id="2100184", offset_hours=8),
        _row("2020-11-01 00:00:00", climate_id="2100184", offset_hours=7),
        _row("2020-11-01 01:00:00", climate_id="2100184", offset_hours=7),
    ]
    result = _parse(
        rows, window=window, climate_id="2100184", timezones={"2100184": "America/Dawson"}
    )
    assert result.frame["time_start"].tolist() == [
        pd.Timestamp("2020-11-01T05:00:00Z"),
        pd.Timestamp("2020-11-01T06:00:00Z"),
        pd.Timestamp("2020-11-01T07:00:00Z"),
    ]
    [rejection] = result.rejections
    assert rejection.reason == "standard_timezone_transition"
    assert rejection.details["local_date"] == "2020-11-01 00:00:00"


def test_a_newfoundland_half_hour_local_date_is_placed_by_its_utc_hour() -> None:
    window = geomet_climate_hourly.Window("8403505", datetime(2021, 7, 1), datetime(2021, 7, 2))
    rows = [
        _row(f"2021-07-01 {hour:02d}:30:00", climate_id="8403505", utc=f"2021-07-01T{hour + 4:02d}:00:00")
        for hour in (0, 19)
    ] + [_row("2021-07-01 23:30:00", climate_id="8403505", utc="2021-07-02T03:00:00")]
    result = _parse(
        rows, window=window, climate_id="8403505", timezones={"8403505": "America/St_Johns"}
    )
    assert not result.rejections
    assert result.frame["time_end"].tolist() == [
        pd.Timestamp("2021-07-01T04:00:00Z"),
        pd.Timestamp("2021-07-01T23:00:00Z"),
        pd.Timestamp("2021-07-02T03:00:00Z"),
    ]
    assert (result.frame["time_end"] - result.frame["time_start"]).eq(pd.Timedelta(hours=1)).all()
    # A half-hour LOCAL_DATE at a whole-hour station is not explained by its
    # offset and is quarantined, not silently placed.
    toronto = _parse([_row("2018-07-01 00:30:00", utc="2018-07-01T05:00:00")])
    assert [item.reason for item in toronto.rejections] == ["publisher_utc_mismatch"]


# ----------------------------------------------------------------------
# Acquisition
# ----------------------------------------------------------------------


def test_fetch_requests_exactly_the_declared_urls_politely(tmp_path: Path) -> None:
    clock = _Clock()
    pages = {
        JULY: [_page_csv(_hours("2018-07-01 00:00:00", 2))],
        JAN: [_page_csv(_hours("2021-01-01 00:00:00", 2))],
    }
    server = _serve(clock, pages)
    result = _fetch(tmp_path, server, clock, [JULY_2018, JANUARY_2021])
    base = "https://api.weather.gc.ca/collections/climate-hourly/items"
    # The datetime filter is inclusive, so a window ends one second early.
    assert [url for url, _ in server.calls] == [
        f"{base}?f=json&CLIMATE_IDENTIFIER=6153301"
        "&datetime=2018-07-01T00:00:00/2018-07-31T23:59:59&resulttype=hits",
        f"{base}?f=csv&CLIMATE_IDENTIFIER=6153301"
        "&datetime=2018-07-01T00:00:00/2018-07-31T23:59:59"
        "&sortby=LOCAL_DATE&limit=10000",
        f"{base}?f=json&CLIMATE_IDENTIFIER=6153301"
        "&datetime=2021-01-01T00:00:00/2021-01-31T23:59:59&resulttype=hits",
        f"{base}?f=csv&CLIMATE_IDENTIFIER=6153301"
        "&datetime=2021-01-01T00:00:00/2021-01-31T23:59:59"
        "&sortby=LOCAL_DATE&limit=10000",
    ]
    starts = [moment for _, moment in server.calls]
    assert all(
        later - earlier >= geomet_acquisition.MIN_REQUEST_INTERVAL_SECONDS
        for earlier, later in zip(starts, starts[1:], strict=False)
    )
    assert result.summary()["http_requests"] == 4
    assert result.summary()["open_windows"] == 0
    assert result.manifest.name.startswith("20260925T063700Z_")
    selection = geomet_climate_hourly.read_manifest(result.manifest, SPEC)
    assert [item.number_matched for item in selection.windows] == [2, 2]
    log = (result.manifest.parent / "fetch-log.jsonl").read_text().splitlines()
    assert len(log) == 4 and all(json.loads(line)["outcome"] == "ok" for line in log)

    cached = _fetch(tmp_path, server, clock, [JULY_2018, JANUARY_2021])
    assert len(server.calls) == 4, "a cached response is not requested again"
    assert cached.manifest == result.manifest


def test_fetch_refuses_to_request_faster_than_the_polite_minimum(tmp_path: Path) -> None:
    clock = _Clock()
    server = _serve(clock, {JULY: [_page_csv(_hours("2018-07-01 00:00:00", 1))]})
    with pytest.raises(ValueError, match="at least 1 s apart"):
        _fetch(tmp_path, server, clock, [JULY_2018], min_interval_seconds=0.0)
    assert server.calls == []


def test_fetch_splits_ranges_into_calendar_years() -> None:
    selected = geomet_climate_hourly.windows(
        ["6153301"], [(datetime(2018, 7, 1), datetime(2020, 3, 1))]
    )
    assert [(item.start, item.end) for item in selected] == [
        (datetime(2018, 7, 1), datetime(2019, 1, 1)),
        (datetime(2019, 1, 1), datetime(2020, 1, 1)),
        (datetime(2020, 1, 1), datetime(2020, 3, 1)),
    ]
    with pytest.raises(ValueError, match="overlap"):
        geomet_climate_hourly.windows(
            ["6153301"],
            [(datetime(2018, 1, 1), datetime(2018, 3, 1)), (datetime(2018, 2, 1), datetime(2018, 4, 1))],
        )
    with pytest.raises(ValueError, match="Climate ID"):
        geomet_climate_hourly.windows(["61533"], [JULY_2018])


def test_fetch_pages_by_offset_beyond_the_limit(tmp_path: Path) -> None:
    api = dict(SPEC.ingest_options["api"], limit=2)
    spec = replace(SPEC, ingest_options=dict(SPEC.ingest_options, api=api))
    registry = Registry([spec])
    rows = _hours("2018-07-01 00:00:00", 5)
    clock = _Clock()
    server = _serve(
        clock,
        {JULY: [_page_csv(rows[0:2]), _page_csv(rows[2:4]), _page_csv(rows[4:5])]},
        spec=spec,
    )
    result = _fetch(tmp_path, server, clock, [JULY_2018], registry=registry)
    page_urls = [url for url, _ in server.calls if "f=csv" in url]
    assert [url.rsplit("limit=2", 1)[1] for url in page_urls] == ["", "&offset=2", "&offset=4"]
    selection = geomet_climate_hourly.read_manifest(result.manifest, spec)
    assert [page.page_offset for page in selection.windows[0].pages] == [0, 2, 4]


def test_fetch_retries_a_busy_server_and_a_truncated_transfer(tmp_path: Path) -> None:
    clock = _Clock()
    server = _serve(clock, {JULY: [_page_csv(_hours("2018-07-01 00:00:00", 2))]})
    api = geomet_climate_hourly.api_options(SPEC)
    count_url = geomet_climate_hourly.hits_url(api, JULY)
    csv_url = geomet_climate_hourly.page_url(api, JULY, 0)
    body = _page_csv(_hours("2018-07-01 00:00:00", 2))
    server.replies[count_url].insert(0, _http_error(count_url, 503, retry_after="7"))
    server.replies[csv_url].insert(0, _csv_reply(body[:-10], content_length=len(body)))
    result = _fetch(tmp_path, server, clock, [JULY_2018])
    assert [url for url, _ in server.calls] == [count_url, count_url, csv_url, csv_url]
    assert 7.0 in clock.sleeps
    assert geomet_acquisition.BACKOFF_SECONDS[0] in clock.sleeps
    outcomes = [
        json.loads(line)["outcome"]
        for line in (result.manifest.parent / "fetch-log.jsonl").read_text().splitlines()
    ]
    assert outcomes == ["http_error", "ok", "rejected", "ok"]


def test_fetch_does_not_retry_a_client_error(tmp_path: Path) -> None:
    clock = _Clock()
    server = _serve(clock, {JULY: [_page_csv(_hours("2018-07-01 00:00:00", 1))]})
    count_url = geomet_climate_hourly.hits_url(geomet_climate_hourly.api_options(SPEC), JULY)
    server.replies[count_url] = [_http_error(count_url, 400)]
    with pytest.raises(RuntimeError, match="HTTP 400"):
        _fetch(tmp_path, server, clock, [JULY_2018])
    assert len(server.calls) == 1


def test_an_unchanged_refresh_reproduces_the_manifest(tmp_path: Path) -> None:
    clock = _Clock()
    pages = {JULY: [_page_csv(_hours("2018-07-01 00:00:00", 3))]}
    first = _fetch(tmp_path, _serve(clock, pages), clock, [JULY_2018])
    later = _serve(clock, pages, stamp="2026-09-26T00:00:00.000000Z")
    second = _fetch(
        tmp_path,
        later,
        clock,
        [JULY_2018],
        refresh=True,
        now=datetime(2026, 9, 26, 0, 0, 0),
    )
    assert len(later.calls) == 2, "a refresh re-requests every response"
    assert second.manifest == first.manifest
    assert second.manifest.read_bytes() == first.manifest.read_bytes()

    changed = {JULY: [_page_csv(_hours("2018-07-01 00:00:00", 4))]}
    third = _fetch(
        tmp_path,
        _serve(clock, changed),
        clock,
        [JULY_2018],
        refresh=True,
        now=datetime(2026, 9, 27, 0, 0, 0),
    )
    assert third.manifest != first.manifest
    assert sorted(path.name for path in third.manifest.parent.glob("20*.tsv")) == sorted(
        [first.manifest.name, third.manifest.name]
    ), "manifest names sort in retrieval order"
    assert first.manifest.name < third.manifest.name


def test_a_window_that_can_still_grow_is_not_reused_from_the_cache(tmp_path: Path) -> None:
    clock = _Clock()
    pages = {JULY: [_page_csv(_hours("2018-07-01 00:00:00", 3))]}
    server = _serve(clock, pages)
    # Retrieved four days into the window: it is open.
    first = _fetch(tmp_path, server, clock, [JULY_2018], now=datetime(2018, 7, 5))
    assert first.summary()["open_windows"] == 1 and len(server.calls) == 2
    # Without --refresh, an open window is asked for again; unchanged, and
    # still open, it keeps its manifest.
    again = _fetch(tmp_path, server, clock, [JULY_2018], now=datetime(2018, 7, 6))
    assert len(server.calls) == 4 and again.manifest == first.manifest
    # Grown: the new hours are fetched.
    server.replies.update(
        _serve(clock, {JULY: [_page_csv(_hours("2018-07-01 00:00:00", 5))]}).replies
    )
    grown = _fetch(tmp_path, server, clock, [JULY_2018], now=datetime(2018, 7, 7))
    assert len(server.calls) == 6 and grown.manifest != first.manifest
    assert geomet_climate_hourly.read_manifest(grown.manifest, SPEC).windows[0].number_matched == 5
    # Unchanged once settled (seven days after its end): recorded again with
    # the retrieval that makes it final, then reused without a request.
    final = _fetch(tmp_path, server, clock, [JULY_2018], now=datetime(2018, 8, 9))
    assert len(server.calls) == 8 and final.summary()["open_windows"] == 0
    reused = _fetch(tmp_path, server, clock, [JULY_2018], now=datetime(2018, 9, 1))
    assert len(server.calls) == 8 and reused.manifest == final.manifest


def test_fetch_refuses_a_count_its_pages_do_not_hold(tmp_path: Path) -> None:
    clock = _Clock()
    server = _serve(clock, {JULY: [_page_csv(_hours("2018-07-01 00:00:00", 2))]})
    count_url = geomet_climate_hourly.hits_url(geomet_climate_hourly.api_options(SPEC), JULY)
    server.replies[count_url] = [_json_reply(_hits(3))]
    with pytest.raises(RuntimeError, match="numberMatched is 3, 2 times"):
        _fetch(tmp_path, server, clock, [JULY_2018])
    assert len(server.calls) == 2 * geomet_acquisition.WINDOW_ATTEMPTS


def test_a_station_the_collection_does_not_serve_is_an_error(tmp_path: Path) -> None:
    clock = _Clock()
    server = _serve(clock, {JULY: []})
    with pytest.raises(ValueError, match="not served by the climate-hourly"):
        _fetch(tmp_path, server, clock, [JULY_2018])


# ----------------------------------------------------------------------
# Ingestion
# ----------------------------------------------------------------------


def _input_details(paths: StorePaths, snapshot: str, role: str) -> list[dict]:
    return [
        json.loads(item["input_details_json"])
        for item in Catalog(paths).provenance(DATASET, snapshot)
        if item["input_role"] == role
    ]


def test_ingest_publishes_one_replacement_snapshot_with_full_provenance(
    tmp_path: Path,
) -> None:
    manifest, paths = _collection(tmp_path)
    snapshot = geomet_climate_hourly.ingest(
        DATASET,
        manifest,
        registry=DEFAULT_REGISTRY,
        paths=paths,
        publisher_vintage="synthetic",
    )
    assert not _HELD_LOCKS, "the GeoMet ingest leaked its write lock"
    frame = load(DATASET, entity="6153301", store=paths.root)
    assert frame.attrs["snapshot_id"] == snapshot
    assert len(frame) == 6
    assert frame["time_start"].iloc[0] == pd.Timestamp("2018-07-01T04:00:00Z")
    assert frame["wind_direction"].unique().tolist() == [260.0]
    assert frame.attrs["units"]["wind_direction"] == "degree_true"

    provenance = Catalog(paths).provenance(DATASET, snapshot)
    roles: dict[str, list[dict]] = {}
    for item in provenance:
        roles.setdefault(item["input_role"], []).append(item)
    assert set(roles) == {"selection_manifest", "observation_source", "completeness_evidence"}
    assert len(roles["observation_source"]) == 2
    assert len(roles["completeness_evidence"]) == 2
    for item in roles["observation_source"]:
        details = json.loads(item["input_details_json"])
        assert item["source_uri"] == details["request_url"]
        assert "f=csv" in item["source_uri"]
        assert pd.Timestamp(item["fetched_at"]) == pd.Timestamp(details["retrieved_at"])
        assert details["rows"] == details["rows_published"] == 3
        assert item["original_name"].startswith("6153301_")
    for item in roles["completeness_evidence"]:
        details = json.loads(item["input_details_json"])
        assert details["settled_at_retrieval"] is True
        assert details["last_local_date"].endswith("T02:00:00")
    [manifest_item] = roles["selection_manifest"]
    details = json.loads(manifest_item["input_details_json"])
    assert details["number_matched_total"] == 6
    assert details["rows_published"] == 6 and details["rows_quarantined"] == 0
    assert details["timezone_inventory_snapshot_id"].startswith("snap_")
    assert details["selection_guard"] == "checked" and details["open_windows"] == 0
    assert manifest_item["source_uri"] == SPEC.ingest_options["api"]["collection_url"]

    again = geomet_climate_hourly.ingest(
        DATASET, manifest, registry=DEFAULT_REGISTRY, paths=paths
    )
    assert again == snapshot, "re-ingesting the same selection is a no-op"


def test_ingest_rebuilds_a_restored_snapshot_from_raw_without_downloads(
    tmp_path: Path, capsys
) -> None:
    manifest, paths = _collection(tmp_path)
    snapshot = geomet_climate_hourly.ingest(
        DATASET, manifest, registry=DEFAULT_REGISTRY, paths=paths
    )
    fragments = Catalog(paths).snapshot_fragment_paths(snapshot)
    for fragment in fragments:
        Path(fragment).unlink()
    shutil.rmtree(paths.downloads)

    assert main(["--store", str(paths.root), "reingest", DATASET]) == 0
    assert snapshot in capsys.readouterr().out
    assert all(Path(fragment).is_file() for fragment in fragments)
    frame = load(DATASET, snapshot=snapshot, store=paths.root)
    assert len(frame) == 6


def test_a_shrinking_replacement_is_refused_unless_allowed(tmp_path: Path) -> None:
    manifest, paths = _collection(tmp_path)
    geomet_climate_hourly.ingest(DATASET, manifest, registry=DEFAULT_REGISTRY, paths=paths)
    smaller, _ = _collection(
        tmp_path,
        windows=(JULY,),
        hours=4,
        refresh=True,
        now=datetime(2026, 9, 26, 6, 37, 0),
    )
    for _attempt in range(2):
        with pytest.raises(ValueError, match="allow-selection-shrink"):
            geomet_climate_hourly.ingest(
                DATASET, smaller, registry=DEFAULT_REGISTRY, paths=paths
            )
    assert not _HELD_LOCKS
    snapshot = geomet_climate_hourly.ingest(
        DATASET,
        smaller,
        registry=DEFAULT_REGISTRY,
        paths=paths,
        allow_selection_shrink=True,
    )
    frame = load(DATASET, snapshot=snapshot, store=paths.root)
    assert len(frame) == 4
    assert frame["time_start"].min() == pd.Timestamp("2018-07-01T04:00:00Z")
    [details] = _input_details(paths, snapshot, "selection_manifest")
    assert details["selection_guard"] == "allowed_by_operator"


def test_the_replacement_guard_compares_the_hours_readers_can_see(tmp_path: Path) -> None:
    year = geomet_climate_hourly.Window("6153301", datetime(2018, 1, 1), datetime(2019, 1, 1))
    # The first selection asks for all of 2018, but the station's records
    # start in July.
    first_rows = {year: _hours("2018-07-01 00:00:00", 3)}
    manifest, paths = _collection(tmp_path, windows=(year,), rows=first_rows)
    geomet_climate_hourly.ingest(DATASET, manifest, registry=DEFAULT_REGISTRY, paths=paths)
    # Starting the next selection at the first record loses nothing.
    later = geomet_climate_hourly.Window("6153301", datetime(2018, 7, 1), datetime(2019, 1, 1))
    narrower, _ = _collection(
        tmp_path,
        windows=(later,),
        rows={later: _hours("2018-07-01 00:00:00", 3, TEMP="21.0")},
        refresh=True,
        now=datetime(2026, 9, 26, 6, 37, 0),
    )
    snapshot = geomet_climate_hourly.ingest(
        DATASET, narrower, registry=DEFAULT_REGISTRY, paths=paths
    )
    assert load(DATASET, snapshot=snapshot, store=paths.root)["air_temperature"].eq(21.0).all()
    # Starting after it would hide an hour readers can see.
    cut = geomet_climate_hourly.Window("6153301", datetime(2018, 7, 1, 1), datetime(2019, 1, 1))
    later_start, _ = _collection(
        tmp_path,
        windows=(cut,),
        rows={cut: _hours("2018-07-01 01:00:00", 2)},
        refresh=True,
        now=datetime(2026, 9, 27, 6, 37, 0),
    )
    with pytest.raises(ValueError, match="6153301 2018-07-01 00:00 to 2018-07-01 02:00"):
        geomet_climate_hourly.ingest(
            DATASET, later_start, registry=DEFAULT_REGISTRY, paths=paths
        )


def test_reingest_replays_published_selections_after_a_version_change(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    july, paths = _collection(tmp_path, windows=(JULY,))
    geomet_climate_hourly.ingest(DATASET, july, registry=DEFAULT_REGISTRY, paths=paths)
    both, _ = _collection(tmp_path, refresh=True, now=datetime(2026, 9, 26, 6, 37, 0))
    geomet_climate_hourly.ingest(DATASET, both, registry=DEFAULT_REGISTRY, paths=paths)
    # A later, smaller selection is refused and never published.
    refused, _ = _collection(
        tmp_path,
        windows=(JULY,),
        hours=4,
        refresh=True,
        now=datetime(2026, 9, 27, 6, 37, 0),
    )
    with pytest.raises(ValueError, match="allow-selection-shrink"):
        geomet_climate_hourly.ingest(DATASET, refused, registry=DEFAULT_REGISTRY, paths=paths)
    capsys.readouterr()

    monkeypatch.setattr(geomet_climate_hourly, "VERSION", "test-bump")
    # Oldest first: the July selection is replayed although the published
    # snapshot also covers January, the July+January one follows, and the
    # refused one is refused again.
    assert main(["--store", str(paths.root), "reingest", DATASET]) == 1
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.startswith("[")]
    assert [": snap_" in line for line in lines] == [True, True, False]
    assert "allow-selection-shrink" in lines[2]
    frame = load(DATASET, store=paths.root)
    assert len(frame) == 6
    [details] = _input_details(paths, frame.attrs["snapshot_id"], "selection_manifest")
    assert details["selection_guard"] == "replay_of_published_selection"


def test_rebuilding_an_older_snapshot_does_not_make_it_live(tmp_path: Path, capsys) -> None:
    older, paths = _collection(tmp_path, windows=(JULY,), hours=2)
    old_snapshot = geomet_climate_hourly.ingest(
        DATASET, older, registry=DEFAULT_REGISTRY, paths=paths
    )
    newer, _ = _collection(tmp_path, refresh=True, now=datetime(2026, 9, 26, 6, 37, 0))
    live = geomet_climate_hourly.ingest(DATASET, newer, registry=DEFAULT_REGISTRY, paths=paths)
    kept = set(Catalog(paths).snapshot_fragment_paths(live))
    for fragment in Catalog(paths).snapshot_fragment_paths(old_snapshot):
        if fragment not in kept:
            Path(fragment).unlink()

    def committed_at() -> dict:
        with Catalog(paths).open(read_only=True) as connection:
            return dict(
                connection.execute(
                    "SELECT snapshot_id, committed_at FROM snapshots WHERE dataset_id = ?",
                    [DATASET],
                ).fetchall()
            )

    before = committed_at()
    assert main(["--store", str(paths.root), "reingest", DATASET]) == 0
    assert old_snapshot in capsys.readouterr().out
    assert committed_at() == before, "a rebuild keeps its original commit time"
    assert len(load(DATASET, snapshot=old_snapshot, store=paths.root)) == 2
    frame = load(DATASET, store=paths.root)
    assert frame.attrs["snapshot_id"] == live and len(frame) == 6


def test_ingest_refuses_inconsistent_selections(tmp_path: Path) -> None:
    manifest, paths = _collection(tmp_path)
    text = manifest.read_text()

    drifted = manifest.with_name("drifted.tsv")
    drifted.write_text(text.replace("limit=10000", "limit=500", 1))
    with pytest.raises(ValueError, match="does not match the declared query"):
        geomet_climate_hourly.ingest(DATASET, drifted, registry=DEFAULT_REGISTRY, paths=paths)

    selection = geomet_climate_hourly.read_manifest(manifest, SPEC)
    page = manifest.parent / selection.windows[0].pages[0].filename
    original = page.read_bytes()
    page.write_bytes(original.replace(b",20.5,", b",20.6,", 1))
    with pytest.raises(ValueError, match="does not match its manifest"):
        geomet_climate_hourly.ingest(DATASET, manifest, registry=DEFAULT_REGISTRY, paths=paths)
    page.write_bytes(original)

    short_root = tmp_path / "short"
    body = _page_csv(_hours("2018-07-01 00:00:00", 2))
    count = _hits(3)
    api = geomet_climate_hourly.api_options(SPEC)
    rows = []
    for role, name, data, url, offset in (
        ("hits", "short/hits.json", count, geomet_climate_hourly.hits_url(api, JULY), ""),
        ("page", "short/page.csv", body, geomet_climate_hourly.page_url(api, JULY, 0), "0"),
    ):
        (short_root / name).parent.mkdir(parents=True, exist_ok=True)
        (short_root / name).write_bytes(data)
        rows.append(
            {
                "climate_id": "6153301",
                "window_start_lst": "2018-07-01T00:00:00",
                "window_end_lst": "2018-08-01T00:00:00",
                "role": role,
                "page_offset": offset,
                "filename": name,
                "size_bytes": str(len(data)),
                "sha256": hashlib.sha256(data).hexdigest(),
                "request_url": url,
                "retrieved_at": "2026-09-25T06:37:00Z",
                "http_date": "",
                "content_type": "",
                "server": "",
                "number_matched": "3",
            }
        )
    short = geomet_climate_hourly.write_manifest(short_root, SPEC, rows)
    with pytest.raises(ValueError, match="hold 2 rows but numberMatched is 3"):
        geomet_climate_hourly.ingest(DATASET, short, registry=DEFAULT_REGISTRY, paths=paths)
    with pytest.raises(LookupError):
        load(DATASET, store=paths.root)


def test_precipitation_is_hly01_element_262_keyed_one_hour_earlier(tmp_path: Path) -> None:
    """The same totals; HLY01 files slot H at [H, H+1), a documented defect."""

    tenths = [hour % 5 for hour in range(24)]
    manifest, paths = _collection(
        tmp_path,
        windows=(JULY,),
        hours=24,
        precipitation=[f"{value / 10:g}" for value in tenths],
    )
    geomet_climate_hourly.ingest(DATASET, manifest, registry=DEFAULT_REGISTRY, paths=paths)
    fields = "".join(f"0{value:05d} " for value in tenths)
    record = f"615330120180701262{fields}"
    assert len(record) == 186
    hly = tmp_path / "HLY01_RCS_P2018"
    hly.write_text(record + "\n", encoding="ascii")
    fixed_width_hourly.ingest(
        "eccc_hly01_observations", hly, registry=DEFAULT_REGISTRY, paths=paths
    )
    api_rows = load(DATASET, variable="precipitation_amount_1h", store=paths.root)
    hly_rows = load(
        "eccc_hly01_observations", variable="precipitation_amount_1h", store=paths.root
    )
    assert len(api_rows) == len(hly_rows) == 24
    assert api_rows["entity_id"].equals(hly_rows["entity_id"])
    for key in ("time_start", "time_end"):
        assert (hly_rows[key] - api_rows[key]).eq(pd.Timedelta(hours=1)).all()
    # HLY01 stores tenths times 0.1 and the API a decimal string, so the two
    # agree to rounding, not bit for bit.
    assert (
        (api_rows["precipitation_amount_1h"] - hly_rows["precipitation_amount_1h"])
        .abs()
        .max()
        < 1e-9
    )
    hly01 = DEFAULT_REGISTRY.get("eccc_hly01_observations").documentation
    assert any("one hour late" in item for item in hly01.limitations)


def test_a_steady_wind_from_one_quadrant_publishes(tmp_path: Path) -> None:
    """A short selection's direction distribution is weather, not a unit error."""

    rows = {
        JULY: [
            _row(
                f"2018-07-01 {hour:02d}:00:00",
                WIND_DIRECTION=str(5 + hour % 4),
                WIND_SPEED="12",
            )
            for hour in range(24)
        ]
    }
    manifest, paths = _collection(tmp_path, windows=(JULY,), rows=rows)
    snapshot = geomet_climate_hourly.ingest(
        DATASET, manifest, registry=DEFAULT_REGISTRY, paths=paths
    )
    frame = load(DATASET, snapshot=snapshot, store=paths.root)
    assert sorted(frame["wind_direction"].unique()) == [50.0, 60.0, 70.0, 80.0]
