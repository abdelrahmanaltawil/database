from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pyarrow as pa
from openpyxl import Workbook

from research_store import load
from research_store.foundation.paths import StorePaths
from research_store.foundation.registry import DEFAULT_REGISTRY
from research_store.ingestion import fixed_width_hourly, inventory_csv


def _hourly_record(
    entity: str,
    date: str,
    element: str,
    values: list[int | None],
    flags: list[str] | None = None,
) -> str:
    assert len(entity) == 7
    assert len(values) == 24
    flags = flags or [""] * 24
    fields: list[str] = []
    for value, flag in zip(values, flags, strict=True):
        raw = "-99999" if value is None else f"0{value:05d}"
        fields.append(raw + (flag or " "))
    result = f"{entity}{date}{element}" + "".join(fields)
    assert len(result) == 186
    return result


def test_eccc_hourly_parser_preserves_ids_flags_sentinels_and_intervals(
    tmp_path: Path,
) -> None:
    values = [27, None, *([0] * 22)]
    flags = ["", "M", *([""] * 22)]
    record = _hourly_record("702S006", "20140801", "262", values, flags)
    source = tmp_path / "HLY01_RCS_P2014"
    # Exercise both a missing final blank flag and blank physical lines.
    source.write_bytes((record[:-1] + "\r\n\r\n").encode("ascii"))

    chunks = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={"702S006": "America/Toronto"},
        )
    )
    table = pa.concat_tables([chunk.table for chunk in chunks])
    frame = table.to_pandas()

    assert len(frame) == 24
    assert frame["entity_id"].unique().tolist() == ["702S006"]
    assert frame["variable"].unique().tolist() == ["precipitation_amount_1h"]
    assert frame["source_element"].unique().tolist() == ["262"]
    assert frame.loc[0, "value"] == 2.7
    assert pd.isna(frame.loc[1, "value"])
    assert frame.loc[0, "quality_flag"] == ""
    assert frame.loc[1, "quality_flag"] == "M"
    assert frame.loc[0, "time_start"] == pd.Timestamp("2014-08-01T05:00:00Z")
    assert frame.loc[0, "time_end"] == pd.Timestamp("2014-08-01T06:00:00Z")


def test_eccc_entity_allowlist_filters_before_publication(tmp_path: Path) -> None:
    first = _hourly_record("702S006", "20140801", "123", [0] * 24)
    second = _hourly_record("1012055", "20140801", "123", [1] * 24)
    source = tmp_path / "HLY03"
    source.write_text(first + "\n" + second + "\n", encoding="ascii")
    base = DEFAULT_REGISTRY.get("eccc_hly03_observations")
    spec = replace(
        base,
        ingest_options={
            **dict(base.ingest_options),
            "entity_allowlist": ["1012055"],
        },
    )

    chunks = list(
        fixed_width_hourly.parse(
            source,
            spec,
            set(),
            timezone_by_entity={"1012055": "America/St_Johns"},
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()
    assert frame["entity_id"].unique().tolist() == ["1012055"]
    assert frame["value"].unique().tolist() == [0.1]


def test_eccc_rcs_elements_have_distinct_variables_scales_and_intervals(
    tmp_path: Path,
) -> None:
    records = [
        _hourly_record("6153193", "20190101", "262", [10] * 24),
        _hourly_record("6153193", "20190101", "263", [20] * 24),
        _hourly_record("6153193", "20190101", "264", [30] * 24),
        _hourly_record("6153193", "20190101", "267", [40] * 24),
        _hourly_record("6153193", "20190101", "279", [180] * 24),
    ]
    source = tmp_path / "HLY01_RCS_P2019"
    source.write_text("\n".join(records) + "\n", encoding="ascii")

    chunks = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={"6153193": "America/Toronto"},
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()

    hourly = frame.loc[frame["source_element"] == "262"].iloc[0]
    first_quarter = frame.loc[frame["source_element"] == "263"].iloc[0]
    second_quarter = frame.loc[frame["source_element"] == "264"].iloc[0]
    gauge = frame.loc[frame["source_element"] == "267"].iloc[0]
    direction = frame.loc[frame["source_element"] == "279"].iloc[0]
    assert hourly["variable"] == "precipitation_amount_1h"
    assert hourly["value"] == 1.0
    assert hourly["time_start"] == pd.Timestamp("2019-01-01T05:00:00Z")
    assert hourly["time_end"] == pd.Timestamp("2019-01-01T06:00:00Z")
    assert first_quarter["variable"] == "precipitation_amount_15min"
    assert first_quarter["value"] == 2.0
    assert first_quarter["time_end"] == pd.Timestamp("2019-01-01T05:15:00Z")
    assert second_quarter["time_start"] == pd.Timestamp("2019-01-01T05:15:00Z")
    assert gauge["variable"] == "precipitation_gauge_weight"
    assert gauge["time_start"] == pd.Timestamp("2019-01-01T05:10:00Z")
    assert gauge["time_end"] == pd.Timestamp("2019-01-01T05:15:00Z")
    assert direction["variable"] == "wind_direction_2m_10min"
    assert direction["value"] == 180.0


def test_eccc_standard_time_policy_ignores_daylight_saving(tmp_path: Path) -> None:
    records = [
        _hourly_record("6153193", "20190115", "262", [0] * 24),
        _hourly_record("6153193", "20190715", "262", [0] * 24),
    ]
    source = tmp_path / "summer-and-winter"
    source.write_text("\n".join(records) + "\n", encoding="ascii")
    chunks = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={"6153193": "America/Toronto"},
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()
    starts = frame.groupby(frame["time_start"].dt.month)["time_start"].min()
    assert starts.loc[1].hour == 5
    assert starts.loc[7].hour == 5


def test_station_inventory_xlsx_with_disclaimers_and_alphanumeric_ids(
    tmp_path: Path,
) -> None:
    spec = DEFAULT_REGISTRY.get("eccc_station_inventory")
    headers = list(spec.ingest_options["column_map"].values())
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["Modified Date: synthetic fixture"])
    sheet.append(["Station inventory disclaimer"])
    sheet.append(["Station ID disclaimer"])
    sheet.append(headers)
    rows = [
        {
            "Climate ID": "10114F6",
            "Name": "SYNTHETIC ALPHA",
            "Province": "ONTARIO",
            "Station ID": 24,
            "WMO ID": None,
            "TC ID": "ABC",
            "Latitude (Decimal Degrees)": 43.25,
            "Longitude (Decimal Degrees)": -79.87,
            "Elevation (m)": 100.5,
            "First Year": 1970,
            "Last Year": 2008,
            "HLY First Year": 1971,
            "HLY Last Year": 2007,
            "DLY First Year": 1970,
            "DLY Last Year": 2008,
            "MLY First Year": 1970,
            "MLY Last Year": 2008,
        },
        {
            "Climate ID": 6153193,
            "Name": "SYNTHETIC NUMERIC",
            "Province": "ONTARIO",
            "Station ID": 49908,
            "WMO ID": 71234,
            "TC ID": None,
            "Latitude (Decimal Degrees)": 43.3,
            "Longitude (Decimal Degrees)": -79.9,
            "Elevation (m)": 120,
            "First Year": 2010,
            "Last Year": 2025,
            "HLY First Year": 2010,
            "HLY Last Year": 2025,
            "DLY First Year": 2010,
            "DLY Last Year": 2025,
            "MLY First Year": 2010,
            "MLY Last Year": 2025,
        },
    ]
    for row in rows:
        sheet.append([row[name] for name in headers])
    source = tmp_path / "station_inventory.xlsx"
    workbook.save(source)

    chunks = list(inventory_csv.parse(source, spec, set()))
    frame = chunks[0].table.to_pandas()
    assert frame["entity_id"].tolist() == ["10114F6", "6153193"]
    assert frame["source_station_id"].tolist() == ["24", "49908"]
    assert pd.isna(frame.loc[0, "wmo_id"])
    assert frame.loc[1, "wmo_id"] == "71234"
    assert frame["longitude"].tolist() == [-79.87, -79.9]
    assert frame["timezone_name"].tolist() == [
        "America/Toronto",
        "America/Toronto",
    ]
    assert frame["timezone_source"].str.startswith("timezonefinder ").all()
    assert (
        frame["timezone_source"]
        .str.contains("timezonefinder-data 1.2026.3", regex=False)
        .all()
    )
    assert frame["timezone_source"].str.contains("tzdata 2026.3", regex=False).all()


def test_eccc_ingest_uses_committed_station_timezone_relationship(
    tmp_path: Path,
) -> None:
    station_spec = DEFAULT_REGISTRY.get("eccc_station_inventory")
    headers = list(station_spec.ingest_options["column_map"].values())
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(headers)
    station = {
        "Climate ID": 6153193,
        "Name": "SYNTHETIC TORONTO",
        "Province": "ONTARIO",
        "Station ID": 49908,
        "WMO ID": 71234,
        "TC ID": "YYZ",
        "Latitude (Decimal Degrees)": 43.6777,
        "Longitude (Decimal Degrees)": -79.6248,
        "Elevation (m)": 173.4,
        "First Year": 2010,
        "Last Year": 2025,
        "HLY First Year": 2010,
        "HLY Last Year": 2025,
        "DLY First Year": 2010,
        "DLY Last Year": 2025,
        "MLY First Year": 2010,
        "MLY Last Year": 2025,
    }
    sheet.append([station[name] for name in headers])
    inventory = tmp_path / "stations.xlsx"
    workbook.save(inventory)

    store_paths = StorePaths(tmp_path / "store")
    inventory_csv.ingest(
        "eccc_station_inventory",
        inventory,
        registry=DEFAULT_REGISTRY,
        paths=store_paths,
    )
    source = tmp_path / "HLY01_RCS_P2019"
    source.write_text(
        _hourly_record("6153193", "20190701", "262", [10] * 24) + "\n",
        encoding="ascii",
    )
    fixed_width_hourly.ingest(
        "eccc_hly01_observations",
        source,
        registry=DEFAULT_REGISTRY,
        paths=store_paths,
    )

    frame = load(
        "eccc_hly01_observations",
        entity="6153193",
        variable="precipitation_amount_1h",
        store=store_paths.root,
    )
    assert frame["time_start"].iloc[0] == pd.Timestamp("2019-07-01T05:00:00Z")
    assert frame["precipitation_amount_1h"].tolist() == [1.0] * 24
