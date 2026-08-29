from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pyarrow as pa
from openpyxl import Workbook

from research_store.foundation.models import DatasetReadiness
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


def _ready_weather(dataset_id: str, timezone: str = "Etc/GMT+5"):
    base = DEFAULT_REGISTRY.get(dataset_id)
    return replace(
        base,
        source_timezone=timezone,
        readiness=DatasetReadiness.READY,
        unresolved_decisions=(),
    )


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
        fixed_width_hourly.parse(source, _ready_weather("weather_family_a"), set())
    )
    table = pa.concat_tables([chunk.table for chunk in chunks])
    frame = table.to_pandas()

    assert len(frame) == 24
    assert frame["entity_id"].unique().tolist() == ["702S006"]
    assert frame["variable"].unique().tolist() == ["precipitation_amount"]
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
    base = _ready_weather("weather_family_b")
    spec = replace(
        base,
        ingest_options={**dict(base.ingest_options), "entity_allowlist": ["1012055"]},
    )

    chunks = list(fixed_width_hourly.parse(source, spec, set()))
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()
    assert frame["entity_id"].unique().tolist() == ["1012055"]
    assert frame["value"].unique().tolist() == [0.1]


def test_station_inventory_xlsx_with_disclaimers_and_alphanumeric_ids(
    tmp_path: Path,
) -> None:
    spec = DEFAULT_REGISTRY.get("station_inventory")
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
