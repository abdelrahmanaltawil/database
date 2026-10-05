from __future__ import annotations

import lzma
import sqlite3
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
from openpyxl import Workbook

from research_store import load
from research_store.foundation.paths import StorePaths
from research_store.foundation.catalog import Catalog
from research_store.foundation.pipeline import ParsedChunk, RejectedRecord
from research_store.foundation.registry import DEFAULT_REGISTRY
from research_store.foundation.station_time import timezone_overrides
from research_store.foundation.writer import _HELD_LOCKS
from research_store.ingestion import (
    fixed_width_daily,
    fixed_width_hourly,
    inventory_csv,
    inventory_sqlite,
    unit_value_corrected,
)


def _unit_value_source(path: Path, station_id: str, rows: list[str]) -> None:
    content = "\n".join(
        [
            f"# Discharge.Working@{station_id}.20110101.csv generated at 2026-08-31",
            "#",
            f"# Time-series identifier: Discharge.Working@{station_id}",
            "# Location: SYNTHETIC",
            "# UTC offset: (UTC-05:00)",
            "# Value units: m^3/s",
            "# Value parameter: Discharge",
            "# Interpolation type: Instantaneous Values",
            "# Time-series type: Derived",
            "#",
            "# Export options: Corrected signal from 2011-01-01T00:00:00Z to End of Record",
            "#",
            "# CSV data starts at line 15.",
            "#",
            "ISO 8601 UTC,Timestamp (UTC-05:00),Value,Approval Level,Grade,Qualifiers",
            *rows,
            "",
        ]
    )
    with lzma.open(path, "wt", encoding="utf-8", newline="") as stream:
        stream.write(content)


def test_corrected_unit_value_parser_preserves_native_instants_and_quality(
    tmp_path: Path,
) -> None:
    source = tmp_path / "station.csv.xz"
    _unit_value_source(
        source,
        "02BF013",
        [
            "2011-01-01T00:00:00Z,2010-12-31 19:00:00,0.01,Approved,-1,",
            "2011-01-01T00:05:00Z,2010-12-31 19:05:00,0.02,Provisional,20,A,B",
        ],
    )
    chunks = list(
        unit_value_corrected.parse(
            source,
            DEFAULT_REGISTRY.get("hydrometric_discharge_unit_corrected"),
            set(),
            station_id="02BF013",
            source_key="fixture",
            rows_per_batch=1,
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()
    assert "time_end" not in frame
    assert frame["entity_id"].tolist() == ["02BF013", "02BF013"]
    assert frame["time_start"].tolist() == [
        pd.Timestamp("2011-01-01T00:00:00Z"),
        pd.Timestamp("2011-01-01T00:05:00Z"),
    ]
    assert frame["discharge"].tolist() == [0.01, 0.02]
    assert frame["approval_level"].tolist() == ["Approved", "Provisional"]
    assert frame["grade"].tolist() == ["-1", "20"]
    assert frame["qualifiers"].tolist() == ["[]", '["A","B"]']


def test_corrected_unit_value_collection_filter_is_a_run_parameter(
    tmp_path: Path,
) -> None:
    root = tmp_path / "corrected"
    (root / "02").mkdir(parents=True)
    filenames = [
        "Discharge.Working@02BF013.20110101_corrected.csv.xz",
        "Discharge.Working@02ZZ999.20110101_corrected.csv.xz",
        "Discharge.Working@02NO001.20110101_corrected.csv.xz",
    ]
    for filename in filenames:
        (root / "02" / filename).write_bytes(b"fixture")
    manifest = root / "corrected_files.tsv"
    manifest.write_text(
        "region\tfilename\tpublisher_modified\tpublisher_listed_size\tsource_url\n"
        + "".join(
            f"02\t{name}\t2026-08-31 00:00\t1K\thttps://example/{name}\n"
            for name in filenames
        )
    )
    hydat = tmp_path / "Hydat.sqlite3"
    with sqlite3.connect(hydat) as connection:
        connection.execute(
            "CREATE TABLE STATIONS "
            "(STATION_NUMBER TEXT, DRAINAGE_AREA_GROSS DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO STATIONS VALUES (?, ?)",
            [("02BF013", 0.53), ("02ZZ999", 11.0)],
        )
    selection = unit_value_corrected.select_sources(
        manifest,
        hydat,
        DEFAULT_REGISTRY.get("hydrometric_discharge_unit_corrected"),
        max_drainage_area_km2=10,
    )
    assert [source.station_id for source in selection.sources] == ["02BF013"]
    assert selection.manifest_count == 3
    assert selection.unmatched_station_count == 1
    assert selection.excluded_by_area_count == 1


def test_corrected_unit_value_collection_ingest_has_per_file_provenance(
    tmp_path: Path,
) -> None:
    root = tmp_path / "corrected"
    (root / "02").mkdir(parents=True)
    selected_name = "Discharge.Working@02BF013.20110101_corrected.csv.xz"
    excluded_name = "Discharge.Working@02ZZ999.20110101_corrected.csv.xz"
    _unit_value_source(
        root / "02" / selected_name,
        "02BF013",
        ["2011-01-01T00:00:00Z,2010-12-31 19:00:00,0.01,Approved,20,ICE"],
    )
    _unit_value_source(
        root / "02" / excluded_name,
        "02ZZ999",
        ["2011-01-01T00:00:00Z,2010-12-31 19:00:00,1.0,Approved,-1,"],
    )
    manifest = root / "corrected_files.tsv"
    manifest.write_text(
        "region\tfilename\tpublisher_modified\tpublisher_listed_size\tsource_url\n"
        f"02\t{selected_name}\t2026-08-31 00:00\t1K\thttps://example/{selected_name}\n"
        f"02\t{excluded_name}\t2026-08-31 00:00\t1K\thttps://example/{excluded_name}\n"
    )
    hydat = tmp_path / "Hydat.sqlite3"
    with sqlite3.connect(hydat) as connection:
        connection.execute(
            "CREATE TABLE STATIONS "
            "(STATION_NUMBER TEXT, DRAINAGE_AREA_GROSS DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO STATIONS VALUES (?, ?)",
            [("02BF013", 0.53), ("02ZZ999", 11.0)],
        )
    paths = StorePaths(tmp_path / "store")
    snapshot = unit_value_corrected.ingest(
        "hydrometric_discharge_unit_corrected",
        manifest,
        station_metadata=hydat,
        max_drainage_area_km2=10,
        registry=DEFAULT_REGISTRY,
        paths=paths,
        source_uri="https://example/corrected/",
        publisher_vintage="fixture",
    )
    frame = load(
        "hydrometric_discharge_unit_corrected",
        snapshot=snapshot,
        store=paths.root,
    )
    assert frame[["entity_id", "discharge"]].to_dict("records") == [
        {"entity_id": "02BF013", "discharge": 0.01}
    ]
    assert frame["approval_level"].tolist() == ["Approved"]
    assert frame["grade"].tolist() == ["20"]
    assert frame["qualifiers"].tolist() == ['["ICE"]']
    provenance = Catalog(paths).provenance(
        "hydrometric_discharge_unit_corrected", snapshot
    )
    assert {item["input_role"] for item in provenance} == {
        "publisher_manifest",
        "selection_metadata",
        "observation_source",
    }
    assert {item["original_name"] for item in provenance} == {
        "corrected_files.tsv",
        "Hydat.sqlite3",
        selected_name,
    }


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


def test_eccc_standard_time_policy_handles_continuous_wartime_dst(
    tmp_path: Path,
) -> None:
    source = tmp_path / "toronto-wartime"
    source.write_text(
        _hourly_record("6153193", "19430101", "123", [0] * 24) + "\n",
        encoding="ascii",
    )

    chunks = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly03_observations"),
            set(),
            timezone_by_entity={"6153193": "America/Toronto"},
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()

    assert frame["time_start"].iloc[0] == pd.Timestamp("1943-01-01T05:00:00Z")
    assert frame["time_end"].iloc[-1] == pd.Timestamp("1943-01-02T05:00:00Z")


def test_eccc_standard_time_policy_handles_inuvik_dst_history(
    tmp_path: Path,
) -> None:
    source = tmp_path / "inuvik-dst-transition"
    source.write_text(
        _hourly_record("2202578", "20041031", "262", [0] * 24) + "\n",
        encoding="ascii",
    )

    chunks = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={"2202578": "America/Inuvik"},
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()

    assert frame["time_start"].iloc[0] == pd.Timestamp("2004-10-31T07:00:00Z")
    assert frame["time_end"].iloc[-1] == pd.Timestamp("2004-11-01T07:00:00Z")
    assert (frame["time_end"] - frame["time_start"]).eq(
        pd.Timedelta(hours=1)
    ).all()


def test_eccc_standard_time_policy_handles_inuvik_dst_boundaries(
    tmp_path: Path,
) -> None:
    records = [
        _hourly_record("2202578", "20040404", "262", [0] * 24),
        _hourly_record("2202578", "20041031", "262", [0] * 24),
    ]
    source = tmp_path / "HLY01_RCS_P2004"
    source.write_text("\n".join(records) + "\n", encoding="ascii")

    chunks = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={"2202578": "America/Inuvik"},
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()

    durations = frame["time_end"] - frame["time_start"]
    assert (durations == pd.Timedelta(hours=1)).all()
    fall_last = frame.loc[
        frame["time_start"] == pd.Timestamp("2004-11-01T06:00:00Z")
    ].iloc[0]
    assert fall_last["time_end"] == pd.Timestamp("2004-11-01T07:00:00Z")


def test_eccc_standard_time_policy_reads_rules_beyond_the_last_transition(
    tmp_path: Path,
) -> None:
    # Pinned tzdata is compiled "slim": Resolute's last explicit transition is
    # in 2007 and later years exist only as the TZif footer rule. Reading the
    # transition table alone leaves Resolute on EST (UTC-5) for good, an hour
    # off for every observation since; its standard time is CST (UTC-6).
    source = tmp_path / "resolute-after-2007"
    source.write_text(
        _hourly_record("2403500", "20100115", "262", [0] * 24) + "\n"
        + _hourly_record("2403500", "20100715", "262", [0] * 24) + "\n",
        encoding="ascii",
    )

    chunks = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={"2403500": "America/Resolute"},
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()

    starts = sorted(frame["time_start"].unique())
    assert starts[0] == pd.Timestamp("2010-01-15T06:00:00Z")
    assert starts[24] == pd.Timestamp("2010-07-15T06:00:00Z")


def test_eccc_hash_filled_snow_depth_is_preserved_as_missing(
    tmp_path: Path,
) -> None:
    record = _hourly_record("2301102", "20041130", "275", [0] * 24)
    hash_slot = 15
    offset = 18 + hash_slot * 7
    record = record[:offset] + "###### " + record[offset + 7 :]
    source = tmp_path / "hash-filled-snow-depth"
    source.write_text(record + "\n", encoding="ascii")

    chunks = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={"2301102": "America/Halifax"},
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()

    assert pd.isna(frame.loc[hash_slot, "value"])
    assert frame.loc[hash_slot, "quality_flag"] == ""


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


def test_eccc_timezone_override_fills_only_an_absent_station(
    tmp_path: Path,
) -> None:
    spec = DEFAULT_REGISTRY.get("eccc_hly01_observations")
    overrides = timezone_overrides(spec)
    assert {
        "1102259": "America/Vancouver",
        "6112335": "America/Toronto",
        "611E001": "America/Toronto",
        "6158434": "America/Toronto",
        "6158435": "America/Toronto",
    }.items() <= overrides.items()
    assert "9040900" not in overrides
    assert "9052008" not in overrides

    source = tmp_path / "missing-inventory-station"
    source.write_text(
        _hourly_record("6112335", "20040101", "262", [10] * 24) + "\n",
        encoding="ascii",
    )
    chunks = list(
        fixed_width_hourly.parse(
            source,
            spec,
            set(),
            timezone_by_entity=overrides,
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()
    assert frame["time_start"].iloc[0] == pd.Timestamp("2004-01-01T05:00:00Z")


def test_hly03_salvages_valid_records_and_quarantines_malformed_lines(
    tmp_path: Path,
) -> None:
    record = _hourly_record("6153193", "20180101", "123", [10] * 24)
    source = tmp_path / "HLY03_INT_P2018"
    source.write_text(
        record + "PUBLISHER-GARBAGE\n" + "truncated\n" + ("X" * 186) + "\n",
        encoding="ascii",
    )

    events = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly03_observations"),
            set(),
            timezone_by_entity={"6153193": "America/Toronto"},
        )
    )
    chunks = [event for event in events if isinstance(event, ParsedChunk)]
    rejections = [event for event in events if isinstance(event, RejectedRecord)]
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()

    assert len(frame) == 24
    assert frame["value"].tolist() == [1.0] * 24
    assert [item.reason for item in rejections] == [
        "extra_bytes_around_valid_fixed_width_records",
        "truncated_fixed_width_record",
        "invalid_fixed_width_record",
    ]
    assert rejections[0].recovered_record_count == 1
    assert rejections[0].details == {
        "expected_width": 186,
        "recovered_offsets": [0],
        "discarded_character_count": 17,
    }


def test_hly01_remains_strict_for_malformed_physical_lines(tmp_path: Path) -> None:
    source = tmp_path / "HLY01_RCS_P2025"
    source.write_text(
        _hourly_record("6153193", "20250101", "262", [0] * 24) + "garbage\n",
        encoding="ascii",
    )

    with pytest.raises(ValueError, match="Unexpected data after position 186"):
        list(
            fixed_width_hourly.parse(
                source,
                DEFAULT_REGISTRY.get("eccc_hly01_observations"),
                set(),
                timezone_by_entity={"6153193": "America/Toronto"},
            )
        )


def test_missing_station_timezone_is_explicitly_quarantined(tmp_path: Path) -> None:
    record = _hourly_record("9052008", "20250101", "262", [0] * 24)
    source = tmp_path / "HLY01_RCS_P2025"
    source.write_text(record + "\n", encoding="ascii")

    events = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={},
        )
    )

    assert len(events) == 1
    rejection = events[0]
    assert isinstance(rejection, RejectedRecord)
    assert rejection.reason == "missing_station_timezone"
    assert rejection.record_locator == "line:1:chars:1-186"
    assert rejection.details == {
        "entity_id": "9052008",
        "source_date": "20250101",
        "source_element": "262",
    }


def test_hly03_quarantines_a_historical_standard_offset_transition(
    tmp_path: Path,
) -> None:
    source = tmp_path / "HLY03_INT_P1960"
    source.write_text(
        _hourly_record("4012160", "19600424", "123", [0] * 24) + "\n",
        encoding="ascii",
    )

    events = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly03_observations"),
            set(),
            timezone_by_entity={"4012160": "America/Regina"},
        )
    )

    assert len(events) == 1
    rejection = events[0]
    assert isinstance(rejection, RejectedRecord)
    assert rejection.reason == "standard_timezone_transition"
    assert rejection.details is not None
    assert rejection.details["timezone_name"] == "America/Regina"
    assert "changes between source-day boundaries" in rejection.details["error"]


def test_hly01_quarantines_every_element_on_a_standard_offset_transition_day(
    tmp_path: Path,
) -> None:
    source = tmp_path / "HLY01_RCS_P2020"
    source.write_text(
        _hourly_record("2100184", "20201031", "271", [0] * 24) + "\n",
        encoding="ascii",
    )

    events = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={"2100184": "America/Dawson"},
        )
    )

    assert len(events) == 1
    rejection = events[0]
    assert isinstance(rejection, RejectedRecord)
    assert rejection.reason == "standard_timezone_transition"
    assert rejection.details is not None
    assert rejection.details["source_date"] == "20201031"
    assert rejection.details["source_element"] == "271"
    assert rejection.details["timezone_name"] == "America/Dawson"
    assert "changes between source-day boundaries" in rejection.details["error"]


def test_eccc_standard_time_policy_keeps_yukon_2020_change_at_november(
    tmp_path: Path,
) -> None:
    source = tmp_path / "yukon-2020"
    source.write_text(
        "\n".join(
            [
                _hourly_record("2100184", "20200630", "271", [0] * 24),
                _hourly_record("2100184", "20200701", "271", [0] * 24),
            ]
        )
        + "\n",
        encoding="ascii",
    )

    chunks = list(
        fixed_width_hourly.parse(
            source,
            DEFAULT_REGISTRY.get("eccc_hly01_observations"),
            set(),
            timezone_by_entity={"2100184": "America/Dawson"},
        )
    )
    frame = pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()

    assert not frame.duplicated(
        ["entity_id", "time_start", "time_end", "variable"]
    ).any()
    assert frame["time_start"].iloc[0] == pd.Timestamp("2020-06-30T08:00:00Z")
    assert frame["time_start"].iloc[24] == pd.Timestamp("2020-07-01T08:00:00Z")


def _daily_record(
    entity: str,
    year_month: str,
    element: str,
    values: list[int | None],
    flags: list[str] | None = None,
) -> str:
    assert len(entity) == 7
    assert len(values) == 31
    flags = flags or [""] * 31
    fields: list[str] = []
    for value, flag in zip(values, flags, strict=True):
        if value is None:
            raw = "-99999"
        else:
            raw = f"{'-' if value < 0 else '0'}{abs(value):05d}"
        fields.append(raw + (flag or " "))
    result = f"{entity}{year_month}{element}" + "".join(fields)
    assert len(result) == 233
    return result


def _daily_frame(source: Path, dataset_id: str = "eccc_dly04_observations"):
    chunks = list(
        fixed_width_daily.parse(source, DEFAULT_REGISTRY.get(dataset_id), set())
    )
    return pa.concat_tables([chunk.table for chunk in chunks]).to_pandas()


def test_eccc_daily_parser_stamps_the_climatological_day(tmp_path: Path) -> None:
    values: list[int | None] = [231, None, *([0] * 29)]
    flags = ["", "M", *([""] * 29)]
    record = _daily_record("6153193", "201408", "001", values, flags)
    source = tmp_path / "DLY04_2014"
    # Exercise both a missing final blank flag and blank physical lines.
    source.write_bytes((record[:-1] + "\r\n\r\n").encode("ascii"))

    frame = _daily_frame(source)

    assert len(frame) == 31
    assert frame["entity_id"].unique().tolist() == ["6153193"]
    assert frame["variable"].unique().tolist() == ["air_temperature_max"]
    assert frame["source_element"].unique().tolist() == ["001"]
    assert frame.loc[0, "value"] == 23.1
    assert pd.isna(frame.loc[1, "value"])
    assert frame.loc[0, "quality_flag"] == ""
    assert frame.loc[1, "quality_flag"] == "M"
    # The day ends at 0600Z of the following day, so it opens at 0600Z of its own.
    assert frame.loc[0, "time_start"] == pd.Timestamp("2014-08-01T06:00:00Z")
    assert frame.loc[0, "time_end"] == pd.Timestamp("2014-08-02T06:00:00Z")
    assert frame.loc[30, "time_start"] == pd.Timestamp("2014-08-31T06:00:00Z")


def test_eccc_daily_elements_keep_distinct_variables_and_scales(
    tmp_path: Path,
) -> None:
    records = [
        _daily_record("6153193", "201901", "002", [-123] * 31),
        _daily_record("6153193", "201901", "003", [-50] * 31),
        _daily_record("6153193", "201901", "010", [25] * 31),
        _daily_record("6153193", "201901", "011", [25] * 31),
        _daily_record("6153193", "201901", "012", [25] * 31),
    ]
    source = tmp_path / "DLY04_2019"
    source.write_text("\n".join(records) + "\n", encoding="ascii")

    frame = _daily_frame(source)
    by_element = {
        element: group.iloc[0]
        for element, group in frame.groupby("source_element", sort=True)
    }

    assert by_element["002"]["variable"] == "air_temperature_min"
    assert by_element["002"]["value"] == -12.3
    assert by_element["003"]["variable"] == "air_temperature_mean"
    assert by_element["003"]["value"] == -5.0
    # Rainfall and total precipitation are tenths of a millimetre; snowfall is
    # tenths of a centimetre. Same stored integer, different registered unit.
    assert by_element["010"]["variable"] == "rainfall_amount"
    assert by_element["010"]["value"] == 2.5
    assert by_element["011"]["variable"] == "snowfall_amount"
    assert by_element["011"]["value"] == 2.5
    assert by_element["012"]["variable"] == "precipitation_amount"
    assert by_element["012"]["value"] == 2.5

    spec = DEFAULT_REGISTRY.get("eccc_dly04_observations")
    assert spec.variable("rainfall_amount").unit == "mm"
    assert spec.variable("snowfall_amount").unit == "cm"
    assert spec.variable("air_temperature_min").unit == "degC"


def test_eccc_daily_drops_day_slots_the_calendar_cannot_hold(tmp_path: Path) -> None:
    records = [
        _daily_record("6153193", "201502", "001", [10] * 31),
        _daily_record("6153193", "201602", "001", [10] * 31),
    ]
    source = tmp_path / "DLY04_february"
    source.write_text("\n".join(records) + "\n", encoding="ascii")

    frame = _daily_frame(source)
    common = frame.loc[frame["time_start"].dt.year == 2015]
    leap = frame.loc[frame["time_start"].dt.year == 2016]

    assert len(common) == 28
    assert len(leap) == 29
    assert common["time_start"].max() == pd.Timestamp("2015-02-28T06:00:00Z")
    assert leap["time_start"].max() == pd.Timestamp("2016-02-29T06:00:00Z")


def test_eccc_daily_refuses_eras_without_a_declared_boundary(tmp_path: Path) -> None:
    source = tmp_path / "DLY04_1961"
    source.write_text(
        _daily_record("6153193", "196106", "001", [10] * 31) + "\n", encoding="ascii"
    )

    with pytest.raises(ValueError, match="No climatological-day rule covers"):
        _daily_frame(source)


def test_eccc_daily_refuses_undeclared_station_day_classes(tmp_path: Path) -> None:
    source = tmp_path / "DLY04_class"
    source.write_text(
        _daily_record("6153193", "201408", "001", [10] * 31) + "\n", encoding="ascii"
    )
    base = DEFAULT_REGISTRY.get("eccc_dly04_observations")
    spec = replace(
        base,
        ingest_options={
            **dict(base.ingest_options),
            "station_day_class": "morning_and_afternoon",
        },
    )

    with pytest.raises(ValueError, match="station_day_class must be declared"):
        list(fixed_width_daily.parse(source, spec, set()))


def test_eccc_daily_publishes_and_reads_back_named_columns(
    tmp_path: Path, store_paths: StorePaths
) -> None:
    source = tmp_path / "DLY04_publish"
    source.write_text(
        "\n".join(
            [
                _daily_record("6153193", "201907", "001", [250] * 31),
                _daily_record("6153193", "201907", "002", [130] * 31),
            ]
        )
        + "\n",
        encoding="ascii",
    )

    fixed_width_daily.ingest(
        "eccc_dly04_observations",
        source,
        registry=DEFAULT_REGISTRY,
        paths=store_paths,
    )
    frame = load(
        "eccc_dly04_observations",
        entity="6153193",
        variable=["air_temperature_max", "air_temperature_min"],
        start="2019",
        end="2020",
        store=store_paths.root,
    )

    assert frame["time_start"].iloc[0] == pd.Timestamp("2019-07-01T06:00:00Z")
    assert frame["air_temperature_max"].tolist() == [25.0] * 31
    assert frame["air_temperature_min"].tolist() == [13.0] * 31


def test_hydrometric_sqlite_inventory_derives_timezones_and_preserves_identifiers(
    tmp_path: Path, store_paths: StorePaths
) -> None:
    db_path = tmp_path / "Hydat_test.sqlite3"
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE STATIONS (
                STATION_NUMBER TEXT,
                STATION_NAME TEXT,
                PROV_TERR_STATE_LOC TEXT,
                REGIONAL_OFFICE_ID TEXT,
                HYD_STATUS TEXT,
                SED_STATUS TEXT,
                LATITUDE DOUBLE,
                LONGITUDE DOUBLE,
                DRAINAGE_AREA_GROSS DOUBLE,
                DRAINAGE_AREA_EFFECT DOUBLE,
                RHBN INTEGER,
                REAL_TIME INTEGER,
                CONTRIBUTOR_ID INTEGER,
                OPERATOR_ID INTEGER,
                DATUM_ID INTEGER
            )
            """
        )
        con.executemany(
            "INSERT INTO STATIONS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "02GA010",
                    "GRAND RIVER AT GALT",
                    "ON",
                    "4",
                    "A",
                    None,
                    43.35694,
                    -80.31639,
                    3520.0,
                    3450.0,
                    1,
                    1,
                    647,
                    647,
                    10,
                ),
                (
                    "08HA001",
                    "FRASER RIVER AT HOPE",
                    "BC",
                    "8",
                    "A",
                    "A",
                    49.38278,
                    -121.45278,
                    217000.0,
                    None,
                    1,
                    1,
                    647,
                    647,
                    10,
                ),
            ],
        )

    snapshot = inventory_sqlite.ingest(
        "hydrometric_station_inventory",
        db_path,
        registry=DEFAULT_REGISTRY,
        paths=store_paths,
        publisher_vintage="test-snapshot",
    )

    frame = load(
        "hydrometric_station_inventory",
        snapshot=snapshot,
        store=store_paths.root,
    )

    assert frame["entity_id"].tolist() == ["02GA010", "08HA001"]
    assert frame["station_name"].tolist() == [
        "GRAND RIVER AT GALT",
        "FRASER RIVER AT HOPE",
    ]
    assert frame["province"].tolist() == ["ON", "BC"]
    assert frame["drainage_area_gross"].tolist() == [3520.0, 217000.0]
    assert frame["drainage_area_effective"].tolist()[0] == 3450.0
    assert pd.isna(frame["drainage_area_effective"].tolist()[1])
    assert frame["is_rhbn"].tolist() == ["1", "1"]
    assert frame["is_real_time"].tolist() == ["1", "1"]
    assert frame["timezone_name"].tolist() == [
        "America/Toronto",
        "America/Vancouver",
    ]
    assert frame["timezone_source"].str.startswith("timezonefinder ").all()



def _corrected_collection(tmp_path: Path) -> tuple[Path, Path]:
    """The smallest publishable corrected unit-value collection."""

    root = tmp_path / "corrected"
    (root / "02").mkdir(parents=True)
    name = "Discharge.Working@02BF013.20110101_corrected.csv.xz"
    _unit_value_source(
        root / "02" / name,
        "02BF013",
        ["2011-01-01T00:00:00Z,2010-12-31 19:00:00,0.01,Approved,20,ICE"],
    )
    manifest = root / "corrected_files.tsv"
    manifest.write_text(
        "region\tfilename\tpublisher_modified\tpublisher_listed_size\tsource_url\n"
        f"02\t{name}\t2026-08-31 00:00\t1K\thttps://example/{name}\n"
    )
    hydat = tmp_path / "Hydat.sqlite3"
    with sqlite3.connect(hydat) as connection:
        connection.execute(
            "CREATE TABLE STATIONS "
            "(STATION_NUMBER TEXT, DRAINAGE_AREA_GROSS DOUBLE)"
        )
        connection.execute("INSERT INTO STATIONS VALUES (?, ?)", ("02BF013", 0.53))
    return manifest, hydat


def test_corrected_unit_value_ingest_releases_the_store_write_lock(
    tmp_path: Path,
) -> None:
    """A writer that leaks its lock shuts the store to every later process."""

    manifest, hydat = _corrected_collection(tmp_path)
    unit_value_corrected.ingest(
        "hydrometric_discharge_unit_corrected",
        manifest,
        station_metadata=hydat,
        registry=DEFAULT_REGISTRY,
        paths=StorePaths(tmp_path / "store"),
    )
    assert not _HELD_LOCKS, "the corrected unit-value ingest leaked its write lock"


def test_corrected_unit_value_ingest_rebuilds_fragments_a_restore_lost(
    tmp_path: Path,
) -> None:
    """A committed run with no fragments on disk is a restore, not a no-op."""

    manifest, hydat = _corrected_collection(tmp_path)
    paths = StorePaths(tmp_path / "store")
    arguments = {
        "station_metadata": hydat,
        "registry": DEFAULT_REGISTRY,
        "paths": paths,
    }
    snapshot = unit_value_corrected.ingest(
        "hydrometric_discharge_unit_corrected", manifest, **arguments
    )
    fragments = Catalog(paths).snapshot_fragment_paths(snapshot)
    assert fragments
    for fragment in fragments:
        Path(fragment).unlink()

    rebuilt = unit_value_corrected.ingest(
        "hydrometric_discharge_unit_corrected", manifest, **arguments
    )
    assert rebuilt == snapshot, "a restore must not mint a new snapshot identity"
    assert all(Path(fragment).is_file() for fragment in fragments)
    frame = load(
        "hydrometric_discharge_unit_corrected", snapshot=snapshot, store=paths.root
    )
    assert frame["discharge"].tolist() == [0.01]
