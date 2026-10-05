from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pyarrow as pa
import pytest

from research_store.foundation.models import (
    DocumentationSpec,
    Registry,
    StorageModel,
    TemporalKind,
)
from research_store.foundation.registry import (
    BASE_REGISTRY,
    DEFAULT_REGISTRY,
    load_registry,
)
from research_store.foundation.schema import validate_table


def _wide_table(entity_values, *, value_type=None, timezone="UTC") -> pa.Table:
    value_type = value_type or pa.float64()
    return pa.table(
        {
            "entity_id": pa.array(entity_values),
            "time_start": pa.array(
                [pd.Timestamp("2024-01-01", tz="UTC")] * len(entity_values),
                type=pa.timestamp("ns", tz=timezone),
            ),
            "time_end": pa.array(
                [pd.Timestamp("2024-01-01 00:10", tz="UTC")] * len(entity_values),
                type=pa.timestamp("ns", tz=timezone),
            ),
            "power": pa.array([1.0] * len(entity_values), type=value_type),
            "power_quality": pa.array(["A"] * len(entity_values)),
            "wind_speed": pa.array([2.0] * len(entity_values), type=value_type),
        }
    )


def test_every_required_source_has_one_registry_entry() -> None:
    ids = {spec.dataset_id for spec in DEFAULT_REGISTRY}
    assert {
        "eccc_climate_hourly_observations",
        "eccc_dly04_observations",
        "eccc_hly01_observations",
        "eccc_hly03_observations",
        "eccc_station_inventory",
        "hydrometric_discharge_unit_corrected",
        "hydrometric_flow_daily",
        "hydrometric_level_daily",
        "hydrometric_station_inventory",
        "reanalysis_points_hourly",
        "wind_scada_10min",
    } == ids


def test_corrected_unit_values_are_generic_and_keep_quality_annotations() -> None:
    spec = DEFAULT_REGISTRY.get("hydrometric_discharge_unit_corrected")
    spec.require_ready()
    assert spec.temporal_kind is TemporalKind.INSTANT
    assert spec.time_end_field is None
    assert spec.variable_names == ("discharge",)
    assert [annotation.name for annotation in spec.annotations] == [
        "approval_level",
        "grade",
        "qualifiers",
    ]
    assert "small" not in spec.dataset_id
    assert "max_drainage_area_km2" not in spec.ingest_options


def test_provisional_sources_refuse_ingestion() -> None:
    with pytest.raises(RuntimeError, match="provisional"):
        DEFAULT_REGISTRY.get("wind_scada_10min").require_ready()


def test_station_inventory_is_resolved() -> None:
    DEFAULT_REGISTRY.get("eccc_station_inventory").require_ready()


def test_eccc_daily_source_is_resolved_and_scoped_to_its_documented_era() -> None:
    spec = DEFAULT_REGISTRY.get("eccc_dly04_observations")
    spec.require_ready()
    options = spec.ingest_options
    assert spec.storage_model is StorageModel.LONG
    assert spec.temporal_kind is TemporalKind.INTERVAL
    assert options["station_day_class"] == "synoptic_24h"
    rules = options["climatological_day_rules"]
    assert [rule["utc_end_hour"] for rule in rules] == [6]
    assert rules[0]["start"] == "1961-07-01"
    assert all(rule["evidence"] for rule in rules)
    # Snow on the ground has no documented observation time, so it must not be
    # mapped to an interval variable.
    assert "013" not in options["elements"]
    assert 16 + 31 * int(options["field_width"]) == 233


def test_eccc_observation_sources_are_resolved() -> None:
    hly01 = DEFAULT_REGISTRY.get("eccc_hly01_observations")
    hly03 = DEFAULT_REGISTRY.get("eccc_hly03_observations")
    hly01.require_ready()
    hly03.require_ready()
    assert set(hly01.ingest_options["elements"]) == {
        str(code) for code in range(262, 281)
    }
    assert hly01.variable("precipitation_amount_1h").unit == "mm"
    assert hly01.variable("snow_depth").unit == "cm"
    hash_rule = next(rule for rule in hly01.sentinel_rules if rule.marker == "######")
    assert hash_rule.meaning == "missing"
    assert hash_rule.replacement is None
    assert "HLY01_RCS_P2004" in hash_rule.evidence
    overrides = hly01.ingest_options["timezone_overrides"]
    assert {"1102259", "6112335", "611E001", "6158434", "6158435"} <= set(
        overrides
    )
    assert {"9040900", "9052008"}.isdisjoint(overrides)
    assert all(item["timezone_name"] and item["evidence"] for item in overrides.values())
    assert overrides["6112335"]["timezone_name"] == "America/Toronto"
    assert "187" in overrides["6112335"]["evidence"]
    assert hly01.ingest_options["missing_timezone_policy"] == "quarantine_record"
    assert hly03.ingest_options["missing_timezone_policy"] == "quarantine_record"
    assert (
        hly01.ingest_options["standard_time_transition_policy"]
        == "quarantine_record"
    )
    assert (
        hly03.ingest_options["malformed_record_policy"]
        == "salvage_valid_fixed_width_records"
    )
    assert (
        hly03.ingest_options["standard_time_transition_policy"]
        == "quarantine_record"
    )


def test_eccc_hourly_metadata_matches_publisher_documentation() -> None:
    hly01 = DEFAULT_REGISTRY.get("eccc_hly01_observations")
    hly03 = DEFAULT_REGISTRY.get("eccc_hly03_observations")
    hly01_elements = hly01.ingest_options["elements"]

    expected_hly01 = {
        "262": ("precipitation_amount_1h", 0.1, "start_minute", 0, 60),
        "263": ("precipitation_amount_15min", 0.1, "start_minute", 0, 15),
        "264": ("precipitation_amount_15min", 0.1, "start_minute", 15, 15),
        "265": ("precipitation_amount_15min", 0.1, "start_minute", 30, 15),
        "266": ("precipitation_amount_15min", 0.1, "start_minute", 45, 15),
        "267": ("precipitation_gauge_weight", 0.1, "end_minute", 15, 5),
        "268": ("precipitation_gauge_weight", 0.1, "end_minute", 30, 5),
        "269": ("precipitation_gauge_weight", 0.1, "end_minute", 45, 5),
        "270": ("precipitation_gauge_weight", 0.1, "end_minute", 60, 5),
        "271": ("wind_speed_2m_15min", 0.1, "start_minute", 0, 15),
        "272": ("wind_speed_2m_15min", 0.1, "start_minute", 15, 15),
        "273": ("wind_speed_2m_15min", 0.1, "start_minute", 30, 15),
        "274": ("wind_speed_2m_15min", 0.1, "start_minute", 45, 15),
        # Snow depth is scaled by 0.01, against the public table's "whole cm",
        # because the delivered RCS bytes are quantised in steps of 100. The
        # deviation and its evidence are recorded in the documentation notes,
        # which the assertions below require to stay there.
        "275": ("snow_depth", 0.01, "end_minute", 60, 5),
        "276": ("snow_depth", 0.01, "end_minute", 15, 5),
        "277": ("snow_depth", 0.01, "end_minute", 30, 5),
        "278": ("snow_depth", 0.01, "end_minute", 45, 5),
        "279": ("wind_direction_2m_10min", 1.0, "start_minute", 50, 10),
        "280": ("wind_speed_2m_10min", 0.1, "start_minute", 50, 10),
    }
    for element, (variable, scale, boundary, minute, duration) in expected_hly01.items():
        options = hly01_elements[element]
        assert options["variable"] == variable
        assert options["scale"] == scale
        assert options[boundary] == minute
        assert options["duration_minutes"] == duration

    for element in (*map(str, range(267, 271)), *map(str, range(275, 279))):
        assert hly01_elements[element]["before_year"] == 2007
        assert hly01_elements[element]["duration_minutes_before"] == 9

    assert hly03.ingest_options["elements"] == {
        "123": {
            "variable": "precipitation_amount_1h",
            "scale": 0.1,
            "start_minute": 0,
            "duration_minutes": 60,
        }
    }
    assert hly01.documentation is not None
    assert hly03.documentation is not None
    assert "186-character" in hly01.documentation.source_format
    assert "raw/un-QC" in hly01.documentation.quality_control
    assert set(hly01.documentation.quality_flags) == {"blank", "M"}
    snow_note = next(
        (note for note in hly01.documentation.notes if "275-278" in note), None
    )
    assert snow_note is not None, "the snow-depth scale deviation must stay documented"
    assert "0.01" in snow_note and "whole centimetres" in snow_note
    assert "K0414MYP9M" in snow_note, "the outstanding publisher query must be cited"
    assert "quality controlled" in hly03.documentation.quality_control
    assert set(hly03.documentation.quality_flags) == {"blank", "H", "I", "J", "M"}
    assert any("winter zero" in item for item in hly03.documentation.limitations)
    assert any("Technical Documentation" in item for item in hly03.documentation.references)


def test_eccc_climate_hourly_declaration_matches_publisher_documentation() -> None:
    spec = DEFAULT_REGISTRY.get("eccc_climate_hourly_observations")
    hly01 = DEFAULT_REGISTRY.get("eccc_hly01_observations")
    spec.require_ready()
    assert spec.producer == "geomet_climate_hourly"
    assert spec.storage_model is StorageModel.WIDE
    assert spec.temporal_kind is TemporalKind.INTERVAL
    assert spec.snapshot_mode == "replace"
    assert spec.native_frequency == "1 hour"
    assert spec.source_timezone == hly01.source_timezone

    expected = {
        "air_temperature": ("degC", "TEMP"),
        "dew_point_temperature": ("degC", "DEW_POINT_TEMP"),
        "relative_humidity": ("%", "RELATIVE_HUMIDITY"),
        "precipitation_amount_1h": ("mm", "PRECIP_AMOUNT"),
        "wind_direction": ("degree_true", "WIND_DIRECTION"),
        "wind_speed": ("km/h", "WIND_SPEED"),
        "visibility": ("km", "VISIBILITY"),
        "station_pressure": ("kPa", "STATION_PRESSURE"),
        "humidex": ("1", "HUMIDEX"),
        "wind_chill": ("1", "WINDCHILL"),
    }
    options = spec.ingest_options
    column_map = options["column_map"]
    assert set(spec.variable_names) == {*expected, "weather_description"}
    for name, (unit, column) in expected.items():
        variable = spec.variable(name)
        assert variable.unit == unit
        assert variable.dtype == "float64"
        assert variable.quality_field == f"{name}_quality"
        assert variable.plausible_band is not None
        assert column_map[name] == column
        assert column_map[variable.quality_field] == f"{column}_FLAG"
    text = spec.variable("weather_description")
    assert (text.dtype, text.unit, text.quality_field) == ("string", None, None)
    assert column_map["weather_description"] == "WEATHER_ENG_DESC"
    # Only wind direction is rescaled: the publisher reports tens of degrees.
    assert {name for name, scale in options["scales"].items() if scale != 1.0} == {
        "wind_direction"
    }
    assert options["scales"]["wind_direction"] == 10.0
    assert set(options["scales"]) == set(expected)
    # The raw encoding is checked per value; scaled, its maximum is north.
    low, high = options["source_integer_ranges"]["wind_direction"]
    assert (low, high) == (0, 36)
    assert high * options["scales"]["wind_direction"] == 360.0, "tens of degrees"
    band = spec.variable("wind_direction").plausible_band
    assert (band.minimum, band.maximum) == (0.0, 360.0)
    # The same threshold as the HLY01 element this field reproduces.
    ours = spec.variable("precipitation_amount_1h").plausible_band
    theirs = hly01.variable("precipitation_amount_1h").plausible_band
    assert (ours.quantile, ours.minimum, ours.maximum, ours.ignore_zeros) == (
        theirs.quantile,
        theirs.minimum,
        theirs.maximum,
        theirs.ignore_zeros,
    )
    # LOCAL_DATE labels the end of the hour: the total is the whole slot and
    # every other variable sits at its end. HLY01 still declares slot H as
    # [H, H+1) for element 262, one hour later, and documents that as a defect.
    assert options["local_time_labels"] == "slot_end"
    timing = options["variable_timing"]
    assert set(timing) == set(spec.variable_names)
    assert timing["precipitation_amount_1h"] == {"start_minute": 0, "duration_minutes": 60}
    for name, placement in timing.items():
        if name != "precipitation_amount_1h":
            assert placement["end_minute"] == 60, name
    assert hly01.ingest_options["elements"]["262"]["start_minute"] == 0
    assert any("one hour late" in item for item in hly01.documentation.limitations)

    meanings = {rule.marker: rule.meaning for rule in spec.sentinel_rules}
    assert meanings == {"": "missing", "NA": "missing", "0": "not_applicable"}
    assert all(rule.replacement is None for rule in spec.sentinel_rules)
    scope = options["sentinel_variables"]
    assert set(scope[""]) == set(spec.variable_names)
    assert scope["NA"] == ["weather_description"]
    assert scope["0"] == ["wind_direction"]
    assert [item.name for item in spec.annotations] == [
        "source_station_id",
        "record_flag",
    ]
    assert options["annotation_map"] == {"source_station_id": "STN_ID", "record_flag": "FLAG"}
    assert len(options["source_columns"]) == 41
    assert len(set(options["source_columns"])) == 41
    assert set(column_map.values()) <= set(options["source_columns"])
    for key in (
        "timezone_policy",
        "station_dataset_id",
        "timezone_overrides",
        "missing_timezone_policy",
        "standard_time_transition_policy",
    ):
        assert options[key] == hly01.ingest_options[key], key
    assert options["publisher_utc_mismatch_policy"] == "quarantine_record"
    assert options["api"]["limit"] == 10000
    assert options["api"]["format"] == "csv"
    assert options["api"]["settle_days"] >= 1
    assert set(options["page_constant_columns"]) <= set(options["source_columns"])
    assert options["coordinate_columns"] == {
        "x": "LONGITUDE_DECIMAL_DEGREES",
        "y": "LATITUDE_DECIMAL_DEGREES",
    }
    assert options["manifest_columns"][:4] == [
        "climate_id",
        "window_start_lst",
        "window_end_lst",
        "role",
    ]

    documentation = spec.documentation
    assert documentation is not None
    assert set(documentation.quality_flags) == {"blank", "M"}
    assert "CRLF" in documentation.source_format
    notes = " ".join(documentation.notes)
    assert "Data Source: Environment and Climate Change Canada" in notes
    assert "element 262" in notes and "calm 0 is stored as null" in notes
    assert any("End-use Licence" in item for item in documentation.references)
    assert any("calm" in item.lower() for item in documentation.limitations)


def test_documentation_is_catalogued_but_does_not_change_ingestion_identity(
    wide_spec,
) -> None:
    documented = replace(
        wide_spec,
        documentation=DocumentationSpec(
            source_format="Synthetic fixed-width fixture",
            references=("Fixture specification, revision 1",),
            quality_control="No quality control is claimed.",
            quality_flags={"blank": "No source flag."},
            limitations=("Synthetic data only.",),
        ),
    )

    payload = documented.serializable()
    assert payload["documentation"]["quality_flags"] == {
        "blank": "No source flag."
    }
    assert Registry([documented]).digest == Registry([wide_spec]).digest


def test_private_registry_overlay_is_local_and_changes_identity(tmp_path) -> None:
    private = tmp_path / "private.json"
    private.write_text(
        """
        {
          "wind_scada_10min": {
            "source_timezone": "UTC",
            "timestamp_semantics": "interval_end",
            "variables": [
              {"name": "power", "quantity": "active power", "unit": "kW"},
              {"name": "wind_speed", "quantity": "wind speed", "unit": "m/s"}
            ],
            "ingest_options": {
              "format": "csv",
              "entity_column": "private_entity",
              "timestamp_column": "private_time",
              "column_map": {
                "power": "private_power",
                "wind_speed": "private_wind"
              }
            },
            "unresolved_decisions": [],
            "readiness": "ready"
          }
        }
        """
    )
    resolved = load_registry(private)
    resolved.get("wind_scada_10min").require_ready()
    assert BASE_REGISTRY.get("wind_scada_10min").readiness.value == "provisional"
    assert resolved.digest != BASE_REGISTRY.digest


def test_duplicate_dataset_declarations_fail(wide_spec) -> None:
    with pytest.raises(ValueError, match="declared more than once"):
        Registry([wide_spec, wide_spec])


def test_identifiers_must_remain_strings(wide_spec) -> None:
    table = _wide_table([100001])
    with pytest.raises(TypeError, match="must be a string"):
        validate_table(table, wide_spec)


def test_float32_narrowing_is_rejected(wide_spec) -> None:
    table = _wide_table(["0100001"], value_type=pa.float32())
    with pytest.raises(TypeError, match="float64"):
        validate_table(table, wide_spec)


def test_naive_or_non_utc_timestamps_are_rejected(wide_spec) -> None:
    table = _wide_table(["0100001"], timezone="America/Toronto")
    with pytest.raises(TypeError, match="UTC"):
        validate_table(table, wide_spec)


def test_registry_serializes_frequency_and_conventions(wide_spec) -> None:
    payload = wide_spec.serializable()
    assert payload["native_frequency"] == "10 minutes"
    assert payload["source_timezone"] == "UTC"
    assert payload["variables"][0]["unit"] == "kW"
