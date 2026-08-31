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
