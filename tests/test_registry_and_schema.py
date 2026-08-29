from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest

from research_store.foundation.models import Registry
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
        "weather_family_a",
        "weather_family_b",
        "hydrometric_flow_daily",
        "hydrometric_level_daily",
        "station_inventory",
        "reanalysis_points_hourly",
        "wind_scada_10min",
    } == ids


def test_provisional_sources_refuse_ingestion() -> None:
    with pytest.raises(RuntimeError, match="provisional"):
        DEFAULT_REGISTRY.get("wind_scada_10min").require_ready()


def test_station_inventory_is_resolved() -> None:
    DEFAULT_REGISTRY.get("station_inventory").require_ready()


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
