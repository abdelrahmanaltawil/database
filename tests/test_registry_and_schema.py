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
        "eccc_hly01_observations",
        "eccc_hly03_observations",
        "eccc_station_inventory",
    } == ids


def test_station_inventory_is_resolved() -> None:
    DEFAULT_REGISTRY.get("eccc_station_inventory").require_ready()


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


def test_private_registry_overlay_changes_identity(tmp_path) -> None:
    private = tmp_path / "private.json"
    private.write_text(
        """
        {
          "eccc_hly03_observations": {
            "description": "Locally annotated ECCC HLY03 observations"
          }
        }
        """
    )
    resolved = load_registry(private)
    assert (
        resolved.get("eccc_hly03_observations").description
        == "Locally annotated ECCC HLY03 observations"
    )
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
