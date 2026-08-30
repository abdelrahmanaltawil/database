"""Canonical ECCC declarations plus an optional local source overlay.

Producers and readers consume the same resolved registry. An optional overlay
can refine an existing declaration without creating undeclared dataset IDs.
"""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

from research_store.foundation.models import (
    DatasetKind,
    DatasetReadiness,
    DatasetSpec,
    Registry,
    SentinelRule,
    StorageModel,
    TemporalKind,
    VariableSpec,
)

PRIVATE_REGISTRY_ENV = "RESEARCH_STORE_PRIVATE_REGISTRY"


BASE_REGISTRY = Registry(
    [
        DatasetSpec(
            dataset_id="eccc_hly01_observations",
            description=(
                "ECCC HLY01 hourly weather observations, including RCS elements"
            ),
            kind=DatasetKind.EXTERNAL,
            producer="fixed_width_hourly",
            storage_model=StorageModel.LONG,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="element-specific intervals from 5 minutes to 1 hour",
            source_timezone="station-specific IANA local standard time",
            timestamp_semantics=(
                "HLY01 slots 00-23; each registered element declares its interval "
                "within the source hour and is converted from local standard time"
            ),
            variables=(
                VariableSpec(
                    "precipitation_amount_1h",
                    "precipitation amount",
                    "mm",
                    quality_field="precipitation_amount_1h_quality",
                ),
                VariableSpec(
                    "precipitation_amount_15min",
                    "precipitation amount",
                    "mm",
                    quality_field="precipitation_amount_15min_quality",
                ),
                VariableSpec(
                    "precipitation_gauge_weight",
                    "precipitation gauge mass per unit area",
                    "kg/m2",
                    quality_field="precipitation_gauge_weight_quality",
                ),
                VariableSpec(
                    "wind_speed_2m_15min",
                    "wind speed at approximately 2 m",
                    "km/h",
                    quality_field="wind_speed_2m_15min_quality",
                ),
                VariableSpec(
                    "snow_depth",
                    "snow depth",
                    "cm",
                    quality_field="snow_depth_quality",
                ),
                VariableSpec(
                    "wind_direction_2m_10min",
                    "wind direction at approximately 2 m",
                    "degree_true",
                    quality_field="wind_direction_2m_10min_quality",
                ),
                VariableSpec(
                    "wind_speed_2m_10min",
                    "wind speed at approximately 2 m",
                    "km/h",
                    quality_field="wind_speed_2m_10min_quality",
                ),
            ),
            sentinel_rules=(
                SentinelRule(
                    marker="-99999",
                    meaning="missing",
                    replacement=None,
                    evidence="ECCC Digital Archive Technical Documentation, section 2.3",
                ),
            ),
            ingest_options={
                "station_slice": [0, 7],
                "date_slice": [7, 15],
                "date_format": "%Y%m%d",
                "element_slice": [15, 18],
                "values_start": 18,
                "field_width": 7,
                "value_width": 6,
                "timezone_policy": "station_inventory",
                "station_dataset_id": "eccc_station_inventory",
                "elements": {
                    "262": {
                        "variable": "precipitation_amount_1h",
                        "scale": 0.1,
                        "start_minute": 0,
                        "duration_minutes": 60,
                    },
                    "263": {
                        "variable": "precipitation_amount_15min",
                        "scale": 0.1,
                        "start_minute": 0,
                        "duration_minutes": 15,
                    },
                    "264": {
                        "variable": "precipitation_amount_15min",
                        "scale": 0.1,
                        "start_minute": 15,
                        "duration_minutes": 15,
                    },
                    "265": {
                        "variable": "precipitation_amount_15min",
                        "scale": 0.1,
                        "start_minute": 30,
                        "duration_minutes": 15,
                    },
                    "266": {
                        "variable": "precipitation_amount_15min",
                        "scale": 0.1,
                        "start_minute": 45,
                        "duration_minutes": 15,
                    },
                    "267": {
                        "variable": "precipitation_gauge_weight",
                        "scale": 0.1,
                        "end_minute": 15,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "268": {
                        "variable": "precipitation_gauge_weight",
                        "scale": 0.1,
                        "end_minute": 30,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "269": {
                        "variable": "precipitation_gauge_weight",
                        "scale": 0.1,
                        "end_minute": 45,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "270": {
                        "variable": "precipitation_gauge_weight",
                        "scale": 0.1,
                        "end_minute": 60,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "271": {
                        "variable": "wind_speed_2m_15min",
                        "scale": 0.1,
                        "start_minute": 0,
                        "duration_minutes": 15,
                    },
                    "272": {
                        "variable": "wind_speed_2m_15min",
                        "scale": 0.1,
                        "start_minute": 15,
                        "duration_minutes": 15,
                    },
                    "273": {
                        "variable": "wind_speed_2m_15min",
                        "scale": 0.1,
                        "start_minute": 30,
                        "duration_minutes": 15,
                    },
                    "274": {
                        "variable": "wind_speed_2m_15min",
                        "scale": 0.1,
                        "start_minute": 45,
                        "duration_minutes": 15,
                    },
                    "275": {
                        "variable": "snow_depth",
                        "scale": 1.0,
                        "end_minute": 60,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "276": {
                        "variable": "snow_depth",
                        "scale": 1.0,
                        "end_minute": 15,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "277": {
                        "variable": "snow_depth",
                        "scale": 1.0,
                        "end_minute": 30,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "278": {
                        "variable": "snow_depth",
                        "scale": 1.0,
                        "end_minute": 45,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "279": {
                        "variable": "wind_direction_2m_10min",
                        "scale": 1.0,
                        "start_minute": 50,
                        "duration_minutes": 10,
                    },
                    "280": {
                        "variable": "wind_speed_2m_10min",
                        "scale": 0.1,
                        "start_minute": 50,
                        "duration_minutes": 10,
                    },
                },
                "encoding": "ascii",
            },
        ),
        DatasetSpec(
            dataset_id="eccc_hly03_observations",
            description="ECCC HLY03 hourly rainfall rate archive (element 123)",
            kind=DatasetKind.EXTERNAL,
            producer="fixed_width_hourly",
            storage_model=StorageModel.LONG,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 hour",
            source_timezone="station-specific IANA local standard time",
            timestamp_semantics=(
                "source slots are hourly intervals ending 01-24 local standard time"
            ),
            variables=(
                VariableSpec(
                    "precipitation_amount_1h",
                    "precipitation amount",
                    "mm",
                    quality_field="precipitation_amount_1h_quality",
                ),
            ),
            sentinel_rules=(
                SentinelRule(
                    marker="-99999",
                    meaning="missing",
                    replacement=None,
                    evidence="ECCC Digital Archive Technical Documentation, section 2.3",
                ),
            ),
            ingest_options={
                "station_slice": [0, 7],
                "date_slice": [7, 15],
                "date_format": "%Y%m%d",
                "element_slice": [15, 18],
                "values_start": 18,
                "field_width": 7,
                "value_width": 6,
                "timezone_policy": "station_inventory",
                "station_dataset_id": "eccc_station_inventory",
                "elements": {
                    "123": {
                        "variable": "precipitation_amount_1h",
                        "scale": 0.1,
                        "start_minute": 0,
                        "duration_minutes": 60,
                    }
                },
                "encoding": "ascii",
            },
        ),
        DatasetSpec(
            dataset_id="eccc_station_inventory",
            description="Versioned ECCC station inventory workbook",
            kind=DatasetKind.REFERENCE,
            producer="inventory_csv",
            storage_model=StorageModel.REFERENCE,
            temporal_kind=TemporalKind.REFERENCE,
            time_start_field=None,
            time_end_field=None,
            canonical_timezone=None,
            snapshot_mode="replace",
            variables=(
                VariableSpec("station_name", "station name", None, dtype="string"),
                VariableSpec("province", "province or territory", None, dtype="string"),
                VariableSpec(
                    "source_station_id",
                    "publisher internal station identifier",
                    None,
                    dtype="string",
                ),
                VariableSpec("wmo_id", "WMO identifier", None, dtype="string"),
                VariableSpec(
                    "tc_id", "Transport Canada identifier", None, dtype="string"
                ),
                VariableSpec("latitude", "latitude", "degree_north"),
                VariableSpec("longitude", "longitude", "degree_east"),
                VariableSpec("elevation", "height above reference datum", "m"),
                VariableSpec("first_year", "first year of record", "year"),
                VariableSpec("last_year", "last year of record", "year"),
                VariableSpec("hly_first_year", "first hourly-data year", "year"),
                VariableSpec("hly_last_year", "last hourly-data year", "year"),
                VariableSpec("dly_first_year", "first daily-data year", "year"),
                VariableSpec("dly_last_year", "last daily-data year", "year"),
                VariableSpec("mly_first_year", "first monthly-data year", "year"),
                VariableSpec("mly_last_year", "last monthly-data year", "year"),
                VariableSpec(
                    "timezone_name",
                    "IANA timezone inferred from station coordinates",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "timezone_source",
                    "timezone boundary dataset used for coordinate lookup",
                    None,
                    dtype="string",
                ),
            ),
            partition_keys=(),
            ingest_options={
                "format": "xlsx",
                "header_identifier": "Climate ID",
                "column_map": {
                    "entity_id": "Climate ID",
                    "station_name": "Name",
                    "province": "Province",
                    "source_station_id": "Station ID",
                    "wmo_id": "WMO ID",
                    "tc_id": "TC ID",
                    "latitude": "Latitude (Decimal Degrees)",
                    "longitude": "Longitude (Decimal Degrees)",
                    "elevation": "Elevation (m)",
                    "first_year": "First Year",
                    "last_year": "Last Year",
                    "hly_first_year": "HLY First Year",
                    "hly_last_year": "HLY Last Year",
                    "dly_first_year": "DLY First Year",
                    "dly_last_year": "DLY Last Year",
                    "mly_first_year": "MLY First Year",
                    "mly_last_year": "MLY Last Year",
                },
                "source_longitude_convention": "signed",
                "derive_timezone_from_coordinates": True,
            },
        ),
    ]
)


def _overlay_spec(spec: DatasetSpec, payload: dict[str, Any]) -> DatasetSpec:
    if not isinstance(payload, dict):
        raise TypeError(
            f"Private registry entry for {spec.dataset_id} must be an object"
        )
    allowed = {
        "description",
        "source_timezone",
        "timestamp_semantics",
        "readiness",
        "snapshot_mode",
        "variables",
        "ingest_options",
        "unresolved_decisions",
    }
    unknown = set(payload) - allowed
    if unknown:
        raise ValueError(
            f"Unsupported private registry fields for {spec.dataset_id}: "
            f"{sorted(unknown)}"
        )
    changes: dict[str, Any] = dict(payload)
    if "readiness" in changes:
        changes["readiness"] = DatasetReadiness(str(changes["readiness"]))
    if "variables" in changes:
        variables = changes["variables"]
        if not isinstance(variables, list):
            raise TypeError(f"variables for {spec.dataset_id} must be a list")
        changes["variables"] = tuple(VariableSpec(**item) for item in variables)
    if "unresolved_decisions" in changes:
        changes["unresolved_decisions"] = tuple(changes["unresolved_decisions"])
    if "ingest_options" in changes:
        private_options = changes["ingest_options"]
        if not isinstance(private_options, dict):
            raise TypeError(f"ingest_options for {spec.dataset_id} must be an object")
        changes["ingest_options"] = {
            **dict(spec.ingest_options),
            **private_options,
        }
    resolved = replace(spec, **changes)
    if resolved.readiness is DatasetReadiness.READY and resolved.unresolved_decisions:
        raise ValueError(
            f"Ready private dataset {spec.dataset_id!r} still has unresolved decisions"
        )
    return resolved


def load_registry(private_config: str | Path | None = None) -> Registry:
    """Resolve the public registry with an optional non-versioned JSON overlay."""

    selected = private_config or os.environ.get(PRIVATE_REGISTRY_ENV)
    if not selected:
        return BASE_REGISTRY
    path = Path(selected).expanduser().resolve(strict=True)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Private registry root must be a JSON object")
    unknown_ids = set(payload) - {spec.dataset_id for spec in BASE_REGISTRY}
    if unknown_ids:
        raise KeyError(
            f"Private registry contains unknown datasets: {sorted(unknown_ids)}"
        )
    return Registry(
        _overlay_spec(spec, payload.get(spec.dataset_id, {})) for spec in BASE_REGISTRY
    )


DEFAULT_REGISTRY = load_registry()
