"""Canonical dataset declarations.

This is deliberately the only file that declares dataset paths, variables,
units, layouts, partitions, and source conventions. Producers and readers both
consume these objects.
"""

from research_store.foundation.models import (
    DatasetKind,
    DatasetReadiness,
    DatasetSpec,
    Registry,
    StorageModel,
    TemporalKind,
    VariableSpec,
)

_PROVISIONAL = DatasetReadiness.PROVISIONAL


DEFAULT_REGISTRY = Registry(
    [
        DatasetSpec(
            dataset_id="weather_family_a",
            description="Fixed-width hourly national weather archive, instrument family A",
            kind=DatasetKind.EXTERNAL,
            producer="fixed_width_hourly",
            storage_model=StorageModel.LONG,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 hour",
            variables=(
                VariableSpec("precipitation_amount", "precipitation amount", "mm"),
            ),
            readiness=_PROVISIONAL,
            ingest_options={
                "station_slice": None,
                "date_slice": None,
                "element_slice": None,
                "values_start": None,
                "field_width": 7,
                "value_width": 6,
                "element_map": {},
                "scale": None,
            },
            unresolved_decisions=(
                "confirm fixed-width offsets and numeric scale from a real file",
                "record the element-code-to-variable mapping",
                "record the source time zone and interval labelling convention",
                "record the exact era-aware sentinel marker and publisher evidence",
            ),
        ),
        DatasetSpec(
            dataset_id="weather_family_b",
            description="Fixed-width hourly national weather archive, instrument family B",
            kind=DatasetKind.EXTERNAL,
            producer="fixed_width_hourly",
            storage_model=StorageModel.LONG,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 hour",
            variables=(
                VariableSpec("precipitation_amount", "precipitation amount", "mm"),
            ),
            readiness=_PROVISIONAL,
            ingest_options={
                "station_slice": None,
                "date_slice": None,
                "element_slice": None,
                "values_start": None,
                "field_width": 7,
                "value_width": 6,
                "element_map": {},
                "scale": None,
            },
            unresolved_decisions=(
                "confirm fixed-width offsets and numeric scale from a real file",
                "record the element-code-to-variable mapping",
                "record the source time zone and interval labelling convention",
                "record the exact era-aware sentinel marker and publisher evidence",
            ),
        ),
        DatasetSpec(
            dataset_id="hydrometric_flow_daily",
            description="Daily discharge values unpivoted from the national SQLite archive",
            kind=DatasetKind.EXTERNAL,
            producer="hydrometric_sqlite",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 day",
            snapshot_mode="replace",
            variables=(
                VariableSpec(
                    "discharge",
                    "volumetric flow rate",
                    "m3/s",
                    quality_field="discharge_quality",
                ),
            ),
            readiness=_PROVISIONAL,
            ingest_options={
                "table": None,
                "station_column": None,
                "year_column": None,
                "month_column": None,
                "value_prefix": None,
                "quality_prefix": None,
            },
            unresolved_decisions=(
                "confirm SQLite table and column names",
                "record quality-symbol meanings and source daily-time semantics",
            ),
        ),
        DatasetSpec(
            dataset_id="hydrometric_level_daily",
            description="Daily stage values unpivoted from the national SQLite archive",
            kind=DatasetKind.EXTERNAL,
            producer="hydrometric_sqlite",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 day",
            snapshot_mode="replace",
            variables=(
                VariableSpec(
                    "stage", "water level", "m", quality_field="stage_quality"
                ),
            ),
            readiness=_PROVISIONAL,
            ingest_options={
                "table": None,
                "station_column": None,
                "year_column": None,
                "month_column": None,
                "value_prefix": None,
                "quality_prefix": None,
            },
            unresolved_decisions=(
                "confirm SQLite table and column names",
                "record quality-symbol meanings and source daily-time semantics",
            ),
        ),
        DatasetSpec(
            dataset_id="station_inventory",
            description="Versioned station inventory read from a CSV with pre-header disclaimers",
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
                VariableSpec("latitude", "latitude", "degree_north"),
                VariableSpec("longitude", "longitude", "degree_east"),
                VariableSpec("elevation", "height above reference datum", "m"),
            ),
            partition_keys=(),
            readiness=_PROVISIONAL,
            ingest_options={
                "header_identifier": None,
                "column_map": {},
                "source_longitude_convention": None,
            },
            unresolved_decisions=(
                "confirm the true header identifier and source column names",
                "confirm coordinate reference system, datum, and longitude convention",
                "decide how station relocations are represented",
            ),
        ),
        DatasetSpec(
            dataset_id="reanalysis_points_hourly",
            description="Hourly reanalysis extracted from a rotated-pole grid at approved targets",
            kind=DatasetKind.EXTERNAL,
            producer="reanalysis_netcdf",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 hour",
            variables=(),
            readiness=_PROVISIONAL,
            ingest_options={
                "sampling_method": None,
                "target_registry": None,
                "variable_map": {},
            },
            unresolved_decisions=(
                "declare variables, units, and NetCDF coordinate names",
                "choose point sampling or area aggregation and preserve its evidence",
                "approve the target point or polygon registry",
            ),
        ),
        DatasetSpec(
            dataset_id="wind_scada_10min",
            description="Offshore turbine SCADA in its naturally dense wide representation",
            kind=DatasetKind.EXTERNAL,
            producer="scada_wide",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="10 minutes",
            source_timezone=None,
            variables=(
                VariableSpec("power", "active power", None),
                VariableSpec("wind_speed", "wind speed", None),
            ),
            readiness=_PROVISIONAL,
            ingest_options={
                "format": None,
                "entity_column": None,
                "timestamp_column": None,
                "column_map": {},
            },
            unresolved_decisions=(
                "confirm file format and all approximately 34 columns with units",
                "record timestamp timezone, interval labelling, and DST behaviour",
                "record meanings of missing, invalid, and not-installed sensor fields",
            ),
        ),
    ]
)
