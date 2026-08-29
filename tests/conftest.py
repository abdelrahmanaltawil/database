from __future__ import annotations

from pathlib import Path

import pytest

from research_store.foundation.models import (
    DatasetKind,
    DatasetSpec,
    Registry,
    StorageModel,
    TemporalKind,
    VariableSpec,
)
from research_store.foundation.paths import StorePaths


@pytest.fixture
def wide_spec() -> DatasetSpec:
    return DatasetSpec(
        dataset_id="test_sensor",
        description="Synthetic dense sensor fixture",
        kind=DatasetKind.EXTERNAL,
        producer="synthetic",
        storage_model=StorageModel.WIDE,
        temporal_kind=TemporalKind.INTERVAL,
        native_frequency="10 minutes",
        source_timezone="UTC",
        timestamp_semantics="interval_start",
        variables=(
            VariableSpec("power", "active power", "kW", quality_field="power_quality"),
            VariableSpec("wind_speed", "wind speed", "m/s"),
        ),
        entity_buckets=8,
    )


@pytest.fixture
def long_spec() -> DatasetSpec:
    return DatasetSpec(
        dataset_id="test_long",
        description="Synthetic sparse fixture",
        kind=DatasetKind.EXTERNAL,
        producer="synthetic",
        storage_model=StorageModel.LONG,
        temporal_kind=TemporalKind.INTERVAL,
        native_frequency="1 hour",
        source_timezone="UTC",
        timestamp_semantics="interval_start",
        variables=(
            VariableSpec("rain", "precipitation amount", "mm"),
            VariableSpec("temperature", "air temperature", "degC"),
        ),
        entity_buckets=8,
    )


@pytest.fixture
def registry(wide_spec, long_spec) -> Registry:
    return Registry([wide_spec, long_spec])


@pytest.fixture
def store_paths(tmp_path: Path) -> StorePaths:
    return StorePaths(tmp_path / "store")
