from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any

DATASET_ID = re.compile(r"^[a-z][a-z0-9_]*$")
FIELD_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class DatasetKind(StrEnum):
    EXTERNAL = "external"
    DERIVED = "derived"
    REFERENCE = "reference"


class StorageModel(StrEnum):
    LONG = "long"
    WIDE = "wide"
    REFERENCE = "reference"


class TemporalKind(StrEnum):
    INSTANT = "instant"
    INTERVAL = "interval"
    REFERENCE = "reference"


class DatasetReadiness(StrEnum):
    READY = "ready"
    PROVISIONAL = "provisional"


@dataclass(frozen=True, slots=True)
class VariableSpec:
    name: str
    quantity: str
    unit: str | None
    dtype: str = "float64"
    nullable: bool = True
    quality_field: str | None = None

    def __post_init__(self) -> None:
        if not FIELD_NAME.fullmatch(self.name):
            raise ValueError(f"Invalid variable name: {self.name!r}")
        if self.quality_field and not FIELD_NAME.fullmatch(self.quality_field):
            raise ValueError(f"Invalid quality field: {self.quality_field!r}")


@dataclass(frozen=True, slots=True)
class SentinelRule:
    """Publisher rule with an inclusive start and exclusive end."""

    marker: str
    meaning: str
    replacement: float | None
    start: str | None = None
    end: str | None = None
    evidence: str = ""

    def __post_init__(self) -> None:
        if self.meaning not in {"missing", "measured_zero"}:
            raise ValueError(f"Unsupported sentinel meaning: {self.meaning}")
        if self.meaning == "missing" and self.replacement is not None:
            raise ValueError("A missing sentinel must map to null")
        if self.meaning == "measured_zero" and self.replacement != 0.0:
            raise ValueError("A measured-zero sentinel must map to 0.0")


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    dataset_id: str
    description: str
    kind: DatasetKind
    producer: str
    storage_model: StorageModel
    temporal_kind: TemporalKind
    entity_field: str = "entity_id"
    time_start_field: str | None = "time_start"
    time_end_field: str | None = "time_end"
    native_frequency: str | None = None
    source_timezone: str | None = None
    canonical_timezone: str | None = "UTC"
    timestamp_semantics: str | None = None
    snapshot_mode: str = "append"
    variables: tuple[VariableSpec, ...] = ()
    partition_keys: tuple[str, ...] = ("year", "entity_bucket")
    entity_buckets: int = 64
    coordinate_convention: str | None = "EPSG:4326; longitude [-180, 180]"
    sentinel_rules: tuple[SentinelRule, ...] = ()
    readiness: DatasetReadiness = DatasetReadiness.READY
    ingest_options: Mapping[str, Any] = field(default_factory=dict)
    unresolved_decisions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not DATASET_ID.fullmatch(self.dataset_id):
            raise ValueError(f"Invalid dataset id: {self.dataset_id!r}")
        names = [variable.name for variable in self.variables]
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate variables in {self.dataset_id}")
        if self.storage_model is StorageModel.REFERENCE:
            if self.temporal_kind is not TemporalKind.REFERENCE:
                raise ValueError("Reference storage requires reference temporal kind")
        elif self.temporal_kind is TemporalKind.REFERENCE:
            raise ValueError("Time-series storage requires a temporal kind")
        if self.entity_buckets < 1:
            raise ValueError("entity_buckets must be positive")
        if self.snapshot_mode not in {"append", "replace"}:
            raise ValueError("snapshot_mode must be append or replace")
        if self.readiness is DatasetReadiness.READY:
            if not self.variables:
                raise ValueError(
                    f"Ready dataset {self.dataset_id!r} must declare variables"
                )
            unresolved_units = [
                variable.name
                for variable in self.variables
                if variable.dtype == "float64" and variable.unit is None
            ]
            if unresolved_units:
                raise ValueError(
                    f"Ready numeric variables must declare units: {sorted(unresolved_units)}"
                )
        object.__setattr__(
            self, "ingest_options", MappingProxyType(dict(self.ingest_options))
        )

    @property
    def variable_names(self) -> tuple[str, ...]:
        return tuple(variable.name for variable in self.variables)

    def variable(self, name: str) -> VariableSpec:
        for variable in self.variables:
            if variable.name == name:
                return variable
        raise KeyError(f"Unknown variable {name!r} for dataset {self.dataset_id!r}")

    def require_ready(self) -> None:
        if self.readiness is DatasetReadiness.PROVISIONAL:
            details = (
                "; ".join(self.unresolved_decisions) or "source metadata is incomplete"
            )
            raise RuntimeError(
                f"Dataset {self.dataset_id!r} is provisional and cannot be used: {details}"
            )

    def serializable(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "description": self.description,
            "kind": self.kind.value,
            "producer": self.producer,
            "storage_model": self.storage_model.value,
            "temporal_kind": self.temporal_kind.value,
            "entity_field": self.entity_field,
            "time_start_field": self.time_start_field,
            "time_end_field": self.time_end_field,
            "native_frequency": self.native_frequency,
            "source_timezone": self.source_timezone,
            "canonical_timezone": self.canonical_timezone,
            "timestamp_semantics": self.timestamp_semantics,
            "snapshot_mode": self.snapshot_mode,
            "variables": [asdict(variable) for variable in self.variables],
            "partition_keys": list(self.partition_keys),
            "entity_buckets": self.entity_buckets,
            "coordinate_convention": self.coordinate_convention,
            "sentinel_rules": [asdict(rule) for rule in self.sentinel_rules],
            "readiness": self.readiness.value,
            "ingest_options": dict(self.ingest_options),
            "unresolved_decisions": list(self.unresolved_decisions),
        }


class Registry:
    """The sole declaration site for dataset layout and semantics."""

    def __init__(self, specs: Iterable[DatasetSpec]):
        by_id: dict[str, DatasetSpec] = {}
        for spec in specs:
            if spec.dataset_id in by_id:
                raise ValueError(f"Dataset declared more than once: {spec.dataset_id}")
            by_id[spec.dataset_id] = spec
        self._specs = MappingProxyType(by_id)

    def __iter__(self):
        return iter(self._specs.values())

    def __len__(self) -> int:
        return len(self._specs)

    def get(self, dataset_id: str) -> DatasetSpec:
        try:
            return self._specs[dataset_id]
        except KeyError as error:
            known = ", ".join(sorted(self._specs))
            raise KeyError(
                f"Unknown dataset {dataset_id!r}; known datasets: {known}"
            ) from error

    @property
    def digest(self) -> str:
        payload = [
            spec.serializable()
            for spec in sorted(self, key=lambda item: item.dataset_id)
        ]
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()
