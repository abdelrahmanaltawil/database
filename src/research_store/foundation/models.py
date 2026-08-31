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
class PlausibleBand:
    """A robust distribution gate that catches unit and scale mistakes.

    A single absurd reading is legitimate in an un-quality-controlled archive,
    so this constrains one quantile of the published distribution rather than
    any individual observation. A scale factor that is wrong by a power of ten
    moves the whole distribution and is caught; a broken sensor is not.
    """

    quantile: float
    minimum: float
    maximum: float
    evidence: str = ""

    def __post_init__(self) -> None:
        if not 0.0 <= self.quantile <= 1.0:
            raise ValueError(f"Quantile must lie in [0, 1]: {self.quantile}")
        if self.minimum >= self.maximum:
            raise ValueError(
                f"Plausible band minimum must be below its maximum: "
                f"{self.minimum} >= {self.maximum}"
            )
        if not self.evidence.strip():
            raise ValueError(
                "A plausible band must cite the physical evidence for its bounds"
            )

    def describe(self) -> str:
        return (
            f"quantile {self.quantile:g} within [{self.minimum:g}, {self.maximum:g}]"
        )


@dataclass(frozen=True, slots=True)
class VariableSpec:
    name: str
    quantity: str
    unit: str | None
    dtype: str = "float64"
    nullable: bool = True
    quality_field: str | None = None
    plausible_band: PlausibleBand | None = None

    def __post_init__(self) -> None:
        if not FIELD_NAME.fullmatch(self.name):
            raise ValueError(f"Invalid variable name: {self.name!r}")
        if self.quality_field and not FIELD_NAME.fullmatch(self.quality_field):
            raise ValueError(f"Invalid quality field: {self.quality_field!r}")
        if self.plausible_band is not None and self.dtype != "float64":
            raise ValueError(
                f"Only numeric variables can declare a plausible band: {self.name!r}"
            )


@dataclass(frozen=True, slots=True)
class AnnotationSpec:
    """A non-measurement source field retained beside each observation."""

    name: str
    meaning: str
    dtype: str = "string"

    def __post_init__(self) -> None:
        if not FIELD_NAME.fullmatch(self.name):
            raise ValueError(f"Invalid annotation name: {self.name!r}")
        if not self.meaning.strip():
            raise ValueError("Annotation meaning must not be blank")
        if self.dtype not in {"string", "float64"}:
            raise ValueError(f"Unsupported annotation dtype: {self.dtype!r}")


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
class DocumentationSpec:
    """Non-transformative source documentation and quality interpretation."""

    source_format: str
    references: tuple[str, ...]
    quality_control: str
    quality_flags: Mapping[str, str] = field(default_factory=dict)
    limitations: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_format.strip():
            raise ValueError("Documentation source_format must not be blank")
        if not self.references or any(not item.strip() for item in self.references):
            raise ValueError("Documentation references must contain non-blank evidence")
        if not self.quality_control.strip():
            raise ValueError("Documentation quality_control must not be blank")
        if any(not code or not meaning.strip() for code, meaning in self.quality_flags.items()):
            raise ValueError("Quality flags require non-blank codes and meanings")
        if any(not item.strip() for item in (*self.limitations, *self.notes)):
            raise ValueError("Documentation limitations and notes must not be blank")
        object.__setattr__(
            self, "quality_flags", MappingProxyType(dict(self.quality_flags))
        )

    def serializable(self) -> dict[str, Any]:
        return {
            "source_format": self.source_format,
            "references": list(self.references),
            "quality_control": self.quality_control,
            "quality_flags": dict(self.quality_flags),
            "limitations": list(self.limitations),
            "notes": list(self.notes),
        }


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
    annotations: tuple[AnnotationSpec, ...] = ()
    documentation: DocumentationSpec | None = None
    readiness: DatasetReadiness = DatasetReadiness.READY
    ingest_options: Mapping[str, Any] = field(default_factory=dict)
    unresolved_decisions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not DATASET_ID.fullmatch(self.dataset_id):
            raise ValueError(f"Invalid dataset id: {self.dataset_id!r}")
        names = [variable.name for variable in self.variables]
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate variables in {self.dataset_id}")
        annotation_names = [annotation.name for annotation in self.annotations]
        if len(annotation_names) != len(set(annotation_names)):
            raise ValueError(f"Duplicate annotations in {self.dataset_id}")
        collisions = set(names) & set(annotation_names)
        if collisions:
            raise ValueError(
                f"Variables and annotations share names in {self.dataset_id}: "
                f"{sorted(collisions)}"
            )
        if self.storage_model is StorageModel.REFERENCE:
            if self.temporal_kind is not TemporalKind.REFERENCE:
                raise ValueError("Reference storage requires reference temporal kind")
        elif self.temporal_kind is TemporalKind.REFERENCE:
            raise ValueError("Time-series storage requires a temporal kind")
        if self.storage_model is StorageModel.LONG and self.annotations:
            raise ValueError("Observation annotations currently require wide storage")
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
            "annotations": [asdict(annotation) for annotation in self.annotations],
            "documentation": (
                self.documentation.serializable() if self.documentation else None
            ),
            "readiness": self.readiness.value,
            "ingest_options": dict(self.ingest_options),
            "unresolved_decisions": list(self.unresolved_decisions),
        }

    def identity_serializable(self) -> dict[str, Any]:
        """Fields that can change parsed output or its analytical meaning."""

        payload = self.serializable()
        # Documentation explains a source and a completed ingest but does not
        # alter parsed values. Updating citations or QC interpretation must not
        # create a second append identity for already ingested raw bytes.
        payload.pop("documentation")
        # A plausible band rejects an implausible publication; it never changes
        # a value that passes. Tightening a band must not re-key finished runs.
        for variable in payload["variables"]:
            variable.pop("plausible_band", None)
        return payload

    @property
    def identity_digest(self) -> str:
        """This dataset's own identity, independent of every other dataset.

        Run identity is keyed on this rather than on a whole-registry digest so
        that declaring an unrelated dataset cannot invalidate finished runs.
        """

        encoded = json.dumps(
            self.identity_serializable(), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


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
        """A whole-registry fingerprint, recorded on runs for provenance only.

        This changes whenever any dataset is declared or edited, so it must not
        be used to decide whether a given ingest has already happened.
        """

        payload = [
            spec.identity_serializable()
            for spec in sorted(self, key=lambda item: item.dataset_id)
        ]
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    def dataset_digest(self, dataset_id: str) -> str:
        """The identity that decides whether one dataset's ingest is a repeat."""

        return self.get(dataset_id).identity_digest
