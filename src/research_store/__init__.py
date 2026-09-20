"""Public API for the research data store."""

from research_store.access.api import connect, describe, load
from research_store.foundation.models import (
    AnnotationSpec,
    DatasetKind,
    DatasetReadiness,
    DatasetSpec,
    DocumentationSpec,
    PlausibleBand,
    Registry,
    SentinelRule,
    StorageModel,
    TemporalKind,
    VariableSpec,
)

# Reading: connect and load return data, describe returns the declaration
# behind it. The spec vocabulary is exported too, so a consumer can type
# against the declaration without reaching into research_store.foundation.
__all__ = [
    "AnnotationSpec",
    "DatasetKind",
    "DatasetReadiness",
    "DatasetSpec",
    "DocumentationSpec",
    "PlausibleBand",
    "Registry",
    "SentinelRule",
    "StorageModel",
    "TemporalKind",
    "VariableSpec",
    "connect",
    "describe",
    "load",
]
__version__ = "0.1.0"
