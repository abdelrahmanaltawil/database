from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from research_store.foundation.catalog import Catalog
from research_store.foundation.models import Registry, StorageModel, TemporalKind
from research_store.foundation.partitioning import entity_bucket
from research_store.foundation.paths import resolve_store_paths
from research_store.foundation.registry import DEFAULT_REGISTRY


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _parquet_relation(paths: Sequence[str]) -> str:
    if not paths:
        raise ValueError("At least one committed Parquet fragment is required")
    values = ", ".join(_sql_string(path) for path in paths)
    return f"read_parquet([{values}], union_by_name = true, hive_partitioning = false)"


def _normalized_variables(
    variable: str | Sequence[str] | None, available: tuple[str, ...]
) -> list[str]:
    if variable is None:
        selected = list(available)
    elif isinstance(variable, str):
        selected = [variable]
    else:
        selected = list(variable)
    if not selected:
        raise ValueError("At least one variable must be selected")
    unknown = set(selected) - set(available)
    if unknown:
        raise KeyError(
            f"Unknown variables: {sorted(unknown)}; available: {sorted(available)}"
        )
    if len(selected) != len(set(selected)):
        raise ValueError("A variable may be requested only once")
    return selected


def _normalized_entities(entity: str | Sequence[str] | None) -> list[str] | None:
    if entity is None:
        return None
    values = [entity] if isinstance(entity, str) else list(entity)
    if any(not isinstance(value, str) for value in values):
        raise TypeError(
            "Entity identifiers must be strings; numeric casting is forbidden"
        )
    return values


def _utc(value: str | datetime | pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None:
        return None
    result = pd.Timestamp(value)
    if result.tzinfo is None:
        result = result.tz_localize("UTC")
    else:
        result = result.tz_convert("UTC")
    return result


def _partition_filter(spec, entities, start, end) -> dict[str, set[Any]]:
    result: dict[str, set[Any]] = {}
    if "entity_bucket" in spec.partition_keys and entities:
        result["entity_bucket"] = {
            entity_bucket(value, spec.entity_buckets) for value in entities
        }
    if "year" in spec.partition_keys and (start is not None or end is not None):
        first = start.year if start is not None else 1
        if end is None:
            last = datetime.now(UTC).year + 1
        else:
            last = (end - pd.Timedelta(nanoseconds=1)).year
        if last < first:
            raise ValueError("end must be later than start")
        result["year"] = set(range(first, last + 1))
    return result


def _logical_select(
    spec, relation: str, selected: list[str], *, include_provenance: bool
) -> str:
    entity = _identifier(spec.entity_field)
    keys = [entity]
    if spec.temporal_kind is not TemporalKind.REFERENCE:
        keys.append(_identifier(spec.time_start_field))
        if spec.time_end_field:
            keys.append(_identifier(spec.time_end_field))

    if spec.storage_model is StorageModel.LONG:
        expressions = list(keys)
        for name in selected:
            literal = _sql_string(name)
            expressions.append(
                f"max(value) FILTER (WHERE variable = {literal}) AS {_identifier(name)}"
            )
            quality = spec.variable(name).quality_field
            if quality:
                expressions.append(
                    f"max(quality_flag) FILTER (WHERE variable = {literal}) "
                    f"AS {_identifier(quality)}"
                )
        if include_provenance:
            expressions.extend(["_source_id", "_producer_run_id"])
        group_keys = list(keys)
        if include_provenance:
            group_keys.extend(["_source_id", "_producer_run_id"])
        return (
            f"SELECT {', '.join(expressions)} FROM {relation} "
            f"GROUP BY {', '.join(group_keys)}"
        )

    expressions = list(keys)
    for name in selected:
        expressions.append(_identifier(name))
        quality = spec.variable(name).quality_field
        if quality:
            expressions.append(_identifier(quality))
    if include_provenance:
        expressions.extend(["_source_id", "_producer_run_id"])
    return f"SELECT {', '.join(expressions)} FROM {relation}"


def load(
    dataset: str,
    *,
    entity: str | Sequence[str] | None = None,
    variable: str | Sequence[str] | None = None,
    start: str | datetime | pd.Timestamp | None = None,
    end: str | datetime | pd.Timestamp | None = None,
    snapshot: str | None = None,
    include_provenance: bool = False,
    store: str | Path | None = None,
    registry: Registry = DEFAULT_REGISTRY,
) -> pd.DataFrame:
    """Load one logical, unit-safe dataframe from committed fragments only.

    `start` is inclusive and `end` is exclusive. Variables are returned as
    separate named columns, so quantities with different units never share a
    generic value column.
    """

    spec = registry.get(dataset)
    spec.require_ready()
    selected = _normalized_variables(variable, spec.variable_names)
    entities = _normalized_entities(entity)
    start_at = _utc(start)
    end_at = _utc(end)
    if start_at is not None and end_at is not None and end_at <= start_at:
        raise ValueError("end must be later than start")

    paths = resolve_store_paths(store)
    if not paths.catalog.exists():
        raise FileNotFoundError(f"Store catalogue does not exist: {paths.catalog}")
    catalog = Catalog(paths)
    filters = _partition_filter(spec, entities, start_at, end_at)
    chosen_snapshot, fragments = catalog.committed_fragments(
        dataset, snapshot, partition_filter=filters
    )
    relation = _parquet_relation(fragments)
    logical = _logical_select(
        spec, relation, selected, include_provenance=include_provenance
    )

    conditions: list[str] = []
    parameters: list[Any] = []
    if entities:
        placeholders = ", ".join("?" for _ in entities)
        conditions.append(f"{_identifier(spec.entity_field)} IN ({placeholders})")
        parameters.extend(entities)
    if spec.temporal_kind is not TemporalKind.REFERENCE:
        time_field = _identifier(spec.time_start_field)
        if start_at is not None:
            conditions.append(f"{time_field} >= ?")
            parameters.append(start_at.to_pydatetime())
        if end_at is not None:
            conditions.append(f"{time_field} < ?")
            parameters.append(end_at.to_pydatetime())
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    order = [_identifier(spec.entity_field)]
    if spec.temporal_kind is not TemporalKind.REFERENCE:
        order.append(_identifier(spec.time_start_field))
    query = f"SELECT * FROM ({logical}) AS logical{where} ORDER BY {', '.join(order)}"
    with duckdb.connect() as connection:
        frame = connection.execute(query, parameters).fetchdf()

    frame[spec.entity_field] = frame[spec.entity_field].astype("string")
    for name in selected:
        variable_spec = spec.variable(name)
        if variable_spec.dtype == "float64":
            frame[name] = frame[name].astype("float64")
        if variable_spec.quality_field and variable_spec.quality_field in frame:
            frame[variable_spec.quality_field] = frame[
                variable_spec.quality_field
            ].astype("string")
    frame.attrs.update(
        {
            "dataset_id": dataset,
            "snapshot_id": chosen_snapshot,
            "units": {name: spec.variable(name).unit for name in selected},
            "start_inclusive": True,
            "end_inclusive": False,
        }
    )
    return frame


def connect(
    *,
    store: str | Path | None = None,
    registry: Registry = DEFAULT_REGISTRY,
) -> duckdb.DuckDBPyConnection:
    """Return an in-memory SQL client with a read-only catalogue and safe views."""

    paths = resolve_store_paths(store)
    if not paths.catalog.exists():
        raise FileNotFoundError(f"Store catalogue does not exist: {paths.catalog}")
    connection = duckdb.connect()
    catalog_path = _sql_string(str(paths.catalog))
    connection.execute(f"ATTACH {catalog_path} AS catalog (READ_ONLY)")
    catalog = Catalog(paths)
    for spec in registry:
        try:
            _, fragments = catalog.committed_fragments(spec.dataset_id)
        except LookupError:
            continue
        relation = _parquet_relation(fragments)
        selected = list(spec.variable_names)
        logical = _logical_select(spec, relation, selected, include_provenance=True)
        connection.execute(f"CREATE VIEW {_identifier(spec.dataset_id)} AS {logical}")
    return connection
