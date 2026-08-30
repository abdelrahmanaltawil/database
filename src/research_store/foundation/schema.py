from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from research_store.foundation.models import DatasetSpec, StorageModel, TemporalKind


def _require_field(table: pa.Table, name: str) -> pa.Field:
    try:
        return table.schema.field(name)
    except KeyError as error:
        raise ValueError(f"Required field is missing: {name}") from error


def _validate_timestamp(field: pa.Field, name: str) -> None:
    if not pa.types.is_timestamp(field.type):
        raise TypeError(f"{name} must be an Arrow timestamp, got {field.type}")
    if field.type.tz != "UTC":
        raise TypeError(f"{name} must have UTC timezone metadata, got {field.type}")


def validate_table(table: pa.Table, spec: DatasetSpec) -> None:
    """Validate canonical data before it can become reader-visible."""

    if table.num_rows == 0:
        raise ValueError("Empty chunks are not publishable")
    entity = _require_field(table, spec.entity_field)
    if not (pa.types.is_string(entity.type) or pa.types.is_large_string(entity.type)):
        raise TypeError(
            f"{spec.entity_field} must be a string so leading zeroes survive, got {entity.type}"
        )
    entity_values = table.column(spec.entity_field)
    if entity_values.null_count:
        raise ValueError("Entity identifiers may not be null")
    if pc.any(pc.equal(entity_values, "")).as_py():
        raise ValueError("Entity identifiers may not be empty")

    if spec.temporal_kind is not TemporalKind.REFERENCE:
        if spec.time_start_field is None:
            raise ValueError("Time-series dataset has no time_start_field")
        _validate_timestamp(
            _require_field(table, spec.time_start_field), spec.time_start_field
        )
        if table.column(spec.time_start_field).null_count:
            raise ValueError("Observation start timestamps may not be null")
        if spec.temporal_kind is TemporalKind.INTERVAL:
            if spec.time_end_field is None:
                raise ValueError("Interval dataset has no time_end_field")
            _validate_timestamp(
                _require_field(table, spec.time_end_field), spec.time_end_field
            )
            if table.column(spec.time_end_field).null_count:
                raise ValueError("Observation end timestamps may not be null")
            valid_intervals = pc.greater(
                table.column(spec.time_end_field), table.column(spec.time_start_field)
            )
            if not pc.all(valid_intervals).as_py():
                raise ValueError("Every interval end must be later than its start")

    declared = {variable.name: variable for variable in spec.variables}
    if spec.storage_model is StorageModel.LONG:
        variable_field = _require_field(table, "variable")
        if not (
            pa.types.is_string(variable_field.type)
            or pa.types.is_large_string(variable_field.type)
        ):
            raise TypeError("Long-form variable identifiers must be strings")
        value_field = _require_field(table, "value")
        if not pa.types.is_float64(value_field.type):
            raise TypeError(
                f"Long-form value must remain float64, got {value_field.type}"
            )
        observed = set(table.column("variable").to_pylist())
        if None in observed:
            raise ValueError("Long-form variable identifiers may not be null")
        unknown = observed - set(declared)
        if unknown:
            raise ValueError(f"Undeclared variables: {sorted(unknown)}")
        if "source_element" in table.column_names:
            source_element = _require_field(table, "source_element")
            if not (
                pa.types.is_string(source_element.type)
                or pa.types.is_large_string(source_element.type)
            ):
                raise TypeError("source_element must be a string")
            if table.column("source_element").null_count:
                raise ValueError("source_element may not be null")
    else:
        for name, variable in declared.items():
            field = _require_field(table, name)
            if variable.dtype == "float64" and not pa.types.is_float64(field.type):
                raise TypeError(f"{name} must remain float64, got {field.type}")
            if variable.dtype == "string" and not (
                pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
            ):
                raise TypeError(f"{name} must be string, got {field.type}")
            if variable.quality_field:
                quality = _require_field(table, variable.quality_field)
                if not (
                    pa.types.is_string(quality.type)
                    or pa.types.is_large_string(quality.type)
                ):
                    raise TypeError(
                        f"{variable.quality_field} must be string, got {quality.type}"
                    )

    key_fields = {spec.entity_field}
    if spec.time_start_field:
        key_fields.add(spec.time_start_field)
    if spec.time_end_field:
        key_fields.add(spec.time_end_field)
    if spec.storage_model is StorageModel.LONG:
        allowed = key_fields | {
            "variable",
            "value",
            "quality_flag",
            "source_element",
        }
    else:
        allowed = key_fields | set(declared)
        allowed.update(
            variable.quality_field
            for variable in declared.values()
            if variable.quality_field
        )
    extra = set(table.column_names) - allowed
    if extra:
        raise ValueError(f"Columns not declared by the registry: {sorted(extra)}")


def validate_no_duplicate_observations(table: pa.Table, spec: DatasetSpec) -> None:
    if spec.temporal_kind is TemporalKind.REFERENCE:
        keys = [spec.entity_field]
    else:
        keys = [spec.entity_field, spec.time_start_field]
        if spec.temporal_kind is TemporalKind.INTERVAL and spec.time_end_field:
            keys.append(spec.time_end_field)
        if spec.storage_model is StorageModel.LONG:
            keys.append("variable")
    frame = table.select(keys).to_pandas()
    duplicates = frame.duplicated(keep=False)
    if duplicates.any():
        sample = frame.loc[duplicates].head(3).to_dict(orient="records")
        raise ValueError(
            f"Duplicate observation keys are not allowed; examples: {sample}"
        )
