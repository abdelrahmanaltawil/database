from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.conventions import utc_timestamps
from research_store.foundation.models import DatasetSpec
from research_store.foundation.pipeline import ParsedChunk, ingest_file

VERSION = "2"


def _frames(path: Path, options: dict[str, Any] | Any) -> Iterator[pd.DataFrame]:
    source_format = options.get("format")
    entity_column = options.get("entity_column")
    if source_format == "csv":
        yield from pd.read_csv(
            path,
            chunksize=int(options.get("rows_per_batch", 250_000)),
            dtype={str(entity_column): "string"},
            na_values=list(options.get("na_values", [])),
            keep_default_na=True,
        )
    elif source_format == "parquet":
        parquet = pq.ParquetFile(path)
        if entity_column and not pa.types.is_string(
            parquet.schema_arrow.field(str(entity_column)).type
        ):
            raise TypeError("SCADA entity column in Parquet must already be a string")
        for batch in parquet.iter_batches(
            batch_size=int(options.get("rows_per_batch", 250_000))
        ):
            yield batch.to_pandas()
    else:
        raise ValueError("SCADA registry format must be 'csv' or 'parquet'")


def parse(path: Path, spec: DatasetSpec, completed: set[str]) -> Iterable[ParsedChunk]:
    options = spec.ingest_options
    entity_column = options.get("entity_column")
    timestamp_column = options.get("timestamp_column")
    column_map = dict(options.get("column_map") or {})
    if not entity_column or not timestamp_column or not column_map:
        raise ValueError("SCADA entity, timestamp, and column map are unresolved")
    if spec.source_timezone is None:
        raise ValueError("SCADA source timezone is unresolved")
    if spec.timestamp_semantics not in {"interval_start", "interval_end"}:
        raise ValueError(
            "SCADA timestamp_semantics must be interval_start or interval_end"
        )
    duration = pd.Timedelta(spec.native_frequency)
    required_targets = set(spec.variable_names)
    required_targets.update(
        variable.quality_field for variable in spec.variables if variable.quality_field
    )
    missing_map = required_targets - set(column_map)
    if missing_map:
        raise ValueError(f"SCADA column map is incomplete: {sorted(missing_map)}")

    for batch_number, source in enumerate(_frames(path, options), start=1):
        required_sources = {
            str(entity_column),
            str(timestamp_column),
            *column_map.values(),
        }
        missing = required_sources - set(source.columns)
        if missing:
            raise ValueError(f"SCADA source columns are missing: {sorted(missing)}")
        canonical = pd.DataFrame()
        canonical[spec.entity_field] = source[str(entity_column)].astype("string")
        if canonical[spec.entity_field].isna().any():
            raise ValueError("SCADA contains a missing entity identifier")
        labelled = utc_timestamps(
            source[str(timestamp_column)], source_timezone=spec.source_timezone
        )
        if spec.timestamp_semantics == "interval_start":
            canonical[spec.time_start_field] = labelled
            canonical[spec.time_end_field] = labelled + duration
        else:
            canonical[spec.time_start_field] = labelled - duration
            canonical[spec.time_end_field] = labelled
        for target, source_name in column_map.items():
            if target in spec.variable_names:
                variable = spec.variable(target)
                if variable.dtype == "float64":
                    canonical[target] = pd.to_numeric(
                        source[source_name], errors="raise"
                    ).astype("float64")
                elif variable.dtype == "string":
                    canonical[target] = source[source_name].astype("string")
                else:
                    raise TypeError(
                        f"Unsupported SCADA dtype for {target}: {variable.dtype}"
                    )
            else:
                canonical[target] = source[source_name].astype("string")
        yield from chunks_from_frame(
            canonical,
            spec,
            key_prefix=f"scada-batch={batch_number}",
            completed=completed,
        )


def ingest(dataset_id: str, source_path: str | Path, **kwargs: Any) -> str:
    return ingest_file(
        dataset_id=dataset_id,
        source_path=source_path,
        parser=parse,
        ingester_version=VERSION,
        **kwargs,
    )
