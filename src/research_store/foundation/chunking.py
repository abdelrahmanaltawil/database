from __future__ import annotations

from collections.abc import Iterable

import pandas as pd
import pyarrow as pa

from research_store.foundation.models import DatasetSpec, TemporalKind
from research_store.foundation.partitioning import entity_bucket
from research_store.foundation.pipeline import ParsedChunk


def chunks_from_frame(
    frame: pd.DataFrame,
    spec: DatasetSpec,
    *,
    key_prefix: str,
    completed: set[str] | None = None,
) -> Iterable[ParsedChunk]:
    """Split a canonical frame by the registry partition declaration."""

    completed = completed or set()
    if frame.empty:
        return
    if spec.temporal_kind is TemporalKind.REFERENCE:
        groups = [((), frame)]
    else:
        if spec.time_start_field is None:
            raise ValueError("Time-series dataset has no start field")
        frame = frame.copy()
        frame["__year"] = frame[spec.time_start_field].dt.year.astype(int)
        frame["__entity_bucket"] = frame[spec.entity_field].map(
            lambda value: entity_bucket(value, spec.entity_buckets)
        )
        groups = frame.groupby(["__year", "__entity_bucket"], sort=True, dropna=False)

    for group_key, group in groups:
        if spec.temporal_kind is TemporalKind.REFERENCE:
            partition: dict[str, int] = {}
            suffix = "reference"
        else:
            year, bucket = group_key
            partition = {"year": int(year), "entity_bucket": int(bucket)}
            suffix = f"year={year}:bucket={bucket}"
            group = group.drop(columns=["__year", "__entity_bucket"])
        chunk_key = f"{key_prefix}:{suffix}"
        if chunk_key in completed:
            continue
        yield ParsedChunk(
            chunk_key=chunk_key,
            table=pa.Table.from_pandas(
                group.reset_index(drop=True), preserve_index=False
            ),
            partition=partition,
        )
