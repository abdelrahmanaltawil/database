from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.models import DatasetSpec
from research_store.foundation.pipeline import ParsedChunk, ingest_file

VERSION = "1"


def parse(path: Path, spec: DatasetSpec, completed: set[str]) -> Iterable[ParsedChunk]:
    try:
        import xarray as xr
    except ImportError as error:
        raise RuntimeError(
            "Install the 'netcdf' optional dependency to ingest NetCDF"
        ) from error

    options = spec.ingest_options
    method = options.get("sampling_method")
    targets_path = options.get("target_registry")
    variable_map = dict(options.get("variable_map") or {})
    time_coordinate = options.get("time_coordinate", "time")
    if method not in {"nearest_point", "area_mean"}:
        raise ValueError(
            "sampling_method must be an explicit nearest_point or area_mean decision"
        )
    if method == "area_mean":
        raise NotImplementedError(
            "Area aggregation requires approved polygon, cell-area, and rotated-CRS conventions"
        )
    if not targets_path or not variable_map:
        raise ValueError("NetCDF target registry and variable map are unresolved")
    target_document = json.loads(Path(str(targets_path)).read_text())
    targets = target_document.get("targets", [])
    if not targets:
        raise ValueError("Target registry contains no approved targets")
    duration = pd.Timedelta(spec.native_frequency)

    with xr.open_dataset(path, decode_times=True) as dataset:
        missing = set(variable_map.values()) - set(dataset.data_vars)
        if missing:
            raise ValueError(f"NetCDF variables are missing: {sorted(missing)}")
        for target_number, target in enumerate(targets, start=1):
            entity = target.get("entity_id")
            indexers = target.get("grid_coordinates")
            if not isinstance(entity, str) or not entity:
                raise TypeError("Every reanalysis target must have a string entity_id")
            if not isinstance(indexers, dict) or not indexers:
                raise ValueError("Each target needs approved rotated-grid coordinates")
            selected = dataset[list(variable_map.values())].sel(
                indexers, method="nearest"
            )
            frame = selected.to_dataframe().reset_index()
            if time_coordinate not in frame:
                raise ValueError(
                    f"NetCDF time coordinate {time_coordinate!r} is absent"
                )
            canonical = pd.DataFrame({spec.entity_field: [entity] * len(frame)})
            labelled = pd.to_datetime(frame[time_coordinate], utc=True, errors="raise")
            if spec.timestamp_semantics == "interval_end":
                canonical[spec.time_start_field] = labelled - duration
                canonical[spec.time_end_field] = labelled
            elif spec.timestamp_semantics == "interval_start":
                canonical[spec.time_start_field] = labelled
                canonical[spec.time_end_field] = labelled + duration
            else:
                raise ValueError("NetCDF timestamp semantics are unresolved")
            for canonical_name, source_name in variable_map.items():
                expected_unit = spec.variable(canonical_name).unit
                source_unit = dataset[source_name].attrs.get("units")
                if expected_unit != source_unit:
                    raise ValueError(
                        f"Unit mismatch for {canonical_name}: registry={expected_unit!r}, "
                        f"NetCDF={source_unit!r}"
                    )
                canonical[canonical_name] = pd.to_numeric(
                    frame[source_name], errors="raise"
                ).astype("float64")
            yield from chunks_from_frame(
                canonical,
                spec,
                key_prefix=f"target={target_number}:{entity}",
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
