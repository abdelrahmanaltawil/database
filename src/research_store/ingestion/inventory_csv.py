from __future__ import annotations

import csv
from collections.abc import Iterable
from numbers import Integral, Real
from pathlib import Path
from typing import Any

import pandas as pd

from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.conventions import signed_longitude
from research_store.foundation.models import DatasetSpec
from research_store.foundation.pipeline import ParsedChunk, ingest_file

VERSION = "2"


def _clean_identifier(value: Any) -> str | None:
    if pd.isna(value):
        return None
    if isinstance(value, Integral):
        return str(int(value))
    if isinstance(value, Real) and float(value).is_integer():
        return str(int(value))
    result = str(value).strip()
    return result or None


def _header_row(frame: pd.DataFrame, identifier: str) -> int:
    for index, row in frame.iterrows():
        if any(str(value).strip() == identifier for value in row if pd.notna(value)):
            return int(index)
    raise ValueError(f"Could not find real table header containing {identifier!r}")


def _read_source(path: Path, options: dict[str, Any] | Any) -> pd.DataFrame:
    source_format = str(options.get("format") or path.suffix.lstrip(".")).lower()
    header_identifier = str(options.get("header_identifier") or "")
    if not header_identifier:
        raise ValueError("Inventory header identifier is unresolved")
    if source_format == "xlsx":
        sheet_name = options.get("sheet_name", 0)
        preview = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=object)
        header = _header_row(preview, header_identifier)
        return pd.read_excel(path, sheet_name=sheet_name, header=header, dtype=object)
    if source_format == "csv":
        encoding = str(options.get("encoding", "utf-8-sig"))
        with path.open("rt", encoding=encoding, newline="") as stream:
            header = next(
                (
                    index
                    for index, row in enumerate(csv.reader(stream))
                    if any(value.strip() == header_identifier for value in row)
                ),
                None,
            )
        if header is None:
            raise ValueError(
                f"Could not find real table header containing {header_identifier!r}"
            )
        return pd.read_csv(path, header=header, dtype=object, encoding=encoding)
    raise ValueError("Inventory registry format must be 'csv' or 'xlsx'")


def parse(path: Path, spec: DatasetSpec, completed: set[str]) -> Iterable[ParsedChunk]:
    options = spec.ingest_options
    column_map = dict(options.get("column_map") or {})
    longitude_convention = options.get("source_longitude_convention")
    if not column_map or not longitude_convention:
        raise ValueError("Inventory column map or coordinate convention is unresolved")

    source = _read_source(path, options)
    missing = set(column_map.values()) - set(source.columns)
    if missing:
        raise ValueError(f"Inventory source columns are missing: {sorted(missing)}")

    canonical = pd.DataFrame(
        {target: source[source_name] for target, source_name in column_map.items()}
    )
    canonical[spec.entity_field] = canonical[spec.entity_field].map(_clean_identifier)
    if canonical[spec.entity_field].isna().any():
        raise ValueError("Inventory entity identifiers must be non-empty")
    canonical[spec.entity_field] = canonical[spec.entity_field].astype("string")

    for variable in spec.variables:
        if variable.name not in canonical:
            raise ValueError(f"Inventory variable is not mapped: {variable.name}")
        if variable.dtype == "string":
            canonical[variable.name] = canonical[variable.name].map(_clean_identifier)
            canonical[variable.name] = canonical[variable.name].astype("string")
        elif variable.dtype == "float64":
            canonical[variable.name] = pd.to_numeric(
                canonical[variable.name], errors="raise"
            ).astype("float64")
        else:
            raise TypeError(
                f"Unsupported inventory dtype for {variable.name}: {variable.dtype}"
            )

    canonical["longitude"] = canonical["longitude"].map(
        lambda value: (
            value
            if pd.isna(value)
            else signed_longitude(float(value), str(longitude_convention))
        )
    )
    yield from chunks_from_frame(
        canonical,
        spec,
        key_prefix="inventory",
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
