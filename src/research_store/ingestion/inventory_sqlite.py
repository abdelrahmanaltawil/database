from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from importlib.metadata import PackageNotFoundError, version
from numbers import Integral, Real
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfoNotFoundError

import pandas as pd
from timezonefinder import TimezoneFinder

from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.conventions import signed_longitude
from research_store.foundation.models import DatasetSpec
from research_store.foundation.pipeline import ParsedChunk, ingest_file
from research_store.foundation.timezones import pinned_zoneinfo, timezone_data_version

VERSION = "1"


def _clean_identifier(value: Any) -> str | None:
    if pd.isna(value):
        return None
    if isinstance(value, Integral):
        return str(int(value))
    if isinstance(value, Real) and float(value).is_integer():
        return str(int(value))
    result = str(value).strip()
    return result or None


def _quoted_identifier(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError("Invalid SQLite identifier")
    return '"' + value.replace('"', '""') + '"'


def _timezone_source() -> str:
    package = version("timezonefinder")
    try:
        data = version("timezonefinder-data")
    except PackageNotFoundError:
        data = "bundled"
    return (
        f"timezonefinder {package}; timezonefinder-data {data}; "
        f"tzdata {timezone_data_version()}"
    )


def _derive_timezones(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    finder = TimezoneFinder(in_memory=True)
    names: list[str | None] = []
    for latitude, longitude in frame[["latitude", "longitude"]].itertuples(
        index=False, name=None
    ):
        if pd.isna(latitude) or pd.isna(longitude):
            names.append(None)
            continue
        name = finder.timezone_at(lng=float(longitude), lat=float(latitude))
        if name is None:
            names.append(None)
            continue
        try:
            pinned_zoneinfo(name)
        except ZoneInfoNotFoundError as error:
            raise ValueError(
                f"Timezone lookup returned unknown zone {name!r}"
            ) from error
        names.append(name)
    source = _timezone_source()
    sources = [source if name is not None else None for name in names]
    return (
        pd.Series(names, index=frame.index, dtype="string"),
        pd.Series(sources, index=frame.index, dtype="string"),
    )


def parse(path: Path, spec: DatasetSpec, completed: set[str]) -> Iterable[ParsedChunk]:
    options = spec.ingest_options
    table_name = options.get("table")
    column_map = dict(options.get("column_map") or {})
    longitude_convention = options.get("source_longitude_convention", "signed")
    if not table_name or not column_map:
        raise ValueError("SQLite inventory table name or column map is unresolved")

    source_cols = list(column_map.values())
    query = (
        "SELECT "
        + ", ".join(_quoted_identifier(name) for name in source_cols)
        + " FROM "
        + _quoted_identifier(str(table_name))
    )
    uri = f"file:{path.expanduser().resolve(strict=True)}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        available = {
            row[1]
            for row in connection.execute(
                f"PRAGMA table_info({_quoted_identifier(str(table_name))})"
            ).fetchall()
        }
        missing = set(source_cols) - available
        if missing:
            raise ValueError(f"SQLite columns are missing: {sorted(missing)}")
        source = pd.read_sql_query(query, connection, dtype=object)

    canonical = pd.DataFrame(
        {target: source[source_name] for target, source_name in column_map.items()}
    )
    canonical[spec.entity_field] = canonical[spec.entity_field].map(_clean_identifier)
    if canonical[spec.entity_field].isna().any():
        raise ValueError("Inventory entity identifiers must be non-empty")
    canonical[spec.entity_field] = canonical[spec.entity_field].astype("string")

    derived = set()
    if options.get("derive_timezone_from_coordinates"):
        derived = {"timezone_name", "timezone_source"}
    for variable in spec.variables:
        if variable.name in derived:
            continue
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
    if derived:
        canonical["timezone_name"], canonical["timezone_source"] = _derive_timezones(
            canonical
        )
    yield from chunks_from_frame(
        canonical,
        spec,
        key_prefix="inventory_sqlite",
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
