from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.conventions import valid_calendar_day
from research_store.foundation.models import DatasetSpec
from research_store.foundation.pipeline import ParsedChunk, ingest_file

VERSION = "1"


def _quoted_identifier(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError("Invalid SQLite identifier")
    return '"' + value.replace('"', '""') + '"'


def parse(
    path: Path,
    spec: DatasetSpec,
    completed: set[str],
    *,
    rows_per_batch: int = 2_000,
) -> Iterable[ParsedChunk]:
    options = spec.ingest_options
    required = [
        "table",
        "station_column",
        "year_column",
        "month_column",
        "value_prefix",
        "quality_prefix",
    ]
    missing = [name for name in required if not options.get(name)]
    if missing:
        raise ValueError(f"Unresolved hydrometric registry options: {missing}")
    if spec.source_timezone is None:
        raise ValueError("source_timezone must be resolved in the registry")
    if len(spec.variables) != 1:
        raise ValueError(
            "Each hydrometric dataset must declare exactly one quantity/unit"
        )
    variable = spec.variables[0]
    if variable.quality_field is None:
        raise ValueError("Hydrometric variable must declare its matching quality field")
    table_name = str(options["table"])
    station = str(options["station_column"])
    year = str(options["year_column"])
    month = str(options["month_column"])
    value_columns = [f"{options['value_prefix']}{day}" for day in range(1, 32)]
    quality_columns = [f"{options['quality_prefix']}{day}" for day in range(1, 32)]
    selected = [station, year, month, *value_columns, *quality_columns]
    query = (
        "SELECT "
        + ", ".join(_quoted_identifier(name) for name in selected)
        + " FROM "
        + _quoted_identifier(table_name)
        + f" ORDER BY {_quoted_identifier(year)}, {_quoted_identifier(month)}, "
        + _quoted_identifier(station)
    )
    uri = f"file:{path}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        available = {
            row[1]
            for row in connection.execute(
                f"PRAGMA table_info({_quoted_identifier(table_name)})"
            ).fetchall()
        }
        absent = set(selected) - available
        if absent:
            raise ValueError(f"SQLite columns are missing: {sorted(absent)}")
        cursor = connection.execute(query)
        batch_number = 0
        while batch := cursor.fetchmany(rows_per_batch):
            batch_number += 1
            records: list[dict[str, Any]] = []
            for row in batch:
                entity = row[0]
                if not isinstance(entity, str):
                    raise TypeError(
                        f"Station id {entity!r} is not stored as text; refusing numeric autocast"
                    )
                source_year = int(row[1])
                source_month = int(row[2])
                for day in range(1, 32):
                    if not valid_calendar_day(source_year, source_month, day):
                        continue
                    raw_value = row[2 + day]
                    raw_quality = row[33 + day]
                    local_start = pd.Timestamp(source_year, source_month, day)
                    start = local_start.tz_localize(
                        spec.source_timezone, ambiguous="raise", nonexistent="raise"
                    ).tz_convert("UTC")
                    end = (
                        (local_start + timedelta(days=1))
                        .tz_localize(
                            spec.source_timezone, ambiguous="raise", nonexistent="raise"
                        )
                        .tz_convert("UTC")
                    )
                    records.append(
                        {
                            spec.entity_field: entity,
                            spec.time_start_field: start,
                            spec.time_end_field: end,
                            variable.name: None
                            if raw_value is None
                            else float(raw_value),
                            variable.quality_field: ""
                            if raw_quality is None
                            else str(raw_quality),
                        }
                    )
            frame = pd.DataFrame(records)
            frame[spec.entity_field] = frame[spec.entity_field].astype("string")
            frame[variable.name] = pd.to_numeric(
                frame[variable.name], errors="raise"
            ).astype("float64")
            frame[variable.quality_field] = frame[variable.quality_field].astype(
                "string"
            )
            yield from chunks_from_frame(
                frame,
                spec,
                key_prefix=f"sqlite-batch={batch_number}",
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
