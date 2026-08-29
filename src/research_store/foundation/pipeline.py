from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from research_store.foundation.models import DatasetSpec, Registry
from research_store.foundation.paths import StorePaths, resolve_store_paths
from research_store.foundation.writer import StoreWriter


@dataclass(frozen=True, slots=True)
class ParsedChunk:
    chunk_key: str
    table: pa.Table
    partition: dict[str, Any]


Parser = Callable[[Path, DatasetSpec, set[str]], Iterable[ParsedChunk]]


def ingest_file(
    *,
    dataset_id: str,
    source_path: str | Path,
    parser: Parser,
    ingester_version: str,
    registry: Registry,
    paths: StorePaths | None = None,
    source_uri: str | None = None,
    publisher_vintage: str | None = None,
    fetched_at: str | None = None,
) -> str:
    """Archive, parse, checkpoint and atomically publish one physical file."""

    spec = registry.get(dataset_id)
    spec.require_ready()
    paths = paths or resolve_store_paths(for_write=True)
    writer = StoreWriter(paths, registry)
    asset = writer.archive_source(
        Path(source_path),
        source_uri=source_uri,
        publisher_vintage=publisher_vintage,
        fetched_at=fetched_at,
    )
    run = writer.begin(spec, asset, ingester_version=ingester_version)
    if run.state == "committed":
        return run.snapshot_id
    completed = writer.catalog.completed_chunk_keys(run.run_id)
    try:
        for chunk in parser(asset.raw_path, spec, completed):
            if chunk.chunk_key in completed:
                continue
            writer.write_chunk(
                run=run,
                spec=spec,
                source=asset,
                chunk_key=chunk.chunk_key,
                table=chunk.table,
                partition=chunk.partition,
            )
        return writer.publish(run=run, spec=spec, source=asset)
    except BaseException as error:
        writer.catalog.mark_run_failed(run.run_id, repr(error))
        raise
