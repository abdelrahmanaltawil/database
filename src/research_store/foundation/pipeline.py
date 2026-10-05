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


@dataclass(frozen=True, slots=True)
class RejectedRecord:
    """Auditable pointer to source bytes that were not published as observations."""

    rejection_key: str
    record_locator: str
    reason: str
    raw_sha256: str
    raw_length: int
    recovered_record_count: int = 0
    details: dict[str, Any] | None = None


ParsedEvent = ParsedChunk | RejectedRecord
Parser = Callable[[Path, DatasetSpec, set[str]], Iterable[ParsedEvent]]


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
    with StoreWriter(paths, registry) as writer:
        asset = writer.archive_source(
            Path(source_path),
            source_uri=source_uri,
            publisher_vintage=publisher_vintage,
            fetched_at=fetched_at,
        )
        run = writer.resume_or_rebuild(
            writer.begin(spec, asset, ingester_version=ingester_version)
        )
        if run.state == "committed":
            return run.snapshot_id

        completed = writer.catalog.completed_chunk_keys(run.run_id)
        pending_rejections: list[RejectedRecord] = []

        def flush_rejections() -> None:
            if not pending_rejections:
                return
            writer.record_rejections(
                run=run,
                spec=spec,
                source=asset,
                rejections=pending_rejections,
            )
            pending_rejections.clear()

        try:
            for event in parser(asset.raw_path, spec, completed):
                if isinstance(event, RejectedRecord):
                    pending_rejections.append(event)
                    if len(pending_rejections) >= 1_000:
                        flush_rejections()
                    continue
                if event.chunk_key in completed:
                    continue
                writer.write_chunk(
                    run=run,
                    spec=spec,
                    source=asset,
                    chunk_key=event.chunk_key,
                    table=event.table,
                    partition=event.partition,
                )
            flush_rejections()
            return writer.publish(run=run, spec=spec, source=asset)
        except BaseException as error:
            flush_rejections()
            writer.catalog.mark_run_failed(run.run_id, repr(error))
            # Staged fragments are deliberately kept so a transient failure can
            # resume without re-parsing. `research-store gc` reclaims the space
            # of runs that will never be resumed, and `doctor` reports them.
            raise
