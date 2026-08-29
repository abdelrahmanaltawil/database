from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from typing import Any

from research_store.foundation.models import DatasetKind, Registry
from research_store.foundation.paths import StorePaths, resolve_store_paths
from research_store.foundation.pipeline import ParsedChunk
from research_store.foundation.writer import StoreWriter

ChunkFactory = Callable[[set[str]], Iterable[ParsedChunk]]


def materialize(
    *,
    dataset_id: str,
    parent_snapshot_ids: list[str],
    query: dict[str, Any],
    producer_version: str,
    chunks: ChunkFactory,
    registry: Registry,
    paths: StorePaths | None = None,
) -> str:
    """Checkpoint and publish a store-derived table with transitive lineage."""

    spec = registry.get(dataset_id)
    spec.require_ready()
    if spec.kind is not DatasetKind.DERIVED:
        raise ValueError(f"Dataset {dataset_id!r} is not declared as derived")
    fingerprint_payload = {
        "parents": parent_snapshot_ids,
        "query": query,
        "producer_version": producer_version,
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    paths = paths or resolve_store_paths(for_write=True)
    writer = StoreWriter(paths, registry)
    run = writer.begin_derived(
        spec,
        input_fingerprint=fingerprint,
        producer_version=producer_version,
        parent_snapshot_ids=parent_snapshot_ids,
    )
    if run.state == "committed":
        return run.snapshot_id
    completed = writer.catalog.completed_chunk_keys(run.run_id)
    try:
        for chunk in chunks(completed):
            if chunk.chunk_key in completed:
                continue
            writer.write_chunk(
                run=run,
                spec=spec,
                source=None,
                chunk_key=chunk.chunk_key,
                table=chunk.table,
                partition=chunk.partition,
            )
        return writer.publish(
            run=run,
            spec=spec,
            source=None,
            parent_snapshot_ids=parent_snapshot_ids,
            derivation_query=query,
            producer_version=producer_version,
        )
    except BaseException as error:
        writer.catalog.mark_run_failed(run.run_id, repr(error), producer_kind="derived")
        raise
