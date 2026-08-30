from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from research_store.foundation.catalog import Catalog, RunRecord
from research_store.foundation.hashing import sha256_file
from research_store.foundation.models import DatasetKind, DatasetSpec, Registry
from research_store.foundation.paths import StorePaths
from research_store.foundation.schema import (
    validate_no_duplicate_observations,
    validate_table,
)


@dataclass(frozen=True, slots=True)
class SourceAsset:
    source_id: str
    sha256: str
    raw_path: Path
    size_bytes: int


class StoreWriter:
    """The only code path allowed to mutate raw, warehouse, or catalogue state."""

    def __init__(self, paths: StorePaths, registry: Registry):
        self.paths = paths
        self.registry = registry
        self.catalog = Catalog(paths)
        self.catalog.initialize(registry)

    def archive_source(
        self,
        source: Path,
        *,
        source_uri: str | None = None,
        publisher_vintage: str | None = None,
        fetched_at: str | None = None,
    ) -> SourceAsset:
        source = source.expanduser().resolve(strict=True)
        if not source.is_file():
            raise ValueError(f"Source is not a regular file: {source}")

        object_root = self.paths.raw / "objects" / "sha256"
        object_root.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix="incoming-", dir=object_root
        )
        digest = hashlib.sha256()
        try:
            with os.fdopen(descriptor, "wb") as target, source.open("rb") as incoming:
                while block := incoming.read(8 * 1024 * 1024):
                    digest.update(block)
                    target.write(block)
                target.flush()
                os.fsync(target.fileno())
            checksum = digest.hexdigest()
            final = object_root / checksum[:2] / checksum[2:]
            final.parent.mkdir(parents=True, exist_ok=True)
            if final.exists():
                Path(temporary_name).unlink()
            else:
                os.replace(temporary_name, final)
                try:
                    final.chmod(0o444)
                except OSError:
                    pass
        except BaseException:
            Path(temporary_name).unlink(missing_ok=True)
            raise

        size = final.stat().st_size
        source_id = self.catalog.record_source(
            sha256=checksum,
            size_bytes=size,
            raw_path=final,
            original_name=source.name,
            source_uri=source_uri,
            publisher_vintage=publisher_vintage,
            fetched_at=fetched_at,
        )
        manifest = {
            "source_id": source_id,
            "sha256": checksum,
            "size_bytes": size,
            "original_name": source.name,
            "source_uri": source_uri,
            "publisher_vintage": publisher_vintage,
            "fetched_at": fetched_at,
            "raw_path": str(final.relative_to(self.paths.root)),
        }
        manifest_path = self.paths.raw / "source-manifests" / f"{checksum}.json"
        if not manifest_path.exists():
            temporary = manifest_path.with_suffix(".json.tmp")
            temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
            os.replace(temporary, manifest_path)
        return SourceAsset(source_id, checksum, final, size)

    def begin(
        self,
        spec: DatasetSpec,
        source: SourceAsset,
        *,
        ingester_version: str,
    ) -> RunRecord:
        spec.require_ready()
        return self.catalog.begin_or_resume_run(
            dataset_id=spec.dataset_id,
            source_id=source.source_id,
            ingester_version=ingester_version,
            registry_hash=self.registry.digest,
        )

    def begin_derived(
        self,
        spec: DatasetSpec,
        *,
        input_fingerprint: str,
        producer_version: str,
        parent_snapshot_ids: list[str],
    ) -> RunRecord:
        spec.require_ready()
        if spec.kind is not DatasetKind.DERIVED:
            raise ValueError(f"Dataset {spec.dataset_id!r} is not declared as derived")
        return self.catalog.begin_or_resume_derivation(
            dataset_id=spec.dataset_id,
            input_fingerprint=input_fingerprint,
            producer_version=producer_version,
            registry_hash=self.registry.digest,
            parent_snapshot_ids=parent_snapshot_ids,
        )

    def write_chunk(
        self,
        *,
        run: RunRecord,
        spec: DatasetSpec,
        source: SourceAsset | None,
        chunk_key: str,
        table: pa.Table,
        partition: dict[str, Any],
    ) -> Path:
        validate_table(table, spec)
        validate_no_duplicate_observations(table, spec)
        for required in spec.partition_keys:
            if required not in partition:
                raise ValueError(
                    f"Chunk partition is missing registry key {required!r}"
                )

        source_id = source.source_id if source is not None else None
        table = table.append_column(
            "_source_id", pa.array([source_id] * table.num_rows, type=pa.string())
        ).append_column(
            "_producer_run_id",
            pa.array([run.run_id] * table.num_rows, type=pa.string()),
        )
        chunk_digest = hashlib.sha256(chunk_key.encode()).hexdigest()[:20]
        components = [f"{key}={partition[key]}" for key in spec.partition_keys]
        relative = Path(*components) / f"part-{chunk_digest}.parquet"
        stage_root = self.paths.staging / "runs" / run.run_id / "fragments"
        destination = stage_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(".parquet.tmp")
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
        )
        os.replace(temporary, destination)
        checksum = sha256_file(destination)
        self.catalog.record_chunk(
            run_id=run.run_id,
            chunk_key=chunk_key,
            relative_path=str(relative),
            partition=partition,
            row_count=table.num_rows,
            content_sha256=checksum,
        )
        return destination

    def publish(
        self,
        *,
        run: RunRecord,
        spec: DatasetSpec,
        source: SourceAsset | None,
        parent_snapshot_ids: list[str] | None = None,
        derivation_query: dict[str, Any] | None = None,
        producer_version: str | None = None,
    ) -> str:
        tier = "derived" if spec.kind is DatasetKind.DERIVED else "external"
        stage = self.paths.staging / "runs" / run.run_id / "fragments"
        final = (
            self.paths.warehouse
            / tier
            / spec.dataset_id
            / "snapshots"
            / run.snapshot_id
        )
        final.parent.mkdir(parents=True, exist_ok=True)
        if stage.exists() and not final.exists():
            new_fragments = sorted(stage.rglob("*.parquet"))
            prior_fragments: list[Path] = []
            if spec.snapshot_mode == "append":
                try:
                    staged = self.catalog.staged_chunks(run.run_id)
                    partitions = [json.loads(row[2]) for row in staged]
                    partition_filter = {
                        key: {partition[key] for partition in partitions}
                        for key in spec.partition_keys
                    }
                    _, prior = self.catalog.committed_fragments(
                        spec.dataset_id,
                        partition_filter=partition_filter,
                    )
                    prior_fragments = [Path(path) for path in prior]
                except LookupError:
                    pass
            self._validate_snapshot_keys([*prior_fragments, *new_fragments], spec)
            os.replace(stage, final)
        elif stage.exists() and final.exists():
            raise RuntimeError(
                f"Both staged and final snapshot directories exist for {run.run_id}"
            )
        elif not final.exists():
            raise RuntimeError(f"No staged snapshot exists for run {run.run_id}")

        self.catalog.commit_snapshot(
            run_id=run.run_id,
            snapshot_id=run.snapshot_id,
            dataset_id=spec.dataset_id,
            source_id=source.source_id if source is not None else None,
            final_snapshot_dir=final,
            snapshot_mode=spec.snapshot_mode,
            producer_kind=spec.kind.value,
            parent_snapshot_ids=parent_snapshot_ids,
            derivation_query=derivation_query,
            producer_version=producer_version,
        )
        run_root = self.paths.staging / "runs" / run.run_id
        if run_root.exists():
            shutil.rmtree(run_root)
        return run.snapshot_id

    def _validate_snapshot_keys(self, paths: list[Path], spec: DatasetSpec) -> None:
        fragments = sorted(str(path) for path in paths)
        if not fragments:
            raise RuntimeError("Cannot publish a snapshot with no Parquet fragments")
        keys = [spec.entity_field]
        if spec.time_start_field:
            keys.append(spec.time_start_field)
        if spec.temporal_kind.value == "interval" and spec.time_end_field:
            keys.append(spec.time_end_field)
        if spec.storage_model.value == "long":
            keys.append("variable")
        quoted = [f'"{key.replace(chr(34), chr(34) * 2)}"' for key in keys]
        path_values = ", ".join(
            "'" + path.replace("'", "''") + "'" for path in fragments
        )
        query = (
            "SELECT count(*) FROM ("
            f"SELECT {', '.join(quoted)} "
            f"FROM read_parquet([{path_values}], union_by_name=true) "
            f"GROUP BY {', '.join(quoted)} HAVING count(*) > 1 LIMIT 3"
            ") AS duplicate_groups"
        )
        with duckdb.connect() as connection:
            duplicate_groups = connection.execute(query).fetchone()[0]
        if duplicate_groups:
            raise ValueError(
                f"Duplicate observation keys across chunks: "
                f"{duplicate_groups} sampled groups"
            )
