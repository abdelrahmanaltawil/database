from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from research_store.foundation.catalog import Catalog, RunRecord
from research_store.foundation.hashing import sha256_file
from research_store.foundation.models import (
    DatasetKind,
    DatasetSpec,
    Registry,
    StorageModel,
    TemporalKind,
)
from research_store.foundation.paths import StorePaths
from research_store.foundation.schema import (
    validate_no_duplicate_observations,
    validate_table,
)

if TYPE_CHECKING:
    from research_store.foundation.pipeline import RejectedRecord


@dataclass(frozen=True, slots=True)
class SourceAsset:
    source_id: str
    sha256: str
    raw_path: Path
    size_bytes: int


_LOCK_GUARD = threading.Lock()
_HELD_LOCKS: dict[str, list[Any]] = {}


class StoreWriteLock:
    """Admit exactly one writing process to a store at a time.

    DuckDB refuses a second writer on the catalogue by itself, but only once
    the catalogue is touched. By then a competing process may already have
    archived source bytes and written staged Parquet, leaving debris behind.
    The lock is taken before any of that happens. It is re-entrant within a
    single process so nested writers in one job cannot deadlock.
    """

    def __init__(self, path: Path):
        self._path = path
        self._key = str(path)
        self._holding = False

    def acquire(self) -> None:
        with _LOCK_GUARD:
            entry = _HELD_LOCKS.get(self._key)
            if entry is not None:
                entry[1] += 1
                self._holding = True
                return
            self._path.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                os.close(descriptor)
                raise RuntimeError(
                    f"Another process is writing to this store; lock held on "
                    f"{self._path}. Concurrent writers are refused because they "
                    f"would interleave staged fragments and catalogue transactions."
                ) from error
            os.ftruncate(descriptor, 0)
            os.write(descriptor, f"{os.getpid()}\n".encode())
            _HELD_LOCKS[self._key] = [descriptor, 1]
            self._holding = True

    def release(self) -> None:
        with _LOCK_GUARD:
            if not self._holding:
                return
            self._holding = False
            entry = _HELD_LOCKS.get(self._key)
            if entry is None:
                return
            entry[1] -= 1
            if entry[1] > 0:
                return
            descriptor = entry[0]
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
            del _HELD_LOCKS[self._key]


class StoreWriter:
    """The only code path allowed to mutate raw, warehouse, or catalogue state."""

    def __init__(self, paths: StorePaths, registry: Registry, *, lock: bool = True):
        self.paths = paths
        self.registry = registry
        paths.create()
        self._lock = StoreWriteLock(paths.write_lock) if lock else None
        if self._lock is not None:
            self._lock.acquire()
        try:
            self.catalog = Catalog(paths)
            self.catalog.initialize(registry)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._lock is not None:
            self._lock.release()
            self._lock = None

    def __enter__(self) -> StoreWriter:
        return self

    def __exit__(self, *exception: object) -> bool:
        self.close()
        return False

    # ------------------------------------------------------------------
    # Source archival
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

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
            dataset_digest=spec.identity_digest,
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

    # ------------------------------------------------------------------
    # Fragment paths
    # ------------------------------------------------------------------

    @staticmethod
    def _tier(spec: DatasetSpec) -> str:
        return "derived" if spec.kind is DatasetKind.DERIVED else "external"

    @staticmethod
    def _chunk_digest(chunk_key: str) -> str:
        return hashlib.sha256(chunk_key.encode()).hexdigest()[:20]

    def _staged_path(self, run_id: str, chunk_key: str) -> Path:
        return (
            self.paths.staging
            / "runs"
            / run_id
            / f"part-{self._chunk_digest(chunk_key)}.parquet"
        )

    def _final_path(
        self, spec: DatasetSpec, partition: dict[str, Any], content_sha256: str
    ) -> Path:
        components = [f"{key}={partition[key]}" for key in spec.partition_keys]
        return (
            self.paths.data_dir(self._tier(spec), spec.dataset_id)
            .joinpath(*components)
            .joinpath(f"part-{content_sha256[:24]}.parquet")
        )

    @staticmethod
    def _time_bounds(table: pa.Table, spec: DatasetSpec) -> tuple[Any, Any]:
        if spec.temporal_kind is TemporalKind.REFERENCE or not spec.time_start_field:
            return None, None
        earliest = pc.min(table.column(spec.time_start_field)).as_py()
        end_field = spec.time_start_field
        if spec.temporal_kind is TemporalKind.INTERVAL and spec.time_end_field:
            end_field = spec.time_end_field
        latest = pc.max(table.column(end_field)).as_py()
        return earliest, latest

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

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
        earliest, latest = self._time_bounds(table, spec)

        destination = self._staged_path(run.run_id, chunk_key)
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
            relative_path=self.paths.relative(
                self._final_path(spec, partition, checksum)
            ),
            partition=partition,
            row_count=table.num_rows,
            content_sha256=checksum,
            source_id=source_id,
            min_time=earliest,
            max_time=latest,
        )
        return destination

    def record_rejection(
        self,
        *,
        run: RunRecord,
        spec: DatasetSpec,
        source: SourceAsset,
        rejection: RejectedRecord,
    ) -> None:
        self.catalog.record_rejection(
            run_id=run.run_id,
            dataset_id=spec.dataset_id,
            source_id=source.source_id,
            rejection_key=rejection.rejection_key,
            record_locator=rejection.record_locator,
            reason=rejection.reason,
            raw_sha256=rejection.raw_sha256,
            raw_length=rejection.raw_length,
            recovered_record_count=rejection.recovered_record_count,
            details=rejection.details,
        )

    def record_rejections(
        self,
        *,
        run: RunRecord,
        spec: DatasetSpec,
        source: SourceAsset,
        rejections: list[RejectedRecord],
    ) -> None:
        self.catalog.record_rejections(
            [
                (
                    run.run_id,
                    spec.dataset_id,
                    source.source_id,
                    rejection.rejection_key,
                    rejection.record_locator,
                    rejection.reason,
                    rejection.raw_sha256,
                    rejection.raw_length,
                    rejection.recovered_record_count,
                    rejection.details,
                )
                for rejection in rejections
            ]
        )

    # ------------------------------------------------------------------
    # Publication
    # ------------------------------------------------------------------

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
        chunks = self.catalog.staged_chunks(run.run_id)
        if not chunks:
            raise RuntimeError(
                f"Cannot publish run {run.run_id}: no staged fragments are recorded"
            )

        moves: list[tuple[Path, Path, dict[str, Any]]] = []
        for chunk_key, relative_path, partition_json, *_rest in chunks:
            staged = self._staged_path(run.run_id, chunk_key)
            final = self.paths.root / relative_path
            if not staged.is_file() and not final.is_file():
                raise RuntimeError(
                    f"Staged fragment for chunk {chunk_key!r} is missing from both "
                    f"{staged} and {final}; re-run the ingest to rebuild it"
                )
            moves.append((staged, final, json.loads(partition_json)))

        # Everything is validated while it is still unreferenced. A crash after
        # this point leaves unreferenced files that `research-store gc` removes;
        # it can never leave a reader looking at unvalidated rows.
        readable = [
            str(staged if staged.is_file() else final) for staged, final, _ in moves
        ]
        self._check_plausibility(readable, spec)
        self._validate_keys_against_prior(
            readable, [partition for _, _, partition in moves], spec
        )

        for staged, final, _ in moves:
            if final.is_file():
                if staged.is_file():
                    staged.unlink()
                continue
            final.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, final)
            try:
                final.chmod(0o444)
            except OSError:
                pass

        self.catalog.commit_snapshot(
            run_id=run.run_id,
            snapshot_id=run.snapshot_id,
            dataset_id=spec.dataset_id,
            source_id=source.source_id if source is not None else None,
            snapshot_mode=spec.snapshot_mode,
            producer_kind=spec.kind.value,
            parent_snapshot_ids=parent_snapshot_ids,
            derivation_query=derivation_query,
            producer_version=producer_version,
        )
        shutil.rmtree(self.paths.staging / "runs" / run.run_id, ignore_errors=True)
        return run.snapshot_id

    # ------------------------------------------------------------------
    # Publication gates
    # ------------------------------------------------------------------

    def _check_plausibility(self, paths: list[str], spec: DatasetSpec) -> None:
        """Refuse a publication whose distribution contradicts its declared unit.

        Individual absurd readings are expected in an un-quality-controlled
        archive and are preserved. A scale factor that is wrong by a power of
        ten instead moves the whole distribution, which is what this catches.
        """

        banded = [
            variable for variable in spec.variables if variable.plausible_band is not None
        ]
        if not banded or not paths:
            return
        relation = "read_parquet([" + ", ".join(_sql_literal(p) for p in paths) + "], union_by_name = true)"
        with duckdb.connect() as connection:
            connection.execute("SET TimeZone = 'UTC'")
            for variable in banded:
                band = variable.plausible_band
                if spec.storage_model is StorageModel.LONG:
                    expression = "value"
                    predicate = (
                        f"variable = {_sql_literal(variable.name)} AND value IS NOT NULL"
                    )
                else:
                    quoted = '"' + variable.name.replace('"', '""') + '"'
                    expression = quoted
                    predicate = f"{quoted} IS NOT NULL"
                row = connection.execute(
                    f"SELECT count(*), quantile_cont({expression}, {band.quantile}) "
                    f"FROM {relation} WHERE {predicate}"
                ).fetchone()
                observed_count, observed = row[0], row[1]
                if not observed_count or observed is None:
                    continue
                if not band.minimum <= observed <= band.maximum:
                    raise ValueError(
                        f"{spec.dataset_id}.{variable.name} fails its plausibility "
                        f"band: {band.describe()} but the observed quantile is "
                        f"{observed:g} {variable.unit or ''}".rstrip()
                        + f". This normally means the declared scale or unit is wrong, "
                        f"not that the data is. Band evidence: {band.evidence}"
                    )

    def _validate_keys_against_prior(
        self,
        readable: list[str],
        new_partitions: list[dict[str, Any]],
        spec: DatasetSpec,
    ) -> None:
        prior_paths: list[str] = []
        prior_partitions: list[dict[str, Any]] = []
        if spec.snapshot_mode == "append":
            _, prior = self.catalog.latest_committed_fragments(spec.dataset_id)
            prior_paths = [path for path, _ in prior]
            prior_partitions = [partition for _, partition in prior]
        if prior_paths and not self._partitions_disjoint(
            prior_partitions, new_partitions, spec
        ):
            self._validate_snapshot_keys([*prior_paths, *readable], spec)
        else:
            self._validate_snapshot_keys(readable, spec)

    @staticmethod
    def _partitions_disjoint(
        prior: list[dict[str, Any]],
        incoming: list[dict[str, Any]],
        spec: DatasetSpec,
    ) -> bool:
        """Avoid rescanning old rows when an append cannot collide with them.

        Every partition key is a function of part of the observation key: `year`
        of the timestamp, `entity_bucket` of the entity. If the two sides share
        no value of any one key, they share no observation key either.
        """

        for key in spec.partition_keys:
            prior_values = {partition.get(key) for partition in prior}
            new_values = {partition.get(key) for partition in incoming}
            if None in prior_values or None in new_values:
                continue
            if prior_values.isdisjoint(new_values):
                return True
        return False

    def _validate_snapshot_keys(self, paths: list[str], spec: DatasetSpec) -> None:
        fragments = sorted(paths)
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
        path_values = ", ".join(_sql_literal(path) for path in fragments)
        query = (
            f"SELECT {', '.join(quoted)}, count(*) AS occurrences "
            f"FROM read_parquet([{path_values}], union_by_name=true) "
            f"GROUP BY {', '.join(quoted)} HAVING count(*) > 1 LIMIT 3"
        )
        with duckdb.connect() as connection:
            duplicates = connection.execute(query).fetchall()
        if duplicates:
            raise ValueError(f"Duplicate observation keys across chunks: {duplicates}")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
