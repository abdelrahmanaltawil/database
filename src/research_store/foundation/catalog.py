from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from research_store.foundation.models import Registry
from research_store.foundation.paths import StorePaths

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id VARCHAR PRIMARY KEY,
    description VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    storage_model VARCHAR NOT NULL,
    readiness VARCHAR NOT NULL,
    producer VARCHAR NOT NULL,
    registry_hash VARCHAR NOT NULL,
    spec_json VARCHAR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS variables (
    dataset_id VARCHAR NOT NULL,
    variable_name VARCHAR NOT NULL,
    quantity VARCHAR NOT NULL,
    unit VARCHAR,
    arrow_dtype VARCHAR NOT NULL,
    quality_field VARCHAR,
    PRIMARY KEY (dataset_id, variable_name)
);

CREATE TABLE IF NOT EXISTS source_files (
    source_id VARCHAR PRIMARY KEY,
    sha256 VARCHAR UNIQUE NOT NULL,
    size_bytes UBIGINT NOT NULL,
    raw_path VARCHAR NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS source_aliases (
    alias_id VARCHAR PRIMARY KEY,
    source_id VARCHAR NOT NULL,
    original_name VARCHAR NOT NULL,
    source_uri VARCHAR,
    publisher_vintage VARCHAR,
    fetched_at TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id VARCHAR PRIMARY KEY,
    dataset_id VARCHAR NOT NULL,
    source_id VARCHAR NOT NULL,
    ingester_version VARCHAR NOT NULL,
    registry_hash VARCHAR NOT NULL,
    snapshot_id VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    completed_at TIMESTAMPTZ,
    error VARCHAR,
    UNIQUE (dataset_id, source_id, ingester_version, registry_hash)
);

CREATE TABLE IF NOT EXISTS derivation_runs (
    run_id VARCHAR PRIMARY KEY,
    dataset_id VARCHAR NOT NULL,
    input_fingerprint VARCHAR NOT NULL,
    producer_version VARCHAR NOT NULL,
    registry_hash VARCHAR NOT NULL,
    snapshot_id VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    completed_at TIMESTAMPTZ,
    error VARCHAR,
    UNIQUE (dataset_id, input_fingerprint, producer_version, registry_hash)
);

CREATE TABLE IF NOT EXISTS ingestion_chunks (
    run_id VARCHAR NOT NULL,
    chunk_key VARCHAR NOT NULL,
    relative_path VARCHAR NOT NULL,
    partition_json VARCHAR NOT NULL,
    row_count UBIGINT NOT NULL,
    content_sha256 VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    PRIMARY KEY (run_id, chunk_key)
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id VARCHAR PRIMARY KEY,
    dataset_id VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    committed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS fragments (
    fragment_id VARCHAR PRIMARY KEY,
    snapshot_id VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    relative_path VARCHAR NOT NULL,
    partition_json VARCHAR NOT NULL,
    row_count UBIGINT NOT NULL,
    content_sha256 VARCHAR NOT NULL,
    source_id VARCHAR,
    min_time TIMESTAMPTZ,
    max_time TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS derivation_edges (
    child_snapshot_id VARCHAR NOT NULL,
    parent_snapshot_id VARCHAR NOT NULL,
    query_json VARCHAR NOT NULL,
    producer_version VARCHAR NOT NULL,
    PRIMARY KEY (child_snapshot_id, parent_snapshot_id)
);

CREATE TABLE IF NOT EXISTS snapshot_parents (
    child_snapshot_id VARCHAR NOT NULL,
    parent_snapshot_id VARCHAR NOT NULL,
    relation VARCHAR NOT NULL,
    PRIMARY KEY (child_snapshot_id, parent_snapshot_id)
);

CREATE TABLE IF NOT EXISTS entity_versions (
    dataset_id VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    valid_from TIMESTAMPTZ,
    valid_to TIMESTAMPTZ,
    name VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    source_id VARCHAR NOT NULL,
    attributes_json VARCHAR NOT NULL,
    PRIMARY KEY (dataset_id, entity_id, source_id)
);

CREATE TABLE IF NOT EXISTS entity_match_candidates (
    candidate_id VARCHAR PRIMARY KEY,
    left_dataset_id VARCHAR NOT NULL,
    left_entity_id VARCHAR NOT NULL,
    right_dataset_id VARCHAR NOT NULL,
    right_entity_id VARCHAR NOT NULL,
    evidence_json VARCHAR NOT NULL,
    method VARCHAR NOT NULL,
    decision_status VARCHAR NOT NULL DEFAULT 'candidate',
    decided_at TIMESTAMPTZ,
    decision_note VARCHAR
);
"""


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    snapshot_id: str
    state: str
    resumed: bool


class Catalog:
    def __init__(self, paths: StorePaths):
        self.paths = paths

    def open(self, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        connection = duckdb.connect(str(self.paths.catalog), read_only=read_only)
        connection.execute("SET TimeZone = 'UTC'")
        return connection

    def initialize(self, registry: Registry) -> None:
        self.paths.create()
        with self.open() as connection:
            connection.execute(SCHEMA_SQL)
            self._register_registry(connection, registry)

    def _register_registry(
        self, connection: duckdb.DuckDBPyConnection, registry: Registry
    ) -> None:
        for spec in registry:
            connection.execute(
                """
                INSERT OR REPLACE INTO datasets
                    (dataset_id, description, kind, storage_model, readiness, producer,
                     registry_hash, spec_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                """,
                [
                    spec.dataset_id,
                    spec.description,
                    spec.kind.value,
                    spec.storage_model.value,
                    spec.readiness.value,
                    spec.producer,
                    registry.digest,
                    json.dumps(spec.serializable(), sort_keys=True),
                ],
            )
            connection.execute(
                "DELETE FROM variables WHERE dataset_id = ?", [spec.dataset_id]
            )
            for variable in spec.variables:
                connection.execute(
                    """
                    INSERT INTO variables
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        spec.dataset_id,
                        variable.name,
                        variable.quantity,
                        variable.unit,
                        variable.dtype,
                        variable.quality_field,
                    ],
                )

    def record_source(
        self,
        *,
        sha256: str,
        size_bytes: int,
        raw_path: Path,
        original_name: str,
        source_uri: str | None,
        publisher_vintage: str | None,
        fetched_at: str | None,
    ) -> str:
        source_id = f"src_{sha256}"
        alias_payload = "\x1f".join(
            [
                source_id,
                original_name,
                source_uri or "",
                publisher_vintage or "",
                fetched_at or "",
            ]
        )
        import hashlib

        alias_id = "alias_" + hashlib.sha256(alias_payload.encode()).hexdigest()
        with self.open() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO source_files
                (source_id, sha256, size_bytes, raw_path)
                VALUES (?, ?, ?, ?)
                """,
                [source_id, sha256, size_bytes, str(raw_path)],
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO source_aliases
                (alias_id, source_id, original_name, source_uri, publisher_vintage, fetched_at)
                VALUES (?, ?, ?, ?, ?, try_cast(? AS TIMESTAMPTZ))
                """,
                [
                    alias_id,
                    source_id,
                    original_name,
                    source_uri,
                    publisher_vintage,
                    fetched_at,
                ],
            )
        return source_id

    def begin_or_resume_run(
        self,
        *,
        dataset_id: str,
        source_id: str,
        ingester_version: str,
        registry_hash: str,
    ) -> RunRecord:
        with self.open() as connection:
            existing = connection.execute(
                """
                SELECT run_id, snapshot_id, state FROM ingestion_runs
                WHERE dataset_id = ? AND source_id = ?
                  AND ingester_version = ? AND registry_hash = ?
                """,
                [dataset_id, source_id, ingester_version, registry_hash],
            ).fetchone()
            if existing:
                return RunRecord(existing[0], existing[1], existing[2], resumed=True)
            run_id = f"run_{uuid.uuid4().hex}"
            snapshot_id = f"snap_{uuid.uuid4().hex}"
            connection.execute(
                """
                INSERT INTO ingestion_runs
                (run_id, dataset_id, source_id, ingester_version, registry_hash,
                 snapshot_id, state)
                VALUES (?, ?, ?, ?, ?, ?, 'running')
                """,
                [
                    run_id,
                    dataset_id,
                    source_id,
                    ingester_version,
                    registry_hash,
                    snapshot_id,
                ],
            )
            connection.execute(
                """
                INSERT INTO snapshots (snapshot_id, dataset_id, run_id, state)
                VALUES (?, ?, ?, 'staging')
                """,
                [snapshot_id, dataset_id, run_id],
            )
            return RunRecord(run_id, snapshot_id, "running", resumed=False)

    def begin_or_resume_derivation(
        self,
        *,
        dataset_id: str,
        input_fingerprint: str,
        producer_version: str,
        registry_hash: str,
        parent_snapshot_ids: list[str],
    ) -> RunRecord:
        with self.open() as connection:
            if parent_snapshot_ids:
                placeholders = ", ".join("?" for _ in parent_snapshot_ids)
                found = {
                    row[0]
                    for row in connection.execute(
                        f"SELECT snapshot_id FROM snapshots WHERE state = 'committed' "
                        f"AND snapshot_id IN ({placeholders})",
                        parent_snapshot_ids,
                    ).fetchall()
                }
                missing = set(parent_snapshot_ids) - found
                if missing:
                    raise ValueError(
                        f"Derived inputs are not committed snapshots: {sorted(missing)}"
                    )
            existing = connection.execute(
                """
                SELECT run_id, snapshot_id, state FROM derivation_runs
                WHERE dataset_id = ? AND input_fingerprint = ?
                  AND producer_version = ? AND registry_hash = ?
                """,
                [dataset_id, input_fingerprint, producer_version, registry_hash],
            ).fetchone()
            if existing:
                return RunRecord(existing[0], existing[1], existing[2], resumed=True)
            run_id = f"run_{uuid.uuid4().hex}"
            snapshot_id = f"snap_{uuid.uuid4().hex}"
            connection.execute(
                """
                INSERT INTO derivation_runs
                (run_id, dataset_id, input_fingerprint, producer_version,
                 registry_hash, snapshot_id, state)
                VALUES (?, ?, ?, ?, ?, ?, 'running')
                """,
                [
                    run_id,
                    dataset_id,
                    input_fingerprint,
                    producer_version,
                    registry_hash,
                    snapshot_id,
                ],
            )
            connection.execute(
                """
                INSERT INTO snapshots (snapshot_id, dataset_id, run_id, state)
                VALUES (?, ?, ?, 'staging')
                """,
                [snapshot_id, dataset_id, run_id],
            )
            return RunRecord(run_id, snapshot_id, "running", resumed=False)

    def completed_chunk_keys(self, run_id: str) -> set[str]:
        with self.open(read_only=True) as connection:
            rows = connection.execute(
                "SELECT chunk_key FROM ingestion_chunks WHERE run_id = ? AND state = 'staged'",
                [run_id],
            ).fetchall()
        return {row[0] for row in rows}

    def record_chunk(
        self,
        *,
        run_id: str,
        chunk_key: str,
        relative_path: str,
        partition: dict[str, Any],
        row_count: int,
        content_sha256: str,
    ) -> None:
        with self.open() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO ingestion_chunks
                (run_id, chunk_key, relative_path, partition_json, row_count,
                 content_sha256, state)
                VALUES (?, ?, ?, ?, ?, ?, 'staged')
                """,
                [
                    run_id,
                    chunk_key,
                    relative_path,
                    json.dumps(partition, sort_keys=True),
                    row_count,
                    content_sha256,
                ],
            )

    def staged_chunks(self, run_id: str) -> list[tuple[Any, ...]]:
        with self.open(read_only=True) as connection:
            return connection.execute(
                """
                SELECT chunk_key, relative_path, partition_json, row_count, content_sha256
                FROM ingestion_chunks WHERE run_id = ? AND state = 'staged'
                ORDER BY chunk_key
                """,
                [run_id],
            ).fetchall()

    @contextmanager
    def transaction(self) -> Iterator[duckdb.DuckDBPyConnection]:
        connection = self.open()
        try:
            connection.execute("BEGIN TRANSACTION")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def commit_snapshot(
        self,
        *,
        run_id: str,
        snapshot_id: str,
        dataset_id: str,
        source_id: str | None,
        final_snapshot_dir: Path,
        snapshot_mode: str,
        producer_kind: str = "external",
        parent_snapshot_ids: list[str] | None = None,
        derivation_query: dict[str, Any] | None = None,
        producer_version: str | None = None,
    ) -> None:
        chunks = self.staged_chunks(run_id)
        if not chunks:
            raise RuntimeError("Cannot publish an ingestion run with no staged chunks")
        with self.transaction() as connection:
            if snapshot_mode == "append":
                previous = connection.execute(
                    """
                    SELECT snapshot_id FROM snapshots
                    WHERE dataset_id = ? AND state = 'committed' AND snapshot_id <> ?
                    ORDER BY committed_at DESC LIMIT 1
                    """,
                    [dataset_id, snapshot_id],
                ).fetchone()
                if previous:
                    parent_snapshot_id = previous[0]
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO snapshot_parents
                        VALUES (?, ?, 'append_base')
                        """,
                        [snapshot_id, parent_snapshot_id],
                    )
                    parent_fragments = connection.execute(
                        """
                        SELECT fragment_id, relative_path, partition_json, row_count,
                               content_sha256, source_id, min_time, max_time
                        FROM fragments WHERE snapshot_id = ?
                        """,
                        [parent_snapshot_id],
                    ).fetchall()
                    for (
                        parent_fragment_id,
                        relative_path,
                        partition_json,
                        row_count,
                        content_sha256,
                        parent_source_id,
                        min_time,
                        max_time,
                    ) in parent_fragments:
                        inherited_id = (
                            "frag_"
                            + uuid.uuid5(
                                uuid.NAMESPACE_URL, snapshot_id + parent_fragment_id
                            ).hex
                        )
                        connection.execute(
                            """
                            INSERT OR IGNORE INTO fragments
                            (fragment_id, snapshot_id, dataset_id, relative_path,
                             partition_json, row_count, content_sha256, source_id,
                             min_time, max_time)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                inherited_id,
                                snapshot_id,
                                dataset_id,
                                relative_path,
                                partition_json,
                                row_count,
                                content_sha256,
                                parent_source_id,
                                min_time,
                                max_time,
                            ],
                        )
            for (
                chunk_key,
                relative_path,
                partition_json,
                row_count,
                content_sha256,
            ) in chunks:
                fragment_id = f"frag_{uuid.uuid5(uuid.NAMESPACE_URL, snapshot_id + chunk_key).hex}"
                path = final_snapshot_dir / relative_path
                if not path.is_file():
                    raise FileNotFoundError(f"Published fragment is missing: {path}")
                connection.execute(
                    """
                    INSERT OR IGNORE INTO fragments
                    (fragment_id, snapshot_id, dataset_id, relative_path, partition_json,
                     row_count, content_sha256, source_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        fragment_id,
                        snapshot_id,
                        dataset_id,
                        str(path),
                        partition_json,
                        row_count,
                        content_sha256,
                        source_id,
                    ],
                )
            connection.execute(
                """
                UPDATE snapshots SET state = 'committed', committed_at = current_timestamp
                WHERE snapshot_id = ?
                """,
                [snapshot_id],
            )
            if producer_kind == "derived":
                if not producer_version:
                    raise ValueError("Derived publication requires a producer version")
                for parent_snapshot_id in parent_snapshot_ids or []:
                    connection.execute(
                        """
                        INSERT OR REPLACE INTO derivation_edges
                        (child_snapshot_id, parent_snapshot_id, query_json, producer_version)
                        VALUES (?, ?, ?, ?)
                        """,
                        [
                            snapshot_id,
                            parent_snapshot_id,
                            json.dumps(derivation_query or {}, sort_keys=True),
                            producer_version,
                        ],
                    )
                connection.execute(
                    """
                    UPDATE derivation_runs
                    SET state = 'committed', completed_at = current_timestamp, error = NULL
                    WHERE run_id = ?
                    """,
                    [run_id],
                )
            else:
                connection.execute(
                    """
                    UPDATE ingestion_runs SET state = 'committed', completed_at = current_timestamp,
                                              error = NULL
                    WHERE run_id = ?
                    """,
                    [run_id],
                )

    def mark_run_failed(
        self, run_id: str, error: str, *, producer_kind: str = "external"
    ) -> None:
        table = "derivation_runs" if producer_kind == "derived" else "ingestion_runs"
        with self.open() as connection:
            connection.execute(
                f"UPDATE {table} SET state = 'failed', error = ? WHERE run_id = ?",
                [error, run_id],
            )

    def committed_fragments(
        self,
        dataset_id: str,
        snapshot_id: str | None = None,
        *,
        partition_filter: dict[str, set[Any]] | None = None,
    ) -> tuple[str, list[str]]:
        with self.open(read_only=True) as connection:
            if snapshot_id is None:
                selected = connection.execute(
                    """
                    SELECT snapshot_id FROM snapshots
                    WHERE dataset_id = ? AND state = 'committed'
                    ORDER BY committed_at DESC LIMIT 1
                    """,
                    [dataset_id],
                ).fetchone()
                if selected is None:
                    raise LookupError(
                        f"No committed snapshot for dataset {dataset_id!r}"
                    )
                snapshot_id = selected[0]
            rows = connection.execute(
                """
                SELECT relative_path, partition_json FROM fragments
                WHERE dataset_id = ? AND snapshot_id = ?
                ORDER BY relative_path
                """,
                [dataset_id, snapshot_id],
            ).fetchall()
        paths: list[str] = []
        for path, partition_json in rows:
            partition = json.loads(partition_json)
            if partition_filter and any(
                partition.get(key) not in accepted
                for key, accepted in partition_filter.items()
            ):
                continue
            paths.append(path)
        if not paths:
            raise LookupError(
                f"Snapshot {snapshot_id!r} has no committed fragments for {dataset_id!r}"
            )
        return snapshot_id, paths

    def provenance(
        self, dataset_id: str, snapshot_id: str | None = None
    ) -> list[dict[str, Any]]:
        chosen, _ = self.committed_fragments(dataset_id, snapshot_id)
        with self.open(read_only=True) as connection:
            result = connection.execute(
                """
                WITH RECURSIVE lineage(snapshot_id) AS (
                    SELECT ?
                    UNION
                    SELECT edge.parent_snapshot_id
                    FROM derivation_edges edge
                    JOIN lineage current
                      ON edge.child_snapshot_id = current.snapshot_id
                )
                SELECT DISTINCT f.snapshot_id, f.source_id, s.sha256, s.size_bytes,
                                a.original_name, a.source_uri, a.publisher_vintage,
                                a.fetched_at
                FROM fragments f
                JOIN lineage ON lineage.snapshot_id = f.snapshot_id
                LEFT JOIN source_files s ON s.source_id = f.source_id
                LEFT JOIN source_aliases a ON a.source_id = f.source_id
                WHERE f.source_id IS NOT NULL
                ORDER BY a.original_name
                """,
                [chosen],
            )
            columns = [item[0] for item in result.description]
            return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
