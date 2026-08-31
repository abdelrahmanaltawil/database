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

SCHEMA_VERSION = 2

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
    dataset_digest VARCHAR NOT NULL,
    snapshot_id VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    completed_at TIMESTAMPTZ,
    error VARCHAR,
    UNIQUE (dataset_id, source_id, ingester_version, dataset_digest)
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
    source_id VARCHAR,
    state VARCHAR NOT NULL,
    min_time TIMESTAMPTZ,
    max_time TIMESTAMPTZ,
    PRIMARY KEY (run_id, chunk_key)
);

CREATE TABLE IF NOT EXISTS ingestion_inputs (
    run_id VARCHAR NOT NULL,
    input_key VARCHAR NOT NULL,
    source_id VARCHAR NOT NULL,
    input_role VARCHAR NOT NULL,
    details_json VARCHAR NOT NULL,
    PRIMARY KEY (run_id, input_key)
);

CREATE TABLE IF NOT EXISTS ingestion_rejections (
    run_id VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    source_id VARCHAR NOT NULL,
    rejection_key VARCHAR NOT NULL,
    record_locator VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    raw_sha256 VARCHAR NOT NULL,
    raw_length UBIGINT NOT NULL,
    recovered_record_count UBIGINT NOT NULL,
    details_json VARCHAR NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (run_id, rejection_key)
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id VARCHAR PRIMARY KEY,
    dataset_id VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    committed_at TIMESTAMPTZ,
    superseded_at TIMESTAMPTZ,
    supersede_reason VARCHAR
);

CREATE TABLE IF NOT EXISTS store_meta (
    key VARCHAR PRIMARY KEY,
    value VARCHAR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

-- relative_path is relative to the store root, never absolute: the store
-- must survive being moved, restored to another path, or copied to another
-- machine. Resolve it through StorePaths.root at read time.
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

    def _columns(self, connection: duckdb.DuckDBPyConnection, table: str) -> set[str]:
        return {
            row[1]
            for row in connection.execute(f"PRAGMA table_info({table!r})").fetchall()
        }

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        with self.open(read_only=True) as connection:
            try:
                row = connection.execute(
                    "SELECT value FROM store_meta WHERE key = ?", [key]
                ).fetchone()
            except duckdb.CatalogException:
                # A catalogue written before store_meta existed.
                return default
        return row[0] if row else default

    def _set_meta(
        self, connection: duckdb.DuckDBPyConnection, key: str, value: str
    ) -> None:
        connection.execute(
            "INSERT OR REPLACE INTO store_meta (key, value, updated_at) "
            "VALUES (?, ?, current_timestamp)",
            [key, value],
        )

    def _migrate_schema(
        self, connection: duckdb.DuckDBPyConnection, registry: Registry
    ) -> None:
        """Bring an older catalogue up to SCHEMA_VERSION without losing history.

        Only catalogue structure is touched here because this runs on every
        writer construction. Relocating fragment files and rewriting stored
        paths is a filesystem migration and lives behind `research-store
        migrate` instead.
        """

        tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        if "ingestion_runs_old" in tables:
            # A previous attempt was interrupted part-way. Undo it rather than
            # building on a half-copied table.
            connection.execute("DROP TABLE IF EXISTS ingestion_runs")
            connection.execute(
                "ALTER TABLE ingestion_runs_old RENAME TO ingestion_runs"
            )

        for table, column, definition in (
            ("ingestion_chunks", "source_id", "VARCHAR"),
            ("ingestion_chunks", "min_time", "TIMESTAMPTZ"),
            ("ingestion_chunks", "max_time", "TIMESTAMPTZ"),
            ("snapshots", "superseded_at", "TIMESTAMPTZ"),
            ("snapshots", "supersede_reason", "VARCHAR"),
        ):
            if column not in self._columns(connection, table):
                connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
                )

        if "dataset_digest" not in self._columns(connection, "ingestion_runs"):
            # The unique key itself changes, so the table is rebuilt. Historical
            # rows take the identity their dataset has today, which makes an
            # unchanged spec correctly recognised as already ingested and a
            # changed one correctly treated as a different identity.
            #
            # Because the old key included the whole-registry hash, one source
            # file can appear more than once under the same ingester version:
            # a failed attempt and a later success, separated only by an
            # unrelated registry edit. Collapsing them onto one identity would
            # violate the new key, so the surviving publication claims it and
            # the earlier attempts keep a distinct historical marker.
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute(
                    "CREATE OR REPLACE TEMP TABLE dataset_digest_map"
                    "(dataset_id VARCHAR, digest VARCHAR)"
                )
                connection.executemany(
                    "INSERT INTO dataset_digest_map VALUES (?, ?)",
                    [[spec.dataset_id, spec.identity_digest] for spec in registry],
                )
                connection.execute(
                    "ALTER TABLE ingestion_runs RENAME TO ingestion_runs_old"
                )
                connection.execute(SCHEMA_SQL)
                connection.execute(
                    """
                    INSERT INTO ingestion_runs
                        (run_id, dataset_id, source_id, ingester_version,
                         registry_hash, dataset_digest, snapshot_id, state,
                         started_at, completed_at, error)
                    SELECT ranked.run_id, ranked.dataset_id, ranked.source_id,
                           ranked.ingester_version, ranked.registry_hash,
                           CASE WHEN ranked.rank = 1
                                THEN coalesce(map.digest, ranked.registry_hash)
                                ELSE 'superseded-identity:' || ranked.registry_hash
                           END,
                           ranked.snapshot_id, ranked.state, ranked.started_at,
                           ranked.completed_at, ranked.error
                    FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY dataset_id, source_id, ingester_version
                            ORDER BY (state = 'committed') DESC, started_at
                        ) AS rank
                        FROM ingestion_runs_old
                    ) AS ranked
                    LEFT JOIN dataset_digest_map AS map
                      ON map.dataset_id = ranked.dataset_id
                    """
                )
                moved = connection.execute(
                    "SELECT count(*) FROM ingestion_runs"
                ).fetchone()[0]
                original = connection.execute(
                    "SELECT count(*) FROM ingestion_runs_old"
                ).fetchone()[0]
                if moved != original:
                    raise RuntimeError(
                        f"Run migration would lose history: {original} rows before, "
                        f"{moved} after"
                    )
                connection.execute("DROP TABLE ingestion_runs_old")
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        self._set_meta(connection, "schema_version", str(SCHEMA_VERSION))

    def initialize(self, registry: Registry) -> None:
        self.paths.create()
        with self.open() as connection:
            connection.execute(SCHEMA_SQL)
            self._migrate_schema(connection, registry)
            for run_table in ("ingestion_runs", "derivation_runs"):
                connection.execute(
                    f"""
                    UPDATE snapshots AS snapshot
                    SET state = 'failed'
                    FROM {run_table} AS run
                    WHERE snapshot.run_id = run.run_id
                      AND snapshot.state = 'staging'
                      AND run.state = 'failed'
                    """
                )
            self._register_registry(connection, registry)

    def _register_registry(
        self, connection: duckdb.DuckDBPyConnection, registry: Registry
    ) -> None:
        declared = {spec.dataset_id for spec in registry}
        catalogued = {
            row[0]
            for row in connection.execute("SELECT dataset_id FROM datasets").fetchall()
        }
        for dataset_id in catalogued - declared:
            holds_data = connection.execute(
                "SELECT 1 FROM snapshots WHERE dataset_id = ? LIMIT 1", [dataset_id]
            ).fetchone()
            if holds_data:
                # Data outlives its declaration: keep the row so the fragments
                # remain explicable, but stop advertising it as usable.
                connection.execute(
                    "UPDATE datasets SET readiness = 'retired' WHERE dataset_id = ?",
                    [dataset_id],
                )
            else:
                connection.execute(
                    "DELETE FROM variables WHERE dataset_id = ?", [dataset_id]
                )
                connection.execute(
                    "DELETE FROM datasets WHERE dataset_id = ?", [dataset_id]
                )
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
        dataset_digest: str,
    ) -> RunRecord:
        with self.open() as connection:
            existing = connection.execute(
                """
                SELECT run_id, snapshot_id, state FROM ingestion_runs
                WHERE dataset_id = ? AND source_id = ?
                  AND ingester_version = ? AND dataset_digest = ?
                """,
                [dataset_id, source_id, ingester_version, dataset_digest],
            ).fetchone()
            if existing:
                return RunRecord(existing[0], existing[1], existing[2], resumed=True)
            run_id = f"run_{uuid.uuid4().hex}"
            snapshot_id = f"snap_{uuid.uuid4().hex}"
            connection.execute(
                """
                INSERT INTO ingestion_runs
                (run_id, dataset_id, source_id, ingester_version, registry_hash,
                 dataset_digest, snapshot_id, state)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'running')
                """,
                [
                    run_id,
                    dataset_id,
                    source_id,
                    ingester_version,
                    registry_hash,
                    dataset_digest,
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
        source_id: str | None = None,
        min_time: Any = None,
        max_time: Any = None,
    ) -> None:
        with self.open() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO ingestion_chunks
                (run_id, chunk_key, relative_path, partition_json, row_count,
                 content_sha256, source_id, state, min_time, max_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'staged', ?, ?)
                """,
                [
                    run_id,
                    chunk_key,
                    relative_path,
                    json.dumps(partition, sort_keys=True),
                    row_count,
                    content_sha256,
                    source_id,
                    min_time,
                    max_time,
                ],
            )

    def record_ingestion_input(
        self,
        *,
        run_id: str,
        input_key: str,
        source_id: str,
        input_role: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.open() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO ingestion_inputs
                (run_id, input_key, source_id, input_role, details_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    input_key,
                    source_id,
                    input_role,
                    json.dumps(details or {}, sort_keys=True),
                ],
            )

    def record_rejection(
        self,
        *,
        run_id: str,
        dataset_id: str,
        source_id: str,
        rejection_key: str,
        record_locator: str,
        reason: str,
        raw_sha256: str,
        raw_length: int,
        recovered_record_count: int,
        details: dict[str, Any] | None,
    ) -> None:
        self.record_rejections(
            [
                (
                    run_id,
                    dataset_id,
                    source_id,
                    rejection_key,
                    record_locator,
                    reason,
                    raw_sha256,
                    raw_length,
                    recovered_record_count,
                    details,
                )
            ]
        )

    def record_rejections(
        self,
        records: list[
            tuple[
                str,
                str,
                str,
                str,
                str,
                str,
                str,
                int,
                int,
                dict[str, Any] | None,
            ]
        ],
    ) -> None:
        if not records:
            return
        values = [
            [*record[:-1], json.dumps(record[-1] or {}, sort_keys=True)]
            for record in records
        ]
        with self.open() as connection:
            connection.executemany(
                """
                INSERT OR REPLACE INTO ingestion_rejections
                (run_id, dataset_id, source_id, rejection_key, record_locator,
                 reason, raw_sha256, raw_length, recovered_record_count,
                 details_json, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                """,
                values,
            )

    def staged_chunks(self, run_id: str) -> list[tuple[Any, ...]]:
        with self.open(read_only=True) as connection:
            return connection.execute(
                """
                SELECT chunk_key, relative_path, partition_json, row_count,
                       content_sha256, source_id
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
                _partition_json,
                _row_count,
                _content_sha256,
                _chunk_source_id,
            ) in chunks:
                fragment_id = f"frag_{uuid.uuid5(uuid.NAMESPACE_URL, snapshot_id + chunk_key).hex}"
                path = self.paths.root / relative_path
                if not path.is_file():
                    raise FileNotFoundError(f"Published fragment is missing: {path}")
                connection.execute(
                    """
                    INSERT OR IGNORE INTO fragments
                    (fragment_id, snapshot_id, dataset_id, relative_path, partition_json,
                     row_count, content_sha256, source_id, min_time, max_time)
                    SELECT ?, ?, ?, relative_path, partition_json, row_count,
                           content_sha256, coalesce(source_id, ?), min_time, max_time
                    FROM ingestion_chunks
                    WHERE run_id = ? AND chunk_key = ?
                    """,
                    [
                        fragment_id,
                        snapshot_id,
                        dataset_id,
                        source_id,
                        run_id,
                        chunk_key,
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
            connection.execute(
                "UPDATE snapshots SET state = 'failed' WHERE run_id = ?",
                [run_id],
            )

    def committed_fragments(
        self,
        dataset_id: str,
        snapshot_id: str | None = None,
        *,
        partition_filter: dict[str, set[Any]] | None = None,
        start: Any = None,
        end: Any = None,
    ) -> tuple[str, list[str]]:
        chosen, paths, _ = self.committed_fragment_plan(
            dataset_id,
            snapshot_id,
            partition_filter=partition_filter,
            start=start,
            end=end,
        )
        return chosen, paths

    def committed_fragment_plan(
        self,
        dataset_id: str,
        snapshot_id: str | None = None,
        *,
        partition_filter: dict[str, set[Any]] | None = None,
        start: Any = None,
        end: Any = None,
    ) -> tuple[str, list[str], int]:
        """Fragments to read, plus the stored row count they add up to.

        The row total is catalogue metadata, so a caller can size a read before
        paying to execute it.
        """

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
            conditions = ["dataset_id = ?", "snapshot_id = ?"]
            parameters: list[Any] = [dataset_id, snapshot_id]
            # A fragment whose bounds are unknown is never pruned away.
            if start is not None:
                conditions.append("(max_time IS NULL OR max_time > ?)")
                parameters.append(start)
            if end is not None:
                conditions.append("(min_time IS NULL OR min_time < ?)")
                parameters.append(end)
            rows = connection.execute(
                f"""
                SELECT relative_path, partition_json, max(row_count) AS row_count
                FROM fragments
                WHERE {' AND '.join(conditions)}
                GROUP BY relative_path, partition_json
                ORDER BY relative_path
                """,
                parameters,
            ).fetchall()
        seen: set[str] = set()
        paths: list[str] = []
        total_rows = 0
        for path, partition_json, row_count in rows:
            partition = json.loads(partition_json)
            if partition_filter and any(
                partition.get(key) not in accepted
                for key, accepted in partition_filter.items()
            ):
                continue
            # `root / path` yields `path` unchanged when it is already absolute,
            # so catalogues written before paths became relative still resolve.
            resolved = str(self.paths.root / path)
            if resolved in seen:
                continue
            seen.add(resolved)
            paths.append(resolved)
            total_rows += int(row_count or 0)
        if not paths:
            raise LookupError(
                f"Snapshot {snapshot_id!r} has no committed fragments for {dataset_id!r}"
            )
        return snapshot_id, paths, total_rows

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
                , sources AS (
                    SELECT snapshot.snapshot_id, input.source_id,
                           input.input_role, input.details_json
                    FROM snapshots AS snapshot
                    JOIN lineage USING (snapshot_id)
                    JOIN ingestion_inputs AS input USING (run_id)
                    UNION ALL
                    SELECT fragment.snapshot_id, fragment.source_id,
                           'observation_source' AS input_role,
                           '{}' AS details_json
                    FROM fragments AS fragment
                    JOIN lineage USING (snapshot_id)
                    WHERE fragment.source_id IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM snapshots AS source_snapshot
                          JOIN ingestion_inputs AS input
                            ON input.run_id = source_snapshot.run_id
                          WHERE source_snapshot.snapshot_id = fragment.snapshot_id
                            AND input.source_id = fragment.source_id
                      )
                )
                SELECT DISTINCT sources.snapshot_id, sources.source_id,
                                sources.input_role,
                                sources.details_json AS input_details_json,
                                file.sha256, file.size_bytes, alias.original_name,
                                alias.source_uri, alias.publisher_vintage,
                                alias.fetched_at
                FROM sources
                LEFT JOIN source_files AS file USING (source_id)
                LEFT JOIN source_aliases AS alias USING (source_id)
                ORDER BY alias.original_name, sources.input_role
                """,
                [chosen],
            )
            columns = [item[0] for item in result.description]
            return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    # ------------------------------------------------------------------
    # Correction, recovery and maintenance
    # ------------------------------------------------------------------

    def supersede_dataset(self, dataset_id: str, reason: str) -> list[str]:
        """Retire every committed snapshot of a dataset without deleting it.

        A correction is not an append. When a registry mistake means published
        values were wrong, the old snapshots must stop being readable and must
        stop being inherited by the next append, but they must remain in the
        catalogue so the record of what was published survives.
        """

        if not reason.strip():
            raise ValueError("Superseding published data requires a recorded reason")
        with self.transaction() as connection:
            affected = [
                row[0]
                for row in connection.execute(
                    "SELECT snapshot_id FROM snapshots "
                    "WHERE dataset_id = ? AND state = 'committed'",
                    [dataset_id],
                ).fetchall()
            ]
            connection.execute(
                """
                UPDATE snapshots
                SET state = 'superseded',
                    superseded_at = current_timestamp,
                    supersede_reason = ?
                WHERE dataset_id = ? AND state = 'committed'
                """,
                [reason, dataset_id],
            )
        return affected

    def reset_run_for_rebuild(self, run_id: str) -> None:
        """Make a committed run re-derive its fragments from the archived bytes.

        Fragment paths are content addressed and fragment ids are derived from
        the snapshot, so a rebuild reproduces exactly the rows the catalogue
        already holds. That is what makes restore-from-raw possible: the
        snapshot keeps its identity instead of being minted again.
        """

        with self.transaction() as connection:
            connection.execute(
                "DELETE FROM ingestion_chunks WHERE run_id = ?", [run_id]
            )
            connection.execute(
                "UPDATE ingestion_runs SET state = 'running', completed_at = NULL "
                "WHERE run_id = ?",
                [run_id],
            )
            connection.execute(
                "UPDATE snapshots SET state = 'staging' WHERE run_id = ?", [run_id]
            )

    def latest_committed_fragments(
        self, dataset_id: str
    ) -> tuple[str | None, list[tuple[str, dict[str, Any]]]]:
        """Absolute path and partition of every fragment in the live snapshot."""

        with self.open(read_only=True) as connection:
            selected = connection.execute(
                """
                SELECT snapshot_id FROM snapshots
                WHERE dataset_id = ? AND state = 'committed'
                ORDER BY committed_at DESC LIMIT 1
                """,
                [dataset_id],
            ).fetchone()
            if selected is None:
                return None, []
            rows = connection.execute(
                "SELECT DISTINCT relative_path, partition_json FROM fragments "
                "WHERE snapshot_id = ?",
                [selected[0]],
            ).fetchall()
        return selected[0], [
            (str(self.paths.root / path), json.loads(partition_json))
            for path, partition_json in rows
        ]

    def archived_sources(self, dataset_id: str) -> list[tuple[Any, ...]]:
        """Every archived source this dataset has ever ingested, with its names.

        This is what makes a restore or a correction repeatable: the bytes and
        the metadata needed to re-present them are both in the catalogue.
        """

        with self.open(read_only=True) as connection:
            return connection.execute(
                """
                SELECT file.raw_path, alias.original_name,
                       alias.publisher_vintage, alias.source_uri,
                       alias.fetched_at::VARCHAR AS fetched_at,
                       max(run.ingester_version) AS ingester_version
                FROM ingestion_runs AS run
                JOIN source_files AS file USING (source_id)
                LEFT JOIN source_aliases AS alias USING (source_id)
                WHERE run.dataset_id = ?
                GROUP BY file.source_id, file.raw_path, alias.alias_id,
                         alias.original_name, alias.publisher_vintage,
                         alias.source_uri, alias.fetched_at, alias.recorded_at
                -- One source can carry several recorded names. Prefer a real
                -- publisher filename over one that is merely a content digest
                -- (which is what an ingest pointed straight at the archived
                -- object records), then the name it was first archived under.
                QUALIFY row_number() OVER (
                    PARTITION BY file.source_id
                    ORDER BY regexp_matches(
                                 coalesce(alias.original_name, ''),
                                 '^[0-9a-f]{32,}$'
                             ) ASC,
                             alias.recorded_at ASC
                ) = 1
                ORDER BY alias.original_name
                """,
                [dataset_id],
            ).fetchall()

    def run_chunk_paths(self, run_id: str) -> list[str]:
        """Absolute paths of the fragments this run itself produced."""

        with self.open(read_only=True) as connection:
            rows = connection.execute(
                "SELECT relative_path FROM ingestion_chunks WHERE run_id = ?",
                [run_id],
            ).fetchall()
        return [str(self.paths.root / row[0]) for row in rows]

    def snapshot_fragment_paths(self, snapshot_id: str) -> list[str]:
        with self.open(read_only=True) as connection:
            rows = connection.execute(
                "SELECT relative_path FROM fragments WHERE snapshot_id = ?",
                [snapshot_id],
            ).fetchall()
        return [str(self.paths.root / row[0]) for row in rows]

    def fragment_inventory(
        self, *, dataset_id: str | None = None, states: tuple[str, ...] = ("committed",)
    ) -> list[tuple[str, str, int, str, str]]:
        """One row per physical file: (path, sha256, rows, dataset, a snapshot).

        Snapshots share fragments, so verification is per file rather than per
        reference; otherwise the same bytes would be re-hashed once for every
        snapshot that names them.
        """

        placeholders = ", ".join("?" for _ in states)
        conditions = [f"snapshot.state IN ({placeholders})"]
        parameters: list[Any] = list(states)
        if dataset_id is not None:
            conditions.append("fragment.dataset_id = ?")
            parameters.append(dataset_id)
        with self.open(read_only=True) as connection:
            return connection.execute(
                f"""
                SELECT fragment.relative_path,
                       min(fragment.content_sha256) AS content_sha256,
                       max(fragment.row_count) AS row_count,
                       min(fragment.dataset_id) AS dataset_id,
                       min(fragment.snapshot_id) AS snapshot_id
                FROM fragments AS fragment
                JOIN snapshots AS snapshot USING (snapshot_id)
                WHERE {' AND '.join(conditions)}
                GROUP BY fragment.relative_path
                ORDER BY fragment.relative_path
                """,
                parameters,
            ).fetchall()

    def referenced_relative_paths(self, *, include_superseded: bool = True) -> set[str]:
        states = ("committed", "superseded") if include_superseded else ("committed",)
        placeholders = ", ".join("?" for _ in states)
        with self.open(read_only=True) as connection:
            rows = connection.execute(
                f"""
                SELECT DISTINCT fragment.relative_path
                FROM fragments AS fragment
                JOIN snapshots AS snapshot USING (snapshot_id)
                WHERE snapshot.state IN ({placeholders})
                """,
                list(states),
            ).fetchall()
        return {row[0] for row in rows}

    def runs_in_state(self, *states: str) -> list[tuple[str, str, str, str | None]]:
        placeholders = ", ".join("?" for _ in states)
        with self.open(read_only=True) as connection:
            return connection.execute(
                f"""
                SELECT run_id, dataset_id, state, error FROM ingestion_runs
                WHERE state IN ({placeholders}) ORDER BY started_at
                """,
                list(states),
            ).fetchall()

    def forget_run(self, run_id: str) -> None:
        """Drop the staged bookkeeping of a run that will never be resumed."""

        with self.transaction() as connection:
            connection.execute(
                "DELETE FROM ingestion_chunks WHERE run_id = ?", [run_id]
            )

    def dataset_overview(self) -> list[tuple[Any, ...]]:
        with self.open(read_only=True) as connection:
            return connection.execute(
                """
                SELECT dataset.dataset_id, dataset.readiness,
                       count(DISTINCT snapshot.snapshot_id)
                           FILTER (WHERE snapshot.state = 'committed') AS committed,
                       count(DISTINCT snapshot.snapshot_id)
                           FILTER (WHERE snapshot.state = 'superseded') AS superseded
                FROM datasets AS dataset
                LEFT JOIN snapshots AS snapshot USING (dataset_id)
                GROUP BY 1, 2 ORDER BY 1
                """
            ).fetchall()

    def absolute_path_count(self) -> int:
        with self.open(read_only=True) as connection:
            return connection.execute(
                "SELECT count(*) FROM fragments WHERE starts_with(relative_path, '/')"
            ).fetchone()[0]
