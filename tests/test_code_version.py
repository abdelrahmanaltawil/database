from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pandas as pd
import pytest

from research_store import cli
from research_store.foundation import code_version
from research_store.foundation.catalog import Catalog
from research_store.foundation.chunking import chunks_from_frame
from research_store.foundation.code_version import (
    ALLOW_UNCOMMITTED_ENV,
    CodeVersion,
    UncommittedCodeError,
    probe_code_version,
)
from research_store.foundation.maintenance import diagnose
from research_store.foundation.models import Registry
from research_store.foundation.pipeline import ingest_file

CLEAN = CodeVersion("a" * 40, ())
DIRTY = CodeVersion("b" * 40, ("src/research_store/ingestion/new_module.py",))
UNKNOWN = CodeVersion(None, None, "git is not installed")


# ----------------------------------------------------------------------
# Reading the working tree
# ----------------------------------------------------------------------


def _git(cwd: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=Test", "-c", "user.email=test@example.org",
         "-c", "commit.gpgsign=false", *arguments],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


@pytest.fixture
def isolated_git(monkeypatch, tmp_path: Path) -> None:
    # Neither the developer's git configuration nor a repository above
    # tmp_path may influence what the probe sees.
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", os.devnull)
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))


@pytest.fixture
def checkout(tmp_path: Path, isolated_git) -> Path:
    root = tmp_path / "checkout"
    package = root / "src" / "research_store"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "writer.py").write_text("VERSION = 1\n")
    (root / "pyproject.toml").write_text("[project]\nname = 'store'\n")
    (root / "docs").mkdir()
    (root / "docs" / "report.md").write_text("draft\n")
    (root / ".gitignore").write_text("__pycache__/\n")
    _git(root, "init", "-q")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "initial")
    return root


def _package(root: Path) -> Path:
    return root / "src" / "research_store"


def test_a_clean_checkout_is_its_head_commit(checkout: Path) -> None:
    version = probe_code_version(_package(checkout))
    assert version.commit == _git(checkout, "rev-parse", "HEAD").strip()
    assert version.uncommitted == ()
    assert version.committed


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        pytest.param(
            lambda root: (_package(root) / "writer.py").write_text("VERSION = 2\n"),
            ("src/research_store/writer.py",),
            id="edited module",
        ),
        pytest.param(
            # The climate-hourly run: new modules that no commit had yet.
            lambda root: (_package(root) / "new_ingester.py").write_text(""),
            ("src/research_store/new_ingester.py",),
            id="untracked module",
        ),
        pytest.param(
            lambda root: (root / "pyproject.toml").write_text("[project]\n"),
            ("pyproject.toml",),
            id="dependencies",
        ),
        pytest.param(
            lambda root: (
                (_package(root) / "writer.py").write_text("VERSION = 3\n"),
                _git(root, "add", "src/research_store/writer.py"),
            ),
            ("src/research_store/writer.py",),
            id="staged but not committed",
        ),
        pytest.param(
            lambda root: _git(
                root, "mv", "src/research_store/writer.py",
                "src/research_store/store_writer.py",
            ),
            ("src/research_store/store_writer.py", "src/research_store/writer.py"),
            id="renamed module",
        ),
    ],
)
def test_changes_to_the_code_are_listed(checkout: Path, change, expected) -> None:
    change(checkout)
    version = probe_code_version(_package(checkout))
    assert version.uncommitted == expected
    assert not version.committed


def test_edits_outside_the_code_do_not_count(checkout: Path) -> None:
    (checkout / "docs" / "report.md").write_text("final\n")
    (checkout / "notes.txt").write_text("scratch\n")
    cached = _package(checkout) / "__pycache__"
    cached.mkdir()
    (cached / "writer.cpython-312.pyc").write_bytes(b"")
    assert probe_code_version(_package(checkout)).committed


def test_code_outside_a_checkout_is_unknown(tmp_path: Path, isolated_git) -> None:
    package = tmp_path / "site-packages" / "research_store"
    package.mkdir(parents=True)
    version = probe_code_version(package)
    assert (version.commit, version.uncommitted) == (None, None)
    assert "not inside a git checkout" in version.reason


def test_a_package_the_checkout_does_not_track_is_unknown(checkout: Path) -> None:
    # A virtualenv inside a checkout holds an installed copy that the
    # repository ignores; it is not the code at the checkout's commit.
    (checkout / ".gitignore").write_text("__pycache__/\n.venv/\n")
    _git(checkout, "commit", "-q", "-am", "ignore venv")
    installed = checkout / ".venv" / "lib" / "site-packages" / "research_store"
    installed.mkdir(parents=True)
    (installed / "__init__.py").write_text("")
    version = probe_code_version(installed)
    assert version.commit is None
    assert "not tracked" in version.reason


def test_without_git_the_version_is_unknown(
    checkout: Path, monkeypatch, tmp_path: Path
) -> None:
    empty = tmp_path / "no-git-here"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    version = probe_code_version(_package(checkout))
    assert version == CodeVersion(None, None, "git is not installed")


def test_this_package_reports_a_version() -> None:
    # Whatever the state of the working tree running the tests, the probe of
    # the real package must reach a verdict rather than raise.
    version = code_version.current_code_version()
    assert version.commit is not None or version.reason


# ----------------------------------------------------------------------
# Writing
# ----------------------------------------------------------------------


def _frame(rows: list[tuple[str, str, float, float]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=["entity_id", "time_start", "power", "wind_speed"])
    frame["entity_id"] = frame["entity_id"].astype("string")
    frame["time_start"] = pd.to_datetime(frame["time_start"], utc=True)
    frame["time_end"] = frame["time_start"] + pd.Timedelta(minutes=10)
    frame["power_quality"] = pd.Series(["A"] * len(frame), dtype="string")
    return frame[
        ["entity_id", "time_start", "time_end", "power", "power_quality", "wind_speed"]
    ]


def _ingest(tmp_path, spec, registry, paths, name, rows, *, fail=False) -> str:
    source = tmp_path / name
    if not source.exists():
        source.write_text(name)
    frame = _frame(rows)

    def parser(path, declared, completed):
        if fail:
            raise RuntimeError("interrupted")
        yield from chunks_from_frame(
            frame, declared, key_prefix=name, completed=completed
        )

    return ingest_file(
        dataset_id=spec.dataset_id,
        source_path=source,
        parser=parser,
        ingester_version="v1",
        registry=registry,
        paths=paths,
    )


def _recorded(paths) -> list[tuple]:
    with Catalog(paths).open(read_only=True) as connection:
        return connection.execute(
            "SELECT attempt, code_commit, uncommitted_json FROM run_code_versions "
            "ORDER BY recorded_at, rowid"
        ).fetchall()


@pytest.fixture
def code_is(monkeypatch):
    def use(version: CodeVersion) -> None:
        monkeypatch.setattr(code_version, "current_code_version", lambda: version)

    return use


@pytest.fixture
def strict(monkeypatch) -> None:
    monkeypatch.delenv(ALLOW_UNCOMMITTED_ENV)


ROW = ("A", "2024-01-01T00:00:00Z", 1.0, 2.0)


@pytest.mark.parametrize("version", [DIRTY, UNKNOWN], ids=["uncommitted", "unknown"])
def test_code_no_commit_holds_is_refused_before_anything_is_written(
    tmp_path, store_paths, registry, wide_spec, code_is, strict, version
) -> None:
    code_is(version)
    with pytest.raises(UncommittedCodeError, match="Refusing to write") as raised:
        _ingest(tmp_path, wide_spec, registry, store_paths, "sensor.csv", [ROW])
    assert ALLOW_UNCOMMITTED_ENV in str(raised.value)
    assert not store_paths.root.exists(), "a refusal must leave no store behind"


def test_committed_code_writes_and_is_recorded(
    tmp_path, store_paths, registry, wide_spec, code_is, strict
) -> None:
    code_is(CLEAN)
    _ingest(tmp_path, wide_spec, registry, store_paths, "sensor.csv", [ROW])
    assert _recorded(store_paths) == [("start", CLEAN.commit, "[]")]


def test_allowed_uncommitted_code_records_what_was_uncommitted(
    tmp_path, store_paths, registry, wide_spec, code_is
) -> None:
    code_is(DIRTY)
    _ingest(tmp_path, wide_spec, registry, store_paths, "sensor.csv", [ROW])
    [(attempt, commit, uncommitted)] = _recorded(store_paths)
    assert (attempt, commit) == ("start", DIRTY.commit)
    assert json.loads(uncommitted) == list(DIRTY.uncommitted)


def test_every_write_attempt_is_recorded_and_a_no_op_is_not(
    tmp_path, store_paths, registry, wide_spec, code_is
) -> None:
    code_is(DIRTY)
    with pytest.raises(RuntimeError, match="interrupted"):
        _ingest(tmp_path, wide_spec, registry, store_paths, "sensor.csv", [ROW], fail=True)
    code_is(CLEAN)
    snapshot = _ingest(tmp_path, wide_spec, registry, store_paths, "sensor.csv", [ROW])
    assert _ingest(
        tmp_path, wide_spec, registry, store_paths, "sensor.csv", [ROW]
    ) == snapshot, "an idempotent re-ingest writes nothing"
    for path in Catalog(store_paths).snapshot_fragment_paths(snapshot):
        Path(path).unlink()
    assert _ingest(tmp_path, wide_spec, registry, store_paths, "sensor.csv", [ROW]) == snapshot

    assert [(attempt, commit) for attempt, commit, _ in _recorded(store_paths)] == [
        ("start", DIRTY.commit),
        ("resume", CLEAN.commit),
        ("rebuild", CLEAN.commit),
    ]


def test_an_append_snapshot_reports_the_code_of_every_run_it_reads(
    tmp_path, store_paths, registry, wide_spec, code_is
) -> None:
    code_is(CLEAN)
    _ingest(tmp_path, wide_spec, registry, store_paths, "annual-2023", [ROW])
    code_is(DIRTY)
    latest = _ingest(
        tmp_path, wide_spec, registry, store_paths, "annual-2024",
        [("A", "2024-06-01T00:00:00Z", 3.0, 4.0)],
    )

    versions = Catalog(store_paths).code_versions(wide_spec.dataset_id, latest)

    assert [
        (row["source_name"], row["code_commit"], row["uncommitted"]) for row in versions
    ] == [
        ("annual-2023", CLEAN.commit, []),
        ("annual-2024", DIRTY.commit, list(DIRTY.uncommitted)),
    ]


def test_a_source_is_named_by_its_publisher_filename_not_its_digest(
    tmp_path, store_paths, registry, wide_spec
) -> None:
    # Ingesting the archived object directly records its digest as a name;
    # the publisher's filename, recorded later, is still the one reported.
    digest_named = tmp_path / ("9" * 64)
    digest_named.write_text("annual bytes")
    snapshot = _ingest(tmp_path, wide_spec, registry, store_paths, digest_named.name, [ROW])
    published = tmp_path / "HLY01_RCS_P2004"
    published.write_text(digest_named.read_text())
    assert _ingest(tmp_path, wide_spec, registry, store_paths, published.name, [ROW]) == snapshot

    [row] = Catalog(store_paths).code_versions(wide_spec.dataset_id, snapshot)
    assert row["source_name"] == "HLY01_RCS_P2004"


def test_doctor_notes_runs_written_from_uncommitted_code(
    tmp_path, store_paths, registry, wide_spec, code_is
) -> None:
    code_is(DIRTY)
    _ingest(tmp_path, wide_spec, registry, store_paths, "sensor.csv", [ROW])

    findings, facts = diagnose(store_paths, registry)

    assert facts["runs_with_uncommitted_code"] == 1
    assert facts["runs_without_code_record"] == 0
    [note] = [item for item in findings if item.code == "uncommitted_code_runs"]
    assert note.severity == "note"


def test_a_catalogue_from_before_code_records_still_reads(
    tmp_path, store_paths, registry, wide_spec
) -> None:
    snapshot = _ingest(tmp_path, wide_spec, registry, store_paths, "sensor.csv", [ROW])
    catalog = Catalog(store_paths)
    with catalog.open() as connection:
        connection.execute("DROP TABLE run_code_versions")
        connection.execute(
            "UPDATE store_meta SET value = '2' WHERE key = 'schema_version'"
        )

    [row] = catalog.code_versions(wide_spec.dataset_id, snapshot)
    assert (row["code_commit"], row["uncommitted"], row["attempt"]) == (None, None, None)
    assert catalog.run_code_summary() is None
    findings, _ = diagnose(store_paths, registry)
    assert any(item.code == "schema_behind" for item in findings)

    catalog.initialize(registry)
    assert catalog.run_code_summary() == {
        "committed_runs": 1,
        "runs_without_code_record": 1,
        "runs_with_uncommitted_code": 0,
    }


# ----------------------------------------------------------------------
# Command line
# ----------------------------------------------------------------------


def test_the_flag_allows_uncommitted_code_and_says_so(
    tmp_path, monkeypatch, capsys, code_is, strict
) -> None:
    code_is(DIRTY)
    seen: list[str | None] = []

    def fake_ingest(dataset_id, source, **kwargs):
        seen.append(os.environ.get(ALLOW_UNCOMMITTED_ENV))
        return "snap_fake"

    monkeypatch.setitem(cli.INGESTERS, "fixed_width_hourly", fake_ingest)
    source = tmp_path / "HLY01_RCS_P2004"
    source.write_text("")

    assert cli.main(
        ["--store", str(tmp_path / "store"), "ingest", "eccc_hly01_observations",
         str(source), "--allow-uncommitted-code"]
    ) == 0
    assert seen == ["1"]
    assert "warning: writing from commit" in capsys.readouterr().err


def test_reingest_refuses_once_rather_than_per_source(
    tmp_path, store_paths, wide_spec, code_is, monkeypatch, capsys
) -> None:
    registry = Registry([wide_spec])
    monkeypatch.setattr(cli, "DEFAULT_REGISTRY", registry)
    for year in ("2023", "2024"):
        _ingest(
            tmp_path, wide_spec, registry, store_paths, f"annual-{year}",
            [("A", f"{year}-01-01T00:00:00Z", 1.0, 2.0)],
        )
    monkeypatch.delenv(ALLOW_UNCOMMITTED_ENV)
    code_is(DIRTY)

    assert cli.main(
        ["--store", str(store_paths.root), "reingest", wide_spec.dataset_id]
    ) == 1
    err = capsys.readouterr().err
    assert err.count("Refusing to write") == 1


@pytest.mark.parametrize("command", ["ingest", "ingest-directory"])
def test_the_cli_refuses_before_announcing_any_work(
    tmp_path, capsys, code_is, strict, command
) -> None:
    code_is(DIRTY)
    source = tmp_path / "HLY01_RCS_P2004"
    source.write_text("")
    target = [str(source)] if command == "ingest" else [str(tmp_path), "--pattern", "*"]

    assert cli.main(
        ["--store", str(tmp_path / "store"), command, "eccc_hly01_observations", *target]
    ) == 1
    err = capsys.readouterr().err
    assert err.startswith("error: Refusing to write")
