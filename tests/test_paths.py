from __future__ import annotations

import warnings
from pathlib import Path

from research_store.foundation.paths import (
    LOCAL_STORE_NAME,
    STORE_ENV,
    resolve_store_paths,
)


def test_one_environment_variable_resolves_the_store(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv(STORE_ENV, str(root))
    assert resolve_store_paths().root == root.resolve()


def test_default_store_is_local_to_the_repository(monkeypatch) -> None:
    monkeypatch.delenv(STORE_ENV, raising=False)
    root = resolve_store_paths().root
    assert root.name == LOCAL_STORE_NAME
    assert (root.parent / "pyproject.toml").is_file()


def test_cloud_synced_store_warns(tmp_path: Path) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        resolve_store_paths(tmp_path / "OneDrive" / "research")
    assert any("cloud-synced" in str(item.message) for item in caught)
