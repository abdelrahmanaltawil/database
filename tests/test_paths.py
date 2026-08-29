from __future__ import annotations

import warnings
from pathlib import Path

from research_store.foundation.paths import STORE_ENV, resolve_store_paths


def test_one_environment_variable_resolves_the_store(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv(STORE_ENV, str(root))
    assert resolve_store_paths().root == root.resolve()


def test_cloud_synced_store_warns(tmp_path: Path) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        resolve_store_paths(tmp_path / "OneDrive" / "research")
    assert any("cloud-synced" in str(item.message) for item in caught)
