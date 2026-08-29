from __future__ import annotations

import os
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path

STORE_ENV = "RESEARCH_DATA_ROOT"
CLOUD_MARKERS = (
    "dropbox",
    "google drive",
    "googledrive",
    "icloud drive",
    "onedrive",
    "box sync",
)


@dataclass(frozen=True, slots=True)
class StorePaths:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse"

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def catalog(self) -> Path:
        return self.root / "catalog" / "store.duckdb"

    @property
    def locks(self) -> Path:
        return self.root / "locks"

    def create(self) -> None:
        for directory in (
            self.raw / "objects" / "sha256",
            self.raw / "source-manifests",
            self.warehouse / "external",
            self.warehouse / "derived",
            self.staging / "runs",
            self.catalog.parent,
            self.locks,
        ):
            directory.mkdir(parents=True, exist_ok=True)


def _platform_fallback() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "research-data-store"
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        return base / "research-data-store"
    base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return base / "research-data-store"


def looks_cloud_synced(path: Path) -> bool:
    lowered = str(path.expanduser().resolve(strict=False)).casefold()
    return any(marker in lowered for marker in CLOUD_MARKERS)


def resolve_store_paths(
    root: str | Path | None = None, *, for_write: bool = False
) -> StorePaths:
    """Resolve explicit argument, then one environment variable, then platform data dir."""

    if root is not None:
        resolved = Path(root).expanduser().resolve(strict=False)
    elif value := os.environ.get(STORE_ENV):
        resolved = Path(value).expanduser().resolve(strict=False)
    else:
        resolved = _platform_fallback().resolve(strict=False)
        if for_write:
            warnings.warn(
                f"{STORE_ENV} is unset; writing to documented fallback {resolved}",
                RuntimeWarning,
                stacklevel=2,
            )
    if looks_cloud_synced(resolved):
        warnings.warn(
            f"Store path appears cloud-synced: {resolved}. Large mutable catalogue files "
            "can be evicted or repeatedly uploaded.",
            RuntimeWarning,
            stacklevel=2,
        )
    return StorePaths(resolved)
