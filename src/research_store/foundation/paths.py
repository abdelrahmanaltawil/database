from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from pathlib import Path

STORE_ENV = "RESEARCH_DATA_ROOT"
LOCAL_STORE_NAME = "ResearchDataStore"
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

    def __post_init__(self) -> None:
        # A plain string here would silently produce a store whose every
        # derived path is wrong, so coerce rather than trust the caller.
        if not isinstance(self.root, Path):
            object.__setattr__(self, "root", Path(self.root))

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse"

    @property
    def downloads(self) -> Path:
        """Publisher downloads awaiting ingestion, one directory per source.

        The store never catalogues this directory, but it is not uniformly
        disposable: it also holds delivered publisher downloads that ingestion
        reports cite for reproduction. Only a `research-store fetch` cache
        (``downloads/<dataset>/`` of an API-acquired dataset) is rebuildable,
        because its ingest archives every byte it read into ``raw/``.
        """

        return self.root / "downloads"

    def raw_object(self, sha256: str) -> Path:
        """Where the archived bytes with this SHA-256 live."""

        if len(sha256) != 64 or any(char not in "0123456789abcdef" for char in sha256):
            raise ValueError(f"Not a lowercase SHA-256 hex digest: {sha256!r}")
        return self.raw / "objects" / "sha256" / sha256[:2] / sha256[2:]

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def catalog(self) -> Path:
        return self.root / "catalog" / "store.duckdb"

    @property
    def locks(self) -> Path:
        return self.root / "locks"

    @property
    def write_lock(self) -> Path:
        """The file whose exclusive lock admits exactly one writing process."""

        return self.locks / "write.lock"

    def data_dir(self, tier: str, dataset_id: str) -> Path:
        """Where a dataset's fragments physically live.

        Fragments belong to a dataset, not to a snapshot. A snapshot is a
        catalogue manifest naming fragments, so appending never copies data and
        no directory on disk can be deleted on the assumption that it holds one
        snapshot's worth of rows.
        """

        return self.warehouse / tier / dataset_id / "data"

    def relative(self, path: Path) -> str:
        """Express a path for storage in the catalogue, relative to the root."""

        candidate = Path(path)
        for base in (self.root, self.root.resolve()):
            try:
                return str(candidate.relative_to(base))
            except ValueError:
                continue
        try:
            return str(candidate.resolve().relative_to(self.root.resolve()))
        except ValueError as error:
            raise ValueError(
                f"Path is outside the store root and cannot be catalogued: {path}"
            ) from error

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


def _repository_fallback() -> Path:
    """Keep the default store beside this repository and out of Git."""

    source = Path(__file__).resolve()
    for parent in source.parents:
        if (parent / "pyproject.toml").is_file() and (parent / "src").is_dir():
            return parent / LOCAL_STORE_NAME
    return Path.cwd() / LOCAL_STORE_NAME


def looks_cloud_synced(path: Path) -> bool:
    lowered = str(path.expanduser().resolve(strict=False)).casefold()
    return any(marker in lowered for marker in CLOUD_MARKERS)


def resolve_store_paths(
    root: str | Path | None = None, *, for_write: bool = False
) -> StorePaths:
    """Resolve an explicit root, the environment, or the repository-local store."""

    if root is not None:
        resolved = Path(root).expanduser().resolve(strict=False)
    elif value := os.environ.get(STORE_ENV):
        resolved = Path(value).expanduser().resolve(strict=False)
    else:
        resolved = _repository_fallback().resolve(strict=False)
    if looks_cloud_synced(resolved):
        warnings.warn(
            f"Store path appears cloud-synced: {resolved}. Large mutable catalogue files "
            "can be evicted or repeatedly uploaded.",
            RuntimeWarning,
            stacklevel=2,
        )
    return StorePaths(resolved)
