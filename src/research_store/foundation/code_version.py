"""Which code is writing to the store, and whether a commit holds it.

A snapshot can only be reproduced if the code that produced it can be found
again. Every run that writes therefore records the git commit of the installed
`research_store` package, and a writer refuses to start from code that no
commit holds: uncommitted edits, untracked modules, or an installation that is
not a git checkout at all. The climate-hourly ingestion of 2026-09-28 ran on
uncommitted modules, and its report had to reconstruct which code ran from
file modification times; this makes that impossible to repeat by accident.

Only the package and `pyproject.toml` are inspected. Editing a report, a test
or a notebook while an ingestion runs is not a change to the code that writes.

The working tree is read once per process, when the first writer opens. That is
also when the code that writes has been imported, so later edits to the files
do not change what the process runs and are rightly not recorded.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from functools import cache
from pathlib import Path

ALLOW_UNCOMMITTED_ENV = "RESEARCH_STORE_ALLOW_UNCOMMITTED_CODE"


class UncommittedCodeError(RuntimeError):
    """The code about to write is not exactly a git commit."""


@dataclass(frozen=True, slots=True)
class CodeVersion:
    """The commit of the package, and what in it differs from that commit.

    `uncommitted` is empty when the package and `pyproject.toml` equal
    `commit`, lists the differing paths (relative to the repository) when they
    do not, and is None, like `commit`, when neither could be determined;
    `reason` then says why.
    """

    commit: str | None
    uncommitted: tuple[str, ...] | None
    reason: str | None = None

    @property
    def committed(self) -> bool:
        return self.commit is not None and self.uncommitted == ()

    def describe(self) -> str:
        if self.commit is None:
            return f"unknown code version ({self.reason})"
        if not self.uncommitted:
            return f"commit {self.commit}"
        return (
            f"commit {self.commit} plus uncommitted changes to "
            + ", ".join(self.uncommitted)
        )


def _git(arguments: list[str], cwd: Path) -> str:
    # --no-optional-locks keeps `status` from taking the index lock, so it
    # cannot collide with git commands the user runs at the same time.
    return subprocess.run(
        ["git", "--no-optional-locks", *arguments],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    ).stdout


def _changed_paths(porcelain: str) -> tuple[str, ...]:
    """Paths named by `git status --porcelain=v1 -z`.

    Each entry is a two-letter status, a space and a path. A rename or copy is
    followed by a bare field holding the path it came from; a renamed-away
    path differs from the commit too, a copied-from one does not.
    """

    fields = porcelain.split("\0")
    paths: set[str] = set()
    index = 0
    while index < len(fields):
        entry = fields[index]
        index += 1
        if not entry:
            continue
        status, path = entry[:2], entry[3:]
        paths.add(path)
        if ("R" in status or "C" in status) and index < len(fields):
            if "R" in status:
                paths.add(fields[index])
            index += 1
    return tuple(sorted(paths))


def probe_code_version(package_dir: Path) -> CodeVersion:
    """Read the commit and uncommitted changes of the package at `package_dir`."""

    package_dir = package_dir.resolve()
    try:
        top = Path(_git(["rev-parse", "--show-toplevel"], package_dir).strip())
    except FileNotFoundError:
        return CodeVersion(None, None, "git is not installed")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return CodeVersion(
            None, None, f"{package_dir} is not inside a git checkout"
        )
    top = top.resolve()
    if not package_dir.is_relative_to(top):
        return CodeVersion(None, None, f"{package_dir} is outside the checkout {top}")
    package = package_dir.relative_to(top)
    scope = [str(package)]
    pyproject = package_dir.parent.parent / "pyproject.toml"
    if pyproject.is_file() and pyproject.resolve().is_relative_to(top):
        scope.append(str(pyproject.resolve().relative_to(top)))
    try:
        # A package that the surrounding repository ignores or does not track,
        # such as one installed into a virtualenv inside a checkout, is not
        # the code at that checkout's commit.
        _git(["ls-files", "--error-unmatch", "--", str(package / "__init__.py")], top)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return CodeVersion(
            None, None, f"{package_dir} is not tracked by the git checkout at {top}"
        )
    try:
        commit = _git(["rev-parse", "--verify", "HEAD"], top).strip()
        status = _git(
            ["status", "--porcelain=v1", "-z", "--untracked-files=all", "--", *scope],
            top,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        detail = getattr(error, "stderr", None) or str(error)
        return CodeVersion(
            None, None, f"git could not read the checkout at {top}: {detail.strip()}"
        )
    return CodeVersion(commit, _changed_paths(status))


@cache
def current_code_version() -> CodeVersion:
    """The code version of this installed package, read once per process."""

    return probe_code_version(Path(__file__).resolve().parents[1])


def uncommitted_code_allowed() -> bool:
    return os.environ.get(ALLOW_UNCOMMITTED_ENV) == "1"


def code_version_for_write() -> CodeVersion:
    """The code version to record, refusing code that no commit holds."""

    version = current_code_version()
    if version.committed or uncommitted_code_allowed():
        return version
    raise UncommittedCodeError(
        f"Refusing to write to the store from {version.describe()}. A run must "
        f"record the commit that produced it, so commit the code first. To "
        f"write anyway, for development or a scratch store, pass "
        f"--allow-uncommitted-code or set {ALLOW_UNCOMMITTED_ENV}=1; the "
        f"uncommitted paths are then recorded with each run."
    )
