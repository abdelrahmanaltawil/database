from __future__ import annotations

import ast
from pathlib import Path

SRC = Path(__file__).parents[1] / "src" / "research_store"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def test_dependencies_point_toward_foundation() -> None:
    forbidden_from_foundation = (
        "research_store.access",
        "research_store.ingestion",
        "research_store.derived",
    )
    for path in (SRC / "foundation").glob("*.py"):
        for imported in _imports(path):
            assert not imported.startswith(forbidden_from_foundation), (path, imported)

    for path in (SRC / "ingestion").glob("*.py"):
        for imported in _imports(path):
            if imported.startswith("research_store."):
                assert imported.startswith("research_store.foundation"), (
                    path,
                    imported,
                )


def test_dataset_specs_are_declared_in_exactly_one_source_file() -> None:
    declarations: list[Path] = []
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                name = node.func.id if isinstance(node.func, ast.Name) else None
                if name == "DatasetSpec":
                    declarations.append(path.relative_to(SRC))
    assert declarations
    assert set(declarations) == {Path("foundation/registry.py")}


def test_parquet_writes_exist_only_in_foundation_writer() -> None:
    offenders: list[Path] = []
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in {"write_table", "to_parquet", "write_dataset"}
                and path != SRC / "foundation" / "writer.py"
            ):
                offenders.append(path.relative_to(SRC))
    assert offenders == []
