from __future__ import annotations

import ast
from pathlib import Path


VERSIONS_DIR = Path(__file__).resolve().parents[2] / "alembic" / "versions"


def _literal_assignment(module: ast.Module, name: str) -> object:
    for node in module.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"missing Alembic assignment: {name}")


def _revision_values(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    if isinstance(value, (tuple, list)):
        return {item for item in value if isinstance(item, str)}
    raise AssertionError(f"unsupported down_revision value: {value!r}")


def test_alembic_revision_chain_has_no_missing_parents() -> None:
    revisions: set[str] = set()
    parents_by_revision: dict[str, set[str]] = {}

    for path in VERSIONS_DIR.glob("*.py"):
        module = ast.parse(path.read_text(encoding="utf-8"))
        revision = _literal_assignment(module, "revision")
        assert isinstance(revision, str)
        assert revision not in revisions
        revisions.add(revision)
        parents_by_revision[revision] = _revision_values(
            _literal_assignment(module, "down_revision")
        )

    missing = {
        revision: sorted(parent for parent in parents if parent not in revisions)
        for revision, parents in parents_by_revision.items()
        if any(parent not in revisions for parent in parents)
    }
    assert missing == {}


def test_alembic_revision_chain_has_single_head() -> None:
    revisions: set[str] = set()
    parent_revisions: set[str] = set()

    for path in VERSIONS_DIR.glob("*.py"):
        module = ast.parse(path.read_text(encoding="utf-8"))
        revision = _literal_assignment(module, "revision")
        assert isinstance(revision, str)
        revisions.add(revision)
        parent_revisions.update(
            _revision_values(_literal_assignment(module, "down_revision"))
        )

    assert sorted(revisions - parent_revisions) == ["20260709_0043"]
