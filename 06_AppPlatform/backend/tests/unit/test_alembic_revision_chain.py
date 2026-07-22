from __future__ import annotations

import ast
import importlib.util
from io import StringIO
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations


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

    assert sorted(revisions - parent_revisions) == ["20260714_0044"]


def test_governance_revision_generates_isolated_postgresql_sql() -> None:
    revision_path = (
        VERSIONS_DIR / "20260714_0044_msrp_source_governance_foundation.py"
    )
    spec = importlib.util.spec_from_file_location(
        "governance_revision_0044",
        revision_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    module.op = Operations(context)

    module.upgrade()

    sql = output.getvalue()
    assert "CREATE TABLE msrp.governance_gate_decisions" in sql
    assert "CREATE TABLE msrp.result_correction_decisions" in sql
    assert "CREATE TABLE msrp.fx_normalization_runs" in sql
    assert "gate_decision_id UUID NOT NULL" in sql

    downgrade_output = StringIO()
    downgrade_context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": downgrade_output},
    )
    module.op = Operations(downgrade_context)
    module.downgrade()

    downgrade_sql = downgrade_output.getvalue()
    assert "DROP TABLE msrp.result_correction_decisions" in downgrade_sql
    assert "DROP TABLE msrp.fx_normalization_runs" in downgrade_sql
    assert "DROP TABLE msrp.governance_gate_decisions" in downgrade_sql
