from __future__ import annotations

import importlib.util
from pathlib import Path
import re
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SWEDEN_BACKFILL_PATH = (
    REPO_ROOT
    / "06_AppPlatform"
    / "backend"
    / "scripts"
    / "backfill_sweden_2026_official_prices.py"
)
XC60_BACKFILL_PATH = REPO_ROOT / "03_Scripts" / "backfill_xc60_structured_fields.py"


def _load(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_sweden_apply_is_rejected_before_any_database_access() -> None:
    module = _load(SWEDEN_BACKFILL_PATH, "sweden_fact_write_guard_test")

    class ExplodingSession:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("database must not be accessed")

    with pytest.raises(RuntimeError, match="approval-gated"):
        module.apply_row(
            ExplodingSession(),
            module.BACKFILL_ROWS[0],
            apply=True,
        )


def test_xc60_commit_mode_is_explicitly_fail_closed() -> None:
    module = _load(XC60_BACKFILL_PATH, "xc60_fact_write_guard_test")

    module.require_read_only_mode(commit=False)
    with pytest.raises(RuntimeError, match="approval-gated"):
        module.require_read_only_mode(commit=True)


def test_legacy_scripts_contain_no_direct_fact_write_primitive() -> None:
    sweden_source = SWEDEN_BACKFILL_PATH.read_text(encoding="utf-8")
    xc60_source = XC60_BACKFILL_PATH.read_text(encoding="utf-8")

    assert "PriceHistory(" not in sweden_source
    assert "session.add(period)" not in sweden_source
    assert "UPDATE msrp.current_prices SET" not in xc60_source
    assert "conn.execute(text(sql)" not in xc60_source


def test_production_fact_write_primitives_are_allowlisted() -> None:
    roots = [
        REPO_ROOT / "03_Scripts",
        REPO_ROOT / "06_AppPlatform" / "backend" / "app",
        REPO_ROOT / "06_AppPlatform" / "backend" / "scripts",
        REPO_ROOT / "07_ScrapingToolkit",
        REPO_ROOT / "airflow",
    ]
    primitive_pattern = re.compile(
        r"(?:CurrentPrice|PriceHistory)\(|"
        r"(?:add|delete)_(?:current_price|price_history)\s*\(|"
        r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"
        r"(?:msrp\.)?(?:current_prices|price_history)",
        re.IGNORECASE,
    )
    call_pattern = re.compile(r"materialize_current_price_from_observation\s*\(")
    primitive_files: set[str] = set()
    call_files: set[str] = set()
    for root in roots:
        for path in root.rglob("*.py"):
            relative = path.relative_to(REPO_ROOT).as_posix()
            if "/tests/" in f"/{relative}/" or "/alembic/versions/" in f"/{relative}/":
                continue
            source = path.read_text(encoding="utf-8")
            if primitive_pattern.search(source):
                primitive_files.add(relative)
            if call_pattern.search(source):
                call_files.add(relative)

    assert primitive_files == {
        "06_AppPlatform/backend/app/db/models.py",
        "06_AppPlatform/backend/app/infra/msrp_repository.py",
        "06_AppPlatform/backend/app/services/msrp_workflow_service.py",
    }
    assert call_files == {
        "06_AppPlatform/backend/app/services/msrp_materialization_service.py",
        "06_AppPlatform/backend/app/services/msrp_workflow_service.py",
    }
