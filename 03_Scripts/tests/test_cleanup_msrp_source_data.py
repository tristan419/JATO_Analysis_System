from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from uuid import uuid4

import pytest


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data_pipeline"
    / "cleanup_msrp_source_data.py"
)
SPEC = importlib.util.spec_from_file_location("cleanup_msrp_source_data", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
cleanup = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cleanup
SPEC.loader.exec_module(cleanup)


def test_cleanup_refuses_direct_current_price_or_history_deletes() -> None:
    plan = cleanup.SourceCleanupPlan(
        source_code="hu_official_source",
        source_id=uuid4(),
        observation_ids=(uuid4(),),
        review_case_ids=(),
        batch_ids=(uuid4(),),
        current_price_ids=(uuid4(),),
        finance_observation_ids=(),
        price_history_ids=(uuid4(),),
        review_decision_ids=(),
        orphan_batch_ids=(),
    )

    with pytest.raises(RuntimeError) as exc_info:
        cleanup._apply_plan(object(), plan)

    assert "persisted editor compensation approval" in str(exc_info.value)
