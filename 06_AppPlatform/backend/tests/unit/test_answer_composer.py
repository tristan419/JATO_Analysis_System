"""Tests for the answer composer module."""

from __future__ import annotations

from app.copilot_governance.answer_composer import compose_answer, StructuredAnswer

SNAPSHOT_BASE = {
    "kpis": {"brandCount": 10, "modelCount": 50},
    "topBrands": [
        {"label": "Volvo", "value": 1000},
        {"label": "Toyota", "value": 800},
        {"label": "VW", "value": 700},
    ],
    "powertrainMix": [
        {"label": "BEV", "value": 5000},
        {"label": "ICE", "value": 3000},
        {"label": "PHEV", "value": 1000},
    ],
    "crossTabs": {
        "availableDimensions": ["drive_type"],
        "driveByFuel": [
            {"_index": "BEV", "4WD_pct": 35.2, "_total": 5000},
            {"_index": "ICE", "4WD_pct": 25.8, "_total": 3000},
        ],
    },
    "overviewSummary": {"headline": "Sweden market summary"},
}


class TestComposeAnswer:
    def test_simple_fact_question_minimal_blocks(self):
        """'你好' → quick answer, no tables."""
        answer = compose_answer(snapshot=SNAPSHOT_BASE, country="Sweden", question="你好")
        assert answer.answer_mode == "quick_answer"
        assert len(answer.blocks) <= 2

    def test_bev_share_gets_powertrain_block(self):
        """'BEV占比' → powertrain question, gets table."""
        answer = compose_answer(snapshot=SNAPSHOT_BASE, country="Sweden", question="BEV占比多少")
        assert any(b.block_type == "table" and "动力" in b.title for b in answer.blocks)

    def test_ranking_question_gets_table(self):
        """'什么车卖得好' → has ranking table."""
        answer = compose_answer(snapshot=SNAPSHOT_BASE, country="Sweden", question="什么车卖的最好")
        assert any(b.block_type == "table" and "品牌" in b.title for b in answer.blocks)

    def test_powertrain_question_gets_powertrain_table(self):
        """'动力结构' → powertrain table."""
        answer = compose_answer(snapshot=SNAPSHOT_BASE, country="Sweden", question="动力结构是什么")
        assert any(b.block_type == "table" and "动力" in b.title for b in answer.blocks)

    def test_causal_question_gets_cross_tab(self):
        """'为什么BEV下滑' → cross-tab evidence."""
        answer = compose_answer(snapshot=SNAPSHOT_BASE, country="Sweden", question="为什么BEV销量下滑")
        assert any(b.block_type == "evidence" and "交叉" in b.title for b in answer.blocks)

    def test_tax_question_marks_strategy(self):
        """Tax question → strategy_brief mode."""
        answer = compose_answer(snapshot=SNAPSHOT_BASE, country="Sweden", question="瑞典碳税怎么算")
        assert answer.answer_mode in ("strategy_brief", "quick_answer")

    def test_empty_snapshot_still_works(self):
        answer = compose_answer(snapshot={}, country="Sweden", question="你好")
        assert isinstance(answer, StructuredAnswer)
        assert answer.blocks

    def test_question_type_detection(self):
        from app.copilot_governance.answer_composer import _is_simple_fact
        assert _is_simple_fact("你好") is True
        assert _is_simple_fact("销量怎么样") is True
        assert _is_simple_fact("为什么bev下滑 驱动因素") is False
        assert _is_simple_fact("什么车卖的最好") is False  # ranking
