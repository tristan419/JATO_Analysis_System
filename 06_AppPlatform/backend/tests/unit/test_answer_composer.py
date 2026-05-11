"""Tests for the answer composer module."""

from __future__ import annotations

from app.copilot_governance.answer_composer import compose_answer, StructuredAnswer, AnswerBlock
from app.copilot_governance.evidence_pack import build_evidence_pack_from_snapshot
from app.copilot_governance.source_plan import plan_sources


class TestComposeAnswer:
    def test_empty_snapshot_produces_summary(self):
        answer = compose_answer(snapshot={}, country="Sweden")
        assert isinstance(answer, StructuredAnswer)
        assert answer.summary

    def test_snapshot_with_brands_produces_table(self):
        snapshot = {
            "kpis": {"brandCount": 10, "modelCount": 50},
            "topBrands": [
                {"label": "Volvo", "value": 1000},
                {"label": "Toyota", "value": 800},
            ],
            "powertrainMix": [
                {"label": "BEV", "value": 5000},
                {"label": "ICE", "value": 3000},
            ],
            "crossTabs": {"availableDimensions": ["drive_type"]},
        }
        answer = compose_answer(snapshot=snapshot, country="Sweden")
        assert len(answer.blocks) >= 2
        block_types = {b.block_type for b in answer.blocks}
        assert "table" in block_types
        assert "summary" in block_types

    def test_with_source_plan_produces_plan_block(self):
        sp = plan_sources("brand-ranking", "销量排名")
        snapshot = {
            "kpis": {"brandCount": 5},
            "topBrands": [{"label": "A", "value": 100}],
            "powertrainMix": [{"label": "BEV", "value": 100}],
            "crossTabs": {},
        }
        answer = compose_answer(source_plan=sp, snapshot=snapshot, country="Norway")
        block_types = {b.block_type for b in answer.blocks}
        assert "evidence" in block_types or "summary" in block_types

    def test_missing_cross_tabs_adds_recommendation(self):
        snapshot = {
            "kpis": {"brandCount": 1},
            "topBrands": [{"label": "A", "value": 1}],
            "powertrainMix": [{"label": "BEV", "value": 1}],
            "crossTabs": {},
        }
        answer = compose_answer(snapshot=snapshot)
        assert any("cross" in r.lower() or "交叉" in r for r in answer.recommendations)

    def test_with_evidence_pack(self):
        sp = plan_sources("policy_tax", "碳税")
        snapshot = {
            "kpis": {"brandCount": 5},
            "topBrands": [{"label": "A", "value": 100}],
            "powertrainMix": [{"label": "BEV", "value": 100}],
            "crossTabs": {"availableDimensions": []},
        }
        ep = build_evidence_pack_from_snapshot(
            snapshot, source_plan=sp, question="碳税", intent="policy_tax", country="Sweden",
        )
        answer = compose_answer(evidence_pack=ep, source_plan=sp, snapshot=snapshot, country="Sweden")
        assert len(answer.blocks) >= 2
        assert any("evidence" in b.block_type for b in answer.blocks)

    def test_powertrain_mix_produces_table(self):
        snapshot = {
            "powertrainMix": [
                {"label": "BEV", "value": 10000},
                {"label": "PHEV", "value": 5000},
                {"label": "ICE", "value": 2000},
            ],
        }
        answer = compose_answer(snapshot=snapshot)
        tables = [b for b in answer.blocks if b.block_type == "table"]
        assert any("动力" in t.title for t in tables)
