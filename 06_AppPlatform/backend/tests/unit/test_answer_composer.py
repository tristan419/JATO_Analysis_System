"""Tests for answer composer — data-driven, no keyword matching."""

from app.copilot_governance.answer_composer import compose_answer, StructuredAnswer

SNAPSHOT_FULL = {
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
        ],
    },
    "overviewSummary": {"headline": "Sweden market summary"},
    "segmentMatrix": {"rows": [{"segment": "SUV-A", "currentMonth": 1234}]},
}


class TestComposeAnswer:
    def test_empty_snapshot_still_works(self):
        answer = compose_answer(snapshot={}, country="Sweden")
        assert isinstance(answer, StructuredAnswer)
        assert len(answer.blocks) >= 1  # summary always

    def test_full_snapshot_produces_all_blocks(self):
        answer = compose_answer(snapshot=SNAPSHOT_FULL, country="Sweden")
        assert len(answer.blocks) >= 4  # summary + brand + powertrain + cross-tab + segment
        block_types = {b.block_type for b in answer.blocks}
        assert "table" in block_types

    def test_sparse_snapshot_produces_fewer_blocks(self):
        sparse = {"kpis": {"brandCount": 5}}
        answer = compose_answer(snapshot=sparse, country="Norway")
        assert len(answer.blocks) <= 2  # summary only, no data to show

    def test_cross_tab_with_data_produces_evidence(self):
        answer = compose_answer(snapshot=SNAPSHOT_FULL, country="Sweden")
        assert any(b.block_type == "evidence" and "驱动" in b.title for b in answer.blocks)

    def test_powertrain_data_produces_table(self):
        snapshot = {"powertrainMix": [{"label": "BEV", "value": 100}, {"label": "ICE", "value": 50}]}
        answer = compose_answer(snapshot=snapshot)
        assert any(b.block_type == "table" and "动力" in b.title for b in answer.blocks)

    def test_answer_mode_derived_from_block_count(self):
        assert compose_answer(snapshot={}).answer_mode == "quick_answer"
        assert compose_answer(snapshot=SNAPSHOT_FULL).answer_mode == "markdown_report"
