"""Tests for the result verifier module."""

from __future__ import annotations

from app.copilot_governance.result_verifier import (
    run_result_verification,
    verify_freshness,
    verify_no_empty,
    verify_row_count,
    verify_share_sum,
    verify_source_coverage,
)


class TestVerifyRowCount:
    def test_empty_rows_returns_warning(self):
        issues = verify_row_count([])
        assert len(issues) == 1
        assert issues[0].code == "EMPTY_RESULT"

    def test_normal_rows_no_issue(self):
        issues = verify_row_count([{"a": 1}] * 10)
        assert len(issues) == 0

    def test_exceeds_max_returns_warning(self):
        issues = verify_row_count([{"a": 1}] * 6000, max_rows=5000)
        assert len(issues) == 1
        assert issues[0].code == "ROW_LIMIT_EXCEEDED"


class TestVerifyFreshness:
    def test_no_label_returns_warning(self):
        issues = verify_freshness(None)
        assert len(issues) == 1
        assert issues[0].code == "FRESHNESS_UNKNOWN"

    def test_with_label_no_issue(self):
        issues = verify_freshness("Norway 2026年3月")
        assert len(issues) == 0


class TestVerifyShareSum:
    def test_sum_near_100_no_issue(self):
        items = [{"share": 50.0}, {"share": 49.8}]
        issues = verify_share_sum(items, share_key="share")
        assert len(issues) == 0

    def test_sum_far_from_100_returns_warning(self):
        items = [{"share": 30.0}, {"share": 20.0}]
        issues = verify_share_sum(items, share_key="share")
        assert len(issues) == 1
        assert issues[0].code == "SHARE_SUM_MISMATCH"

    def test_empty_items_no_issue(self):
        issues = verify_share_sum([])
        assert len(issues) == 0


class TestVerifySourceCoverage:
    def test_missing_source_returns_warning(self):
        from app.copilot_governance.source_plan import SourcePlan, SourcePlanItem
        sp = SourcePlan(items=[SourcePlanItem(source_id="missing_src", source_lane="voc", required=True)])
        issues = verify_source_coverage(sp, ["jato_sales_parquet"])
        assert len(issues) == 1
        assert issues[0].code == "SOURCE_MISSING"

    def test_all_covered_no_issue(self):
        from app.copilot_governance.source_plan import SourcePlan, SourcePlanItem
        sp = SourcePlan(items=[SourcePlanItem(source_id="jato_sales_parquet", source_lane="structured_bi", required=True)])
        issues = verify_source_coverage(sp, ["jato_sales_parquet"])
        assert len(issues) == 0


class TestVerifyNoEmpty:
    def test_empty_key_returns_info(self):
        issues = verify_no_empty({}, "topBrands", "头部品牌")
        assert len(issues) == 1
        assert issues[0].code == "FIELD_EMPTY"

    def test_present_key_no_issue(self):
        issues = verify_no_empty({"topBrands": [{"brand": "A"}]}, "topBrands")
        assert len(issues) == 0


class TestRunResultVerification:
    def test_empty_snapshot_warns(self):
        report = run_result_verification({})
        assert report.status == "warning"
        assert len(report.issues) >= 1

    def test_good_snapshot_passes(self):
        snapshot = {
            "ytdBrandRanking": [{"brand": "A", "volume": 100}],
            "powertrainMix": [{"label": "BEV", "value": 5000}, {"label": "ICE", "value": 5000}],
            "topBrands": [{"label": "A", "value": 100}],
            "periodLabel": "Norway 2026年3月",
        }
        report = run_result_verification(snapshot)
        assert report.status in ("pass", "warning")
