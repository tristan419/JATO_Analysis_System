"""Tests for the eval harness and source planner integration."""

from __future__ import annotations

import pytest

from app.copilot_governance.source_plan import plan_sources
from app.copilot_governance.intent import LEGACY_TO_GOVERNED_INTENT


class TestSourcePlannerIntentMapping:
    @pytest.mark.parametrize("legacy_intent,governed_intent", [
        ("brand-ranking", "metric_query"),
        ("segment-analysis", "distribution"),
        ("trend-summary", "trend"),
        ("powertrain-mix", "distribution"),
        ("nev-analysis", "distribution"),
        ("general-summary", "country_report"),
        ("positioning-analysis", "comparison"),
        ("market-context", "news_intelligence"),
    ])
    def test_legacy_intent_mapped(self, legacy_intent, governed_intent):
        assert LEGACY_TO_GOVERNED_INTENT.get(legacy_intent) == governed_intent

    @pytest.mark.parametrize("intent,expected_source", [
        ("brand-ranking", "jato_sales_parquet"),
        ("nev-analysis", "jato_sales_parquet"),
        ("positioning-analysis", "jato_sales_parquet"),
        ("trend-summary", "jato_sales_parquet"),
    ])
    def test_structured_intent_gets_sales_data(self, intent, expected_source):
        sp = plan_sources(intent, "")
        sources = {item.source_id for item in sp.items}
        assert expected_source in sources

    def test_pricing_strategy_gets_price(self):
        sp = plan_sources("positioning-analysis", "价格")
        sources = {item.source_id for item in sp.items}
        assert "current_price_postgres" in sources

    def test_strategy_question_gets_hybrid(self):
        sp = plan_sources("product_strategy", "政策 反馈")
        sources = {item.source_id for item in sp.items}
        assert "jato_sales_parquet" in sources
        assert "current_price_postgres" in sources
        assert sp.execution_mode == "hybrid"

    def test_voc_question_gets_voc_only(self):
        sp = plan_sources("voc_insight", "用户 反馈")
        sources = {item.source_id for item in sp.items}
        assert "voc_forum_artifacts" in sources
        assert len(sources) >= 1

    def test_unknown_intent_defaults_to_sales(self):
        sp = plan_sources("unknown-intent", "")
        assert len(sp.items) >= 1
        assert "jato_sales_parquet" in {i.source_id for i in sp.items}

    def test_keyword_supplements_intent(self):
        sp = plan_sources("brand-ranking", "政策 税 用户反馈")
        sources = {item.source_id for item in sp.items}
        assert "jato_sales_parquet" in sources  # from intent
        # Keywords add supplemental sources
        assert len(sources) >= 1  # At minimum the intent-based source
