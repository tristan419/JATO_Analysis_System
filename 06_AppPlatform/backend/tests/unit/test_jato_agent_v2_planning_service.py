from __future__ import annotations

from app.services.jato_agent_eval_v2_service import check_golden_question_v2
from app.services.jato_agent_eval_v2_service import list_golden_questions_v2
from app.services.jato_agent_eval_v2_service import run_eval_v2
from app.services.jato_agent_llm_judge_service import judge_answer_with_llm
from app.services.jato_agent_llm_judge_service import preflight_judge_provider
from app.services import jato_agent_llm_judge_service as llm_judge_service
from app.services.jato_answer_grounding_service import apply_answer_grounding_guard
from app.services.jato_agent_deterministic_judge_service import score_deterministic_answer
from app.services.jato_evidence_package_service import build_evidence_package
from app.services.jato_evidence_package_service import evidence_ref_count
from app.services.jato_agent_planning_service import build_evidence_plan
from app.services.jato_followup_service import normalize_follow_ups
from app.services.jato_intent_tool_matrix_service import get_intent_tool_rule
from app.services.jato_tool_registry_service import allowed_tools_for_intent
from app.services.jato_tool_registry_service import get_tool_card
from app.services.jato_tool_registry_service import list_tool_cards
from app.services.jato_tool_coverage_guard_service import missing_required_tools
from app.services.jato_tool_coverage_guard_service import tool_satisfies_required


def _market_chart_context_tool_result(powertrain: str = "HEV") -> dict:
    return {
        "toolName": "build_market_chart",
        "query": {"country": "Sweden", "intent": "pricing_analysis"},
        "success": True,
        "result": {
            "tool": "build_market_chart",
            "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 1},
            "data": {
                "extractedParams": {"country": "Sweden", "powertrain": powertrain},
                "contextSnapshot": {
                    "powertrainMix": [
                        {"label": "BEV", "value": 25235, "share": 0.42},
                        {"label": "PHEV", "value": 15028, "share": 0.25},
                        {"label": "HEV", "value": 5051, "share": 0.08},
                    ],
                },
            },
        },
    }


def test_tool_registry_exposes_pricing_tool_card() -> None:
    card = get_tool_card("query_msrp_pricing")

    assert card is not None
    assert "pricing_analysis" in card["intentTags"]
    assert card["requiresTenantData"] is True
    assert "query_msrp_pricing" in allowed_tools_for_intent("pricing_analysis")
    assert len(list_tool_cards()) >= 10


def test_tool_registry_exposes_leasing_and_existing_lens_cards() -> None:
    leasing = get_tool_card("query_leasing_offers")

    assert leasing is not None
    assert leasing["requiresTenantData"] is True
    assert "pricing_analysis" in leasing["intentTags"]
    assert get_tool_card("query_time_series") is not None
    assert get_tool_card("query_price_positioning") is not None
    assert get_tool_card("query_competitive_landscape") is not None


def test_judge_provider_preflight_reports_disabled(monkeypatch) -> None:
    monkeypatch.delenv("APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED", raising=False)
    monkeypatch.delenv("APP_ASTRBOT_AUTO_HUMAN_JUDGE_ENABLED", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    result = preflight_judge_provider(live_check=False)

    assert result["ready"] is False
    assert result["enabled"] is False
    assert result["missingKey"] is True
    assert result["status"] == "disabled"


def test_judge_provider_preflight_can_skip_live_check(monkeypatch) -> None:
    monkeypatch.setenv("APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED", "true")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    result = preflight_judge_provider(live_check=False)

    assert result["ready"] is True
    assert result["enabled"] is True
    assert result["missingKey"] is False
    assert result["status"] == "ready"


def test_judge_provider_preflight_live_check(monkeypatch) -> None:
    monkeypatch.setenv("APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED", "true")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(llm_judge_service, "_post_chat", lambda *_args, **_kwargs: '{"ok":true}')

    result = preflight_judge_provider()

    assert result["ready"] is True
    assert result["status"] == "ok"
    assert result["responsePreview"] == '{"ok":true}'


def test_evidence_plan_routes_pricing_question_to_price_tools() -> None:
    plan = build_evidence_plan("Sweden", "瑞典 J7 HEV 应该如何定价？")

    assert plan["intent"] == "pricing_analysis"
    assert "query_msrp_pricing" in plan["allowedTools"]
    assert plan["requiredTools"] == ["query_msrp_pricing", "build_market_chart"]
    assert "own_model_price" in plan["mustHaveEvidence"]
    assert "market_context" in plan["mustHaveEvidence"]
    assert plan["answerMode"] == "analysis"
    assert any(item["name"] == "current_msrp" for item in plan["evidenceNeeded"])


def test_blind_leasing_question_requires_offer_data_and_scopes_models() -> None:
    plan = build_evidence_plan(
        "Sweden",
        "Nimbus E BEV 和 Solaris One BEV 的 36 个月月供、RV 和总持有成本怎么比较？",
    )

    assert plan["intent"] == "pricing_analysis"
    assert "query_leasing_offers" in plan["requiredTools"]
    assert any(
        item["name"] == "leasing_tco_or_company_car_evidence"
        for item in plan["evidenceNeeded"]
    )
    step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_leasing_offers")
    assert step["input"]["models"] == ["Nimbus E BEV", "Solaris One BEV"]
    assert "monthly payment" in step["expectedEvidence"]


def test_evidence_plan_requires_competitor_corridor_for_price_reasonableness() -> None:
    plan = build_evidence_plan("Sweden", "O9 在瑞典 53k-55k 欧元是否合理？")

    assert plan["intent"] == "pricing_analysis"
    assert plan["entities"]["models"] == ["O9"]
    assert plan["requiredTools"] == ["query_msrp_pricing", "build_market_chart", "compare_competitive_set", "query_price_positioning"]
    assert "compare_competitive_set" in plan["allowedTools"]
    assert "build_market_chart" in plan["allowedTools"]
    assert "query_price_positioning" in plan["allowedTools"]
    msrp_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_msrp_pricing")
    assert msrp_step["input"]["model"] == "O9"
    price_positioning_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_price_positioning")
    assert "model" not in price_positioning_step["input"]
    assert "price_corridor" in [item["name"] for item in plan["evidenceNeeded"]]


def test_evidence_plan_uses_j7_method_competitor_pool_as_tool_seed() -> None:
    plan = build_evidence_plan("Sweden", "瑞典 J7 HEV 应该怎么定价？请给出竞品价格走廊、数据支撑和图表。")

    assert plan["intent"] == "pricing_analysis"
    assert "Corolla Cross" in plan["entities"]["competitors"]
    assert "RAV4" in plan["entities"]["competitors"]
    msrp_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_msrp_pricing")
    compare_step = next(item for item in plan["toolPlan"] if item["toolName"] == "compare_competitive_set")

    assert msrp_step["input"]["model"] == "J7 HEV"
    assert "J7 HEV" in msrp_step["input"]["models"]
    assert "Corolla Cross" in msrp_step["input"]["models"]
    assert "RAV4" in msrp_step["input"]["models"]
    assert compare_step["input"]["competitors"][:2] == ["Corolla Cross", "RAV4"]


def test_pricing_with_competitor_configuration_and_market_context_stays_pricing() -> None:
    plan = build_evidence_plan(
        "Sweden",
        "瑞典 J7 HEV 应该怎么定价？请结合 HEV 市场、核心竞品、价格走廊、配置差异和图表，给产品经理结论。",
    )

    assert plan["intent"] == "pricing_analysis"
    assert plan["requiredTools"] == [
        "query_msrp_pricing",
        "build_market_chart",
        "compare_competitive_set",
        "query_price_positioning",
        "compare_vehicle_variants",
        "query_country_snapshot",
    ]
    assert "Corolla Cross" in plan["entities"]["competitors"]
    assert "RAV4" in plan["entities"]["competitors"]
    assert [item["toolName"] for item in plan["toolPlan"]] == plan["requiredTools"]


def test_evidence_plan_routes_why_question_to_cross_reference_tools() -> None:
    plan = build_evidence_plan("Germany", "Why does VW ID.7 sell better than expected?")

    assert plan["intent"] == "market_overview"
    assert "query_country_snapshot" in plan["requiredTools"]
    assert "build_market_chart" in plan["requiredTools"]
    assert "analyze_model_performance" in plan["allowedTools"]
    assert "external_research" not in plan["allowedTools"]


def test_evidence_plan_routes_model_validation_question_to_market_overview() -> None:
    plan = build_evidence_plan("Hungary", "匈牙利 J7 HEV 是否值得继续验证？请简短回答。")

    assert plan["intent"] == "market_overview"
    assert "J7 HEV" in plan["entities"]["models"]
    assert plan["requiredTools"] == [
        "query_country_snapshot",
        "build_market_chart",
        "query_segment_breakdown",
    ]
    assert "query_country_snapshot" in plan["allowedTools"]
    assert "build_market_chart" in plan["allowedTools"]
    assert "query_segment_breakdown" in plan["allowedTools"]
    assert "compare_competitive_set" not in plan["requiredTools"]
    assert "query_msrp_pricing" not in plan["requiredTools"]
    assert "query_with_filters" in plan["allowedTools"]
    segment_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_segment_breakdown")
    assert segment_step["input"]["powertrain"] == "HEV"
    assert "external_research" not in plan["allowedTools"]
    assert plan["answerMode"] == "analysis"


def test_market_monthly_trend_question_requires_scoped_time_series() -> None:
    plan = build_evidence_plan(
        "Sweden",
        "匈牙利 HEV 市场最近的规模、月度走势和主销 SUV 级别是什么？",
    )

    assert plan["country"] == "Hungary"
    assert plan["intent"] == "market_overview"
    assert "query_time_series" in plan["requiredTools"]
    step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_time_series")
    assert step["input"]["country"] == "Hungary"
    assert step["input"]["powertrain"] == "HEV"
    assert step["input"]["granularity"] == "monthly"


def test_market_overview_with_competitor_request_requires_competitive_evidence() -> None:
    plan = build_evidence_plan(
        "Sweden",
        "瑞典 HEV 市场为什么适合 J7？请用销量、SUV A0/A 结构、竞品和图表支持，给产品经理结论。",
    )

    assert plan["intent"] == "market_overview"
    assert "J7" in plan["entities"]["models"]
    assert plan["entities"]["competitors"][:4] == ["Corolla Cross", "RAV4", "C-HR", "Qashqai"]
    assert plan["requiredTools"] == [
        "query_country_snapshot",
        "build_market_chart",
        "query_segment_breakdown",
        "compare_competitive_set",
        "query_msrp_pricing",
    ]
    assert "compare_competitive_set" in plan["allowedTools"]
    assert "query_msrp_pricing" in plan["allowedTools"]
    compare_step = next(item for item in plan["toolPlan"] if item["toolName"] == "compare_competitive_set")
    msrp_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_msrp_pricing")
    assert compare_step["input"]["competitors"][:4] == ["Corolla Cross", "RAV4", "C-HR", "Qashqai"]
    assert compare_step["input"]["model"] == "J7"
    assert msrp_step["input"]["models"][:5] == ["J7", "Corolla Cross", "RAV4", "C-HR", "Qashqai"]


def test_evidence_plan_keeps_powertrain_route_comparison_unfiltered() -> None:
    plan = build_evidence_plan("Sweden", "匈牙利市场现在适合推 PHEV 还是 HEV？请基于数据给结论。")

    assert plan["country"] == "Hungary"
    assert plan["entities"]["powertrains"] == ["PHEV", "HEV"]
    segment_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_segment_breakdown")
    assert "powertrain" not in segment_step["input"]
    assert segment_step["input"]["country"] == "Hungary"


def test_evidence_plan_powertrain_route_with_comparison_table_stays_market_overview() -> None:
    plan = build_evidence_plan("Sweden", "匈牙利市场现在适合推 PHEV 还是 HEV？请基于数据给结论，并展示图表和对比表。")

    assert plan["country"] == "Hungary"
    assert plan["intent"] == "market_overview"
    assert "compare_vehicle_variants" not in plan["allowedTools"]
    assert "query_segment_breakdown" in plan["requiredTools"]
    segment_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_segment_breakdown")
    assert "powertrain" not in segment_step["input"]


def test_evidence_plan_country_mention_overrides_ui_default_country() -> None:
    plan = build_evidence_plan("Sweden", "匈牙利 J7 HEV 市场情况怎么样？")

    assert plan["country"] == "Hungary"
    assert plan["entities"]["countries"] == ["Hungary"]
    assert plan["intent"] == "market_overview"
    assert "query_country_snapshot" in plan["allowedTools"]
    assert "query_cross_country" not in plan["allowedTools"]
    assert all(
        item["input"].get("country") == "Hungary"
        for item in plan["toolPlan"]
        if item["toolName"] != "query_cross_country"
    )
    assert all(
        "Sweden" not in str(item["input"])
        for item in plan["toolPlan"]
    )


def test_evidence_plan_market_situation_with_chart_and_table_stays_market_overview() -> None:
    plan = build_evidence_plan("Sweden", "匈牙利 J7 HEV 市场情况怎么样？请基于数据给结论，并展示图表和对比表。")

    assert plan["country"] == "Hungary"
    assert plan["intent"] == "market_overview"
    assert plan["requiredTools"] == [
        "query_country_snapshot",
        "build_market_chart",
        "query_segment_breakdown",
    ]
    assert "compare_vehicle_variants" not in plan["allowedTools"]
    assert "compare_competitive_set" not in plan["requiredTools"]
    assert "query_msrp_pricing" not in plan["requiredTools"]


def test_evidence_plan_pricing_country_mention_overrides_ui_default_country() -> None:
    plan = build_evidence_plan("Sweden", "Hungary J7 HEV pricing outlook?")

    assert plan["country"] == "Hungary"
    assert plan["entities"]["countries"] == ["Hungary"]
    assert plan["intent"] == "pricing_analysis"
    msrp_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_msrp_pricing")
    assert msrp_step["input"]["country"] == "Hungary"
    assert msrp_step["input"]["model"] == "J7 HEV"


def test_evidence_plan_extracts_sorento_as_requested_competitor() -> None:
    plan = build_evidence_plan("Sweden", "J8 7 座四驱为什么能打 Sorento？")

    assert plan["intent"] == "competitor_compare"
    assert "J8" in plan["entities"]["models"]
    assert "Sorento" in plan["entities"]["models"]
    assert "Sorento" in plan["entities"]["competitors"]
    assert plan["requiredTools"][:3] == ["compare_competitive_set", "build_market_chart", "query_msrp_pricing"]
    assert "build_market_chart" in plan["allowedTools"]


def test_named_competitor_question_with_price_and_configuration_evidence_stays_competitor_compare() -> None:
    plan = build_evidence_plan(
        "Sweden",
        "瑞典 J8 7座四驱为什么能打 Sorento？请基于销量、动力、级别、配置和价格证据给出竞争结论，并生成对比表。",
    )

    assert plan["intent"] == "competitor_compare"
    assert plan["requiredTools"][:3] == ["compare_competitive_set", "build_market_chart", "query_msrp_pricing"]
    assert "compare_vehicle_variants" in plan["requiredTools"]
    assert "Sorento" in plan["entities"]["competitors"]


def test_evidence_plan_extracts_named_competitors_into_tool_inputs() -> None:
    plan = build_evidence_plan("Hungary", "匈牙利 T7 HEV 应该对标 Corolla Cross 还是 Tucson？")

    assert plan["intent"] == "competitor_compare"
    assert plan["country"] == "Hungary"
    assert plan["entities"]["models"][0] == "T7 HEV"
    assert "Corolla Cross" in plan["entities"]["competitors"]
    assert "Tucson" in plan["entities"]["competitors"]

    compare_step = next(item for item in plan["toolPlan"] if item["toolName"] == "compare_competitive_set")
    msrp_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_msrp_pricing")

    assert compare_step["input"]["country"] == "Hungary"
    assert compare_step["input"]["competitors"][:2] == ["Corolla Cross", "Tucson"]
    assert "T7 HEV" in compare_step["input"]["models"]
    assert "Corolla Cross" in compare_step["input"]["models"]
    assert "Tucson" in compare_step["input"]["models"]
    assert msrp_step["input"]["competitors"][:2] == ["Corolla Cross", "Tucson"]


def test_evidence_plan_routes_source_research_to_governed_search() -> None:
    plan = build_evidence_plan("Sweden", "Research latest EV policy sources with citations")

    assert plan["intent"] == "news_policy_search"
    assert plan["requiredTools"] == ["search_market_news", "query_country_snapshot", "build_market_chart"]
    assert "query_country_snapshot" in plan["allowedTools"]
    assert "build_market_chart" in plan["allowedTools"]
    assert "search_market_news" in plan["allowedTools"]
    assert [item["toolName"] for item in plan["toolPlan"][:3]] == [
        "search_market_news",
        "query_country_snapshot",
        "build_market_chart",
    ]
    assert plan["shouldUseWeb"] is True


def test_evidence_plan_routes_sweden_policy_business_terms_to_research() -> None:
    plan = build_evidence_plan("Sweden", "Elbilspremien 2026 会影响哪些车型？")

    assert plan["intent"] == "news_policy_search"
    assert "search_market_news" in plan["requiredTools"]
    assert "query_country_snapshot" in plan["requiredTools"]
    assert "build_market_chart" in plan["requiredTools"]
    assert "search_market_news" in plan["allowedTools"]
    assert plan["shouldUseWeb"] is True


def test_evidence_plan_requires_price_tool_for_policy_price_cap_question() -> None:
    plan = build_evidence_plan("Sweden", "BEV 补贴价格上限对 O5 BEV 定价有什么影响？")

    assert plan["intent"] == "news_policy_search"
    assert plan["requiredTools"] == [
        "search_market_news",
        "query_country_snapshot",
        "build_market_chart",
        "query_msrp_pricing",
    ]
    assert "query_msrp_pricing" in plan["allowedTools"]
    assert [item["toolName"] for item in plan["toolPlan"][:4]] == [
        "search_market_news",
        "query_country_snapshot",
        "build_market_chart",
        "query_msrp_pricing",
    ]


def test_evidence_plan_routes_material_number_questions_to_inventory() -> None:
    plan = build_evidence_plan("Sweden", "OMODA9 一个版型多个物料号应该怎么解释？")

    assert plan["intent"] == "inventory_analysis"
    assert plan["requiredTools"] == ["query_country_snapshot", "query_with_filters"]
    assert "query_with_filters" in plan["allowedTools"]


def test_evidence_plan_routes_pi_market_split_questions_to_inventory() -> None:
    plan = build_evidence_plan("Sweden", "SE/FI 合并 PI 但车辆分市场生成，逻辑是否正确？")

    assert plan["intent"] == "inventory_analysis"
    assert plan["entities"]["countries"] == ["Sweden", "Finland"]
    assert plan["requiredTools"] == ["query_cross_country", "query_with_filters"]
    assert plan["allowedTools"][0] == "query_cross_country"
    assert "query_with_filters" in plan["allowedTools"]
    cross_country_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_cross_country")
    assert cross_country_step["input"]["countries"] == "Sweden, Finland"


def test_evidence_plan_routes_battery_value_questions_to_configuration() -> None:
    plan = build_evidence_plan("Sweden", "A0 SUV BEV 为什么需要 80kWh 电池？请给出市场结构、竞品配置逻辑和图表。")

    assert plan["intent"] == "configuration_analysis"
    assert "80kWh" in plan["entities"]["features"]
    assert plan["requiredTools"] == [
        "compare_competitive_set",
        "compare_vehicle_variants",
        "query_country_snapshot",
        "build_market_chart",
    ]
    assert plan["toolPlan"][0]["toolName"] == "compare_competitive_set"
    assert "compare_competitive_set" in plan["allowedTools"]
    assert "query_country_snapshot" in plan["allowedTools"]
    assert "build_market_chart" in plan["allowedTools"]
    variant_step = next(item for item in plan["toolPlan"] if item["toolName"] == "compare_vehicle_variants")
    assert "80kWh" in variant_step["input"]["features"]


def test_evidence_plan_adds_named_competitors_to_configuration_variant_args() -> None:
    plan = build_evidence_plan("Sweden", "O5 BEV 和 EV3 的配置差异是什么？")

    assert plan["intent"] == "configuration_analysis"
    variant_step = next(item for item in plan["toolPlan"] if item["toolName"] == "compare_vehicle_variants")
    assert "O5 BEV" in variant_step["input"]["models"]
    assert "EV3" in variant_step["input"]["models"]
    assert variant_step["input"]["competitors"] == ["EV3"]


def test_evidence_plan_routes_configuration_comparison_table_to_configuration() -> None:
    plan = build_evidence_plan(
        "Sweden",
        "O5 BEV 和 Kia EV3 的配置差异怎么讲？请用数据支撑结论，并给出配置对比表。",
    )

    assert plan["intent"] == "configuration_analysis"
    assert plan["requiredTools"][:2] == ["compare_competitive_set", "compare_vehicle_variants"]
    assert "query_msrp_pricing" in plan["allowedTools"]
    assert plan["entities"]["competitors"] == ["EV3"]
    assert plan["entities"]["models"] == ["O5 BEV", "EV3"]
    variant_step = next(item for item in plan["toolPlan"] if item["toolName"] == "compare_vehicle_variants")
    assert "O5 BEV" in variant_step["input"]["models"]
    assert "EV3" in variant_step["input"]["models"]
    assert variant_step["input"]["competitors"] == ["EV3"]


def test_evidence_plan_routes_high_spec_and_winter_configuration_to_market_chart_context() -> None:
    high_spec = build_evidence_plan("Sweden", "4.7m A-SUV 为什么要 95kWh + 双电机 + 800V？")
    winter = build_evidence_plan("Sweden", "北欧市场冬季包应该包含什么？")

    assert high_spec["intent"] == "configuration_analysis"
    assert high_spec["entities"]["features"] == ["95kWh", "800V", "双电机"]
    assert high_spec["requiredTools"] == ["compare_vehicle_variants", "compare_competitive_set", "build_market_chart"]
    assert "build_market_chart" in high_spec["allowedTools"]
    high_spec_variant_step = next(item for item in high_spec["toolPlan"] if item["toolName"] == "compare_vehicle_variants")
    assert high_spec_variant_step["input"]["features"] == ["95kWh", "800V", "双电机"]

    assert winter["intent"] == "configuration_analysis"
    assert winter["entities"]["features"] == ["冬季包"]
    assert winter["requiredTools"] == ["query_cross_country", "compare_vehicle_variants", "search_market_news", "build_market_chart"]
    assert winter["toolPlan"][0]["toolName"] == "query_cross_country"
    assert winter["toolPlan"][0]["input"]["countries"] == "Sweden, Finland, Norway, Denmark"
    winter_variant_step = next(item for item in winter["toolPlan"] if item["toolName"] == "compare_vehicle_variants")
    assert winter_variant_step["input"]["features"] == ["冬季包"]
    assert "build_market_chart" in winter["allowedTools"]


def test_evidence_plan_routes_price_reasonableness_terms_to_pricing() -> None:
    cheaper = build_evidence_plan("Sweden", "J7 HEV 是否应该比 Kia Sportage HEV 便宜？")
    corridor = build_evidence_plan("Sweden", "O9 在瑞典 53k-55k 欧元是否合理？")

    assert cheaper["intent"] == "pricing_analysis"
    assert cheaper["requiredTools"] == ["query_msrp_pricing", "build_market_chart", "compare_competitive_set", "query_price_positioning"]
    assert "compare_competitive_set" in cheaper["allowedTools"]
    assert "build_market_chart" in cheaper["allowedTools"]
    assert corridor["intent"] == "pricing_analysis"
    assert corridor["requiredTools"] == ["query_msrp_pricing", "build_market_chart", "compare_competitive_set", "query_price_positioning"]
    assert "compare_competitive_set" in corridor["allowedTools"]
    assert "build_market_chart" in corridor["allowedTools"]


def test_evidence_plan_allows_source_repair_without_forcing_web_for_plain_bev_price_delta() -> None:
    plan = build_evidence_plan("Sweden", "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？")

    assert plan["intent"] == "pricing_analysis"
    assert plan["requiredTools"] == ["query_msrp_pricing", "build_market_chart", "compare_competitive_set", "query_price_positioning"]
    assert "search_market_news" not in plan["requiredTools"]
    assert "external_research" not in plan["requiredTools"]
    assert "search_market_news" in plan["optionalTools"]
    assert "external_research" not in plan["optionalTools"]
    assert "search_market_news" in plan["allowedTools"]
    assert "external_research" not in plan["allowedTools"]
    assert "search_market_news" not in [item["toolName"] for item in plan["toolPlan"]]
    assert "external_research" not in [item["toolName"] for item in plan["toolPlan"]]


def test_evidence_plan_routes_positioning_difference_to_competitor_compare() -> None:
    plan = build_evidence_plan("Sweden", "O9 和 XC60 / EX60 的定位差异是什么？")
    core_pool = build_evidence_plan("Sweden", "J7 HEV 的核心竞品是谁？")

    assert plan["intent"] == "competitor_compare"
    assert plan["requiredTools"] == ["compare_competitive_set", "compare_vehicle_variants", "query_msrp_pricing"]
    assert "query_msrp_pricing" in plan["allowedTools"]
    assert core_pool["intent"] == "competitor_compare"
    assert core_pool["requiredTools"] == ["compare_competitive_set"]


def test_report_generation_uses_underlying_pricing_or_competitor_tools() -> None:
    pricing_report = build_evidence_plan("Sweden", "把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。")
    compare_report = build_evidence_plan("Sweden", "生成 O5 BEV 对标 EX30 和 EV3 的一页竞品汇报框架。")
    benchmark_report = build_evidence_plan("Sweden", "O5 BEV 应该对标 EX30 还是 EV3？请给出数据支撑和一页汇报结构。")
    market_report = build_evidence_plan("Sweden", "把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。")
    policy_report = build_evidence_plan("Sweden", "Elbilspremien 2026 会影响哪些车型？请给出来源、受影响车型、图表和一页汇报结构。")
    policy_price_report = build_evidence_plan("Sweden", "BEV 补贴价格上限对 O5 BEV 定价有什么影响？请给出来源和一页汇报结构。")

    assert pricing_report["intent"] == "report_generation"
    assert pricing_report["answerMode"] == "report"
    assert pricing_report["requiredTools"] == ["query_msrp_pricing", "compare_competitive_set", "build_market_chart"]
    assert "build_market_chart" in pricing_report["allowedTools"]
    assert compare_report["intent"] == "report_generation"
    assert compare_report["answerMode"] == "report"
    assert compare_report["requiredTools"] == ["compare_competitive_set", "compare_vehicle_variants", "query_msrp_pricing"]
    assert "compare_competitive_set" in compare_report["allowedTools"]
    assert benchmark_report["intent"] == "report_generation"
    assert benchmark_report["requiredTools"] == ["compare_competitive_set", "compare_vehicle_variants", "query_msrp_pricing"]
    assert benchmark_report["toolPlan"][0]["toolName"] == "compare_competitive_set"
    assert market_report["intent"] == "report_generation"
    assert market_report["answerMode"] == "report"
    assert market_report["requiredTools"] == ["query_country_snapshot", "build_market_chart"]
    assert market_report["allowedTools"] == ["query_country_snapshot", "build_market_chart"]
    assert [item["toolName"] for item in market_report["toolPlan"]] == ["query_country_snapshot", "build_market_chart"]
    assert market_report["shouldUseWeb"] is False
    assert policy_report["intent"] == "report_generation"
    assert policy_report["answerMode"] == "report"
    assert policy_report["requiredTools"] == ["search_market_news", "query_country_snapshot", "build_market_chart"]
    assert policy_report["toolPlan"][0]["toolName"] == "search_market_news"
    assert policy_report["shouldUseWeb"] is True
    assert policy_price_report["requiredTools"] == ["search_market_news", "query_country_snapshot", "build_market_chart", "query_msrp_pricing"]


def test_leasing_phev_business_question_routes_to_pricing_with_external_context() -> None:
    plan = build_evidence_plan("Sweden", "大客户 leasing 场景下，PHEV 还有没有理由？")

    assert plan["intent"] == "pricing_analysis"
    assert plan["requiredTools"] == [
        "query_msrp_pricing",
        "build_market_chart",
        "query_leasing_offers",
        "search_market_news",
        "query_country_snapshot",
    ]
    assert [item["toolName"] for item in plan["toolPlan"]] == plan["requiredTools"]
    assert next(item for item in plan["toolPlan"] if item["toolName"] == "search_market_news")["input"]["country"] == "Sweden"
    assert "query_country_snapshot" in plan["allowedTools"]
    assert "build_market_chart" in plan["allowedTools"]


def test_user_voice_questions_route_to_voc_research() -> None:
    v2h = build_evidence_plan("Sweden", "瑞典用户会不会把 V2H 当成真实购买卖点？")
    nordic_needs = build_evidence_plan("Sweden", "拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？")
    complaints = build_evidence_plan("Sweden", "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？")

    assert v2h["intent"] == "voc_analysis"
    assert v2h["entities"]["features"] == ["V2H"]
    assert v2h["requiredTools"] == ["search_market_news", "query_country_snapshot"]
    assert "query_country_snapshot" in v2h["allowedTools"]
    assert "search_market_news" in v2h["allowedTools"]
    v2h_research_step = next(item for item in v2h["toolPlan"] if item["toolName"] == "search_market_news")
    assert v2h_research_step["input"]["features"] == ["V2H"]
    assert nordic_needs["intent"] == "voc_analysis"
    assert nordic_needs["entities"]["features"] == ["冬季胎", "拖车钩", "roof load"]
    assert nordic_needs["requiredTools"] == ["search_market_news", "query_country_snapshot"]
    assert "query_country_snapshot" in nordic_needs["allowedTools"]
    assert "search_market_news" in nordic_needs["allowedTools"]
    nordic_research_step = next(item for item in nordic_needs["toolPlan"] if item["toolName"] == "search_market_news")
    assert nordic_research_step["input"]["features"] == ["冬季胎", "拖车钩", "roof load"]
    assert complaints["intent"] == "voc_analysis"
    assert complaints["entities"]["brands"] == ["OMODA", "JAECOO"]
    assert complaints["requiredTools"] == ["search_market_news", "query_country_snapshot"]
    assert "query_country_snapshot" in complaints["allowedTools"]
    assert "search_market_news" in complaints["allowedTools"]
    complaints_research_step = next(item for item in complaints["toolPlan"] if item["toolName"] == "search_market_news")
    assert complaints_research_step["input"]["brands"] == ["OMODA", "JAECOO"]


def test_tool_coverage_does_not_alias_removed_external_research() -> None:
    assert tool_satisfies_required("search_market_news", "external_research") is False
    assert tool_satisfies_required("pageindex_search_documents", "external_research") is False
    assert tool_satisfies_required("query_with_filters", "query_country_snapshot") is False


def test_tool_coverage_fails_closed_for_removed_external_research_requirement() -> None:
    evidence_plan = {
        "requiredTools": ["query_msrp_pricing", "external_research", "search_market_news"],
        "allowedTools": ["query_msrp_pricing", "external_research", "search_market_news"],
    }

    missing = missing_required_tools(
        evidence_plan,
        ["query_msrp_pricing", "search_market_news"],
        allowed_tools=evidence_plan["allowedTools"],
    )

    assert missing == ["external_research"]
    assert "external_research" not in get_intent_tool_rule("news_policy_search")["requiredTools"]


def test_evidence_plan_does_not_treat_overview_as_rv_pricing() -> None:
    plan = build_evidence_plan("Sweden", "Give me a Sweden market overview with top models and powertrain mix")

    assert plan["intent"] == "market_overview"
    assert "query_country_snapshot" in plan["requiredTools"]
    assert "query_msrp_pricing" not in plan["requiredTools"]


def test_evidence_plan_routes_powertrain_growth_space_to_market_overview() -> None:
    plan = build_evidence_plan("Sweden", "北欧 BEV 增长是否会压缩 HEV 空间？")

    assert plan["intent"] == "market_overview"
    assert plan["entities"]["countries"] == ["Sweden", "Finland", "Norway", "Denmark"]
    assert plan["requiredTools"] == ["query_cross_country"]
    assert plan["allowedTools"][0] == "query_cross_country"
    cross_country_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_cross_country")
    assert cross_country_step["input"]["countries"] == "Sweden, Finland, Norway, Denmark"
    assert "query_msrp_pricing" not in plan["requiredTools"]


def test_evidence_plan_routes_suv_a0_a_structure_to_market_chart_first() -> None:
    plan = build_evidence_plan("Sweden", "SUV A0/A 级为什么是主销结构？")

    assert plan["intent"] == "market_overview"
    assert plan["requiredTools"] == ["query_country_snapshot", "build_market_chart", "query_segment_breakdown"]
    assert plan["allowedTools"][:2] == ["query_country_snapshot", "build_market_chart"]
    assert "query_segment_breakdown" in plan["allowedTools"]
    segment_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_segment_breakdown")
    assert "segment" not in segment_step["input"]
    assert "analyze_market_dynamics" in plan["allowedTools"]
    assert "analyze_model_performance" in plan["allowedTools"]


def test_evidence_plan_routes_sweden_finland_difference_to_cross_country() -> None:
    plan = build_evidence_plan("Sweden", "瑞典和芬兰销量差异为什么大？")

    assert plan["intent"] == "market_overview"
    assert plan["entities"]["countries"] == ["Sweden", "Finland"]
    assert plan["requiredTools"] == ["query_cross_country"]
    assert plan["allowedTools"][0] == "query_cross_country"
    cross_country_step = next(item for item in plan["toolPlan"] if item["toolName"] == "query_cross_country")
    assert cross_country_step["input"]["countries"] == "Sweden, Finland"


def test_normalize_followups_preserves_structured_objects_and_legacy_strings() -> None:
    plan = build_evidence_plan("Sweden", "瑞典 J7 HEV 应该如何定价？")

    result = normalize_follow_ups(
        [
            {
                "label": "看竞品价格走廊",
                "question": "对比 J7 HEV 和核心竞品的 MSRP。",
                "intent": "compare",
                "expectedTools": ["query_msrp_pricing"],
                "priority": 1,
            },
            "生成一页定位定价汇报框架。",
        ],
        country="Sweden",
        question="瑞典 J7 HEV 应该如何定价？",
        tools=["query_msrp_pricing"],
        evidence_plan=plan,
    )

    assert result[0]["label"] == "看竞品价格走廊"
    assert result[0]["intent"] == "compare"
    assert result[0]["expectedTools"] == ["query_msrp_pricing"]
    assert result[1]["question"] == "生成一页定位定价汇报框架。"


def test_normalize_followups_backfills_missing_business_types() -> None:
    plan = {
        "intent": "competitor_compare",
        "followUpTypes": ["compare", "action", "report", "why"],
        "allowedTools": ["compare_competitive_set", "query_msrp_pricing"],
    }

    result = normalize_follow_ups(
        [
            {
                "label": "对比竞品池",
                "question": "对比 O9 与 XC60 的竞品池。",
                "intent": "compare",
                "priority": 1,
            },
            {
                "label": "补价格数据",
                "question": "补 O9 与 XC60 的 MSRP。",
                "intent": "data_check",
                "priority": 2,
            },
        ],
        country="Sweden",
        question="O9 和 XC60 / EX60 的定位差异是什么？",
        tools=["compare_competitive_set", "query_msrp_pricing"],
        evidence_plan=plan,
    )

    intents = [item["intent"] for item in result]

    assert len(result) == 4
    assert intents == ["compare", "action", "report", "why"]
    assert result[1]["expectedOutput"] == "recommendation"
    assert result[2]["expectedOutput"] == "report"


def test_generic_pricing_followups_are_artifact_next_steps() -> None:
    plan = {
        "intent": "pricing_analysis",
        "followUpTypes": ["compare", "data_check", "action", "report"],
        "allowedTools": ["query_msrp_pricing", "compare_competitive_set", "compare_vehicle_variants"],
    }

    result = normalize_follow_ups(
        [],
        country="Hungary",
        question="匈牙利 T7 HEV 应该怎么定价？请给出数据和图表。",
        tools=["query_msrp_pricing", "compare_competitive_set", "compare_vehicle_variants"],
        evidence_plan=plan,
    )

    labels = [item["label"] for item in result]
    questions = " ".join(item["question"] for item in result)

    assert labels == ["生成价格走廊表", "补官方价格证据", "拆定价动作", "生成定价 PPT block"]
    assert "匈牙利T7 HEV" in questions
    assert "Pricing evidence table" in questions
    assert "官方 MSRP" in questions
    assert "低配锚点" in questions
    assert result[0]["expectedOutput"] == "table"
    assert result[1]["expectedOutput"] == "checklist"
    assert result[3]["expectedOutput"] == "report"


def test_j7_pricing_fallback_followups_are_pm_next_steps() -> None:
    plan = build_evidence_plan("Sweden", "基于瑞典市场、竞品格局和配置差异，J7 HEV 的定价逻辑应该怎么写成一页产品经理汇报？")

    result = normalize_follow_ups(
        [],
        country="Sweden",
        question="基于瑞典市场、竞品格局和配置差异，J7 HEV 的定价逻辑应该怎么写成一页产品经理汇报？",
        tools=["query_country_snapshot", "query_msrp_pricing", "compare_competitive_set"],
        evidence_plan=plan,
    )

    assert [item["label"] for item in result] == [
        "补竞品价格矩阵",
        "拆市场窗口结构",
        "验证高配价差和话术",
        "生成一页 PPT block",
    ]
    assert "Corolla Cross、RAV4、C-HR、Qashqai" in result[0]["question"]
    assert result[1]["intent"] == "drilldown"
    assert "SUV A0/A 结构" in result[1]["question"]
    assert "3,230€" in result[2]["question"]
    assert "展厅话术" in result[2]["question"]
    assert result[3]["expectedOutput"] == "report"
    assert result[0]["expectedTools"][:2] == ["query_msrp_pricing", "compare_competitive_set"]
    assert result[1]["expectedOutput"]


def test_generic_chinese_followups_localize_country_label() -> None:
    question = "V2H 对匈牙利用户到底是不是卖点？请简短回答，不要回答瑞典。"
    plan = {
        "intent": "voc_analysis",
        "followUpTypes": ["why", "external_search", "action", "report"],
        "allowedTools": ["external_research", "search_market_news", "query_country_snapshot"],
    }

    result = normalize_follow_ups(
        [],
        country="Hungary",
        question=question,
        tools=["external_research", "search_market_news", "query_country_snapshot"],
        evidence_plan=plan,
    )

    questions = " ".join(str(item["question"]) for item in result)
    assert len(result) == 4
    assert "匈牙利" in questions
    assert "Hungary" not in questions
    assert "瑞典" not in questions
    assert " 匈牙利 " not in questions
    assert result[0]["intent"] == "why"
    assert result[1]["intent"] == "external_search"
    assert result[2]["intent"] == "action"
    assert result[3]["expectedOutput"] == "table"


def test_country_scope_mismatch_followup_prioritizes_target_market_requery() -> None:
    question = "匈牙利 HEV 市场为什么适合 J7？请不要回答瑞典。"
    plan = {
        "intent": "market_overview",
        "followUpTypes": ["drilldown", "compare", "action", "report"],
        "requiredTools": ["query_country_snapshot"],
        "allowedTools": ["query_country_snapshot", "build_market_chart"],
        "entities": {"countries": ["Hungary"], "models": ["J7 HEV"]},
    }
    package = build_evidence_package(
        session_id="sess_hungary_scope_guard_followup",
        country="Hungary",
        question=question,
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden", "question": question},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "source": "jato_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot", "country": "Sweden"},
                    "data": {"country": "Sweden", "marketSnapshot": {"kpis": {"cumulativeSales": 1182452}}},
                },
            }
        ],
    )

    result = normalize_follow_ups(
        [],
        country="Hungary",
        question=question,
        tools=["query_country_snapshot"],
        evidence_plan=plan,
        evidence_package=package,
    )

    first = result[0]
    assert first["label"] == "重查匈牙利数据"
    assert first["intent"] == "data_check"
    assert first["expectedTools"] == ["query_country_snapshot"]
    assert first["risk"] == "country_scope_mismatch"
    assert "按匈牙利重新调用市场快照工具" in first["question"]
    assert "不要使用瑞典结果" in first["question"]
    assert "过滤非目标市场证据" in first["question"]
    assert result[1]["priority"] == 2
    visible = " ".join(str(item["label"]) + " " + str(item["question"]) for item in result)
    assert "3,230" not in visible
    assert "Corolla Cross、RAV4、C-HR、Qashqai" not in visible
    assert "J7_HEV_V4" not in visible
    assert "生成匈牙利市场图" in result[1]["label"]
    assert "补匈牙利竞品证据" in result[2]["label"]


def test_country_scope_mismatch_followup_uses_target_model_instead_of_hardcoded_j7() -> None:
    question = "匈牙利 O5 BEV 是否适合继续验证？请不要回答瑞典。"
    plan = {
        "intent": "market_overview",
        "followUpTypes": ["drilldown", "compare", "action", "report"],
        "requiredTools": ["query_country_snapshot"],
        "allowedTools": ["query_country_snapshot", "build_market_chart"],
        "entities": {"countries": ["Hungary"], "models": ["O5 BEV"]},
    }
    package = build_evidence_package(
        session_id="sess_hungary_o5_scope_guard_followup",
        country="Hungary",
        question=question,
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden", "question": question},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "source": "jato_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot", "country": "Sweden"},
                    "data": {"country": "Sweden", "marketSnapshot": {"kpis": {"cumulativeSales": 1182452}}},
                },
            }
        ],
    )

    result = normalize_follow_ups(
        [],
        country="Hungary",
        question=question,
        tools=["query_country_snapshot"],
        evidence_plan=plan,
        evidence_package=package,
    )

    visible = " ".join(str(item["label"]) + " " + str(item["question"]) for item in result)
    assert "O5 BEV" in visible
    assert "J7 HEV" not in visible
    assert "3,230" not in visible
    assert "Corolla Cross" not in visible


def test_o5_bev_subsidy_cap_followups_are_business_next_steps() -> None:
    question = "BEV 补贴价格上限对 O5 BEV 定价有什么影响？"
    plan = {
        "intent": "news_policy_search",
        "followUpTypes": ["external_search", "report", "compare", "action"],
        "allowedTools": ["external_research", "search_market_news", "read_web_page", "query_msrp_pricing"],
    }

    result = normalize_follow_ups(
        [],
        country="Sweden",
        question=question,
        tools=["external_research", "search_market_news", "read_web_page", "query_msrp_pricing"],
        evidence_plan=plan,
    )

    labels = [item["label"] for item in result]
    questions = " ".join(str(item["question"]) for item in result)
    assert labels == [
        "核对官方政策边界",
        "做补贴内外定价页",
        "对比 EX30 / EV3 门槛",
        "拆低配/高配动作",
    ]
    assert [item["intent"] for item in result] == ["external_search", "report", "compare", "action"]
    assert "瑞典 BEV 补贴价格上限的官方原文" in questions
    assert "O5 BEV 与 EX30、EV3" in questions
    assert "补贴内/补贴外两套定价页" in questions
    assert "能否提供" not in questions


def test_co2_phev_policy_followups_are_tco_next_steps() -> None:
    question = "CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？"
    plan = {
        "intent": "news_policy_search",
        "followUpTypes": ["external_search", "compare", "action", "report"],
        "allowedTools": ["external_research", "search_market_news", "read_web_page", "query_msrp_pricing"],
    }

    result = normalize_follow_ups(
        [],
        country="Sweden",
        question=question,
        tools=["external_research", "search_market_news", "read_web_page", "query_msrp_pricing"],
        evidence_plan=plan,
    )

    labels = [item["label"] for item in result]
    questions = " ".join(str(item["question"]) for item in result)
    assert labels == [
        "核对 CO2/税率公式",
        "做 PHEV TCO 场景表",
        "拆公司车适用场景",
        "生成政策影响 PPT block",
    ]
    assert [item["intent"] for item in result] == ["external_search", "compare", "action", "report"]
    assert "瑞典 CO2 0-75g/km 税率阶梯" in questions
    assert "company car TCO 场景表" in questions
    assert "PHEV 仍有理由" in questions
    assert "能否提供" not in questions


def test_suv_structure_followups_are_specific_market_next_steps() -> None:
    question = "SUV A0/A 级为什么是主销结构？"
    plan = {
        "intent": "market_overview",
        "followUpTypes": ["drilldown", "compare", "action", "report"],
        "allowedTools": ["query_country_snapshot", "build_market_chart", "analyze_market_dynamics"],
    }

    result = normalize_follow_ups(
        [],
        country="Sweden",
        question=question,
        tools=["query_country_snapshot", "build_market_chart"],
        evidence_plan=plan,
    )

    labels = [item["label"] for item in result]
    questions = " ".join(str(item["question"]) for item in result)
    assert labels == ["拆 SUV A0/A 数据", "对比竞品/邻国结构", "映射到车型机会", "生成机会页"]
    assert "SUV A0/A 拆到销量、BEV/PHEV/HEV 渗透率" in questions
    assert "这个主销结构是否可复制" in questions
    assert "OMODA/JAECOO" in questions
    assert "继续深挖数据" not in labels
    assert "转成业务动作" not in labels


def test_bom_followups_are_entity_mapping_next_steps() -> None:
    question = "SE/FI 合并 PI 但车辆分市场生成，逻辑是否正确？"
    plan = {
        "intent": "inventory_analysis",
        "followUpTypes": ["drilldown", "action", "report"],
        "allowedTools": ["query_inventory_pipeline", "compare_vehicle_variants"],
    }

    result = normalize_follow_ups(
        [],
        country="Sweden",
        question=question,
        tools=["query_inventory_pipeline"],
        evidence_plan=plan,
    )

    labels = [item["label"] for item in result]
    questions = " ".join(str(item["question"]) for item in result)
    assert labels[:3] == ["画实体关系图", "定义校验规则", "生成管理表"]
    assert "PI header、market overlay、business variant、material code" in questions
    assert "phase-out 和跨市场混用" in questions
    assert "继续深挖数据" not in labels
    assert "看竞品/邻国对比" not in labels


def test_eval_v2_golden_question_checks_intent_tools_and_followups() -> None:
    questions = list_golden_questions_v2()
    checked = check_golden_question_v2("astr-v2-002")

    assert questions["total"] == 100
    assert questions["categoryCounts"]["pricing_analysis"] == 15
    assert checked["question"]["expectedIntent"] == "pricing_analysis"
    assert checked["scores"]["intent"] == 1
    assert checked["scores"]["toolPrecision"] == 1
    assert checked["scores"]["followUpTypes"] == 1
    assert checked["deterministicScore"]["totalScore"] > 0.8


def test_intent_tool_matrix_defines_required_pricing_tool() -> None:
    rule = get_intent_tool_rule("pricing_analysis")

    assert rule["requiredTools"] == ["query_msrp_pricing", "build_market_chart"]
    assert "market_context" in rule["mustHaveEvidence"]
    assert "competitor_price_range" in rule["mustHaveEvidence"]
    market_rule = get_intent_tool_rule("market_overview")
    assert market_rule["requiredTools"] == ["query_country_snapshot", "build_market_chart"]
    assert "query_segment_breakdown" in market_rule["optionalTools"]
    assert "query_with_filters" in market_rule["optionalTools"]
    assert "segment_or_channel_structure" in market_rule["mustHaveEvidence"]
    policy_rule = get_intent_tool_rule("news_policy_search")
    assert policy_rule["requiredTools"] == ["search_market_news", "query_country_snapshot", "build_market_chart"]
    assert "market_context" in policy_rule["mustHaveEvidence"]


def test_intent_tool_matrix_keeps_competitor_and_report_evidence_boundaries() -> None:
    competitor_rule = get_intent_tool_rule("competitor_compare")
    report_rule = get_intent_tool_rule("report_generation")
    report_plan = build_evidence_plan("Sweden", "把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。")

    assert competitor_rule["mustHaveEvidence"] == ["competitor_pool"]
    assert "compare_vehicle_variants" in competitor_rule["optionalTools"]
    assert report_rule["mustHaveEvidence"] == ["supporting_evidence"]
    assert "report_outline" not in report_rule["mustHaveEvidence"]
    assert report_plan["evidenceNeeded"][0]["name"] == "supporting_evidence"
    assert report_plan["evidenceNeeded"][0]["priority"] == 1
    assert report_plan["evidenceNeeded"][1]["name"] == "report_outline"
    assert report_plan["evidenceNeeded"][1]["priority"] == 2


def test_evidence_package_extracts_refs_and_missing_evidence() -> None:
    plan = build_evidence_plan("Sweden", "瑞典 J7 HEV 应该如何定价？")
    package = build_evidence_package(
        session_id="sess_test",
        country="Sweden",
        question="瑞典 J7 HEV 应该如何定价？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "J7 HEV"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {"items": [{"model": "J7 HEV", "msrp": 300000}]},
                },
            }
        ],
    )

    assert package["intent"] == "pricing_analysis"
    assert package["toolResults"][0]["sourceType"] == "postgres"
    assert package["toolResults"][0]["evidenceRefs"]
    assert package["confidence"] in {"medium", "high"}


def test_evidence_package_marks_successful_empty_tool_as_missing_refs() -> None:
    plan = build_evidence_plan("Sweden", "瑞典 O5 BEV 应该如何定价？")
    package = build_evidence_package(
        session_id="sess_empty",
        country="Sweden",
        question="瑞典 O5 BEV 应该如何定价？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "O5 BEV"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {"items": []},
                },
            }
        ],
    )

    assert package["confidence"] == "low"
    assert any(item["name"] == "evidence_refs" for item in package["missingEvidence"])
    assert any(item["name"] == "current_msrp" for item in package["missingEvidence"])


def test_evidence_package_extracts_query_with_filters_nested_results() -> None:
    plan = build_evidence_plan("Sweden", "BOM、车型版本、内外饰颜色之间应该怎么建模？")
    package = build_evidence_package(
        session_id="sess_filtered_results",
        country="Sweden",
        question="BOM、车型版本、内外饰颜色之间应该怎么建模？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_with_filters",
                "query": {"country": "Sweden", "model": "OMODA9"},
                "success": True,
                "result": {
                    "tool": "query_with_filters",
                    "metadata": {"source": "jato_filtered_query"},
                    "data": {
                        "country": "Sweden",
                        "appliedFilters": {"model": "OMODA9"},
                        "results": {
                            "topModels": [
                                {"label": "OMODA9 Comfort AWD", "sales": 128, "share": 0.12, "rank": 7},
                            ],
                            "topBrands": [
                                {"label": "OMODA", "sales": 220, "share": 0.2},
                            ],
                            "powertrainMix": [
                                {"label": "PHEV", "sales": 90, "share": 0.41},
                            ],
                            "kpis": {"totalSales": 438, "modelCount": 4},
                        },
                    },
                },
            }
        ],
    )

    refs = package["toolResults"][0]["evidenceRefs"]
    labels = {str(ref.get("label")) for ref in refs}
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert package["toolResults"][0]["rowCount"] >= 3
    assert any(label.startswith("results.topModels.") for label in labels)
    assert "query_with_filters_weak_evidence_refs" not in missing_names


def test_evidence_package_extracts_segment_breakdown_cross_tabs() -> None:
    plan = build_evidence_plan("Sweden", "瑞典 HEV 市场为什么适合 J7？")
    package = build_evidence_package(
        session_id="sess_segment_breakdown",
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_segment_breakdown",
                "query": {"country": "Sweden", "powertrain": "HEV"},
                "success": True,
                "result": {
                    "tool": "query_segment_breakdown",
                    "metadata": {"source": "jato_segment_breakdown"},
                    "data": {
                        "country": "Sweden",
                        "appliedFilters": {"powertrain": "HEV"},
                        "driveByFuel": [
                            {"_index": "HEV", "_total": 1946, "2WD_pct": 85.9, "4WD_pct": 14.1},
                            {"_index": "BEV", "_total": 25235, "2WD_pct": 44.0, "4WD_pct": 56.0},
                        ],
                        "driveBySegment": [
                            {"_index": "SUV A0", "_total": 753, "2WD_pct": 92.0, "4WD_pct": 8.0},
                            {"_index": "SUV A", "_total": 405, "2WD_pct": 78.0, "4WD_pct": 22.0},
                        ],
                        "segmentByFuel": [
                            {"_index": "SUV A0", "_total": 753, "HEV_pct": 38.7, "BEV_pct": 45.6},
                            {"_index": "SUV A", "_total": 405, "HEV_pct": 20.8, "BEV_pct": 40.0},
                        ],
                        "registrationByFuel": [
                            {"_index": "HEV", "_total": 1946, "Private_pct": 46.0, "Business_pct": 54.0},
                        ],
                    },
                },
            }
        ],
    )

    refs = package["toolResults"][0]["evidenceRefs"]
    labels = {str(ref.get("label")) for ref in refs}
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert package["toolResults"][0]["rowCount"] >= 6
    assert "contextSnapshot.crossTabs.driveByFuel.HEV.sales" in labels
    assert "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct" in labels
    assert "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales" in labels
    assert "evidence_refs" not in missing_names


def test_evidence_package_extracts_top_level_monthly_trend_series_refs() -> None:
    plan = {
        "intent": "report_generation",
        "entities": {"countries": ["Sweden"]},
        "requiredTools": ["query_country_snapshot"],
        "mustHaveEvidence": ["supporting_evidence"],
        "evidenceNeeded": [
            {"name": "monthly_trend_series", "reason": "Need BEV month trend.", "priority": 1},
            {"name": "supporting_evidence", "reason": "Need grounded refs.", "priority": 1},
        ],
    }
    package = build_evidence_package(
        session_id="sess_monthly_trend",
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden", "question": "BEV market share by month"},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot"},
                    "data": {
                        "kpis": {"totalRows": 33327},
                        "monthSeries": [
                            {"month": "2026-01", "bevShare": 0.31, "sales": 1020},
                            {"month": "2026-02", "bevShare": 0.34, "sales": 1110},
                        ],
                    },
                },
            }
        ],
    )

    refs = package["toolResults"][0]["evidenceRefs"]
    labels = {str(ref.get("label")) for ref in refs}
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert any(label.startswith("monthSeries.2026-01.bevShare") for label in labels)
    assert any(label.startswith("monthSeries.2026-02.sales") for label in labels)
    assert "monthly_trend_series" not in missing_names
    assert "query_country_snapshot_weak_evidence_refs" not in missing_names


def test_evidence_package_extracts_chart_context_monthly_trend_series_refs() -> None:
    plan = {
        "intent": "report_generation",
        "entities": {"countries": ["Sweden"]},
        "requiredTools": ["build_market_chart"],
        "mustHaveEvidence": ["supporting_evidence"],
        "evidenceNeeded": [
            {"name": "monthly_trend_series", "reason": "Need chartable monthly trend.", "priority": 1},
            {"name": "supporting_evidence", "reason": "Need grounded refs.", "priority": 1},
        ],
    }
    package = build_evidence_package(
        session_id="sess_chart_monthly_trend",
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "build_market_chart",
                "query": {"country": "Sweden", "question": "BEV market share by month"},
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck"},
                    "data": {
                        "contextSnapshot": {
                            "kpis": {"totalRows": 33327},
                            "monthSeries": [
                                {"month": "2026-01", "bevShare": 0.31, "sales": 1020},
                                {"month": "2026-02", "bevShare": 0.34, "sales": 1110},
                            ],
                            "topModels": [
                                {"model": "EX30", "value": 1518},
                            ],
                        },
                    },
                },
            }
        ],
    )

    refs = package["toolResults"][0]["evidenceRefs"]
    labels = {str(ref.get("label")) for ref in refs}
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert any(label.startswith("contextSnapshot.monthSeries.2026-01.bevShare") for label in labels)
    assert any(label.startswith("contextSnapshot.monthSeries.2026-02.sales") for label in labels)
    assert "monthly_trend_series" not in missing_names
    assert "market_snapshot_data_unavailable" not in missing_names


def test_optional_external_search_failure_does_not_pollute_local_report_evidence() -> None:
    plan = {
        "intent": "report_generation",
        "entities": {"countries": ["Sweden"]},
        "requiredTools": ["query_country_snapshot", "build_market_chart"],
        "mustHaveEvidence": ["supporting_evidence"],
        "evidenceNeeded": [
            {"name": "supporting_evidence", "reason": "Need chart evidence.", "priority": 1},
            {"name": "report_outline", "reason": "Need report structure.", "priority": 2},
        ],
    }
    package = build_evidence_package(
        session_id="sess_optional_external_empty",
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot"},
                    "data": {
                        "kpis": {"cumulativeSales": 1182452},
                        "yearSeries": [
                            {"year": "2024", "sales": 288087},
                            {"year": "2025", "sales": 289827},
                        ],
                    },
                },
            },
            {
                "toolName": "build_market_chart",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck"},
                    "data": {
                        "contextSnapshot": {
                            "yearSeries": [
                                {"year": "2024", "sales": 288087},
                                {"year": "2025", "sales": 289827},
                            ],
                        },
                    },
                },
            },
            {
                "toolName": "search_market_news",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "search_market_news",
                    "metadata": {"source": "jato_web_search_service"},
                    "data": {"items": []},
                },
            },
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert "external_research_claims_unavailable" not in missing_names
    assert package["confidence"] == "high"


def test_required_external_research_failure_remains_missing() -> None:
    plan = {
        "intent": "news_policy_search",
        "entities": {"countries": ["Sweden"]},
        "requiredTools": ["external_research"],
        "mustHaveEvidence": [],
        "evidenceNeeded": [
            {"name": "fresh_external_signal", "reason": "Need fresh policy source.", "priority": 1},
        ],
    }
    package = build_evidence_package(
        session_id="sess_required_external_empty",
        country="Sweden",
        question="Elbilspremien 2026 会影响哪些车型？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "external_research",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "external_research",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {"items": []},
                },
            },
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert "external_research_claims_unavailable" in missing_names
    assert "fresh_external_signal" in missing_names
    assert package["confidence"] == "low"


def test_evidence_package_flags_weak_market_snapshot_refs() -> None:
    plan = build_evidence_plan("Sweden", "瑞典 HEV 市场为什么适合 J7？")
    package = build_evidence_package(
        session_id="sess_weak_market",
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot"},
                    "data": {
                        "kpis": {
                            "totalRows": 0,
                            "countryCount": 0,
                            "brandCount": 0,
                            "modelCount": 0,
                        }
                    },
                },
            }
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert package["confidence"] == "low"
    assert missing["market_snapshot_data_unavailable"]["impact"] == "weakens_answer"
    assert any("market" in name for name in missing)


def test_evidence_package_keeps_medium_when_named_model_lacks_model_level_evidence() -> None:
    plan = {
        "intent": "market_overview",
            "entities": {"models": ["J8 HEV"]},
        "requiredTools": ["query_country_snapshot"],
        "mustHaveEvidence": [],
        "evidenceNeeded": [
            {"name": "market_kpis", "reason": "Need market evidence.", "priority": 1},
            {"name": "trend_or_mix", "reason": "Need trend or mix evidence.", "priority": 2},
        ],
    }
    package = build_evidence_package(
        session_id="sess_optional_weak_model",
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J8？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot"},
                    "data": {
                        "kpis": {"cumulativeSales": 1182452},
                        "powertrainMix": [
                            {"label": "BEV", "sales": 25235, "share": 0.42},
                            {"label": "HEV", "sales": 22816, "share": 0.18},
                        ],
                    },
                },
            },
            {
                "toolName": "analyze_model_performance",
                    "query": {"country": "Sweden", "model": "J8 HEV"},
                "success": True,
                "result": {
                    "tool": "analyze_model_performance",
                    "metadata": {"source": "jato_cross_reference"},
                        "data": {"country": "Sweden", "model": "J8 HEV"},
                },
            },
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert "analyze_model_performance_weak_evidence_refs" not in missing_names
    assert "market_snapshot_data_unavailable" not in missing_names
    assert "model_level_market_opportunity_evidence" in missing_names
    assert package["confidence"] == "medium"


def test_evidence_package_marks_missing_finland_for_country_pair_question() -> None:
    plan = build_evidence_plan("Sweden", "瑞典和芬兰销量差异为什么大？")
    package = build_evidence_package(
        session_id="sess_country_pair_partial",
        country="Sweden",
        question="瑞典和芬兰销量差异为什么大？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot"},
                    "data": {
                        "kpis": {"cumulativeSales": 1182452},
                        "powertrainMix": [{"label": "BEV", "sales": 10875, "share": 40.9}],
                    },
                },
            }
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert "missing_required_tool:query_cross_country" in missing
    assert missing["missing_country_snapshot:Finland"]["impact"] == "blocking"
    assert "missing_country_snapshot:Sweden" not in missing
    assert package["confidence"] in {"low", "medium"}


def test_evidence_package_extracts_bilateral_cross_country_refs() -> None:
    plan = build_evidence_plan("Sweden", "瑞典和芬兰销量差异为什么大？")
    package = build_evidence_package(
        session_id="sess_country_pair_full",
        country="Sweden",
        question="瑞典和芬兰销量差异为什么大？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_cross_country",
                "query": {"countries": "Sweden, Finland", "question": "瑞典和芬兰销量差异为什么大？"},
                "success": True,
                "result": {
                    "tool": "query_cross_country",
                    "metadata": {"source": "jato_cross_country", "countries": ["Sweden", "Finland"]},
                    "data": {
                        "countries": ["Sweden", "Finland"],
                        "comparison": {
                            "Sweden": {
                                "kpis": {"cumulativeSales": 1182452},
                                "powertrainMix": [{"label": "BEV", "sales": 10875, "share": 40.9}],
                                "topModels": [{"label": "EX40", "sales": 2945}],
                            },
                            "Finland": {
                                "kpis": {"cumulativeSales": 416200},
                                "powertrainMix": [{"label": "BEV", "sales": 2800, "share": 27.2}],
                                "topModels": [{"label": "Yaris Cross", "sales": 980}],
                            },
                        },
                    },
                },
            }
        ],
    )

    labels = {str(ref.get("label")) for ref in package["toolResults"][0]["evidenceRefs"]}
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert any(label.startswith("crossCountry.Sweden.") for label in labels)
    assert any(label.startswith("crossCountry.Finland.") for label in labels)
    assert "missing_country_snapshot:Finland" not in missing_names
    assert "missing_required_tool:query_cross_country" not in missing_names
    assert package["confidence"] == "high"


def test_evidence_package_extracts_nested_model_performance_refs() -> None:
    plan = {
        "intent": "market_overview",
        "entities": {"models": ["J7 HEV"]},
        "requiredTools": ["analyze_model_performance"],
        "mustHaveEvidence": [],
        "evidenceNeeded": [
            {"name": "market_kpis", "reason": "Need sales/share rankings.", "priority": 1},
            {"name": "trend_or_mix", "reason": "Need market movement evidence.", "priority": 2},
        ],
    }
    package = build_evidence_package(
        session_id="sess_nested_model",
        country="Sweden",
        question="J7 HEV 的核心竞品是谁？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "analyze_model_performance",
                "query": {"country": "Sweden", "model": "J7 HEV"},
                "success": True,
                "result": {
                    "tool": "analyze_model_performance",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {
                        "findings": {
                            "sales": {
                                "rankings": [
                                    {"model": "Toyota RAV4", "value": 1150, "share": 0.18, "rank": 1},
                                    {"model": "Kia Sportage", "value": 960, "share": 0.15, "rank": 2},
                                ],
                                "totalModels": 24,
                            },
                            "pricing": {
                                "records": [
                                    {"model": "Toyota RAV4", "msrp": 39200, "currency": "EUR"},
                                ],
                                "count": 1,
                            },
                            "variants": {
                                "subjects": [{"model": "J7 HEV", "powertrain": "HEV"}],
                                "diffFeatures": [{"feature": "HUD", "customerValue": "visible high trim value"}],
                            },
                        },
                    },
                },
            }
        ],
    )

    refs = [
        ref
        for item in package["toolResults"]
        for ref in item["evidenceRefs"]
    ]
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert any(str(ref["label"]).startswith("sales.rankings.Toyota RAV4") for ref in refs)
    assert any(str(ref["label"]).startswith("pricing.records.Toyota RAV4.msrp") for ref in refs)
    assert "analyze_model_performance_weak_evidence_refs" not in missing_names
    assert package["confidence"] == "high"


def test_evidence_package_extracts_nested_market_dynamics_snapshot_refs() -> None:
    plan = {
        "intent": "market_overview",
        "entities": {"countries": ["Sweden"]},
        "requiredTools": ["analyze_market_dynamics"],
        "mustHaveEvidence": [],
        "evidenceNeeded": [
            {"name": "market_kpis", "reason": "Need market size.", "priority": 1},
            {"name": "trend_or_mix", "reason": "Need powertrain or time trend.", "priority": 2},
        ],
    }
    package = build_evidence_package(
        session_id="sess_nested_dynamics",
        country="Sweden",
        question="北欧 BEV 增长是否会压缩 HEV 空间？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "analyze_market_dynamics",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "analyze_market_dynamics",
                    "metadata": {"source": "jato_market_dynamics"},
                    "data": {
                        "dynamics": {
                            "marketSnapshot": {
                                "kpis": {"totalRows": 33327, "cumulativeSales": 1182452},
                                "powertrainMix": [
                                    {"label": "BEV", "value": 25235, "share": 0.42},
                                    {"label": "HEV", "value": 22816, "share": 0.18},
                                ],
                                "yearSeries": [
                                    {"year": "2025", "sales": 102000, "share": 0.35},
                                ],
                            },
                        },
                    },
                },
            }
        ],
    )

    refs = [
        ref
        for item in package["toolResults"]
        for ref in item["evidenceRefs"]
    ]
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert any(str(ref["label"]).startswith("marketSnapshot.kpis.cumulativeSales") for ref in refs)
    assert any(str(ref["label"]).startswith("marketSnapshot.powertrainMix.BEV.share") for ref in refs)
    assert "analyze_market_dynamics_weak_evidence_refs" not in missing_names
    assert package["confidence"] == "high"


def test_evidence_package_surfaces_msrp_coverage_diagnostics() -> None:
    plan = build_evidence_plan("Sweden", "O9 在瑞典 53k-55k 欧元是否合理？")
    package = build_evidence_package(
        session_id="sess_o9_coverage",
        country="Sweden",
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "O9"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {
                        "items": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_country",
                            "requested": {"country": "Sweden", "models": ["O9"]},
                            "currentPriceRows": {
                                "total": 3,
                                "requestedCountry": 0,
                                "availableCountries": [{"country": "dk", "count": 3}],
                            },
                            "nextActions": [
                                "Add current price observations for Sweden before numeric price claims."
                            ],
                        },
                    },
                },
            }
        ],
    )

    tool_evidence = package["toolResults"][0]
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert tool_evidence["coverageDiagnostics"]["diagnosis"] == "no_current_prices_for_country"
    assert any("coverage_diagnosis" in item for item in tool_evidence["keyFindings"])
    assert "coverage_diagnostic:no_current_prices_for_country" in missing_names


def test_evidence_package_does_not_treat_row_count_as_pricing_coverage() -> None:
    plan = build_evidence_plan("Sweden", "O9 在瑞典 53k-55k 欧元是否合理？")
    package = build_evidence_package(
        session_id="sess_o9_empty",
        country="Sweden",
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "O9"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {"items": []},
                },
            },
            {
                "toolName": "compare_competitive_set",
                "query": {"country": "Sweden", "question": "O9 price"},
                "success": True,
                "result": {
                    "tool": "compare_competitive_set",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {"analysis": {"totalCompared": 0}},
                },
            },
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert package["confidence"] == "medium"
    assert "current_msrp" in missing_names
    assert "own_model_price" not in missing_names
    assert "price_corridor" in missing_names
    assert "competitor_price_range" in missing_names


def test_evidence_package_uses_price_positioning_for_corridor_not_own_msrp() -> None:
    plan = build_evidence_plan("Sweden", "O9 在瑞典 53k-55k 欧元是否合理？")
    package = build_evidence_package(
        session_id="sess_o9_positioning",
        country="Sweden",
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "O9"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {"items": []},
                },
            },
            {
                "toolName": "query_price_positioning",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_price_positioning",
                    "metadata": {"source": "jato_price_positioning"},
                    "data": {
                        "priceStats": {"min": 48000, "max": 62000, "avg": 55000, "median": 54000, "count": 8},
                        "priceRecords": [
                            {"model": "XC60", "msrp": 56900},
                            {"model": "EX60", "msrp": 61000},
                        ],
                    },
                },
            },
            _market_chart_context_tool_result(),
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}
    positioning_refs = [
        ref
        for item in package["toolResults"]
        if item["toolName"] == "query_price_positioning"
        for ref in item["evidenceRefs"]
    ]

    assert any(str(ref["label"]).startswith("priceStats.") for ref in positioning_refs)
    assert "current_msrp" in missing_names
    assert "own_model_price" not in missing_names
    assert "price_corridor" not in missing_names
    assert "competitor_price_range" not in missing_names


def test_evidence_package_uses_user_target_price_for_reasonableness_without_calling_it_official_msrp() -> None:
    plan = build_evidence_plan("Sweden", "O9 在瑞典 53k-55k 欧元是否合理？")
    package = build_evidence_package(
        session_id="sess_o9_target_price",
        country="Sweden",
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "O9"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {
                        "items": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_requested_models",
                            "requested": {"country": "Sweden", "models": ["O9"]},
                            "currentPriceRows": {
                                "total": 17,
                                "requestedCountry": 17,
                                "requestedModels": [{"model": "O9", "count": 0}],
                            },
                        },
                    },
                },
            },
            {
                "toolName": "query_price_positioning",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_price_positioning",
                    "metadata": {"source": "jato_price_positioning"},
                    "data": {
                        "priceStats": {"min": 48000, "max": 62000, "avg": 55000, "median": 54000, "count": 8},
                        "priceRecords": [
                            {"model": "XC90", "msrp": 994000, "currency": "SEK"},
                            {"model": "EX90", "msrp": 899000, "currency": "SEK"},
                        ],
                    },
                },
            },
            {
                "toolName": "compare_competitive_set",
                "query": {"country": "Sweden", "question": "O9 price"},
                "success": True,
                "result": {
                    "tool": "compare_competitive_set",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {
                        "items": [{"model": "XC90", "priceRange": "994000 SEK"}],
                        "analysis": {"totalCompared": 2, "hasPricing": True},
                    },
                },
            },
            _market_chart_context_tool_result(),
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}
    target_evidence = next(item for item in package["toolResults"] if item["toolName"] == "user_supplied_target_price")
    target_refs = {ref["label"]: ref for ref in target_evidence["evidenceRefs"]}

    assert target_refs["User supplied own-model target price min"]["value"] == 53000
    assert target_refs["User supplied own-model target price max"]["value"] == 55000
    assert target_refs["User supplied own-model target price midpoint"]["value"] == 54000
    assert target_refs["User supplied own-model target price min"]["unit"] == "EUR"
    assert "own_model_price" not in missing
    assert "price_corridor" not in missing
    assert "competitor_price_range" not in missing
    assert missing["current_msrp"]["impact"] == "weakens_answer"
    assert missing["coverage_diagnostic:no_current_prices_for_requested_models"]["impact"] == "weakens_answer"
    assert all(item["impact"] != "blocking" for item in package["missingEvidence"])


def test_evidence_package_does_not_block_target_price_scenario_on_missing_official_msrp_alone() -> None:
    plan = build_evidence_plan("Sweden", "O9 在瑞典 53k-55k 欧元是否合理？")
    package = build_evidence_package(
        session_id="sess_o9_target_price_no_corridor",
        country="Sweden",
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "O9"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {
                        "items": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_requested_models",
                            "requested": {"country": "Sweden", "models": ["O9"]},
                            "currentPriceRows": {
                                "total": 17,
                                "requestedCountry": 17,
                                "requestedModels": [{"model": "O9", "count": 0}],
                            },
                        },
                    },
                },
            }
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert missing["current_msrp"]["impact"] == "weakens_answer"
    assert missing["competitor_price_range"]["impact"] == "blocking"
    assert "user_supplied_target_price" in [item["toolName"] for item in package["toolResults"]]


def test_evidence_package_uses_j7_method_material_when_live_price_tool_is_empty() -> None:
    question = "基于我给的 J7_HEV_V4 材料，瑞典 J7 HEV 应该如何定价？"
    plan = build_evidence_plan("Sweden", question)
    package = build_evidence_package(
        session_id="sess_j7_method",
        country="Sweden",
        question=question,
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "J7 HEV"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {"items": []},
                },
            },
            _market_chart_context_tool_result(),
        ],
    )

    method_evidence = next(item for item in package["toolResults"] if item["toolName"] == "business_method_material")
    ref_labels = " ".join(ref["label"] for ref in method_evidence["evidenceRefs"])
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert package["confidence"] == "high"
    assert method_evidence["sourceType"] == "user_material"
    assert all(ref.get("evidenceStatus") == "hypothesis" for ref in method_evidence["evidenceRefs"])
    assert "J7 HEV user material main trim MSRP" in ref_labels
    assert "J7 HEV user material competitor corridor" in ref_labels
    assert "evidence_refs" not in missing_names
    assert "current_official_msrp_cross_check" not in missing_names

    guarded = apply_answer_grounding_guard(
        {"direct": "J7 HEV 应按 34,720 EUR 主销高配价格推进。", "bullets": [], "limitations": []},
        package,
        country="Sweden",
        question=question,
        evidence_plan=plan,
    )

    assert guarded["answerStatus"] == "answered"
    assert guarded["confidence"] == "high"
    assert guarded["grounding"]["confidence"] == "high"
    assert "用户材料假设边界" in guarded["direct"]
    assert "验证版定价立场" in guarded["direct"]
    assert "34,720 EUR" in guarded["direct"]
    assert "不能写死具体差额" in guarded["direct"]
    assert "应按 34,720 EUR 主销高配价格推进" not in guarded["direct"]


def test_evidence_package_uses_j7_method_material_for_competitor_question() -> None:
    question = "基于我给的 J7_HEV_V4 材料，J7 HEV 的核心竞品是谁？"
    plan = build_evidence_plan("Sweden", question)
    package = build_evidence_package(
        session_id="sess_j7_competitor_method",
        country="Sweden",
        question=question,
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "compare_competitive_set",
                "query": {"country": "Sweden", "question": "J7 HEV 的核心竞品是谁？"},
                "success": True,
                "result": {
                    "tool": "compare_competitive_set",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {"competitors": [], "analysis": {"totalCompared": 0}},
                },
            }
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}
    method_evidence = next(item for item in package["toolResults"] if item["toolName"] == "business_method_material")
    ref_labels = " ".join(str(ref["label"]) for ref in method_evidence["evidenceRefs"])

    assert plan["intent"] == "competitor_compare"
    assert "J7 HEV user material competitor pool" in ref_labels
    assert "competitor_pool" not in missing_names


def test_evidence_package_extracts_competitor_and_variant_diff_refs() -> None:
    competitor_plan = build_evidence_plan("Sweden", "O9 和 XC60 / EX60 的定位差异是什么？")
    competitor_package = build_evidence_package(
        session_id="sess_competitor_refs",
        country="Sweden",
        question="O9 和 XC60 / EX60 的定位差异是什么？",
        evidence_plan=competitor_plan,
        tool_results=[
            {
                "toolName": "compare_competitive_set",
                "query": {"country": "Sweden", "question": "O9 和 XC60 / EX60 的定位差异是什么？"},
                "success": True,
                "result": {
                    "tool": "compare_competitive_set",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {
                        "targetModel": "O9",
                        "competitors": [
                            {"model": "XC60", "sales": 1200, "avgPrice": 56000, "priceRecords": 3},
                            {"model": "EX60", "sales": 800, "avgPrice": 61000, "priceRecords": 2},
                        ],
                        "analysis": {"totalCompared": 2, "sourceCount": 2, "hasPricing": True},
                    },
                },
            }
        ],
    )
    competitor_missing = {item["name"] for item in competitor_package["missingEvidence"]}
    competitor_labels = " ".join(
        str(ref["label"])
        for item in competitor_package["toolResults"]
        for ref in item["evidenceRefs"]
    )

    assert "competitor_pool" not in competitor_missing
    assert "competitor.1.model" in competitor_labels
    assert "XC60.avgPrice" in competitor_labels

    config_plan = build_evidence_plan("Sweden", "O5 BEV 和 EV3 的配置差异是什么？")
    config_package = build_evidence_package(
        session_id="sess_variant_refs",
        country="Sweden",
        question="O5 BEV 和 EV3 的配置差异是什么？",
        evidence_plan=config_plan,
        tool_results=[
            {
                "toolName": "compare_vehicle_variants",
                "query": {"country": "Sweden", "models": ["O5 BEV", "EV3"]},
                "success": True,
                "result": {
                    "tool": "compare_vehicle_variants",
                    "metadata": {"source": "jato_variant_diff_service"},
                    "data": {
                        "subjects": [{"model": "O5 BEV", "powertrain": "BEV"}, {"model": "EV3", "powertrain": "BEV"}],
                        "differentFeatures": [
                            {
                                "feature": "battery",
                                "targetValue": "61 kWh",
                                "competitorValue": "81 kWh",
                                "gap": "smaller battery",
                                "priority": "P0",
                            }
                        ],
                    },
                },
            }
        ],
    )
    config_missing = {item["name"] for item in config_package["missingEvidence"]}
    config_labels = " ".join(
        str(ref["label"])
        for item in config_package["toolResults"]
        for ref in item["evidenceRefs"]
    )

    assert "feature_diff" not in config_missing
    assert "key_features" not in config_missing
    assert "configuration_delta.battery" in config_labels
    assert "battery.targetValue" in config_labels


def test_competitor_gap_accepts_sales_refs_when_variant_and_price_are_thin() -> None:
    plan = build_evidence_plan("Sweden", "O9 和 XC60 / EX60 的定位差异是什么？")
    package = build_evidence_package(
        session_id="sess_competitor_gap_sales_refs",
        country="Sweden",
        question="O9 和 XC60 / EX60 的定位差异是什么？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "compare_vehicle_variants",
                "query": {"country": "Sweden", "question": "O9 和 XC60 / EX60 的定位差异是什么？"},
                "success": True,
                "result": {
                    "tool": "compare_vehicle_variants",
                    "metadata": {"source": "jato_variant_diff_service"},
                    "data": {
                        "subjects": [],
                        "differentFeatures": [],
                        "commonFeatures": [],
                        "selectionNotes": [],
                    },
                },
            },
            {
                "toolName": "compare_competitive_set",
                "query": {"country": "Sweden", "question": "O9 和 XC60 / EX60 的定位差异是什么？"},
                "success": True,
                "result": {
                    "tool": "compare_competitive_set",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {
                        "targetModel": "O9",
                        "competitors": [
                            {"model": "XC60", "sales": 2893},
                            {"model": "EX30", "sales": 1518},
                        ],
                        "analysis": {"totalCompared": 2, "sourceCount": 2, "hasPricing": False},
                    },
                },
            },
            {
                "toolName": "query_competitive_landscape",
                "query": {"country": "Sweden", "model": "O9"},
                "success": True,
                "result": {
                    "tool": "query_competitive_landscape",
                    "metadata": {"source": "jato_competitive_landscape"},
                    "data": {
                        "targetModel": "O9",
                        "competitors": [],
                        "analysis": {"totalCompared": 0, "sourceCount": 1},
                    },
                },
            },
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "models": ["O9", "XC60", "EX60"]},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {
                        "items": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_requested_models",
                            "nextActions": ["Map requested model names."],
                        },
                    },
                },
            },
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert "price_or_config_gap" not in missing
    assert "competitive_or_configuration_data_unavailable" not in missing
    assert "query_competitive_landscape_weak_evidence_refs" not in missing
    assert missing["coverage_diagnostic:no_current_prices_for_requested_models"]["impact"] == "weakens_answer"


def test_report_generation_uses_method_material_as_supporting_evidence() -> None:
    question = "基于我给的 J7_HEV_V4 材料，把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。"
    plan = build_evidence_plan("Sweden", question)
    package = build_evidence_package(
        session_id="sess_j7_report_method",
        country="Sweden",
        question=question,
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "J7 HEV"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {"items": []},
                },
            },
            {
                "toolName": "compare_competitive_set",
                "query": {"country": "Sweden", "question": "J7 HEV report"},
                "success": True,
                "result": {
                    "tool": "compare_competitive_set",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {"competitors": [], "analysis": {"totalCompared": 0}},
                },
            },
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}
    method_evidence = next(item for item in package["toolResults"] if item["toolName"] == "business_method_material")

    assert "supporting_evidence" not in missing
    assert "report_outline" in missing
    assert missing["report_outline"]["impact"] == "weakens_answer"
    assert method_evidence["evidenceRefs"]


def test_j7_method_material_downgrades_empty_live_msrp_diagnostic_to_cross_check() -> None:
    question = "基于我给的 J7_HEV_V4 材料，瑞典 J7 HEV 应该怎么定价？"
    plan = build_evidence_plan("Sweden", question)
    package = build_evidence_package(
        session_id="sess_j7_coverage_downgrade",
        country="Sweden",
        question=question,
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "model": "J7 HEV"},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {
                        "items": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_requested_models",
                            "nextActions": ["Map requested JATO model names to official MSRP model names."],
                        },
                    },
                },
            },
            {
                "toolName": "compare_competitive_set",
                "query": {"country": "Sweden", "question": "瑞典 J7 HEV 应该怎么定价？"},
                "success": True,
                "result": {
                    "tool": "compare_competitive_set",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {"competitors": [], "analysis": {"totalCompared": 0}},
                },
            },
            _market_chart_context_tool_result(),
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert "business_method_material" in {item["toolName"] for item in package["toolResults"]}
    assert missing["coverage_diagnostic:no_current_prices_for_requested_models"]["impact"] == "weakens_answer"
    assert all(item["impact"] != "blocking" for item in package["missingEvidence"])


def test_user_supplied_price_delta_downgrades_own_msrp_to_cross_check_when_context_exists() -> None:
    plan = build_evidence_plan("Sweden", "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？")
    package = build_evidence_package(
        session_id="sess_o5_delta",
        country="Sweden",
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "models": ["O5 BEV", "EV3"]},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres"},
                    "data": {
                        "items": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_requested_models",
                            "nextActions": ["Map requested model names."],
                        },
                    },
                },
            },
            {
                "toolName": "compare_competitive_set",
                "query": {"country": "Sweden", "models": ["O5 BEV", "EV3"]},
                "success": True,
                "result": {
                    "tool": "compare_competitive_set",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {
                        "targetModel": "O5 BEV",
                        "competitors": [
                            {
                                "model": "EV3",
                                "source": "user_question_model_candidate",
                                "basis": "explicitly named in the user question",
                            }
                        ],
                        "analysis": {"totalCompared": 1},
                    },
                },
            },
            {
                "toolName": "query_price_positioning",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_price_positioning",
                    "metadata": {"source": "jato_price_positioning"},
                    "data": {
                        "priceStats": {"min": 30000, "max": 42000, "median": 36000, "count": 6},
                        "priceRecords": [{"model": "EV3", "msrp": 35000}],
                    },
                },
            },
            {
                "toolName": "search_market_news",
                "query": {"country": "Sweden", "question": "O5 BEV EV3"},
                "success": True,
                "result": {
                    "tool": "search_market_news",
                    "metadata": {"source": "jato_web_search_service"},
                    "data": {
                        "items": [
                            {
                                "title": "O5 BEV and EV3 market context",
                                "date": "2026-06-01",
                                "claim": "EV3 is the named benchmark in this scenario.",
                            }
                        ]
                    },
                },
            },
            _market_chart_context_tool_result("BEV"),
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}
    delta_evidence = next(item for item in package["toolResults"] if item["toolName"] == "user_supplied_price_delta")
    delta_refs = {ref["label"]: ref for ref in delta_evidence["evidenceRefs"]}

    assert delta_refs["User supplied relative price delta"]["value"] == 3000
    assert delta_refs["User supplied relative price delta"]["unit"] == "EUR"
    assert missing["current_msrp"]["impact"] == "weakens_answer"
    assert missing["own_model_price"]["impact"] == "weakens_answer"
    assert missing["coverage_diagnostic:no_current_prices_for_requested_models"]["impact"] == "weakens_answer"
    assert all(item["impact"] != "blocking" for item in package["missingEvidence"])


def test_evidence_package_marks_missing_required_tool_as_blocking() -> None:
    plan = build_evidence_plan("Sweden", "瑞典 J7 HEV 应该如何定价？")
    package = build_evidence_package(
        session_id="sess_missing_tool",
        country="Sweden",
        question="瑞典 J7 HEV 应该如何定价？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_parquet"},
                    "data": {"kpis": {"market_share": 12}},
                },
            }
        ],
    )

    assert package["confidence"] == "medium"
    assert any(
        item["name"] == "missing_required_tool:query_msrp_pricing" and item["impact"] == "blocking"
        for item in package["missingEvidence"]
    )


def test_grounding_guard_blocks_numeric_answer_without_refs() -> None:
    guarded = apply_answer_grounding_guard(
        {"direct": "建议定价 300000 SEK。", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_empty",
            "intent": "pricing_analysis",
            "country": "Sweden",
            "toolResults": [],
            "missingEvidence": [],
            "confidence": "low",
        },
    )

    assert guarded["answerStatus"] == "insufficient_evidence"
    assert "不能" in guarded["direct"]


def test_grounding_guard_does_not_count_zero_row_snapshot_refs_as_business_evidence() -> None:
    guarded = apply_answer_grounding_guard(
        {"direct": "Hungary market analysis complete.", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_hu_zero_rows",
            "intent": "market_overview",
            "country": "Hungary",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "jato_parquet",
                    "summary": "query_country_snapshot returned 0 evidence rows.",
                    "keyFindings": ["totalRows: 0", "countryCount: 0 units", "brandCount: 0 units"],
                    "evidenceRefs": [
                        {"refId": "ev_1_1", "label": "totalRows", "value": 0},
                        {"refId": "ev_1_2", "label": "countryCount", "value": 0, "unit": "units"},
                        {"refId": "ev_1_3", "label": "brandCount", "value": 0, "unit": "units"},
                        {"refId": "ev_1_4", "label": "modelCount", "value": 0, "unit": "units"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "market_kpis", "reason": "Need market KPIs.", "impact": "blocking"},
                {
                    "name": "market_snapshot_data_unavailable",
                    "reason": "Snapshot returned only weak count refs.",
                    "impact": "weakens_answer",
                },
            ],
            "confidence": "low",
        },
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
        evidence_plan={"intent": "market_overview"},
    )

    rendered = " ".join([
        guarded["direct"],
        guarded["summary"],
        " ".join(guarded["bullets"]),
        " ".join(guarded["reportReadyBullets"]),
    ])
    assert guarded["answerStatus"] == "insufficient_evidence"
    assert guarded["grounding"]["evidenceRefCount"] == 0
    assert "缺少可引用证据" in rendered
    assert "4 条可引用证据" not in rendered
    assert "0 条可引用证据" not in rendered
    assert all(not action["evidenceRefs"] for action in guarded["recommendedActions"])


def test_grounding_guard_keeps_useful_path_when_required_tool_missing() -> None:
    guarded = apply_answer_grounding_guard(
        {"direct": "当前可以先判断价格逻辑。", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_partial",
            "intent": "pricing_analysis",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [{"refId": "ev_1", "label": "market_share", "value": 12}],
                }
            ],
            "missingEvidence": [
                {
                    "name": "missing_required_tool:query_msrp_pricing",
                    "reason": "Pricing intent requires MSRP coverage.",
                    "impact": "blocking",
                }
            ],
            "confidence": "medium",
        },
    )

    assert guarded["answerStatus"] == "partially_answered"
    joined = " ".join(guarded["bullets"])
    assert "当前能判断" in joined
    assert "建议查数动作" in joined
    assert "建议输出形态" in joined


def test_deterministic_judge_scores_quality_loop_contract() -> None:
    score = score_deterministic_answer(
        expected={
            "expectedIntent": "pricing_analysis",
            "mustUseTools": ["query_msrp_pricing"],
            "expectedFollowUpTypes": ["compare", "drilldown"],
        },
        predicted_intent="pricing_analysis",
        tools_used=["query_msrp_pricing"],
        answer={"direct": "当前基于证据回答。", "answerStatus": "answered"},
        evidence_package={
            "evidenceId": "evpkg_test",
            "toolResults": [{"evidenceRefs": [{"refId": "ev_1"}]}],
            "missingEvidence": [],
            "confidence": "high",
        },
        follow_ups=[
            {"intent": "compare"},
            {"intent": "drilldown"},
            {"intent": "action"},
        ],
    )

    assert score["totalScore"] == 1
    assert score["failures"] == []


def test_deterministic_judge_caps_score_when_blocking_evidence_is_missing() -> None:
    score = score_deterministic_answer(
        expected={
            "expectedIntent": "pricing_analysis",
            "mustUseTools": ["query_msrp_pricing"],
            "expectedFollowUpTypes": ["compare", "drilldown"],
        },
        predicted_intent="pricing_analysis",
        tools_used=["query_msrp_pricing"],
        answer={"direct": "当前只能做方向判断。", "answerStatus": "partially_answered"},
        evidence_package={
            "evidenceId": "evpkg_partial",
            "toolResults": [{"evidenceRefs": [{"refId": "ev_1", "label": "O9 price claim"}]}],
            "missingEvidence": [
                {"name": "current_msrp", "reason": "No MSRP evidence.", "impact": "blocking"},
            ],
            "confidence": "low",
        },
        follow_ups=[
            {"intent": "compare"},
            {"intent": "drilldown"},
            {"intent": "action"},
        ],
    )

    assert score["groundingScore"] == 0.6
    assert score["businessCompletenessScore"] <= 0.72
    assert score["totalScore"] == 0.74
    assert "missing_blocking_evidence" in score["failures"]


def test_deterministic_judge_accepts_one_clear_p0_business_action() -> None:
    score = score_deterministic_answer(
        expected={
            "expectedIntent": "market_overview",
            "mustUseTools": ["build_market_chart"],
            "expectedFollowUpTypes": ["drilldown", "compare"],
        },
        predicted_intent="market_overview",
        tools_used=["build_market_chart"],
        answer={
            "direct": "直接结论：匈牙利 HEV + SUV A0/A 可以作为优先验证入口。",
            "answerStatus": "answered",
            "businessSynthesisPlan": {
                "intent": "market_overview",
                "executiveConclusion": "直接结论：匈牙利 HEV + SUV A0/A 可以作为优先验证入口。",
                "evidenceAlignment": {"status": "partially_aligned"},
            },
            "businessImplications": ["市场数据要落到机会 segment。", "动力结构要转成进入顺序。"],
            "recommendedActions": [
                {
                    "priority": "P0",
                    "action": "拆到匈牙利市场 HEV SUV A0/A 车型级竞品池、价格和配置矩阵",
                    "rationale": "把市场结构落到车型级可执行验证。",
                    "evidenceRefs": ["ev_1"],
                }
            ],
            "reportReadyBullets": ["机会入口：HEV + SUV A0/A。", "关键证据：HEV 规模。", "下一步：补车型级矩阵。"],
        },
        evidence_package={
            "evidenceId": "evpkg_market",
            "intent": "market_overview",
            "toolResults": [{"evidenceRefs": [{"refId": "ev_1", "label": "HEV sales"}]}],
            "missingEvidence": [],
            "confidence": "medium",
        },
        follow_ups=[
            {"intent": "drilldown"},
            {"intent": "compare"},
            {"intent": "action"},
        ],
    )

    assert score["actionabilityScore"] == 1.0
    assert "business_missing_recommended_actions" not in score["failures"]


def test_deterministic_judge_penalizes_template_only_report_synthesis() -> None:
    score = score_deterministic_answer(
        expected={
            "expectedIntent": "report_generation",
            "mustUseTools": ["build_market_chart"],
            "expectedFollowUpTypes": ["report", "data_check"],
        },
        predicted_intent="report_generation",
        tools_used=["build_market_chart"],
        answer={
            "direct": "直接结论：Sweden 这页汇报应收敛为 Title / Key message / Evidence / Product implication / Next action。",
            "answerStatus": "answered",
            "businessSynthesisPlan": {
                "intent": "report_generation",
                "executiveConclusion": "直接结论：Sweden 这页汇报应收敛为 Title / Key message / Evidence / Product implication / Next action。",
                "evidenceAlignment": {"status": "partially_aligned"},
            },
            "businessImplications": ["汇报生成要先压出 key message。", "缺口写成验证项。"],
            "recommendedActions": [{"action": "生成一页 PPT block"}, {"action": "补齐证据引用"}],
            "reportReadyBullets": [
                "Sweden 汇报页应压成 Title / Key message / Evidence / Product implication / Next action 五块。",
                "Key message 必须先给业务立场。",
                "建议动作：生成一页 PPT block。",
            ],
        },
        evidence_package={
            "evidenceId": "evpkg_report",
            "intent": "report_generation",
            "toolResults": [{"evidenceRefs": [{"refId": "ev_1", "label": "chart_count"}]}],
            "missingEvidence": [],
            "confidence": "low",
        },
        follow_ups=[
            {"intent": "report"},
            {"intent": "data_check"},
            {"intent": "drilldown"},
        ],
    )

    assert score["businessSynthesisScore"] < 0.8
    assert score["reportReadinessScore"] == 0.4
    assert "business_synthesis_too_generic" in score["failures"]


def test_eval_v2_run_outputs_markdown_report() -> None:
    report = run_eval_v2(limit=5)

    assert report["summary"]["total"] == 5
    assert "# AstrBot Eval v2 Report" in report["markdown"]


def test_llm_judge_is_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("APP_ASTRBOT_LLM_JUDGE_ENABLED", raising=False)

    result = judge_answer_with_llm(
        question="瑞典 J7 HEV 应该如何定价？",
        answer={"direct": "当前证据不足。"},
        evidence_package={"toolResults": [], "missingEvidence": []},
        follow_ups=[],
    )

    assert result["status"] == "disabled"


def test_side_by_side_judge_rubric_caps_unsupported_numbers_and_missing_artifacts() -> None:
    scores = llm_judge_service._apply_side_by_side_rubric_caps(
        {
            "astrbotScores": {
                "intentAccuracy": 5,
                "toolSelection": 5,
                "grounding": 5,
                "pmInsight": 5,
                "actionability": 5,
                "artifactQuality": 5,
                "followUpValue": 5,
                "presentationReadiness": 5,
            },
            "countryCopilotScores": {
                "intentAccuracy": 5,
                "toolSelection": 5,
                "grounding": 5,
                "pmInsight": 5,
                "actionability": 5,
                "artifactQuality": 5,
                "followUpValue": 5,
                "presentationReadiness": 5,
            },
            "failureTags": [],
        },
        record={
            "category": "report_generation",
            "question": "生成瑞典 J7 HEV 定价图表和汇报结构。",
            "astrbot": {
                "answerPreview": "建议定价 34720 EUR。",
                "evidenceRefCount": 0,
                "visualArtifacts": [],
                "followUps": [],
            },
            "countryCopilot": {
                "answerPreview": "建议定价 34720 EUR。",
                "sourceCount": 0,
                "chartLinkCount": 0,
                "evidenceTableCount": 0,
            },
        },
        score_dimensions=[
            "intentAccuracy",
            "toolSelection",
            "grounding",
            "pmInsight",
            "actionability",
            "artifactQuality",
            "followUpValue",
            "presentationReadiness",
        ],
        failure_taxonomy=[
            "evidence_missing",
            "hallucination_risk",
            "chart_not_useful",
            "followup_low_value",
            "answer_too_conservative",
            "pm_insight_weak",
        ],
    )

    assert scores["astrbotScores"]["grounding"] == 2
    assert scores["countryCopilotScores"]["grounding"] == 2
    assert scores["astrbotScores"]["artifactQuality"] == 3
    assert scores["astrbotScores"]["followUpValue"] == 3
    assert "hallucination_risk" in scores["failureTags"]
    assert "chart_not_useful" in scores["failureTags"]


def test_evidence_package_uses_published_date_refs_for_policy_governance() -> None:
    plan = build_evidence_plan("Sweden", "CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？")
    package = build_evidence_package(
        session_id="sess_policy_dates",
        country="Sweden",
        question="CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        evidence_plan=plan,
        tool_results=[
            {
                "toolName": "external_research",
                "query": {"country": "Sweden", "question": "CO2 0-75g/km tax PHEV"},
                "success": True,
                "result": {
                    "tool": "external_research",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "items": [
                            {
                                "title": "Sweden company car tax update",
                                "url": "https://example.test/policy",
                                "publishedAt": "2026-05-01",
                                "claim": "CO2 tax bands affect PHEV company car economics.",
                            }
                        ],
                        "researchGovernance": {
                            "policyStatus": "warning",
                            "missingEvidence": [
                                {
                                    "name": "published_date",
                                    "reason": "Research policy requires publish dates.",
                                    "impact": "weakens_answer",
                                }
                            ],
                        },
                    },
                },
            }
        ],
    )

    labels = [
        str(ref["label"])
        for item in package["toolResults"]
        for ref in item["evidenceRefs"]
    ]
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert any(label.endswith(".date") for label in labels)
    assert "published_date" not in missing_names


def test_evidence_needed_published_date_accepts_date_refs_without_metric_card_strength() -> None:
    package = build_evidence_package(
        session_id="sess_policy_date_needed",
        country="Sweden",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        evidence_plan={
            "intent": "news_policy_search",
            "entities": {"models": ["O5 BEV"]},
            "requiredTools": ["external_research"],
            "allowedTools": ["external_research"],
            "evidenceNeeded": [
                {
                    "name": "published_date",
                    "reason": "Policy answers need dated source evidence.",
                    "priority": 1,
                }
            ],
        },
        tool_results=[
            {
                "toolName": "external_research",
                "query": {"country": "Sweden", "question": "BEV subsidy cap O5 BEV"},
                "success": True,
                "result": {
                    "tool": "external_research",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "citations": [
                            {
                                "title": "EV incentive cap",
                                "url": "https://example.test/incentive",
                                "date": "2026-04-15",
                                "supportedClaim": "Price caps change BEV trim positioning.",
                            }
                        ],
                    },
                },
            }
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}
    date_refs = [
        ref
        for item in package["toolResults"]
        for ref in item["evidenceRefs"]
        if str(ref["label"]).endswith(".date")
    ]

    assert date_refs
    assert "published_date" not in missing_names


def test_external_claim_refs_suppress_duplicate_weak_research_gaps() -> None:
    package = build_evidence_package(
        session_id="sess_policy_claims",
        country="Sweden",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        evidence_plan={
            "intent": "news_policy_search",
            "entities": {"models": ["O5 BEV"]},
            "requiredTools": ["external_research"],
            "allowedTools": ["external_research", "read_web_page"],
            "evidenceNeeded": [
                {
                    "name": "fresh_external_signal",
                    "reason": "Policy answers need source-backed external evidence.",
                    "priority": 1,
                }
            ],
        },
        tool_results=[
            {
                "toolName": "external_research",
                "query": {
                    "country": "Sweden",
                    "question": "BEV subsidy cap O5 BEV",
                },
                "success": True,
                "result": {
                    "tool": "external_research",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "items": [
                            {
                                "title": "Sweden EV incentive cap",
                                "url": "https://example.test/incentive",
                                "claim": (
                                    "The vehicle price cap changes eligible BEV "
                                    "trim positioning."
                                ),
                            }
                        ],
                        "researchGovernance": {
                            "policyStatus": "warning",
                            "missingEvidence": [
                                {
                                    "name": "external_research_claims_unavailable",
                                    "reason": "No supported claims were available.",
                                    "impact": "weakens_answer",
                                }
                            ],
                        },
                    },
                },
            },
            {
                "toolName": "read_web_page",
                "query": {"url": "https://example.test/incentive"},
                "success": True,
                "result": {
                    "tool": "read_web_page",
                    "metadata": {"source": "jato_browser_readonly"},
                    "data": {
                        "status": "ok",
                        "url": "https://example.test/incentive",
                    },
                },
            },
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}
    claim_refs = [
        ref
        for item in package["toolResults"]
        for ref in item["evidenceRefs"]
        if str(ref["label"]).endswith(".claim")
    ]

    assert claim_refs
    assert "external_research_claims_unavailable" not in missing_names
    assert "read_web_page_weak_evidence_refs" not in missing_names


def test_external_research_snippet_becomes_claim_and_date_evidence() -> None:
    package = build_evidence_package(
        session_id="sess_policy_snippet_claim",
        country="Sweden",
        question="瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        evidence_plan={
            "intent": "news_policy_search",
            "entities": {},
            "requiredTools": ["external_research"],
            "allowedTools": ["external_research"],
            "evidenceNeeded": [
                {
                    "name": "fresh_external_signal",
                    "reason": "Policy answer needs source-backed external evidence.",
                    "priority": 1,
                },
                {
                    "name": "published_date",
                    "reason": "Policy answer needs a dated source.",
                    "priority": 1,
                },
            ],
        },
        tool_results=[
            {
                "toolName": "external_research",
                "query": {
                    "country": "Sweden",
                    "question": "Sweden company car BEV PHEV tax benefit",
                },
                "success": True,
                "result": {
                    "tool": "external_research",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "items": [
                            {
                                "title": "Sweden company car tax benefit for electric and plug-in hybrid vehicles",
                                "url": "https://example.test/company-car",
                                "snippet": "Official vehicle tax guidance explains how company car benefit values differ for BEV and PHEV models in Sweden.",
                                "publishedAt": "2026-06-01",
                            }
                        ],
                        "researchGovernance": {
                            "policyStatus": "warning",
                            "missingEvidence": [
                                {
                                    "name": "external_research_claims_unavailable",
                                    "reason": "No supported external claims were available.",
                                    "impact": "weakens_answer",
                                },
                                {
                                    "name": "published_date",
                                    "reason": "Research policy requires publish dates.",
                                    "impact": "weakens_answer",
                                },
                            ],
                        },
                    },
                },
            }
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}
    refs = [
        ref
        for item in package["toolResults"]
        for ref in item["evidenceRefs"]
    ]

    assert any(str(ref["label"]).endswith(".claim") for ref in refs)
    assert any(str(ref["label"]).endswith(".date") for ref in refs)
    claim_refs = [ref for ref in refs if str(ref["label"]).endswith(".claim")]
    assert claim_refs
    assert claim_refs[0]["source"] == "https://example.test/company-car"
    assert "external_research_claims_unavailable" not in missing_names
    assert "published_date" not in missing_names


def test_external_research_title_can_be_claim_when_snippet_is_missing() -> None:
    package = build_evidence_package(
        session_id="sess_policy_title_claim",
        country="Hungary",
        question="匈牙利 2026 EV 补贴政策有什么官方来源？",
        evidence_plan={
            "intent": "news_policy_search",
            "entities": {},
            "requiredTools": ["external_research"],
            "allowedTools": ["external_research"],
            "evidenceNeeded": [
                {
                    "name": "fresh_external_signal",
                    "reason": "Policy answer needs source-backed external evidence.",
                    "priority": 1,
                }
            ],
        },
        tool_results=[
            {
                "toolName": "external_research",
                "query": {
                    "country": "Hungary",
                    "question": "Hungary 2026 EV subsidy official source",
                },
                "success": True,
                "result": {
                    "tool": "external_research",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "citations": [
                            {
                                "title": "Hungary official 2026 electric vehicle subsidy eligibility update for company buyers",
                                "url": "https://gov.hu/ev-subsidy-2026",
                                "snippet": "",
                                "publishedAt": "2026-01-10",
                            }
                        ]
                    },
                },
            }
        ],
    )

    refs = [
        ref
        for item in package["toolResults"]
        for ref in item["evidenceRefs"]
    ]
    missing_names = {item["name"] for item in package["missingEvidence"]}
    claim_refs = [ref for ref in refs if str(ref["label"]).endswith(".claim")]

    assert claim_refs
    assert claim_refs[0]["source"] == "https://gov.hu/ev-subsidy-2026"
    assert "Hungary official 2026 electric vehicle subsidy" in str(claim_refs[0]["value"])
    assert "external_research_claims_unavailable" not in missing_names


def test_empty_external_research_and_technical_snapshot_counts_keep_low_confidence() -> None:
    package = build_evidence_package(
        session_id="sess_voc_empty_external",
        country="Sweden",
        question="拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        evidence_plan={
            "intent": "voc_analysis",
            "entities": {"models": ["OMODA", "JAECOO"]},
            "requiredTools": ["external_research"],
            "allowedTools": ["external_research", "query_country_snapshot"],
            "evidenceNeeded": [
                {
                    "name": "fresh_external_signal",
                    "reason": "VOC answers need source-backed user/media evidence.",
                    "priority": 1,
                }
            ],
        },
        tool_results=[
            {
                "toolName": "external_research",
                "query": {
                    "country": "Sweden",
                    "question": "Nordic user VOC tow hook roof load winter tires",
                },
                "success": True,
                "result": {
                    "tool": "external_research",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {"status": "empty", "items": [], "citations": []},
                },
            },
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot"},
                    "data": {
                        "kpis": {
                            "totalRows": 33327,
                            "countryCount": 1,
                            "modelCount": 539,
                        }
                    },
                },
            },
        ],
    )

    missing_names = {item["name"] for item in package["missingEvidence"]}
    snapshot_refs = [
        ref
        for item in package["toolResults"]
        if item["toolName"] == "query_country_snapshot"
        for ref in item["evidenceRefs"]
    ]

    assert snapshot_refs
    assert "external_research_claims_unavailable" in missing_names
    assert evidence_ref_count(package) == 0
    assert package["confidence"] == "low"
