from __future__ import annotations

import inspect

from app.services import jato_business_composer_service as composer
from app.services.jato_answer_grounding_service import apply_answer_grounding_guard
from app.services.jato_business_composer_service import apply_business_composer
from app.services.jato_business_composer_service import build_business_synthesis_plan
from app.services.jato_business_composer_service import _competitor_requested_entities
from app.services.jato_business_composer_service import _clean_visible_direct_text
from app.services.jato_business_composer_service import _source_repair_action_text
from app.services.jato_evidence_package_service import build_evidence_package


def _pricing_package(**overrides):
    package = {
        "evidenceId": "evpkg_pricing",
        "intent": "pricing_analysis",
        "country": "Sweden",
        "toolResults": [
            {
                "toolName": "query_msrp_pricing",
                "success": True,
                "rowCount": 2,
                "sourceType": "postgres",
                "summary": "J7 HEV Premium price and competitor corridor evidence.",
                "keyFindings": ["J7 HEV Premium MSRP: 34720 EUR", "Core corridor: 30000-40000 EUR"],
                "evidenceRefs": [
                    {"refId": "ev_1", "label": "J7 Premium MSRP", "value": 34720, "unit": "currency"},
                    {"refId": "ev_2", "label": "competitor corridor", "value": "30000-40000 EUR"},
                ],
            },
            {
                "toolName": "external_research",
                "success": True,
                "rowCount": 2,
                "sourceType": "web",
                "summary": "RAV4 delivery and Corolla Cross market context.",
                "keyFindings": ["RAV4 supply window supports challenger entry"],
                "evidenceRefs": [{"refId": "ev_3", "label": "RAV4 supply claim", "value": "delivery window"}],
            },
        ],
        "missingEvidence": [],
        "confidence": "high",
        "jatoCrossCheck": {"status": "matched", "summary": "Internal and external direction align."},
        "insightCards": [
            {
                "title": "High trim value",
                "claim": "Premium trim value is visible to customers.",
                "implication": "低配应作为价格锚点，高配作为主推版本。",
                "recommendedAction": "把 J7 HEV 定价页写成市场窗口、竞品走廊、配置价值和版本策略四段。",
                "citations": ["R1"],
                "confidence": "high",
            }
        ],
    }
    package.update(overrides)
    return package


def test_evidence_digest_displays_market_metric_period_scope() -> None:
    package = _pricing_package(
        toolResults=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "rowCount": 1,
                "sourceType": "jato_parquet",
                "summary": "Scoped market evidence.",
                "keyFindings": [],
                "evidenceRefs": [
                    {
                        "refId": "ev_month_hev",
                        "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales",
                        "value": 1946,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                        "periodType": "month",
                        "periodLabel": "2026-03 当月",
                        "periodStart": "2026-03",
                        "periodEnd": "2026-03",
                    },
                    {
                        "refId": "ev_ytd_hev",
                        "label": "contextSnapshot.powertrainMix.HEV.sales",
                        "value": 5051,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                        "periodType": "ytd",
                        "periodLabel": "2026 YTD（截至 2026-03）",
                        "periodStart": "2026-01",
                        "periodEnd": "2026-03",
                    },
                ],
            },
        ],
    )

    lines = composer._evidence_digest_lines(package, "market_overview", limit=4)

    assert any("2026-03 当月 HEV 动力销量 = 1,946 units" in line for line in lines)
    assert any("2026 YTD（截至 2026-03） HEV 动力销量 = 5,051 units" in line for line in lines)
    assert composer._market_cross_tab_ref_value(
        package,
        table="driveByFuel",
        row="HEV",
        metric="sales",
    ) == "1,946 units（2026-03 当月）"


def _pricing_package_with_method_material(**overrides):
    package = _pricing_package()
    method_tool = {
        "toolName": "business_method_material",
        "success": True,
        "rowCount": 6,
        "sourceType": "generated",
        "summary": "J7 HEV pricing method distilled from user material.",
        "keyFindings": ["Use mid-corridor positioning as a hypothesis."],
        "evidenceRefs": [
            {
                "refId": "method_positioning",
                "label": "J7 HEV user material positioning",
                "value": "核心竞争带中段 + 高配主推",
                "source": "J7_HEV_V4.pptx",
                "table": "business_method_material",
            },
            {
                "refId": "method_price",
                "label": "J7 HEV user material main trim MSRP",
                "value": 34720,
                "unit": "EUR",
                "source": "J7_HEV_V4.pptx",
                "table": "business_method_material",
            },
            {
                "refId": "method_corridor",
                "label": "J7 HEV user material competitor corridor",
                "value": "30,000-40,000 EUR",
                "source": "J7_HEV_V4.pptx",
                "table": "business_method_material",
            },
            {
                "refId": "method_gap",
                "label": "J7 HEV user material price gap",
                "value": 3230,
                "unit": "EUR",
                "source": "J7_HEV_V4.pptx",
                "table": "business_method_material",
            },
            {
                "refId": "method_pva",
                "label": "J7 HEV user material PVA coverage",
                "value": 118,
                "unit": "%",
                "source": "J7_HEV_V4.pptx",
                "table": "business_method_material",
            },
            {
                "refId": "method_pool",
                "label": "J7 HEV user material competitor pool",
                "value": "Corolla Cross, RAV4, C-HR, Qashqai",
                "source": "J7_HEV_V4.pptx",
                "table": "business_method_material",
            },
            {
                "refId": "method_market",
                "label": "J7 HEV user material market window",
                "value": "瑞典 2025.04–2026.03 HEV 总规模约 22,816 台。",
                "source": "J7_HEV_V4.pptx",
                "table": "business_method_material",
            },
        ],
    }
    package["toolResults"] = [*package["toolResults"], method_tool]
    package.update(overrides)
    return package


def _market_package(**overrides):
    package = {
        "evidenceId": "evpkg_market",
        "intent": "market_overview",
        "country": "Sweden",
        "toolResults": [
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 12,
                "sourceType": "jato_parquet",
                "summary": "Sweden market snapshot with fuel and segment split.",
                "keyFindings": ["BEV leads electrified demand", "HEV keeps a practical low-risk role"],
                "evidenceRefs": [
                    {"refId": "ev_bev", "label": "BEV share", "value": "40.9%", "unit": "%"},
                    {"refId": "ev_hev", "label": "HEV share", "value": "7.3%", "unit": "%"},
                    {"refId": "ev_suv", "label": "SUV A0/A concentration", "value": "mainstream"},
                ],
            }
        ],
        "missingEvidence": [],
        "confidence": "high",
        "jatoCrossCheck": {"status": "partially_aligned", "summary": "Internal market evidence is usable."},
        "insightCards": [],
    }
    package.update(overrides)
    return package


def _external_research_tool(title: str, url: str, *, claim: str, date: str = "2026-02-10", rank: int = 1):
    return {
        "toolName": "external_research",
        "success": True,
        "rowCount": 1,
        "sourceType": "web",
        "summary": claim,
        "keyFindings": [claim],
        "evidenceRefs": [
            {"refId": f"ext_{rank}_source", "label": f"{title}.source", "value": url, "source": url},
            {"refId": f"ext_{rank}_claim", "label": f"{title}.claim", "value": claim, "source": url},
            {"refId": f"ext_{rank}_date", "label": f"{title}.date", "value": date, "source": url},
            {"refId": f"ext_{rank}_rank", "label": f"{title}.rank", "value": rank, "source": url},
        ],
    }


def test_pricing_synthesis_uses_j7_material_style() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package_with_method_material(),
        question="瑞典 J7 HEV 应该怎么定价？",
    )

    assert "验证版" in plan["reportReadyBullets"][0]
    assert "最终 MSRP 需要官方价格和竞品走廊证明" in plan["reportReadyBullets"][0]
    report_text = " ".join(plan["reportReadyBullets"])
    assert "用户材料假设" in report_text
    assert "用户材料" in report_text
    assert "最终定价结论" not in report_text
    all_business_text = " ".join([
        plan["executiveConclusion"],
        *plan["businessImplications"],
        *plan["reportReadyBullets"],
    ])
    assert "。。" not in all_business_text
    assert "。." not in all_business_text
    assert plan["evidenceAlignment"]["status"] == "aligned"
    assert any("高配" in item for item in plan["businessImplications"])
    assert plan["recommendedActions"][0]["citationIds"] == ["R1"]


def test_direct_cleaner_localizes_artifact_names_without_touching_structured_display_plan() -> None:
    direct = _clean_visible_direct_text(
        "下一步执行：刷新 Pricing corridor chart，再输出 Competitor comparison table 和 PPT-ready block。",
        strip_artifact_names=True,
    )

    assert "Pricing corridor chart" not in direct
    assert "Competitor comparison table" not in direct
    assert "PPT-ready block" not in direct
    assert "价格走廊图" in direct
    assert "竞品对比表" in direct
    assert "汇报块" in direct


def test_market_drive_decision_uses_retrieved_2wd_4wd_mix() -> None:
    package = _market_package(
        toolResults=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "sourceType": "jato_parquet",
                "summary": "Hungary HEV drivetrain split.",
                "keyFindings": ["HEV is primarily 2WD in the retrieved market structure."],
                "evidenceRefs": [
                    {"refId": "hev_2wd", "label": "driveByFuel.HEV.2WD_pct", "value": 89.5, "unit": "%"},
                    {"refId": "hev_4wd", "label": "driveByFuel.HEV.4WD_pct", "value": 9.9, "unit": "%"},
                ],
            },
        ],
        country="Hungary",
    )

    composed = apply_business_composer(
        {"direct": "", "answerStatus": "answered"},
        package,
        country="Hungary",
        question="匈牙利 J7 HEV 应先把 2WD 还是 4WD 作为主销？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7 HEV"]}},
    )

    assert "优先以 2WD 作为主销方向" in composed["direct"]
    assert "89.5" in composed["direct"]
    assert "9.9" in composed["direct"]
    assert "最终版型配比仍需" in composed["direct"]


def test_market_drive_decision_supports_blind_model_from_evidence_entities() -> None:
    package = _market_package(
        country="Hungary",
        entities={"models": ["Aurora HEV"], "powertrains": ["HEV"]},
        toolResults=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "sourceType": "jato_parquet",
                "summary": "Hungary blind-model drivetrain context.",
                "evidenceRefs": [
                    {"refId": "blind_hev_2wd", "label": "driveByFuel.HEV.2WD_pct", "value": 76.4, "unit": "%"},
                    {"refId": "blind_hev_4wd", "label": "driveByFuel.HEV.4WD_pct", "value": 23.6, "unit": "%"},
                ],
            },
        ],
    )

    composed = apply_business_composer(
        {"direct": "", "answerStatus": "answered"},
        package,
        country="Hungary",
        question="匈牙利 Aurora HEV 应先把 2WD 还是 4WD 作为主销？",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"models": ["Aurora HEV"], "powertrains": ["HEV"]},
        },
    )

    assert "匈牙利市场 Aurora HEV 应优先以 2WD 作为主销方向" in composed["direct"]
    assert "76.4%" in composed["direct"]
    assert "23.6%" in composed["direct"]
    assert all(name not in composed["direct"] for name in ("J7", "J8", "O5", "O9"))


def test_composer_keeps_provider_percent_values_when_refs_store_value_and_unit_separately() -> None:
    provider_direct = (
        "基于本轮数据，匈牙利 HEV 市场 2WD 占绝对主导（89.5%），4WD 仅占 9.9%，"
        "且 SUV A0 细分市场同样以 2WD 为主（88.1%）。J7 HEV 应优先将 2WD 作为主销配置，"
        "4WD 可作为高配选项满足少数冬季和牵引需求，但不应作为主推。"
    )
    package = _market_package(
        country="Hungary",
        toolResults=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "sourceType": "jato_parquet",
                "summary": "Hungary HEV drivetrain split.",
                "keyFindings": [],
                "evidenceRefs": [
                    {"refId": "hev_2wd", "label": "driveByFuel.HEV.2WD_pct", "value": 89.5, "unit": "%"},
                    {"refId": "hev_4wd", "label": "driveByFuel.HEV.4WD_pct", "value": 9.9, "unit": "%"},
                    {"refId": "suv_a0_2wd", "label": "driveBySegment.SUV A0.2WD_pct", "value": 88.1, "unit": "%"},
                ],
            },
        ],
    )

    composed = apply_business_composer(
        {"direct": provider_direct, "answerStatus": "answered"},
        package,
        country="Hungary",
        question="匈牙利 J7 HEV 应先把 2WD 还是 4WD 作为主销？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7 HEV"]}},
    )

    assert composed["direct"] == provider_direct
    assert composed["grounding"]["providerNarrativeStatus"] == "kept"


def test_j7_material_only_pricing_answer_does_not_present_template_as_data() -> None:
    package = _pricing_package(
        toolResults=[
            {
                "toolName": "business_method_material",
                "success": True,
                "rowCount": 3,
                "sourceType": "generated",
                "summary": "J7 HEV pricing method distilled from user material.",
                "keyFindings": ["Use mid-corridor positioning as a hypothesis."],
                "evidenceRefs": [
                    {
                        "refId": "method_positioning",
                        "label": "J7 HEV user material positioning",
                        "value": "核心竞争带中段 + 高配主推",
                        "source": "J7_HEV_V4.pptx",
                        "table": "business_method_material",
                    },
                    {
                        "refId": "method_price",
                        "label": "J7 HEV user material main trim MSRP",
                        "value": 34720,
                        "unit": "EUR",
                        "source": "J7_HEV_V4.pptx",
                        "table": "business_method_material",
                    },
                    {
                        "refId": "method_corridor",
                        "label": "J7 HEV user material competitor corridor",
                        "value": "30,000-40,000 EUR",
                        "source": "J7_HEV_V4.pptx",
                        "table": "business_method_material",
                    },
                ],
            }
        ],
        missingEvidence=[
            {"name": "current_msrp", "reason": "No official MSRP.", "impact": "weakens_answer"},
            {"name": "competitor_price_range", "reason": "No competitor price rows.", "impact": "weakens_answer"},
        ],
        confidence="medium",
    )

    answer = apply_business_composer(
        {"title": "Pricing", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        package,
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("验证版定价立场：瑞典 J7 HEV 可以先按")
    assert "低配做价格锚点，高配做主推版本" in direct
    assert "用户材料假设边界" in direct
    assert "34,720" in direct or "34720" in direct
    assert "不是最终官方 MSRP" in direct
    assert "不是当前官方 MSRP 或已验证竞品价" in direct


def test_pricing_answer_hides_internal_coverage_diagnostic_codes() -> None:
    package = _pricing_package(
        missingEvidence=[
            {
                "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                "reason": "No current price rows.",
                "impact": "weakens_answer",
            },
            {
                "name": "coverage_diagnostic:no_config_projects_for_country",
                "reason": "No configuration projects for requested market.",
                "impact": "weakens_answer",
            },
        ],
    )

    answer = apply_business_composer(
        {"title": "Pricing", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        package,
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]}},
    )

    direct = answer["direct"]
    assert "coverage_diagnostic" not in direct
    assert "coverage diagnostic" not in direct
    assert "no config projects" not in direct
    assert "当前价格" in direct
    assert "配置" in direct


def test_pricing_method_answer_uses_method_scope_not_j7_hardcoded_switch(monkeypatch) -> None:
    fake_method = {
        "methodId": "method_test_hev",
        "methodType": "pricing_positioning",
        "sourceName": "Test_HEV_Method.pdf",
        "market": "Sweden",
        "model": "Test HEV",
        "priceCorridor": {
            "positioning": "核心带低位 + 高配主推",
            "coreCorridor": "31,000-39,000 EUR",
            "mainTrimPrice": "34,100 EUR",
        },
        "versionStrategy": {
            "priceGap": "2,800 EUR",
            "pvaCoverage": "112%",
            "salesTalk": ["省心", "高配可见"],
        },
        "competitorPool": ["Model A", "Model B"],
        "featureValueClaims": [{"featureName": "HUD"}],
        "pricingPlaybook": {
            "market_window": "HEV demand window needs live cross-check",
            "competitor_corridor": "Test HEV should be checked against Model A and Model B.",
            "product_value_delta": "Visible high-trim value must cover the price gap.",
        },
        "coreClaims": [],
    }
    monkeypatch.setattr(composer, "get_active_pricing_method", lambda **_: fake_method)
    package = _pricing_package(
        toolResults=[
            {
                "toolName": "business_method_material",
                "success": True,
                "rowCount": 3,
                "sourceType": "generated",
                "summary": "Test HEV pricing method distilled from user material.",
                "keyFindings": ["Use low-corridor positioning as a hypothesis."],
                "evidenceRefs": [
                    {
                        "refId": "method_test_positioning",
                        "label": "Test HEV user material positioning",
                        "value": "核心带低位 + 高配主推",
                        "source": "Test_HEV_Method.pdf",
                        "table": "business_method_material",
                    },
                    {
                        "refId": "method_test_price",
                        "label": "Test HEV user material main trim MSRP",
                        "value": 34100,
                        "unit": "EUR",
                        "source": "Test_HEV_Method.pdf",
                        "table": "business_method_material",
                    },
                    {
                        "refId": "method_test_corridor",
                        "label": "Test HEV user material competitor corridor",
                        "value": "31,000-39,000 EUR",
                        "source": "Test_HEV_Method.pdf",
                        "table": "business_method_material",
                    },
                ],
            }
        ],
        missingEvidence=[
            {"name": "current_msrp", "reason": "No official MSRP.", "impact": "weakens_answer"},
            {"name": "competitor_price_range", "reason": "No competitor price rows.", "impact": "weakens_answer"},
        ],
        confidence="medium",
    )

    answer = apply_business_composer(
        {"title": "Pricing", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        package,
        country="Sweden",
        question="瑞典 Test HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["Test HEV"], "powertrains": ["HEV"]}},
    )

    direct = answer["direct"]
    assert answer["title"] == "瑞典 · Test HEV 验证版定价逻辑"
    assert direct.startswith("验证版定价立场：瑞典 Test HEV 可以先按")
    assert "34,100 EUR" in direct or "34100" in direct
    assert "Model A" in direct
    assert "J7" not in direct
    assert "RAV4" not in direct


def test_non_j7_pricing_does_not_use_j7_trim_strategy_template() -> None:
    package = _pricing_package(
        insightCards=[],
        missingEvidence=[
            {"name": "current_msrp", "reason": "No current MSRP evidence.", "impact": "blocking"},
            {"name": "price_corridor", "reason": "No corridor evidence.", "impact": "weakens_answer"},
        ],
        confidence="low",
    )
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "partially_answered"},
        evidence_package=package,
        country="Sweden",
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O9"]}},
    )

    joined = " ".join([plan["executiveConclusion"], *plan["businessImplications"], *plan["reportReadyBullets"]])
    assert "核心竞争带中段 + 高配主推" not in joined
    assert "低配做价格锚点" not in joined
    assert "价格矩阵、竞品走廊和高配价值验证表" in plan["executiveConclusion"]
    assert "J7" not in plan["executiveConclusion"]
    assert "RAV4" not in plan["executiveConclusion"]
    assert "leasing/RV" in " ".join(plan["reportReadyBullets"])


def test_market_overview_direct_leads_with_evidence_claims_not_generic_template() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "rowCount": 12,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary market snapshot with fuel and segment split.",
                    "keyFindings": ["HEV remains visible in Hungary SUV demand."],
                    "evidenceRefs": [
                        {"refId": "hu_hev", "label": "HEV share", "value": "18.2%", "unit": "%"},
                        {"refId": "hu_phev", "label": "PHEV share", "value": "6.4%", "unit": "%"},
                        {"refId": "hu_suv", "label": "SUV A0/A concentration", "value": "high"},
                    ],
                }
            ],
        ),
        country="Hungary",
        question="匈牙利市场整体结构怎么看？",
        evidence_plan={"intent": "market_overview", "entities": {"countries": ["Hungary"], "powertrains": ["HEV", "PHEV"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("匈牙利市场总览先看已查数据：")
    assert "HEV 18.2%" in direct
    assert "PHEV 6.4%" in direct
    assert "市场总览的重点不是复述份额" not in direct
    assert "已有可引用总览证据" not in direct
    assert "套用固定话术" not in direct


def test_relative_price_delta_uses_structured_scenario_evidence() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _pricing_package(
            toolResults=[
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No O5 or EV3 official MSRP rows.",
                    "evidenceRefs": [],
                },
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "Price positioning sample is available.",
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato"},
                        {"refId": "ev_avg", "label": "priceStats.avg", "value": 48467.39, "unit": "currency", "source": "jato"},
                        {"refId": "ev_med", "label": "priceStats.median", "value": 52130.43, "unit": "currency", "source": "jato"},
                    ],
                },
                {
                    "toolName": "user_supplied_price_delta",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "generated",
                    "summary": "User supplied a 3k cheaper scenario.",
                    "evidenceRefs": [
                        {"refId": "ev_delta", "label": "User supplied relative price delta", "value": 3000, "unit": "EUR", "source": "user_question"},
                        {"refId": "ev_dir", "label": "User supplied price-delta direction", "value": "cheaper", "source": "user_question"},
                    ],
                },
            ],
            missingEvidence=[
                {"name": "current_msrp", "reason": "No official MSRP for O5 or EV3.", "impact": "weakens_answer"},
                {"name": "own_model_price", "reason": "No O5 official MSRP.", "impact": "weakens_answer"},
            ],
            confidence="high",
        ),
        country="Sweden",
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("相对定价判断：O5 BEV 在瑞典比 EV3 低 3,000 EUR 可以作为待验证价格场景")
    assert "不是最终价差结论" in direct
    assert "用户提出的低 3,000 EUR 是决策输入，不是已查事实" in direct
    assert "如果该差额能覆盖 EV3 的配置/续航/动力优势与 O5 BEV 的品牌、残值或渠道风险" in direct
    assert "当前价格样本显示" not in direct
    assert "参考价格样本区间约 39,122-53,165" not in direct
    assert "当前还没有形成 O5 BEV 与 EV3 的官方 MSRP、月供/RV 和促销支持可引用矩阵" in direct
    assert "不能写死用户场景价差 3,000 EUR" in direct
    assert "补齐 O5 BEV 与 EV3 的 MSRP / TP / 月供 / RV / 配置差异矩阵" in direct
    assert "J7 HEV" not in direct
    assert "Pricing corridor chart" not in direct
    assert "Pricing evidence table" not in direct
    assert "Pricing reference sample chart" in answer["displayPlan"]
    assert "Pricing evidence table" in answer["displayPlan"]
    assert "MSRP source validation table" in answer["displayPlan"]
    assert "柱状图" not in answer["displayPlan"]
    assert "必须补证：先把 O5 和 EV3" not in direct
    digest = answer["evidenceDigest"]
    assert digest[0] == "本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证"
    assert any(item.startswith("背景价格样本最低值 = 39,121.7") for item in digest)
    assert any(item.startswith("用户给定相对价差 = 3,000 EUR") for item in digest)
    assert not any(item.startswith("价格样本最低值") for item in digest)
    assert "User supplied relative price delta" not in " ".join(digest)


def test_pricing_verified_evidence_skips_price_stats_when_requested_prices_are_missing() -> None:
    package = _pricing_package(
        entities={"models": ["J7 HEV"], "competitors": ["Sportage HEV"]},
        toolResults=[
            {
                "toolName": "query_msrp_pricing",
                "success": True,
                "rowCount": 0,
                "sourceType": "postgres",
                "summary": "No current MSRP rows for requested models.",
                "evidenceRefs": [
                    {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_msrp_postgres"},
                    {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_msrp_postgres"},
                    {"refId": "ev_avg", "label": "priceStats.avg", "value": 48467.39, "unit": "currency", "source": "jato_msrp_postgres"},
                ],
            },
        ],
        missingEvidence=[
            {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "reason": "No J7/Sportage current MSRP rows.", "impact": "weakens_answer"},
            {"name": "current_msrp", "reason": "Requested current MSRP is missing.", "impact": "weakens_answer"},
        ],
        confidence="medium",
    )

    lines = composer._pricing_verified_evidence_lines(package, limit=4)

    assert not lines


def test_relative_price_delta_prefers_evidence_over_prompt_number() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _pricing_package(
            toolResults=[
                {
                    "toolName": "user_supplied_price_delta",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "generated",
                    "summary": "User supplied delta extracted from context.",
                    "evidenceRefs": [
                        {"refId": "ev_delta", "label": "User supplied relative price delta", "value": 2500, "unit": "EUR", "source": "user_question"},
                        {"refId": "ev_dir", "label": "User supplied price-delta direction", "value": "cheaper", "source": "user_question"},
                    ],
                }
            ],
            missingEvidence=[
                {"name": "current_msrp", "reason": "No official MSRP for O5 or EV3.", "impact": "weakens_answer"},
                {"name": "own_model_price", "reason": "No O5 official MSRP.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]}},
    )

    direct = answer["direct"]
    digest = " ".join(answer["evidenceDigest"])
    assert "比 EV3 低 2,500 EUR" in direct
    assert "用户提出的低 2,500 EUR 是决策输入" in direct
    assert "用户场景价差 2,500 EUR" in direct
    assert "3,000 EUR" not in direct
    assert "用户给定相对价差 = 2,500 EUR" in digest


def test_weak_supporting_price_gap_downgrades_runtime_status_and_confidence() -> None:
    answer = apply_answer_grounding_guard(
        {
            "title": "Competitor",
            "direct": "XC60 sales evidence supports the positioning, but requested-model price coverage is missing.",
            "bullets": [],
            "limitations": [],
        },
        _pricing_package(
            intent="competitor_compare",
            confidence="high",
            missingEvidence=[
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Requested models have no current MSRP rows.",
                    "impact": "weakens_answer",
                }
            ],
        ),
        country="Sweden",
        question="O9 和 XC60 / EX60 的定位差异是什么？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["O9"], "competitors": ["XC60", "EX60"]}},
    )

    assert answer["answerStatus"] == "insufficient_evidence"
    assert answer["confidence"] == "low"
    assert answer["grounding"]["confidence"] == "low"
    assert answer["evidencePackage"]["confidence"] == "low"
    assert "J7" not in str(answer["evidencePackage"])


def test_competitor_direct_uses_current_market_not_sweden_template() -> None:
    answer = apply_business_composer(
        {
            "title": "Hungary O9 competitor positioning",
            "direct": "O9 should be compared with XC60 and EX60.",
            "bullets": [],
            "limitations": [],
        },
        _pricing_package(
            intent="competitor_compare",
            country="Hungary",
            entities={"models": ["O9"], "competitors": ["XC60", "EX60"]},
            toolResults=[
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary competitor evidence for XC60 and EX60.",
                    "keyFindings": ["XC60 has a competitor sales anchor."],
                    "evidenceRefs": [
                        {"refId": "xc60_sales", "label": "topModels.XC60.sales", "value": 740, "unit": "units", "source": "jato"},
                        {"refId": "ex60_role", "label": "competitor.EX60.role", "value": "BEV validation anchor", "source": "jato"},
                    ],
                }
            ],
            missingEvidence=[
                {"name": "current_msrp", "reason": "No O9 current MSRP.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Hungary",
        question="匈牙利 O9 和 XC60 / EX60 的定位差异是什么？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["O9"], "competitors": ["XC60", "EX60"]}},
    )

    visible_text = " ".join([answer["direct"], *answer["bullets"], *answer["reportReadyBullets"]])

    assert "匈牙利" in visible_text
    assert "瑞典" not in visible_text


def test_competitor_market_context_direct_turns_evidence_into_business_verdict() -> None:
    answer = apply_business_composer(
        {"title": "Compare", "direct": "基于证据回答。", "answerStatus": "answered"},
        _market_package(
            intent="competitor_compare",
            country="Sweden",
            entities={"models": ["J8"], "competitors": ["Sorento"]},
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 4,
                    "sourceType": "jato_parquet",
                    "summary": "Segment and channel context for 7-seat AWD competitor question.",
                    "evidenceRefs": [
                        {
                            "refId": "ev_suv_a_sales",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                            "value": 7544,
                            "unit": "units",
                            "source": "jato_country_chart_deck",
                        },
                        {
                            "refId": "ev_suv_a_4wd",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct",
                            "value": 60.1,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                        {
                            "refId": "ev_suv_a_phev",
                            "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.PHEV_pct",
                            "value": 38.2,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                        {
                            "refId": "ev_phev_business",
                            "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct",
                            "value": 64.8,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                    ],
                }
            ],
            missingEvidence=[
                {"name": "current_msrp", "reason": "No J8/Sorento official price rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No J8/Sorento configuration rows.", "impact": "weakens_answer"},
                {"name": "monthly_payment", "reason": "No leasing/RV rows.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="J8 7 座四驱为什么能打 Sorento？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento"]}},
    )

    direct = answer["direct"]

    assert direct.startswith("对标判断：")
    assert "关键证据：瑞典" in direct
    assert "第一版结论不是“已胜出”，而是“有场景型切入理由”" in direct
    assert "J8 可以先按场景型挑战者去打 Sorento" in direct
    assert "SUV A 有 7,544 units 规模" in direct
    assert "SUV A 四驱需求占比达到 60.1%" in direct
    assert "SUV A PHEV 渗透率达到 38.2%" in direct
    assert "PHEV 公司车注册占比达到 64.8%" in direct
    assert "价格 = 待补本车型和核心竞品官方 MSRP / 当前价格来源" in direct
    assert "配置差异 = 待补逐项配置 / 版本 / 价值差异" in direct
    assert "月供/RV = 待补 leasing、残值和 company car 成本口径" in direct
    assert "不能替代车型级销量、MSRP、配置差异或 TCO 证据" in direct
    assert "现在只能先做场景验证" not in direct
    assert "JATO 图表数据" in direct
    assert "contextSnapshot" not in direct


def test_business_question_subject_removes_control_prompt_residue() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "partially_answered"},
        evidence_package=_market_package(country="Hungary"),
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？请不要回答瑞典。",
        evidence_plan={"intent": "market_overview", "entities": {}},
    )

    assert "分析对象：匈牙利市场现在适合推 PHEV 还是 HEV" in plan["executiveConclusion"]
    assert "请。" not in plan["executiveConclusion"]
    assert "不要回答瑞典" not in plan["executiveConclusion"]


def test_market_overview_uses_question_specific_business_stance() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_market_package(),
        country="Sweden",
        question="北欧 BEV 增长是否会压缩 HEV 空间？",
        evidence_plan={"intent": "market_overview"},
    )

    assert "BEV 增长会压缩 HEV 空间" in plan["executiveConclusion"]
    assert "不是把 HEV 一次性替代掉" in plan["executiveConclusion"]
    assert "这题需要先给业务立场" not in plan["executiveConclusion"]


def test_j7_hev_market_overview_avoids_generic_market_template() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_market_package(),
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={"intent": "market_overview"},
    )

    assert "J7 HEV 的机会入口已有市场结构证据支撑" in plan["executiveConclusion"]
    assert "HEV 份额 7.3%" in plan["executiveConclusion"]
    assert "SUV A0/A 集中度 mainstream" in plan["executiveConclusion"]
    assert "不是最终上市或定价结论" in plan["executiveConclusion"]
    assert "市场总览的重点不是复述份额" not in plan["executiveConclusion"]


def test_j7_hev_market_overview_uses_powertrain_mix_when_cross_tabs_are_missing() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            toolResults=[
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "rowCount": 6,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden powertrain mix snapshot.",
                    "keyFindings": ["HEV demand pool is measurable."],
                    "evidenceRefs": [
                        {
                            "refId": "ev_bev_sales",
                            "label": "contextSnapshot.powertrainMix.BEV.sales",
                            "value": 25235,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_phev_sales",
                            "label": "contextSnapshot.powertrainMix.PHEV.sales",
                            "value": 15028,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_hev_sales",
                            "label": "contextSnapshot.powertrainMix.HEV.sales",
                            "value": 5051,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_2023",
                            "label": "contextSnapshot.yearSeries.2023.value",
                            "value": 289827,
                            "source": "jato_country_snapshot",
                        },
                        {
                            "refId": "ev_2024",
                            "label": "contextSnapshot.yearSeries.2024.value",
                            "value": 269580,
                            "source": "jato_country_snapshot",
                        },
                        {
                            "refId": "ev_2025",
                            "label": "contextSnapshot.yearSeries.2025.value",
                            "value": 272998,
                            "source": "jato_country_snapshot",
                        },
                    ],
                }
            ]
        ),
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={"intent": "market_overview"},
    )

    direct = answer["direct"]
    assert "J7 HEV 的机会入口已有市场结构证据支撑" in direct
    assert "不是最终上市或定价结论" in direct
    assert "内部 JATO 动力类型 mix 显示 HEV 规模 5,051 units" in direct
    assert "动力结构对比显示 BEV 25,235 units、PHEV 15,028 units，国家总量口径 HEV 5,051 units" in direct
    assert "HEV 机会应按真实需求池、细分集中度和渠道场景判断" in direct
    assert "年度走势 2023 289,827 -> 2024 269,580 -> 2025 272,998" in direct
    assert "yearSeries" not in direct
    assert "还不能单独证明 J7 HEV 已适配" in direct
    assert "补 HEV + SUV A0/A 结构" in direct
    assert "直接结论：Sweden HEV 市场适合 J7 的理由不是 HEV 总量足够大" not in direct


def test_market_annual_trend_does_not_compare_incomplete_current_year_with_full_years() -> None:
    trend = composer._market_annual_total_trend_evidence(
        [
            {"label": "contextSnapshot.yearSeries.2024.value", "value": 269580},
            {"label": "contextSnapshot.yearSeries.2025.value", "value": 272998},
            {"label": "contextSnapshot.yearSeries.2026.value", "value": 61960},
        ],
        current_year=2026,
    )

    assert "年度走势 2024 269,580 -> 2025 272,998，较2024年增长1.3%" in trend
    assert "2026年内累计 61,960，不与完整年直接比较" in trend
    assert "下降77" not in trend


def test_market_overview_missing_hungary_hev_evidence_does_not_claim_j7_fit() -> None:
    package = _market_package(
        country="Hungary",
        toolResults=[
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 0,
                "sourceType": "jato_parquet",
                "summary": "No Hungary internal market snapshot rows were available.",
                "keyFindings": ["totalRows: 0"],
                "evidenceRefs": [{"refId": "ev_empty", "label": "totalRows", "value": 0}],
            },
            {
                "toolName": "external_research",
                "success": True,
                "rowCount": 3,
                "sourceType": "web",
                "summary": "External sources mainly cover Hungary BEV policy signals.",
                "keyFindings": ["BEV subsidy context is available"],
                "evidenceRefs": [{"refId": "ev_web", "label": "BEV policy context", "value": "BEV incentives"}],
            },
        ],
        missingEvidence=[
            {"name": "market_snapshot_data_unavailable", "reason": "No internal HEV market rows.", "impact": "weakens_answer"},
            {"name": "jato_cross_check", "reason": "No JATO cross-check evidence.", "impact": "weakens_answer"},
        ],
        confidence="medium",
        jatoCrossCheck={"status": "missing", "summary": "Internal cross-check unavailable."},
    )
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "partially_answered"},
        evidence_package=package,
        country="Hungary",
        question="匈牙利 HEV 市场现在适不适合 J7？请先判断国家，不要回答瑞典。",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7"]}},
    )

    assert "暂不能把匈牙利市场判定为 J7 HEV 的已验证进入机会" in plan["executiveConclusion"]
    assert "待验证机会" in plan["executiveConclusion"]
    assert "再决定是否进入定价页" in plan["executiveConclusion"]
    assert "J7 HEV 的 HEV SUV A0/A、核心竞品池和价格/配置证据" in plan["executiveConclusion"]
    assert "Toyota/Kia 竞品池" not in plan["executiveConclusion"]
    assert "下一步执行：拆到车型/品牌" not in plan["executiveConclusion"]
    assert "内部市场快照、HEV 销量/份额和车型结构证据" in plan["executiveConclusion"]
    assert "JATO 内部交叉验证" in plan["executiveConclusion"]
    assert "HEV 市场适合 J7 的理由" not in plan["executiveConclusion"]
    assert "不要回答瑞典" not in plan["executiveConclusion"]
    assert "Market opportunity playbook" not in " ".join(plan["businessImplications"])
    assert "市场机会方法" in plan["businessImplications"][0]


def test_market_overview_uses_chart_refs_even_when_snapshot_gap_is_stale() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "基于证据回答。", "answerStatus": "answered", "bullets": [], "limitations": []},
        _market_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "generated",
                    "summary": "Hungary market chart with usable HEV/SUV structure refs.",
                    "keyFindings": ["HEV and SUV segment structure are available."],
                    "evidenceRefs": [
                        {
                            "refId": "hu_hev_units",
                            "label": "contextSnapshot.powertrainMix.HEV.sales",
                            "value": 2687,
                            "unit": "units",
                        },
                        {
                            "refId": "hu_suv_a0_units",
                            "label": "contextSnapshot.crossTabs.registrationBySegment.SUV A0.sales",
                            "value": 7303,
                            "unit": "units",
                        },
                        {
                            "refId": "hu_hev_2wd",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct",
                            "value": 89.5,
                            "unit": "%",
                        },
                    ],
                }
            ],
            missingEvidence=[
                {
                    "name": "market_snapshot_data_unavailable",
                    "reason": "query_country_snapshot returned no direct rows.",
                    "impact": "weakens_answer",
                }
            ],
            confidence="high",
        ),
        country="Hungary",
        question="匈牙利 HEV 市场机会是什么？请不要回答瑞典，请给出市场结构证据并生成图表。",
        evidence_plan={"intent": "market_overview"},
    )

    visible_text = " ".join([
        answer["direct"],
        *answer["bullets"],
        *answer["limitations"],
        *answer["reportReadyBullets"],
        *[item["action"] for item in answer["recommendedActions"]],
    ])

    assert "2,687" in visible_text
    assert "89.5" in visible_text
    assert "已有市场结构证据支撑" in answer["direct"]
    assert "市场路线判断只能先作为方向假设" not in visible_text
    assert "市场快照证据不足" not in visible_text
    assert "market_snapshot_data_unavailable" not in visible_text
    assert "瑞典" not in answer["direct"]


def test_market_fit_gap_uses_pm_stance_even_without_usable_refs() -> None:
    package = _market_package(
        country="Hungary",
        toolResults=[],
        missingEvidence=[
            {"name": "market_snapshot_data_unavailable", "reason": "No internal HEV market rows.", "impact": "weakens_answer"},
            {"name": "jato_cross_check", "reason": "No JATO cross-check evidence.", "impact": "weakens_answer"},
        ],
        confidence="low",
        jatoCrossCheck={"status": "missing", "summary": "Internal cross-check unavailable."},
    )
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "insufficient_evidence"},
        evidence_package=package,
        country="Hungary",
        question="匈牙利 HEV 市场现在适不适合 J7？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7"]}},
    )

    assert "暂不能把匈牙利市场判定为 J7 HEV 的已验证进入机会" in plan["executiveConclusion"]
    assert "当前缺少可引用证据" in plan["executiveConclusion"]
    assert "J7 HEV 的 HEV SUV A0/A、核心竞品池和价格/配置证据" in plan["executiveConclusion"]
    assert "Toyota/Kia 竞品池" not in plan["executiveConclusion"]
    assert "现在还不能给确定数字" not in plan["executiveConclusion"]


def test_j7_hev_validation_question_uses_market_fit_gap_stance() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "当前证据不足。", "answerStatus": "insufficient_evidence"},
        _market_package(
            country="Hungary",
            toolResults=[],
            missingEvidence=[
                {"name": "market_snapshot_data_unavailable", "reason": "No Hungary HEV rows.", "impact": "weakens_answer"},
                {"name": "jato_cross_check", "reason": "No JATO cross-check evidence.", "impact": "weakens_answer"},
            ],
            confidence="low",
            jatoCrossCheck={"status": "missing", "summary": "Internal cross-check unavailable."},
        ),
        country="Hungary",
        question="匈牙利 J7 HEV 是否值得继续验证？请简短回答。",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7"]}},
    )

    direct = answer["direct"]
    assert "暂不能把匈牙利市场判定为 J7 HEV 的已验证进入机会" in direct
    assert "待验证机会" in direct
    assert "补齐匈牙利市场 J7 HEV 的 HEV SUV A0/A、核心竞品池和价格/配置证据" in direct
    assert "Toyota/Kia 竞品池" not in direct
    assert "瑞典" not in direct
    assert "分析对象" not in direct
    assert "请简短回答" not in direct
    assert "现在还不能给确定数字" not in direct
    assert answer["recommendedActions"][0]["action"] == "补齐匈牙利市场 J7 HEV 的 HEV SUV A0/A、核心竞品池和价格/配置证据"
    assert "待验证机会" in answer["reportReadyBullets"][0]
    assert "核心竞品池" in answer["reportReadyBullets"][1]
    assert "竞品池 竞品" not in answer["reportReadyBullets"][1]
    assert "目标市场" not in " ".join(item["action"] for item in answer["recommendedActions"])
    assert "拆到车型/品牌" not in " ".join(item["action"] for item in answer["recommendedActions"])
    assert "目标市场" not in " ".join(answer["bullets"])
    assert "分析对象" not in " ".join(answer["bullets"])
    assert "拆到车型/品牌" not in " ".join(answer["bullets"])
    assert answer["summary"].startswith("匈牙利市场总览")


def test_j7_hev_market_fit_uses_method_competitor_pool_not_fixed_toyota_kia(monkeypatch) -> None:
    fake_method = {
        "methodId": "method_j7_test_pool",
        "methodType": "pricing_positioning",
        "sourceName": "J7_Test_Method.pdf",
        "market": "Hungary",
        "model": "J7 HEV",
        "priceCorridor": {"positioning": "核心带中段 + 高配主推"},
        "competitorPool": ["Model A", "Model B"],
        "pricingPlaybook": {"market_window": "HEV demand window needs live cross-check"},
        "featureValueClaims": [],
        "coreClaims": [],
    }
    monkeypatch.setattr(composer, "get_active_pricing_method", lambda **_: fake_method)

    answer = apply_business_composer(
        {"title": "Market", "direct": "当前证据不足。", "answerStatus": "insufficient_evidence"},
        _market_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "generated",
                    "summary": "J7 HEV method distilled from user material.",
                    "keyFindings": ["Use Model A and Model B as the first competitor pool."],
                    "evidenceRefs": [
                        {
                            "refId": "method_j7_pool",
                            "label": "J7 HEV user material competitor pool",
                            "value": "Model A, Model B",
                            "source": "J7_Test_Method.pdf",
                            "table": "business_method_material",
                        }
                    ],
                }
            ],
            missingEvidence=[
                {"name": "market_snapshot_data_unavailable", "reason": "No Hungary HEV rows.", "impact": "weakens_answer"},
                {"name": "jato_cross_check", "reason": "No JATO cross-check evidence.", "impact": "weakens_answer"},
            ],
            confidence="low",
            jatoCrossCheck={"status": "missing", "summary": "Internal cross-check unavailable."},
        ),
        country="Hungary",
        question="匈牙利 J7 HEV 是否值得继续验证？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7 HEV"]}},
    )

    text = " ".join([
        answer["direct"],
        *answer["reportReadyBullets"],
        *[item["action"] for item in answer["recommendedActions"]],
    ])
    assert "Model A、Model B" in text
    assert "Model A、Model B 竞品窗口" in text
    assert "Toyota/Kia" not in text


def test_j7_hev_market_fit_ignores_unscoped_method_pool(monkeypatch) -> None:
    fake_method = {
        "methodId": "method_wrong_market",
        "methodType": "pricing_positioning",
        "sourceName": "J7_HEV_V4.pptx",
        "market": "Sweden",
        "model": "J7 HEV",
        "priceCorridor": {"positioning": "核心带中段 + 高配主推"},
        "competitorPool": ["Corolla Cross", "RAV4"],
        "pricingPlaybook": {"market_window": "Sweden HEV demand window"},
        "featureValueClaims": [],
        "coreClaims": [],
    }
    monkeypatch.setattr(composer, "get_active_pricing_method", lambda **_: fake_method)

    answer = apply_business_composer(
        {"title": "Market", "direct": "当前证据不足。", "answerStatus": "insufficient_evidence"},
        _market_package(
            country="Hungary",
            toolResults=[],
            missingEvidence=[
                {"name": "market_snapshot_data_unavailable", "reason": "No Hungary HEV rows.", "impact": "weakens_answer"},
                {"name": "jato_cross_check", "reason": "No JATO cross-check evidence.", "impact": "weakens_answer"},
            ],
            confidence="low",
            jatoCrossCheck={"status": "missing", "summary": "Internal cross-check unavailable."},
        ),
        country="Hungary",
        question="匈牙利 J7 HEV 是否值得继续验证？请不要回答瑞典。",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7 HEV"]}},
    )

    visible_text = " ".join([
        answer["direct"],
        *answer["reportReadyBullets"],
        *[item["action"] for item in answer["recommendedActions"]],
    ])
    assert "Corolla Cross" not in visible_text
    assert "RAV4" not in visible_text
    assert "Sweden" not in visible_text
    assert "瑞典" not in visible_text
    assert "核心竞品池" in visible_text


def test_missing_market_plan_labels_are_business_readable_chinese() -> None:
    package = _market_package(
        country="Hungary",
        toolResults=[],
        missingEvidence=[
            {"name": "market_kpis", "reason": "No market KPI evidence.", "impact": "blocking"},
            {"name": "trend_or_mix", "reason": "No trend evidence.", "impact": "weakens_answer"},
        ],
        confidence="low",
    )
    guarded = apply_answer_grounding_guard(
        {"direct": "", "bullets": [], "limitations": []},
        package,
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
        evidence_plan={"intent": "market_overview"},
    )

    joined = " ".join([
        str(guarded.get("direct") or ""),
        *[str(item) for item in guarded.get("bullets", [])],
        *[str(item) for item in guarded.get("limitations", [])],
    ])
    assert "市场规模、份额、排名或趋势证据" in joined
    assert "动力结构变化或趋势证据" in joined
    assert "market kpis" not in joined.lower()
    assert "trend or mix" not in joined.lower()


def test_hungary_hev_phev_route_question_outputs_pm_decision_frame() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_market_package(country="Hungary"),
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
        evidence_plan={"intent": "market_overview"},
    )

    joined = " ".join([plan["executiveConclusion"], *plan["reportReadyBullets"]])
    assert "匈牙利" in plan["executiveConclusion"]
    assert "HEV 做低风险主线" in joined
    assert "PHEV 做公司车/TCO 验证线" in joined
    assert "二选一" in plan["executiveConclusion"]
    assert plan["recommendedActions"][0]["action"] == "建立 HEV vs PHEV 场景决策表"
    assert "市场总览的重点不是复述份额" not in plan["executiveConclusion"]


def test_hungary_hev_phev_route_uses_available_powertrain_evidence() -> None:
    package = _market_package(
        country="Hungary",
        toolResults=[
            {
                "toolName": "query_segment_breakdown",
                "success": True,
                "rowCount": 8,
                "sourceType": "jato_parquet",
                "summary": "Hungary HEV and PHEV cross-tab evidence.",
                "keyFindings": ["HEV and PHEV rows available."],
                "evidenceRefs": [
                    {"refId": "ev_hev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 2687, "unit": "units", "source": "jato"},
                    {"refId": "ev_hev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 84.5, "unit": "%", "source": "jato"},
                    {"refId": "ev_phev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.sales", "value": 969, "unit": "units", "source": "jato"},
                    {"refId": "ev_phev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.2WD_pct", "value": 52.0, "unit": "%", "source": "jato"},
                    {"refId": "ev_phev_4wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.4WD_pct", "value": 46.9, "unit": "%", "source": "jato"},
                ],
            }
        ],
    )
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=package,
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
        evidence_plan={"intent": "market_overview"},
    )

    direct = plan["executiveConclusion"]
    assert "已查数据：HEV 2,687 units，2WD 84.5%" in direct
    assert "PHEV 969 units，2WD 52%，4WD 46.9%" in direct
    assert "HEV 做低风险主线" in direct
    assert "PHEV 做公司车/TCO 验证线" in direct
    assert "query_segment_breakdown" not in direct
    assert "后续仍要用销量、份额、税费和月供数据校准主推权重" not in direct


def test_hev_phev_route_keeps_market_mix_and_segment_by_fuel_separate() -> None:
    package = _market_package(
        country="Hungary",
        toolResults=[
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 8,
                "sourceType": "jato_parquet",
                "summary": "Hungary market structure evidence.",
                "evidenceRefs": [
                    {"refId": "hev_sales", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 8200, "unit": "units", "source": "jato_country_snapshot"},
                    {"refId": "hev_share", "label": "contextSnapshot.powertrainMix.HEV.share", "value": 18.5, "unit": "%", "source": "jato_country_snapshot"},
                    {"refId": "phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 3100, "unit": "units", "source": "jato_country_snapshot"},
                    {"refId": "phev_share", "label": "contextSnapshot.powertrainMix.PHEV.share", "value": 7.0, "unit": "%", "source": "jato_country_snapshot"},
                    {"refId": "suv_a_hev_sales", "label": "crossTabs.segmentByFuel.SUV A.HEV.sales", "value": 2450, "unit": "units", "source": "jato_cross_tab"},
                    {"refId": "suv_a_phev_sales", "label": "crossTabs.segmentByFuel.SUV A.PHEV.sales", "value": 780, "unit": "units", "source": "jato_cross_tab"},
                ],
            }
        ],
        missingEvidence=[
            {"name": "price_or_config_gap", "reason": "No competitor price/config matrix yet.", "impact": "weakens_answer"}
        ],
        confidence="high",
    )
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        package,
        country="Hungary",
        question="匈牙利 HEV 市场为什么适合 T7 HEV？请拆 SUV A 级别和 PHEV 对比。",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["T7 HEV"], "powertrains": ["HEV", "PHEV"], "segments": ["SUV A"]}},
    )

    direct = answer["direct"]
    assert "HEV 8,200 units，份额 18.5%" in direct
    assert "PHEV 3,100 units，份额 7%" in direct
    assert "SUV A 细分 HEV 2,450 units，PHEV 780 units" in direct
    assert "HEV 2,450 units，份额 18.5%" not in direct
    assert "PHEV 780 units，份额 7%" not in direct


def test_generic_market_overview_direct_leads_with_available_snapshot_evidence() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "rowCount": 10,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary country snapshot.",
                    "evidenceRefs": [
                        {"refId": "hu_total", "label": "marketSnapshot.kpis.cumulativeSales", "value": 12000, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "hu_bev", "label": "marketSnapshot.powertrainMix.BEV.sales", "value": 4200, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "hu_hev", "label": "marketSnapshot.powertrainMix.HEV.sales", "value": 1200, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "hu_suv_a", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 3535, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "hu_corolla", "label": "topModels.Corolla Cross.sales", "value": 1250, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "hu_tucson", "label": "topModels.Tucson.sales", "value": 980, "unit": "units", "source": "jato_country_snapshot"},
                    ],
                }
            ],
            confidence="high",
        ),
        country="Hungary",
        question="匈牙利市场情况怎么样？",
        evidence_plan={"intent": "market_overview", "entities": {"countries": ["Hungary"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("匈牙利市场总览先看已查数据")
    assert "锁定机会入口、动力结构和级别/车型方向" in direct
    assert "累计销量 12,000 units" in direct
    assert "动力结构 BEV 4,200 units，HEV 1,200 units" in direct
    assert "级别结构 SUV A 3,535 units" in direct
    assert "Top models Corolla Cross 1,250 units，Tucson 980 units" in direct
    assert "具体车型 MSRP、配置差异、月供/RV" in direct
    assert "Powertrain mix chart" in answer["displayPlan"]
    assert "Key metrics" in answer["displayPlan"]
    assert "Market decision table" in answer["displayPlan"]
    assert "需要先给业务立场" not in direct
    assert "已有可引用总览证据" not in direct


def test_market_overview_generic_next_action_uses_entities_instead_of_generic_drilldown() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 8,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary chart snapshot with usable HEV cross-tabs.",
                    "keyFindings": ["HEV and SUV A0/A cross-tabs are available."],
                    "evidenceRefs": [
                        {
                            "refId": "ev_hu_hev_sales",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales",
                            "value": 2687,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_hu_suv_a0",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales",
                            "value": 7303,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_hu_suv_a",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                            "value": 3535,
                            "unit": "units",
                        },
                    ],
                }
            ],
            confidence="medium",
        ),
        country="Hungary",
        question="匈牙利 J7 HEV 市场情况怎么样？请给数据支撑和图表。",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]}},
    )

    direct = answer["direct"]
    assert "下一步执行：把匈牙利市场 J7 HEV 拆到 SUV A0 / SUV A 车型级竞品池、MSRP、配置差异和月供/RV 表" in direct
    assert "拆到车型/品牌" not in direct
    assert "HEV 2,687 units" in direct
    assert "SUV A0 7,303 units" in direct
    assert "瑞典" not in direct


def test_hungary_hev_phev_route_with_internal_gap_still_leads_with_pm_stance() -> None:
    package = _market_package(
        country="Hungary",
        toolResults=[
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 0,
                "sourceType": "jato_parquet",
                "summary": "No Hungary internal market rows.",
                "keyFindings": ["totalRows: 0"],
                "evidenceRefs": [{"refId": "ev_empty", "label": "totalRows", "value": 0}],
            },
            {
                "toolName": "external_research",
                "success": True,
                "rowCount": 2,
                "sourceType": "web",
                "summary": "Hungary external policy context.",
                "keyFindings": ["BEV incentives and company car policy context"],
                "evidenceRefs": [
                    {"refId": "ev_policy", "label": "Hungary BEV incentive claim", "value": "company-only BEV subsidy programme"},
                    {"refId": "ev_tax", "label": "Hungary registration tax claim", "value": "zero-emission exemption"},
                ],
            },
        ],
        missingEvidence=[
            {"name": "market_snapshot_data_unavailable", "reason": "No internal market rows.", "impact": "weakens_answer"},
            {"name": "jato_cross_check", "reason": "No JATO cross-check evidence.", "impact": "weakens_answer"},
        ],
        confidence="medium",
        jatoCrossCheck={"status": "not_available", "summary": "No matching JATO context."},
    )
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "partially_answered"},
        evidence_package=package,
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？不要回答瑞典。",
        evidence_plan={"intent": "market_overview"},
    )

    assert plan["executiveConclusion"].startswith("直接结论：匈牙利市场现阶段不要把 HEV/PHEV 简化成二选一")
    assert "HEV 做低风险主线" in plan["executiveConclusion"]
    assert "PHEV 做公司车/TCO 验证线" in plan["executiveConclusion"]
    assert "产品路线初判" in plan["executiveConclusion"]
    assert "不能下“适合/不适合”的确定结论" not in plan["executiveConclusion"]
    assert "不要回答瑞典" not in plan["executiveConclusion"]


def test_hungary_hev_phev_route_direct_does_not_repeat_report_bullets() -> None:
    package = _market_package(
        country="Hungary",
        toolResults=[
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 0,
                "sourceType": "jato_parquet",
                "summary": "No Hungary internal market rows.",
                "keyFindings": ["totalRows: 0"],
                "evidenceRefs": [{"refId": "ev_empty", "label": "totalRows", "value": 0}],
            },
            {
                "toolName": "external_research",
                "success": True,
                "rowCount": 2,
                "sourceType": "web",
                "summary": "Hungary external policy context.",
                "keyFindings": ["BEV incentives and company car policy context"],
                "evidenceRefs": [
                    {"refId": "ev_policy", "label": "Hungary BEV incentive claim", "value": "company-only BEV subsidy programme"},
                    {"refId": "ev_tax", "label": "Hungary registration tax claim", "value": "zero-emission exemption"},
                ],
            },
        ],
        missingEvidence=[
            {"name": "market_snapshot_data_unavailable", "reason": "No internal market rows.", "impact": "weakens_answer"},
            {"name": "jato_cross_check", "reason": "No JATO cross-check evidence.", "impact": "weakens_answer"},
        ],
        confidence="medium",
        jatoCrossCheck={"status": "not_available", "summary": "No matching JATO context."},
    )
    guarded = apply_answer_grounding_guard(
        {"direct": "基于证据回答。", "bullets": [], "limitations": []},
        package,
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？不要回答瑞典。",
        evidence_plan={"intent": "market_overview"},
    )

    direct = str(guarded.get("direct") or "")
    assert direct.startswith("直接结论：匈牙利市场现阶段不要把 HEV/PHEV 简化成二选一")
    assert "外部背景：Hungary BEV incentive claim = company-only BEV subsidy programme" in direct
    assert "Hungary registration tax claim = zero-emission exemption" in direct
    assert "外部背景不能替代内部销量、动力结构和车型证据" in direct
    assert direct.count("HEV 做低风险主线") == 1
    assert direct.count("PHEV 做公司车/TCO 验证线") == 1
    assert "HEV/PHEV 路线判断不能直接二选一" not in direct
    assert "建议动作：建立 HEV vs PHEV 场景决策表" not in direct
    assert "证据状态" not in direct
    assert "分析对象" not in direct
    assert "证据边界：" not in direct
    assert "条可引用证据" not in direct
    for bullet in guarded["reportReadyBullets"]:
        assert "证据状态" not in bullet
        assert "分析对象" not in bullet
        assert "证据边界：" not in bullet
        assert "条可引用证据" not in bullet
    assert any("HEV/PHEV 路线判断不能直接二选一" in item for item in guarded["reportReadyBullets"])


def test_pricing_target_range_outputs_direct_reasonableness_verdict() -> None:
    package = _pricing_package(
        insightCards=[],
        toolResults=[
            {
                "toolName": "query_price_positioning",
                "success": True,
                "rowCount": 10,
                "sourceType": "jato_parquet",
                "summary": "Price positioning stats.",
                "keyFindings": ["priceStats.median: 53165", "priceStats.avg: 58300"],
                "evidenceRefs": [
                    {"refId": "ev_min", "label": "priceStats.min", "value": 38600, "unit": "currency"},
                    {"refId": "ev_max", "label": "priceStats.max", "value": 91304, "unit": "currency"},
                    {"refId": "ev_avg", "label": "priceStats.avg", "value": 58300, "unit": "currency"},
                    {"refId": "ev_median", "label": "priceStats.median", "value": 53165, "unit": "currency"},
                ],
            },
            {
                "toolName": "user_supplied_target_price",
                "success": True,
                "sourceType": "generated",
                "summary": "User supplied target price.",
                "keyFindings": ["target_price_range:53000-55000 EUR"],
                "evidenceRefs": [
                    {"refId": "ev_target_min", "label": "User supplied own-model target price min", "value": 53000, "unit": "EUR"},
                    {"refId": "ev_target_max", "label": "User supplied own-model target price max", "value": 55000, "unit": "EUR"},
                    {"refId": "ev_target_mid", "label": "User supplied own-model target price midpoint", "value": 54000, "unit": "EUR"},
                ],
            },
        ],
        missingEvidence=[
            {"name": "current_msrp", "reason": "Official current MSRP not materialized.", "impact": "weakens_answer"},
        ],
    )

    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=package,
        country="Sweden",
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O9"]}},
    )

    assert "53,000-55,000 EUR" in plan["executiveConclusion"]
    assert "具备继续验证的合理性" in plan["executiveConclusion"]
    assert "低于均值 58,300" in plan["executiveConclusion"]
    assert "不是官方 MSRP，需要交叉验证" in plan["executiveConclusion"]


def test_pricing_target_range_partial_low_overlap_is_not_reported_inside_corridor() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "当前证据不足，不能给确定数字。", "answerStatus": "partially_answered"},
        _pricing_package(
            insightCards=[],
            entities={"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            toolResults=[
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 4,
                    "sourceType": "postgres",
                    "summary": "Hungary pricing anchors.",
                    "evidenceRefs": [
                        {"refId": "target_min", "label": "User supplied own-model target price min", "value": 32000, "unit": "EUR", "source": "user_material"},
                        {"refId": "target_max", "label": "User supplied own-model target price max", "value": 35000, "unit": "EUR", "source": "user_material"},
                        {"refId": "target_mid", "label": "User supplied own-model target price midpoint", "value": 33500, "unit": "EUR", "source": "user_material"},
                        {"refId": "cor_price", "label": "Corolla Cross.msrp", "value": 34500, "unit": "EUR", "source": "current_price"},
                        {"refId": "tuc_price", "label": "Tucson.msrp", "value": 36800, "unit": "EUR", "source": "current_price"},
                        {"refId": "price_min", "label": "priceStats.min", "value": 34500, "unit": "EUR", "source": "current_price"},
                        {"refId": "price_max", "label": "priceStats.max", "value": 36800, "unit": "EUR", "source": "current_price"},
                        {"refId": "price_median", "label": "priceStats.median", "value": 35650, "unit": "EUR", "source": "current_price"},
                    ],
                },
            ],
            missingEvidence=[
                {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No config diff.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Hungary",
        question="匈牙利 T7 HEV 如果定在 32,000-35,000 EUR，是否能打 Corolla Cross 和 Tucson？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]}},
    )

    direct = answer["direct"]
    joined = " ".join([direct, *answer["bullets"], *answer["reportReadyBullets"]])
    assert "T7 HEV 在匈牙利 32,000-35,000 EUR" in direct
    assert "下沿 32,000 低于参考价格样本区间 34,500-36,800的下沿 34,500" in direct
    assert "上沿 35,000 进入参考价格样本区间 34,500-36,800" in direct
    assert "更像低位切入价或入门锚点" in direct
    assert "整体位于参考价格样本区间 34,500-36,800内" not in joined
    assert "位于参考价格样本区间 34,500-36,800 内" not in joined


def test_generic_pricing_direct_uses_actual_price_refs_before_gap_language() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "当前证据不足，不能给确定数字。", "answerStatus": "partially_answered"},
        _pricing_package(
            insightCards=[],
            country="Hungary",
            entities={"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            toolResults=[
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 3,
                    "sourceType": "postgres",
                    "summary": "Hungary current MSRP anchors.",
                    "evidenceRefs": [
                        {"refId": "t7_price", "label": "T7 HEV.msrp", "value": 33000, "unit": "EUR", "source": "current_price"},
                        {"refId": "cor_price", "label": "Corolla Cross.msrp", "value": 34500, "unit": "EUR", "source": "current_price"},
                        {"refId": "tuc_price", "label": "Tucson.msrp", "value": 36800, "unit": "EUR", "source": "current_price"},
                        {"refId": "price_min", "label": "priceStats.min", "value": 33000, "unit": "EUR", "source": "current_price"},
                        {"refId": "price_max", "label": "priceStats.max", "value": 36800, "unit": "EUR", "source": "current_price"},
                        {"refId": "price_median", "label": "priceStats.median", "value": 34500, "unit": "EUR", "source": "current_price"},
                    ],
                },
            ],
            missingEvidence=[
                {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No feature delta rows.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Hungary",
        question="匈牙利 T7 HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("定价判断：匈牙利 T7 HEV / Corolla Cross / Tucson")
    assert "T7 HEV 33,000 EUR 低于已查竞品价格带 34,500-36,800 EUR" in direct
    assert "本车型 T7 HEV 价格 33,000 EUR" in direct
    assert "竞品 Corolla Cross 价格 34,500 EUR" in direct
    assert "竞品 Tucson 价格 36,800 EUR" in direct
    assert "月供/RV" in direct
    assert "配置差异" in direct
    assert "不能给确定数字" not in direct
    assert "瑞典" not in direct


def test_o9_target_price_direct_uses_pm_verdict_not_raw_status_sentence() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "O9 target price can be considered.", "bullets": [], "limitations": []},
        _pricing_package(
            insightCards=[],
            toolResults=[
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "rowCount": 10,
                    "sourceType": "jato_parquet",
                    "summary": "Price positioning stats.",
                    "keyFindings": ["priceStats.min: 39121", "priceStats.max: 53165", "priceStats.avg: 48467"],
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121.7, "unit": "currency"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.2, "unit": "currency"},
                        {"refId": "ev_avg", "label": "priceStats.avg", "value": 48467.4, "unit": "currency"},
                        {"refId": "ev_median", "label": "priceStats.median", "value": 47000, "unit": "currency"},
                    ],
                },
                {
                    "toolName": "user_supplied_target_price",
                    "success": True,
                    "sourceType": "generated",
                    "summary": "User supplied target price.",
                    "keyFindings": ["target_price_range:53000-55000 EUR"],
                    "evidenceRefs": [
                        {"refId": "ev_target_min", "label": "User supplied own-model target price min", "value": 53000, "unit": "EUR"},
                        {"refId": "ev_target_max", "label": "User supplied own-model target price max", "value": 55000, "unit": "EUR"},
                        {"refId": "ev_target_mid", "label": "User supplied own-model target price midpoint", "value": 54000, "unit": "EUR"},
                    ],
                },
            ],
            missingEvidence=[
                {"name": "current_msrp", "reason": "Official current MSRP not materialized.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O9"]}},
    )

    direct = answer["direct"]
    assert answer["title"] == "瑞典 · O9 目标价合理性判断"
    assert direct.startswith("目标价判断：O9 在瑞典 53,000-55,000 EUR 可以继续验证")
    assert "当前参考价格样本显示：目标价下沿" in direct
    assert "下沿 53,000 仍贴近参考价格样本区间 39,121.7-53,165.2的上沿" in direct
    assert "中点 54,000 和上沿 55,000 已高于参考样本上沿 53,165.2" in direct
    assert "更强配置、质保、公司车或 leasing 价值支撑" in direct
    assert "Pricing corridor chart" not in direct
    assert "Pricing evidence table" not in direct
    assert "Pricing reference sample chart" in answer["displayPlan"]
    assert "不能把参考样本当作核心竞品走廊" in answer["displayPlan"]
    assert "业务含义" in direct
    assert "campaign/RV" in direct
    assert "位于参考价格样本区间 39,121.7-53,165.2 内" not in direct
    assert "已经偏离参考价格样本区间" not in direct
    assert "瑞典 目标价" not in direct
    assert "瑞典 定价逻辑" not in direct
    digest = answer["evidenceDigest"]
    assert digest[0] == "本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证"
    assert any(item.startswith("背景价格样本最低值 = 39,121.7") for item in digest)
    assert any(item.startswith("背景价格样本最高值 = 53,165.2") for item in digest)
    assert any(item.startswith("用户目标价下沿 = 53,000") for item in digest)
    assert not any(item.startswith("价格样本最低值") for item in digest)
    assert "row_count" not in " ".join(digest)
    takeaways = answer["keyTakeaways"]
    joined_takeaways = " ".join(takeaways)
    assert takeaways[0].startswith("Target price：53,000-55,000 EUR")
    assert "Reference sample：39,121.7-53,165.2" in joined_takeaways
    assert "Position：下沿 53,000 仍贴近参考价格样本区间 39,121.7-53,165.2的上沿" in joined_takeaways
    assert "Gap：" in joined_takeaways
    assert "官方 MSRP" in joined_takeaways
    assert "瑞典 定价逻辑应先验证" not in joined_takeaways


def test_generic_target_price_supports_blind_model_and_competitor_sample() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "Use the checked target-price evidence.", "bullets": [], "limitations": []},
        _pricing_package(
            insightCards=[],
            country="Hungary",
            entities={"models": ["Nimbus E"], "competitors": ["Solaris One", "Vela X"]},
            toolResults=[
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "rowCount": 6,
                    "sourceType": "jato_parquet",
                    "summary": "Blind-model reference price sample.",
                    "evidenceRefs": [
                        {"refId": "blind_min", "label": "priceStats.min", "value": 40000, "unit": "EUR"},
                        {"refId": "blind_max", "label": "priceStats.max", "value": 43500, "unit": "EUR"},
                        {"refId": "blind_median", "label": "priceStats.median", "value": 41800, "unit": "EUR"},
                        {"refId": "blind_avg", "label": "priceStats.avg", "value": 42100, "unit": "EUR"},
                    ],
                },
                {
                    "toolName": "user_supplied_target_price",
                    "success": True,
                    "sourceType": "generated",
                    "summary": "User supplied blind-model target price.",
                    "evidenceRefs": [
                        {"refId": "blind_target_min", "label": "User supplied own-model target price min", "value": 42000, "unit": "EUR"},
                        {"refId": "blind_target_max", "label": "User supplied own-model target price max", "value": 44000, "unit": "EUR"},
                        {"refId": "blind_target_mid", "label": "User supplied own-model target price midpoint", "value": 43000, "unit": "EUR"},
                    ],
                },
            ],
            missingEvidence=[
                {"name": "current_msrp", "reason": "No official current target MSRP.", "impact": "weakens_answer"},
                {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Hungary",
        question="Nimbus E 在匈牙利定价 42,000-44,000 EUR 是否合理？",
        evidence_plan={
            "intent": "pricing_analysis",
            "entities": {"models": ["Nimbus E"], "competitors": ["Solaris One", "Vela X"]},
        },
    )

    direct = answer["direct"]
    assert answer["title"] == "匈牙利 · Nimbus E 目标价合理性判断"
    assert direct.startswith("目标价判断：Nimbus E 在匈牙利 42,000-44,000 EUR 可以继续验证")
    assert "上沿 44,000 已高于参考样本上沿 43,500" in direct
    assert "目标价上沿超过参考样本上沿" in direct
    assert "补齐 Nimbus E 官方 MSRP、核心竞品价格走廊、月供/RV 和配置价值表" in direct
    assert all(name not in direct for name in ("O9", "O5", "J7", "EV3"))


def test_generic_target_price_core_has_no_model_specific_branch_literals() -> None:
    core_functions = (
        composer._compose_pricing_direct_answer,
        composer._pricing_target_range_title,
        composer._pricing_target_model_label,
        composer._pricing_target_range_action,
        composer._target_range_position_statement,
        composer._target_range_business_implication,
    )
    source = "\n".join(inspect.getsource(function) for function in core_functions).casefold()

    for model_literal in ("o9", "o5", "j7", "ev3", "nimbus e", "solaris one", "vela x"):
        assert model_literal not in source


def test_user_material_classification_core_has_no_model_specific_branch_literals() -> None:
    core_functions = (
        composer._evidence_ref_is_user_material,
        composer._relative_pricing_ref_is_user_material,
        composer._is_pricing_user_material_ref,
        composer._is_business_method_material_ref,
        composer._pricing_user_material_ref_matches_scope,
        composer._pricing_user_material_group_country_hint,
        composer._legacy_material_source_matches_registered_method,
        composer._public_evidence_ref_source,
    )
    source = "\n".join(inspect.getsource(function) for function in core_functions).casefold()

    for model_literal in ("j7", "o5", "o9", "ev3", "aurora hev", "helios one"):
        assert model_literal not in source


def test_generic_user_output_core_has_no_model_specific_branch_literals() -> None:
    core_functions = (
        composer._user_title_from_plan,
        composer._business_implications,
        composer._public_direct_action_summary,
        composer._executive_conclusion,
        composer._question_specific_executive_conclusion,
        composer._market_drive_strategy_conclusion,
    )
    source = "\n".join(inspect.getsource(function) for function in core_functions).casefold()

    for model_literal in (
        "j7", "j8", "o5", "o9", "ev3", "ex30", "sportage", "sorento", "aurora hev", "test hev",
    ):
        assert model_literal not in source


def test_j7_vs_sportage_pricing_direct_answers_relative_price_question() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "J7 should be positioned carefully.", "bullets": [], "limitations": []},
        _pricing_package(
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "generated",
                    "summary": "Sweden HEV market cross-tabs.",
                    "keyFindings": ["HEV and SUV A0/A pricing context available."],
                    "evidenceRefs": [
                        {"refId": "hev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 1946, "unit": "units"},
                        {"refId": "hev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 85.9, "unit": "%"},
                        {"refId": "hev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.HEV.Business_pct", "value": 54.0, "unit": "%"},
                        {"refId": "hev_private", "label": "contextSnapshot.crossTabs.registrationByFuel.HEV.Private_pct", "value": 46.0, "unit": "%"},
                        {"refId": "suv_a0_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 5416, "unit": "units"},
                        {"refId": "suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units"},
                        {"refId": "suv_a0_hev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A0.HEV_pct", "value": 13.9, "unit": "%"},
                        {"refId": "suv_a_hev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.HEV_pct", "value": 5.4, "unit": "%"},
                    ],
                },
                *_pricing_package()["toolResults"],
            ],
        ),
        country="Sweden",
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"], "competitors": ["Kia Sportage HEV"]}},
    )

    direct = answer["direct"]
    assert answer["title"] == "瑞典 · J7 HEV vs Kia Sportage HEV 定价判断"
    assert direct.startswith("相对定价判断：J7 HEV 在瑞典应比 Kia Sportage HEV 保持更强价格吸引力")
    assert "不能先写死具体价差" in direct
    assert "市场结构证据：已查 HEV 1,946 units；2WD 85.9%" in direct
    assert "Business 54% / Private 46%" in direct
    assert "SUV A0 5,416 units / HEV 13.9%" in direct
    assert "SUV A 7,544 units / HEV 5.4%" in direct
    assert "不能替代 Kia Sportage HEV 当前 MSRP" in direct
    assert "7年/15万公里质保" not in direct
    assert "Pricing corridor chart" not in direct
    assert "Pricing evidence table" not in direct
    assert "Pricing corridor chart" in answer["displayPlan"]
    assert "Pricing evidence table" in answer["displayPlan"]
    assert "柱状图" not in answer["displayPlan"]
    assert "J7 HEV 与 Kia Sportage HEV 的 MSRP / TP / 月供 / RV / 配置差异矩阵" in direct
    assert not direct.startswith("基于瑞典市场、竞品格局和配置差异")


def test_relative_pricing_replaces_grounded_but_evasive_provider_price_sample() -> None:
    provider_direct = (
        "瑞典 J7 HEV 与 Sportage HEV 的定价目前可由市场参考价格样本支撑："
        "最低值 39121.74，最高值 53165.22，均值 48467.39。"
        "这些是背景样本，不是本车型或竞品的当前官方 MSRP，最终价格需要补齐配置和月供数据。"
    )
    answer = apply_business_composer(
        {"title": "Pricing", "direct": provider_direct, "bullets": [], "limitations": [], "answerStatus": "partially_answered"},
        _pricing_package(
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden HEV market cross-tabs.",
                    "keyFindings": ["HEV market context available."],
                    "evidenceRefs": [
                        {"refId": "hev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 1946, "unit": "units"},
                        {"refId": "hev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 85.9, "unit": "%"},
                    ],
                },
                *_pricing_package()["toolResults"],
            ],
        ),
        country="Sweden",
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"], "competitors": ["Kia Sportage HEV"]}},
    )

    assert answer["direct"].startswith(
        "相对定价判断：J7 HEV 在瑞典应比 Kia Sportage HEV 保持更强价格吸引力"
    )
    assert "市场参考价格样本最低值" not in answer["direct"]
    assert answer["grounding"]["providerNarrativeStatus"] == "replaced:relative_pricing_verdict_missing"


def test_relative_pricing_question_echo_does_not_count_as_provider_verdict() -> None:
    question = "J7 HEV 是否应该比 Kia Sportage HEV 便宜？"
    provider_direct = (
        f"直接结论：针对 Sweden 的问题“{question}”，当前应优先依据 query_msrp_pricing 的结果判断。"
        "本次证据来自市场结构和价格定位工具，但当前没有命中本题车型的官方 MSRP。"
    )
    answer = apply_business_composer(
        {
            "title": "Pricing",
            "direct": provider_direct,
            "bullets": [],
            "limitations": [],
            "answerStatus": "partially_answered",
        },
        _pricing_package(
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden HEV market cross-tabs.",
                    "keyFindings": ["HEV market context available."],
                    "evidenceRefs": [
                        {
                            "refId": "hev_sales",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales",
                            "value": 1946,
                            "unit": "units",
                        },
                    ],
                },
                *_pricing_package()["toolResults"],
            ],
        ),
        country="Sweden",
        question=question,
        evidence_plan={
            "intent": "pricing_analysis",
            "entities": {"models": ["J7 HEV"], "competitors": ["Kia Sportage HEV"]},
        },
    )

    assert answer["direct"].startswith(
        "相对定价判断：J7 HEV 在瑞典应比 Kia Sportage HEV 保持更强价格吸引力"
    )
    assert "应优先依据 query_msrp_pricing" not in answer["direct"]
    assert answer["grounding"]["providerNarrativeStatus"] == "replaced:relative_pricing_verdict_missing"


def test_generic_relative_pricing_direct_supports_non_j7_pairs() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "T7 HEV should be cheaper than Tucson HEV.", "bullets": [], "limitations": []},
        _pricing_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary HEV market cross-tabs.",
                    "keyFindings": ["HEV market context available."],
                    "evidenceRefs": [
                        {"refId": "hu_hev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 3200, "unit": "units"},
                        {"refId": "hu_hev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 72.5, "unit": "%"},
                        {"refId": "hu_hev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.HEV.Business_pct", "value": 41.2, "unit": "%"},
                        {"refId": "hu_suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 6100, "unit": "units"},
                        {"refId": "hu_suv_a_hev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.HEV_pct", "value": 18.6, "unit": "%"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "postgres",
                    "summary": "T7 and Tucson price rows.",
                    "keyFindings": ["T7 HEV MSRP 33000 EUR", "Tucson HEV MSRP 35500 EUR"],
                    "evidenceRefs": [
                        {"refId": "hu_t7_msrp", "label": "pricing.records.T7 HEV.msrp", "value": 33000, "unit": "EUR"},
                        {"refId": "hu_tucson_msrp", "label": "pricing.records.Tucson HEV.msrp", "value": 35500, "unit": "EUR"},
                    ],
                },
            ],
        ),
        country="Hungary",
        question="T7 HEV 是否应该比 Tucson HEV 便宜？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["T7 HEV"], "competitors": ["Tucson HEV"]}},
    )

    direct = answer["direct"]

    assert answer["title"] == "匈牙利 · T7 HEV vs Tucson HEV 定价判断"
    assert direct.startswith("相对定价判断：T7 HEV 在匈牙利应比 Tucson HEV 保持更强价格吸引力")
    assert "HEV 3,200 units" in direct
    assert "2WD 72.5%" in direct
    assert "Business 41.2%" in direct
    assert "SUV A 6,100 units / HEV 18.6%" in direct
    assert "价格证据：已查 T7 HEV 33,000 EUR；Tucson HEV 35,500 EUR" in direct
    assert "T7 HEV 与 Tucson HEV 的 MSRP / TP / 月供 / RV / 配置差异矩阵" in direct
    assert "J7" not in direct
    assert "Sportage" not in direct
    assert "540°全景影像" not in direct


def test_generic_relative_pricing_supports_blind_model_pair_with_evidence_delta() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "Compare the two verified price positions.", "bullets": [], "limitations": []},
        _pricing_package(
            country="Hungary",
            entities={"models": ["Aster Q HEV"], "competitors": ["Boreal One HEV"]},
            toolResults=[
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "postgres",
                    "summary": "Accepted current MSRP rows for the requested models.",
                    "evidenceRefs": [
                        {"refId": "aster_price", "label": "pricing.records.Aster Q HEV.msrp", "value": 31500, "unit": "EUR"},
                        {"refId": "boreal_price", "label": "pricing.records.Boreal One HEV.msrp", "value": 34000, "unit": "EUR"},
                    ],
                },
                {
                    "toolName": "user_supplied_price_delta",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "generated",
                    "summary": "User supplied a relative-price scenario.",
                    "evidenceRefs": [
                        {"refId": "blind_delta", "label": "User supplied relative price delta", "value": 2500, "unit": "EUR", "source": "user_question"},
                        {"refId": "blind_direction", "label": "User supplied price-delta direction", "value": "cheaper", "source": "user_question"},
                    ],
                },
            ],
            missingEvidence=[
                {"name": "configuration_delta", "reason": "No feature delta rows.", "impact": "weakens_answer"},
                {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Hungary",
        question="Aster Q HEV 比 Boreal One HEV 低 2500 欧元是否合理？",
        evidence_plan={
            "intent": "pricing_analysis",
            "entities": {"models": ["Aster Q HEV"], "competitors": ["Boreal One HEV"]},
        },
    )

    direct = answer["direct"]
    assert direct.startswith(
        "相对定价判断：Aster Q HEV 在匈牙利比 Boreal One HEV 低 2,500 EUR 可以作为待验证价格场景"
    )
    assert "价格证据：已查 Aster Q HEV 31,500 EUR；Boreal One HEV 34,000 EUR" in direct
    assert "用户提出的低 2,500 EUR 是决策输入，不是已查事实" in direct
    assert "补齐 Aster Q HEV 与 Boreal One HEV 的 MSRP / TP / 月供 / RV / 配置差异矩阵" in direct
    assert all(name not in direct for name in ("J7", "O5", "EV3", "Sportage"))


def test_generic_relative_pricing_core_has_no_model_specific_branch_literals() -> None:
    core_functions = (
        composer._generic_relative_pricing_direct_answer,
        composer._relative_pricing_pair,
        composer._relative_pricing_stance,
        composer._relative_pricing_scenario_decision_note,
        composer._relative_pricing_boundary_note,
    )
    source = "\n".join(inspect.getsource(function) for function in core_functions).casefold()

    for model_literal in ("j7", "o5", "ev3", "sportage", "tucson", "aster q", "boreal one"):
        assert model_literal not in source


def test_relative_pricing_separates_user_material_and_source_draft_from_official_price() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "J7 should be cheaper than Sportage.", "bullets": [], "limitations": []},
        _pricing_package(
            toolResults=[
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "rowCount": 5,
                    "sourceType": "generated",
                    "summary": "J7 HEV user material pricing method.",
                    "keyFindings": ["Main trim 34720 EUR", "Core corridor 30000-40000 EUR"],
                    "evidenceRefs": [
                        {"refId": "ev_j7_price", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No accepted Sportage current MSRP rows; source draft is available.",
                    "keyFindings": ["coverage_gap:no_current_prices_for_requested_models"],
                    "evidenceRefs": [
                        {"refId": "ev_sportage_draft", "label": "pricing.records.Sportage HEV.sourceDraftPath", "value": "source_draft_available", "source": "jato_msrp_postgres"},
                        {"refId": "ev_sportage_materialization", "label": "pricing.records.Sportage HEV.materializationStatus", "value": "ready_for_extraction", "source": "jato_msrp_postgres"},
                    ],
                    "coverageDiagnostics": {
                        "diagnosis": "no_current_prices_for_requested_models",
                        "sourceRepairCandidates": {
                            "dataStatus": "source_draft_candidate_not_price_evidence",
                            "ownModel": [
                                {
                                    "brand": "KIA",
                                    "model": "Sportage HEV",
                                    "draftStatus": "source_draft_available",
                                    "candidateSourceType": "source_draft",
                                    "materializationStatus": "ready_for_extraction",
                                    "sourceDraftPath": "se/05_kia_sportage_se.yaml",
                                }
                            ],
                            "competitorCorridor": [],
                            "candidateCount": 1,
                        },
                    },
                },
            ],
            missingEvidence=[
                {"name": "current_msrp", "reason": "Sportage accepted current MSRP is not materialized.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"], "competitors": ["Kia Sportage HEV"]}},
    )

    direct = answer["direct"]

    assert "用户材料价格假设 J7 HEV 34,720 EUR / 30,000-40,000 EUR / PVA 118%" in direct
    assert "不是当前官方 MSRP" in direct
    assert "补源状态：Sportage HEV 只有搜索候选/来源草稿" in direct
    assert "需要审核来源、版本、币种、发布日期并 ingest 成 current price 后才能作为官方价格证据" in direct
    assert "Sportage HEV source_draft_available" not in direct
    assert "Sportage HEV ready_for_extraction" not in direct
    assert "价格证据：已查 Sportage HEV" not in direct


def test_partial_pricing_evidence_adds_limited_progress_bullet() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Pricing", "direct": "O9 target price can be considered.", "bullets": [], "limitations": []},
        _pricing_package(
            toolResults=[
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "Price corridor sample exists.",
                    "keyFindings": ["priceStats.min: 39121", "priceStats.max: 53165"],
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121, "unit": "currency"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165, "unit": "currency"},
                    ],
                },
                {
                    "toolName": "user_supplied_target_price",
                    "success": True,
                    "sourceType": "generated",
                    "summary": "User supplied target price.",
                    "keyFindings": ["target_price_range:53000-55000 EUR"],
                    "evidenceRefs": [
                        {"refId": "ev_target_min", "label": "User supplied own-model target price min", "value": 53000, "unit": "EUR"},
                        {"refId": "ev_target_max", "label": "User supplied own-model target price max", "value": 55000, "unit": "EUR"},
                    ],
                },
            ],
            missingEvidence=[
                {"name": "current_msrp", "reason": "Official current MSRP not materialized.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O9"]}},
    )

    limited_bullet = next(item for item in answer["bullets"] if item.startswith("证据有限但可推进"))
    assert "目标价/价格走廊" in limited_bullet
    assert "官方 MSRP 交叉验证" in limited_bullet
    assert "最终价格锚点" in limited_bullet
    assert "月供/RV/company car" in limited_bullet


def test_grounding_guard_adds_business_composer_fields() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Pricing", "direct": "J7 HEV can sit in the core corridor.", "bullets": [], "limitations": []},
        _pricing_package_with_method_material(),
    )

    assert answer["businessSynthesisPlan"]["intent"] == "pricing_analysis"
    assert "MSRP" in answer["keyTakeaways"][0]
    assert "Competitor corridor" in answer["keyTakeaways"][1]
    assert "Leasing/RV/company car" in answer["keyTakeaways"][2]
    assert answer["pmInsight"]
    assert "定价分析" in answer["summary"] or "定价" in answer["summary"]
    assert answer["recommendedActions"]
    assert len(answer["reportReadyBullets"]) >= 3
    assert answer["grounding"]["businessSynthesisStatus"] == "aligned"
    assert set(answer["businessFrame"]) == {"verdict", "why", "soWhat", "action", "risk"}
    joined_bullets = " ".join(answer["bullets"])
    assert answer["direct"]
    for label in ("证据：", "产品经理判断：", "下一步动作：", "风险边界："):
        assert label in joined_bullets
    assert not any(
        item.startswith("结论：") and answer["direct"] in item
        for item in answer["bullets"]
    )
    assert "结论：直接结论" not in joined_bullets
    assert len(answer["bullets"]) <= 6
    assert "Verdict：" not in joined_bullets
    assert "P0 动作：" not in joined_bullets


def test_j7_pricing_question_labels_material_as_positioning_hypothesis() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "J7 HEV can sit in the core corridor.", "bullets": [], "limitations": []},
        _pricing_package_with_method_material(),
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"]}},
    )

    direct = answer["direct"]
    assert answer["title"] == "瑞典 · J7 HEV 验证版定价逻辑"
    assert "核心竞争带中段 + 高配主推" not in answer["title"]
    assert direct.startswith("验证版定价立场：瑞典 J7 HEV 可以先按")
    assert "低配做价格锚点，高配做主推版本" in direct
    assert "这是一版可推进的定价假设，不是最终官方 MSRP" in direct
    assert "关键证据" in direct
    assert "用户材料假设边界" in direct
    assert "不是当前官方 MSRP" in direct
    assert "来自材料/方法样例" in direct
    assert "不能把材料价格走廊当最终价" in direct
    assert "数据边界" in direct
    assert "补数动作" in direct
    assert "30,000-40,000 EUR" in direct
    assert direct.count("30,000-40,000 EUR") == 1
    assert "Title：" not in direct
    assert "Verdict：" not in direct
    joined_bullets = " ".join(answer["bullets"])
    actions = [item["action"] for item in answer["recommendedActions"]]
    assert len(answer["bullets"]) <= 6
    assert "Verdict：" not in joined_bullets
    assert "P0 动作：" not in joined_bullets
    assert "结论：" in joined_bullets
    assert "证据链：" in joined_bullets
    assert "产品经理判断：" in joined_bullets
    assert "风险边界：" in joined_bullets
    assert "下一步执行" not in answer["bullets"][0]
    assert "补齐本车型与竞品 MSRP / TP / 月供价格矩阵" in actions
    assert "补齐竞品 MSRP" not in actions
    visible_text = " ".join([
        *answer["keyTakeaways"],
        *answer["reportReadyBullets"],
    ])
    assert "高低配价差 高低配价差" not in visible_text
    assert "PVA 覆盖 高配 PVA" not in visible_text
    assert "高低配价差 3,230 EUR" in visible_text


def test_j7_method_pricing_stance_wins_over_generated_competitor_compare() -> None:
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "J7 HEV can sit in the core corridor.", "bullets": [], "limitations": []},
        _pricing_package_with_method_material(
            entities={
                "models": ["J7 HEV"],
                "competitors": ["Corolla Cross"],
            },
        ),
        country="Sweden",
        question="基于瑞典市场、竞品格局和配置差异，J7 HEV 应该怎么定价？请给出定价结论、关键数据支撑、价格走廊，并生成竞品表或图表。",
        evidence_plan={
            "intent": "pricing_analysis",
            "entities": {
                "models": ["J7 HEV"],
                "competitors": ["Corolla Cross"],
            },
        },
    )

    direct = answer["direct"]
    assert direct.startswith("验证版定价立场：瑞典 J7 HEV 可以先按")
    assert "核心竞争带中段 + 高配主推" in direct
    assert "竞品池先按 Corolla Cross, RAV4, C-HR, Qashqai 交叉验证" in direct
    assert "配置价值假设" in direct
    assert "7年/15万公里质保" in direct
    assert "相对定价判断：J7 HEV 与 Corolla Cross" not in direct


def test_j7_pricing_direct_summarizes_msrp_source_candidate_status_without_claiming_price() -> None:
    package = _pricing_package_with_method_material(
        toolResults=[
            *_pricing_package_with_method_material()["toolResults"],
            {
                "toolName": "query_msrp_pricing",
                "success": True,
                "rowCount": 0,
                "sourceType": "postgres",
                "summary": "No current MSRP rows for requested J7 HEV pricing set.",
                "keyFindings": ["coverage_gap:no_current_prices_for_requested_models"],
                "evidenceRefs": [],
                "coverageDiagnostics": {
                    "diagnosis": "no_current_prices_for_requested_models",
                    "sourceRepairCandidates": {
                        "dataStatus": "own_model_current_price_source_candidates",
                        "ownModel": [
                            {
                                "model": "J7 HEV",
                                "draftStatus": "candidate_search_query",
                                "candidateSourceType": "generic_official_price_search",
                            },
                            {
                                "brand": "TOYOTA",
                                "model": "Corolla Cross",
                                "draftStatus": "source_draft_available",
                                "candidateSourceType": "source_draft",
                                "currentPriceRows": 0,
                            },
                            {
                                "brand": "TOYOTA",
                                "model": "RAV4",
                                "draftStatus": "source_draft_available",
                                "candidateSourceType": "source_draft",
                                "currentPriceRows": 0,
                            },
                            {
                                "brand": "SKODA",
                                "model": "ENYAQ",
                                "draftStatus": "current_price_materialized",
                                "candidateSourceType": "source_draft",
                                "currentPriceRows": 1,
                            },
                        ],
                        "competitorCorridor": [],
                        "candidateCount": 3,
                        "materializedCandidateCount": 0,
                    },
                },
            },
        ],
        missingEvidence=[
            {"name": "current_msrp", "reason": "No current MSRP rows.", "impact": "weakens_answer"},
        ],
    )
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "J7 HEV can sit in the core corridor.", "bullets": [], "limitations": []},
        package,
        country="Sweden",
        question="基于瑞典市场、竞品格局和配置差异，J7 HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"]}},
    )

    direct = answer["direct"]
    assert "价格来源状态" in direct
    assert "来源草稿待抽取前审核：Corolla Cross, RAV4" in direct
    assert "官方价格搜索候选：J7 HEV" in direct
    assert "当前还没有可直接引用为 MSRP 的价格记录" in direct
    assert "ENYAQ" not in direct
    assert "J7 HEV 当前 MSRP = 34,720 EUR" not in direct


def test_j7_pricing_digest_prioritizes_material_over_background_price_stats() -> None:
    package = _pricing_package(
        toolResults=[
            {
                "toolName": "query_msrp_pricing",
                "success": True,
                "rowCount": 0,
                "sourceType": "postgres",
                "summary": "No current MSRP rows for requested J7 HEV competitors.",
                "keyFindings": ["coverage_gap:no_current_prices_for_requested_models"],
                "evidenceRefs": [
                    {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_price_positioning"},
                    {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_price_positioning"},
                    {"refId": "ev_avg", "label": "priceStats.avg", "value": 48467.39, "unit": "currency", "source": "jato_price_positioning"},
                    {"refId": "ev_median", "label": "priceStats.median", "value": 52130.43, "unit": "currency", "source": "jato_price_positioning"},
                ],
            },
            {
                "toolName": "business_method_material",
                "success": True,
                "rowCount": 5,
                "sourceType": "generated",
                "summary": "J7 HEV user material price method.",
                "keyFindings": ["Main trim 34720 EUR", "PVA coverage 118%"],
                "evidenceRefs": [
                    {"refId": "ev_j7_price", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_gap", "label": "J7 HEV user material price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_market", "label": "J7 HEV user material market window", "value": "2025.04–2026.03 HEV 总规模约 22,816 台。", "source": "J7_HEV_V4.pptx"},
                ],
            },
            {
                "toolName": "build_market_chart",
                "success": True,
                "rowCount": 1,
                "sourceType": "generated",
                "summary": "Sweden powertrain mix.",
                "keyFindings": ["HEV sales 5051", "BEV sales 25235", "PHEV sales 15028"],
                "evidenceRefs": [
                    {"refId": "ev_drive_hev", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 1946, "unit": "units", "source": "jato_country_chart_deck"},
                    {"refId": "ev_drive_hev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 85.9, "unit": "%", "source": "jato_country_chart_deck"},
                    {"refId": "ev_market_hev", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units", "source": "jato_country_chart_deck"},
                    {"refId": "ev_market_bev", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                    {"refId": "ev_market_phev", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                ],
            },
        ],
        missingEvidence=[
            {"name": "current_msrp", "reason": "Requested official MSRP is not materialized.", "impact": "weakens_answer"},
        ],
        confidence="medium",
    )
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "J7 HEV can sit in the core corridor.", "bullets": [], "limitations": []},
        package,
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？请给出竞品价格走廊、数据支撑和图表。",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"]}},
    )

    digest = answer["evidenceDigest"]
    direct = answer["direct"]
    assert digest[0] == "本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证"
    assert "J7 HEV 主销高配价格 = 34,720 EUR" in digest[1]
    assert "J7 HEV 竞品价格带 = 30,000-40,000 EUR" in digest[2]
    assert any("J7 HEV 高低配价差 = 3,230 EUR" in item for item in digest)
    assert "背景价格样本" not in " ".join(digest[:4])
    assert "背景价格样本" not in direct
    assert direct.startswith("验证版定价立场：瑞典 J7 HEV 可以先按")
    assert "低配做价格锚点，高配做主推版本" in direct
    assert "配置价值假设" in direct
    assert "7年/15万公里质保" in direct
    assert "540°全景影像" in direct
    assert "用户材料假设边界" in direct
    assert "34,720 EUR" in direct
    assert "不能把材料价格走廊当最终价" in direct
    assert "JATO 工具数据补充" not in direct
    assert "JATO 图表口径" not in direct
    assert direct.count("本轮 JATO 市场信号显示") == 1
    assert "HEV 总量信号 5,051 units" in direct
    assert "HEV 结构拆分 2WD 85.9%" in direct
    assert "BEV 25,235 units" not in direct
    assert "PHEV 15,028 units" not in direct
    assert "HEV 动力销量 = 1,946 units" not in direct
    assert "用户材料里的 HEV 22,816 台属于材料周期口径" in direct
    assert "JATO 数据周期统一后" in direct


def test_j7_pricing_direct_uses_direct_material_refs_without_missing_citable_evidence() -> None:
    package = _pricing_package(
        toolResults=[
            {
                "toolName": "business_method_material",
                "success": True,
                "rowCount": 5,
                "sourceType": "generated",
                "summary": "J7 HEV pricing method from user material.",
                "keyFindings": ["Main trim 34720 EUR", "PVA coverage 118%"],
                "evidenceRefs": [
                    {"refId": "ev_j7_price", "label": "J7 HEV main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_corridor", "label": "J7 HEV competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_gap", "label": "J7 HEV high-low trim price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_pva", "label": "J7 HEV PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_pool", "label": "J7 HEV competitor pool", "value": "Corolla Cross, RAV4, C-HR, Qashqai", "source": "J7_HEV_V4.pptx"},
                ],
            }
        ],
        missingEvidence=[],
        confidence="medium",
    )
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "J7 HEV can sit in the core corridor.", "bullets": [], "limitations": []},
        package,
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"]}},
    )

    direct = answer["direct"]
    digest = " ".join(answer["evidenceDigest"])
    assert "J7 HEV 主销高配价格 = 34,720 EUR" in digest
    assert "J7 HEV 竞品价格带 = 30,000-40,000 EUR" in digest
    assert "J7 HEV 高低配价差 = 3,230 EUR" in digest
    assert "J7 HEV 高配 PVA 覆盖率 = 118%" in digest
    assert "当前缺少可直接引用的官方 MSRP / 竞品价格记录" not in direct
    assert "缺少可引用证据" not in direct
    assert direct.startswith("验证版定价立场：瑞典 J7 HEV 可以先按")
    assert "高低配价差 3,230 EUR、PVA 覆盖 118% 支撑高配价值假设" in direct
    assert "竞品池先按 Corolla Cross, RAV4, C-HR, Qashqai 交叉验证" in direct
    assert "配置价值假设" in direct
    assert "7年/15万公里质保" in direct
    assert "540°全景影像" in direct
    assert "关键证据：当前没有拿到 J7 HEV 或核心竞品的官方当前 MSRP" in direct
    assert "用户材料假设边界" in direct
    assert "来自材料/方法样例" in direct
    assert "不能把材料价格走廊当最终价" in direct
    assert "34,720 EUR" in direct
    assert "30,000-40,000 EUR" in direct


def test_hungary_j7_pricing_does_not_use_sweden_j7_material_template() -> None:
    package = _pricing_package(
        country="Hungary",
        entities={"models": ["J7 HEV"]},
        toolResults=[
            {
                "toolName": "business_method_material",
                "success": True,
                "rowCount": 5,
                "sourceType": "generated",
                "summary": "J7 HEV Sweden user material price method.",
                "keyFindings": ["Main trim 34720 EUR", "PVA coverage 118%"],
                "evidenceRefs": [
                    {"refId": "ev_j7_price", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_gap", "label": "J7 HEV user material price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_market", "label": "J7 HEV user material market window", "value": "瑞典 2025.04–2026.03 HEV 总规模约 22,816 台。", "source": "J7_HEV_V4.pptx"},
                ],
            }
        ],
        missingEvidence=[
            {"name": "current_msrp", "reason": "Hungary J7 current MSRP is not available.", "impact": "weakens_answer"},
            {"name": "competitor_price_range", "reason": "Hungary competitor price corridor is not available.", "impact": "weakens_answer"},
        ],
        confidence="low",
    )
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "Evidence is limited.", "bullets": [], "limitations": [], "answerStatus": "partially_answered"},
        package,
        country="Hungary",
        question="匈牙利 J7 HEV 应该怎么定价？请不要回答瑞典。",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"]}},
    )

    visible_text = " ".join([
        answer["direct"],
        *answer["evidenceDigest"],
        *answer["reportReadyBullets"],
        *answer["businessImplications"],
    ])
    assert "匈牙利" in answer["direct"]
    assert answer["direct"].startswith("直接结论：匈牙利 J7 HEV 定价现在不能给确定数字")
    assert "价格走廊、竞品池、配置价值和购买场景" in answer["direct"]
    assert "瑞典" not in visible_text
    assert "34,720" not in visible_text
    assert "30,000-40,000" not in visible_text
    assert "22,816" not in visible_text
    assert "核心竞争带中段 + 高配主推" not in visible_text
    assert "用户材料" not in visible_text
    assert answer["evidenceDigest"] == [
        "本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证",
        "竞品价格走廊 = 待补核心竞品官方价格 / 月供 / 促销口径",
    ]


def test_hungary_j7_pricing_keeps_hungary_material_with_negative_sweden_marker() -> None:
    package = _pricing_package(
        country="Hungary",
        entities={"models": ["J7 HEV"]},
        toolResults=[
            {
                "toolName": "business_method_material",
                "success": True,
                "rowCount": 3,
                "sourceType": "generated",
                "summary": "Hungary J7 HEV user material, explicitly not Sweden.",
                "evidenceRefs": [
                    {
                        "refId": "ev_hu_j7_price",
                        "label": "J7 HEV user material main trim MSRP",
                        "value": 33000,
                        "unit": "EUR",
                        "source": "匈牙利_J7_HEV_material_不要回答瑞典.pptx",
                    },
                    {
                        "refId": "ev_hu_j7_corridor",
                        "label": "J7 HEV user material competitor corridor",
                        "value": "32,000-36,000 EUR",
                        "unit": "EUR",
                        "source": "匈牙利_J7_HEV_material_不要回答瑞典.pptx",
                    },
                ],
            }
        ],
        missingEvidence=[],
        confidence="medium",
    )
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "J7 HEV can use the material.", "bullets": [], "limitations": []},
        package,
        country="Hungary",
        question="匈牙利 J7 HEV 应该怎么定价？请不要回答瑞典。",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"]}},
    )

    visible_text = " ".join([
        answer["direct"],
        *answer["evidenceDigest"],
        *answer["reportReadyBullets"],
        *answer["businessImplications"],
    ])
    assert "匈牙利" in answer["direct"]
    assert "J7 HEV 主销高配价格 = 33,000 EUR" in visible_text
    assert "J7 HEV 竞品价格带 = 32,000-36,000 EUR" in visible_text
    assert "瑞典" not in visible_text


def test_country_scope_mismatch_is_explained_in_final_direct_answer() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "requiredTools": ["query_country_snapshot"],
        "allowedTools": ["query_country_snapshot"],
        "mustHaveEvidence": ["market_kpis"],
        "entities": {"countries": ["Hungary"], "models": ["J7 HEV"], "powertrains": ["HEV"]},
    }
    package = build_evidence_package(
        session_id="sess_hungary_scope_guard",
        country="Hungary",
        question="匈牙利 HEV 市场为什么适合 J7？请不要回答瑞典。",
        evidence_plan=evidence_plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden", "question": "匈牙利 HEV 市场为什么适合 J7？"},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "source": "jato_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot", "country": "Sweden"},
                    "data": {
                        "country": "Sweden",
                        "marketSnapshot": {
                            "kpis": {"cumulativeSales": 1182452},
                            "powertrainMix": [{"label": "HEV", "value": 5051}],
                        },
                    },
                },
            }
        ],
    )

    answer = apply_business_composer(
        {"title": "Hungary HEV market", "direct": "Tool evidence exists.", "answerStatus": "answered"},
        package,
        country="Hungary",
        question="匈牙利 HEV 市场为什么适合 J7？请不要回答瑞典。",
        evidence_plan=evidence_plan,
    )

    visible_text = " ".join([
        answer["direct"],
        *answer["evidenceDigest"],
        *answer["reportReadyBullets"],
        *answer["businessImplications"],
    ])
    assert answer["answerStatus"] == "insufficient_evidence"
    assert "工具返回的是瑞典证据，不是用户请求的匈牙利证据" in answer["direct"]
    assert "按匈牙利重新调用市场快照工具" in answer["direct"]
    assert "目标市场 evidenceRef" in answer["direct"]
    assert "1,182,452" not in visible_text
    assert "1182452" not in visible_text
    assert "HEV 5,051" not in visible_text
    assert "瑞典 HEV" not in visible_text
    action_bullets = [
        item
        for item in answer["bullets"]
        if item.startswith("建议动作：") or item.startswith("下一步动作：")
    ]
    assert len(action_bullets) == 1


def test_pricing_evidence_digest_prioritizes_user_material_market_window_generically() -> None:
    package = _pricing_package()
    package["toolResults"].append(
        {
            "toolName": "business_method_material",
            "success": True,
            "rowCount": 7,
            "sourceType": "generated",
            "summary": "O9 pricing method distilled from user material.",
            "keyFindings": ["瑞典 O9 市场窗口和价格定位。"],
            "evidenceRefs": [
                {
                    "refId": "ev_market_window",
                    "label": "O9 user material market window",
                    "value": "瑞典高端 SUV 机会应先看 BEV/PHEV 公司车需求、SUV A/B 需求集中度和核心竞品换代窗口。",
                    "source": "O9_user_material.pptx",
                },
                {
                    "refId": "ev_j7_msrp",
                    "label": "O9 user material main trim MSRP",
                    "value": "54000",
                    "unit": "EUR",
                    "source": "O9_user_material.pptx",
                },
                {
                    "refId": "ev_j7_corridor",
                    "label": "O9 user material competitor corridor",
                    "value": "53,000-55,000 EUR",
                    "unit": "EUR",
                    "source": "O9_user_material.pptx",
                },
                {
                    "refId": "ev_j7_gap",
                    "label": "O9 user material price gap",
                    "value": "2000",
                    "unit": "EUR",
                    "source": "O9_user_material.pptx",
                },
                {
                    "refId": "ev_j7_pva",
                    "label": "O9 user material PVA coverage",
                    "value": "105",
                    "unit": "%",
                    "source": "O9_user_material.pptx",
                },
            ],
        }
    )
    answer = apply_business_composer(
        {"title": "Pricing", "direct": "J7 HEV can sit in the core corridor.", "bullets": [], "limitations": []},
        package,
        country="Sweden",
        question="瑞典 O9 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O9"]}},
    )

    digest = answer["evidenceDigest"]
    assert digest[0].startswith("O9 市场窗口 = 瑞典高端 SUV 机会应先看 BEV/PHEV 公司车需求")
    assert "O9 主销高配价格 = 54,000 EUR" in digest[1]
    assert any("O9 竞品价格带 = 53,000-55,000 EUR" in line for line in digest)


def test_grounding_guard_turns_insufficient_evidence_into_useful_next_steps() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Pricing", "direct": "The price should be 34,720 EUR.", "bullets": [], "limitations": []},
        _pricing_package(
            toolResults=[],
            missingEvidence=[
                {"name": "missing_required_tool:query_msrp_pricing", "reason": "Pricing requires MSRP.", "impact": "blocking"},
                {"name": "competitor_price_range", "reason": "No corridor evidence.", "impact": "blocking"},
            ],
            confidence="low",
        ),
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis"},
    )

    assert answer["answerStatus"] == "insufficient_evidence"
    assert "缺少MSRP/当前价格工具结果" in answer["direct"]
    assert "missing_required_tool" not in answer["direct"]
    assert "evidenceRef" not in answer["direct"]
    assert "置信度低" in answer["direct"]
    assert any(item.startswith("当前能判断：") for item in answer["bullets"])
    assert any(item.startswith("下一步动作：") for item in answer["bullets"])
    assert any(item.startswith("建议查数动作：") for item in answer["bullets"])
    assert any(item.startswith("建议输出形态：") for item in answer["bullets"])
    assert not any(item.startswith("建议调用工具：") for item in answer["bullets"])
    assert answer["recommendedActions"][0]["priority"] == "P0"
    assert "补齐本车型当前 MSRP/价格证据" in answer["recommendedActions"][0]["action"]
    assert "Pricing evidence table" in answer["recommendedActions"][0]["action"]
    assert "query_msrp_pricing" not in answer["recommendedActions"][0]["action"]
    assert "MSRP/当前价格" in answer["recommendedActions"][0]["rationale"]
    assert any("补齐本车型当前 MSRP/价格证据，并生成 Pricing evidence table" in item for item in answer["reportReadyBullets"])
    assert not any("补跑 query_msrp_pricing" in item for item in answer["reportReadyBullets"])


def test_business_composer_replaces_generic_summary_and_pm_insight() -> None:
    answer = apply_answer_grounding_guard(
        {
            "title": "Market",
            "direct": "Grounded answer.",
            "summary": "Analysis complete.",
            "pmInsight": "Based on the available evidence.",
            "bullets": [],
            "limitations": [],
        },
        _pricing_package(intent="market_overview"),
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={"intent": "market_overview"},
    )

    assert "市场总览" in answer["summary"] or "市场" in answer["summary"]
    assert answer["pmInsight"] != "Based on the available evidence."
    assert any(label in " ".join(answer["bullets"]) for label in ("结论：", "产品经理判断：", "下一步动作："))


def test_market_overview_direct_keeps_display_backbone_out_of_executive_answer() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(),
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={"intent": "market_overview"},
    )

    direct = answer["direct"]
    assert "展示骨架" not in direct
    assert "Key metrics" not in direct
    assert "Market decision table" not in direct
    assert "Key metrics" in answer["displayPlan"]
    assert "Market decision table" in answer["displayPlan"]
    assert "机会 segment" in answer["displayPlan"]
    assert "下一步执行" in direct
    assert "HEV 份额 7.3%" in direct
    assert "SUV A0/A 集中度 mainstream" in direct
    assert "J7 HEV 的机会入口已有市场结构证据支撑" in direct
    assert "不是最终上市或定价结论" in direct
    assert "车型级价格/配置矩阵" in direct


def test_market_overview_direct_uses_powertrain_refs_for_bev_hev_space_pressure() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            toolResults=[
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "rowCount": 8,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden fuel/channel snapshot.",
                    "keyFindings": ["BEV has a larger pool than HEV.", "HEV remains a practical low-risk role."],
                    "evidenceRefs": [
                        {"refId": "ev_bev_sales", "label": "BEV.sales", "value": 10875, "unit": "units"},
                        {"refId": "ev_bev_share", "label": "BEV share", "value": "40.9%", "unit": "%"},
                        {
                            "refId": "ev_bev_business",
                            "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Business_pct",
                            "value": 60.3,
                            "unit": "%",
                        },
                        {
                            "refId": "ev_bev_private",
                            "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Private_pct",
                            "value": 39.7,
                            "unit": "%",
                        },
                        {"refId": "ev_hev_sales", "label": "HEV.sales", "value": 1946, "unit": "units"},
                        {"refId": "ev_hev_share", "label": "HEV share", "value": "7.3%", "unit": "%"},
                        {
                            "refId": "ev_phev_sales",
                            "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.sales",
                            "value": 6498,
                            "unit": "units",
                        },
                    ],
                }
            ],
            confidence="high",
        ),
        country="Sweden",
        question="北欧 BEV 增长是否会压缩 HEV 空间？",
        evidence_plan={"intent": "market_overview", "entities": {"powertrains": ["BEV", "HEV"]}},
    )

    direct = answer["direct"]
    assert "BEV 增长会压缩 HEV 空间" in direct
    assert "市场证据：BEV 10,875 units，份额 40.9%，Business 60.3%，Private 39.7%" in direct
    assert "HEV 1,946 units，份额 7.3%" in direct
    assert "PHEV 6,498 units" in direct
    assert "BEV 的规模明显高于 HEV" in direct
    assert "HEV 定义到无稳定充电、价格敏感、低使用风险和 SUV A0/A 实用场景" in direct
    assert "Market decision table" not in direct
    assert "Powertrain mix chart" in answer["displayPlan"]
    assert "Market decision table" in answer["displayPlan"]
    assert "需要先给业务立场" not in direct


def test_market_overview_direct_uses_nordic_cross_country_powertrain_refs() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            toolResults=[
                {
                    "toolName": "query_cross_country",
                    "success": True,
                    "rowCount": 4,
                    "sourceType": "jato_parquet",
                    "summary": "Nordic BEV and HEV cross-country comparison.",
                    "keyFindings": ["Nordic countries have BEV and HEV refs."],
                    "evidenceRefs": [
                        {"refId": "se_bev", "label": "crossCountry.Sweden.powertrainMix.BEV.sales", "value": 25235, "unit": "units"},
                        {"refId": "fi_bev", "label": "crossCountry.Finland.powertrainMix.BEV.sales", "value": 2800, "unit": "units"},
                        {"refId": "no_bev", "label": "crossCountry.Norway.powertrainMix.BEV.sales", "value": 42000, "unit": "units"},
                        {"refId": "dk_bev", "label": "crossCountry.Denmark.powertrainMix.BEV.sales", "value": 18000, "unit": "units"},
                        {"refId": "se_hev", "label": "crossCountry.Sweden.powertrainMix.HEV.sales", "value": 5051, "unit": "units"},
                        {"refId": "fi_hev", "label": "crossCountry.Finland.powertrainMix.HEV.sales", "value": 2687, "unit": "units"},
                        {"refId": "no_hev", "label": "crossCountry.Norway.powertrainMix.HEV.sales", "value": 1400, "unit": "units"},
                        {"refId": "dk_hev", "label": "crossCountry.Denmark.powertrainMix.HEV.sales", "value": 2500, "unit": "units"},
                    ],
                }
            ],
            missingEvidence=[],
            entities={"countries": ["Sweden", "Finland", "Norway", "Denmark"]},
            confidence="high",
        ),
        country="Sweden",
        question="北欧 BEV 增长是否会压缩 HEV 空间？",
        evidence_plan={"intent": "market_overview", "entities": {"countries": ["Sweden", "Finland", "Norway", "Denmark"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("直接结论：北欧 BEV 增长会压缩 HEV 空间")
    assert "区域证据：" in direct
    assert "Sweden BEV 25,235 units" in direct
    assert "Finland BEV 2,800 units" in direct
    assert "Norway BEV 42,000 units" in direct
    assert "Denmark BEV 18,000 units" in direct
    assert "BEV 对 HEV 的空间压缩是区域性压力" in direct
    assert "直接结论：瑞典 BEV" not in direct


def test_market_overview_direct_turns_cross_tabs_into_hev_opportunity_stance() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 8,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary chart snapshot with usable cross-tabs.",
                    "keyFindings": ["HEV and SUV A0/A cross-tabs are available."],
                    "evidenceRefs": [
                        {
                            "refId": "ev_hu_hev_sales",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales",
                            "value": 2687,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_hu_hev_2wd",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct",
                            "value": 89.5,
                            "unit": "%",
                        },
                        {
                            "refId": "ev_hu_hev_4wd",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.4WD_pct",
                            "value": 9.9,
                            "unit": "%",
                        },
                        {
                            "refId": "ev_hu_suv_a0",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales",
                            "value": 7303,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_hu_suv_a",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                            "value": 3535,
                            "unit": "units",
                        },
                    ],
                }
            ],
            missingEvidence=[
                {
                    "name": "jato_cross_check",
                    "reason": "Lightweight country snapshot was empty before chart cross-tabs were attached.",
                    "impact": "weakens_answer",
                }
            ],
            confidence="medium",
            jatoCrossCheck={"status": "not_available", "summary": "No lightweight JATO context before chart guard."},
        ),
        country="Hungary",
        question="匈牙利 HEV 市场机会？请用内部 JATO 数据给直接结论，并展示市场结构表。",
        evidence_plan={"intent": "market_overview", "entities": {"powertrains": ["HEV"]}},
    )

    direct = answer["direct"]
    assert "匈牙利市场 HEV 产品线机会入口已有市场结构证据支撑" in direct
    assert "目标 HEV 产品" not in direct
    assert "HEV + SUV A0/A 可作为优先验证入口" in direct
    assert "2,687 units" in direct
    assert "SUV A0 7,303 units" in direct
    assert "SUV A 3,535 units" in direct
    assert "两驱占 89.5%" in direct
    assert "HEV + SUV A0/A + 主销驱动形式" in direct
    assert "Market structure chart" not in direct
    assert "Market decision table" not in direct
    assert "Market structure chart" in answer["displayPlan"]
    assert "Market decision table" in answer["displayPlan"]
    assert "需要先给业务立场" not in direct
    assert "缺少JATO 内部交叉验证" not in direct
    assert "缺少内部市场快照" not in direct
    key_takeaways = " ".join(answer["keyTakeaways"])
    assert "contextSnapshot" not in key_takeaways
    assert "HEV 动力销量 2,687 units" in key_takeaways
    assert "SUV A0 细分销量 7,303 units" in key_takeaways
    assert "SUV A 细分销量 3,535 units" in key_takeaways


def test_market_opportunity_cross_tabs_support_blind_bev_model() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Use the retrieved market structure.", "bullets": [], "limitations": []},
        _market_package(
            country="Hungary",
            entities={"models": ["Lumen X"], "powertrains": ["BEV"]},
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 5,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary BEV and SUV cross-tabs.",
                    "evidenceRefs": [
                        {"refId": "blind_bev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.BEV.sales", "value": 4200, "unit": "units"},
                        {"refId": "blind_bev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.BEV.2WD_pct", "value": 71.0, "unit": "%"},
                        {"refId": "blind_bev_4wd", "label": "contextSnapshot.crossTabs.driveByFuel.BEV.4WD_pct", "value": 29.0, "unit": "%"},
                        {"refId": "blind_suv_a0", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 6100, "unit": "units"},
                        {"refId": "blind_suv_a", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 4800, "unit": "units"},
                    ],
                }
            ],
            missingEvidence=[
                {"name": "model_level_market_opportunity_evidence", "reason": "Need model-level evidence.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Hungary",
        question="匈牙利 Lumen X BEV 是否值得进入？",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"models": ["Lumen X"], "powertrains": ["BEV"]},
        },
    )

    direct = answer["direct"]
    assert "匈牙利市场 Lumen X BEV 的机会入口已有市场结构证据支撑" in direct
    assert "BEV + SUV A0/A 可作为优先验证入口" in direct
    assert "BEV cross-tab" not in direct
    assert "BEV 规模 4,200 units" in direct
    assert "BEV 内部两驱占 71%" in direct
    assert "SUV A0 6,100 units" in direct
    assert "SUV A 4,800 units" in direct
    assert "建立 匈牙利市场 Lumen X BEV 在 SUV A0/A 的车型级价格/配置矩阵、竞品池和市场结构表" in direct
    assert all(name not in direct for name in ("J7", "O5", "EV3", "Sportage"))


def test_market_opportunity_gap_supports_blind_bev_model_without_product_template() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "Insufficient evidence.", "answerStatus": "insufficient_evidence"},
        evidence_package=_market_package(
            country="Hungary",
            entities={"models": ["Lumen X"], "powertrains": ["BEV"]},
            toolResults=[],
            missingEvidence=[
                {"name": "market_snapshot_data_unavailable", "reason": "No internal BEV rows.", "impact": "weakens_answer"},
                {"name": "jato_cross_check", "reason": "No internal cross-check.", "impact": "weakens_answer"},
            ],
            confidence="low",
            jatoCrossCheck={"status": "missing", "summary": "Internal cross-check unavailable."},
        ),
        country="Hungary",
        question="匈牙利 Lumen X BEV 是否值得进入？",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"models": ["Lumen X"], "powertrains": ["BEV"]},
        },
    )

    direct = plan["executiveConclusion"]
    assert "暂不能把匈牙利市场判定为 Lumen X BEV 的已验证进入机会" in direct
    assert "不能证明 BEV 需求、SUV A0/A 结构和 Lumen X BEV 竞品定位已经成立" in direct
    assert "Lumen X BEV 的价格、配置和渠道价值能够闭环" in direct
    assert "补齐匈牙利市场 Lumen X BEV 的 BEV SUV A0/A、核心竞品池和价格/配置证据" in direct
    assert all(name not in direct for name in ("J7", "O5", "EV3", "Sportage"))


def test_generic_market_opportunity_core_has_no_model_specific_branch_literals() -> None:
    core_functions = (
        composer._market_fit_target_label,
        composer._market_model_from_question,
        composer._is_market_fit_question,
        composer._market_fit_gap_conclusion,
        composer._market_fit_gap_action,
        composer._market_fit_next_action,
        composer._is_market_opportunity_question,
        composer._market_opportunity_powertrain,
        composer._market_powertrain_opportunity_cross_tab_conclusion,
        composer._market_powertrain_opportunity_subject,
        composer._market_powertrain_pressure_clause,
    )
    source = "\n".join(inspect.getsource(function) for function in core_functions).casefold()

    for model_literal in ("j7", "o5", "ev3", "sportage", "lumen x"):
        assert model_literal not in source


def test_market_overview_named_model_gap_becomes_partial_but_keeps_market_data() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 8,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary chart snapshot with usable HEV cross-tabs.",
                    "keyFindings": ["HEV and SUV A0/A cross-tabs are available."],
                    "evidenceRefs": [
                        {
                            "refId": "ev_hu_hev_sales",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales",
                            "value": 2687,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_hu_hev_2wd",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct",
                            "value": 89.5,
                            "unit": "%",
                        },
                        {
                            "refId": "ev_hu_suv_a0",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales",
                            "value": 7303,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_hu_suv_a",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                            "value": 3535,
                            "unit": "units",
                        },
                    ],
                }
            ],
            missingEvidence=[
                {
                    "name": "model_level_market_opportunity_evidence",
                    "reason": "Need model-level competitor, price and configuration evidence.",
                    "impact": "weakens_answer",
                }
            ],
            confidence="medium",
        ),
        country="Hungary",
        question="匈牙利 J7 HEV 市场机会是什么？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]}},
    )

    direct = answer["direct"]
    assert answer["answerStatus"] == "partially_answered"
    assert answer["confidence"] == "medium"
    assert "匈牙利市场 J7 HEV 的机会入口已有市场结构证据支撑" in direct
    assert "HEV + SUV A0/A 可作为优先验证入口" in direct
    assert "2,687 units" in direct
    assert "SUV A0 7,303 units" in direct
    assert "车型级竞品、价格和配置机会证据" in " ".join(answer["limitations"])
    assert "瑞典" not in direct
    key_takeaways = " ".join(answer["keyTakeaways"])
    assert "Top models：当前工具未返回匈牙利市场 J7 HEV 的车型级销量/价格记录" in key_takeaways
    assert "MSRP、配置、月供/RV 和车型级销量矩阵" in key_takeaways


def test_j7_hev_market_overview_does_not_treat_pva_as_market_share() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            toolResults=[
                {
                    "toolName": "query_segment_breakdown",
                    "success": True,
                    "rowCount": 33,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden HEV segment breakdown.",
                    "keyFindings": ["HEV and SUV A0/A cross-tabs are available."],
                    "evidenceRefs": [
                        {
                            "refId": "ev_se_hev_sales",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales",
                            "value": 1946,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_se_hev_2wd",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct",
                            "value": 85.9,
                            "unit": "%",
                        },
                        {
                            "refId": "ev_se_hev_4wd",
                            "label": "contextSnapshot.crossTabs.driveByFuel.HEV.4WD_pct",
                            "value": 14.1,
                            "unit": "%",
                        },
                        {
                            "refId": "ev_se_suv_a",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                            "value": 7544,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_se_suv_a0",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales",
                            "value": 5416,
                            "unit": "units",
                        },
                    ],
                },
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "generated",
                    "summary": "J7 HEV method material.",
                    "keyFindings": ["PVA coverage is method material, not market share."],
                    "evidenceRefs": [
                        {
                            "refId": "ev_j7_pva",
                            "label": "J7 HEV user material PVA coverage",
                            "value": 118,
                            "unit": "%",
                            "source": "J7_HEV_V4.pptx",
                        }
                    ],
                },
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "rowCount": 3,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden national powertrain totals.",
                    "keyFindings": ["National HEV total is a distinct scope from cross-tab coverage."],
                    "evidenceRefs": [
                        {
                            "refId": "ev_se_hev_total",
                            "label": "contextSnapshot.powertrainMix.HEV.sales",
                            "value": 5051,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_se_bev_total",
                            "label": "contextSnapshot.powertrainMix.BEV.sales",
                            "value": 25235,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_se_phev_total",
                            "label": "contextSnapshot.powertrainMix.PHEV.sales",
                            "value": 15028,
                            "unit": "units",
                        },
                    ],
                },
            ],
        ),
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]}},
    )

    direct = answer["direct"]
    assert "HEV cross-tab 覆盖样本 1,946 units" in direct
    assert "国家动力总量口径的 HEV 为 5,051 units，两者不直接相加或比较" in direct
    assert "SUV A0 5,416 units" in direct
    assert "SUV A 7,544 units" in direct
    assert "两驱占 85.9%" in direct
    assert "份额 118%" not in direct
    assert "瑞典市场 J7 HEV 现在应作为“待验证机会”" not in direct
    assert "验证重点是 HEV 需求" not in direct
    assert answer["businessImplications"][0].startswith("瑞典市场 J7 HEV 应先作为 HEV + SUV A0/A 的低风险进入验证线")
    assert "HEV 1,946 units" in answer["businessImplications"][0]
    assert "HEV 2WD 85.9%" in answer["businessImplications"][0]
    assert "市场机会方法" not in answer["businessImplications"][0]


def test_market_overview_direct_uses_suv_a0_a_structure_cross_tabs() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 8,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden segment cross-tabs.",
                    "keyFindings": ["SUV A0/A cross-tabs are available."],
                    "evidenceRefs": [
                        {
                            "refId": "ev_se_suv_a0_sales",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales",
                            "value": 7303,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_se_suv_a_sales",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                            "value": 3535,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_se_suv_a0_bev",
                            "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A0.BEV_pct",
                            "value": 45.6,
                            "unit": "%",
                        },
                        {
                            "refId": "ev_se_suv_a_bev",
                            "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.BEV_pct",
                            "value": 40.0,
                            "unit": "%",
                        },
                        {
                            "refId": "ev_se_suv_a_4wd",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct",
                            "value": 60.1,
                            "unit": "%",
                        },
                    ],
                }
            ],
            confidence="high",
            jatoCrossCheck={"status": "matched", "summary": "Segment cross-tabs are available."},
        ),
        country="Sweden",
        question="SUV A0/A 级为什么是主销结构？",
        evidence_plan={"intent": "market_overview", "entities": {"segments": ["SUV A0", "SUV A"]}},
    )

    direct = answer["direct"]
    assert "瑞典市场 SUV A0/A 成为主销结构" in direct
    assert "SUV A0 7,303 units" in direct
    assert "SUV A 3,535 units" in direct
    assert "SUV A0 BEV 45.6%" in direct
    assert "SUV A BEV 40%" in direct
    assert "SUV A 4WD 60.1%" in direct
    assert "泛 SUV 热" in direct
    assert "Market decision table" not in direct
    assert "Market structure chart" in answer["displayPlan"]
    assert "Market decision table" in answer["displayPlan"]
    assert "只复述销量" not in direct
    assert "对目标产品组合的动作" in direct
    assert "OMODA" not in direct
    assert "JAECOO" not in direct


def test_market_overview_direct_uses_suv_a0_a_snapshot_concentration() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(),
        country="Sweden",
        question="SUV A0/A 级为什么是主销结构？",
        evidence_plan={"intent": "market_overview", "entities": {"segments": ["SUV A0", "SUV A"]}},
    )

    direct = answer["direct"]
    assert "瑞典市场 SUV A0/A 成为主销结构" in direct
    assert "SUV A0/A 集中度 mainstream" in direct
    assert "主销结构假设和优先验证入口" in direct
    assert "还不能替代 segment cross-tab 的销量、动力结构和驱动形式证据" in direct
    assert "BEV/HEV/PHEV 分别落到 SUV A0/A 的主销价格带" in direct
    assert "Market decision table" not in direct
    assert "泛 SUV 热" in direct
    assert "对目标产品组合的动作" in direct
    assert "OMODA" not in direct
    assert "JAECOO" not in direct


def test_market_overview_includes_scoped_monthly_trend_and_removes_conflicting_limitation() -> None:
    answer = apply_business_composer(
        {
            "title": "Hungary HEV market",
            "direct": "Grounded answer.",
            "bullets": [],
            "limitations": [
                "月度走势数据为整体市场时间序列，未单独标注 HEV 动力类型的月度数据。",
                "SUV 级别仍需补充车型级竞品证据。",
            ],
        },
        _market_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary SUV structure.",
                    "evidenceRefs": [
                        {
                            "refId": "ev_hu_suv_a0",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales",
                            "value": 7303,
                            "unit": "units",
                        },
                        {
                            "refId": "ev_hu_suv_a",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                            "value": 3535,
                            "unit": "units",
                        },
                    ],
                },
                {
                    "toolName": "query_time_series",
                    "query": {"country": "Hungary", "powertrain": "HEV", "granularity": "monthly"},
                    "success": True,
                    "rowCount": 4,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary HEV monthly registration series.",
                    "evidenceRefs": [
                        {"refId": "ev_hu_hev_dec", "label": "monthSeries.2025 Dec.sales", "value": 1991, "unit": "units"},
                        {"refId": "ev_hu_hev_jan", "label": "monthSeries.2026 Jan.sales", "value": 1890, "unit": "units"},
                        {"refId": "ev_hu_hev_feb", "label": "monthSeries.2026 Feb.sales", "value": 1906, "unit": "units"},
                        {"refId": "ev_hu_hev_mar", "label": "monthSeries.2026 Mar.sales", "value": 2687, "unit": "units"},
                    ],
                },
            ],
            confidence="high",
            entities={"countries": ["Hungary"], "powertrains": ["HEV"], "segments": ["SUV A0", "SUV A"]},
        ),
        country="Hungary",
        question="匈牙利 HEV 市场最近的规模、月度走势和主销 SUV 级别是什么？",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"countries": ["Hungary"], "powertrains": ["HEV"], "segments": ["SUV A0", "SUV A"]},
        },
    )

    assert "月度走势方面，HEV 月度注册量从 2025 Dec 1,991 units 到 2026 Mar 2,687 units" in answer["direct"]
    assert "最近三个月为 2026 Jan 1,890 units、2026 Feb 1,906 units、2026 Mar 2,687 units" in answer["direct"]
    assert "月度走势数据为整体市场时间序列" not in answer["limitations"]
    assert "SUV 级别仍需补充车型级竞品证据。" in answer["limitations"]


def test_market_overview_country_pair_blocks_full_verdict_without_finland_refs() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            toolResults=[
                {
                    "toolName": "query_country_snapshot",
                    "query": {"country": "Sweden"},
                    "success": True,
                    "rowCount": 6,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden market snapshot only.",
                    "keyFindings": ["Sweden snapshot is available."],
                    "evidenceRefs": [
                        {"refId": "ev_se_sales", "label": "cumulativeSales", "value": 1182452, "unit": "units"},
                        {"refId": "ev_se_bev", "label": "BEV share", "value": 40.9, "unit": "%"},
                    ],
                }
            ],
            missingEvidence=[
                {
                    "name": "missing_country_snapshot:Finland",
                    "reason": "No Finland market snapshot.",
                    "impact": "blocking",
                }
            ],
            entities={"countries": ["Sweden", "Finland"]},
            confidence="medium",
        ),
        country="Sweden",
        question="瑞典和芬兰销量差异为什么大？",
        evidence_plan={"intent": "market_overview", "entities": {"countries": ["Sweden", "Finland"]}},
    )

    direct = answer["direct"]
    assert "不能把瑞典和芬兰销量差异写成确定结论" in direct
    assert "缺少芬兰的 market snapshot / cross-country 证据" in direct
    assert "query_cross_country" in direct
    assert "瑞典更适合作为 BEV/PHEV/HEV 产品验证主市场" not in direct
    assert "芬兰更适合验证价格敏感" not in direct


def test_market_overview_country_pair_does_not_default_to_sweden_finland_when_scope_is_missing() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            toolResults=[
                {
                    "toolName": "query_country_snapshot",
                    "query": {"country": "Hungary"},
                    "success": True,
                    "rowCount": 6,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary market snapshot only.",
                    "keyFindings": ["Hungary snapshot is available."],
                    "evidenceRefs": [
                        {"refId": "ev_hu_sales", "label": "cumulativeSales", "value": 84210, "unit": "units"},
                        {"refId": "ev_hu_bev", "label": "BEV share", "value": 8.7, "unit": "%"},
                    ],
                }
            ],
            missingEvidence=[],
            entities={},
            confidence="medium",
        ),
        country="Hungary",
        question="瑞典和芬兰销量差异为什么大？",
        evidence_plan={"intent": "market_overview"},
    )

    direct = answer["direct"]
    assert direct.startswith("直接结论：当前不能生成跨国家销量差异结论")
    assert "只有匈牙利侧可引用市场证据" in direct
    assert "不能把瑞典和芬兰销量差异写成确定结论" not in direct
    assert "瑞典侧证据" not in direct
    assert "Sweden/Finland" not in direct


def test_market_overview_country_pair_uses_bilateral_refs_for_verdict() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _market_package(
            toolResults=[
                {
                    "toolName": "query_cross_country",
                    "query": {"countries": "Sweden, Finland"},
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden and Finland cross-country comparison.",
                    "keyFindings": ["Both countries have usable refs."],
                    "evidenceRefs": [
                        {"refId": "ev_se_sales", "label": "crossCountry.Sweden.kpis.cumulativeSales", "value": 1182452, "unit": "units"},
                        {"refId": "ev_se_bev", "label": "crossCountry.Sweden.powertrainMix.BEV.share", "value": 40.9, "unit": "%"},
                        {"refId": "ev_fi_sales", "label": "crossCountry.Finland.kpis.cumulativeSales", "value": 416200, "unit": "units"},
                        {"refId": "ev_fi_bev", "label": "crossCountry.Finland.powertrainMix.BEV.share", "value": 27.2, "unit": "%"},
                    ],
                }
            ],
            missingEvidence=[],
            entities={"countries": ["Sweden", "Finland"]},
            confidence="high",
        ),
        country="Sweden",
        question="瑞典和芬兰销量差异为什么大？",
        evidence_plan={"intent": "market_overview", "entities": {"countries": ["Sweden", "Finland"]}},
    )

    direct = answer["direct"]
    assert "瑞典和芬兰销量差异可以进入双边对比判断" in direct
    assert "瑞典销量口径为 1,182,452 units，芬兰为 416,200 units" in direct
    assert "BEV 结构口径为瑞典 40.9%、芬兰 27.2%" in direct
    assert "side-by-side market table" in direct
    assert "对目标产品组合的动作" in direct
    assert "OMODA" not in direct
    assert "JAECOO" not in direct


def test_inventory_direct_references_inventory_bom_table_backbone() -> None:
    package = _pricing_package(
        intent="inventory_analysis",
        toolResults=[
            {
                "toolName": "inspect_bom_materials",
                "success": True,
                "rowCount": 3,
                "sourceType": "postgres",
                "summary": "BOM material lifecycle evidence.",
                "keyFindings": ["One OMODA9 version maps to multiple material codes."],
                "evidenceRefs": [
                    {"refId": "ev_market", "label": "bom.records.OMODA9.market", "value": "Sweden", "source": "bom"},
                    {"refId": "ev_version", "label": "bom.records.OMODA9.version", "value": "Premium AWD", "source": "bom"},
                    {"refId": "ev_material", "label": "bom.records.OMODA9.materialCode", "value": "MTRL-001 / MTRL-002", "source": "bom"},
                    {"refId": "ev_risk", "label": "BOM material lifecycle risk", "value": "duplicate material mapping", "source": "bom"},
                ],
            }
        ],
    )
    answer = apply_business_composer(
        {"title": "Inventory", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        package,
        country="Sweden",
        question="OMODA9 一个版型多个物料号应该怎么解释？",
        evidence_plan={"intent": "inventory_analysis", "entities": {"models": ["OMODA9"]}},
    )

    direct = answer["direct"]
    assert "展示骨架" not in direct
    assert "BOM / entity mapping validation table" not in direct
    assert "Inventory / BOM evidence table" not in direct
    assert "BOM/库存关系表" in answer["displayPlan"]
    assert "物料号" in direct
    assert "生命周期" in direct
    assert "OMODA9 一个版型多个物料号不能直接判错" in direct
    assert "业务版本 Premium AWD" in direct
    assert "物料号 MTRL-001 / MTRL-002" in direct
    assert "同一业务版本存在多个物料号" in direct
    digest = " ".join(answer["evidenceDigest"])
    assert "OMODA9 物料号 = MTRL-001 / MTRL-002" in digest
    assert "OMODA9 业务版本 = Premium AWD" in digest
    assert "BOM 物料生命周期风险 = duplicate material mapping" in digest


def test_inventory_generic_direct_gets_evidence_backed_lead_before_action_language() -> None:
    answer = apply_business_composer(
        {"title": "Inventory", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_inventory_stock",
            "intent": "inventory_analysis",
            "country": "Hungary",
            "toolResults": [
                {
                    "toolName": "stock_lookup",
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "postgres",
                    "summary": "Inventory signals available.",
                    "evidenceRefs": [
                        {"refId": "ev_stock", "label": "inventory.available_units", "value": 42, "unit": "units", "source": "stock"},
                        {"refId": "ev_materials", "label": "material.active_codes", "value": 6, "unit": "units", "source": "stock"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "lifecycle_status", "reason": "Lifecycle status missing.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Stock evidence is usable."},
            "insightCards": [],
        },
        country="Hungary",
        question="匈牙利当月选品表如何从物料号转成客户可编辑数量？",
        evidence_plan={"intent": "inventory_analysis", "entities": {"countries": ["Hungary"]}},
    )

    direct = answer["direct"]
    assert answer["evidenceBackedLead"].startswith("已查数据：匈牙利")
    assert "inventory.available_units = 42 units" in answer["evidenceBackedLead"]
    assert "material.active_codes = 6 units" in answer["evidenceBackedLead"]
    assert "业务判断：库存/BOM 判断应先把可用数量" in answer["evidenceBackedLead"]
    assert "缺生命周期或市场 overlay 时，不能直接开放客户可编辑数量" in answer["evidenceBackedLead"]
    assert "inventory / available units = 42 units" in direct
    assert "下一步执行" in direct
    assert "技术统计" not in direct


def test_inventory_pi_market_split_direct_does_not_fall_back_to_market_overview() -> None:
    answer = apply_business_composer(
        {"title": "Inventory", "direct": "Grounded answer.", "bullets": [], "limitations": []},
        _pricing_package(
            intent="inventory_analysis",
            toolResults=[
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "rowCount": 10,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden market snapshot without SE/FI PI tables.",
                    "keyFindings": ["country snapshot is available", "PI table is not available"],
                    "evidenceRefs": [
                        {"refId": "ev_rows", "label": "totalRows", "value": 33327, "source": "jato_country_snapshot"},
                        {"refId": "ev_versions", "label": "versionCount", "value": 9204, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_sales", "label": "cumulativeSales", "value": 1182452, "unit": "units", "source": "jato_country_snapshot"},
                    ],
                }
            ],
            missingEvidence=[
                {"name": "se_fi_pi_structure", "reason": "SE/FI PI table was not available.", "impact": "weakens_answer"},
                {"name": "vehicle_generation_mapping", "reason": "Market-level vehicle generation table was not available.", "impact": "weakens_answer"},
            ],
            confidence="high",
        ),
        country="Sweden",
        question="SE/FI 合并 PI 但车辆分市场生成，逻辑是否正确？",
        evidence_plan={"intent": "inventory_analysis", "entities": {"markets": ["SE", "FI"]}},
    )

    direct = answer["direct"]
    assert "SE/FI 合并 PI、车辆分市场生成的逻辑原则上可以成立" in direct
    assert "PI 只承载共用计划/产品信息层" in direct
    assert "market-level overlay" in direct
    assert "PI header + market overlay + materialCode / vehicle generation mapping" in direct
    assert direct.count("合并 PI") == 2
    assert direct.count("车辆生成、物料号") == 1
    assert "当前证据状态为" not in direct
    assert not direct.rstrip().endswith("，")
    assert "瑞典 SE/FI" not in direct
    assert "瑞典 合并 PI" not in direct
    assert "市场判断应落到机会 segment" not in direct
    assert "动力路线" not in direct
    assert "拆到车型/品牌" not in direct
    assert any("SE/FI 底表" in item for item in answer["reportReadyBullets"])
    assert answer["recommendedActions"][0]["action"] == "定义 PI header + market overlay + vehicle/material generation mapping"


def test_business_composer_outputs_intent_structures_for_core_automotive_intents() -> None:
    expected_labels = {
        "market_overview": ["Key metrics", "Powertrain mix", "Top models", "Product implication"],
        "pricing_analysis": ["MSRP", "Competitor corridor", "Leasing/RV/company car", "Recommendation"],
        "competitor_compare": ["Competitor table", "Feature delta", "Positioning statement"],
        "configuration_analysis": ["Validation matrix", "Must-have features", "Evidence status", "Recommendation"],
        "inventory_analysis": ["Stock/material/BOM logic", "Risk", "Next action"],
        "report_generation": ["Title", "Key message", "Evidence", "Product implication", "Next action"],
    }
    for intent, labels in expected_labels.items():
        package = _pricing_package(intent=intent)
        package["toolResults"][0]["evidenceRefs"] = [
            {"refId": "ev_price", "label": "MSRP price corridor", "value": 34720, "unit": "EUR"},
            {"refId": "ev_powertrain", "label": "BEV powertrain mix", "value": 42, "unit": "%"},
            {"refId": "ev_model", "label": "top model ranking", "value": "RAV4"},
            {"refId": "ev_feature", "label": "winter package feature delta", "value": "seat heat"},
            {"refId": "ev_bom", "label": "BOM material lifecycle risk", "value": "duplicate material"},
        ]
        answer = apply_answer_grounding_guard(
            {"title": "Analysis", "direct": "Grounded answer.", "bullets": [], "limitations": []},
            package,
            evidence_plan={"intent": intent},
        )

        joined = " ".join(answer["keyTakeaways"])
        for label in labels:
            assert label in joined
        assert answer["summary"]
        assert answer["pmInsight"]


def test_business_synthesis_report_bullets_use_intent_specific_pm_templates() -> None:
    expected_phrases = {
        "competitor_compare": "竞品判断应先锁定竞品池",
        "configuration_analysis": "配置判断必须连接真实使用场景",
        "inventory_analysis": "BOM/库存问题应先建立实体关系",
        "report_generation": "Title：",
        "voc_analysis": "VOC 判断要区分真实用户痛点",
    }
    for intent, phrase in expected_phrases.items():
        plan = build_business_synthesis_plan(
            answer={"direct": "基于证据回答。", "answerStatus": "answered"},
            evidence_package=_pricing_package(intent=intent),
            country="Sweden",
            question="业务验证问题",
            evidence_plan={"intent": intent},
        )

        assert phrase in plan["reportReadyBullets"][0]


def test_business_synthesis_direct_is_intent_specific_for_report_and_voc() -> None:
    report_plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(intent="report_generation"),
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan={"intent": "report_generation"},
    )
    voc_plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(intent="voc_analysis"),
        country="Sweden",
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        evidence_plan={"intent": "voc_analysis"},
    )

    assert "BEV 渗透率变化" in report_plan["executiveConclusion"]
    assert "J7 HEV" not in report_plan["executiveConclusion"]
    assert "Title / Key message / Evidence / Product implication / Next action" not in report_plan["executiveConclusion"]
    assert "真实用户痛点、媒体观点、论坛噪音和可转化卖点" in voc_plan["executiveConclusion"]
    assert "Sweden 的 业务分析" not in voc_plan["executiveConclusion"]


def test_j7_report_generation_uses_distilled_pricing_material() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package_with_method_material(intent="report_generation"),
        country="Sweden",
        question="把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["J7 HEV"]}},
    )

    assert "验证版定价逻辑" in plan["executiveConclusion"]
    assert "不能直接把“核心竞争带中段 + 高配主推”当成最终 MSRP 结论" in plan["executiveConclusion"]
    assert "用户材料假设" in plan["executiveConclusion"]
    assert "官方 MSRP、竞品价、月供/RV 和 PVA 口径缺口" in plan["executiveConclusion"]
    assert "Title / Key message / Evidence / Product implication / Next action" not in plan["executiveConclusion"]
    assert plan["reportReadyBullets"][0] == "Title：瑞典 J7 HEV 验证版定价逻辑"
    assert "核心竞争带中段 + 高配主推" not in plan["reportReadyBullets"][0]
    assert any("Key message" in item and "验证版价格走廊" in item for item in plan["reportReadyBullets"])
    assert any("Evidence" in item and "用户材料假设" in item for item in plan["reportReadyBullets"])
    assert plan["methodDistillation"]["model"] == "J7 HEV"


def test_j7_report_generation_keeps_business_action_ahead_of_source_repair() -> None:
    method_package = _pricing_package_with_method_material()
    package = _pricing_package_with_method_material(
        intent="report_generation",
        toolResults=[
            *_pricing_package()["toolResults"],
            *method_package["toolResults"][len(_pricing_package()["toolResults"]):],
            {
                "toolName": "query_msrp_pricing",
                "success": True,
                "rowCount": 0,
                "sourceType": "postgres",
                "summary": "No current MSRP rows for the requested model.",
                "keyFindings": ["coverage_gap:no_current_prices_for_requested_models"],
                "evidenceRefs": [],
                "coverageDiagnostics": {
                    "diagnosis": "no_current_prices_for_requested_models",
                    "sourceRepairCandidates": {
                        "dataStatus": "source_draft_candidate_not_price_evidence",
                        "ownModel": [{"brand": "", "model": "J7 HEV", "draftStatus": "candidate_search_query"}],
                        "competitorCorridor": [
                            {"brand": "TOYOTA", "model": "COROLLA CROSS", "draftStatus": "source_draft_available"},
                            {"brand": "TOYOTA", "model": "RAV4", "draftStatus": "source_draft_available"},
                        ],
                        "candidateCount": 3,
                        "materializedCandidateCount": 0,
                    },
                },
            },
        ],
        missingEvidence=[
            {
                "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                "reason": "Add current price rows for J7 HEV and core competitors.",
                "impact": "weakens_answer",
            }
        ],
    )
    answer = apply_business_composer(
        {"title": "Report", "direct": "基于证据回答。", "answerStatus": "answered"},
        package,
        country="Sweden",
        question="把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["J7 HEV"]}},
    )

    first_action = answer["recommendedActions"][0]["action"]
    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    report_next_actions = [item for item in answer["reportReadyBullets"] if item.startswith("Next action")]
    assert first_action == "把 J7 HEV 一页汇报写成市场窗口、竞品走廊、配置价值、低配锚点和高配主推"
    assert "MSRP 来源验证表" not in first_action
    assert "MSRP 来源验证表" in action_text
    assert any(first_action in item for item in report_next_actions)
    assert not any("MSRP 来源验证表" in item for item in report_next_actions)
    assert f"下一步执行：{first_action}" in answer["direct"]
    assert "下一步执行：先在 MSRP 来源验证表" not in answer["direct"]


def test_bev_penetration_report_generation_outputs_product_definition_page() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_market_package(intent="report_generation", confidence="low"),
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan={"intent": "report_generation", "entities": {"models": []}},
    )

    joined = " ".join([plan["executiveConclusion"], *plan["reportReadyBullets"]])
    assert "BEV 渗透率变化" in plan["executiveConclusion"]
    assert "产品定义验证页" in plan["executiveConclusion"]
    assert "Title / Key message / Evidence / Product implication / Next action" not in joined
    assert plan["reportReadyBullets"][0] == "Title：瑞典 BEV 渗透率变化对产品定义的影响"
    assert any("续航、充电、冬季包、价格门槛" in item for item in plan["reportReadyBullets"])
    assert plan["recommendedActions"][0]["action"].startswith("补齐 BEV 年/月度渗透率")
    assert "J7 HEV" not in " ".join(item["action"] for item in plan["recommendedActions"][:2])


def test_bev_penetration_report_generation_uses_powertrain_trend_and_top_model_refs() -> None:
    package = _market_package(
        intent="report_generation",
        confidence="high",
        toolResults=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "rowCount": 1,
                "sourceType": "jato_parquet",
                "summary": "Sweden BEV report evidence.",
                "evidenceRefs": [
                    {"refId": "ev_bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 10875, "unit": "units", "source": "jato"},
                    {"refId": "ev_bev_share", "label": "contextSnapshot.powertrainMix.BEV.share", "value": 40.9, "unit": "%", "source": "jato"},
                    {"refId": "ev_phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 6498, "unit": "units", "source": "jato"},
                    {"refId": "ev_hev_share", "label": "contextSnapshot.powertrainMix.HEV.share", "value": 7.3, "unit": "%", "source": "jato"},
                    {"refId": "ev_bev_2023", "label": "contextSnapshot.yearSeries.2023.bevShare", "value": 33.0, "unit": "%", "source": "jato"},
                    {"refId": "ev_bev_2025", "label": "contextSnapshot.yearSeries.2025.bevShare", "value": 40.9, "unit": "%", "source": "jato"},
                    {"refId": "ev_ex40", "label": "contextSnapshot.topModels.EX40.sales", "value": 2945, "unit": "units", "source": "jato"},
                    {"refId": "ev_ev3", "label": "contextSnapshot.topModels.EV3.sales", "value": 980, "unit": "units", "source": "jato"},
                ],
            }
        ],
    )
    answer = apply_business_composer(
        {"title": "Report", "direct": "Grounded answer.", "answerStatus": "answered"},
        package,
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan={"intent": "report_generation", "entities": {}},
    )

    direct = answer["direct"]
    joined_bullets = " ".join(answer["reportReadyBullets"])
    assert "BEV 渗透率变化已经可以先做一页产品定义验证页初稿" in direct
    assert "动力结构：BEV 10,875 units，份额 40.9%" in direct
    assert "PHEV 6,498 units" in direct
    assert "HEV share 7.3%" in direct or "HEV 份额 7.3%" in direct
    assert "趋势点：2023 bevShare 33%" in direct
    assert "2025 bevShare 40.9%" in direct
    assert "主销车型：EX40 2,945 units" in direct
    assert "真实续航、低温充电/热管理、冬季包、价格门槛" in direct
    assert "Evidence：动力结构：BEV 10,875 units" in joined_bullets
    assert "Product implication" in joined_bullets


def test_bev_penetration_report_treats_plain_year_values_as_annual_context_not_bev_trend() -> None:
    package = _market_package(
        intent="report_generation",
        confidence="high",
        toolResults=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "rowCount": 1,
                "sourceType": "jato_parquet",
                "summary": "Sweden BEV report evidence.",
                "evidenceRefs": [
                    {"refId": "ev_bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato"},
                    {"refId": "ev_bev_share", "label": "contextSnapshot.powertrainMix.BEV.share", "value": 42.0, "unit": "%", "source": "jato"},
                    {"refId": "ev_phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato"},
                    {"refId": "ev_hev_sales", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units", "source": "jato"},
                    {"refId": "ev_2024_total", "label": "contextSnapshot.yearSeries.2024.value", "value": 269580, "source": "jato"},
                    {"refId": "ev_2025_total", "label": "contextSnapshot.yearSeries.2025.value", "value": 272998, "source": "jato"},
                ],
            }
        ],
    )
    answer = apply_business_composer(
        {"title": "Report", "direct": "Grounded answer.", "answerStatus": "answered"},
        package,
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan={"intent": "report_generation", "entities": {}},
    )

    direct = answer["direct"]
    assert "年度总量背景：2024 269,580，2025 272,998" in direct
    assert "趋势点：2024 value" not in direct
    assert "yearSeries_1" not in direct


def test_report_generation_replaces_generic_answer_title() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        _market_package(intent="report_generation", confidence="medium"),
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan={"intent": "report_generation"},
    )

    assert answer["title"] == "瑞典 BEV 渗透率变化对产品定义的影响"
    assert "完整年/月度趋势" in answer["direct"]


def test_report_generation_replaces_local_fallback_title() -> None:
    answer = apply_business_composer(
        {
            "title": "Sweden 的 J7 HEV · 汇报生成",
            "direct": "已按 report_generation 证据计划处理 Sweden 的 J7 HEV。",
            "answerStatus": "answered",
        },
        _pricing_package_with_method_material(intent="report_generation"),
        country="Sweden",
        question="把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["J7 HEV"]}},
    )

    assert answer["title"] == "瑞典 J7 HEV 验证版定价逻辑"
    assert "核心竞争带中段 + 高配主推" not in answer["title"]
    assert "Grounded agent answer" not in answer["title"]
    assert "汇报生成" not in answer["title"]


def test_j7_sportage_pricing_outputs_direct_value_position() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(),
        country="Sweden",
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"], "competitors": ["Kia Sportage HEV"]}},
    )

    assert "Kia Sportage HEV" in plan["executiveConclusion"]
    assert "价格吸引力" in plan["executiveConclusion"]
    assert "不能先写死具体价差" in plan["executiveConclusion"]
    assert "市场结构、官方 MSRP、月供/RV 和配置价值共同证明" in plan["executiveConclusion"]


def test_o5_ev3_price_delta_is_framed_as_validation_hypothesis() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(
            insightCards=[],
            missingEvidence=[
                {"name": "current_msrp", "reason": "No current MSRP evidence.", "impact": "weakens_answer"},
                {"name": "own_model_price", "reason": "No own model price.", "impact": "weakens_answer"},
            ],
            confidence="high",
        ),
        country="Sweden",
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]}},
    )

    assert "当前缺少可引用证据" in plan["executiveConclusion"]
    assert "O5 BEV 如果比 EV3 小电池便宜 3k" in plan["executiveConclusion"]
    assert "条件成立的入门价格锚点" not in plan["executiveConclusion"]
    assert "J7" not in plan["executiveConclusion"]
    assert "官方 MSRP 交叉验证" in plan["executiveConclusion"]
    actions = [item["action"] for item in plan["recommendedActions"]]
    assert "补齐本车型与竞品 MSRP / TP / 月供价格矩阵" in actions
    assert "补齐竞品 MSRP" not in actions


def test_relative_price_question_does_not_invent_unstructured_prompt_delta() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Pricing", "direct": "O5 BEV should be cheaper.", "bullets": [], "limitations": []},
        _pricing_package(
            insightCards=[],
            missingEvidence=[
                {"name": "current_msrp", "reason": "No current MSRP evidence.", "impact": "weakens_answer"},
                {"name": "own_model_price", "reason": "No own model price.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("相对定价判断：O5 BEV 在瑞典应比 EV3 保持更强价格吸引力")
    assert "3,000 EUR" not in direct
    assert "配置差异" in direct
    assert "官方 MSRP" in direct
    assert "月供" in direct
    assert "RV" in direct
    assert "补齐 O5 BEV 与 EV3 的 MSRP / TP / 月供 / RV / 配置差异矩阵" in direct
    assert "J7 HEV" not in direct


def test_a0_suv_bev_80kwh_configuration_direct_gives_pm_stance() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Configuration", "direct": "80kWh is useful.", "bullets": [], "limitations": []},
        _market_package(
            intent="configuration_analysis",
            toolResults=[
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 3,
                    "sourceType": "engineering",
                    "summary": "A0 SUV BEV battery and winter configuration evidence.",
                    "keyFindings": ["80kWh long-range trim supports winter range confidence."],
                    "evidenceRefs": [
                        {"refId": "ev_battery", "label": "80kWh battery", "value": "long range"},
                        {"refId": "ev_heatpump", "label": "heat pump", "value": "winter efficiency"},
                    ],
                }
            ],
        ),
        country="Sweden",
        question="A0 SUV BEV 为什么需要 80kWh 电池？",
        evidence_plan={"intent": "configuration_analysis", "entities": {"models": ["A0 SUV BEV"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("配置判断：瑞典 A0 SUV BEV")
    assert "已查配置证据" in direct
    assert "80kWh battery = long range" in direct
    assert "heat pump = winter efficiency" in direct
    assert "价格、版本策略、用户场景和销售话术" in direct
    assert "A0 SUV BEV 80kWh 续航-价格-重量验证表" in direct
    assert "A0 SUV BEV 在北欧不是所有版本都必须 80kWh" not in direct
    assert "需要先给业务立场" not in direct


def test_configuration_key_takeaways_do_not_treat_market_snapshot_counts_as_config_evidence() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Configuration", "direct": "80kWh is useful.", "bullets": [], "limitations": []},
        _market_package(
            intent="configuration_analysis",
            toolResults=[
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "engineering",
                    "summary": "No usable variant rows.",
                    "evidenceRefs": [{"refId": "ev_row_count", "label": "row_count", "value": 1}],
                },
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "rowCount": 10,
                    "sourceType": "jato_parquet",
                    "summary": "Market background only.",
                    "evidenceRefs": [
                        {"refId": "ev_version", "label": "versionCount", "value": 9204, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_avg_msrp", "label": "avgMsrp", "value": 57954, "unit": "currency", "source": "jato_country_snapshot"},
                    ],
                },
            ],
            missingEvidence=[
                {
                    "name": "competitive_or_configuration_data_unavailable",
                    "reason": "No trim/configuration matrix.",
                    "impact": "weakens_answer",
                }
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="A0 SUV BEV 为什么需要 80kWh 电池？",
        evidence_plan={"intent": "configuration_analysis", "entities": {"models": ["A0 SUV BEV"]}},
    )

    joined = " ".join(answer["keyTakeaways"])
    assert "Validation matrix：Configuration validation matrix" in joined
    assert "80kWh 长续航/高配安全边界" in joined
    assert "待补竞品配置/价格证据" in joined
    assert "versionCount" not in joined
    assert "avgMsrp" not in joined
    evidence_digest = " ".join(answer["evidenceDigest"])
    assert "配置验证项" in evidence_digest
    assert "待补竞品配置/价格证据" in evidence_digest
    assert "竞品/配置证据不足" in evidence_digest
    assert "已尝试工具" in evidence_digest
    assert "配置差异" in evidence_digest
    assert "versionCount" not in evidence_digest
    assert "avgMsrp" not in evidence_digest


def test_configuration_direct_uses_market_context_without_clearing_config_gap() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Configuration", "direct": "80kWh is useful.", "bullets": [], "limitations": []},
        _market_package(
            intent="configuration_analysis",
            toolResults=[
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "engineering",
                    "summary": "No usable variant rows.",
                    "evidenceRefs": [{"refId": "ev_row_count", "label": "row_count", "value": 1}],
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "summary": "Segment cross-tabs for configuration context.",
                    "evidenceRefs": [
                        {
                            "refId": "ev_suva0_sales",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales",
                            "value": 5416,
                            "unit": "units",
                            "source": "jato_country_chart_deck",
                        },
                        {
                            "refId": "ev_suva_sales",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                            "value": 7544,
                            "unit": "units",
                            "source": "jato_country_chart_deck",
                        },
                        {
                            "refId": "ev_suva_bev",
                            "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.BEV_pct",
                            "value": 40.0,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                    ],
                },
            ],
            missingEvidence=[
                {
                    "name": "competitive_or_configuration_data_unavailable",
                    "reason": "No trim/configuration matrix.",
                    "impact": "weakens_answer",
                }
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="A0 SUV BEV 为什么需要 80kWh 电池？",
        evidence_plan={"intent": "configuration_analysis", "entities": {"models": ["A0 SUV BEV"]}},
    )

    direct = answer["direct"]
    assert "市场上下文" in direct
    assert "SUV A BEV" in direct
    assert "40%" in direct
    assert "SUV A0 细分销量 = 5,416 units" in direct
    assert "SUV A 细分销量 = 7,544 units" in direct
    assert "提供市场背景，但不能单独证明配置取舍" in direct
    assert "电池、续航、充电、价格、竞品配置矩阵交叉验证" in direct
    assert "JATO 图表数据" in direct
    assert "jato_country_chart_deck" not in direct
    assert answer["answerStatus"] == "partially_answered"
    assert answer["confidence"] == "medium"

    evidence_digest = " ".join(answer["evidenceDigest"])
    assert "SUV A BEV" in evidence_digest
    assert "SUV A0 细分销量 = 5,416 units" in evidence_digest
    assert "配置验证项" in evidence_digest
    assert "待补竞品配置/价格证据" in evidence_digest
    assert "contextSnapshot" not in evidence_digest


def test_configuration_direct_uses_competitor_context_when_variant_matrix_missing() -> None:
    answer = apply_business_composer(
        {"title": "Configuration", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        _pricing_package(
            intent="configuration_analysis",
            country="Sweden",
            entities={"models": ["O5 BEV"], "competitors": ["EV3"]},
            toolResults=[
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_cross_reference",
                    "summary": "EV3 competitor evidence.",
                    "evidenceRefs": [
                        {"refId": "ev3_model", "label": "competitor.1.model", "value": "EV3", "source": "jato_cross_reference"},
                        {"refId": "ev3_sales", "label": "EV3.sales", "value": 980, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "ev3_price_status", "label": "EV3.priceEvidenceStatus", "value": "source_draft_available", "source": "jato_cross_reference"},
                        {"refId": "ev3_source", "label": "EV3.sourceDraftPath", "value": "se/04_kia_ev3_se.yaml", "source": "jato_cross_reference"},
                    ],
                },
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "engineering",
                    "summary": "compare_vehicle_variants returned no variant/configuration matrix rows.",
                    "evidenceRefs": [],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No current MSRP rows for requested models.",
                    "evidenceRefs": [
                        {"refId": "price_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_msrp_postgres"},
                        {"refId": "price_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_msrp_postgres"},
                    ],
                },
            ],
            missingEvidence=[
                {
                    "name": "coverage_diagnostic:no_config_projects_for_country",
                    "reason": "Import engineering configuration projects.",
                    "impact": "weakens_answer",
                },
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Add current MSRP rows.",
                    "impact": "blocking",
                },
            ],
            confidence="low",
        ),
        country="Sweden",
        question="O5 BEV 和 Kia EV3 的配置差异怎么讲？请用数据支撑结论，并给出配置对比表。",
        evidence_plan={"intent": "configuration_analysis", "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("配置判断：瑞典 O5 BEV / EV3")
    assert "不能写成已验证配置胜负" in direct
    assert "EV3 销量 = 980 units" in direct
    assert "EV3 价格来源状态 = MSRP 来源草稿待审核" in direct
    assert "价格样本最低值" in direct
    assert "配置矩阵" in direct
    assert "compare_vehicle_variants" in direct
    assert "当前 MSRP" in direct
    assert "下一步执行：把瑞典市场 O5 BEV / EV3 做成配置对比矩阵，字段包括电池/续航、充电、ADAS、冬季配置、质保、空间/拖车、MSRP 和来源日期" in direct
    assert answer["recommendedActions"][0]["action"] == "把瑞典市场 O5 BEV / EV3 做成配置对比矩阵，字段包括电池/续航、充电、ADAS、冬季配置、质保、空间/拖车、MSRP 和来源日期"
    assert "J7 HEV" not in direct


def test_generic_configuration_direct_turns_feature_refs_into_business_stance() -> None:
    answer = apply_business_composer(
        {"title": "Configuration", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package(
            intent="configuration_analysis",
            country="Sweden",
            entities={"models": ["O5 BEV"], "competitors": ["EV3"]},
            toolResults=[
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 5,
                    "sourceType": "engineering",
                    "summary": "O5 BEV and EV3 feature delta rows.",
                    "evidenceRefs": [
                        {"refId": "o5_battery", "label": "O5 BEV.battery_kWh", "value": 61, "unit": "kWh", "source": "engineering_variant_diff"},
                        {"refId": "ev3_battery", "label": "EV3.battery_kWh", "value": 81, "unit": "kWh", "source": "engineering_variant_diff"},
                        {"refId": "o5_range", "label": "O5 BEV.WLTP_range_km", "value": 430, "unit": "km", "source": "engineering_variant_diff"},
                        {"refId": "ev3_range", "label": "EV3.WLTP_range_km", "value": 600, "unit": "km", "source": "engineering_variant_diff"},
                        {"refId": "o5_hud", "label": "O5 BEV.HUD", "value": "standard", "source": "engineering_variant_diff"},
                    ],
                },
            ],
            missingEvidence=[
                {"name": "current_msrp", "reason": "No current price rows.", "impact": "weakens_answer"},
                {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="O5 BEV 和 EV3 的配置差异怎么讲？",
        evidence_plan={"intent": "configuration_analysis", "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("配置判断：瑞典 O5 BEV / EV3")
    assert "电池容量不是目标车型的参数领先项" in direct
    assert "需要用更低价格、可见配置、质保/售后或版本策略补偿竞品优势" in direct
    assert "O5 BEV 电池容量 = 61 kWh" in direct
    assert "EV3 电池容量 = 81 kWh" in direct
    assert "O5 BEV WLTP 续航 = 430 km" in direct
    assert "月供/RV" in direct
    assert "价格" in direct
    assert "下一步执行：把瑞典市场 O5 BEV / EV3 做成配置对比矩阵" in direct
    assert "不能直接写成配置胜出" in direct
    assert "配置更高" not in direct
    assert "J7 HEV" not in direct


def test_high_spec_configuration_uses_powertrain_mix_context_when_cross_tabs_are_missing() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Configuration", "direct": "95kWh + 800V is useful.", "bullets": [], "limitations": []},
        _market_package(
            intent="configuration_analysis",
            toolResults=[
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "engineering",
                    "summary": "No usable variant rows.",
                    "evidenceRefs": [{"refId": "ev_row_count", "label": "row_count", "value": 1}],
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "summary": "Powertrain mix context.",
                    "evidenceRefs": [
                        {"refId": "ev_bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_hev_sales", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            missingEvidence=[
                {
                    "name": "competitive_or_configuration_data_unavailable",
                    "reason": "No trim/configuration matrix.",
                    "impact": "weakens_answer",
                }
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="4.7m A-SUV 为什么要 95kWh + 双电机 + 800V？",
        evidence_plan={"intent": "configuration_analysis", "entities": {"models": ["A-SUV BEV"]}},
    )

    direct = answer["direct"]
    assert "市场上下文" in direct
    assert "BEV 动力销量 = 25,235 units" in direct
    assert "PHEV 动力销量 = 15,028 units" in direct
    assert "提供市场背景，但不能单独证明配置取舍" in direct
    assert "电池、续航、充电、价格、竞品配置矩阵交叉验证" in direct
    assert "JATO 图表数据" in direct
    assert "jato_country_chart_deck" not in direct

    evidence_digest = " ".join(answer["evidenceDigest"])
    assert "BEV 动力销量 = 25,235 units" in evidence_digest
    assert "PHEV 动力销量 = 15,028 units" in evidence_digest
    assert "配置验证项 = 95kWh、双电机、800V、牵引/补能效率和价格带" in evidence_digest
    assert "contextSnapshot" not in evidence_digest


def test_asuv_95kwh_dual_motor_800v_configuration_direct_explains_architecture() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Configuration", "direct": "Use 95kWh and 800V.", "bullets": [], "limitations": []},
        _market_package(
            intent="configuration_analysis",
            toolResults=[
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 4,
                    "sourceType": "engineering",
                    "summary": "4.7m A-SUV high value BEV architecture evidence.",
                    "keyFindings": ["95kWh, AWD and 800V map to winter long-distance use."],
                    "evidenceRefs": [
                        {"refId": "ev_pack", "label": "95kWh battery", "value": "high range"},
                        {"refId": "ev_800v", "label": "800V charging", "value": "fast charging"},
                    ],
                }
            ],
        ),
        country="Sweden",
        question="4.7m A-SUV 为什么要 95kWh + 双电机 + 800V？",
        evidence_plan={"intent": "configuration_analysis", "entities": {"models": ["A-SUV BEV"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("配置判断：瑞典 A-SUV BEV")
    assert "已查配置证据" in direct
    assert "95kWh battery = high range" in direct
    assert "800V charging = fast charging" in direct
    assert "电池/续航和补能效率" in direct
    assert "价格、版本策略、用户场景和销售话术" in direct
    assert "需要先给业务立场" not in direct


def test_nordic_winter_package_configuration_direct_lists_must_have_and_visible_value() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Configuration", "direct": "Winter package should include comfort features.", "bullets": [], "limitations": []},
        _market_package(
            intent="configuration_analysis",
            toolResults=[
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 5,
                    "sourceType": "engineering",
                    "summary": "Nordic winter package evidence.",
                    "keyFindings": ["Heat pump, preconditioning, heated seats and winter tires are core winter package items."],
                    "evidenceRefs": [
                        {"refId": "ev_winter", "label": "winter package", "value": "heat pump, preconditioning, winter tires"},
                    ],
                }
            ],
        ),
        country="Sweden",
        question="北欧市场冬季包应该包含什么？",
        evidence_plan={"intent": "configuration_analysis", "entities": {"models": []}},
    )

    direct = answer["direct"]
    assert direct.startswith("配置判断：瑞典 目标车型")
    assert "已查配置证据" in direct
    assert "winter package = heat pump, preconditioning, winter tires" in direct
    assert "低温可用性、热管理和冬季舒适配置" in direct
    assert "价格、版本策略、用户场景和销售话术" in direct
    assert "需要先给业务立场" not in direct


def test_mixed_locale_configuration_title_is_replaced_with_business_title() -> None:
    answer = apply_business_composer(
        {
            "title": "Sweden 相关问题：北欧市场冬季包应该包含什么？请简短回答 · 配置价值分析",
            "direct": "Winter package should include comfort features.",
            "answerStatus": "answered",
        },
        _market_package(intent="configuration_analysis"),
        country="Sweden",
        question="北欧市场冬季包应该包含什么？请简短回答。",
        evidence_plan={"intent": "configuration_analysis", "entities": {"models": []}},
    )

    assert answer["title"] == "瑞典 · 北欧冬季包配置判断"
    assert answer["direct"].startswith("配置结论：当前还没有拿到可引用的冬季包逐项配置")
    assert "不能直接下 must-have 清单" in answer["direct"]


def test_o5_ex30_ev3_compare_answers_primary_benchmark() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(intent="competitor_compare"),
        country="Sweden",
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    assert "当前缺少可引用证据" in plan["executiveConclusion"]
    assert "O5 BEV 应该对标 EX30 还是 EV3" in plan["executiveConclusion"]
    assert "J7" not in plan["executiveConclusion"]
    assert "RAV4" not in plan["executiveConclusion"]
    assert "EX30 做主对标" not in plan["executiveConclusion"]


def test_apply_business_composer_exposes_scoped_evidence_package_for_artifacts() -> None:
    answer = apply_business_composer(
        {"title": "Compare", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package(intent="competitor_compare"),
        country="Sweden",
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    scoped_package = answer["evidencePackage"]
    serialized = str(scoped_package)
    assert scoped_package["confidence"] == "low"
    assert "requested_entity_evidence" in serialized
    assert "J7" not in serialized
    assert "RAV4" not in serialized
    assert "competitor corridor" not in serialized


def test_conflicting_cross_check_downgrades_business_alignment() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Policy", "direct": "Policy will lift BEV demand.", "bullets": [], "limitations": []},
        _pricing_package(jatoCrossCheck={"status": "conflicting", "summary": "External policy claim conflicts with JATO mix."}),
    )

    assert answer["answerStatus"] == "partially_answered"
    assert answer["businessSynthesisPlan"]["evidenceAlignment"]["status"] == "conflicting"
    assert "不能直接下确定判断" in answer["direct"]


def test_insufficient_evidence_still_outputs_actions_and_report_bullets() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Pricing", "direct": "建议定价 34720 EUR。", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_empty",
            "intent": "pricing_analysis",
            "country": "Sweden",
            "toolResults": [],
            "missingEvidence": [{"name": "own_model_price", "reason": "No MSRP evidence.", "impact": "blocking"}],
            "confidence": "low",
        },
    )

    assert answer["answerStatus"] == "insufficient_evidence"
    assert "不能给确定数字" in answer["direct"]
    assert answer["recommendedActions"]
    assert len(answer["reportReadyBullets"]) >= 3
    assert any("补数前仍可推进" in item for item in answer["bullets"])


def test_insufficient_evidence_direct_separates_judgment_gap_and_next_step() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Compare", "direct": "J8 clearly beats Sorento with 1234 units.", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_compare_empty",
            "intent": "competitor_compare",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": False,
                    "rowCount": 0,
                    "sourceType": "jato_parquet",
                    "summary": "No competitor rows returned.",
                    "keyFindings": [],
                    "evidenceRefs": [],
                }
            ],
            "missingEvidence": [
                {"name": "competitor_pool", "reason": "No competitor pool evidence.", "impact": "blocking"},
                {"name": "configuration_delta", "reason": "No feature delta evidence.", "impact": "weakens_answer"},
            ],
            "confidence": "low",
        },
        country="Sweden",
        question="J8 7 座四驱为什么能打 Sorento？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento"]}},
    )

    direct = answer["direct"]
    assert answer["answerStatus"] == "insufficient_evidence"
    assert "当前可判断" in direct
    assert "主对标、价格锚点、配置校验锚点和销售替代对象" in direct
    assert "不能下结论" in direct
    assert "缺少竞品池" in direct
    assert "下一步" in direct
    assert "竞品角色矩阵" in direct
    assert "1234 units" not in direct
    assert any(item.startswith("当前能判断：") for item in answer["bullets"])
    assert any(item.startswith("缺少证据：") for item in answer["bullets"])
    assert any(item.startswith("下一步动作：") for item in answer["bullets"])
    assert any(item.startswith("建议查数动作：") for item in answer["bullets"])
    assert any(item.startswith("建议输出形态：") for item in answer["bullets"])


def test_j7_method_playbook_requires_supporting_evidence() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Pricing", "direct": "建议定价 34720 EUR。", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_empty",
            "intent": "pricing_analysis",
            "country": "Sweden",
            "toolResults": [],
            "missingEvidence": [{"name": "own_model_price", "reason": "No MSRP evidence.", "impact": "blocking"}],
            "confidence": "low",
        },
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"]}},
    )

    assert answer["answerStatus"] == "insufficient_evidence"
    assert "methodDistillation" not in answer
    visible_text = " ".join([
        answer["direct"],
        *answer["bullets"],
        *answer["reportReadyBullets"],
        *answer["limitations"],
    ])
    assert "核心竞争带中段 + 高配主推" not in visible_text
    assert "34,720" not in visible_text
    assert "定价现在不能给确定数字" in answer["direct"]
    assert "价格走廊、竞品池、配置价值和购买场景" in answer["direct"]
    assert any("补齐本车型与竞品 MSRP / TP / 月供价格矩阵" in item for item in answer["reportReadyBullets"])


def test_j7_pricing_surfaces_source_repair_candidates_without_treating_them_as_price_evidence() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Pricing", "direct": "建议定价 34720 EUR。", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_j7_source_gap",
            "intent": "pricing_analysis",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No current MSRP rows for the requested model.",
                    "keyFindings": ["coverage_gap:no_current_prices_for_requested_models"],
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "diagnosis": "no_current_prices_for_requested_models",
                        "sourceRepairCandidates": {
                            "dataStatus": "source_draft_only_not_price_evidence",
                            "missingOwnModelSource": True,
                            "ownModel": [],
                            "competitorCorridor": [
                                {"brand": "TOYOTA", "model": "COROLLA CROSS", "sourceCode": "toyota_corolla_cross_se"},
                                {"brand": "TOYOTA", "model": "COROLLA", "sourceCode": "toyota_corolla_se"},
                                {"brand": "TOYOTA", "model": "RAV4", "sourceCode": "toyota_rav4_se"},
                                {"brand": "POLESTAR", "model": "4", "sourceCode": "polestar_4_se"},
                                {"brand": "KIA", "model": "SPORTAGE", "sourceCode": "kia_sportage_se"},
                            ],
                            "candidateCount": 5,
                            "materializedCandidateCount": 0,
                        },
                    },
                }
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Requested model has no current price row.",
                    "impact": "weakens_answer",
                }
            ],
            "confidence": "low",
        },
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV"]}},
    )

    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    limitation_text = " ".join(answer["limitations"])
    assert "MSRP 来源验证表" in action_text
    assert "共" in action_text
    assert "COROLLA CROSS" in action_text
    assert "RAV4" in action_text
    assert "TOYOTA COROLLA CROSS" not in action_text
    assert "KIA SPORTAGE" not in action_text
    assert "POLESTAR 4" not in action_text
    assert "COROLLA," not in action_text
    assert "不能直接当作官方价格证据" in action_text
    assert "价格来源修复候选" in limitation_text
    assert "不能直接当作官方价格证据" in limitation_text


def test_msrp_review_pending_candidates_use_review_queue_action() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Pricing", "direct": "当前缺正式 current price。", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_ex30_pending_msrp",
            "intent": "pricing_analysis",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No current MSRP rows, but review-required observations exist.",
                    "keyFindings": ["coverage_gap:no_current_prices_for_requested_models"],
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "diagnosis": "no_current_prices_for_requested_models",
                        "sourceRepairCandidates": {
                            "dataStatus": "source_draft_candidate_not_price_evidence",
                            "missingOwnModelSource": True,
                            "ownModel": [
                                {
                                    "brand": "VOLVO",
                                    "model": "EX30",
                                    "candidateSourceType": "source_draft",
                                    "draftStatus": "source_draft_available",
                                    "reviewPendingRows": 3,
                                    "reviewPendingStatus": "review_pending_not_current_price",
                                    "reviewPendingObservations": [
                                        {
                                            "trim": "Core",
                                            "sourceMsrpValue": 429000,
                                            "sourceCurrency": "SEK",
                                            "evidenceStatus": "review_pending_not_current_price",
                                        },
                                        {
                                            "trim": "Plus",
                                            "sourceMsrpValue": 457000,
                                            "sourceCurrency": "SEK",
                                            "evidenceStatus": "review_pending_not_current_price",
                                        },
                                        {
                                            "trim": "Ultra",
                                            "sourceMsrpValue": 559000,
                                            "sourceCurrency": "SEK",
                                            "evidenceStatus": "review_pending_not_current_price",
                                        }
                                    ],
                                }
                            ],
                            "competitorCorridor": [],
                            "candidateCount": 1,
                            "reviewPendingObservationCount": 3,
                        },
                    },
                }
            ],
            "missingEvidence": [
                {
                    "name": "current_msrp",
                    "reason": "Requested model has no accepted current price row.",
                    "impact": "weakens_answer",
                }
            ],
            "confidence": "low",
        },
        country="Sweden",
        question="EX30 和 EV3 怎么做价格对比？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["EX30", "EV3"]}},
    )

    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    rationale_text = " ".join(item["rationale"] for item in answer["recommendedActions"])

    assert "MSRP review queue" in action_text
    assert "共3条" in action_text
    assert "EX30" in action_text
    assert "不能直接当作确定 MSRP" in action_text
    assert "待审核官方价格观察" in rationale_text
    assert "生成 current price 后写确定价格数字" in rationale_text
    assert "已抓到 3 条官方来源待审核 MSRP 观察" in answer["direct"]
    assert "429,000-559,000 SEK" in answer["direct"]
    assert "未审核前不能当正式 current MSRP" in answer["direct"]
    assert "不能直接写成正式成交价" in answer["direct"]
    assert "已抓到 3 条官方来源待审核 MSRP 观察" in answer["summary"]
    assert any("MSRP review" in item and "429,000-559,000 SEK" in item for item in answer["keyTakeaways"])
    assert any("待审核价格" in item and "未审核前不能当正式 current MSRP" in item for item in answer["bullets"])


def test_policy_source_repair_candidates_do_not_use_msrp_language() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Policy", "direct": "当前证据不足，先给情景框架。", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_policy_source_gap",
            "intent": "news_policy_search",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "summary": "No usable policy source rows.",
                    "keyFindings": [],
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "sourceRepairCandidates": {
                            "dataStatus": "external_policy_source_candidates",
                            "missingOwnModelSource": False,
                            "ownModel": [],
                            "competitorCorridor": [
                                {
                                    "brand": "official",
                                    "model": "Sweden government policy source",
                                    "sourceCode": "policy-official-sweden-1",
                                    "sourceUrl": "https://www.google.com/search?q=site%3Aregeringen.se+elbilspremie",
                                },
                                {
                                    "brand": "official",
                                    "model": "Sweden vehicle-tax/bonus official source",
                                    "sourceCode": "policy-official-sweden-2",
                                },
                            ],
                            "candidateCount": 2,
                            "materializedCandidateCount": 0,
                        },
                    },
                }
            ],
            "missingEvidence": [
                {
                    "name": "minimum_external_sources",
                    "reason": "Policy/news claims require usable external sources.",
                    "impact": "blocking",
                }
            ],
            "confidence": "low",
        },
        country="Sweden",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        evidence_plan={"intent": "news_policy_search"},
    )

    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    rationale_text = " ".join(item["rationale"] for item in answer["recommendedActions"])
    assert "政策/新闻官方来源候选" in action_text
    assert "发布日期、适用对象和限制条件" in action_text
    assert "不能直接当作政策事实" in action_text
    assert "MSRP" not in action_text
    assert "price claims" not in rationale_text


def test_msrp_search_candidates_do_not_use_source_draft_language() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Pricing", "direct": "可以先做走廊假设，但缺官方 MSRP。", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_o5_search_candidate_gap",
            "intent": "pricing_analysis",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No current MSRP rows for requested models.",
                    "keyFindings": ["coverage_diagnosis:no_current_prices_for_requested_models"],
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "diagnosis": "no_current_prices_for_requested_models",
                        "sourceRepairCandidates": {
                            "dataStatus": "own_model_current_price_source_candidates",
                            "missingOwnModelSource": True,
                            "ownModel": [
                                {
                                    "brand": "",
                                    "model": "O5 BEV",
                                    "sourceCode": "msrp-source-sweden-o5-bev-1",
                                    "draftStatus": "candidate_search_query",
                                    "sourceUrl": "https://www.google.com/search?q=Sweden+O5+BEV+official+price+MSRP",
                                },
                                {
                                    "brand": "",
                                    "model": "EV3",
                                    "sourceCode": "msrp-source-sweden-ev3-2",
                                    "draftStatus": "candidate_search_query",
                                    "sourceUrl": "https://www.google.com/search?q=Sweden+EV3+official+price+MSRP",
                                },
                            ],
                            "competitorCorridor": [],
                            "candidateCount": 2,
                            "materializedCandidateCount": 0,
                        },
                    },
                }
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Requested models have no current price row.",
                    "impact": "weakens_answer",
                }
            ],
            "confidence": "low",
        },
        country="Sweden",
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["O5 BEV", "EV3"]}},
    )

    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    assert "MSRP 来源验证表" in action_text
    assert "官方价格候选" in action_text
    assert "共2项" in action_text
    assert "O5 BEV" in action_text
    assert "EV3" in action_text
    assert "搜索候选只是补源入口" in action_text
    assert "来源草稿" not in action_text


def test_external_research_gap_is_rendered_as_business_language() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "VOC", "direct": "Based on available evidence.", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_voc_gap",
            "intent": "voc_analysis",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "web",
                    "summary": "Only source/date/count refs were returned.",
                    "keyFindings": [],
                    "evidenceRefs": [
                        {"refId": "src_1", "label": "forum source count", "value": 2},
                    ],
                }
            ],
            "missingEvidence": [
                {
                    "name": "external_research_claims_unavailable",
                    "reason": "External research returned only source/date/count refs; no supported claim or source-backed business finding was available.",
                    "impact": "weakens_answer",
                }
            ],
            "confidence": "medium",
        },
        country="Sweden",
        question="拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        evidence_plan={"intent": "voc_analysis"},
    )

    visible_text = " ".join([
        answer["direct"],
        *answer["bullets"],
        *answer["limitations"],
        *answer["reportReadyBullets"],
    ])
    assert "外部来源结论不足" in visible_text
    assert "补 Tavily/web/VOC 可引用来源" in visible_text
    assert "external_research_claims_unavailable" not in visible_text
    assert "source/date/count refs" not in visible_text
    assert "Business Composer" not in visible_text


def test_voc_external_research_gap_surfaces_attempted_queries_not_as_evidence() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "VOC", "direct": "Evidence is missing.", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_voc_query_gap",
            "intent": "voc_analysis",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "summary": "No external research sources were returned by the configured search providers.",
                    "keyFindings": [],
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "externalResearchQueries": [
                            "OMODA JAECOO Sweden owner review complaints",
                            "OMODA JAECOO Sweden winter charging forum",
                        ],
                        "sourceRepairCandidates": {},
                    },
                }
            ],
            "missingEvidence": [
                {
                    "name": "external_research_claims_unavailable",
                    "reason": "No citation-ready VOC claims were returned.",
                    "impact": "weakens_answer",
                },
                {
                    "name": "minimum_external_sources",
                    "reason": "voc_analysis requires at least 1 external sources; 0 usable sources were kept.",
                    "impact": "weakens_answer",
                }
            ],
            "confidence": "medium",
        },
        country="Sweden",
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        evidence_plan={"intent": "voc_analysis"},
    )

    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    rationale_text = " ".join(item["rationale"] for item in answer["recommendedActions"])
    digest_text = " ".join(answer["evidenceDigest"])
    assert "外部来源修复表" in action_text
    assert "共2条" in action_text
    assert "OMODA JAECOO Sweden owner review complaints" in action_text
    assert "不能直接当作用户高频吐槽证据" in action_text
    assert "VOC/媒体/论坛检索线索" in rationale_text
    assert "可引用来源" in rationale_text
    assert "外部来源验证矩阵" in answer["displayPlan"]
    assert "再看 VOC 主题表" in answer["displayPlan"]
    assert "owner review complaints" not in digest_text
    assert "外部来源数量不足" in " ".join([answer["direct"], *answer["limitations"], *answer["reportReadyBullets"]])
    assert "minimum external sources" not in " ".join([answer["direct"], *answer["limitations"], *answer["reportReadyBullets"]])


def test_v2h_voc_keeps_business_validation_action_ahead_of_external_source_repair() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "V2H VOC", "direct": "Evidence is missing.", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_v2h_query_gap",
            "intent": "voc_analysis",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "summary": "No citation-ready V2H sources were returned.",
                    "keyFindings": [],
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "externalResearchQueries": [
                            "V2H Sweden owner review complaints",
                            "V2H Sverige ägare recension problem",
                        ],
                        "sourceRepairCandidates": {},
                    },
                }
            ],
            "missingEvidence": [
                {
                    "name": "external_research_claims_unavailable",
                    "reason": "No citation-ready VOC claims were returned.",
                    "impact": "weakens_answer",
                }
            ],
            "confidence": "medium",
        },
        country="Sweden",
        question="瑞典用户会不会把 V2H 当成真实购买卖点？",
        evidence_plan={"intent": "voc_analysis"},
    )

    first_action = answer["recommendedActions"][0]["action"]
    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    report_text = " ".join(answer["reportReadyBullets"])
    assert first_action == "抓取瑞典/北欧 V2H 用户原声和媒体测评证据"
    assert "外部来源修复表" not in first_action
    assert "外部来源修复表" in action_text
    assert "建议动作：抓取瑞典/北欧 V2H 用户原声和媒体测评证据" in report_text
    assert "下一步执行：抓取瑞典/北欧 V2H 用户原声和媒体测评证据" in answer["direct"]


def test_external_research_query_repair_removes_negative_country_constraint() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Hungary route", "direct": "Evidence is missing.", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_hungary_query_gap",
            "intent": "market_overview",
            "country": "Hungary",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "summary": "No citation-ready sources were returned.",
                    "keyFindings": [],
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "externalResearchQueries": [
                            "匈牙利 B SUV 现在应该优先看 PHEV 还是 HEV？请用数据和图表支持，不要回答瑞典。 Hungary Magyarország HEV PHEV automotive market sales registration",
                        ],
                        "sourceRepairCandidates": {},
                    },
                }
            ],
            "missingEvidence": [
                {
                    "name": "minimum_external_sources",
                    "reason": "market_overview requires at least 1 external sources; 0 usable sources were kept.",
                    "impact": "weakens_answer",
                },
            ],
            "confidence": "medium",
        },
        country="Hungary",
        question="匈牙利 B SUV 现在应该优先看 PHEV 还是 HEV？请用数据和图表支持，不要回答瑞典。",
        evidence_plan={"intent": "market_overview"},
    )

    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    assert "匈牙利 B SUV 现在应该优先看 PHEV 还是 HEV" in action_text
    assert "Hungary Magyarország" in action_text
    assert "不要回答瑞典" not in action_text
    assert "瑞典" not in action_text


def test_phev_leasing_external_research_gap_uses_tco_source_repair_language() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "PHEV leasing", "direct": "PHEV may still have a fleet role.", "bullets": [], "limitations": []},
        {
            "evidenceId": "evpkg_phev_leasing_gap",
            "intent": "pricing_analysis",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "summary": "No usable TCO sources were returned.",
                    "keyFindings": [],
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "externalResearchQueries": [
                            "site:skatteverket.se bilförmån laddhybrid 2026",
                            "Sweden PHEV company car benefit leasing TCO residual value",
                        ],
                        "sourceRepairCandidates": {},
                    },
                }
            ],
            "missingEvidence": [
                {
                    "name": "minimum_external_sources",
                    "reason": "pricing_analysis requires at least 2 external sources; 0 usable sources were kept.",
                    "impact": "blocking",
                },
                {
                    "name": "leasing_tco_or_company_car_evidence",
                    "reason": "Question requires monthly payment, residual value, tax benefit, or fleet TCO evidence.",
                    "impact": "blocking",
                },
            ],
            "confidence": "low",
        },
        country="Sweden",
        question="大客户 leasing 场景下，PHEV 还有没有理由？",
        evidence_plan={"intent": "pricing_analysis"},
    )

    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    rationale_text = " ".join(item["rationale"] for item in answer["recommendedActions"])
    risk_text = " ".join(
        [
            risk["impact"] + " " + risk["mitigation"]
            for risk in answer["businessSynthesisPlan"]["risksAndMissingEvidence"]
        ]
    )
    visible_text = " ".join([
        action_text,
        rationale_text,
        risk_text,
        answer["displayPlan"],
        *answer["reportReadyBullets"],
    ])

    assert "leasing/TCO/company-car 补源线索" in action_text
    assert "月供/残值/税务 benefit" in action_text
    assert "不能直接当作 PHEV 大客户 TCO 结论" in action_text
    assert "税务 benefit、年里程和充电条件" in rationale_text
    assert "TCO/company-car 验证表" in answer["displayPlan"]
    assert "Skatteverket" in risk_text or "company-car benefit" in risk_text
    assert "VOC/媒体/论坛" not in visible_text
    assert "高频用户" not in visible_text
    assert "高频吐槽" not in visible_text


def test_competitor_report_without_evidence_uses_data_first_generic_framework() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(intent="report_generation"),
        country="Sweden",
        question="生成 O5 BEV 对标 EX30 和 EV3 的一页竞品汇报框架。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    joined = " ".join([plan["executiveConclusion"], *plan["businessImplications"], *plan["reportReadyBullets"]])
    assert "对标假设" in joined
    assert "当前缺少可引用证据" in joined
    assert "EX30 做主对标" not in joined
    assert "J7" not in joined
    assert plan["recommendedActions"][0]["action"].startswith("补齐 O5 BEV vs EX30 / EV3 的官方 MSRP")
    assert plan["reportReadyBullets"][0] == "Title：瑞典 O5 BEV vs EX30 / EV3 竞品定位页"


def test_competitor_report_without_direct_evidence_does_not_assign_fixed_roles() -> None:
    evidence_package = _pricing_package(
        intent="report_generation",
        entities={"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
        toolResults=[],
        missingEvidence=[
            {
                "name": "supporting_evidence",
                "reason": "No direct O5/EX30/EV3 evidence.",
                "impact": "weakens_answer",
            }
        ],
        confidence="low",
    )

    answer = apply_business_composer(
        {"title": "Report", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        evidence_package,
        country="Sweden",
        question="生成一页 O5 BEV 对标 EX30 和 EV3 的汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    direct = answer["direct"]
    assert "不能按车型名称套固定主/辅对标模板" in direct
    assert "不能判断主对标或价格/配置校验锚点" in direct
    assert "EX30 做主对标" not in direct
    assert "EV3 做价格/配置校验锚点" not in direct
    assert "应先给定位假设、证据边界和补证路径" not in direct
    assert "先道歉说缺数字" not in direct


def test_o5_ex30_ev3_report_direct_outputs_ppt_ready_structure() -> None:
    evidence_package = _pricing_package(
        intent="report_generation",
        entities={"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
        toolResults=[
            {
                "toolName": "compare_competitive_set",
                "success": True,
                "rowCount": 1,
                "sourceType": "jato_parquet",
                "summary": "EX30 and EV3 direct sales evidence.",
                "evidenceRefs": [
                    {"refId": "ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                    {"refId": "ev3_sales", "label": "EV3.sales", "value": 980, "unit": "units", "source": "jato_cross_reference"},
                ],
            }
        ],
        missingEvidence=[
            {
                "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                "reason": "Add or map current MSRP rows for O5 BEV, EX30, EV3 in Sweden.",
                "impact": "blocking",
            }
        ],
    )
    answer = apply_business_composer(
        {
            "title": "Sweden 相关问题：生成 O5 BEV 对标 EX30 和 EV3 的汇报结构 · 汇报生成",
            "direct": "当前证据不足，不能给确定数字。",
            "answerStatus": "partially_answered",
        },
        evidence_package,
        country="Sweden",
        question="生成一页 O5 BEV 对标 EX30 和 EV3 的汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    direct = answer["direct"]
    assert answer["title"] == "瑞典 O5 BEV vs EX30 / EV3 竞品定位页"
    assert direct.startswith("一页汇报结论：瑞典市场 O5 BEV 与 EX30 / EV3 不应等权罗列")
    assert "当前最可引用锚点是 EX30（销量 1,518 units）" in direct
    assert "EV3（销量 980 units）" in direct
    assert "O5 BEV 仍缺直接销量/MSRP/配置证据" in direct
    assert "不能写成已验证胜出" in direct
    assert "Evidence：" in direct
    assert "Product implication：" in direct
    assert "Next action：" in direct
    assert "应先给定位假设、证据边界和补证路径" not in direct
    assert "不能给确定数字" not in direct
    joined_report = " ".join(answer["reportReadyBullets"])
    assert "Key message：瑞典市场 O5 BEV 与 EX30 / EV3 不应等权罗列" in joined_report
    assert "EX30 销量 = 1,518 units" in joined_report
    assert "EV3 销量 = 980 units" in joined_report


def test_generic_competitor_report_brief_does_not_hardcode_main_secondary_roles() -> None:
    evidence_package = _pricing_package(
        intent="report_generation",
        country="Hungary",
        entities={"models": ["Nova Prime"], "competitors": ["Atlas E", "Vector Z"]},
        toolResults=[
            {
                "toolName": "compare_competitive_set",
                "success": True,
                "rowCount": 3,
                "sourceType": "jato_parquet",
                "summary": "Direct blind-model comparison evidence.",
                "evidenceRefs": [
                    {"refId": "nova_price", "label": "Nova Prime.msrp", "value": 36000, "unit": "EUR", "source": "current_price"},
                    {"refId": "atlas_sales", "label": "Atlas E.sales", "value": 1518, "unit": "units", "source": "jato"},
                    {"refId": "vector_price", "label": "Vector Z.msrp", "value": 39000, "unit": "EUR", "source": "current_price"},
                ],
            }
        ],
        missingEvidence=[
            {"name": "configuration_delta", "reason": "No feature matrix.", "impact": "weakens_answer"},
            {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
        ],
    )

    role_line = composer._generic_competitor_report_brief(
        country_label="Hungary",
        evidence_package=evidence_package,
        question_text="生成 Nova Prime 对标 Atlas E 和 Vector Z 的一页竞品汇报。",
    )

    assert "Nova Prime 与 Atlas E / Vector Z 不应等权罗列" in role_line
    assert "Nova Prime（价格 36,000 EUR）" in role_line
    assert "Atlas E（销量 1,518 units）" in role_line
    assert "Vector Z（价格 39,000 EUR）" in role_line
    assert "Atlas E 做主对标" not in role_line
    assert "Vector Z 做价格/配置校验锚点" not in role_line


def test_generic_competitor_report_core_has_no_model_specific_branch_literals() -> None:
    core_functions = (
        composer._generic_competitor_evidence_brief,
        composer._is_competitor_report_scope,
        composer._competitor_report_subject_label,
        composer._competitor_report_action_label,
        composer._competitor_report_evidence_action_label,
        composer._competitor_report_has_evidence_gap,
        composer._generic_competitor_report_brief,
    )
    source = "\n".join(inspect.getsource(function) for function in core_functions).casefold()

    for model_literal in (
        "j7", "j8", "o5", "ex30", "ev3", "sorento", "nova prime", "atlas e", "vector z",
    ):
        assert model_literal not in source


def test_competitor_compare_direct_uses_positioning_structure() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package(
            intent="competitor_compare",
            entities={"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
            toolResults=[
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "EX30 direct sales evidence only.",
                    "evidenceRefs": [
                        {"refId": "ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                    ],
                }
            ],
        ),
        country="Sweden",
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    assert answer["direct"].startswith("对标判断：")
    assert "关键证据：瑞典 EX30 销量 = 1,518 units" in answer["direct"]
    assert "O5 BEV 与 EX30 / EV3 不应等权罗列" in answer["direct"]
    assert "当前最可引用锚点是 EX30（销量 1,518 units）" in answer["direct"]
    assert "O5 BEV、EV3 仍缺直接销量/MSRP/配置证据" in answer["direct"]
    assert "EX30 设为主对标假设" not in answer["direct"]
    assert "EV3 设为价格/配置校验锚点" not in answer["direct"]
    assert "Competitor comparison table" not in answer["direct"]
    assert "Competitor comparison table" in answer["displayPlan"]
    assert "竞品矩阵" not in answer["displayPlan"]
    assert "产品动作：" in answer["direct"]
    assert "把瑞典市场 O5 BEV vs EX30 / EV3 做成车型级销量、MSRP、配置差异和月供/RV 对标表" in answer["direct"]
    assert answer["recommendedActions"][0]["action"] == "把瑞典市场 O5 BEV vs EX30 / EV3 做成车型级销量、MSRP、配置差异和月供/RV 对标表"
    assert "J7 HEV" not in answer["direct"]
    assert "J7 HEV" not in " ".join(answer["bullets"])
    assert len(answer["bullets"]) <= 5
    assert "Verdict：" not in " ".join(answer["bullets"])
    assert "So What：" not in " ".join(answer["bullets"])


def test_competitor_compare_direct_can_make_o5_role_judgment_when_all_direct_evidence_exists() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package(
            intent="competitor_compare",
            entities={"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
            toolResults=[
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 3,
                    "sourceType": "jato_parquet",
                    "summary": "Direct O5, EX30 and EV3 comparison evidence.",
                    "evidenceRefs": [
                        {"refId": "o5_price", "label": "O5 BEV.avgPrice", "value": 36000, "unit": "EUR", "source": "jato"},
                        {"refId": "ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato"},
                        {"refId": "ev3_price", "label": "EV3.avgPrice", "value": 39000, "unit": "EUR", "source": "jato"},
                    ],
                }
            ],
        ),
        country="Sweden",
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    assert "对标判断：" in answer["direct"]
    assert "O5 BEV 与 EX30" in answer["direct"]
    assert "不应等权罗列" in answer["direct"]
    assert "EX30（销量 1,518 units）" in answer["direct"]
    assert "O5 BEV（价格 36,000 EUR）" in answer["direct"]
    assert "EV3（价格 39,000 EUR）" in answer["direct"]
    assert "O5 BEV 可优先用 EX30 做主对标、EV3 做价格/配置校验锚点" not in answer["direct"]
    assert "下一步执行：把瑞典市场 O5 BEV vs EX30 / EV3 做成车型级销量、MSRP、配置差异和月供/RV 对标表" in answer["direct"]
    assert answer["recommendedActions"][0]["action"] == "把瑞典市场 O5 BEV vs EX30 / EV3 做成车型级销量、MSRP、配置差异和月供/RV 对标表"


def test_generic_competitor_compare_direct_uses_available_evidence_not_template() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_hu_t7_competitor",
            "intent": "competitor_compare",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 3,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary competitor anchors returned.",
                    "evidenceRefs": [
                        {"refId": "cor_sales", "label": "Corolla Cross.sales", "value": 1250, "unit": "units", "source": "jato"},
                        {"refId": "cor_segment", "label": "Corolla Cross.segment", "value": "SUV A", "source": "jato"},
                        {"refId": "tuc_segment", "label": "Tucson.segment", "value": "SUV A", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "target_model_price", "reason": "No T7 price rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No config diff.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Some competitor evidence is usable."},
            "insightCards": [],
        },
        country="Hungary",
        question="匈牙利 T7 HEV 应该对标 Corolla Cross 还是 Tucson？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("对标判断：")
    assert "关键证据：匈牙利" in direct
    assert "对标判断：匈牙利市场 T7 HEV 与 Corolla Cross / Tucson 不应等权罗列" in direct
    assert "当前最可引用锚点是 Corolla Cross（销量 1,250 units、级别 SUV A）" in direct
    assert "Tucson（级别 SUV A）" in direct
    assert "展示方式：把可引用竞品锚点做成对比表" in direct
    assert "字段至少包括车型、销量/份额、级别和证据来源" in direct
    assert "T7 HEV 仍缺直接销量/MSRP/配置证据" in direct
    assert "第一版结论应围绕对标角色和验证路径" in direct
    assert "J7" not in direct
    assert "O5" not in direct
    assert "EX30" not in direct
    assert "Sorento" not in direct


def test_competitor_compare_does_not_treat_zero_sales_as_anchor() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_zero_sales_competitor",
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"], "competitors": ["Sportage HEV"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "sp_sales", "label": "Sportage HEV.sales", "value": 0, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "sp_status", "label": "Sportage HEV.priceEvidenceStatus", "value": "source_draft_available", "source": "jato_cross_reference"},
                        {"refId": "sp_path", "label": "Sportage HEV.sourceDraftPath", "value": "se/13_kia_sportage_se.yaml", "source": "jato_cross_reference"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "competitor_sales", "reason": "No positive sales rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No config diff.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Source draft only."},
            "insightCards": [],
        },
        country="Sweden",
        question="J7 HEV 是否应该比 Sportage HEV 便宜？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J7 HEV"], "competitors": ["Sportage HEV"]}},
    )

    direct = answer["direct"]
    assert "现在不能直接写胜负或价差" in direct
    assert "待物化来源线索" in direct
    assert "不能当作已验证销量、MSRP 或配置差异" in direct
    assert "销量 0" not in direct
    assert "sales = 0" not in direct
    assert "0 units" not in direct
    takeaways = " ".join(answer["keyTakeaways"])
    assert "0 units" not in takeaways
    assert "source_draft_available" not in takeaways
    assert "priceEvidenceStatus" not in takeaways
    assert "sourceDraftPath" not in takeaways


def test_generic_competitor_compare_direct_uses_price_positioning_when_available() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_hu_t7_competitor_prices",
            "intent": "competitor_compare",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 6,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary competitor price anchors returned.",
                    "evidenceRefs": [
                        {"refId": "t7_price", "label": "T7 HEV.msrp", "value": 33000, "unit": "EUR", "source": "current_price"},
                        {"refId": "t7_segment", "label": "T7 HEV.segment", "value": "SUV A", "source": "jato"},
                        {"refId": "cor_price", "label": "Corolla Cross.msrp", "value": 34500, "unit": "EUR", "source": "current_price"},
                        {"refId": "cor_segment", "label": "Corolla Cross.segment", "value": "SUV A", "source": "jato"},
                        {"refId": "tuc_price", "label": "Tucson.msrp", "value": 36800, "unit": "EUR", "source": "current_price"},
                        {"refId": "tuc_segment", "label": "Tucson.segment", "value": "SUV A", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "configuration_delta", "reason": "No feature delta.", "impact": "weakens_answer"},
                {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Requested price anchors are usable."},
            "insightCards": [],
        },
        country="Hungary",
        question="匈牙利 T7 HEV 和 Corolla Cross / Tucson 怎么对标？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("对标判断：")
    assert "关键证据：匈牙利" in direct
    assert "对标判断：匈牙利市场 T7 HEV 与 Corolla Cross / Tucson 不应等权罗列" in direct
    assert "价格位置：T7 HEV 33,000 EUR 低于已查竞品价格下沿 34,500" in direct
    assert "可以先写成价格锚点/低风险进入假设" in direct
    assert "不是单纯低价" in direct
    assert "竞品价格锚点为 Corolla Cross 34,500 EUR / Tucson 36,800 EUR" in direct
    assert "配置差异" in direct
    assert "月供/RV" in direct
    assert "J7 HEV" not in direct
    assert "瑞典" not in direct


def test_competitor_case_branch_requires_matching_entities_when_summary_is_contaminated() -> None:
    answer = apply_business_composer(
        {
            "title": "Compare",
            "direct": "J8 7座四驱打 Sorento 的可行打法是场景型错位进攻。",
            "answerStatus": "answered",
        },
        {
            "evidenceId": "evpkg_hu_t7_contaminated_summary",
            "intent": "competitor_compare",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 3,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary competitor anchors returned.",
                    "evidenceRefs": [
                        {"refId": "cor_sales", "label": "Corolla Cross.sales", "value": 1250, "unit": "units", "source": "jato"},
                        {"refId": "cor_segment", "label": "Corolla Cross.segment", "value": "SUV A", "source": "jato"},
                        {"refId": "tuc_segment", "label": "Tucson.segment", "value": "SUV A", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "target_model_price", "reason": "No T7 price rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No config diff.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Some competitor evidence is usable."},
            "insightCards": [],
        },
        country="Hungary",
        question="匈牙利 T7 HEV 应该对标 Corolla Cross 还是 Tucson？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("对标判断：")
    assert "关键证据：匈牙利" in direct
    assert "对标判断：匈牙利市场 T7 HEV 与 Corolla Cross / Tucson 不应等权罗列" in direct
    assert "Corolla Cross（销量 1,250 units、级别 SUV A）" in direct
    assert "Tucson（级别 SUV A）" in direct
    assert "J8" not in direct
    assert "Sorento" not in direct


def test_generic_report_generation_uses_available_competitor_evidence_not_template() -> None:
    answer = apply_business_composer(
        {"title": "Report", "direct": "当前证据不足，不能给确定数字。", "answerStatus": "partially_answered"},
        {
            "evidenceId": "evpkg_hu_t7_report",
            "intent": "report_generation",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 3,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary competitor anchors returned.",
                    "evidenceRefs": [
                        {"refId": "cor_sales", "label": "Corolla Cross.sales", "value": 1250, "unit": "units", "source": "jato"},
                        {"refId": "cor_segment", "label": "Corolla Cross.segment", "value": "SUV A", "source": "jato"},
                        {"refId": "tuc_segment", "label": "Tucson.segment", "value": "SUV A", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "target_model_price", "reason": "No T7 price rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No config diff.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Some competitor evidence is usable."},
            "insightCards": [],
        },
        country="Hungary",
        question="生成一页匈牙利 T7 HEV 对标 Corolla Cross 和 Tucson 的汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]}},
    )

    visible = " ".join([answer["direct"], *answer["reportReadyBullets"], *answer["evidenceDigest"]])
    assert answer["direct"].startswith("一页汇报结论：匈牙利市场 T7 HEV 与 Corolla Cross / Tucson 不应等权罗列")
    assert "当前最可引用锚点是 Corolla Cross（销量 1,250 units、级别 SUV A）" in visible
    assert "Tucson（级别 SUV A）" in visible
    assert "目标车型价格、配置差异" in visible
    assert "补齐 T7 HEV vs Corolla Cross / Tucson 的官方 MSRP、竞品价格走廊" in visible
    assert any(item.startswith("Key message：匈牙利市场 T7 HEV") for item in answer["reportReadyBullets"])
    assert any(item.startswith("Evidence：Corolla Cross 销量 = 1,250 units") for item in answer["reportReadyBullets"])
    assert "KEY MESSAGE" not in visible
    assert "PPT-READY BLOCK" not in visible
    assert "J7" not in visible
    assert "O5" not in visible
    assert "EX30" not in visible
    assert "Sorento" not in visible


def test_report_generation_direct_uses_price_positioning_when_available() -> None:
    answer = apply_business_composer(
        {"title": "Report", "direct": "当前证据不足，不能给确定数字。", "answerStatus": "partially_answered"},
        {
            "evidenceId": "evpkg_hu_t7_report_prices",
            "intent": "report_generation",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 6,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary competitor price anchors returned.",
                    "evidenceRefs": [
                        {"refId": "t7_price", "label": "T7 HEV.msrp", "value": 33000, "unit": "EUR", "source": "current_price"},
                        {"refId": "t7_segment", "label": "T7 HEV.segment", "value": "SUV A", "source": "jato"},
                        {"refId": "cor_price", "label": "Corolla Cross.msrp", "value": 34500, "unit": "EUR", "source": "current_price"},
                        {"refId": "cor_segment", "label": "Corolla Cross.segment", "value": "SUV A", "source": "jato"},
                        {"refId": "tuc_price", "label": "Tucson.msrp", "value": 36800, "unit": "EUR", "source": "current_price"},
                        {"refId": "tuc_segment", "label": "Tucson.segment", "value": "SUV A", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "configuration_delta", "reason": "No feature delta.", "impact": "weakens_answer"},
                {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Requested price anchors are usable."},
            "insightCards": [],
        },
        country="Hungary",
        question="生成一页匈牙利 T7 HEV 对标 Corolla Cross 和 Tucson 的汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]}},
    )

    visible = " ".join([answer["direct"], *answer["reportReadyBullets"], *answer["evidenceDigest"]])
    assert answer["direct"].startswith("一页汇报结论：匈牙利市场 T7 HEV 与 Corolla Cross / Tucson 不应等权罗列")
    assert "价格位置：T7 HEV 33,000 EUR 低于已查竞品价格下沿 34,500" in visible
    assert "竞品价格锚点为 Corolla Cross 34,500 EUR / Tucson 36,800 EUR" in visible
    assert "Product implication：T7 HEV 可作为低位切入/价格锚点" in visible
    assert "低价会被理解成低价值" in visible
    assert "配置差异" in visible
    assert "月供/RV" in visible
    assert "先给对标角色、验证路径" not in visible
    assert "J7" not in visible
    assert "瑞典" not in visible


def test_report_generation_reference_template_does_not_override_current_entities() -> None:
    answer = apply_business_composer(
        {"title": "Report", "direct": "当前证据不足，不能给确定数字。", "answerStatus": "partially_answered"},
        {
            "evidenceId": "evpkg_hu_t7_report_reference_o5",
            "intent": "report_generation",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 3,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary competitor anchors returned.",
                    "evidenceRefs": [
                        {"refId": "cor_sales", "label": "Corolla Cross.sales", "value": 1250, "unit": "units", "source": "jato"},
                        {"refId": "cor_segment", "label": "Corolla Cross.segment", "value": "SUV A", "source": "jato"},
                        {"refId": "tuc_segment", "label": "Tucson.segment", "value": "SUV A", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "target_model_price", "reason": "No T7 price rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No config diff.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Some competitor evidence is usable."},
            "insightCards": [],
        },
        country="Hungary",
        question="参考 O5 BEV 对标 EX30 和 EV3 的页面结构，生成匈牙利 T7 HEV 对标 Corolla Cross 和 Tucson 的汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]}},
    )

    action_text = " ".join(str(item.get("action") or "") for item in answer["recommendedActions"])
    visible = " ".join([answer["title"], answer["direct"], *answer["reportReadyBullets"], action_text])
    assert "T7 HEV 与 Corolla Cross / Tucson" in visible
    assert "Corolla Cross 销量 = 1,250 units" in visible
    assert "O5 BEV vs EX30 / EV3 竞品定位页" not in visible
    assert "EX30 做主对标" not in visible
    assert "EV3 做价格/配置校验锚点" not in visible
    assert "生成 O5/EX30/EV3 一页竞品对标框架" not in visible


def test_competitor_compare_direct_references_sales_chart_when_numeric_competitor_evidence_exists() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_competitor_chart",
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento", "XC60"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "jato_parquet",
                    "summary": "Competitor sales evidence.",
                    "keyFindings": ["Sorento 882 units", "XC60 2893 units"],
                    "evidenceRefs": [
                        {"refId": "ev_sor_sales", "label": "Sorento.sales", "value": 882, "unit": "units", "source": "jato"},
                        {"refId": "ev_sor_segment", "label": "Sorento.segment", "value": "SUV B", "source": "jato"},
                        {"refId": "ev_xc60_sales", "label": "XC60.sales", "value": 2893, "unit": "units", "source": "jato"},
                        {"refId": "ev_xc60_price", "label": "XC60.avgPrice", "value": 53165, "unit": "EUR", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [],
            "confidence": "high",
            "jatoCrossCheck": {"status": "matched", "summary": "Internal competitor evidence is usable."},
            "insightCards": [],
        },
        country="Sweden",
        question="J8 7 座四驱为什么能打 Sorento？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento", "XC60"]}},
    )

    direct = answer["direct"]
    assert "Competitor sales chart" not in direct
    assert "Competitor comparison table" not in direct
    assert "Competitor sales chart" in answer["displayPlan"]
    assert "Competitor comparison table" in answer["displayPlan"]
    assert "柱状图" not in answer["displayPlan"]
    assert "竞品量级" not in direct
    assert "级别" in direct
    assert "价格/配置" in direct


def test_j8_sorento_compare_turns_suv_awd_evidence_into_business_stance() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_j8_sorento",
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "Sorento competitor context.",
                    "evidenceRefs": [
                        {"refId": "ev_sor_model", "label": "competitor.1.model", "value": "Sorento", "source": "jato"},
                        {"refId": "ev_sor_sales", "label": "Sorento.sales", "value": 882, "unit": "units", "source": "jato"},
                        {"refId": "ev_sor_seg", "label": "Sorento.segment", "value": "SUV B", "source": "jato"},
                        {"refId": "ev_sor_power", "label": "Sorento.powertrain", "value": "PHEV", "source": "jato"},
                        {"refId": "ev_sor_seats", "label": "Sorento.seats", "value": "7 seats", "source": "jato"},
                        {"refId": "ev_suvb_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV B.4WD_pct", "value": 65.9, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_4wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.4WD_pct", "value": 68.0, "unit": "%", "source": "jato"},
                        {"refId": "ev_suvb_phev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV B.PHEV_pct", "value": 21.1, "unit": "%", "source": "jato"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No J8 or Sorento current MSRP rows.",
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "diagnosis": "no_current_prices_for_requested_models",
                        "sourceRepairCandidates": {
                            "dataStatus": "source_draft_candidate_not_price_evidence",
                            "ownModel": [
                                {"brand": "", "model": "J8", "draftStatus": "candidate_search_query"},
                                {"brand": "KIA", "model": "Sorento", "draftStatus": "candidate_search_query"},
                            ],
                            "competitorCorridor": [],
                            "candidateCount": 2,
                            "materializedCandidateCount": 0,
                        },
                    },
                },
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "jato_variant_diff_service",
                    "summary": "No J8/Sorento feature delta rows.",
                    "evidenceRefs": [],
                },
            ],
            "missingEvidence": [
                {"name": "current_msrp", "reason": "No J8/Sorento MSRP rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No J8/Sorento feature delta rows.", "impact": "weakens_answer"},
            ],
            "confidence": "high",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Sorento context evidence is usable."},
            "insightCards": [],
        },
        country="Sweden",
        question="J8 7 座四驱为什么能打 Sorento？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento"]}},
    )

    direct = answer["direct"]
    assert answer["title"] == "瑞典 · J8 vs Sorento 场景型对标判断"
    assert direct.startswith("对标判断：")
    assert "关键证据：瑞典" in direct
    assert "Sorento 销量 = 882 units" in direct
    assert "Sorento 级别 = SUV B" in direct
    assert "动力 PHEV" in direct
    assert "SUV B 4WD 占比 = 65.9%" in direct
    assert "PHEV 4WD 占比 = 68%" in direct
    assert "不应等权罗列" in direct
    assert "直接写胜负" in direct
    assert "对标判断：J8 7座四驱打 Sorento 目前应写成场景型错位验证" not in direct
    assert "只说尺寸或座位数" not in direct
    assert "应先生成竞品矩阵验证可赢点和短板" not in direct
    assert "Competitor comparison table" not in direct
    assert "MSRP source validation table" in answer["displayPlan"]
    assert "Competitor comparison table" in answer["displayPlan"]
    assert "竞品矩阵" not in answer["displayPlan"]
    digest = " ".join(answer["evidenceDigest"])
    assert "SUV B 4WD 占比 = 65.9%" in digest
    assert "PHEV 4WD 占比 = 68%" in digest
    takeaways = " ".join(answer["keyTakeaways"])
    assert "Sorento 销量 882 units" in takeaways
    assert "Sorento 级别 SUV B" in takeaways
    assert "Sorento.segment" not in takeaways
    assert "contextSnapshot" not in takeaways
    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    assert answer["recommendedActions"][0]["action"].startswith("先在 MSRP 来源验证表中验证")
    assert "把瑞典市场 J8 vs Sorento 做成车型级销量、MSRP、配置差异、月供/RV、座位布局、四驱对标表" in action_text
    assert "MSRP 来源验证表" in action_text
    assert action_text != "生成竞品矩阵"
    suggestion_bullets = [item for item in answer["reportReadyBullets"] if item.startswith("建议动作：")]
    assert any("座位布局、四驱对标表" in item for item in suggestion_bullets)


def test_structural_competitor_context_supports_blind_seven_seat_awd_pair() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "Use the retrieved evidence.", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_blind_large_suv_pair",
            "intent": "competitor_compare",
            "country": "Hungary",
            "entities": {
                "models": ["Orion Max"],
                "competitors": ["Titan Seven"],
                "segments": ["SUV B"],
                "powertrains": ["PHEV"],
                "features": ["7 seats", "AWD"],
            },
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "Blind competitor and structural market evidence.",
                    "evidenceRefs": [
                        {"refId": "titan_sales", "label": "Titan Seven.sales", "value": 640, "unit": "units", "source": "jato"},
                        {"refId": "titan_segment", "label": "Titan Seven.segment", "value": "SUV B", "source": "jato"},
                        {"refId": "titan_powertrain", "label": "Titan Seven.powertrain", "value": "PHEV", "source": "jato"},
                        {"refId": "titan_seats", "label": "Titan Seven.seats", "value": "7 seats", "source": "jato"},
                        {"refId": "blind_suvb_awd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV B.4WD_pct", "value": 58.4, "unit": "%", "source": "jato"},
                        {"refId": "blind_phev_awd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.4WD_pct", "value": 62.1, "unit": "%", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "current_msrp", "reason": "No accepted price rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No direct feature delta rows.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
        },
        country="Hungary",
        question="匈牙利 Orion Max 7 座四驱能否对标 Titan Seven？",
        evidence_plan={
            "intent": "competitor_compare",
            "entities": {
                "models": ["Orion Max"],
                "competitors": ["Titan Seven"],
                "segments": ["SUV B"],
                "powertrains": ["PHEV"],
                "features": ["7 seats", "AWD"],
            },
        },
    )

    direct = answer["direct"]
    actions = " ".join(item["action"] for item in answer["recommendedActions"])
    assert answer["title"] == "匈牙利 · Orion Max vs Titan Seven 场景型对标判断"
    assert "Orion Max 与 Titan Seven 不应等权罗列" in direct
    assert "Titan Seven（销量 640 units、级别 SUV B、动力 PHEV）" in direct
    assert "SUV B 4WD 占比 = 58.4%" in direct
    assert "PHEV 4WD 占比 = 62.1%" in direct
    assert "座位布局、四驱" in actions
    assert all(name not in direct for name in ("J8", "Sorento", "O5", "EX30"))


def test_generic_structural_competitor_core_has_no_model_specific_branch_literals() -> None:
    core_functions = (
        composer._competitor_structural_context_text,
        composer._competitor_prefers_large_suv_electrified_context,
        composer._competitor_scenario_dimensions,
        composer._competitor_market_context_metric_specs,
        composer._competitor_market_context_scenario_reason,
        composer._competitor_package_needs_market_context,
        composer._competitor_next_action,
    )
    source = "\n".join(inspect.getsource(function) for function in core_functions).casefold()

    for model_literal in ("j8", "sorento", "o5", "ex30", "orion max", "titan seven"):
        assert model_literal not in source


def test_j8_sorento_compare_uses_entities_even_when_summary_is_generic() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_j8_sorento_entities_only",
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "Requested competitor context.",
                    "evidenceRefs": [
                        {"refId": "ev_sor_sales", "label": "Sorento.sales", "value": 882, "unit": "units", "source": "jato"},
                        {"refId": "ev_sor_seg", "label": "Sorento.segment", "value": "SUV B", "source": "jato"},
                        {"refId": "ev_sor_power", "label": "Sorento.powertrain", "value": "PHEV", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "current_msrp", "reason": "No J8/Sorento MSRP rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No J8/Sorento feature delta rows.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
        },
        country="Sweden",
        question="",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("对标判断：")
    assert "关键证据：瑞典" in direct
    assert "J8 与 Sorento 不应等权罗列" in direct
    assert "当前最可引用锚点是 Sorento" in direct
    assert "销量 882 units" in direct
    assert "价格 = 待补本车型和核心竞品官方 MSRP" in direct
    assert "直接写胜负" in direct
    assert "J8 7座四驱打 Sorento 目前应写成场景型错位验证" not in direct


def test_j8_sorento_compare_uses_market_context_when_direct_sorento_rows_are_missing() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        {
            "evidenceId": "evpkg_j8_sorento_market_context",
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 12,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden SUV/PHEV market context.",
                    "evidenceRefs": [
                        {"refId": "ev_suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct", "value": 60.1, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a_phev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.PHEV_pct", "value": 38.2, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_b_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV B.sales", "value": 2410, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_b_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV B.4WD_pct", "value": 65.9, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_4wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.4WD_pct", "value": 68.0, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_b_phev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV B.PHEV_pct", "value": 21.1, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No J8 or Sorento current MSRP rows.",
                    "evidenceRefs": [],
                },
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "jato_variant_diff_service",
                    "summary": "No J8/Sorento feature delta rows.",
                    "evidenceRefs": [],
                },
            ],
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "reason": "No J8/Sorento MSRP rows.", "impact": "weakens_answer"},
                {"name": "coverage_diagnostic:no_config_projects_for_country", "reason": "No J8/Sorento feature delta rows.", "impact": "weakens_answer"},
            ],
            "confidence": "high",
        },
        country="Sweden",
        question="J8 7 座四驱为什么能打 Sorento？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("对标判断：")
    assert "关键证据：瑞典" in direct
    assert "第一版结论不是“已胜出”，而是“有场景型切入理由”" in direct
    assert "J8 可以先按场景型挑战者去打 Sorento" in direct
    assert "因为 SUV B 有 2,410 units 规模" in direct
    assert "SUV B 4WD 占比 = 65.9%" in direct
    assert "PHEV 4WD 占比 = 68%" in direct
    assert "SUV B PHEV 渗透率达到 21.1%" in direct
    assert "PHEV 公司车注册占比达到 64.8%" in direct
    assert "产品动作：把已查市场场景证据先转成场景型可打理由和销售切入点" in direct
    assert "输出上用 metric cards / 场景证据表承载这些数字" in direct
    assert "缺口矩阵需列出 配置差异 = 待补逐项配置 / 版本 / 价值差异；价格 = 待补本车型和核心竞品官方 MSRP / 当前价格来源" in direct
    assert "不能替代车型级销量、MSRP、配置差异或 TCO 证据" in direct
    assert "缺口模型不能写胜出" in direct
    assert "当前缺少可引用的 Sorento/J8 价格、配置和销量证据" not in direct
    assert "J8 7座四驱打 Sorento 目前应写成场景型错位验证" not in direct
    digest = " ".join(answer["evidenceDigest"])
    assert "SUV B 4WD 占比 = 65.9%" in digest
    assert "PHEV 4WD 占比 = 68%" in digest
    assert "PHEV Business 占比 = 64.8%" in digest
    takeaways = " ".join(answer["keyTakeaways"])
    assert "SUV B 细分销量 2,410 units" in takeaways
    assert "SUV B 4WD 占比 65.9%" in takeaways
    assert "PHEV 4WD 占比 68%" in takeaways
    assert "contextSnapshot" not in takeaways


def test_hungary_j8_sorento_compare_keeps_requested_country_in_evidence_first_answer() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        {
            "evidenceId": "evpkg_hu_j8_sorento_market_context",
            "intent": "competitor_compare",
            "country": "Hungary",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 8,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary SUV/PHEV market context.",
                    "evidenceRefs": [
                        {"refId": "ev_suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 3100, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct", "value": 54.2, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 61.5, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No Hungary J8 or Sorento current MSRP rows.",
                    "evidenceRefs": [],
                },
            ],
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "reason": "No Hungary J8/Sorento MSRP rows.", "impact": "weakens_answer"},
                {"name": "coverage_diagnostic:no_config_projects_for_country", "reason": "No Hungary J8/Sorento feature delta rows.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
        },
        country="Hungary",
        question="匈牙利 J8 7 座四驱为什么能打 Sorento？不要回答瑞典。",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento"]}},
    )

    visible_text = " ".join([
        answer["title"],
        answer["direct"],
        *answer["bullets"],
        *answer["reportReadyBullets"],
    ])
    assert answer["direct"].startswith("对标判断：")
    assert "关键证据：匈牙利" in answer["direct"]
    assert "SUV A 细分销量 = 3,100 units" in answer["direct"]
    assert "PHEV 公司车注册占比达到 61.5%" in answer["direct"]
    assert "产品动作：把已查市场场景证据先转成场景型可打理由和销售切入点" in answer["direct"]
    assert "瑞典" not in visible_text


def test_competitor_market_context_does_not_treat_zero_business_share_as_support() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        {
            "evidenceId": "evpkg_hu_j8_sorento_zero_business",
            "intent": "competitor_compare",
            "country": "Hungary",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 12,
                    "sourceType": "jato_parquet",
                    "summary": "Hungary SUV/PHEV market context.",
                    "evidenceRefs": [
                        {"refId": "ev_suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 3535, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct", "value": 30.0, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 0.0, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "reason": "No Hungary J8/Sorento MSRP rows.", "impact": "weakens_answer"},
                {"name": "coverage_diagnostic:no_config_projects_for_country", "reason": "No Hungary J8/Sorento feature delta rows.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
        },
        country="Hungary",
        question="匈牙利 J8 7 座四驱为什么能打 Sorento？不要回答瑞典。",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento"]}},
    )

    direct = answer["direct"]
    assert "J8 可以先按场景型挑战者去打 Sorento" in direct
    assert "因为 SUV A 有 3,535 units 规模，SUV A 四驱需求占比达到 30%" in direct
    assert "PHEV Business 占比 = 0%" not in direct
    assert "PHEV 公司车注册占比达到 0%" not in direct
    assert "公司车注册占比达到 0" not in direct
    assert "瑞典" not in direct


def test_j8_sorento_compare_does_not_use_year_series_as_competitor_anchor() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_j8_sorento_year_series_only",
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 4,
                    "sourceType": "jato_parquet",
                    "summary": "Only market context returned.",
                    "evidenceRefs": [
                        {"refId": "ev_2023", "label": "contextSnapshot.yearSeries.2023.value", "value": 289827, "source": "jato"},
                        {"refId": "ev_2024", "label": "contextSnapshot.yearSeries.2024.value", "value": 269580, "source": "jato"},
                        {"refId": "ev_2025", "label": "contextSnapshot.yearSeries.2025.value", "value": 272998, "source": "jato"},
                        {"refId": "ev_ex40", "label": "EX40.sales", "value": 2945, "unit": "units", "source": "jato"},
                        {"refId": "ev_xc60", "label": "XC60.sales", "value": 2893, "unit": "units", "source": "jato"},
                        {"refId": "ev_sor_model", "label": "competitor.1.model", "value": "Sorento", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "competitor_sales", "reason": "No Sorento/J8 direct competitor metric rows.", "impact": "weakens_answer"},
                {"name": "current_msrp", "reason": "No J8/Sorento MSRP rows.", "impact": "weakens_answer"},
            ],
            "confidence": "medium",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Only market context exists."},
            "insightCards": [],
        },
        country="Sweden",
        question="J8 7 座四驱为什么能打 Sorento？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento"]}},
    )

    direct = answer["direct"]
    assert "缺少直接车型级销量/MSRP/配置证据" in direct
    assert "不能用未请求车型、相邻竞品池或市场年序列替代请求竞品结论" in direct
    assert "相邻竞品池销量锚点" not in direct
    assert "yearSeries" not in direct
    assert "289,827" not in direct
    assert "EX40" not in direct
    assert "XC60" not in direct
    assert "Competitor sales chart" not in direct
    assert "MSRP source validation table" not in direct
    assert "Competitor comparison table" not in direct
    assert "MSRP source validation table" in answer["displayPlan"]
    assert "Competitor comparison table" in answer["displayPlan"]
    assert "竞品矩阵" not in answer["displayPlan"]
    assert answer["evidenceDigest"] == []
    key_takeaways = " ".join(answer["keyTakeaways"])
    assert "Competitor table：待补可引用证据" in key_takeaways
    assert "Feature delta：待补可引用证据" in key_takeaways
    assert "EX40" not in key_takeaways
    assert "XC60" not in key_takeaways


def test_competitor_digest_does_not_fallback_to_unrequested_sales_when_requested_missing() -> None:
    package = _pricing_package(
        intent="competitor_compare",
        entities={"models": ["J8"], "competitors": ["Sorento"]},
        toolResults=[
            {
                "toolName": "compare_competitive_set",
                "success": True,
                "rowCount": 4,
                "sourceType": "jato_parquet",
                "summary": "Adjacent competitor pool without requested model evidence.",
                "keyFindings": ["Unrelated adjacent competitor sales should not become key evidence."],
                "evidenceRefs": [
                    {"refId": "ex40_sales", "label": "EX40.sales", "value": 2945, "unit": "units", "source": "jato_cross_reference"},
                    {"refId": "xc60_sales", "label": "XC60.sales", "value": 2893, "unit": "units", "source": "jato_cross_reference"},
                    {"refId": "modely_sales", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato_cross_reference"},
                    {"refId": "ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                ],
            },
        ],
    )

    answer = apply_business_composer(
        {"title": "Competitor", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        package,
        country="Sweden",
        question="J8 7 座四驱为什么能打 Sorento？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8"], "competitors": ["Sorento"]}},
    )

    assert answer["evidenceDigest"] == []
    joined = " ".join(answer["direct"])
    assert "EX40" not in joined
    assert "XC60" not in joined
    assert "MODEL Y" not in joined
    key_takeaways = " ".join(answer["keyTakeaways"])
    assert "Competitor table：待补可引用证据" in key_takeaways
    assert "EX40" not in key_takeaways
    assert "XC60" not in key_takeaways


def test_competitor_compare_direct_turns_volume_refs_into_positioning_stance() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_o9_xc60_positioning",
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["O9", "XC60", "EX60"], "competitors": ["XC60", "EX60"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "Competitor set returned Volvo anchors but no O9/EX60 direct rows.",
                    "evidenceRefs": [
                        {"refId": "ev_ex40_model", "label": "competitor.1.model", "value": "EX40", "source": "jato"},
                        {"refId": "ev_ex40_sales", "label": "EX40.sales", "value": 2945, "unit": "units", "source": "jato"},
                        {"refId": "ev_xc60_model", "label": "competitor.2.model", "value": "XC60", "source": "jato"},
                        {"refId": "ev_xc60_sales", "label": "XC60.sales", "value": 2893, "unit": "units", "source": "jato"},
                        {"refId": "ev_modely_model", "label": "competitor.3.model", "value": "MODEL Y", "source": "jato"},
                        {"refId": "ev_modely_sales", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato"},
                        {"refId": "ev_ex30_model", "label": "competitor.4.model", "value": "EX30", "source": "jato"},
                        {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No O9 or EX60 MSRP rows.",
                    "evidenceRefs": [],
                },
            ],
            "missingEvidence": [
                {"name": "current_msrp", "reason": "No O9/EX60 MSRP rows.", "impact": "weakens_answer"},
            ],
            "confidence": "high",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Competitor volume evidence is usable."},
            "insightCards": [],
        },
        country="Sweden",
        question="O9 和 XC60 / EX60 的定位差异是什么？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["O9"], "competitors": ["XC60", "EX60"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("对标判断：")
    assert "关键证据：瑞典 XC60 销量 = 2,893 units" in direct
    assert "O9 与 XC60 / EX60 不应等权罗列" in direct
    assert "当前最可引用锚点是 XC60（销量 2,893 units）" in direct
    assert "EX40 2,945 units" not in direct
    assert "MODEL Y 2,412 units" not in direct
    assert "O9" in direct
    assert "EX60" in direct
    assert "O9、EX60 仍缺直接销量/MSRP/配置证据" in direct
    assert "XC60 / EX60 / MSRP" not in direct
    assert "MSRP, URL 没有直接销量" not in direct
    assert "URL" not in direct
    assert "不能写成已验证胜出" in direct
    assert "Competitor sales chart" not in direct
    assert "Competitor comparison table" not in direct
    assert "关键证据" in direct
    assert "已查证据包括 XC60（销量 2,893 units）" in direct
    assert "XC60 销量 = 2,893 units" in direct
    assert "XC60.sales" not in direct
    assert "把已查竞品证据先转成主对标、价格/配置校验锚点或销售替代对象" in direct
    assert "Competitor comparison table" in answer["displayPlan"]
    assert "MSRP source validation table" in answer["displayPlan"]
    assert "Competitor sales chart" not in answer["displayPlan"]
    assert "竞品矩阵" not in answer["displayPlan"]
    assert "竞品对比要先锁定竞品池" not in direct


def test_competitor_requested_entities_collapses_brand_prefixed_repair_candidates() -> None:
    targets, competitors = _competitor_requested_entities(
        {
            "entities": {"models": ["O9", "XC60", "EX60"], "competitors": ["XC60", "EX60"]},
        },
        (
            "先按官方价格搜索候选补齐本车型 MSRP 来源"
            "（O9, VOLVO XC60, VOLVO EX60），确认 URL、版本/配置、币种。"
        ),
    )

    assert targets == ["O9"]
    assert competitors == ["XC60", "EX60"]


def test_source_repair_action_uses_model_labels_not_brand_prefixed_labels() -> None:
    text = _source_repair_action_text(
        {
            "dataStatus": "competitor_current_price_available_own_model_missing",
            "ownModel": [
                {"brand": "", "model": "O9", "draftStatus": "candidate_search_query"},
                {"brand": "VOLVO", "model": "XC60", "draftStatus": "candidate_search_query"},
                {"brand": "VOLVO", "model": "EX60", "draftStatus": "candidate_search_query"},
            ],
        }
    )

    assert "MSRP 来源验证表" in text
    assert "共3项" in text
    assert "示例：O9, XC60" in text
    assert "VOLVO XC60" not in text
    assert "VOLVO EX60" not in text


def test_source_repair_action_distinguishes_mixed_search_and_source_draft_candidates() -> None:
    text = _source_repair_action_text(
        {
            "dataStatus": "source_draft_candidate_not_price_evidence",
            "ownModel": [
                {"brand": "", "model": "O5 BEV", "draftStatus": "candidate_search_query"},
                {
                    "brand": "VOLVO",
                    "model": "EX30",
                    "draftStatus": "source_draft_available",
                    "candidateSourceType": "source_draft",
                    "sourceDraftPath": "se/05_volvo_ex30_se.yaml",
                },
            ],
        }
    )

    assert "搜索候选和来源草稿" in text
    assert "搜索候选1项：O5 BEV" in text
    assert "来源草稿1项：EX30" in text
    assert "不能直接当作官方价格证据" in text
    assert "审核本车型/竞品 MSRP 来源草稿" not in text


def test_j7_competitor_positioning_uses_user_material_not_unrequested_sales() -> None:
    package = _pricing_package(
        intent="competitor_compare",
        entities={"models": ["J7 HEV"], "competitors": ["COROLLA CROSS", "RAV4"]},
        toolResults=[
            {
                "toolName": "compare_competitive_set",
                "success": True,
                "rowCount": 4,
                "sourceType": "jato_parquet",
                "summary": "Adjacent market sales plus J7 user-material competitor evidence.",
                "keyFindings": ["J7 material defines the direct competitor pool."],
                "evidenceRefs": [
                    {"refId": "ex40_sales", "label": "EX40.sales", "value": 2945, "unit": "units", "source": "jato_cross_reference"},
                    {"refId": "xc60_sales", "label": "XC60.sales", "value": 2893, "unit": "units", "source": "jato_cross_reference"},
                    {"refId": "modely_sales", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato_cross_reference"},
                    {
                        "refId": "j7_pool",
                        "label": "J7 HEV user material competitor pool",
                        "value": "Corolla Cross, RAV4, C-HR, Qashqai",
                        "source": "J7_HEV_V4.pptx",
                    },
                    {
                        "refId": "j7_corridor",
                        "label": "J7 HEV user material competitor corridor",
                        "value": "30,000-40,000 EUR",
                        "source": "J7_HEV_V4.pptx",
                    },
                ],
            },
        ],
        missingEvidence=[
            {"name": "requested_competitor_sales", "reason": "No Corolla Cross/RAV4 direct volume rows.", "impact": "weakens_answer"},
        ],
    )

    answer = apply_business_composer(
        {"title": "Competitor", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        package,
        country="Sweden",
        question="J7 HEV 的核心竞品是谁？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J7 HEV"], "competitors": ["COROLLA CROSS", "RAV4"]}},
    )

    direct = answer["direct"]
    digest = " ".join(answer["evidenceDigest"])
    assert "EX40" not in direct
    assert "XC60" not in direct
    assert "MODEL Y" not in direct
    assert "J7 HEV 竞品池" in digest
    assert "J7 HEV 竞品价格带" in digest
    assert "MODEL Y.sales" not in digest


def test_policy_company_car_direct_uses_internal_bev_phev_channel_evidence() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_policy_company_car",
            "intent": "news_policy_search",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "generated",
                    "summary": "BEV and PHEV registration context.",
                    "evidenceRefs": [
                        {"refId": "ev_bev_sales", "label": "BEV.sales", "value": 10875, "unit": "units", "source": "jato"},
                        {"refId": "ev_bev_bus", "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Business_pct", "value": 60.3, "unit": "%", "source": "jato"},
                        {"refId": "ev_bev_pri", "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Private_pct", "value": 39.7, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_sales", "label": "PHEV.sales", "value": 6498, "unit": "units", "source": "jato"},
                        {"refId": "ev_phev_bus", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_pri", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Private_pct", "value": 35.2, "unit": "%", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [],
            "confidence": "high",
            "jatoCrossCheck": {"status": "matched", "summary": "Internal JATO market context is usable."},
            "insightCards": [],
        },
        country="Sweden",
        question="瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        evidence_plan={"intent": "news_policy_search"},
    )

    direct = answer["direct"]
    assert "市场证据：BEV 10,875 units，Business 60.3%，Private 39.7%" in direct
    assert "PHEV 6,498 units，Business 64.8%，Private 35.2%" in direct
    assert "BEV 的绝对盘更大" in direct
    assert "PHEV 更依赖公司车渠道" in direct
    assert "BEV 逻辑：已查 BEV 10,875 units，Business 60.3%，Private 39.7%" in direct
    assert "PHEV 逻辑：已查 PHEV 6,498 units，Business 64.8%，Private 35.2%" in direct
    assert "company car benefit 对 BEV 和 PHEV 的差异" in direct
    assert "建立 BEV/PHEV company car benefit 对比表" in direct


def test_policy_co2_phev_direct_uses_internal_phev_channel_evidence() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_policy_co2_phev",
            "intent": "news_policy_search",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "generated",
                    "summary": "PHEV channel and drive context.",
                    "evidenceRefs": [
                        {"refId": "ev_phev_sales", "label": "PHEV.sales", "value": 6498, "unit": "units", "source": "jato"},
                        {"refId": "ev_phev_bus", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_pri", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Private_pct", "value": 35.2, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_4wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.4WD_pct", "value": 68.0, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.2WD_pct", "value": 31.8, "unit": "%", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [],
            "confidence": "high",
            "jatoCrossCheck": {"status": "matched", "summary": "Internal JATO market context is usable."},
            "insightCards": [],
        },
        country="Sweden",
        question="CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        evidence_plan={"intent": "news_policy_search"},
    )

    direct = answer["direct"]
    assert "市场证据：PHEV 6,498 units，Business 64.8%，Private 35.2%" in direct
    assert "4WD 68%、2WD 31.8%" in direct
    assert "公司车/TCO 场景确实重要" in direct
    assert "不能自动证明税率阶梯有利" in direct
    assert "CO2 0-75g/km 阶梯只是 PHEV 的入场条件" in direct
    assert "产品动作：已查 PHEV 6,498 units，Business 64.8%，Private 35.2%，4WD 68%、2WD 31.8%" in direct


def test_policy_co2_phev_keeps_business_action_before_source_repair_and_ignores_zero_share() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        {
            "evidenceId": "evpkg_policy_co2_phev_source_gap",
            "intent": "news_policy_search",
            "country": "Sweden",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "summary": "Policy source candidates were found but not materialized.",
                    "keyFindings": [],
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "sourceRepairCandidates": {
                            "dataStatus": "external_policy_source_candidates",
                            "missingOwnModelSource": False,
                            "ownModel": [],
                            "competitorCorridor": [
                                {
                                    "brand": "official",
                                    "model": "Sweden CO2/company car tax official source",
                                    "sourceCode": "policy-official-sweden-co2",
                                },
                                {
                                    "brand": "official",
                                    "model": "Sweden benefit formula official source",
                                    "sourceCode": "policy-official-sweden-benefit",
                                },
                            ],
                            "candidateCount": 2,
                            "materializedCandidateCount": 0,
                        },
                    },
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 3,
                    "sourceType": "generated",
                    "summary": "PHEV channel context without official tax formula.",
                    "evidenceRefs": [
                        {"refId": "ev_phev_zero_share", "label": "contextSnapshot.powertrainMix.PHEV.share", "value": 0.0, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_bus", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_pri", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Private_pct", "value": 35.2, "unit": "%", "source": "jato"},
                    ],
                },
            ],
            "missingEvidence": [
                {"name": "minimum_external_sources", "reason": "No usable official source rows.", "impact": "blocking"},
                {"name": "official_source", "reason": "Official tax formula source is missing.", "impact": "blocking"},
            ],
            "confidence": "low",
            "jatoCrossCheck": {"status": "partially_aligned", "summary": "Internal market data is usable, official policy source is missing."},
            "insightCards": [],
        },
        country="Sweden",
        question="CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        evidence_plan={"intent": "news_policy_search"},
    )

    actions = [item["action"] for item in answer["recommendedActions"]]
    direct = answer["direct"]
    joined = " ".join([
        direct,
        *answer["reportReadyBullets"],
        *actions,
    ])
    assert actions[0] == "核对 PHEV 认证 CO2、税率阶梯、company car 计算公式和发布日期"
    assert "政策/新闻官方来源候选" in " ".join(actions[1:])
    assert "下一步执行：核对 PHEV 认证 CO2、税率阶梯、company car 计算公式和发布日期" in direct
    assert "PHEV 0%" not in joined
    assert "PHEV share 0%" not in joined
    assert "Business 64.8%" in direct
    assert "Private 35.2%" in direct


def test_configuration_direct_uses_user_scenario_structure() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package(intent="configuration_analysis"),
        country="Sweden",
        question="北欧市场冬季包应该包含什么？A0 SUV BEV 为什么需要 80kWh 电池？",
        evidence_plan={"intent": "configuration_analysis"},
    )

    assert "配置结论：" in answer["direct"]
    assert "还没有拿到可引用的冬季包、80kWh、续航、价格和竞品配置矩阵" in answer["direct"]
    assert "不能把冬季包 + 80kWh 写成确定版本策略" in answer["direct"]
    assert "可推进判断：" in answer["direct"]
    assert "真实续航、补能效率和高配价值" in answer["direct"]
    assert "Configuration validation matrix" not in answer["direct"]
    assert "acceptance criteria" not in answer["direct"]
    assert "current status" not in answer["direct"]
    assert "配置差异表" in answer["displayPlan"]
    assert "逐项配置/价格证据" in answer["direct"]
    assert "生成北欧冬季包 + A0 SUV BEV 80kWh 版本策略验证表" in answer["direct"]
    assert "北欧冬季包和 A0 SUV BEV 80kWh 目前只能作为联动版本策略假设" in answer["businessSynthesisPlan"]["executiveConclusion"]
    assert "需要先给业务立场" not in answer["businessSynthesisPlan"]["executiveConclusion"]
    assert answer["businessSynthesisPlan"]["recommendedActions"][0]["action"] == "生成北欧冬季包 + A0 SUV BEV 80kWh 版本策略验证表"
    assert answer["summary"].startswith("Sweden A0 SUV BEV 的冬季包和 80kWh 目前只能作为版本策略假设验证")
    assert "J7 HEV" not in answer["direct"]
    assert "J7 HEV" not in " ".join(answer["bullets"])
    assert len(answer["bullets"]) <= 5
    assert "Verdict：" not in " ".join(answer["bullets"])


def test_configuration_direct_localizes_cross_country_powertrain_refs() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package(
            intent="configuration_analysis",
            toolResults=[
                {
                    "toolName": "query_cross_country",
                    "success": True,
                    "rowCount": 4,
                    "sourceType": "jato_parquet",
                    "summary": "Nordic BEV winter package context.",
                    "evidenceRefs": [
                        {"refId": "se_bev", "label": "crossCountry.Sweden.动力类型Mix.BEV.sales", "value": "25235 units", "unit": "units", "source": "jato_cross_country"},
                        {"refId": "no_bev", "label": "crossCountry.Norway.动力类型Mix.BEV.sales", "value": "26617 units", "unit": "units", "source": "jato_cross_country"},
                        {"refId": "fi_bev", "label": "crossCountry.Finland.动力类型Mix.BEV.sales", "value": "8062 units", "unit": "units", "source": "jato_cross_country"},
                    ],
                }
            ],
            confidence="medium",
        ),
        country="Sweden",
        question="北欧市场冬季包应该包含什么？",
        evidence_plan={"intent": "configuration_analysis"},
    )

    direct = answer["direct"]
    assert "瑞典BEV 动力销量 = 25,235 units" in direct
    assert "挪威BEV 动力销量 = 26,617 units" in direct
    assert "芬兰BEV 动力销量 = 8,062 units" in direct
    assert "crossCountry." not in direct
    assert "动力类型Mix" not in direct
    assert "不能直接下 must-have 清单" in direct
    assert "热管理、加热舒适、低温充电" in direct


def test_v2h_voc_answer_is_hypothesis_with_validation_path() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(intent="voc_analysis"),
        country="Sweden",
        question="瑞典用户会不会把 V2H 当成真实购买卖点？",
        evidence_plan={"intent": "voc_analysis"},
    )

    joined = " ".join([plan["executiveConclusion"], *plan["businessImplications"], *plan["reportReadyBullets"]])
    assert "高感知但待验证" in joined
    assert "不能直接写成高频购买卖点" in joined
    assert "家庭能源、安全备份、冬季用车" in joined
    assert plan["recommendedActions"][0]["action"] == "抓取瑞典/北欧 V2H 用户原声和媒体测评证据"


def test_v2h_voc_insufficient_evidence_still_leads_with_business_stance() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "证据不足。", "answerStatus": "insufficient_evidence"},
        evidence_package={
            "evidenceId": "evpkg_v2h_gap",
            "intent": "voc_analysis",
            "country": "Sweden",
            "toolResults": [],
            "missingEvidence": [
                {"name": "consumer_signal", "reason": "No direct VOC source.", "impact": "blocking"},
                {
                    "name": "external_research_claims_unavailable",
                    "reason": "No source-backed V2H claim was available.",
                    "impact": "blocking",
                },
            ],
            "confidence": "low",
        },
        country="Sweden",
        question="瑞典用户会不会把 V2H 当成真实购买卖点？",
        evidence_plan={"intent": "voc_analysis"},
    )

    direct = plan["executiveConclusion"]
    assert direct.startswith("直接结论：瑞典 V2H 暂时不能定位为真实高频购买卖点")
    assert "高感知但待验证" in direct
    assert "家庭能源、安全备份、冬季用车" in direct
    assert "当前缺少用户原声/VOC 信号、外部来源结论不足" in direct
    assert "不是消费者调研结论" in direct
    assert "抓取瑞典/北欧 V2H 用户原声和媒体测评证据" in direct
    assert "现在不能给确定数字" not in direct


def test_v2h_voc_actions_follow_current_country_not_sweden_default() -> None:
    answer = apply_business_composer(
        {
            "title": "V2H 对匈牙利用户是不是卖点？",
            "direct": "Based on available evidence.",
            "answerStatus": "partially_answered",
        },
        _market_package(intent="voc_analysis", country="Hungary", confidence="medium"),
        country="Hungary",
        question="V2H 对匈牙利用户到底是不是卖点？请不要回答瑞典。",
        evidence_plan={"intent": "voc_analysis"},
    )

    visible_text = " ".join([
        answer["title"],
        answer["direct"],
        *answer["bullets"],
        *[item["action"] for item in answer["recommendedActions"]],
    ])
    assert answer["title"] == "匈牙利 · V2H 用户卖点验证"
    assert answer["direct"].startswith("VOC 判断：匈牙利 V2H 暂时不能定位为真实高频购买卖点")
    assert answer["recommendedActions"][0]["action"] == "抓取匈牙利/中东欧 V2H 用户原声和媒体测评证据"
    assert "请简短回答" not in answer["summary"]
    assert "不要回答" not in answer["summary"]
    assert "瑞典" not in visible_text


def test_v2h_voc_uses_market_proxy_without_claiming_consumer_research() -> None:
    package = _market_package(
        intent="voc_analysis",
        confidence="medium",
        toolResults=[
            {
                "toolName": "external_research",
                "success": True,
                "rowCount": 0,
                "sourceType": "web",
                "summary": "External research returned no direct VOC claims.",
                "keyFindings": [],
                "evidenceRefs": [],
            },
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 3,
                "sourceType": "jato_parquet",
                "summary": "Sweden market context for BEV and SUV user scenarios.",
                "keyFindings": ["BEV demand is meaningful", "SUV A0/A concentrates practical family use cases"],
                "evidenceRefs": [
                    {"refId": "ev_bev_business", "label": "BEV business share", "value": "60.3%", "unit": "%"},
                    {"refId": "ev_bev_private", "label": "BEV private share", "value": "39.7%", "unit": "%"},
                    {"refId": "ev_suv_context", "label": "SUV A0/A concentration", "value": "high"},
                ],
            },
        ],
        missingEvidence=[
            {
                "name": "consumer_signal",
                "reason": "No direct user voice source was available.",
                "impact": "weakens_answer",
            },
            {
                "name": "external_research_claims_unavailable",
                "reason": "No source-backed V2H consumer claim was available.",
                "impact": "weakens_answer",
            },
        ],
    )
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "partially_answered"},
        evidence_package=package,
        country="Sweden",
        question="瑞典用户会不会把 V2H 当成真实购买卖点？",
        evidence_plan={"intent": "voc_analysis"},
    )

    joined = " ".join([plan["executiveConclusion"], *plan["businessImplications"]])
    assert "代理判断" in joined
    assert "不是消费者调研结论" in joined
    assert "不应被写成真实高频购买卖点" in joined
    assert "高感知但待验证" in joined


def test_v2h_voc_direct_and_title_are_not_generic_or_duplicated() -> None:
    answer = apply_business_composer(
        {
            "title": "V2H 对瑞典用户到底是不是卖点？",
            "direct": "Based on available evidence.",
            "answerStatus": "partially_answered",
        },
        _market_package(intent="voc_analysis", confidence="medium"),
        country="Sweden",
        question="V2H 对瑞典用户到底是不是卖点？请简短回答。",
        evidence_plan={"intent": "voc_analysis"},
    )

    direct = answer["direct"]
    assert answer["title"] == "瑞典 · V2H 用户卖点验证"
    assert direct.startswith("VOC 判断：瑞典 V2H 暂时不能定位为真实高频购买卖点")
    assert "用户价值：" in direct
    assert "产品动作：" in direct
    assert "下一步执行：" in direct
    assert direct.count("高感知但待验证") == 1
    assert "直接结论：" not in direct


def test_non_v2h_voc_direct_outputs_candidate_themes_and_actions() -> None:
    answer = apply_business_composer(
        {
            "title": "OMODA/JAECOO VOC",
            "direct": "Based on available evidence.",
            "answerStatus": "partially_answered",
        },
        _market_package(intent="voc_analysis", confidence="medium"),
        country="Sweden",
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        evidence_plan={"intent": "voc_analysis"},
    )

    direct = answer["direct"]
    assert direct.startswith("VOC 判断：瑞典 当前不能把这些主题写成已验证“高频吐槽”")
    assert "新品牌信任/售后服务" in direct
    assert "车机/ADAS/交付体验" in direct
    assert "冬季续航和充电可靠性" in direct
    assert "配置包、交付检查、售后承诺和销售话术" in direct
    assert "生成 VOC 主题表" in direct
    assert "直接结论：" not in direct
    assert answer["evidenceDigest"] == []


def test_voc_evidence_digest_does_not_use_market_kpis_without_voc_sources() -> None:
    package = _market_package(
        intent="voc_analysis",
        confidence="medium",
        toolResults=[
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 10,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {"refId": "ev_sales", "label": "cumulativeSales", "value": 1182452.0, "unit": "units", "source": "jato_country_snapshot"},
                    {"refId": "ev_msrp", "label": "avgMsrp", "value": 57954.07, "unit": "currency", "source": "jato_country_snapshot"},
                ],
            }
        ],
        missingEvidence=[
            {
                "name": "external_research_claims_unavailable",
                "reason": "External research returned no citation-ready VOC claim evidence.",
                "impact": "weakens_answer",
            }
        ],
    )

    answer = apply_business_composer(
        {
            "title": "OMODA/JAECOO VOC",
            "direct": "Based on available evidence.",
            "answerStatus": "partially_answered",
        },
        package,
        country="Sweden",
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        evidence_plan={"intent": "voc_analysis"},
    )

    digest = answer["evidenceDigest"]
    assert digest == []
    visible_text = " ".join([answer["direct"], *answer["bullets"]])
    assert "avgMsrp" not in visible_text
    assert "cumulativeSales" not in visible_text
    assert "当前缺少可追溯 VOC 来源" in visible_text
    assert "条可引用证据" not in visible_text
    assert "JATO 内部证据与外部研究方向一致" not in visible_text


def test_non_v2h_voc_direct_uses_source_backed_theme_boundary() -> None:
    package = _market_package(
        intent="voc_analysis",
        confidence="high",
        toolResults=[
            _external_research_tool(
                "Agera PR launches the new car brand OMODA in Sweden",
                "https://agerapr.se/en/agera-pr-launches-the-new-car-brand-omoda-in-sweden",
                claim="OMODA market entry depends on dealer confidence and delivery experience.",
            ),
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 10,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {"refId": "ev_sales", "label": "cumulativeSales", "value": 1182452.0, "unit": "units", "source": "jato_country_snapshot"},
                    {"refId": "ev_msrp", "label": "avgMsrp", "value": 57954.07, "unit": "currency", "source": "jato_country_snapshot"},
                ],
            },
        ],
    )
    answer = apply_business_composer(
        {
            "title": "OMODA/JAECOO VOC",
            "direct": "Based on available evidence.",
            "answerStatus": "partially_answered",
        },
        package,
        country="Sweden",
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        evidence_plan={"intent": "voc_analysis"},
    )

    direct = answer["direct"]
    assert "可引用证据：" in direct
    assert "新品牌信任/售后与交付体验" in direct
    assert "Agera PR launches the new car brand OMODA in Sweden" in direct
    assert "不能证明瑞典真实用户已经形成高频吐槽" in direct
    assert "cumulativeSales" not in direct
    assert "avgMsrp" not in direct
    assert "世界杯" not in direct


def test_policy_questions_output_specific_business_boundaries() -> None:
    elbil = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(intent="news_policy_search"),
        country="Sweden",
        question="Elbilspremien 2026 会影响哪些车型？",
        evidence_plan={"intent": "news_policy_search"},
    )
    company_car = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(intent="news_policy_search"),
        country="Sweden",
        question="瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        evidence_plan={"intent": "news_policy_search"},
    )

    assert "车型资格、价格上限" in elbil["executiveConclusion"]
    assert "不能点名确定受益车型" in elbil["executiveConclusion"]
    assert elbil["recommendedActions"][0]["action"] == "补官方政策原文、发布日期和资格/价格上限"
    assert "benefit tax、月供、残值" in company_car["executiveConclusion"]
    assert "PHEV 的理由只在长途/无稳定充电/低风险替代场景成立" in company_car["executiveConclusion"]


def test_policy_recommended_actions_hide_internal_next_prefixes() -> None:
    package = _pricing_package(
        intent="news_policy_search",
        insightCards=[
            {
                "title": "Policy source gap",
                "claim": "Policy claim requires official-source verification.",
                "recommendedAction": "Next: confirm official source, publish date, and affected vehicle eligibility.",
                "citations": ["policy_card"],
            }
        ],
    )
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "partially_answered"},
        evidence_package=package,
        country="Sweden",
        question="瑞典最新政策新闻会影响哪些车型？",
        evidence_plan={"intent": "news_policy_search"},
    )

    actions = [item["action"] for item in plan["recommendedActions"]]
    joined = " ".join(actions)
    assert "确认官方来源、发布日期和受影响车型资格" in actions
    assert "Next:" not in joined
    assert "下一步：" not in joined


def test_policy_answer_direct_uses_pm_decision_structure() -> None:
    elbil = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package(intent="news_policy_search"),
        country="Sweden",
        question="Elbilspremien 2026 会影响哪些车型？",
        evidence_plan={"intent": "news_policy_search"},
    )
    company_car = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package(intent="news_policy_search"),
        country="Sweden",
        question="瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        evidence_plan={"intent": "news_policy_search"},
    )

    assert "政策边界：" in elbil["direct"]
    assert "受影响车型：" in elbil["direct"]
    assert "产品动作：" in elbil["direct"]
    assert "Policy / news evidence table" not in elbil["direct"]
    assert "来源表" in elbil["displayPlan"]
    assert "下一步执行：" in elbil["direct"]
    assert "不能先点名确定受益车型" in elbil["direct"]
    assert "BEV SUV A0/A" in elbil["direct"]
    assert "直接结论：" not in elbil["direct"]
    assert "BEV 逻辑：" in company_car["direct"]
    assert "PHEV 逻辑：" in company_car["direct"]
    assert "Policy / news evidence table" not in company_car["direct"]
    assert "来源表" in company_car["displayPlan"]
    assert "TCO/月供/残值算不出优势" in company_car["direct"]
    assert len(elbil["direct"]) < 900
    assert len(company_car["direct"]) < 900
    assert len(elbil["bullets"]) <= 5
    assert len(company_car["bullets"]) <= 5
    assert "Verdict：" not in " ".join(elbil["bullets"])
    assert "So What：" not in " ".join(company_car["bullets"])


def test_policy_report_generation_with_external_research_failure_uses_policy_boundary() -> None:
    package = _market_package(
        intent="report_generation",
        country="Sweden",
        toolResults=[
            {
                "toolName": "external_research",
                "success": False,
                "rowCount": 0,
                "sourceType": "web",
                "summary": "external_research timed out before returning policy sources.",
                "keyFindings": [],
                "evidenceRefs": [],
            },
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 12,
                "sourceType": "jato_parquet",
                "summary": "Sweden market snapshot with powertrain mix.",
                "keyFindings": ["BEV is the largest electrified powertrain pool."],
                "evidenceRefs": [
                    {"refId": "ev_bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato"},
                    {"refId": "ev_bev_share", "label": "contextSnapshot.powertrainMix.BEV.share", "value": "40.9%", "unit": "%", "source": "jato"},
                    {"refId": "ev_phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato"},
                    {"refId": "ev_hev_sales", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units", "source": "jato"},
                    {"refId": "ev_total", "label": "marketSnapshot.cumulativeSales", "value": 1182452, "unit": "units", "source": "jato"},
                    {"refId": "ev_avg_msrp", "label": "marketSnapshot.avgMsrp", "value": 57954.1, "source": "jato"},
                ],
            },
        ],
        missingEvidence=[
            {"name": "missing_required_tool:external_research", "reason": "External research timed out.", "impact": "blocking"},
            {"name": "external_research_failed", "reason": "No official Elbilspremien source was retrieved.", "impact": "weakens_answer"},
        ],
        confidence="low",
    )

    answer = apply_business_composer(
        {"title": "Policy report", "direct": "基于证据回答。", "answerStatus": "answered"},
        package,
        country="Sweden",
        question="Elbilspremien 2026 会影响哪些车型？请给出来源、受影响车型、图表和一页汇报结构。",
        evidence_plan={
            "intent": "report_generation",
            "requiredTools": ["external_research", "query_country_snapshot", "build_market_chart"],
        },
    )

    direct = answer["direct"]
    report_text = " ".join(answer["reportReadyBullets"])
    visible = " ".join([direct, report_text])
    assert answer["answerStatus"] == "insufficient_evidence"
    assert "Elbilspremien 2026" in direct
    assert "不能先点名确定受益车型" in direct
    assert "候选筛选逻辑" in direct
    assert "BEV 25,235 units" in direct
    assert "外部研究工具结果" in direct
    assert "官方政策原文" in visible
    assert "受影响车型矩阵" in visible
    assert "SOURCE TABLE" not in visible
    assert "cumulativeSales" not in visible
    assert "avgMsrp" not in visible


def test_policy_o5_price_cap_direct_uses_sources_before_framework() -> None:
    package = _market_package(
        intent="news_policy_search",
        country="Sweden",
        toolResults=[
            _external_research_tool(
                "Sweden EV incentive 2026",
                "https://example.gov.se/ev-incentive-2026",
                claim="A proposed 2026 BEV incentive would use a price cap and private buyer eligibility criteria.",
                date="2026-03-15",
                rank=1,
            ),
            _external_research_tool(
                "Industry summary BEV price cap",
                "https://example.com/auto/bev-cap",
                claim="Price caps would pressure entry trims to stay below eligibility thresholds while high trims need value justification.",
                date="2026-03-20",
                rank=2,
            ),
        ],
        missingEvidence=[
            {"name": "official_source", "reason": "Official regulation text was not retrieved.", "impact": "blocking"},
            {"name": "current_msrp", "reason": "O5 BEV current MSRP was not retrieved.", "impact": "weakens_answer"},
        ],
        confidence="medium",
    )

    answer = apply_business_composer(
        {"title": "Policy", "direct": "基于证据回答。", "answerStatus": "answered"},
        package,
        country="Sweden",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        evidence_plan={"intent": "news_policy_search", "entities": {"models": ["O5 BEV"]}},
    )

    direct = answer["direct"]
    assert direct.startswith("直接结论：")
    assert "关键证据：" in direct
    assert "[R1] Sweden EV incentive 2026（example.gov.se，2026-03-15）" in direct
    assert "proposed 2026 BEV incentive" in direct
    assert "[R2] Industry summary BEV price cap（example.com，2026-03-20）" in direct
    assert "Price caps would pressure entry trims" in direct
    assert "不能支持“O5 BEV 确定适用”" in direct
    assert "仍缺证据：官方来源, 官方 MSRP 交叉验证" in direct
    assert "补贴内入门锚点" in direct
    assert "补贴外高配价值" in direct
    assert "情景矩阵" not in direct
    assert "A 有效且 O5 适用" not in direct


def test_policy_answer_status_downgrades_when_named_policy_source_is_missing() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_elbilspremien_gap",
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {},
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "summary": "Generic Swedish EV tax pages were found.",
                    "keyFindings": ["Bonus - for low emission vehicles has ended - Transportstyrelsen"],
                    "evidenceRefs": [
                        {
                            "refId": "policy_bonus_ended_claim",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.claim",
                            "value": "The bonus for low emission vehicles has ended.",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                        }
                    ],
                }
            ],
            "missingEvidence": [
                {
                    "name": "target_policy_source:elbilspremien_2026",
                    "reason": "No source matched Elbilspremien 2026.",
                    "impact": "blocking",
                }
            ],
            "confidence": "low",
        },
        country="Sweden",
        question="Elbilspremien 2026 会影响哪些车型？",
        evidence_plan={"intent": "news_policy_search"},
    )

    assert answer["answerStatus"] == "partially_answered"
    assert "Transportstyrelsen 低排放车辆 bonus 已结束" in answer["direct"]
    assert "不能替代 Elbilspremien 2026 的官方原文、车型资格和价格上限" in answer["direct"]
    assert "不能先点名确定受益车型" in answer["direct"]
    assert "目标政策原文/年份来源" in " ".join(answer["limitations"])


def test_o5_bev_subsidy_cap_answer_requires_current_policy_boundary() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        _pricing_package(intent="news_policy_search", confidence="low"),
        country="Sweden",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        evidence_plan={"intent": "news_policy_search", "entities": {"models": ["O5 BEV"]}},
    )

    direct = answer["direct"]
    joined = " ".join([
        direct,
        *answer["businessImplications"],
        *answer["reportReadyBullets"],
        *(item["action"] for item in answer["recommendedActions"]),
    ])
    assert "不能默认当成现行约束" in direct
    assert "是否仍有效" in direct
    assert "如果价格上限仍有效" in direct
    assert "如果已失效" in direct
    assert "情景矩阵" in direct
    assert "A 有效且 O5 BEV 适用" in direct
    assert "B 失效或不适用" in direct
    assert "C 新计划未确认" in direct
    assert "补贴内资格锚点" in direct
    assert "补贴外高配价值" in direct
    assert "核对瑞典 BEV 补贴价格上限是否仍有效及 O5 BEV 是否适用" in joined
    assert "能否提供" not in joined


def test_o5_bev_subsidy_cap_uses_bonus_ended_official_evidence_for_direct_conclusion() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        {
            "evidenceId": "evpkg_policy_bonus_ended",
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"]},
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "web",
                    "summary": "Official Transportstyrelsen source says the bonus for low emission vehicles has ended.",
                    "keyFindings": [
                        "Bonus - for low emission vehicles has ended - Transportstyrelsen",
                        "Malus - for high emission vehicles - Transportstyrelsen",
                    ],
                    "evidenceRefs": [
                        {
                            "refId": "policy_bonus_ended_source",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.source",
                            "value": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                            "retrievedAt": "2026-06-23T12:00:00+08:00",
                        },
                        {
                            "refId": "policy_bonus_ended_claim",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.claim",
                            "value": "The bonus for low emission vehicles has ended.",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                            "retrievedAt": "2026-06-23T12:00:00+08:00",
                        },
                        {
                            "refId": "policy_bonus_ended_date",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.date",
                            "value": "2026-04-17",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                            "retrievedAt": "2026-06-23T12:00:00+08:00",
                        },
                        {
                            "refId": "policy_bonus_ended_rank",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.rank",
                            "value": "1",
                            "source": "jato_external_research_web",
                            "retrievedAt": "2026-06-23T12:00:00+08:00",
                        },
                        {
                            "refId": "policy_malus_source",
                            "label": "Malus - for high emission vehicles - Transportstyrelsen.source",
                            "value": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/malus/",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/malus/",
                            "retrievedAt": "2026-06-23T12:00:00+08:00",
                        },
                        {
                            "refId": "policy_malus_claim",
                            "label": "Malus - for high emission vehicles - Transportstyrelsen.claim",
                            "value": "Transportstyrelsen vehicle tax context.",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/malus/",
                            "retrievedAt": "2026-06-23T12:00:00+08:00",
                        },
                        {
                            "refId": "policy_malus_date",
                            "label": "Malus - for high emission vehicles - Transportstyrelsen.date",
                            "value": "2026-02-25",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/malus/",
                            "retrievedAt": "2026-06-23T12:00:00+08:00",
                        },
                        {
                            "refId": "policy_malus_rank",
                            "label": "Malus - for high emission vehicles - Transportstyrelsen.rank",
                            "value": "2",
                            "source": "jato_external_research_web",
                            "retrievedAt": "2026-06-23T12:00:00+08:00",
                        },
                    ],
                },
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "rowCount": 12,
                    "sourceType": "jato_parquet",
                    "summary": "Sweden BEV and SUV A0/A market context is available.",
                    "keyFindings": ["BEV and SUV A0/A market context available."],
                    "evidenceRefs": [
                        {"refId": "market_bev_share", "label": "BEV share", "value": "42%", "unit": "%"},
                    ],
                },
            ],
            "missingEvidence": [
                {
                    "name": "minimum_external_sources",
                    "reason": "news_policy_search requires at least 3 external sources; 2 usable sources were kept.",
                    "impact": "blocking",
                }
            ],
            "confidence": "low",
        },
        country="Sweden",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        evidence_plan={"intent": "news_policy_search", "entities": {"models": ["O5 BEV"]}},
    )

    direct = answer["direct"]
    assert "当前不应把 BEV 补贴价格上限当作 O5 BEV 的现行定价约束" in direct
    assert "bonus 已结束" in direct
    assert "回到竞品价格走廊" in direct
    assert "历史敏感锚点" in direct
    assert "补齐当前 O5 BEV MSRP、竞品价格走廊和 24/36 个月月供" in direct
    assert "候选" not in direct
    assert "Business Validation" not in direct
    assert "情景矩阵" not in direct
    assert "A 有效且 O5 适用" not in direct
    action_text = " ".join(item["action"] for item in answer["recommendedActions"])
    assert "补齐当前 O5 BEV MSRP、竞品价格走廊和 24/36 个月月供" in action_text
    assert "生成补贴失效口径下的 O5 BEV 主销版定价页" in action_text
    assert "监控瑞典是否发布新的 BEV 补贴计划" in action_text
    assert "候选" not in action_text
    assert "Business Validation" not in action_text
    assert "确认官方来源" not in action_text
    implication_text = " ".join(answer["businessImplications"])
    assert "官方 bonus 已结束后，O5 BEV 定价不应围绕补贴门槛倒推" in implication_text
    assert "O5 BEV MSRP、同价带 BEV SUV 竞品价格和 24/36 个月月供" in implication_text
    assert "确认官方来源" not in implication_text
    assert "official-source confirmation" not in implication_text
    assert "来源日期或官方来源不足" not in implication_text
    assert answer["evidenceDigest"][0] == "[R1] Bonus - for low emission vehicles has ended - Transportstyrelsen（transportstyrelsen.se，2026-04-17）"
    assert answer["evidenceDigest"][1] == "[R2] Malus - for high emission vehicles - Transportstyrelsen（transportstyrelsen.se，2026-02-25）"
    assert "rank" not in " ".join(answer["evidenceDigest"]).lower()
    assert "Gå till" not in " ".join(answer["evidenceDigest"])


def test_bev_subsidy_cap_supports_blind_model_without_product_template() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "Use the verified policy evidence.", "answerStatus": "partially_answered"},
        {
            "evidenceId": "evpkg_blind_bev_policy_cap",
            "intent": "news_policy_search",
            "country": "Hungary",
            "entities": {"models": ["Aurora E"]},
            "toolResults": [],
            "missingEvidence": [
                {
                    "name": "official_policy_source",
                    "reason": "No current official price-cap source was retrieved.",
                    "impact": "blocking",
                },
                {
                    "name": "current_msrp",
                    "reason": "No current Aurora E MSRP was retrieved.",
                    "impact": "weakens_answer",
                },
            ],
            "confidence": "low",
        },
        country="Hungary",
        question="BEV 补贴价格上限对 Aurora E 定价有什么影响？",
        evidence_plan={"intent": "news_policy_search", "entities": {"models": ["Aurora E"]}},
    )

    direct = answer["direct"]
    actions = " ".join(item["action"] for item in answer["recommendedActions"])
    implications = " ".join(answer["businessImplications"])
    assert "不能默认当成现行约束" in direct
    assert "A 有效且 Aurora E 适用" in direct
    assert "B 失效或不适用" in direct
    assert "C 新计划未确认" in direct
    assert "核对匈牙利 BEV 补贴价格上限是否仍有效及 Aurora E 是否适用" in actions
    assert "Aurora E" in implications
    assert all(name not in " ".join((direct, actions, implications)) for name in ("O5 BEV", "J7", "EV3"))


def test_bev_subsidy_cap_core_has_no_model_specific_branch_literals() -> None:
    core_functions = (
        composer._is_bev_subsidy_cap_question,
        composer._policy_target_model,
        composer._bev_subsidy_cap_bonus_ended_recommended_actions,
        composer._policy_primary_action,
        composer._policy_action_is_primary,
    )
    source = "\n".join(inspect.getsource(function) for function in core_functions).casefold()

    for model_literal in ("o5", "j7", "ev3", "aurora e"):
        assert model_literal not in source


def test_voc_evidence_digest_uses_citations_before_structured_refs() -> None:
    package = _market_package(
        intent="voc_analysis",
        toolResults=[
            _external_research_tool(
                "Nordic V2H owner forum",
                "https://example.com/forum/v2h-winter",
                claim="Owners discuss backup power, winter charging, and home energy resilience.",
            ),
            {
                "toolName": "query_voc_summary",
                "success": True,
                "rowCount": 24,
                "sourceType": "voc",
                "summary": "VOC summary for V2H concerns.",
                "keyFindings": ["V2H mentions cluster around backup power and winter reliability."],
                "evidenceRefs": [
                    {"refId": "voc_count", "label": "VOC complaint count", "value": 24, "source": "voc_database"},
                ],
            },
        ],
    )

    answer = apply_business_composer(
        {"title": "VOC", "direct": "基于证据回答。", "answerStatus": "answered"},
        package,
        country="Sweden",
        question="瑞典用户会不会把 V2H 当成真实购买卖点？",
        evidence_plan={"intent": "voc_analysis"},
    )

    digest = answer["evidenceDigest"]
    assert digest[0] == "[R1] Nordic V2H owner forum（example.com，2026-02-10）"
    assert any("VOC complaint count = 24（voc_database）" == item for item in digest)
    joined = " ".join(digest)
    assert ".claim" not in joined
    assert "rank" not in joined.lower()


def test_report_generation_evidence_digest_uses_citations_before_structured_refs() -> None:
    package = _market_package(
        intent="report_generation",
        toolResults=[
            _external_research_tool(
                "O5 BEV EX30 EV3 benchmark article",
                "https://example.com/research/o5-ex30-ev3",
                claim="Benchmark article compares O5 BEV positioning against EX30 and EV3.",
                date="2026-03-03",
            ),
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 12,
                "sourceType": "jato_parquet",
                "summary": "BEV market snapshot.",
                "keyFindings": ["BEV share is material for the report page."],
                "evidenceRefs": [
                    {"refId": "bev_share", "label": "BEV share", "value": "42%", "unit": "%", "source": "jato_parquet"},
                ],
            },
        ],
    )

    answer = apply_business_composer(
        {"title": "Report", "direct": "基于证据回答。", "answerStatus": "answered"},
        package,
        country="Sweden",
        question="生成一页 O5 BEV 对标 EX30 和 EV3 的汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    digest = answer["evidenceDigest"]
    assert digest[0] == "[R1] O5 BEV EX30 EV3 benchmark article（example.com，2026-03-03）"
    assert any("BEV share = 42%" in item for item in digest)
    joined = " ".join(digest)
    assert ".claim" not in joined
    assert "rank" not in joined.lower()


def test_competitor_compare_evidence_digest_keeps_citation_and_internal_competitor_data() -> None:
    package = _pricing_package(
        intent="competitor_compare",
        toolResults=[
            _external_research_tool(
                "EX30 EV3 competitor review",
                "https://example.com/reviews/ex30-ev3",
                claim="Review compares EX30 and EV3 as different benchmark roles.",
                date="2026-01-15",
            ),
            {
                "toolName": "compare_competitor_set",
                "success": True,
                "rowCount": 2,
                "sourceType": "jato_parquet",
                "summary": "Competitor sales and configuration evidence.",
                "keyFindings": ["EX30 has stronger brand pull; EV3 is a price/configuration anchor."],
                "evidenceRefs": [
                    {"refId": "ex30_sales", "label": "EX30 sales volume", "value": 1518, "unit": "units", "source": "jato_parquet"},
                    {"refId": "ev3_config", "label": "EV3 configuration delta", "value": "price/config anchor", "source": "jato_parquet"},
                ],
            },
        ],
    )

    answer = apply_business_composer(
        {"title": "Competitor", "direct": "基于证据回答。", "answerStatus": "answered"},
        package,
        country="Sweden",
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    digest = answer["evidenceDigest"]
    assert digest[0] == "[R1] EX30 EV3 competitor review（example.com，2026-01-15）"
    assert any("EX30 sales volume = 1,518 units（jato_parquet）" == item for item in digest)
    joined = " ".join(digest)
    assert ".claim" not in joined
    assert "rank" not in joined.lower()


def test_competitor_compare_evidence_digest_prioritizes_requested_models() -> None:
    package = _pricing_package(
        intent="competitor_compare",
        entities={"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
        toolResults=[
            {
                "toolName": "compare_competitive_set",
                "success": True,
                "rowCount": 4,
                "sourceType": "jato_parquet",
                "summary": "Mixed competitor evidence.",
                "keyFindings": ["EX30 and EV3 are requested; XC60 and Model Y are background only."],
                "evidenceRefs": [
                    {"refId": "xc60_sales", "label": "XC60.sales", "value": 2893, "unit": "units", "source": "jato_cross_reference"},
                    {"refId": "modely_sales", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato_cross_reference"},
                    {"refId": "ex30_model", "label": "competitor.3.model", "value": "EX30", "source": "jato_cross_reference"},
                    {"refId": "ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                    {"refId": "ev3_role", "label": "EV3.configurationRole", "value": "price/config anchor", "source": "jato_cross_reference"},
                ],
            },
        ],
    )

    answer = apply_business_composer(
        {"title": "Competitor", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        package,
        country="Sweden",
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]}},
    )

    digest = answer["evidenceDigest"]
    joined = " ".join(digest)
    assert any(item == "EX30 销量 = 1,518 units（JATO 交叉引用）" for item in digest)
    assert any(item == "EV3.configurationRole = price/config anchor（JATO 交叉引用）" for item in digest)
    assert "jato_cross_reference" not in joined
    assert "XC60.sales" not in joined
    assert "EX30.sales" not in joined
    assert "MODEL Y.sales" not in joined
    assert "competitor.3.model" not in joined


def test_co2_phev_policy_answer_requires_tax_tco_and_usage_boundary() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "partially_answered"},
        _pricing_package(intent="news_policy_search", confidence="medium"),
        country="Sweden",
        question="CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        evidence_plan={"intent": "news_policy_search"},
    )

    direct = answer["direct"]
    joined = " ".join([
        direct,
        *answer["businessImplications"],
        *answer["reportReadyBullets"],
        *(item["action"] for item in answer["recommendedActions"]),
    ])
    assert "入场条件" in direct
    assert "不是自动利好" in direct
    assert "官方税率/benefit 公式" in direct
    assert "认证 CO2" in direct
    assert "真实充电行为" in direct
    assert "TCO 表" in direct
    assert "PHEV 先保留为公司车/TCO 验证线" in direct
    assert "核对 PHEV 认证 CO2、税率阶梯、company car 计算公式和发布日期" in joined
    assert "company car TCO 场景表" in joined
    assert "一定有利" not in direct
    assert len(direct) < 900


def test_market_overview_keeps_business_action_before_msrp_source_repair() -> None:
    answer = apply_business_composer(
        {"title": "Market", "direct": "基于证据回答。", "answerStatus": "answered"},
        {
            "evidenceId": "evpkg_market_j7_source_gap",
            "intent": "market_overview",
            "country": "Sweden",
            "entities": {
                "models": ["J7 HEV"],
                "competitors": ["Corolla Cross", "RAV4", "C-HR", "Qashqai"],
                "powertrains": ["HEV"],
            },
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "generated",
                    "summary": "J7 HEV market context.",
                    "evidenceRefs": [
                        {"refId": "ev_hev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 1946, "unit": "units", "source": "jato"},
                        {"refId": "ev_suva0_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 5416, "unit": "units", "source": "jato"},
                        {"refId": "ev_suva_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "summary": "No materialized current MSRP rows, but source candidates are available.",
                    "evidenceRefs": [
                        {"refId": "ev_price_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_msrp_postgres"},
                    ],
                    "coverageDiagnostics": {
                        "sourceRepairCandidates": {
                            "dataStatus": "source_candidates_available",
                            "missingOwnModelSource": True,
                            "ownModel": [
                                {
                                    "brand": "JAECOO",
                                    "model": "J7 HEV",
                                    "sourceCode": "j7-source-search",
                                    "draftStatus": "candidate_search_query",
                                }
                            ],
                            "competitorCorridor": [
                                {
                                    "brand": "Toyota",
                                    "model": "Corolla Cross",
                                    "sourceCode": "corolla-cross-source",
                                    "candidateSourceType": "source_draft",
                                    "materializationStatus": "ready_for_extraction",
                                    "currentPriceRows": 0,
                                },
                                {
                                    "brand": "Toyota",
                                    "model": "RAV4",
                                    "sourceCode": "rav4-source",
                                    "candidateSourceType": "source_draft",
                                    "materializationStatus": "ready_for_extraction",
                                    "currentPriceRows": 0,
                                },
                            ],
                            "candidateCount": 3,
                            "materializedCandidateCount": 0,
                        },
                    },
                },
            ],
            "missingEvidence": [],
            "confidence": "high",
            "jatoCrossCheck": {"status": "matched", "summary": "Internal market context is usable."},
            "insightCards": [],
        },
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？请用销量、SUV A0/A 结构、竞品和图表支持，给产品经理结论。",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]}},
    )

    actions = [item["action"] for item in answer["recommendedActions"]]
    assert actions[0].startswith("建立 J7 HEV vs Corolla Cross")
    assert "MSRP 来源验证表" not in actions[0]
    assert any("MSRP 来源验证表" in action for action in actions[1:])
    assert "下一步执行：建立 J7 HEV vs Corolla Cross" in answer["direct"]


def test_phev_leasing_answer_uses_tco_decision_frame() -> None:
    plan = build_business_synthesis_plan(
        answer={"direct": "基于证据回答。", "answerStatus": "answered"},
        evidence_package=_pricing_package(intent="pricing_analysis"),
        country="Sweden",
        question="大客户 leasing 场景下，PHEV 还有没有理由？",
        evidence_plan={"intent": "pricing_analysis"},
    )

    joined = " ".join([plan["executiveConclusion"], *plan["businessImplications"], *plan["reportReadyBullets"]])
    assert "TCO、月供、残值、税费" in joined
    assert "可油可电" in plan["executiveConclusion"]
    assert "瑞典大客户 leasing 场景" in plan["executiveConclusion"]
    assert "瑞典 大客户" not in joined
    assert plan["recommendedActions"][0]["action"] == "建立 PHEV fleet leasing TCO 表"


def test_phev_leasing_policy_news_uses_tco_decision_frame_when_sources_missing() -> None:
    answer = apply_business_composer(
        {"title": "PHEV leasing", "direct": "Insufficient evidence.", "answerStatus": "partially_answered"},
        _market_package(
            intent="news_policy_search",
            confidence="medium",
            missingEvidence=[
                {"name": "minimum_external_sources", "reason": "No external source date.", "impact": "blocking"},
                {
                    "name": "leasing_tco_or_company_car_evidence",
                    "reason": "Question requires monthly payment, residual value, tax benefit, or fleet TCO evidence.",
                    "impact": "weakens_answer",
                },
            ],
        ),
        country="Sweden",
        question="大客户 leasing 场景下，PHEV 还有没有理由？",
        evidence_plan={"intent": "news_policy_search"},
    )

    direct = answer["direct"]
    joined = " ".join([
        direct,
        *answer["businessImplications"],
        *answer["reportReadyBullets"],
        *(item["action"] for item in answer["recommendedActions"]),
    ])
    assert direct.startswith("直接结论：瑞典大客户 leasing 场景下 PHEV 仍可能有理由")
    assert "fleet/TCO 验证线" in direct
    assert "月供、残值/RV、税务 benefit、年里程、充电条件" in direct
    assert "如果这些口径算不出成本或使用风险优势，PHEV 就不应主推" in direct
    assert answer["recommendedActions"][0]["action"] == "建立 PHEV fleet leasing TCO 表"
    assert "验证 leasing/TCO/company-car 来源候选" in joined
    assert "政策/新闻分析必须先确认来源日期" not in direct
    assert "查政策原文" not in direct


def test_phev_leasing_direct_uses_channel_and_drive_evidence() -> None:
    answer = apply_business_composer(
        {"title": "PHEV leasing", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package(
            intent="pricing_analysis",
            confidence="high",
            toolResults=[
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "jato_parquet",
                    "summary": "PHEV fleet leasing context.",
                    "evidenceRefs": [
                        {"refId": "ev_phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 6498, "unit": "units", "source": "jato"},
                        {"refId": "ev_phev_bus", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_pri", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Private_pct", "value": 35.2, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_4wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.4WD_pct", "value": 68.0, "unit": "%", "source": "jato"},
                        {"refId": "ev_phev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.2WD_pct", "value": 31.8, "unit": "%", "source": "jato"},
                    ],
                }
            ],
        ),
        country="Sweden",
        question="大客户 leasing 场景下，PHEV 还有没有理由？",
        evidence_plan={"intent": "pricing_analysis"},
    )

    direct = answer["direct"]
    assert direct.startswith("fleet leasing 判断：瑞典 大客户 leasing 场景下 PHEV")
    assert "PHEV 6,498 units，Business 64.8%，Private 35.2%" in direct
    assert "4WD 68%、2WD 31.8%" in direct
    assert "PHEV fleet leasing TCO 表" in direct
    assert "如果月供/RV/税费算不出优势，就不应主推" in direct
    assert "定价逻辑应先验证目标车型的价格走廊" not in direct
    assert "Pricing evidence table" not in direct


def test_internal_weak_evidence_codes_are_hidden_from_visible_answer() -> None:
    answer = apply_answer_grounding_guard(
        {"title": "Market", "direct": "Based on available evidence.", "bullets": [], "limitations": []},
        _market_package(
            country="Hungary",
            toolResults=[],
            missingEvidence=[
                {
                    "name": "analyze_market_dynamics_weak_evidence_refs",
                    "reason": "analyze_market_dynamics weak evidence refs",
                    "impact": "weakens_answer",
                },
                {
                    "name": "query_country_snapshot_weak_evidence_refs",
                    "reason": "query_country_snapshot weak evidence refs",
                    "impact": "weakens_answer",
                },
                {
                    "name": "market_snapshot_data_unavailable",
                    "reason": "No Hungary HEV rows.",
                    "impact": "weakens_answer",
                },
            ],
            confidence="low",
        ),
        country="Hungary",
        question="匈牙利 HEV 市场现在适不适合 J7？",
        evidence_plan={"intent": "market_overview"},
    )

    visible_text = " ".join([
        answer["direct"],
        *answer["bullets"],
        *answer["limitations"],
        *answer["reportReadyBullets"],
    ])
    assert "市场动态证据不足" in visible_text
    assert "市场快照证据不足" in visible_text
    assert "内部市场快照、HEV 销量/份额和车型结构证据" in visible_text
    assert "weak_evidence_refs" not in visible_text
    assert "weak evidence refs" not in visible_text
    assert "market_snapshot_data_unavailable" not in visible_text


def test_direct_answer_is_compact_and_skips_report_title_label() -> None:
    answer = apply_business_composer(
        {"title": "Analysis", "direct": "基于证据回答。", "answerStatus": "answered"},
        _pricing_package_with_method_material(intent="report_generation"),
        country="Sweden",
        question="把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        evidence_plan={"intent": "report_generation", "entities": {"models": ["J7 HEV"]}},
    )

    assert "低配锚点和高配主推" in answer["direct"]
    assert answer["title"] == "瑞典 J7 HEV 验证版定价逻辑"
    assert "核心竞争带中段 + 高配主推" not in answer["title"]
    assert answer["direct"].startswith("已验证证据：")
    assert "瑞典 J7 HEV 定价页现在应先做成验证版" in answer["direct"]
    assert "已验证证据" in answer["direct"]
    assert "用户材料假设" in answer["direct"]
    assert "材料中的市场层面假设" in answer["direct"]
    assert "待验证竞品层面" in answer["direct"]
    assert "待验证配置层面" in answer["direct"]
    assert "因此，低配可先作为价格锚点、高配作为主推版本的待验证假设" in answer["direct"]
    assert "蒸馏出的定价假设" in answer["direct"]
    assert "不是当前官方 MSRP" in answer["direct"]
    assert "高低配价差为 高低配价差" not in answer["direct"]
    assert "PVA 覆盖约 高配 PVA" not in answer["direct"]
    assert "高低配价差 3,230 EUR" in answer["direct"]
    assert "全景天窗 等" not in answer["direct"]
    assert answer["direct"].count("30,000-40,000 EUR") == 1
    assert "Pricing corridor chart" not in answer["direct"]
    assert "Pricing evidence table" not in answer["direct"]
    assert "report block" in answer["displayPlan"]
    assert "Title：" not in answer["direct"]
    assert "下一步执行：" in answer["direct"]
    assert "下一步执行 " not in answer["direct"]
    assert len(answer["direct"]) < 1100
    assert len(answer["bullets"]) <= 5
    assert answer["bullets"][0].startswith("Title：")
    assert any(item.startswith("Key message：") for item in answer["bullets"])
    assert "Verdict：" not in " ".join(answer["bullets"])


def test_composer_preserves_grounded_provider_narrative_instead_of_replacing_it_with_playbook_text() -> None:
    provider_direct = (
        "瑞典的 BEV 已占 40.9%，HEV 仍有 7.3% 份额，因此 J7 HEV 的机会不在追随纯电增量，"
        "而在无稳定充电、价格敏感和低使用风险的 SUV 用户；下一步应该把这一入口与车型级价格和配置数据交叉验证。"
    )

    answer = apply_business_composer(
        {"title": "Market", "direct": provider_direct, "bullets": [], "limitations": [], "answerStatus": "answered"},
        _market_package(),
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7"]}},
    )

    assert answer["direct"] == provider_direct
    assert "市场总览的重点不是复述份额" not in answer["direct"]
    assert "证据状态" not in answer["direct"]
    assert answer["grounding"]["providerNarrativeStatus"] == "kept"


def test_composer_rejects_provider_numeric_claim_that_is_not_in_evidence() -> None:
    answer = apply_business_composer(
        {
            "title": "Market",
            "direct": "瑞典 HEV 的市场份额已经达到 99.9%，因此 J7 应立即以最高配进入市场，并用高价策略抢占所有用户。",
            "bullets": [],
            "limitations": [],
            "answerStatus": "answered",
        },
        _market_package(),
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7"]}},
    )

    assert "99.9%" not in answer["direct"]
    assert "HEV 份额 7.3%" in answer["direct"]
    assert answer["grounding"]["providerNarrativeStatus"].startswith("replaced:")
    assert "99.9%" in answer["grounding"]["providerNarrativePreview"]


def test_composer_rejects_provider_claim_that_market_data_is_empty_when_positive_evidence_exists() -> None:
    answer = apply_business_composer(
        {
            "title": "Market",
            "direct": "目前 JATO 系统中匈牙利 HEV 市场及 J7 车型的可用数据为空（0 行记录），无法计算市场规模或趋势。",
            "bullets": [],
            "limitations": [],
            "answerStatus": "answered",
        },
        _market_package(
            country="Hungary",
            toolResults=[
                {
                    "toolName": "query_segment_breakdown",
                    "success": True,
                    "rowCount": 2,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 2687, "unit": "units"},
                        {"label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 89.5, "unit": "%"},
                        {"label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 7303, "unit": "units"},
                    ],
                }
            ],
        ),
        country="Hungary",
        question="匈牙利 HEV 市场是否适合 J7？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7"], "powertrains": ["HEV"]}},
    )

    assert "数据为空" not in answer["direct"]
    assert "0 行记录" not in answer["direct"]
    assert "2,687 units" in answer["direct"]
    assert "7,303 units" in answer["direct"]
