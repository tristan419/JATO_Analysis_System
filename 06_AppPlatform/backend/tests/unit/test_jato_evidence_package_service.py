from __future__ import annotations

from app.services import jato_evidence_package_service as evidence
from app.services.jato_evidence_package_service import build_evidence_package


def test_time_series_evidence_keeps_latest_twelve_months() -> None:
    months = [
        {"period": f"2025-{month:02d}", "sales": month * 100}
        for month in range(1, 13)
    ] + [
        {"period": "2026-01", "sales": 1300},
        {"period": "2026-02", "sales": 1400},
    ]
    package = build_evidence_package(
        session_id="sess_latest_months",
        country="Hungary",
        question="匈牙利 HEV 最近月度走势如何？",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"countries": ["Hungary"], "powertrains": ["HEV"]},
            "requiredTools": ["query_time_series"],
            "evidenceNeeded": [{
                "name": "monthly_trend_series",
                "reason": "Need recent monthly trend.",
                "priority": 1,
            }],
        },
        tool_results=[{
            "toolName": "query_time_series",
            "query": {"country": "Hungary", "powertrain": "HEV"},
            "success": True,
            "result": {
                "tool": "query_time_series",
                "metadata": {"source": "jato_time_series", "country": "Hungary"},
                "data": {"country": "Hungary", "monthSeries": months},
            },
        }],
    )

    refs = package["toolResults"][0]["evidenceRefs"]
    labels = [str(ref["label"]) for ref in refs]

    assert len(refs) == 12
    assert labels[0] == "monthSeries.2025-03.sales"
    assert labels[-1] == "monthSeries.2026-02.sales"
    assert "monthSeries.2025-01.sales" not in labels
    assert "monthly_trend_series" not in {item["name"] for item in package["missingEvidence"]}


def test_evidence_package_keeps_requested_hev_when_powertrain_mix_would_be_truncated() -> None:
    package = build_evidence_package(
        session_id="sess_market_hev",
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={"intent": "market_overview", "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]}},
        tool_results=[
            {
                "toolName": "analyze_market_dynamics",
                "success": True,
                "result": {
                    "tool": "analyze_market_dynamics",
                    "source": "jato_cross_reference",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {
                        "dynamics": {
                            "marketSnapshot": {
                                "kpis": {
                                    "totalRows": 60000,
                                    "countryCount": 1,
                                    "brandCount": 40,
                                    "modelCount": 200,
                                    "versionCount": 1000,
                                    "cumulativeSales": 1182452,
                                    "avgMsrp": 42100,
                                },
                                "powertrainMix": [
                                    {"label": "BEV", "value": 25235},
                                    {"label": "PHEV", "value": 15028},
                                    {"label": "MHEV", "value": 8515},
                                    {"label": "ICE", "value": 8129},
                                    {"label": "HEV", "value": 5051},
                                    {"label": "REEV", "value": 2},
                                ],
                            }
                        }
                    },
                },
            }
        ],
    )

    refs = package["toolResults"][0]["evidenceRefs"]
    labels = [item["label"] for item in refs]

    assert "marketSnapshot.powertrainMix.HEV.value" in labels
    assert "marketSnapshot.powertrainMix.BEV.value" in labels
    assert labels.index("marketSnapshot.powertrainMix.HEV.value") < labels.index("marketSnapshot.powertrainMix.BEV.value")
    assert len(refs) <= 12


def test_evidence_package_does_not_auto_add_j7_user_material_for_plain_sweden_question() -> None:
    package = build_evidence_package(
        session_id="sess_sweden_j7_plain_market",
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]},
        },
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 1},
                    "data": {
                        "extractedParams": {"powertrain": "HEV"},
                        "contextSnapshot": {
                            "metricScopes": {
                                "powertrainMix": {
                                    "periodType": "ytd",
                                    "periodLabel": "2026 YTD（截至 2026-03）",
                                    "periodStart": "2026-01",
                                    "periodEnd": "2026-03",
                                },
                                "crossTabs": {
                                    "periodType": "month",
                                    "periodLabel": "2026-03 当月",
                                    "periodStart": "2026-03",
                                    "periodEnd": "2026-03",
                                },
                            },
                            "powertrainMix": [
                                {"label": "BEV", "value": 25235},
                                {"label": "PHEV", "value": 15028},
                                {"label": "HEV", "value": 5051},
                            ],
                        },
                    },
                },
            },
        ],
    )

    labels = [
        ref["label"]
        for tool in package["toolResults"]
        for ref in tool.get("evidenceRefs", [])
    ]
    missing = {item["name"] for item in package["missingEvidence"]}
    tool_names = [tool["toolName"] for tool in package["toolResults"]]

    assert "J7 HEV user material market window" not in labels
    assert "J7 HEV user material competitor pool" not in labels
    assert "J7 HEV user material competitor corridor" not in labels
    assert "business_method_material" not in tool_names
    assert "model_level_market_opportunity_evidence" in missing


def test_evidence_package_adds_j7_user_material_when_user_requests_material() -> None:
    package = build_evidence_package(
        session_id="sess_sweden_j7_market_material",
        country="Sweden",
        question="基于我给的 J7_HEV_V4 PPT 材料，瑞典 HEV 市场为什么适合 J7？",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]},
        },
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 1},
                    "data": {
                        "extractedParams": {"powertrain": "HEV"},
                        "contextSnapshot": {
                            "powertrainMix": [
                                {"label": "BEV", "value": 25235},
                                {"label": "PHEV", "value": 15028},
                                {"label": "HEV", "value": 5051},
                            ],
                        },
                    },
                },
            },
        ],
    )

    labels = [
        ref["label"]
        for tool in package["toolResults"]
        for ref in tool.get("evidenceRefs", [])
    ]
    missing = {item["name"] for item in package["missingEvidence"]}

    assert "J7 HEV user material market window" in labels
    assert "J7 HEV user material competitor pool" in labels
    assert "J7 HEV user material competitor corridor" in labels
    assert "model_level_market_opportunity_evidence" not in missing
    assert any(tool["toolName"] == "business_method_material" for tool in package["toolResults"])


def test_evidence_package_adds_scoped_j7_material_for_sweden_pricing_question() -> None:
    package = build_evidence_package(
        session_id="sess_sweden_j7_scoped_pricing",
        country="Sweden",
        question="基于瑞典市场、竞品格局和配置差异，J7 HEV 应该怎么定价？",
        evidence_plan={
            "intent": "pricing_analysis",
            "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]},
        },
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres", "country": "Sweden"},
                    "data": {"items": []},
                },
            },
        ],
    )

    labels = [
        ref["label"]
        for tool in package["toolResults"]
        for ref in tool.get("evidenceRefs", [])
    ]
    tool_names = [tool["toolName"] for tool in package["toolResults"]]
    method_tool = next(tool for tool in package["toolResults"] if tool["toolName"] == "business_method_material")

    assert "business_method_material" in tool_names
    assert method_tool["sourceType"] == "user_material"
    assert "J7 HEV user material main trim MSRP" in labels
    assert "J7 HEV user material price gap" in labels
    assert "J7 HEV user material PVA coverage" in labels
    assert all(ref.get("evidenceStatus") == "hypothesis" for ref in method_tool.get("evidenceRefs", []))


def test_evidence_package_builds_generic_method_material_refs_for_blind_model(monkeypatch) -> None:
    fake_method = {
        "model": "Aurora HEV",
        "market": "Hungary",
        "sourceName": "Aurora_HEV_Method.pdf",
        "deckTitle": "Aurora HEV pricing method",
        "priceCorridor": {
            "positioning": "核心带中段 + 高配主推",
            "coreCorridor": "31,000-38,000 EUR",
            "mainTrimPrice": "34,500 EUR",
        },
        "versionStrategy": {"priceGap": "2,600 EUR", "pvaCoverage": "110%"},
        "competitorPool": ["Helios One", "Vector HEV"],
        "pricingPlaybook": {"market_window": "Hungary HEV window requires live cross-check."},
        "featureValueClaims": [
            {"featureName": "HUD", "customerValue": "Visible high-trim value.", "businessUse": "Support trim value."},
        ],
        "coreClaims": ["Treat the material as a hypothesis."],
    }
    monkeypatch.setattr(evidence, "get_active_pricing_method", lambda **_: fake_method)

    package = build_evidence_package(
        session_id="sess_blind_method_material",
        country="Hungary",
        question="匈牙利 Aurora HEV 应该怎么定价？",
        evidence_plan={
            "intent": "pricing_analysis",
            "entities": {"countries": ["Hungary"], "models": ["Aurora HEV"], "powertrains": ["HEV"]},
        },
        tool_results=[],
    )

    method_tool = next(tool for tool in package["toolResults"] if tool["toolName"] == "business_method_material")
    refs = method_tool["evidenceRefs"]
    labels = [str(ref.get("label") or "") for ref in refs]
    assert "Aurora HEV user material main trim MSRP" in labels
    assert "Aurora HEV user material competitor corridor" in labels
    assert all("J7" not in label for label in labels)
    assert all(ref.get("sourceType") == "user_material" for ref in refs)
    assert all(ref.get("evidenceStatus") == "hypothesis" for ref in refs)
    assert all(ref.get("entityIds") == ["Aurora HEV"] for ref in refs)
    assert all(ref.get("country") == "Hungary" for ref in refs)
    claim_types = {str(ref.get("claimType") or "") for ref in refs}
    assert all(claim_types)
    assert {"pricing_positioning", "competitor_price_corridor", "main_trim_msrp_hypothesis"} <= claim_types
    missing_reason = " ".join(item["reason"] for item in package["missingEvidence"])
    assert "Aurora HEV pricing method is backed by user material" in missing_reason
    assert "J7 HEV pricing method" not in missing_reason


def test_evidence_package_does_not_add_sweden_j7_material_when_question_targets_hungary() -> None:
    package = build_evidence_package(
        session_id="sess_default_sweden_but_question_hungary",
        country="Sweden",
        question="匈牙利 J7 HEV 市场情况怎么样？请不要回答瑞典。",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"countries": ["Hungary"], "models": ["J7 HEV"], "powertrains": ["HEV"]},
        },
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "query": {"country": "Hungary"},
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot", "country": "Hungary"},
                    "data": {
                        "country": "Hungary",
                        "marketSnapshot": {
                            "kpis": {"cumulativeSales": 12000},
                            "powertrainMix": [{"label": "HEV", "value": 1200}],
                        },
                    },
                },
            },
        ],
    )

    tool_names = [tool["toolName"] for tool in package["toolResults"]]
    labels = [
        ref["label"]
        for tool in package["toolResults"]
        for ref in tool.get("evidenceRefs", [])
    ]

    assert "business_method_material" not in tool_names
    assert "J7 HEV user material market window" not in labels
    assert "J7 HEV user material competitor corridor" not in labels


def test_evidence_package_does_not_add_sweden_j7_material_for_hungary_pricing_question() -> None:
    package = build_evidence_package(
        session_id="sess_hungary_j7_pricing_no_sweden_material",
        country="Hungary",
        question="匈牙利 J7 HEV 应该怎么定价？请不要回答瑞典。",
        evidence_plan={
            "intent": "pricing_analysis",
            "entities": {"countries": ["Hungary"], "models": ["J7 HEV"], "powertrains": ["HEV"]},
        },
        tool_results=[
            {
                "toolName": "query_msrp_pricing",
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_postgres", "country": "Hungary"},
                    "data": {"items": []},
                },
            },
        ],
    )

    labels = [
        ref["label"]
        for tool in package["toolResults"]
        for ref in tool.get("evidenceRefs", [])
    ]
    tool_names = [tool["toolName"] for tool in package["toolResults"]]

    assert "business_method_material" not in tool_names
    assert "J7 HEV user material main trim MSRP" not in labels
    assert "J7 HEV user material competitor corridor" not in labels


def test_evidence_package_excludes_country_scoped_refs_when_tool_returns_wrong_country() -> None:
    package = build_evidence_package(
        session_id="sess_hungary_scope_guard",
        country="Hungary",
        question="匈牙利 HEV 市场为什么适合 J7？请不要回答瑞典。",
        evidence_plan={
            "intent": "market_overview",
            "requiredTools": ["query_country_snapshot"],
            "allowedTools": ["query_country_snapshot"],
            "mustHaveEvidence": ["market_kpis"],
            "entities": {"countries": ["Hungary"], "models": ["J7 HEV"], "powertrains": ["HEV"]},
        },
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

    tool = package["toolResults"][0]
    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert package["country"] == "Hungary"
    assert tool["success"] is False
    assert tool["evidenceRefs"] == []
    assert "country_scope_mismatch:Sweden!=requested:Hungary" in tool["keyFindings"]
    assert tool["coverageDiagnostics"]["diagnosis"] == "country_scope_mismatch"
    assert missing["missing_required_tool:query_country_snapshot"]["impact"] == "blocking"
    assert missing["coverage_diagnostic:country_scope_mismatch"]["impact"] == "blocking"
    assert "Sweden" in missing["coverage_diagnostic:country_scope_mismatch"]["reason"]
    assert "Hungary" in missing["coverage_diagnostic:country_scope_mismatch"]["reason"]
    assert "market_kpis" in missing
    assert package["confidence"] == "low"


def test_failed_required_external_tool_is_not_double_counted_as_missing_required_tool() -> None:
    package = build_evidence_package(
        session_id="sess_external_timeout",
        country="Sweden",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？请给出来源和一页汇报结构。",
        evidence_plan={
            "intent": "report_generation",
            "requiredTools": ["external_research", "query_country_snapshot"],
            "allowedTools": ["external_research", "query_country_snapshot"],
            "mustHaveEvidence": [],
            "entities": {"countries": ["Sweden"], "models": ["O5 BEV"]},
        },
        tool_results=[
            {
                "toolName": "external_research",
                "query": {"country": "Sweden", "topic": "BEV subsidy price cap"},
                "success": False,
                "error": "external_research timed out after 20s",
                "result": {},
            },
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Sweden"},
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot", "country": "Sweden"},
                    "data": {"marketSnapshot": {"kpis": {"cumulativeSales": 1182452}}},
                },
            },
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert "missing_required_tool:external_research" not in missing
    assert missing["external_research_failed"]["impact"] == "weakens_answer"
    assert package["toolResults"][0]["toolName"] == "external_research"
    assert package["toolResults"][0]["success"] is False


def test_policy_price_cap_msrp_gap_weakens_when_policy_or_market_evidence_exists() -> None:
    package = build_evidence_package(
        session_id="sess_policy_price_cap_msrp_gap",
        country="Sweden",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？请给出来源和一页汇报结构。",
        evidence_plan={
            "intent": "report_generation",
            "requiredTools": ["external_research", "query_msrp_pricing"],
            "allowedTools": ["external_research", "query_msrp_pricing"],
            "entities": {"countries": ["Sweden"], "models": ["O5 BEV"], "powertrains": ["BEV"]},
        },
        tool_results=[
            {
                "toolName": "external_research",
                "query": {"country": "Sweden", "topic": "BEV subsidy price cap"},
                "success": True,
                "result": {
                    "tool": "external_research",
                    "source": "jato_external_research_web",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "items": [
                            {
                                "title": "Sweden EV incentive 2026",
                                "url": "https://example.gov.se/ev-incentive-2026",
                                "source": "Government source",
                                "supportedClaim": "A proposed BEV incentive would use a price cap and private buyer eligibility criteria.",
                                "date": "2026-03-15",
                            }
                        ]
                    },
                },
            },
            {
                "toolName": "query_msrp_pricing",
                "query": {"country": "Sweden", "models": ["O5 BEV"]},
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_pricing", "country": "Sweden"},
                    "data": {
                        "items": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_requested_models",
                            "requestedModels": ["O5 BEV"],
                        },
                    },
                },
            },
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert missing["coverage_diagnostic:no_current_prices_for_requested_models"]["impact"] == "weakens_answer"
    assert package["confidence"] == "medium"


def test_evidence_package_filters_irrelevant_external_tool_rows() -> None:
    package = build_evidence_package(
        session_id="sess_test",
        country="Sweden",
        question="拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        evidence_plan={"intent": "voc_analysis", "entities": {}},
        tool_results=[
            {
                "toolName": "search_market_news",
                "success": True,
                "result": {
                    "tool": "search_market_news",
                    "source": "jato_web_search_service",
                    "metadata": {"source": "jato_web_search_service"},
                    "data": {
                        "summary": "Raw search summary includes mixed web results.",
                        "items": [
                            {
                                "label": "EVERY ANGLE of Sweden vs. Tunisia: How to Watch, TV Channel, Live Stream - FOX Sports",
                                "url": "https://www.foxsports.com/stories/soccer/sweden-vs-tunisia",
                                "source": "foxsports.com",
                                "snippet": "Sweden faces Tunisia at the FIFA World Cup. Terms of Use and Privacy Policy.",
                                "date": "2026-06-14",
                            },
                            {
                                "title": "Nordic EV owners discuss towing hook and winter tyre needs",
                                "url": "https://example.com/nordic-ev-towing-winter-tyres",
                                "source": "example.com",
                                "snippet": "Nordic EV owners discuss towing hook, roof load and winter tyre needs in daily use.",
                                "date": "2026-06-10",
                            },
                        ],
                        "researchGovernance": {
                            "rejectedSources": [
                                {
                                    "title": "EVERY ANGLE of Sweden vs Tunisia - FOX Sports",
                                    "url": "https://www.foxsports.com/stories/soccer/sweden-vs-tunisia",
                                    "source": "foxsports.com",
                                    "sourceCategory": "unknown",
                                    "reason": "low_question_relevance",
                                }
                            ],
                        },
                    },
                },
            }
        ],
    )

    tool = package["toolResults"][0]
    visible = " ".join([
        tool.get("summary", ""),
        " ".join(tool.get("keyFindings", [])),
        str(tool.get("evidenceRefs", [])),
    ]).lower()

    assert tool["rowCount"] == 1
    assert "fifa" not in visible
    assert "tunisia" not in visible
    assert "foxsports" not in visible
    assert "foxsports" not in str(package).lower()
    assert package["researchGovernance"]["rejectedSourceCount"] == 1
    assert package["researchGovernance"]["rejectedSources"] == []
    assert "towing" in visible
    assert tool["coverageDiagnostics"]["externalRowsReturned"] == 2
    assert tool["coverageDiagnostics"]["externalRowsKept"] == 1
    assert tool["coverageDiagnostics"]["externalRowsFiltered"] == 1


def test_evidence_package_filters_irrelevant_external_refs_after_extraction() -> None:
    package = build_evidence_package(
        session_id="sess_test",
        country="Sweden",
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        evidence_plan={"intent": "voc_analysis", "entities": {}},
        tool_results=[
            {
                "toolName": "search_market_news",
                "success": True,
                "result": {
                    "tool": "search_market_news",
                    "source": "jato_web_search_service",
                    "metadata": {"source": "jato_web_search_service"},
                    "data": {
                        "kpis": {
                            "EVERY ANGLE of Sweden vs Tunisia 2026 FIFA World Cup - FOX Sports.claim": (
                                "FIFA World Cup highlights from Sweden vs Tunisia."
                            )
                        }
                    },
                },
            }
        ],
    )

    tool = package["toolResults"][0]
    visible = " ".join([
        tool.get("summary", ""),
        " ".join(tool.get("keyFindings", [])),
        str(tool.get("evidenceRefs", [])),
    ]).lower()

    assert tool["rowCount"] == 1
    assert tool["evidenceRefs"] == [
        {
            "refId": "ev_1_1",
            "label": "row_count",
            "value": 1,
            "source": "jato_web_search_service",
            "table": "jato_web_search_service",
            "rowCount": 1,
            "retrievedAt": tool["freshness"],
        }
    ]
    assert "fifa" not in visible
    assert "tunisia" not in visible
    assert "fox" not in visible
    assert package["confidence"] == "low"
    assert any(item["name"] == "external_research_claims_unavailable" for item in package["missingEvidence"])


def test_evidence_package_promotes_real_external_source_to_claim_refs() -> None:
    package = build_evidence_package(
        session_id="sess_external_v2h",
        country="Sweden",
        question="V2H 对瑞典用户是不是购车决策因素？请带来源。",
        evidence_plan={
            "intent": "voc_analysis",
            "entities": {},
            "mustHaveEvidence": ["external_source", "published_date"],
        },
        tool_results=[
            {
                "toolName": "external_research",
                "success": True,
                "result": {
                    "tool": "external_research",
                    "source": "jato_external_research_web",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "items": [
                            {
                                "title": "Swedish EV owners discuss V2H and home energy backup",
                                "url": "https://example.com/sweden-v2h-owner-study",
                                "source": "Example EV Research",
                                "snippet": (
                                    "Swedish EV owners mention V2H mainly as a home backup and energy flexibility "
                                    "feature, but purchase influence depends on charger support and electricity pricing."
                                ),
                                "publishedAt": "2026-05-12",
                            }
                        ],
                    },
                },
            }
        ],
    )

    refs = package["toolResults"][0]["evidenceRefs"]
    labels = [ref["label"] for ref in refs]
    values = [str(ref.get("value") or "") for ref in refs]

    assert any(label.endswith(".source") for label in labels)
    assert any(label.endswith(".claim") for label in labels)
    assert any(label.endswith(".date") for label in labels)
    assert any("V2H mainly as a home backup" in value for value in values)
    assert not any(
        item["name"] == "external_research_claims_unavailable"
        for item in package["missingEvidence"]
    )
    assert package["confidence"] == "medium"


def test_evidence_package_does_not_treat_external_search_candidates_as_claim_evidence() -> None:
    package = build_evidence_package(
        session_id="sess_external_candidates_only",
        country="Sweden",
        question="拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        evidence_plan={
            "intent": "voc_analysis",
            "entities": {},
            "mustHaveEvidence": ["external_source"],
        },
        tool_results=[
            {
                "toolName": "external_research",
                "success": True,
                "result": {
                    "tool": "external_research",
                    "source": "jato_external_research_web",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "dataStatus": "external_research_query_candidates",
                        "sourceRepairCandidates": {
                            "dataStatus": "external_research_query_candidates",
                            "candidateCount": 1,
                            "materializedCandidateCount": 0,
                            "competitorCorridor": [
                                {
                                    "sourceCode": "voc-source-sweden-1",
                                    "model": "Sweden tow hook roof load winter tire owner forum",
                                    "sourceUrl": (
                                        "https://www.google.com/search?q=Sweden+SUV+tow+hook+roof+load+winter+tires"
                                    ),
                                    "draftStatus": "candidate_search_query",
                                }
                            ],
                        },
                    },
                },
            }
        ],
    )

    refs = package["toolResults"][0]["evidenceRefs"]

    assert [ref["label"] for ref in refs] == ["row_count"]
    assert any(
        item["name"] == "external_research_claims_unavailable"
        for item in package["missingEvidence"]
    )
    assert package["confidence"] == "low"


def test_evidence_package_marks_named_policy_year_gap_when_only_generic_policy_context_found() -> None:
    package = build_evidence_package(
        session_id="sess_elbil",
        country="Sweden",
        question="Elbilspremien 2026 会影响哪些车型？",
        evidence_plan={
            "intent": "news_policy_search",
            "entities": {},
            "mustHaveEvidence": ["official_source", "published_date", "policy_effect"],
        },
        tool_results=[
            {
                "toolName": "external_research",
                "success": True,
                "result": {
                    "tool": "external_research",
                    "source": "jato_external_research_web",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "items": [
                            {
                                "title": "Bonus - for low emission vehicles has ended - Transportstyrelsen",
                                "url": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                                "source": "Transportstyrelsen",
                                "snippet": "Official Swedish vehicle tax context for low emission vehicle bonus.",
                                "supportedClaim": "The bonus for low emission vehicles has ended.",
                                "date": "2026-04-17",
                            },
                            {
                                "title": "Malus - for high emission vehicles - Transportstyrelsen",
                                "url": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/malus/",
                                "source": "Transportstyrelsen",
                                "snippet": "Official Swedish malus tax context for high emission vehicles.",
                                "supportedClaim": "Malus applies to high emission vehicles.",
                                "date": "2026-02-25",
                            },
                        ],
                    },
                },
            }
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}
    assert "target_policy_source:elbilspremien_2026" in missing
    assert missing["target_policy_source:elbilspremien_2026"]["impact"] == "blocking"
    assert package["confidence"] == "low"


def test_evidence_package_does_not_mark_named_policy_gap_when_target_source_matches() -> None:
    package = build_evidence_package(
        session_id="sess_elbil",
        country="Sweden",
        question="Elbilspremien 2026 会影响哪些车型？",
        evidence_plan={
            "intent": "news_policy_search",
            "entities": {},
            "mustHaveEvidence": ["official_source", "published_date", "policy_effect"],
        },
        tool_results=[
            {
                "toolName": "external_research",
                "success": True,
                "result": {
                    "tool": "external_research",
                    "source": "jato_external_research_web",
                    "metadata": {"source": "jato_external_research_web"},
                    "data": {
                        "items": [
                            {
                                "title": "Elbilspremien 2026 official eligibility rules",
                                "url": "https://example.se/elbilspremien-2026",
                                "source": "Swedish policy source",
                                "snippet": "Elbilspremien 2026 eligibility, price cap and vehicle scope.",
                                "supportedClaim": "Elbilspremien 2026 applies to eligible BEV models under the stated cap.",
                                "date": "2026-03-01",
                            }
                        ],
                    },
                },
            }
        ],
    )

    assert not any(
        item["name"] == "target_policy_source:elbilspremien_2026"
        for item in package["missingEvidence"]
    )


def test_evidence_package_marks_empty_variant_matrix_as_configuration_gap() -> None:
    package = build_evidence_package(
        session_id="sess_empty_variant_matrix",
        country="Sweden",
        question="J8 7座四驱为什么能打 Sorento？",
        evidence_plan={
            "intent": "competitor_compare",
            "entities": {"models": ["J8", "Sorento"]},
            "mustHaveEvidence": ["configuration_delta"],
        },
        tool_results=[
            {
                "toolName": "compare_vehicle_variants",
                "query": {"country": "Sweden", "models": ["J8", "Sorento"]},
                "success": True,
                "result": {
                    "tool": "compare_vehicle_variants",
                    "metadata": {"source": "jato_variant_diff_service"},
                    "data": {
                        "country": "Sweden",
                        "queryModels": ["J8", "Sorento"],
                        "subjects": [],
                        "differentFeatures": [],
                        "commonFeatures": [],
                        "selectionNotes": [],
                    },
                },
            }
        ],
    )

    tool = package["toolResults"][0]
    missing_names = {item["name"] for item in package["missingEvidence"]}

    assert tool["rowCount"] == 0
    assert tool["evidenceRefs"] == []
    assert "no variant/configuration matrix rows" in tool["summary"]
    assert any(str(item).startswith("variant_matrix_unavailable:") for item in tool["keyFindings"])
    assert "competitive_or_configuration_data_unavailable" in missing_names
    assert "configuration_delta" in missing_names
    assert package["confidence"] == "low"


def test_evidence_package_extracts_market_chart_cross_tabs_when_snapshot_kpis_are_empty() -> None:
    package = build_evidence_package(
        session_id="sess_hungary",
        country="Hungary",
        question="匈牙利 HEV 市场机会？",
        evidence_plan={"intent": "market_overview", "entities": {}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Hungary", "chartCount": 0},
                    "data": {
                        "extractedParams": {"powertrain": "HEV"},
                        "contextSnapshot": {
                            "kpis": {
                                "totalRows": 0,
                                "countryCount": 0,
                                "brandCount": 0,
                                "modelCount": 0,
                            },
                            "topModels": [],
                            "powertrainMix": [],
                            "crossTabs": {
                                "driveByFuel": [
                                    {"_index": "HEV", "_total": 2687, "2WD_pct": 89.5, "4WD_pct": 9.9},
                                    {"_index": "PHEV", "_total": 969, "2WD_pct": 52.0, "4WD_pct": 46.9},
                                ],
                                "driveBySegment": [
                                    {"_index": "SUV A0", "_total": 7303, "2WD_pct": 88.1, "4WD_pct": 11.9},
                                    {"_index": "SUV A", "_total": 3535, "2WD_pct": 69.0, "4WD_pct": 30.0},
                                ],
                            },
                        }
                    },
                },
            }
        ],
    )

    tool = package["toolResults"][0]
    labels = [ref["label"] for ref in tool["evidenceRefs"]]
    assert labels[0] == "contextSnapshot.crossTabs.driveByFuel.HEV.sales"
    assert "contextSnapshot.crossTabs.driveByFuel.HEV.sales" in labels
    assert "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct" in labels
    assert "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales" in labels
    assert not any(item["name"] == "market_snapshot_data_unavailable" for item in package["missingEvidence"])


def test_evidence_package_prioritizes_cross_tabs_for_powertrain_pricing_questions() -> None:
    package = build_evidence_package(
        session_id="sess_sweden_j7_sportage_pricing",
        country="Sweden",
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        evidence_plan={"intent": "pricing_analysis", "entities": {"models": ["J7 HEV", "Sportage HEV"], "powertrains": ["HEV"]}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 1},
                    "data": {
                        "contextSnapshot": {
                            "metricScopes": {
                                "powertrainMix": {
                                    "periodType": "ytd",
                                    "periodLabel": "2026 YTD（截至 2026-03）",
                                    "periodStart": "2026-01",
                                    "periodEnd": "2026-03",
                                },
                                "crossTabs": {
                                    "periodType": "month",
                                    "periodLabel": "2026-03 当月",
                                    "periodStart": "2026-03",
                                    "periodEnd": "2026-03",
                                },
                            },
                            "powertrainMix": [
                                {"label": "BEV", "value": 25235},
                                {"label": "PHEV", "value": 15028},
                                {"label": "HEV", "value": 5051},
                            ],
                            "yearSeries": [
                                {"year": "2024", "value": 269580},
                                {"year": "2025", "value": 272998},
                            ],
                            "crossTabs": {
                                "driveByFuel": [
                                    {"_index": "HEV", "_total": 1946, "2WD_pct": 85.9, "4WD_pct": 14.1},
                                    {"_index": "BEV", "_total": 10875, "2WD_pct": 51.0, "4WD_pct": 48.3},
                                ],
                                "registrationByFuel": [
                                    {"_index": "HEV", "_total": 1946, "Business_pct": 54.0, "Private_pct": 46.0},
                                    {"_index": "BEV", "_total": 10875, "Business_pct": 60.3, "Private_pct": 39.7},
                                ],
                                "driveBySegment": [
                                    {"_index": "SUV A0", "_total": 5416, "2WD_pct": 85.2, "4WD_pct": 14.8},
                                    {"_index": "SUV A", "_total": 7544, "2WD_pct": 39.9, "4WD_pct": 60.1},
                                ],
                                "segmentByFuel": [
                                    {"_index": "SUV A0", "_total": 5416, "HEV_pct": 13.9, "BEV_pct": 45.6},
                                    {"_index": "SUV A", "_total": 7544, "HEV_pct": 5.4, "BEV_pct": 40.0},
                                ],
                            },
                        },
                    },
                },
            }
        ],
    )

    labels = [ref["label"] for ref in package["toolResults"][0]["evidenceRefs"]]
    assert labels[1] == "contextSnapshot.crossTabs.driveByFuel.HEV.sales"
    assert "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct" in labels
    assert "contextSnapshot.crossTabs.registrationByFuel.HEV.Business_pct" in labels
    assert "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales" in labels
    assert "contextSnapshot.yearSeries.2024.value" not in labels[:8]
    refs = package["toolResults"][0]["evidenceRefs"]
    month_ref = next(ref for ref in refs if ref["label"] == "contextSnapshot.crossTabs.driveByFuel.HEV.sales")
    ytd_ref = next(ref for ref in refs if ref["label"] == "contextSnapshot.powertrainMix.HEV.sales")
    assert month_ref["periodType"] == "month"
    assert month_ref["periodLabel"] == "2026-03 当月"
    assert ytd_ref["periodType"] == "ytd"
    assert ytd_ref["periodLabel"] == "2026 YTD（截至 2026-03）"
    assert package["scopeDiagnostics"]["hasBlockingConflict"] is False
    assert package["scopeDiagnostics"]["parallelScopes"][0]["metric"] == "powertrain:HEV:sales"


def test_evidence_package_blocks_same_scope_metric_conflicts() -> None:
    package = build_evidence_package(
        session_id="sess_scope_conflict",
        country="Sweden",
        question="瑞典 HEV 市场规模是多少？",
        evidence_plan={"intent": "market_overview", "entities": {"powertrains": ["HEV"]}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden"},
                    "data": {
                        "contextSnapshot": {
                            "metricScopes": {
                                "powertrainMix": {
                                    "periodType": "month",
                                    "periodLabel": "2026-03 当月",
                                    "periodStart": "2026-03",
                                    "periodEnd": "2026-03",
                                },
                                "crossTabs": {
                                    "periodType": "month",
                                    "periodLabel": "2026-03 当月",
                                    "periodStart": "2026-03",
                                    "periodEnd": "2026-03",
                                },
                            },
                            "powertrainMix": [{"label": "HEV", "value": 5051}],
                            "crossTabs": {
                                "driveByFuel": [{"_index": "HEV", "_total": 1946, "2WD_pct": 85.9}],
                            },
                        },
                    },
                },
            },
        ],
    )

    assert package["scopeDiagnostics"]["hasBlockingConflict"] is True
    assert package["scopeDiagnostics"]["conflicts"][0]["values"] == [5051, 1946]
    assert any(
        item["name"] == "evidence_scope_conflict:powertrain:HEV:sales"
        and item["impact"] == "blocking"
        for item in package["missingEvidence"]
    )
    assert package["confidence"] == "low"


def test_evidence_package_prioritizes_segment_cross_tabs_for_suv_structure_questions() -> None:
    package = build_evidence_package(
        session_id="sess_sweden_suv_segments",
        country="Sweden",
        question="SUV A0/A 级为什么是主销结构？",
        evidence_plan={"intent": "market_overview", "entities": {"segments": ["SUV A0", "SUV A"]}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 1},
                    "data": {
                        "contextSnapshot": {
                            "powertrainMix": [
                                {"label": "BEV", "value": 10875, "share": 40.9},
                                {"label": "PHEV", "value": 6498, "share": 24.4},
                            ],
                            "crossTabs": {
                                "driveBySegment": [
                                    {"_index": "SUV A", "_total": 7544, "4WD_pct": 60.1, "2WD_pct": 39.9},
                                    {"_index": "SUV A0", "_total": 5416, "4WD_pct": 14.8, "2WD_pct": 85.2},
                                ],
                                "segmentByFuel": [
                                    {"_index": "SUV A", "_total": 7544, "BEV_pct": 40.0, "PHEV_pct": 38.2},
                                    {"_index": "SUV A0", "_total": 5416, "BEV_pct": 45.6, "PHEV_pct": 5.7},
                                ],
                                "registrationBySegment": [
                                    {"_index": "SUV A", "_total": 7544, "Business_pct": 60.3, "Private_pct": 39.7},
                                    {"_index": "SUV A0", "_total": 5416, "Business_pct": 48.4, "Private_pct": 51.6},
                                ],
                            },
                        }
                    },
                },
            }
        ],
    )

    labels = [ref["label"] for ref in package["toolResults"][0]["evidenceRefs"] if ref["label"] != "row_count"]

    assert labels[0] == "contextSnapshot.crossTabs.driveBySegment.SUV A.sales"
    assert "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales" in labels
    assert "contextSnapshot.crossTabs.segmentByFuel.SUV A.BEV_pct" in labels
    assert "contextSnapshot.crossTabs.segmentByFuel.SUV A0.BEV_pct" in labels
    if "contextSnapshot.powertrainMix.BEV.sales" in labels:
        assert labels.index("contextSnapshot.crossTabs.driveBySegment.SUV A.sales") < labels.index(
            "contextSnapshot.powertrainMix.BEV.sales"
        )
    assert not any(item["name"] == "market_snapshot_data_unavailable" for item in package["missingEvidence"])


def test_evidence_package_prioritizes_large_suv_competitor_context_tabs() -> None:
    package = build_evidence_package(
        session_id="sess_j8_sorento_context",
        country="Sweden",
        question="J8 7 座四驱为什么能打 Sorento？",
        evidence_plan={"intent": "competitor_compare", "entities": {"models": ["J8", "Sorento"]}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 1},
                    "data": {
                        "contextSnapshot": {
                            "suvB": {
                                "totalRanking": {
                                    "items": [
                                        {
                                            "model": "Sorento",
                                            "volume": 309.0,
                                            "sharePct": 0.0332365279,
                                            "fuelMix": {"PHEV": 309.0, "BEV": 0.0, "HEV": 0.0},
                                            "driveMix": {"2WD": 0.0, "4WD": 309.0},
                                            "registrationMix": {"Business": 152.0, "Private": 157.0},
                                            "driveSharePct": 1.0,
                                            "yoy": {"value": 0.8502994012},
                                        }
                                    ]
                                }
                            },
                            "powertrainMix": [
                                {"label": "BEV", "value": 25235, "share": 42.0},
                                {"label": "PHEV", "value": 15028, "share": 25.0},
                            ],
                            "yearSeries": [
                                {"label": "2025", "value": 272998},
                            ],
                            "crossTabs": {
                                "driveBySegment": [
                                    {"_index": "SUV A", "_total": 7544, "4WD_pct": 60.1, "2WD_pct": 39.9},
                                    {"_index": "SUV B", "_total": 2800, "4WD_pct": 65.9, "2WD_pct": 34.1},
                                ],
                                "registrationByFuel": [
                                    {"_index": "BEV", "_total": 25235, "Business_pct": 60.3, "Private_pct": 39.7},
                                    {"_index": "PHEV", "_total": 15028, "Business_pct": 64.8, "Private_pct": 35.2},
                                ],
                                "segmentByFuel": [
                                    {"_index": "SUV A", "_total": 7544, "BEV_pct": 40.0, "PHEV_pct": 38.2},
                                    {"_index": "SUV B", "_total": 2800, "BEV_pct": 12.0, "PHEV_pct": 21.1},
                                ],
                                "registrationBySegment": [
                                    {"_index": "SUV A", "_total": 7544, "Business_pct": 60.3, "Private_pct": 39.7},
                                ],
                            },
                        }
                    },
                },
            }
        ],
    )

    labels = [ref["label"] for ref in package["toolResults"][0]["evidenceRefs"] if ref["label"] != "row_count"]

    assert labels[0] == "Sorento.sales"
    assert "Sorento.segment" in labels
    assert "Sorento.powertrain" in labels
    assert "Sorento.4WD_sales" in labels
    assert "Sorento.Business_sales" in labels
    assert "contextSnapshot.crossTabs.driveBySegment.SUV B.sales" in labels
    assert "contextSnapshot.crossTabs.driveBySegment.SUV B.4WD_pct" in labels
    assert "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct" in labels
    assert "contextSnapshot.crossTabs.segmentByFuel.SUV B.PHEV_pct" in labels
    assert "contextSnapshot.powertrainMix.BEV.sales" not in labels[:4]


def test_evidence_package_prioritizes_channel_cross_tabs_for_company_car_questions() -> None:
    package = build_evidence_package(
        session_id="sess_sweden_company_car",
        country="Sweden",
        question="瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        evidence_plan={"intent": "news_policy_search", "entities": {}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 1},
                    "data": {
                        "extractedParams": {"powertrain": "BEV"},
                        "contextSnapshot": {
                            "powertrainMix": [
                                {"label": "BEV", "value": 25235, "share": 42.0},
                                {"label": "PHEV", "value": 15028, "share": 25.0},
                            ],
                            "crossTabs": {
                                "registrationByFuel": [
                                    {"_index": "BEV", "_total": 25235, "Business_pct": 60.3, "Private_pct": 39.7},
                                    {"_index": "PHEV", "_total": 15028, "Business_pct": 64.8, "Private_pct": 35.2},
                                ],
                                "driveByFuel": [
                                    {"_index": "BEV", "_total": 25235, "4WD_pct": 50.0},
                                    {"_index": "PHEV", "_total": 15028, "4WD_pct": 68.0},
                                ],
                            },
                        }
                    },
                },
            }
        ],
    )

    labels = [ref["label"] for ref in package["toolResults"][0]["evidenceRefs"] if ref["label"] != "row_count"]

    assert labels[0] == "contextSnapshot.crossTabs.registrationByFuel.BEV.sales"
    assert "contextSnapshot.crossTabs.registrationByFuel.BEV.Business_pct" in labels
    assert "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct" in labels
    assert "contextSnapshot.powertrainMix.BEV.sales" in labels


def test_evidence_package_prioritizes_channel_cross_tabs_for_phev_co2_tax_questions() -> None:
    package = build_evidence_package(
        session_id="sess_sweden_phev_co2",
        country="Sweden",
        question="CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        evidence_plan={"intent": "news_policy_search", "entities": {"powertrains": ["PHEV"]}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 1},
                    "data": {
                        "extractedParams": {"powertrain": "PHEV"},
                        "contextSnapshot": {
                            "powertrainMix": [
                                {"label": "BEV", "value": 25235, "share": 42.0},
                                {"label": "PHEV", "value": 15028, "share": 25.0},
                            ],
                            "crossTabs": {
                                "registrationByFuel": [
                                    {"_index": "BEV", "_total": 25235, "Business_pct": 60.3, "Private_pct": 39.7},
                                    {"_index": "PHEV", "_total": 15028, "Business_pct": 64.8, "Private_pct": 35.2},
                                ],
                                "driveByFuel": [
                                    {"_index": "PHEV", "_total": 15028, "4WD_pct": 68.0, "2WD_pct": 31.8},
                                ],
                            },
                        }
                    },
                },
            }
        ],
    )

    labels = [ref["label"] for ref in package["toolResults"][0]["evidenceRefs"] if ref["label"] != "row_count"]

    assert labels[0] == "contextSnapshot.crossTabs.registrationByFuel.PHEV.sales"
    assert "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct" in labels
    assert "contextSnapshot.crossTabs.registrationByFuel.PHEV.Private_pct" in labels
    assert "contextSnapshot.crossTabs.registrationByFuel.BEV.Business_pct" in labels
    assert "contextSnapshot.crossTabs.driveByFuel.PHEV.4WD_pct" in labels


def test_evidence_package_prioritizes_powertrain_and_trend_refs_for_bev_report_chart() -> None:
    package = build_evidence_package(
        session_id="sess_sweden_report",
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan={"intent": "report_generation", "entities": {}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 3},
                    "data": {
                        "contextSnapshot": {
                            "kpis": {
                                "totalRows": 33327,
                                "brandCount": 79,
                                "modelCount": 539,
                                "versionCount": 9204,
                                "cumulativeSales": 1182452,
                            },
                            "powertrainMix": [
                                {"label": "BEV", "value": 10875, "share": 40.9},
                                {"label": "PHEV", "value": 6498, "share": 24.5},
                                {"label": "HEV", "value": 1946, "share": 7.3},
                            ],
                            "yearSeries": [
                                {"year": "2023", "bevShare": 33.0},
                                {"year": "2024", "bevShare": 38.0},
                                {"year": "2025", "bevShare": 40.9},
                            ],
                            "topModels": [
                                {"label": "EX40", "value": 2945},
                                {"label": "MODEL Y", "value": 2412},
                            ],
                        }
                    },
                },
            },
            {
                "toolName": "external_research",
                "success": True,
                "result": {
                    "tool": "external_research",
                    "metadata": {"source": "jato_web_search_service"},
                    "data": {"items": [], "sourceCoverage": {"sourceCount": 0}},
                },
            },
        ],
    )

    labels = [ref["label"] for ref in package["toolResults"][0]["evidenceRefs"] if ref["label"] != "row_count"]
    assert labels[0] == "contextSnapshot.powertrainMix.BEV.sales"
    assert "contextSnapshot.powertrainMix.BEV.share" in labels
    assert "contextSnapshot.powertrainMix.PHEV.sales" in labels
    assert "contextSnapshot.powertrainMix.HEV.share" in labels
    assert "contextSnapshot.yearSeries.2025.bevShare" in labels
    assert "contextSnapshot.topModels.EX40.sales" in labels
    assert "contextSnapshot.kpis.totalRows" not in labels
    assert not any(
        item["name"] == "external_research_claims_unavailable"
        for item in package["missingEvidence"]
    )


def test_evidence_package_uses_time_as_year_series_label_before_technical_label() -> None:
    package = build_evidence_package(
        session_id="sess_sweden_report_time_series",
        country="Sweden",
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        evidence_plan={"intent": "report_generation", "entities": {}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden", "chartCount": 1},
                    "data": {
                        "contextSnapshot": {
                            "powertrainMix": [
                                {"label": "BEV", "value": 10875, "share": 40.9},
                                {"label": "PHEV", "value": 6498, "share": 24.4},
                            ],
                            "yearSeries": [
                                {"label": "contextSnapshot.yearSeries_1", "time": "2024", "value": 269580},
                                {"label": "contextSnapshot.yearSeries_2", "time": "2025", "value": 272998},
                            ],
                        }
                    },
                },
            }
        ],
    )

    labels = [ref["label"] for ref in package["toolResults"][0]["evidenceRefs"] if ref["label"] != "row_count"]

    assert "contextSnapshot.yearSeries.2024.value" in labels
    assert "contextSnapshot.yearSeries.2025.value" in labels
    assert not any("yearSeries_1" in label for label in labels)


def test_evidence_package_requires_external_claims_for_policy_report_questions() -> None:
    package = build_evidence_package(
        session_id="sess_policy_report",
        country="Sweden",
        question="把瑞典 BEV 政策新闻影响转成一页带来源的汇报。",
        evidence_plan={"intent": "report_generation", "entities": {}},
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Sweden"},
                    "data": {
                        "contextSnapshot": {
                            "powertrainMix": [{"label": "BEV", "value": 10875, "share": 40.9}],
                        }
                    },
                },
            },
            {
                "toolName": "external_research",
                "success": True,
                "result": {
                    "tool": "external_research",
                    "metadata": {"source": "jato_web_search_service"},
                    "data": {"items": [], "sourceCoverage": {"sourceCount": 0}},
                },
            },
        ],
    )

    assert any(
        item["name"] == "external_research_claims_unavailable"
        for item in package["missingEvidence"]
    )


def test_evidence_package_does_not_mark_market_snapshot_missing_when_chart_cross_tabs_cover_it() -> None:
    package = build_evidence_package(
        session_id="sess_hungary",
        country="Hungary",
        question="匈牙利 HEV 市场机会？",
        evidence_plan={"intent": "market_overview", "entities": {"powertrains": ["HEV"]}},
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "result": {
                    "tool": "query_country_snapshot",
                    "metadata": {"source": "jato_country_snapshot", "country": "Hungary"},
                    "data": {
                        "kpis": {
                            "totalRows": 0,
                            "countryCount": 0,
                            "brandCount": 0,
                            "modelCount": 0,
                            "versionCount": 0,
                        }
                    },
                },
            },
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Hungary", "chartCount": 0},
                    "data": {
                        "extractedParams": {"powertrain": "HEV"},
                        "contextSnapshot": {
                            "crossTabs": {
                                "driveByFuel": [
                                    {"_index": "HEV", "_total": 2687, "2WD_pct": 89.5, "4WD_pct": 9.9},
                                ],
                                "driveBySegment": [
                                    {"_index": "SUV A0", "_total": 7303, "2WD_pct": 88.1, "4WD_pct": 11.9},
                                ],
                            },
                        },
                    },
                },
            },
        ],
    )

    assert not any(item["name"] == "market_snapshot_data_unavailable" for item in package["missingEvidence"])
    assert any(
        ref["label"] == "contextSnapshot.crossTabs.driveByFuel.HEV.sales"
        for tool in package["toolResults"]
        for ref in tool["evidenceRefs"]
    )


def test_market_overview_price_country_gap_weakens_when_market_evidence_exists() -> None:
    package = build_evidence_package(
        session_id="sess_hungary_hev_market_price_gap",
        country="Hungary",
        question="匈牙利 HEV 市场机会是什么？请给出数据支撑和图表。",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"powertrains": ["HEV"]},
            "requiredTools": ["build_market_chart", "query_msrp_pricing"],
        },
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Hungary", "chartCount": 1},
                    "data": {
                        "contextSnapshot": {
                            "crossTabs": {
                                "driveByFuel": [
                                    {"_index": "HEV", "_total": 2687, "2WD_pct": 89.5, "4WD_pct": 9.9},
                                ],
                                "driveBySegment": [
                                    {"_index": "SUV A0", "_total": 7303, "2WD_pct": 88.1, "4WD_pct": 11.9},
                                ],
                            },
                        },
                    },
                },
            },
            {
                "toolName": "query_msrp_pricing",
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_pricing", "country": "Hungary"},
                    "data": {
                        "items": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_country",
                            "requested": {"country": "Hungary"},
                        },
                    },
                },
            },
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert missing["coverage_diagnostic:no_current_prices_for_country"]["impact"] == "weakens_answer"
    assert package["confidence"] == "high"


def test_market_overview_price_country_gap_blocks_when_question_asks_pricing() -> None:
    package = build_evidence_package(
        session_id="sess_hungary_hev_pricing_gap",
        country="Hungary",
        question="匈牙利 J7 HEV 应该怎么定价？请给出 MSRP 和价格走廊。",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]},
            "requiredTools": ["build_market_chart", "query_msrp_pricing"],
        },
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Hungary", "chartCount": 1},
                    "data": {
                        "contextSnapshot": {
                            "crossTabs": {
                                "driveByFuel": [
                                    {"_index": "HEV", "_total": 2687, "2WD_pct": 89.5, "4WD_pct": 9.9},
                                ],
                            },
                        },
                    },
                },
            },
            {
                "toolName": "query_msrp_pricing",
                "success": True,
                "result": {
                    "tool": "query_msrp_pricing",
                    "metadata": {"source": "jato_msrp_pricing", "country": "Hungary"},
                    "data": {
                        "items": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_country",
                            "requested": {"country": "Hungary"},
                        },
                    },
                },
            },
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert missing["coverage_diagnostic:no_current_prices_for_country"]["impact"] == "blocking"
    assert package["confidence"] == "medium"


def test_evidence_package_marks_model_level_gap_for_named_model_market_opportunity() -> None:
    package = build_evidence_package(
        session_id="sess_hungary_j7_market",
        country="Hungary",
        question="匈牙利 J7 HEV 市场机会是什么？",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"models": ["J7 HEV"], "powertrains": ["HEV"]},
        },
        tool_results=[
            {
                "toolName": "build_market_chart",
                "success": True,
                "result": {
                    "tool": "build_market_chart",
                    "metadata": {"source": "jato_country_chart_deck", "country": "Hungary", "chartCount": 0},
                    "data": {
                        "extractedParams": {"powertrain": "HEV"},
                        "contextSnapshot": {
                            "crossTabs": {
                                "driveByFuel": [
                                    {"_index": "HEV", "_total": 2687, "2WD_pct": 89.5, "4WD_pct": 9.9},
                                ],
                                "driveBySegment": [
                                    {"_index": "SUV A0", "_total": 7303, "2WD_pct": 88.1, "4WD_pct": 11.9},
                                    {"_index": "SUV A", "_total": 3535, "2WD_pct": 69.0, "4WD_pct": 30.0},
                                ],
                            },
                        },
                    },
                },
            },
        ],
    )

    missing = {item["name"]: item for item in package["missingEvidence"]}

    assert "market_snapshot_data_unavailable" not in missing
    assert missing["model_level_market_opportunity_evidence"]["impact"] == "weakens_answer"
    assert "model-level competitor" in missing["model_level_market_opportunity_evidence"]["reason"]
    assert package["confidence"] == "medium"


def test_evidence_package_balances_competitor_refs_across_requested_models() -> None:
    package = build_evidence_package(
        session_id="sess_o5_benchmark",
        country="Sweden",
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        evidence_plan={
            "intent": "report_generation",
            "entities": {"models": ["O5 BEV", "EX30", "EV3"], "competitors": ["EX30", "EV3"]},
            "requiredTools": ["compare_competitive_set"],
        },
        tool_results=[
            {
                "toolName": "compare_competitive_set",
                "success": True,
                "result": {
                    "tool": "compare_competitive_set",
                    "metadata": {"source": "jato_cross_reference"},
                    "data": {
                        "targetModel": "O5 BEV",
                        "competitors": [
                            {
                                "model": "EX30",
                                "sales": 1518,
                                "priceEvidenceStatus": "source_draft_available",
                                "reviewPendingRows": 3,
                                "currentPriceRows": 0,
                                "candidateDomain": "volvocars.com",
                                "sourceDraftPath": "se/05_volvo_ex30_se.yaml",
                            },
                            {
                                "model": "EV3",
                                "sales": 980,
                                "priceEvidenceStatus": "source_draft_available",
                                "currentPriceRows": 0,
                                "candidateDomain": "kia.com",
                                "sourceDraftPath": "se/04_kia_ev3_se.yaml",
                            },
                        ],
                    },
                },
            }
        ],
    )

    refs = package["toolResults"][0]["evidenceRefs"]
    by_label = {str(ref["label"]): ref for ref in refs}

    assert by_label["EX30.sales"]["value"] == 1518
    assert by_label["EV3.sales"]["value"] == 980
    assert by_label["EX30.priceEvidenceStatus"]["value"] == "source_draft_available"
    assert by_label["EV3.priceEvidenceStatus"]["value"] == "source_draft_available"
    assert by_label["EX30.priceEvidenceStatus"].get("unit") in {None, ""}
    assert by_label["EV3.priceEvidenceStatus"].get("unit") in {None, ""}
    assert by_label["EV3.currentPriceRows"]["unit"] == "units"
    assert "EV3.sales: 980 units" in package["toolResults"][0]["keyFindings"]


def test_evidence_package_compacts_nordic_cross_country_powertrain_refs() -> None:
    package = build_evidence_package(
        session_id="sess_nordic_cross_country",
        country="Sweden",
        question="北欧 BEV 增长是否会压缩 HEV 空间？",
        evidence_plan={
            "intent": "market_overview",
            "entities": {"countries": ["Sweden", "Finland", "Norway", "Denmark"]},
            "requiredTools": ["query_cross_country"],
            "mustHaveEvidence": [],
        },
        tool_results=[
            {
                "toolName": "query_cross_country",
                "query": {"countries": "Sweden, Finland, Norway, Denmark"},
                "success": True,
                "result": {
                    "tool": "query_cross_country",
                    "metadata": {"source": "jato_cross_country", "countries": ["Sweden", "Finland", "Norway", "Denmark"]},
                    "data": {
                        "countries": ["Sweden", "Finland", "Norway", "Denmark"],
                        "comparison": {
                            "Sweden": {
                                "powertrainMix": [
                                    {"label": "BEV", "sales": 25235, "share": 42.0},
                                    {"label": "HEV", "sales": 5051, "share": 8.4},
                                    {"label": "PHEV", "sales": 15028, "share": 25.0},
                                ]
                            },
                            "Finland": {
                                "powertrainMix": [
                                    {"label": "BEV", "sales": 2800, "share": 27.2},
                                    {"label": "HEV", "sales": 2687, "share": 26.1},
                                ]
                            },
                            "Norway": {
                                "powertrainMix": [
                                    {"label": "BEV", "sales": 42000, "share": 80.0},
                                    {"label": "HEV", "sales": 1400, "share": 2.6},
                                ]
                            },
                            "Denmark": {
                                "powertrainMix": [
                                    {"label": "BEV", "sales": 18000, "share": 50.0},
                                    {"label": "HEV", "sales": 2500, "share": 7.0},
                                ]
                            },
                        },
                    },
                },
            }
        ],
    )

    labels = {str(ref.get("label")) for tool in package["toolResults"] for ref in tool["evidenceRefs"]}
    missing_names = {item["name"] for item in package["missingEvidence"]}

    for country in ("Sweden", "Finland", "Norway", "Denmark"):
        assert f"crossCountry.{country}.powertrainMix.BEV.sales" in labels
        assert f"crossCountry.{country}.powertrainMix.HEV.sales" in labels
        assert f"missing_country_snapshot:{country}" not in missing_names
    assert "missing_required_tool:query_cross_country" not in missing_names
def test_blind_leasing_offer_becomes_postgres_monthly_and_residual_evidence() -> None:
    package = build_evidence_package(
        session_id="sess_blind_leasing",
        country="Sweden",
        question="Nimbus E BEV 的 36 个月月供和 RV 是否有竞争力？",
        evidence_plan={
            "intent": "pricing_analysis",
            "entities": {"countries": ["Sweden"], "models": ["Nimbus E BEV"]},
            "requiredTools": ["query_leasing_offers"],
            "evidenceNeeded": [{
                "name": "leasing_tco_or_company_car_evidence",
                "reason": "Need a cited lease offer.",
                "priority": 1,
            }],
        },
        tool_results=[{
            "toolName": "query_leasing_offers",
            "query": {"country": "Sweden", "models": ["Nimbus E BEV"]},
            "success": True,
            "result": {
                "tool": "query_leasing_offers",
                "metadata": {
                    "source": "jato_lease_offer_postgres",
                    "country": "Sweden",
                    "resultCount": 1,
                },
                "data": {
                    "country": "Sweden",
                    "items": [{
                        "modelName": "Nimbus E BEV",
                        "status": "active",
                        "termMonths": 36,
                        "mileagePerYear": 15000,
                        "effectiveMonthlyEur": 529.0,
                        "residualValuePercent": 54.0,
                        "totalContractCostEur": 19044.0,
                        "sourceUrl": "https://example.test/nimbus-lease",
                    }],
                },
            },
        }],
    )

    leasing = next(item for item in package["toolResults"] if item["toolName"] == "query_leasing_offers")
    refs = {str(ref["label"]): ref for ref in leasing["evidenceRefs"]}
    missing = {item["name"] for item in package["missingEvidence"]}

    assert leasing["sourceType"] == "postgres"
    assert refs["Nimbus E BEV.effectiveMonthlyEur"]["unit"] == "EUR/month"
    assert refs["Nimbus E BEV.residualValuePercent"]["unit"] == "%"
    assert refs["Nimbus E BEV.termMonths"]["unit"] == "months"
    assert refs["Nimbus E BEV.effectiveMonthlyEur"]["source"] == "jato_lease_offer_postgres"
    assert "leasing_tco_or_company_car_evidence" not in missing
