from __future__ import annotations

import pytest

from app.services import jato_agent_provider_service
from app.services import jato_mcp_tools_service
from app.services import jato_query_tools


@pytest.fixture(autouse=True)
def disable_real_final_composer(monkeypatch) -> None:
    monkeypatch.setenv("APP_ASTRBOT_FINAL_COMPOSER_ENABLED", "false")
    monkeypatch.delenv(jato_agent_provider_service.ASTRBOT_PROVIDER_KEY_ENV, raising=False)
    monkeypatch.setattr(jato_mcp_tools_service, "track_followup_impression", lambda **_kwargs: None)
    monkeypatch.setattr(jato_mcp_tools_service, "track_tool_call_event", lambda **_kwargs: None)


def test_jato_cross_check_does_not_treat_zero_count_kpis_as_support(monkeypatch) -> None:
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "_build_country_snapshot_with_fallback",
        lambda *_args, **_kwargs: (
            {
                "periodLabel": "Hungary 2026年3月市场扫描",
                "kpis": {
                    "totalRows": 0,
                    "countryCount": 0,
                    "brandCount": 0,
                    "modelCount": 0,
                    "versionCount": 0,
                },
                "topModels": [],
                "powertrainMix": [],
            },
            "Hungary",
        ),
    )

    cross_check = jato_mcp_tools_service._build_jato_cross_check(
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
    )

    assert cross_check["status"] == "not_available"
    assert cross_check["checks"] == []
    assert cross_check["conflictRisk"] == "none"


def test_jato_cross_check_accepts_nonzero_market_kpis(monkeypatch) -> None:
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "_build_country_snapshot_with_fallback",
        lambda *_args, **_kwargs: (
            {
                "periodLabel": "Sweden 2026年3月市场扫描",
                "kpis": {"totalRows": 200, "marketShare": 12.5},
                "topModels": [],
                "powertrainMix": [],
            },
            "Sweden",
        ),
    )

    cross_check = jato_mcp_tools_service._build_jato_cross_check(
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
    )

    assert cross_check["status"] == "matched"
    assert cross_check["checks"][0]["name"] == "market_kpis"


def test_query_time_series_pushes_country_powertrain_and_segment_to_parquet(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        jato_query_tools.query_service.repo,
        "list_columns",
        lambda: ["国家", "动总规整", "细分市场（按车长）", "2026 Jan", "2026 Feb"],
    )

    def fake_query_time_series(*, filters, grain, top_n):
        captured.update({"filters": filters, "grain": grain, "top_n": top_n})
        return {
            "grain": "month",
            "rows": 2,
            "items": [
                {"time": "2026 Jan", "value": 1890.0},
                {"time": "2026 Feb", "value": 1906.0},
            ],
        }

    monkeypatch.setattr(jato_query_tools.query_service, "query_time_series", fake_query_time_series)

    result = jato_query_tools.query_time_series(
        country="匈牙利",
        powertrain="HEV",
        segment="SUV A0 / SUV A",
        granularity="monthly",
    )

    assert captured["filters"] == {
        "国家": ["匈牙利"],
        "动总规整": ["HEV"],
        "细分市场（按车长）": ["SUV A0", "SUV A"],
    }
    assert captured["grain"] == "month"
    assert result["monthSeries"] == [
        {"period": "2026 Jan", "sales": 1890.0},
        {"period": "2026 Feb", "sales": 1906.0},
    ]
    assert result["dataPoints"] == 2
    assert result["stats"]["endSales"] == 1906.0


def test_query_time_series_does_not_relabel_sales_as_requested_share(monkeypatch) -> None:
    monkeypatch.setattr(jato_query_tools.query_service.repo, "list_columns", lambda: ["国家", "2026 Jan"])
    monkeypatch.setattr(
        jato_query_tools.query_service,
        "query_time_series",
        lambda **_kwargs: {"items": [{"time": "2026 Jan", "value": 100.0}]},
    )

    result = jato_query_tools.query_time_series(country="匈牙利", metric="share")

    assert result["metric"] == "sales"
    assert result["requestedMetric"] == "share"
    assert "returned registration sales" in result["coverageDiagnostics"]["metricBoundary"]


def test_jato_data_country_reuses_full_country_alias_registry() -> None:
    assert jato_mcp_tools_service._jato_data_country("Hungary") == "匈牙利"
    assert jato_mcp_tools_service._jato_data_country("Poland") == "波兰"
    assert jato_mcp_tools_service._jato_data_country("Czech Republic") == "捷克"


def test_route_agent_request_selects_chart_tool(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_country_chart_deck(**kwargs):
        captured.update(kwargs)
        return {
            "country": kwargs["country"],
            "question": kwargs["question"],
            "primaryIntent": "market_trend",
            "intents": ["market_trend"],
            "deckIntents": ["market_trend"],
            "intentRoute": "chart",
            "controls": {},
            "extractedParams": {},
            "contextSnapshot": {"country": kwargs["country"], "yearSeries": [{"year": 2025, "sales": 100}]},
        }

    monkeypatch.setattr(
        jato_mcp_tools_service,
        "build_country_chart_deck",
        fake_build_country_chart_deck,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "route_agent_request",
        {"country": "Sweden", "question": "Draw a BEV trend chart for 2025"},
    )

    assert captured["country"] == "Sweden"
    assert result["tool"] == "route_agent_request"
    assert result["metadata"]["selectedTool"] == "build_market_chart"
    assert result["metadata"]["profileId"] == "pm_coder_market_assistant"
    assert result["metadata"]["skillId"] == "market_chart_analysis"
    assert result["data"]["profile"]["id"] == "pm_coder_market_assistant"
    assert result["data"]["skill"]["id"] == "market_chart_analysis"
    assert result["data"]["route"]["reason"] == "retrieval_router_chart_signal"
    assert result["data"]["route"]["retrievalPath"] == "structured_mcp"
    assert result["data"]["routeSource"] == "retrieval_router"
    assert result["data"]["retrievalClassification"]["primaryPath"] == "structured_mcp"
    assert "secondaryPaths" in result["data"]["retrievalClassification"]
    assert result["data"]["retrievalToolPlan"]["primaryPath"] == "structured_mcp"
    assert len(result["data"]["retrievalToolPlan"]["steps"]) >= 1
    assert result["data"]["evidencePack"]["classification"]["primaryPath"] == "structured_mcp"
    assert result["data"]["display"]["summary"] == "Chart-ready market context is available for rendering and explanation."
    assert result["data"]["answer"]["title"] == "Grounded answer"
    assert "直接结论" in result["data"]["answer"]["direct"]
    assert result["data"]["answer"]["tool"] == "build_market_chart"
    assert result["data"]["answer"]["citations"][0]["tool"] == "build_market_chart"
    assert result["data"]["answer"]["followUps"]
    assert result["data"]["answer"]["structuredFollowUps"][0]["expectedTools"]
    assert any(
        "品牌" in item["question"] or "车型" in item["question"]
        for item in result["data"]["answer"]["structuredFollowUps"]
    )
    assert result["data"]["evidencePlan"]["intent"] == "market_overview"
    assert result["data"]["evidencePackage"]["toolResults"]
    assert result["data"]["visualArtifacts"]
    assert result["data"]["answer"]["visualArtifacts"] == result["data"]["visualArtifacts"]
    assert result["data"]["qualityScore"]["safetyScore"] == 1
    assert result["data"]["modelUsage"]["status"] == "disabled"
    assert result["data"]["display"]["cards"][0]["value"] == "pm_coder_market_assistant"
    assert result["data"]["display"]["cards"][1]["value"] == "market_chart_analysis"
    assert result["data"]["display"]["cards"][3]["value"] == "build_market_chart"
    # Phase 6: toolPlan now lists all tools in priority order; executed step is marked
    executed_steps = [s for s in result["data"]["toolPlan"] if s.get("executed")]
    assert len(executed_steps) == 1
    assert executed_steps[0]["tool"] == "build_market_chart"
    assert executed_steps[0]["path"] == "structured_mcp"
    assert result["data"]["primaryResult"]["tool"] == "build_market_chart"


def test_route_agent_request_country_mention_overrides_ui_default(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_country_chart_deck(**kwargs):
        captured.update(kwargs)
        return {
            "country": kwargs["country"],
            "question": kwargs["question"],
            "primaryIntent": "market_trend",
            "intents": ["market_trend"],
            "deckIntents": ["market_trend"],
            "intentRoute": "chart",
            "controls": {},
            "extractedParams": {},
            "contextSnapshot": {"country": kwargs["country"], "yearSeries": [{"year": 2025, "sales": 100}]},
        }

    monkeypatch.setattr(
        jato_mcp_tools_service,
        "build_country_chart_deck",
        fake_build_country_chart_deck,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "route_agent_request",
        {"country": "Sweden", "question": "Draw a BEV trend chart for 匈牙利 2025"},
    )

    assert captured["country"] == "Hungary"
    assert result["data"]["evidencePlan"]["country"] == "Hungary"
    assert result["data"]["evidencePackage"]["country"] == "Hungary"


def test_route_agent_request_uses_evidence_plan_countries_for_nordic_cross_country(monkeypatch) -> None:
    captured: list[tuple[str, dict[str, object]]] = []

    def fake_call_jato_mcp_tool(tool_name: str, args: dict[str, object]):
        captured.append((tool_name, dict(args)))
        assert tool_name == "query_cross_country"
        countries = [
            country.strip()
            for country in str(args.get("countries") or "").split(",")
            if country.strip()
        ]
        return {
            "tool": tool_name,
            "data": {
                "countries": countries,
                "question": args["question"],
                "comparison": {
                    country: {
                        "kpis": {"totalSales": 1000},
                        "powertrainMix": [
                            {"label": "BEV", "sales": 600, "share": 60.0},
                            {"label": "HEV", "sales": 200, "share": 20.0},
                        ],
                        "topModels": [],
                    }
                    for country in countries
                },
                "countryCount": len(countries),
            },
            "metadata": {"source": "fake_cross_country", "limitations": []},
        }

    monkeypatch.setattr(jato_mcp_tools_service, "agent_select_tools", lambda **_kwargs: None)
    monkeypatch.setattr(jato_mcp_tools_service, "run_agent_loop", lambda **_kwargs: None)
    monkeypatch.setattr(jato_mcp_tools_service, "call_jato_mcp_tool", fake_call_jato_mcp_tool)

    result = jato_mcp_tools_service.route_agent_request({
        "country": "Sweden",
        "question": "北欧 BEV 增长是否会压缩 HEV 空间？",
    })

    assert captured[0] == (
        "query_cross_country",
        {
            "country": "Sweden",
            "question": "北欧 BEV 增长是否会压缩 HEV 空间？",
            "countries": "Sweden, Finland, Norway, Denmark",
        },
    )
    assert result["metadata"]["selectedTool"] == "query_cross_country"
    assert result["data"]["evidencePlan"]["entities"]["countries"] == [
        "Sweden",
        "Finland",
        "Norway",
        "Denmark",
    ]
    assert result["data"]["primaryResult"]["data"]["countries"] == [
        "Sweden",
        "Finland",
        "Norway",
        "Denmark",
    ]


def test_compare_competitive_set_uses_explicit_question_models_when_snapshot_empty(monkeypatch) -> None:
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "query_country_snapshot",
        lambda _args: {
            "data": {"topModels": []},
            "metadata": {"source": "test_snapshot"},
        },
    )
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "query_msrp_pricing",
        lambda _args: {
            "data": {"items": []},
            "metadata": {"source": "test_msrp"},
        },
    )

    result = jato_mcp_tools_service.compare_competitive_set({
        "country": "Sweden",
        "question": "O5 BEV 应该对标 EX30 还是 EV3？",
        "model": "O5 BEV",
        "models": ["O5 BEV", "EX30", "EV3"],
    })

    competitors = result["data"]["competitors"]

    assert result["metadata"]["competitorCount"] == 2
    assert [item["model"] for item in competitors] == ["EX30", "EV3"]
    assert all(item["source"] == "user_question_model_candidate" for item in competitors)


def test_compare_competitive_set_uses_explicit_competitor_pool_before_snapshot(monkeypatch) -> None:
    snapshot_calls: list[dict] = []
    msrp_calls: list[dict] = []

    def fake_snapshot(args: dict) -> dict:
        snapshot_calls.append(args)
        return {
            "data": {
                "topModels": [
                    {"label": "EX40", "value": 2945, "rank": 1},
                    {"label": "Toyota RAV4", "value": 930, "rank": 6},
                    {"label": "Toyota C-HR", "value": 720, "rank": 8},
                ]
            },
            "metadata": {"source": "test_snapshot"},
        }

    def fake_msrp(args: dict) -> dict:
        msrp_calls.append(args)
        if args.get("model") == "Corolla Cross":
            return {
                "data": {
                    "items": [],
                    "coverageDiagnostics": {
                        "diagnosis": "no_current_prices_for_requested_models",
                        "nextActions": ["Validate Corolla Cross official MSRP source."],
                        "sourceRepairCandidates": {
                            "ownModel": [
                                {
                                    "model": "Corolla Cross",
                                    "draftStatus": "source_draft_available",
                                    "sourceCategory": "current_price",
                                    "sourceDraftPath": "se/16_toyota_corolla_cross_se.yaml",
                                    "candidateDomain": "toyota.se",
                                    "candidateSourceType": "source_draft",
                                    "materializationStatus": "draft_ready",
                                    "materializationNextStep": "Run MSRP dry-run for Corolla Cross.",
                                    "reviewPendingRows": 2,
                                }
                            ],
                            "competitorCorridor": [],
                        },
                    },
                },
                "metadata": {"source": "test_msrp"},
            }
        return {
            "data": {"items": [{"model": args.get("model"), "msrp": 40200}]},
            "metadata": {"source": "test_msrp"},
        }

    monkeypatch.setattr(jato_mcp_tools_service, "query_country_snapshot", fake_snapshot)
    monkeypatch.setattr(jato_mcp_tools_service, "query_msrp_pricing", fake_msrp)

    result = jato_mcp_tools_service.compare_competitive_set({
        "country": "Sweden",
        "question": "瑞典 J7 HEV 应该怎么定价？",
        "model": "J7 HEV",
        "competitors": ["Corolla Cross", "RAV4", "C-HR", "Qashqai"],
    })

    competitors = result["data"]["competitors"]

    assert len(snapshot_calls) == 1
    assert [item["model"] for item in competitors] == ["Corolla Cross", "RAV4", "C-HR", "Qashqai"]
    assert all(item["source"] == "explicit_competitor_pool" for item in competitors)
    assert competitors[0]["sales"] == 0
    assert competitors[0]["priceEvidenceStatus"] == "source_draft_available"
    assert competitors[0]["sourceDraftPath"] == "se/16_toyota_corolla_cross_se.yaml"
    assert competitors[0]["candidateDomain"] == "toyota.se"
    assert competitors[0]["priceEvidenceNextStep"] == "Run MSRP dry-run for Corolla Cross."
    assert competitors[1]["sales"] == 930
    assert competitors[1]["snapshotLabel"] == "Toyota RAV4"
    assert competitors[2]["sales"] == 720
    assert competitors[2]["rank"] == 8
    assert [call["model"] for call in msrp_calls] == ["Corolla Cross", "RAV4", "C-HR", "Qashqai"]
    assert result["metadata"]["sources"][0] == "explicit_competitor_pool"
    assert "jato_country_snapshot" in result["metadata"]["sources"]


def test_route_agent_request_respects_requested_skill(monkeypatch) -> None:
    def fake_search_market_news(*, country: str, question: str, limit: int):
        return [{"title": "Policy", "provider": "tavily", "source": country, "url": "https://example.com"}]

    monkeypatch.setattr(
        jato_mcp_tools_service.web_search_service,
        "search_market_news",
        fake_search_market_news,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "route_agent_request",
        {
            "country": "Sweden",
            "question": "What should I watch?",
            "skill_id": "policy_news_scan",
        },
    )

    assert result["metadata"]["selectedTool"] == "search_market_news"
    assert result["metadata"]["skillId"] == "policy_news_scan"
    assert result["data"]["skill"]["routeMode"] == "news"
    assert result["data"]["primaryResult"]["data"]["items"][0]["provider"] == "tavily"


def test_route_agent_request_executes_secondary_evidence_path(monkeypatch) -> None:
    def fake_build_country_chart_deck(**kwargs):
        return {
            "country": kwargs["country"],
            "question": kwargs["question"],
            "primaryIntent": "market_trend",
            "intents": ["market_trend"],
            "deckIntents": ["market_trend"],
            "intentRoute": "chart",
            "controls": {},
            "extractedParams": {},
            "contextSnapshot": {"country": kwargs["country"], "yearSeries": [{"year": 2025, "sales": 100}]},
        }

    def fake_search_market_news(*, country: str, question: str, limit: int):
        return [{"title": "Policy update", "provider": "tavily", "source": country, "url": "https://example.com"}]

    monkeypatch.setattr(jato_mcp_tools_service, "build_country_chart_deck", fake_build_country_chart_deck)
    monkeypatch.setattr(jato_mcp_tools_service.web_search_service, "search_market_news", fake_search_market_news)

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "route_agent_request",
        {
            "country": "Sweden",
            "question": "Draw a BEV share chart and include policy implications.",
        },
    )

    assert result["metadata"]["selectedTool"] == "build_market_chart"
    assert "search_market_news" in result["metadata"]["secondaryTools"]
    assert "structured_mcp" in result["data"]["evidencePack"]["pathsContributed"]
    assert "hybrid_rag" in result["data"]["evidencePack"]["pathsContributed"]
    assert result["data"]["answer"]["sourceCount"] >= 2
    assert result["data"]["answer"]["businessSynthesisPlan"]["intent"] == "news_policy_search"
    executed_steps = [s for s in result["data"]["toolPlan"] if s.get("executed")]
    assert {step["tool"] for step in executed_steps} >= {"build_market_chart", "search_market_news"}


def test_route_agent_request_fills_required_tools_for_pricing_report(monkeypatch) -> None:
    calls: list[str] = []

    def fake_call_jato_mcp_tool(tool_name: str, args: dict[str, object]):
        calls.append(tool_name)
        if tool_name == "query_msrp_pricing":
            return {
                "tool": tool_name,
                "data": {
                    "items": [{"model": "J7 HEV", "msrp": 34720, "currency": "EUR"}],
                    "modelSummaries": [{"model": "J7 HEV", "price": 34720}],
                },
                "metadata": {"source": "fake_msrp", "limitations": []},
            }
        if tool_name == "compare_competitive_set":
            return {
                "tool": tool_name,
                "data": {
                    "items": [{"targetModel": "J7 HEV", "competitors": ["Corolla Cross", "RAV4"]}],
                    "analysis": "J7 HEV should be placed in the core HEV SUV corridor.",
                },
                "metadata": {"source": "fake_competitor", "limitations": []},
            }
        if tool_name == "build_market_chart":
            return {
                "tool": tool_name,
                "data": {
                    "contextSnapshot": {
                        "powertrainMix": [
                            {"label": "BEV", "value": 25235, "share": 0.42},
                            {"label": "HEV", "value": 5051, "share": 0.08},
                        ]
                    }
                },
                "metadata": {"source": "fake_market_chart", "limitations": []},
            }
        raise AssertionError(f"unexpected tool {tool_name}")

    monkeypatch.setattr(jato_mcp_tools_service, "agent_select_tools", lambda **_kwargs: None)
    monkeypatch.setattr(jato_mcp_tools_service, "run_agent_loop", lambda **_kwargs: None)
    monkeypatch.setattr(jato_mcp_tools_service, "call_jato_mcp_tool", fake_call_jato_mcp_tool)

    result = jato_mcp_tools_service.route_agent_request({
        "country": "Sweden",
        "question": "把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
    })

    assert result["metadata"]["selectedTool"] == "query_msrp_pricing"
    assert result["metadata"]["secondaryTools"] == ["compare_competitive_set", "build_market_chart"]
    assert calls[0] == "query_msrp_pricing"
    assert "compare_competitive_set" in calls
    assert "build_market_chart" in calls
    assert calls.index("query_msrp_pricing") < calls.index("compare_competitive_set")
    assert calls.index("compare_competitive_set") < calls.index("build_market_chart")
    assert result["data"]["evidencePlan"]["intent"] == "report_generation"
    assert result["data"]["evidencePlan"]["requiredTools"] == [
        "query_msrp_pricing",
        "compare_competitive_set",
        "build_market_chart",
    ]
    assert {
        item["toolName"]
        for item in result["data"]["evidencePackage"]["toolResults"]
    } >= {"query_msrp_pricing", "compare_competitive_set", "build_market_chart"}
    assert not any(
        failure.startswith("missing_required_tools")
        for failure in result["data"]["qualityScore"]["failures"]
    )


def test_route_agent_request_repairs_weak_internal_evidence_with_governed_search(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    question = "瑞典 V2H 对用户购车决策是不是重要？"

    def fake_call_jato_mcp_tool(tool_name: str, args: dict[str, object]):
        calls.append((tool_name, dict(args)))
        if tool_name == "query_country_snapshot":
            return {
                "tool": tool_name,
                "data": {"kpis": {"totalRows": 0, "countryCount": 0}, "topModels": [], "powertrainMix": []},
                "metadata": {"source": "fake_empty_snapshot", "limitations": []},
            }
        if tool_name == "search_market_news":
            return {
                "tool": tool_name,
                "data": {
                    "items": [
                        {
                            "title": "Swedish EV owners discuss V2H",
                            "url": "https://example.com/sweden-v2h",
                            "source": "Example EV Research",
                            "snippet": (
                                "Swedish EV owners mention V2H as a backup power and energy-flexibility feature, "
                                "but charger availability affects purchase relevance."
                            ),
                            "publishedAt": "2026-05-12",
                        }
                    ],
                },
                "metadata": {"source": "jato_web_search_service", "limitations": []},
            }
        raise AssertionError(f"unexpected tool {tool_name}")

    monkeypatch.setattr(jato_mcp_tools_service, "agent_select_tools", lambda **_kwargs: None)
    monkeypatch.setattr(jato_mcp_tools_service, "run_agent_loop", lambda **_kwargs: None)
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "build_evidence_plan",
        lambda _country, _question: {
            "intent": "market_overview",
            "entities": {},
            "requiredTools": ["query_country_snapshot"],
            "allowedTools": ["query_country_snapshot", "search_market_news"],
            "toolPlan": [
                {"toolName": "query_country_snapshot", "input": {"country": "Sweden", "question": question}},
                {"toolName": "search_market_news", "input": {"country": "Sweden", "question": question}},
            ],
            "answerMode": "analysis",
        },
    )
    monkeypatch.setattr(jato_mcp_tools_service, "call_jato_mcp_tool", fake_call_jato_mcp_tool)

    result = jato_mcp_tools_service.route_agent_request({"country": "Sweden", "question": question})

    tool_names = [tool for tool, _args in calls]
    assert tool_names[:2] == ["query_country_snapshot", "search_market_news"]
    assert result["metadata"]["secondaryTools"] == ["search_market_news"]
    evidence_tools = [
        item["toolName"]
        for item in result["data"]["evidencePackage"]["toolResults"]
    ]
    assert evidence_tools == ["query_country_snapshot", "search_market_news"]
    external_refs = result["data"]["evidencePackage"]["toolResults"][1]["evidenceRefs"]
    assert any(ref["label"].endswith(".claim") for ref in external_refs)


def test_route_agent_request_repairs_missing_msrp_with_governed_search(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    question = "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？"

    def fake_call_jato_mcp_tool(tool_name: str, args: dict[str, object]):
        calls.append((tool_name, dict(args)))
        if tool_name == "query_msrp_pricing":
            return {
                "tool": tool_name,
                "data": {
                    "items": [],
                    "modelSummaries": [],
                    "coverageDiagnostics": {
                        "diagnosis": "no_current_prices_for_requested_models",
                        "message": "Requested models have no current price rows.",
                        "requestedModels": ["O5 BEV", "EV3"],
                    },
                },
                "metadata": {"source": "fake_msrp", "limitations": []},
            }
        if tool_name == "search_market_news":
            return {
                "tool": tool_name,
                "data": {
                    "items": [
                        {
                            "title": "Kia EV3 Sweden official price",
                            "url": "https://example.com/kia-ev3-sweden-price",
                            "source": "Kia Sweden",
                            "snippet": "Kia Sweden lists EV3 pricing and trim information for Swedish customers.",
                            "publishedAt": "2026-06-15",
                        }
                    ],
                },
                "metadata": {"source": "jato_web_search_service", "limitations": []},
            }
        raise AssertionError(f"unexpected tool {tool_name}")

    monkeypatch.setattr(
        jato_mcp_tools_service,
        "agent_select_tools",
        lambda **_kwargs: {
            "source": "llm_agent",
            "confidence": "high",
            "primary_tool": "query_msrp_pricing",
            "mode": "pricing",
            "reasoning": "Pricing delta needs current MSRP first.",
        },
    )
    monkeypatch.setattr(jato_mcp_tools_service, "run_agent_loop", lambda **_kwargs: None)
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "build_evidence_plan",
        lambda _country, _question: {
            "intent": "pricing_analysis",
            "entities": {"models": ["O5 BEV", "EV3"]},
            "requiredTools": ["query_msrp_pricing"],
            "allowedTools": ["query_msrp_pricing", "search_market_news"],
            "toolPlan": [
                {
                    "toolName": "query_msrp_pricing",
                    "input": {"country": "Sweden", "question": question, "models": ["O5 BEV", "EV3"]},
                },
                {
                    "toolName": "search_market_news",
                    "input": {"country": "Sweden", "question": question},
                },
            ],
            "answerMode": "analysis",
        },
    )
    monkeypatch.setattr(jato_mcp_tools_service, "call_jato_mcp_tool", fake_call_jato_mcp_tool)

    result = jato_mcp_tools_service.route_agent_request({"country": "Sweden", "question": question})

    assert [tool for tool, _args in calls] == ["query_msrp_pricing", "search_market_news"]
    assert result["metadata"]["secondaryTools"] == ["search_market_news"]
    evidence_tools = [
        item["toolName"]
        for item in result["data"]["evidencePackage"]["toolResults"]
    ]
    assert evidence_tools[:2] == ["query_msrp_pricing", "search_market_news"]
    assert "user_supplied_price_delta" in evidence_tools
    assert any(
        item["name"] == "coverage_diagnostic:no_current_prices_for_requested_models"
        for item in result["data"]["evidencePackage"]["missingEvidence"]
    )
    assert result["data"]["evidencePackage"]["toolResults"][1]["evidenceRefs"]


def test_route_agent_request_rebuilds_evidence_package_with_agent_loop_tool_results(monkeypatch) -> None:
    tool_args: list[tuple[str, dict[str, object]]] = []

    def fake_call_jato_mcp_tool(tool_name: str, args: dict[str, object]):
        tool_args.append((tool_name, dict(args)))
        if tool_name == "query_msrp_pricing":
            return {
                "tool": tool_name,
                "data": {"items": [], "modelSummaries": []},
                "metadata": {"source": "fake_msrp", "limitations": []},
            }
        if tool_name == "compare_competitive_set":
            return {
                "tool": tool_name,
                "data": {"competitors": [], "analysis": {"totalCompared": 0, "hasPricing": False}},
                "metadata": {"source": "fake_competitor", "limitations": []},
            }
        raise AssertionError(f"unexpected tool {tool_name}")

    def fake_run_agent_loop(**_kwargs):
        news_result = {
            "tool": "search_market_news",
            "data": {
                "items": [
                    {
                        "title": "O9 Sweden price source",
                        "url": "https://example.com/o9-sweden-price",
                        "claim": "O9 price check requires current Swedish competitor prices.",
                        "publishedAt": "2026-06-01",
                    }
                ]
            },
            "metadata": {"source": "fake_news", "limitations": []},
        }
        return {
            "answer": {
                "title": "O9 price check",
                "direct": "需要用 MSRP、竞品走廊和外部价格来源验证。",
                "bullets": ["先查竞品价格走廊。"],
                "limitations": [],
                "followUps": [],
            },
            "toolCalls": [
                {
                    "round": 1,
                    "tool": "search_market_news",
                    "args": {"country": "Sweden", "question": "O9 在瑞典 53k-55k 欧元是否合理？"},
                    "reason": "Need external price source.",
                    "status": "ok",
                    "hasData": True,
                    "result": news_result,
                }
            ],
            "rounds": 1,
            "usage": {"status": "ok", "provider": "fake", "model": "fake", "promptTokens": 1, "completionTokens": 1, "totalTokens": 2},
        }

    monkeypatch.setattr(
        jato_mcp_tools_service,
        "agent_select_tools",
        lambda **_kwargs: {
            "source": "llm_agent",
            "confidence": "high",
            "primary_tool": "query_msrp_pricing",
            "mode": "pricing",
            "reasoning": "Pricing reasonableness requires MSRP first.",
        },
    )
    monkeypatch.setattr(jato_mcp_tools_service, "run_agent_loop", fake_run_agent_loop)
    monkeypatch.setattr(jato_mcp_tools_service, "call_jato_mcp_tool", fake_call_jato_mcp_tool)

    result = jato_mcp_tools_service.route_agent_request({
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
    })

    tool_names = {
        item["toolName"]
        for item in result["data"]["evidencePackage"]["toolResults"]
    }
    assert {"query_msrp_pricing", "compare_competitive_set", "search_market_news"}.issubset(tool_names)
    assert ("query_msrp_pricing", {
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "model": "O9",
        "max_items": 12,
    }) in tool_args
    assert result["data"]["evidencePackage"]["confidence"] in {"medium", "high"}
    assert result["data"]["evidencePackage"]["toolResults"][-1]["evidenceRefs"]
    assert "search_market_news" in result["metadata"]["secondaryTools"]


def test_route_agent_request_uses_provider_final_answer(monkeypatch) -> None:
    usage_calls: list[dict[str, object]] = []

    def fake_build_country_chart_deck(**kwargs):
        return {
            "country": kwargs["country"],
            "question": kwargs["question"],
            "primaryIntent": "market_trend",
            "intents": ["market_trend"],
            "deckIntents": ["market_trend"],
            "intentRoute": "chart",
            "controls": {},
            "extractedParams": {},
            "contextSnapshot": {"country": kwargs["country"], "yearSeries": [{"year": 2025, "sales": 100}]},
        }

    def fake_compose_agent_final_answer(**kwargs):
        answer = dict(kwargs["deterministic_answer"])
        answer.update({
            "title": "JATO answer",
            "direct": "DPV4 final answer",
            "bullets": ["DPV4 bullet"],
            "composer": "dpv4",
        })
        return {
            "answer": answer,
            "usage": {
                "provider": "deepseek",
                "model": "deepseek-chat",
                "status": "ok",
                "promptTokens": 100,
                "completionTokens": 50,
                "totalTokens": 150,
                "promptCacheHitTokens": 0,
                "promptCacheMissTokens": 100,
                "finishReason": "stop",
            },
        }

    def fake_track_agent_answer_run(**kwargs):
        usage_calls.append(kwargs)
        return {
            "usageId": "agent_usage_test",
            "estimatedCostCny": 0.0002,
            "currency": "CNY",
            "pricingModel": "deepseek-v4-flash",
        }

    monkeypatch.setattr(jato_mcp_tools_service, "build_country_chart_deck", fake_build_country_chart_deck)
    monkeypatch.setattr(jato_mcp_tools_service, "compose_agent_final_answer", fake_compose_agent_final_answer)
    monkeypatch.setattr(jato_mcp_tools_service, "track_agent_answer_run", fake_track_agent_answer_run)

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "route_agent_request",
        {"country": "Sweden", "question": "Draw a BEV trend chart for 2025"},
    )

    assert result["data"]["answer"]["direct"].startswith("直接结论")
    assert result["data"]["answer"]["bullets"]
    assert all("TOOL:" not in bullet for bullet in result["data"]["answer"]["bullets"])
    assert result["data"]["answer"]["businessSynthesisPlan"]["intent"] == "market_overview"
    assert result["data"]["answer"]["composer"] == "dpv4"
    assert result["data"]["modelUsage"]["usageId"] == "agent_usage_test"
    assert result["metadata"]["answerComposer"] == "dpv4"
    assert result["metadata"]["modelUsageStatus"] == "ok"
    assert usage_calls[0]["selected_tool"] == "build_market_chart"


def test_route_agent_request_ignores_missing_key_agent_loop_answer(monkeypatch) -> None:
    def fake_build_country_chart_deck(**kwargs):
        return {
            "country": kwargs["country"],
            "question": kwargs["question"],
            "primaryIntent": "market_trend",
            "intents": ["market_trend"],
            "deckIntents": ["market_trend"],
            "intentRoute": "chart",
            "controls": {},
            "extractedParams": {},
            "contextSnapshot": {"country": kwargs["country"], "yearSeries": [{"year": 2025, "sales": 100}]},
        }

    def fake_run_agent_loop(**_kwargs):
        return {
            "answer": {
                "direct": "Agent loop requires DEEPSEEK_API_KEY.",
                "bullets": [],
                "limitations": ["No API key"],
            },
            "toolCalls": [],
            "rounds": 0,
            "usage": {"status": "missing_key"},
        }

    monkeypatch.setattr(jato_mcp_tools_service, "build_country_chart_deck", fake_build_country_chart_deck)
    monkeypatch.setattr(jato_mcp_tools_service, "run_agent_loop", fake_run_agent_loop)

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "route_agent_request",
        {"country": "Sweden", "question": "Draw a BEV trend chart for 2025"},
    )

    assert "Agent loop requires" not in result["data"]["answer"]["direct"]
    assert result["data"]["answer"]["composer"] == "deterministic"
    assert result["data"]["modelUsage"]["status"] == "disabled"
    assert result["metadata"]["answerComposer"] == "deterministic"


def test_route_agent_request_routes_url_to_read_web_page(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_browser_read_web_page(url: str, *, question: str = "", max_chars: int = 6000):
        captured["url"] = url
        captured["question"] = question
        captured["max_chars"] = max_chars
        return {
            "status": "ok",
            "url": url,
            "httpStatus": 200,
            "contentType": "text/html",
            "title": "EV Report",
            "description": "Public report",
            "headings": ["Market overview"],
            "textPreview": "EV market report text",
            "links": [{"label": "Source", "url": "https://example.com/source"}],
            "truncated": False,
            "limitations": ["readonly"],
        }

    monkeypatch.setattr(jato_mcp_tools_service, "browser_read_web_page", fake_browser_read_web_page)

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "route_agent_request",
        {
            "country": "Sweden",
            "question": "Summarize https://example.com/report for me.",
        },
    )

    assert captured["url"] == "https://example.com/report"
    assert result["metadata"]["selectedTool"] == "read_web_page"
    assert result["metadata"]["routeSource"] == "url_router"
    assert result["data"]["route"]["mode"] == "web"
    assert result["data"]["route"]["retrievalPath"] == "web_search"
    assert result["data"]["primaryResult"]["data"]["title"] == "EV Report"
    assert result["data"]["display"]["summary"].startswith("Static public page text")
    assert result["data"]["answer"]["citations"][0]["url"] == "https://example.com/report"
    assert result["data"]["nextActions"] == ["summarize_static_page", "cite_page_url"]


def test_route_agent_request_routes_snapshot_url_to_browser_snapshot(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_browser_capture_snapshot(
        url: str,
        *,
        question: str = "",
        max_chars: int = 6000,
        capture_screenshot: bool = False,
        timeout_ms: int = 12000,
    ):
        captured["url"] = url
        captured["question"] = question
        captured["capture_screenshot"] = capture_screenshot
        captured["timeout_ms"] = timeout_ms
        return {
            "status": "fallback_static",
            "url": url,
            "browserEngine": "unavailable",
            "title": "Snapshot Report",
            "textPreview": "Snapshot text",
            "headings": ["Overview"],
            "links": [],
            "screenshot": None,
            "truncated": False,
            "limitations": ["fallback"],
        }

    monkeypatch.setattr(jato_mcp_tools_service, "browser_capture_snapshot", fake_browser_capture_snapshot)

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "route_agent_request",
        {
            "country": "Sweden",
            "question": "请给我这个页面的浏览器截图 https://example.com/report",
        },
    )

    assert captured["url"] == "https://example.com/report"
    assert captured["capture_screenshot"] is True
    assert result["metadata"]["selectedTool"] == "browser_snapshot"
    assert result["data"]["route"]["mode"] == "web"
    assert result["data"]["primaryResult"]["metadata"]["browserEngine"] == "unavailable"
    assert result["data"]["nextActions"] == [
        "summarize_browser_snapshot",
        "cite_page_url",
        "plan_confirmed_browser_action_if_needed",
    ]


def test_browser_snapshot_mcp_tool_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "browser_capture_snapshot",
        lambda url, question="", max_chars=6000, capture_screenshot=False, timeout_ms=12000: {
            "status": "fallback_static",
            "url": url,
            "browserEngine": "unavailable",
            "title": "Snapshot",
            "textPreview": "Snapshot text",
            "headings": [],
            "links": [],
            "screenshot": None,
            "truncated": False,
            "limitations": ["fallback"],
        },
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "browser_snapshot",
        {"url": "https://example.com/report", "question": "snapshot"},
    )

    assert result["tool"] == "browser_snapshot"
    assert result["metadata"]["source"] == "jato_browser_snapshot"
    assert result["metadata"]["readonly"] is True
    assert result["data"]["status"] == "fallback_static"


def test_browser_interaction_plan_mcp_tool_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "browser_plan_interaction",
        lambda url, action_goal="", max_actions=6, timeout_ms=12000: {
            "status": "fallback_static",
            "url": url,
            "browserEngine": "unavailable",
            "title": "Portal",
            "actions": [
                {
                    "actionId": "act_01",
                    "actionType": "click",
                    "label": "Open dashboard",
                    "confirmationToken": "token",
                    "requiresUserApproval": True,
                }
            ],
            "limitations": ["fallback"],
        },
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "browser_interaction_plan",
        {"url": "https://example.com/report", "action_goal": "open dashboard"},
    )

    assert result["tool"] == "browser_interaction_plan"
    assert result["metadata"]["source"] == "jato_browser_interaction"
    assert result["metadata"]["requiresUserApproval"] is True
    assert result["metadata"]["actionCount"] == 1


def test_browser_click_confirmed_mcp_tool_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "browser_confirm_click",
        lambda url, action_id, confirmation_token, timeout_ms=12000, max_chars=6000: {
            "status": "ok",
            "action": "click",
            "actionId": action_id,
            "url": url,
            "resultUrl": "https://example.com/dashboard",
            "title": "Dashboard",
            "textPreview": "Dashboard text",
            "truncated": False,
            "limitations": [],
        },
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "browser_click_confirmed",
        {
            "url": "https://example.com/report",
            "action_id": "act_01",
            "confirmation_token": "token",
        },
    )

    assert result["tool"] == "browser_click_confirmed"
    assert result["metadata"]["confirmedAction"] is True
    assert result["metadata"]["actionId"] == "act_01"
    assert result["metadata"]["resultUrl"] == "https://example.com/dashboard"


def test_browser_type_confirmed_mcp_tool_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "browser_confirm_type",
        lambda url, action_id, confirmation_token, text, timeout_ms=12000, max_chars=6000: {
            "status": "ok",
            "action": "type",
            "actionId": action_id,
            "url": url,
            "resultUrl": url,
            "title": "Portal",
            "textPreview": "Typed page",
            "typedCharacters": len(text),
            "truncated": False,
            "limitations": [],
        },
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "browser_type_confirmed",
        {
            "url": "https://example.com/report",
            "action_id": "act_02",
            "confirmation_token": "token",
            "text": "Volvo EX40",
        },
    )

    assert result["tool"] == "browser_type_confirmed"
    assert result["metadata"]["confirmedAction"] is True
    assert result["metadata"]["actionId"] == "act_02"
    assert result["metadata"]["typedCharacters"] == 10


def test_query_country_snapshot_wraps_existing_snapshot_service(monkeypatch) -> None:
    def fake_build_country_snapshot(country: str, user_params=None, news_payload_override=None):
        return {
            "country": country,
            "kpis": {"volume": 100},
            "topBrands": [{"brand": "Volvo"} for _ in range(45)],
        }

    monkeypatch.setattr(
        jato_mcp_tools_service,
        "build_country_snapshot",
        fake_build_country_snapshot,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "query_country_snapshot",
        {"country": "Sweden", "include_sections": ["country", "kpis", "topBrands"]},
    )

    assert result["tool"] == "query_country_snapshot"
    assert result["metadata"]["source"] == "jato_country_snapshot"
    assert result["metadata"]["truncated"] is True
    assert result["data"]["country"] == "Sweden"
    assert result["data"]["kpis"]["volume"] == 100
    assert len(result["data"]["topBrands"]) == jato_mcp_tools_service.MAX_SECTION_ITEMS


def test_query_country_snapshot_falls_back_to_jato_data_country_alias(monkeypatch) -> None:
    calls: list[str] = []

    def fake_build_country_snapshot(country: str, user_params=None, news_payload_override=None):
        calls.append(country)
        if country == "Sweden":
            return {
                "country": country,
                "kpis": {"totalRows": 0, "brandCount": 0},
                "topModels": [],
                "topBrands": [],
                "powertrainMix": [],
            }
        return {
            "country": country,
            "kpis": {"totalRows": 33327, "cumulativeSales": 1182452},
            "topModels": [{"label": "EX40", "value": 2945}],
            "topBrands": [{"label": "VOLVO", "value": 12000}],
            "powertrainMix": [{"label": "BEV", "value": 25235}],
        }

    monkeypatch.setattr(
        jato_mcp_tools_service,
        "build_country_snapshot",
        fake_build_country_snapshot,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "query_country_snapshot",
        {"country": "Sweden", "include_sections": ["country", "kpis", "topModels", "powertrainMix"]},
    )

    assert calls[:2] == ["Sweden", "瑞典"]
    assert result["metadata"]["country"] == "Sweden"
    assert result["metadata"]["jatoCountry"] == "瑞典"
    assert result["data"]["country"] == "Sweden"
    assert result["data"]["jatoCountry"] == "瑞典"
    assert result["data"]["kpis"]["totalRows"] == 33327
    assert result["data"]["topModels"][0]["label"] == "EX40"
    assert result["data"]["powertrainMix"][0]["label"] == "BEV"


def test_query_msrp_pricing_reuses_lookup_service(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_lookup_current_msrp_from_db(**kwargs):
        captured.update(kwargs)
        return {"items": [{"model": "XC60", "msrp": 55000.0}]}

    monkeypatch.setattr(
        jato_mcp_tools_service.msrp_lookup_service,
        "lookup_current_msrp_from_db",
        fake_lookup_current_msrp_from_db,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "query_msrp_pricing",
        {"country": "Sweden", "model": "XC60", "max_items": 999},
    )

    assert captured["country"] == "瑞典"
    assert captured["model"] == "XC60"
    assert captured["max_items"] == 50
    assert result["metadata"]["country"] == "Sweden"
    assert result["metadata"]["jatoCountry"] == "瑞典"
    assert result["data"]["country"] == "Sweden"
    assert result["data"]["jatoCountry"] == "瑞典"
    assert result["data"]["items"][0]["model"] == "XC60"


def test_query_leasing_offers_reuses_service_and_returns_generic_financial_evidence(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(jato_mcp_tools_service, "get_session_factory", lambda: FakeSession)

    def fake_list_offers(_session, **kwargs):
        calls.append(kwargs)
        model = str(kwargs.get("model_name") or "Nimbus E BEV")
        return [{
            "offerId": f"offer-{model}",
            "countryCode": "SE",
            "modelName": model,
            "version": "Long Range",
            "status": "active",
            "termMonths": 36,
            "mileagePerYear": 15000,
            "effectiveMonthlyEur": 529.0 if model == "Nimbus E BEV" else 549.0,
            "residualValuePercent": 54.0,
            "totalContractCostEur": 19044.0,
            "sourceUrl": "https://example.test/lease",
        }]

    monkeypatch.setattr(
        jato_mcp_tools_service.lease_comparison_service,
        "list_offers",
        fake_list_offers,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "query_leasing_offers",
        {
            "country": "Sweden",
            "models": ["Nimbus E BEV", "Solaris One BEV"],
            "term_months": 36,
        },
    )

    assert [call["country"] for call in calls] == ["SE", "SE"]
    assert [call["model_name"] for call in calls] == ["Nimbus E BEV", "Solaris One BEV"]
    assert result["metadata"]["resultCount"] == 2
    assert result["data"]["leasingStats"]["monthlyPaymentEurMin"] == 529.0
    assert result["data"]["leasingStats"]["monthlyPaymentEurMax"] == 549.0
    assert result["data"]["coverageDiagnostics"]["diagnosis"] == "leasing_offers_available"


def test_query_msrp_pricing_adds_empty_coverage_diagnostics(monkeypatch) -> None:
    def fake_lookup_current_msrp_from_db(**_kwargs):
        return {"items": [], "queryModels": ["O5 BEV", "EV3"]}

    monkeypatch.setattr(
        jato_mcp_tools_service.msrp_lookup_service,
        "lookup_current_msrp_from_db",
        fake_lookup_current_msrp_from_db,
    )
    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "query_msrp_pricing",
        {"country": "Sweden", "models": ["O5 BEV", "EV3"]},
    )

    diagnostics = result["data"]["coverageDiagnostics"]
    assert diagnostics["diagnosis"] == "no_current_prices_for_requested_models"
    assert diagnostics["requested"]["models"] == ["O5 BEV", "EV3"]
    assert diagnostics["currentPriceRows"]["requestedCountry"] == 0
    assert "sourceRepairCandidates" not in diagnostics
    assert "source URL" in diagnostics["nextActions"][1]


def test_query_msrp_pricing_adds_reference_price_sample_without_faking_requested_model(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_lookup_current_msrp_from_db(**kwargs):
        calls.append(dict(kwargs))
        if kwargs.get("models"):
            return {"items": [], "queryModels": ["O5 BEV", "EV3"]}
        return {
            "items": [
                {"brand": "KIA", "model": "EV3", "trim": "Plus", "msrp": 39121.7, "currency": "EUR"},
                {"brand": "VOLVO", "model": "EX30", "trim": "Core", "msrp": 42130.4, "currency": "EUR"},
                {"brand": "VOLKSWAGEN", "model": "ID.4", "trim": "Pro", "msrp": 53165.2, "currency": "EUR"},
            ],
            "queryModels": [],
        }

    monkeypatch.setattr(
        jato_mcp_tools_service.msrp_lookup_service,
        "lookup_current_msrp_from_db",
        fake_lookup_current_msrp_from_db,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "query_msrp_pricing",
        {"country": "Sweden", "models": ["O5 BEV", "EV3"], "powertrain": "BEV"},
    )

    data = result["data"]
    diagnostics = data["coverageDiagnostics"]

    assert data["items"] == []
    assert data["priceStats"]["min"] == 39121.7
    assert data["priceStats"]["max"] == 53165.2
    assert data["referencePriceSample"]["requestedModelsMissing"] == ["O5 BEV", "EV3"]
    assert data["referencePriceSample"]["sampleModels"] == ["EV3", "EX30", "ID.4"]
    assert data["referencePriceSample"]["scope"]["country"] == "Sweden"
    assert data["referencePriceSample"]["scope"]["jatoCountry"] == "瑞典"
    assert diagnostics["diagnosis"] == "no_current_prices_for_requested_models"
    assert diagnostics["requested"]["country"] == "Sweden"
    assert diagnostics["requested"]["jatoCountry"] == "瑞典"
    assert diagnostics["referencePriceSample"]["dataStatus"] == "reference_price_sample"
    assert diagnostics["referencePriceSample"]["sampleCount"] == 3
    assert "sourceRepairCandidates" not in diagnostics
    assert calls[0]["country"] == "瑞典"
    assert calls[0]["models"] == ["O5 BEV", "EV3"]
    assert calls[1]["country"] == "瑞典"
    assert calls[1]["models"] is None
    assert calls[1]["model"] is None
    assert calls[1]["powertrain"] == "BEV"


def test_query_msrp_pricing_falls_back_to_market_reference_sample_when_brand_empty(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_lookup_current_msrp_from_db(**kwargs):
        calls.append(dict(kwargs))
        if kwargs.get("model") or kwargs.get("models"):
            return {"items": [], "queryModels": [kwargs.get("model") or ""]}
        if kwargs.get("brand"):
            return {"items": []}
        return {
            "items": [
                {"brand": "SKODA", "model": "ENYAQ", "trim": "85", "msrp": 39121.74, "currency": "EUR"},
                {"brand": "VOLKSWAGEN", "model": "TAYRON", "trim": "Life", "msrp": 53165.22, "currency": "EUR"},
            ],
            "queryModels": [],
        }

    monkeypatch.setattr(
        jato_mcp_tools_service.msrp_lookup_service,
        "lookup_current_msrp_from_db",
        fake_lookup_current_msrp_from_db,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "query_msrp_pricing",
        {"country": "Sweden", "brand": "Kia", "model": "Sportage HEV"},
    )

    diagnostics = result["data"]["coverageDiagnostics"]
    sample = diagnostics["referencePriceSample"]

    assert sample["scope"]["requestedBrand"] == "Kia"
    assert sample["scope"]["fallbackScope"] == "brand_sample_empty_market_reference_used"
    assert sample["priceStats"]["min"] == 39121.74
    assert sample["priceStats"]["max"] == 53165.22
    assert "market-level reference sample" in " ".join(sample["limitations"])
    assert any(call.get("brand") == "Kia" for call in calls)
    assert any(call.get("brand") is None and not call.get("model") and not call.get("models") for call in calls)


def test_route_msrp_arguments_keep_planned_multi_model_query() -> None:
    question = "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？"
    plan = {
        "toolPlan": [
            {
                "toolName": "query_msrp_pricing",
                "input": {
                    "country": "Sweden",
                    "question": question,
                    "model": "O5 BEV",
                    "models": ["O5 BEV", "EV3"],
                },
            }
        ]
    }

    args = jato_mcp_tools_service._build_route_tool_arguments(
        "query_msrp_pricing",
        {"country": "Sweden", "question": question},
        "Sweden",
        question,
        plan,
    )

    assert args["models"] == ["O5 BEV", "EV3"]
    assert "model" not in args


def test_search_market_news_reuses_web_search_service(monkeypatch) -> None:
    def fake_search_market_news(*, country: str, question: str, limit: int):
        return [{"title": f"{country}:{question}", "provider": "fake", "url": "https://example.com"}]

    monkeypatch.setattr(
        jato_mcp_tools_service.web_search_service,
        "search_market_news",
        fake_search_market_news,
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "search_market_news",
        {"country": "Germany", "question": "subsidy", "limit": 3},
    )

    assert result["metadata"]["limit"] == 3
    assert result["data"]["items"][0]["provider"] == "fake"


def test_pageindex_get_section_uses_live_client_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(jato_mcp_tools_service, "pageindex_configured", lambda: True)
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "pageindex_section",
        lambda section_id: {
            "status": "ok",
            "text": "Clause text",
            "citations": [{"url": "https://example.com/report.pdf"}],
            "summary": f"Retrieved {section_id}",
        },
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "pageindex_get_section",
        {"country": "Sweden", "question": "policy clause", "section_id": "sec-5"},
    )

    assert result["metadata"]["source"] == "pageindex_mcp"
    assert result["data"]["status"] == "live"
    assert result["data"]["sectionId"] == "sec-5"


def test_pageindex_list_documents_uses_live_client_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(jato_mcp_tools_service, "pageindex_configured", lambda: True)
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "pageindex_list",
        lambda: {
            "status": "ok",
            "documents": [{"id": "doc-1", "title": "EV Policy Report"}],
            "summary": "Found one document.",
        },
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "pageindex_list_documents",
        {"country": "Sweden"},
    )

    assert result["metadata"]["source"] == "pageindex_mcp"
    assert result["data"]["documents"][0]["id"] == "doc-1"


def test_minirag_explain_entity_uses_live_client_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(jato_mcp_tools_service, "minirag_configured", lambda: True)
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "minirag_explain",
        lambda entity: {
            "status": "ok",
            "relatedEntities": [{"entity": entity, "relationship": "affects"}],
            "evidence": [{"source": "graph"}],
            "summary": "Graph explanation ready.",
        },
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "minirag_explain_entity",
        {"country": "Sweden", "question": "J7 PHEV", "entity": "J7 PHEV"},
    )

    assert result["metadata"]["source"] == "minirag_live"
    assert result["data"]["status"] == "live"
    assert result["data"]["relatedEntities"][0]["entity"] == "J7 PHEV"


def test_query_segment_breakdown_preserves_relevant_cross_tabs_with_powertrain_filter(monkeypatch) -> None:
    monkeypatch.setattr(
        jato_query_tools,
        "build_country_snapshot",
        lambda *_args, **_kwargs: {
            "kpis": {"totalRows": 100},
            "topModels": [],
            "crossTabs": {
                "driveByFuel": [
                    {"_index": "BEV", "_total": 1000, "2WD_pct": 50.0},
                    {"_index": "HEV", "_total": 1946, "2WD_pct": 85.9, "4WD_pct": 14.1},
                ],
                "driveBySegment": [
                    {"_index": "SUV A0", "_total": 753, "2WD_pct": 92.0},
                    {"_index": "SUV A", "_total": 405, "2WD_pct": 78.0},
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
    )

    result = jato_query_tools.query_segment_breakdown("Sweden", powertrain="HEV")

    assert result["driveByFuel"] == [{"_index": "HEV", "_total": 1946, "2WD_pct": 85.9, "4WD_pct": 14.1}]
    assert [row["_index"] for row in result["driveBySegment"]] == ["SUV A0", "SUV A"]
    assert [row["_index"] for row in result["segmentByFuel"]] == ["SUV A0", "SUV A"]
    assert result["registrationByFuel"][0]["Business_pct"] == 54.0


def test_unknown_tool_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Unsupported JATO MCP tool"):
        jato_mcp_tools_service.call_jato_mcp_tool("unknown", {})
