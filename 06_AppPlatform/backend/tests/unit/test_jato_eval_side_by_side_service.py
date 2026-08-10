from __future__ import annotations

import json

import pytest

from app.services import jato_eval_service as eval_service


def test_business_review_removes_whole_source_candidate_boundary_sentence() -> None:
    value = (
        "相对定价判断：J7 HEV 应比 Sportage HEV 保持更强价格吸引力。"
        "补源状态：J7 HEV, Sportage HEV 只有搜索候选/来源草稿，需要审核后才能作为官方价格证据。"
        "价值解释：价差必须由配置和 TCO 共同证明。"
    )

    cleaned = eval_service._strip_source_candidate_boundary_sentence(value)

    assert "补源状态" not in cleaned
    assert "只有" not in cleaned
    assert "价值解释：价差必须由配置和 TCO 共同证明" in cleaned


def test_business_review_actions_filter_mismatched_powertrain_candidates() -> None:
    actions = [
        "P0 · 补齐 J7 HEV 与 Sportage HEV 的当前 MSRP 矩阵。",
        "P1 · 对比 543 900 kr Kia Sportage Plug-In Hybrid Advance。",
        "P1 · 补齐 HEV 配置价值和月供/RV。",
    ]

    filtered = eval_service._business_review_actions_for_question(
        actions,
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
    )

    assert actions[0] in filtered
    assert actions[2] in filtered
    assert all("Plug-In Hybrid" not in line for line in filtered)


def test_side_by_side_refreshes_pending_msrp_review_artifacts() -> None:
    record = {
        "comparisonId": "cmp_pending_msrp_artifacts",
        "validationType": "business",
        "questionId": "biz-pricing-pending-msrp",
        "category": "pricing",
        "country": "Sweden",
        "question": "EX30 和 EV3 怎么做价格对比？",
        "expectedIntent": "pricing_analysis",
        "astrbot": {
            "status": "ok",
            "answerPreview": "EX30 已抓到待审核价格观察，但还没有正式 current price。",
            "visualArtifacts": [
                {
                    "id": "artifact_msrp_source_repair_table",
                    "type": "table",
                    "title": "MSRP source validation table",
                }
            ],
            "sourceRepairCandidates": {
                "dataStatus": "source_draft_candidate_not_price_evidence",
                "missingOwnModelSource": True,
                "ownModel": [
                    {
                        "brand": "VOLVO",
                        "model": "EX30",
                        "candidateSourceType": "source_draft",
                        "draftStatus": "source_draft_available",
                        "reviewPendingRows": 2,
                        "reviewPendingStatus": "review_pending_not_current_price",
                        "reviewPendingObservations": [
                            {
                                "brand": "VOLVO",
                                "model": "EX30",
                                "trim": "Ultra",
                                "sourceMsrpValue": 559000,
                                "sourceCurrency": "SEK",
                                "msrpValue": 48608.7,
                                "currency": "EUR",
                                "sourceUrl": "https://www.volvocars.com/se/build/ex30-electric/",
                                "reviewStatus": "open",
                                "matchStatus": "review_required",
                                "evidenceStatus": "review_pending_not_current_price",
                            },
                            {
                                "brand": "VOLVO",
                                "model": "EX30",
                                "trim": "Plus",
                                "sourceMsrpValue": 457000,
                                "sourceCurrency": "SEK",
                                "msrpValue": 39739.13,
                                "currency": "EUR",
                                "sourceUrl": "https://www.volvocars.com/se/build/ex30-electric/",
                                "reviewStatus": "open",
                                "matchStatus": "review_required",
                                "evidenceStatus": "review_pending_not_current_price",
                            },
                        ],
                    }
                ],
                "competitorCorridor": [],
                "candidateCount": 1,
                "reviewPendingObservationCount": 2,
            },
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "entities": {"models": ["EX30"]},
                "toolResults": [],
                "missingEvidence": [{"name": "current_msrp", "impact": "weakens_answer"}],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "humanScoring": {"status": "pending"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    artifact_ids = [item["id"] for item in enriched["astrbot"]["visualArtifacts"]]

    assert artifact_ids[:3] == [
        "artifact_pending_msrp_review_chart",
        "artifact_pending_msrp_review_table",
        "artifact_msrp_source_repair_table",
    ]
    pending_table = next(
        item for item in enriched["astrbot"]["visualArtifacts"]
        if item["id"] == "artifact_pending_msrp_review_table"
    )
    assert pending_table["data"]["rows"][0]["localMsrp"] == "559,000 SEK"
    assert pending_table["spec"]["evidenceMode"] == "review_pending_not_current_price"


def _write_questions(path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "id": "q_structured_1",
            "category": "structured",
            "country": "Sweden",
            "question": "Summarize Sweden BEV market.",
            "expectedRetrievalPath": "structured_mcp",
            "expectedTools": ["query_country_snapshot"],
        },
        {
            "id": "q_structured_2",
            "category": "structured",
            "country": "Norway",
            "question": "Summarize Norway BEV market.",
            "expectedRetrievalPath": "structured_mcp",
            "expectedTools": ["query_country_snapshot"],
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _fake_astrbot_result(country: str, question: str) -> dict:
    return {
        "tool": "route_agent_request",
        "metadata": {
            "selectedTool": "query_country_snapshot",
            "answerComposer": "dpv4",
        },
        "data": {
            "answer": {
                "title": f"{country} answer",
                "direct": f"AstrBot answer for {question}",
                "bullets": ["Evidence backed"],
                "limitations": [],
                "composer": "dpv4",
                "structuredFollowUps": [
                    {
                        "id": "fu_test",
                        "label": "继续对比",
                        "question": "继续对比核心竞品。",
                        "intent": "compare",
                        "expectedTools": ["query_country_snapshot"],
                        "priority": 1,
                    }
                ],
                "visualArtifacts": [
                    {
                        "id": "artifact_test",
                        "type": "metric_cards",
                        "title": "Key metrics",
                        "data": [{"label": "Volume", "value": 10}],
                        "sourceEvidenceRefs": ["ev_1"],
                    }
                ],
                "businessSynthesisPlan": {
                    "executiveConclusion": "Market opportunity exists.",
                    "businessImplications": ["Prioritize the strongest segment."],
                    "recommendedActions": ["Build a competitor matrix."],
                    "evidenceAlignment": {"status": "aligned"},
                    "reportReadyBullets": ["Evidence-backed business point."],
                },
                "evidenceDigest": ["Volume = 10 units（fixture）"],
                "displayPlan": "Use metric cards and a market chart to show the data-backed conclusion.",
            },
            "retrievalClassification": {
                "primaryPath": "structured_mcp",
                "allPaths": ["structured_mcp"],
            },
            "evidencePack": {
                "items": [{"label": "snapshot"}],
                "sources": ["snapshot"],
                "limitations": [],
            },
            "evidencePackage": {
                "evidenceId": "evpkg_test",
                "intent": "market_overview",
                "country": country,
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "query_country_snapshot",
                        "query": {"country": country},
                        "success": True,
                        "rowCount": 10,
                        "freshness": "test",
                        "sourceType": "jato_parquet",
                        "summary": "snapshot",
                        "keyFindings": ["Volume exists"],
                        "evidenceRefs": [
                            {
                                "refId": "ev_1",
                                "label": "Volume",
                                "value": 10,
                                "unit": "units",
                                "source": "fixture",
                                "retrievedAt": "2026-06-12T00:00:00Z",
                            }
                        ],
                    }
                ],
                "missingEvidence": [],
            },
            "primaryResult": {
                "data": {
                    "chartSpecs": {"chartCount": 1},
                },
            },
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 0.5,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "totalScore": 0.9,
                "businessSynthesisScore": 0.8,
                "failures": [],
            },
            "modelUsage": {"status": "ok"},
        },
    }


def _fake_country_result(country: str, question: str) -> dict:
    return {
        "country": country,
        "question": question,
        "answer": f"CountryCopilot answer for {question}",
        "answerMode": "grounded-direct",
        "provider": "snapshot",
        "model": None,
        "providerReason": "",
        "intentRoute": "market_overview",
        "focusedIntents": ["market_overview"],
        "grounding": {
            "trust": {"confidence": "high"},
            "evidenceTables": [{"title": "Snapshot"}],
        },
        "evidencePack": {"sources": [{"source_id": "snapshot"}]},
        "chartLinks": [{"label": "chart"}],
    }


def _business_side_by_side_record(
    comparison_id: str,
    question_id: str,
    *,
    category: str = "pricing",
) -> dict:
    question = f"{question_id} business question"
    return {
        "comparisonId": comparison_id,
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": question_id,
        "category": category,
        "country": "Sweden",
        "question": question,
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "scoreSchema": eval_service._business_score_dimensions(),
        "astrbot": {
            "status": "ok",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": f"AstrBot answer for {question}",
            "scores": {"composite": 0.82},
            "qualityScore": {"totalScore": 0.82},
            "evidencePackage": {
                "evidenceId": f"evpkg_{comparison_id}",
                "intent": "pricing_analysis",
                "country": "Sweden",
                "confidence": "high",
                "toolResults": [],
                "missingEvidence": [],
            },
            "visualArtifacts": [],
            "followUps": [],
        },
        "countryCopilot": {
            "status": "ok",
            "answerPreview": f"CountryCopilot answer for {question}",
            "answerMode": "grounded-model",
            "provider": "snapshot",
            "sourceCount": 2,
        },
        "comparison": {
            "bothReturned": True,
            "requiresHumanScoring": True,
            "errorCount": 0,
        },
        "humanScoring": eval_service._initial_human_scoring("business"),
        "errors": {},
        "businessPlaybook": {},
        "failureTags": [],
    }


def _ok_business_judge_result(winner: str = "astrbot") -> dict:
    return {
        "status": "ok",
        "provider": {
            "provider": "openai",
            "model": "gpt-judge-test",
            "keySource": "OPENAI_API_KEY",
        },
        "scores": {
            "winner": winner,
            "notes": "Formal judge scored this existing record.",
            "failureTags": ["pm_insight_weak"] if winner == "countryCopilot" else [],
            "astrbotScores": {
                "intentAccuracy": 5,
                "toolSelection": 5,
                "grounding": 5,
                "pmInsight": 4,
                "actionability": 4,
                "artifactQuality": 4,
                "followUpValue": 4,
                "presentationReadiness": 4,
            },
            "countryCopilotScores": {
                "intentAccuracy": 3,
                "toolSelection": 3,
                "grounding": 3,
                "pmInsight": 3,
                "actionability": 3,
                "artifactQuality": 3,
                "followUpValue": 2,
                "presentationReadiness": 3,
            },
        },
    }


def test_auto_score_uses_actual_evidence_tools_and_tool_aliases() -> None:
    question_def = {
        "id": "biz-policy-001",
        "expectedRetrievalPath": "hybrid_rag",
        "expectedTools": ["search_market_news", "pageindex_search_documents"],
    }
    result = {
        "metadata": {"selectedTool": "external_research"},
        "data": {
            "retrievalClassification": {"primaryPath": "hybrid_rag"},
            "answer": {"direct": "Policy answer with sourced evidence."},
            "evidencePack": {
                "items": [{"label": "policy source"}],
                "sources": ["market_news"],
                "limitations": ["official policy detail pending"],
            },
            "evidencePackage": {
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "success": True,
                        "evidenceRefs": [],
                    },
                    {
                        "toolName": "search_market_news",
                        "success": True,
                        "evidenceRefs": [],
                    },
                ],
                "missingEvidence": [],
            },
        },
    }

    scores = eval_service._auto_score(question_def, result)

    assert scores["toolSelectionRelevance"] == 1.0
    assert scores["breakdown"]["tool"]["missing"] == []
    assert scores["breakdown"]["tool"]["actual"] == ["external_research", "search_market_news"]


def test_auto_score_tool_relevance_uses_expected_tool_recall() -> None:
    question_def = {
        "id": "biz-pricing-001",
        "expectedRetrievalPath": "structured_mcp",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
    }
    result = {
        "metadata": {"selectedTool": "query_msrp_pricing"},
        "data": {
            "retrievalClassification": {"primaryPath": "structured_mcp"},
            "answer": {"direct": "Pricing answer with partial evidence."},
            "evidencePack": {
                "items": [{"label": "msrp"}],
                "sources": ["msrp"],
                "limitations": [],
            },
            "evidencePackage": {
                "toolResults": [
                    {
                        "toolName": "query_msrp_pricing",
                        "success": True,
                        "evidenceRefs": [],
                    }
                ],
                "missingEvidence": [],
            },
        },
    }

    scores = eval_service._auto_score(question_def, result)

    assert scores["toolSelectionRelevance"] == 0.5
    assert scores["breakdown"]["tool"]["missing"] == ["compare_competitive_set"]
    assert scores["breakdown"]["tool"]["actual"] == ["query_msrp_pricing"]


def test_enrich_business_record_backfills_actual_tools_from_evidence_package() -> None:
    record = _business_side_by_side_record("cmp_actual_tools", "biz-policy-001", category="policy_news")
    record["expectedTools"] = ["search_market_news", "pageindex_search_documents"]
    record["astrbot"]["selectedTool"] = "external_research"
    record["astrbot"]["evidencePackage"] = {
        "toolResults": [
            {"toolName": "external_research", "success": True, "evidenceRefs": []},
            {"toolName": "search_market_news", "success": True, "evidenceRefs": []},
        ],
        "missingEvidence": [],
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    assert enriched["astrbot"]["actualTools"] == ["external_research", "search_market_news"]


def test_run_eval_side_by_side_question_persists_comparison(tmp_path, monkeypatch) -> None:
    questions_file = tmp_path / "eval_questions.jsonl"
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    _write_questions(questions_file)
    monkeypatch.setattr(eval_service, "_QUESTIONS_FILE", questions_file)
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )

    record = eval_service.run_eval_side_by_side_question("q_structured_1")

    assert record["questionId"] == "q_structured_1"
    assert record["astrbot"]["selectedTool"] == "query_country_snapshot"
    assert record["astrbot"]["actualTools"] == ["query_country_snapshot"]
    assert record["astrbot"]["scores"]["composite"] > 0
    assert record["astrbot"]["scores"]["breakdown"]["tool"]["actual"] == ["query_country_snapshot"]
    assert record["countryCopilot"]["answerMode"] == "grounded-direct"
    assert record["comparison"]["bothReturned"] is True
    assert record["humanScoring"]["status"] == "pending"
    assert side_by_side_file.exists()

    listed = eval_service.list_eval_side_by_side_results()
    assert listed["total"] == 1
    assert listed["summary"]["pendingHumanScoring"] == 1


def test_side_by_side_visual_artifact_shrink_preserves_table_rows() -> None:
    result = _fake_astrbot_result("Sweden", "J7 HEV pricing")
    answer = result["data"]["answer"]
    answer["visualArtifacts"] = [
        {
            "id": "artifact_pricing_table",
            "type": "table",
            "title": "Pricing evidence table",
            "subtitle": "Readable business table.",
            "data": {
                "rows": [
                    {
                        "model": "J7 HEV",
                        "powertrain": "HEV",
                        "msrp": "34,720 EUR",
                        "monthlyPayment": "待补",
                        "rv": "待补",
                        "pricePosition": "core corridor",
                        "action": "push high trim",
                        "evidenceRef": "ev_price",
                    }
                ],
                "intentAnalysis": {"template": "pricing_analysis"},
            },
            "spec": {
                "columns": ["model", "powertrain", "msrp", "monthlyPayment", "rv", "pricePosition", "action"],
                "maxRows": 10,
                "sortBy": "pricePosition",
                "businessExplanation": "Pricing table uses fixed decision columns.",
            },
            "sourceEvidenceRefs": ["ev_price"],
        }
    ]

    summary = eval_service._summarize_astrbot_side(result, {"composite": 0.9}, None)
    table = summary["visualArtifacts"][0]

    assert isinstance(table["data"]["rows"], list)
    assert table["data"]["rows"][0]["model"] == "J7 HEV"
    assert table["spec"]["columns"] == ["model", "powertrain", "msrp", "monthlyPayment", "rv", "pricePosition", "action"]


def test_side_by_side_astrbot_summary_counts_external_evidence_package_sources() -> None:
    result = _fake_astrbot_result("Sweden", "BEV subsidy price cap")
    data = result["data"]
    data["evidencePack"]["sources"] = ["transportstyrelsen.se"]
    data["evidencePackage"] = {
        "evidenceId": "evpkg_policy",
        "intent": "news_policy_search",
        "country": "Sweden",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "external_research",
                "success": True,
                "rowCount": 4,
                "sourceType": "web",
                "summary": "Four official Transportstyrelsen sources.",
                "evidenceRefs": [
                    {
                        "refId": "ev_1",
                        "label": "Bonus claim",
                        "value": "bonus ended",
                        "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                    },
                    {
                        "refId": "ev_2",
                        "label": "Malus claim",
                        "value": "malus",
                        "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/malus/",
                    },
                    {
                        "refId": "ev_3",
                        "label": "Vehicle tax claim",
                        "value": "vehicle tax",
                        "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/vehicle-tax/",
                    },
                    {
                        "refId": "ev_4",
                        "label": "Taxes fees claim",
                        "value": "taxes and fees",
                        "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/",
                    },
                ],
            }
        ],
        "missingEvidence": [],
    }

    summary = eval_service._summarize_astrbot_side(result, {"composite": 1.0}, None)

    assert summary["sourceCount"] == 4


def test_run_eval_side_by_side_category_honors_limit(tmp_path, monkeypatch) -> None:
    questions_file = tmp_path / "eval_questions.jsonl"
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    _write_questions(questions_file)
    monkeypatch.setattr(eval_service, "_QUESTIONS_FILE", questions_file)
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )

    result = eval_service.run_eval_side_by_side_category("structured", limit=1)

    assert result["total"] == 1
    assert result["summary"]["count"] == 1
    assert eval_service.list_eval_side_by_side_results(category="structured")["total"] == 1


def test_update_eval_side_by_side_human_score_persists_record(tmp_path, monkeypatch) -> None:
    questions_file = tmp_path / "eval_questions.jsonl"
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    _write_questions(questions_file)
    monkeypatch.setattr(eval_service, "_QUESTIONS_FILE", questions_file)
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    record = eval_service.run_eval_side_by_side_question("q_structured_1")

    updated = eval_service.update_eval_side_by_side_human_score(
        record["comparisonId"],
        {
            "status": "scored",
            "winner": "astrbot",
            "notes": "AstrBot used the expected structured tool.",
            "dimensions": record["humanScoring"]["dimensions"],
            "astrbotScores": {
                key: 5
                for key in record["humanScoring"]["dimensions"]
            },
            "countryCopilotScores": {
                key: 4
                for key in record["humanScoring"]["dimensions"]
            },
        },
    )

    assert updated["humanScoring"]["status"] == "scored"
    assert updated["humanScoring"]["winner"] == "astrbot"
    assert updated["humanScoring"]["notes"] == "AstrBot used the expected structured tool."
    assert updated["humanScoring"]["updatedAt"]

    listed = eval_service.list_eval_side_by_side_results()
    assert listed["summary"]["pendingHumanScoring"] == 0
    assert listed["items"][0]["humanScoring"]["winner"] == "astrbot"


def test_update_eval_side_by_side_llm_judge_score_persists_provider_metadata(tmp_path, monkeypatch) -> None:
    questions_file = tmp_path / "eval_questions.jsonl"
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    _write_questions(questions_file)
    monkeypatch.setattr(eval_service, "_QUESTIONS_FILE", questions_file)
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    record = eval_service.run_eval_side_by_side_question("q_structured_1")

    updated = eval_service.update_eval_side_by_side_human_score(
        record["comparisonId"],
        {
            "status": "scored",
            "source": "llm_judge",
            "judgeProvider": {
                "source": "gpt5_5_reference_judge",
                "pathId": "gpt5_5",
                "label": "GPT5.5 / GPT Judge",
                "provider": "openai",
                "model": "gpt-5.5",
                "apiBase": "https://api.openai.com/v1",
                "keySource": "OPENAI_API_KEY",
                "ignored": "not persisted",
            },
            "winner": "astrbot",
            "dimensions": record["humanScoring"]["dimensions"],
            "astrbotScores": {
                key: 5
                for key in record["humanScoring"]["dimensions"]
            },
            "countryCopilotScores": {
                key: 3
                for key in record["humanScoring"]["dimensions"]
            },
        },
    )

    provider = updated["humanScoring"]["judgeProvider"]
    assert updated["humanScoring"]["source"] == "llm_judge"
    assert provider == {
        "source": "gpt5_5_reference_judge",
        "pathId": "gpt5_5",
        "label": "GPT5.5 / GPT Judge",
        "provider": "openai",
        "model": "gpt-5.5",
        "apiBase": "https://api.openai.com/v1",
        "keySource": "OPENAI_API_KEY",
    }
    assert "ignored" not in provider


def test_business_validation_question_inventory_has_30_real_questions() -> None:
    inventory = eval_service.load_business_validation_questions()

    assert inventory["total"] == 30
    assert inventory["byCategory"]["pricing"] == 4
    assert inventory["byCategory"]["competitor_compare"] == 4
    assert inventory["byCategory"]["market_overview"] == 4
    assert inventory["byCategory"]["policy_news"] == 5
    assert inventory["byCategory"]["configuration"] == 3
    assert inventory["byCategory"]["inventory_bom"] == 4
    assert inventory["byCategory"]["voc"] == 3
    assert inventory["byCategory"]["report_generation"] == 3
    assert [item["key"] for item in inventory["scoreDimensions"]] == [
        "intentAccuracy",
        "toolSelection",
        "grounding",
        "pmInsight",
        "actionability",
        "artifactQuality",
        "followUpValue",
        "presentationReadiness",
    ]


def test_run_business_validation_question_persists_quality_context(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )

    record = eval_service.run_business_validation_question("biz-pricing-001")

    assert record["validationType"] == "business"
    assert record["expectedIntent"] == "pricing_analysis"
    assert "query_msrp_pricing" in record["expectedTools"]
    assert record["scoreSchema"][0]["key"] == "intentAccuracy"
    assert record["humanScoring"]["dimensions"] == [
        "intentAccuracy",
        "toolSelection",
        "grounding",
        "pmInsight",
        "actionability",
        "artifactQuality",
        "followUpValue",
        "presentationReadiness",
    ]
    assert record["id"] == record["comparisonId"]
    assert record["astrbotAnswer"].startswith("AstrBot answer for 瑞典 J7 HEV 应该怎么定价？")
    assert "直接结论：瑞典 J7 HEV 定价应围绕" not in record["astrbotAnswer"]
    assert "## 产品经理判断" in record["astrbotAnswer"]
    assert "## 下一步动作" in record["astrbotAnswer"]
    assert "## 汇报口径" in record["astrbotAnswer"]
    assert record["copilotAnswer"].startswith("CountryCopilot answer")
    assert record["comparison"]["astrbotAnswerChars"] == len(record["astrbotAnswer"])
    assert record["astrbotEvidencePackage"]
    refs = record["astrbotEvidencePackage"]["toolResults"][0]["evidenceRefs"]
    assert isinstance(refs, list)
    assert refs[0]["label"] == "Volume"
    assert record["astrbotVisualArtifacts"]
    assert record["astrbotFollowUps"]
    assert record["astrbotQualityScore"]
    assert record["astrbotEvidenceDigest"] == ["Volume = 10 units（fixture）"]
    assert record["astrbotDisplayPlan"].startswith("Use metric cards")
    assert "scores" in record
    assert "winner" in record
    assert "humanNotes" in record
    assert record["businessValidation"]["astrbotAnswer"].startswith("AstrBot answer for 瑞典 J7 HEV 应该怎么定价？")
    assert "Build a competitor matrix." in record["businessValidation"]["astrbotAnswer"]
    assert record["businessPlaybook"]["id"] == "pricing_analysis"
    assert "tool_missing" in record["failureTags"]
    assert record["businessValidation"]["failureTags"] == record["failureTags"]
    assert record["businessValidation"]["astrbotEvidenceDigest"] == ["Volume = 10 units（fixture）"]
    assert record["businessValidation"]["astrbotDisplayPlan"].startswith("Use metric cards")
    assert side_by_side_file.exists()


def test_business_review_answer_text_projects_structured_fields() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "直接结论：需要先验证价格走廊。",
        "bullets": ["Verdict：需要先验证价格走廊。", "Why：当前 MSRP 证据不足。"],
        "evidenceDigest": ["J7 高配 MSRP = 34,720 EUR", "竞品价格走廊 = 30,000-40,000 EUR"],
        "displayPlan": "用价格证据表展示 MSRP、竞品走廊和月供/RV 缺口。",
        "businessImplications": ["低配做价格锚点，高配做主推版本。"],
        "recommendedActions": [
            {
                "priority": "P0",
                "action": "补齐竞品 MSRP",
                "rationale": "定价需要价格走廊和月供口径。",
            }
        ],
        "reportReadyBullets": ["J7 HEV 应围绕核心竞争带中段组织定价。"],
        "limitations": ["Missing evidence: current_msrp."],
    })

    assert text.startswith("直接结论")
    assert "## 关键证据" in text
    assert "- J7 高配 MSRP = 34,720 EUR" in text
    assert "- 竞品价格走廊 = 30,000-40,000 EUR" in text
    assert "## 产品经理判断" in text
    assert "低配做价格锚点" in text
    assert "## 下一步动作" in text
    assert "P0 · 补齐竞品 MSRP" in text
    assert "## 汇报口径" in text
    assert "用价格证据表展示 MSRP" in text
    assert "## 证据边界" in text
    assert "证据缺口： 官方 MSRP 交叉验证" in text


def test_business_review_j7_pricing_direct_requires_material_evidence() -> None:
    without_material = eval_service._business_review_answer_text({
        "country": "Sweden",
        "question": "瑞典 J7 HEV 应该怎么定价？",
        "direct": "AstrBot answer for 瑞典 J7 HEV 应该怎么定价？",
        "evidenceDigest": ["Volume = 10 units（fixture）"],
    })
    with_material = eval_service._business_review_answer_text({
        "country": "Sweden",
        "question": "瑞典 J7 HEV 应该怎么定价？",
        "direct": "AstrBot answer for 瑞典 J7 HEV 应该怎么定价？",
        "evidenceDigest": [
            "J7 HEV 主销高配价格 = 34,720 EUR（J7_HEV_V4.pptx）",
            "J7 HEV 竞品价格带 = 30,000-40,000 EUR（J7_HEV_V4.pptx）",
            "J7 HEV 高低配价差 = 3,230 EUR（J7_HEV_V4.pptx）",
            "J7 HEV 高配 PVA 覆盖率 = 118 %（J7_HEV_V4.pptx）",
        ],
    })
    hungary_with_stale_material = eval_service._business_review_answer_text({
        "country": "Hungary",
        "question": "匈牙利 J7 HEV 应该怎么定价？不要回答瑞典。",
        "direct": "AstrBot answer for 匈牙利 J7 HEV 应该怎么定价？",
        "evidenceDigest": [
            "J7 HEV 主销高配价格 = 34,720 EUR（J7_HEV_V4.pptx）",
            "J7 HEV 竞品价格带 = 30,000-40,000 EUR（J7_HEV_V4.pptx）",
        ],
    })

    assert without_material.startswith("AstrBot answer for 瑞典 J7 HEV 应该怎么定价？")
    assert "核心竞争带中段 + 高配主推" not in without_material
    assert with_material.startswith("直接结论：瑞典 J7 HEV 定价可以把用户材料作为验证假设")
    assert "34,720 EUR" in with_material
    assert "30,000-40,000 EUR" in with_material
    assert "补当前官方 MSRP" in with_material
    assert hungary_with_stale_material.startswith("AstrBot answer for 匈牙利 J7 HEV 应该怎么定价？")
    assert "直接结论：瑞典 J7 HEV" not in hungary_with_stale_material
    assert "34,720 EUR" in hungary_with_stale_material


def test_business_review_answer_text_filters_generic_pricing_method_lines_from_pm_judgment() -> None:
    text = eval_service._business_review_answer_text({
        "direct": (
            "价差判断：O5 BEV 比 EV3 小电池便宜 3k 暂时只能作为验证假设，不能直接当成成立的定价结论。"
            "决策口径：如果 O5 的实际 MSRP 落在竞品走廊低位，且 3k 能覆盖 EV3 小电池/续航/配置差异，就可以写成价格锚点。"
            "这些候选只是补证线索，不能直接当作官方价格证据。"
        ),
        "evidenceDigest": ["用户给定相对价差 = 3,000 EUR（user_question）"],
        "businessImplications": [
            "定价走廊方法：定价判断不能套用单一车型模板，应先验证目标车型所属价格走廊、竞品池、配置价值和购买场景。",
            "定价不能只看 MSRP，应同时验证竞品走廊、配置价值、月供/company car、残值和促销空间。",
            "若缺少最新价格证据，第一版建议先输出价格矩阵模板和验证路径，而不是直接报价格。",
        ],
    })

    pm_section = text.split("## 产品经理判断", 1)[1].split("\n\n##", 1)[0]
    assert "如果 O5 的实际 MSRP 落在竞品走廊低位" in pm_section
    assert "这些候选只是补证线索" not in text
    assert "不能直接当作官方价格证据" not in pm_section
    assert "定价判断不能套用" not in pm_section
    assert "定价不能只看 MSRP" not in pm_section
    assert "第一版建议先输出价格矩阵模板" not in pm_section


def test_business_review_refreshes_stale_o5_ev3_background_price_wording() -> None:
    text = eval_service._business_review_answer_text({
        "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        "answerPreview": (
            "价差判断：O5 BEV 比 EV3 小电池便宜 3k 暂时只能作为验证假设，不能直接当成成立的定价结论。"
            "当前价格样本显示：样本走廊 39,121.7-53,165.2，中位数 52,130.4，均值 48,467.4。"
            "当前仍缺 O5/EV3 官方 MSRP 或版本价差证据，所以这只是场景判断，不是最终定价结论。"
        ),
        "evidenceDigest": [
            "本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证",
            "已物化价格样本 = SKODA ENYAQ",
        ],
    })

    lead = text.split("\n\n", 1)[0]
    assert "当前价格样本显示" not in lead
    assert "非本题核心车型的已物化价格背景" in lead
    assert "不能当作 O5/EV3 官方 MSRP 或竞品价格走廊" in lead


def test_business_review_answer_text_keeps_source_repair_out_of_direct_and_pm() -> None:
    source_repair_tail = (
        "来源草稿1项：EV3），确认 URL、版本/配置、币种、发布日期后生成当前价格记录；"
        "搜索候选和来源草稿都只是补证线索，不能直接当作官方价格证据。"
    )
    text = eval_service._business_review_answer_text({
        "direct": (
            "价差判断：O5 BEV 比 EV3 小电池便宜 3k 暂时只能作为验证假设。"
            "决策口径：如果 O5 的实际 MSRP 落在竞品走廊低位，且 3k 能覆盖 EV3 小电池/续航/配置差异，就可以写成价格锚点。"
            f"{source_repair_tail}"
        ),
        "evidenceDigest": [
            "用户给定相对价差 = 3,000 EUR（user_question）",
            "已定位 MSRP 来源草稿 = KIA EV3（待抽价/审核）",
        ],
        "businessImplications": [
            (
                "如果 O5 的实际价格不能比 EV3 形成清晰低位，就需要用版本配置、续航和主销配置重新定义价差。"
                f" {source_repair_tail}"
            )
        ],
        "recommendedActions": [
            {
                "priority": "P0",
                "action": "先在 MSRP review queue 中审核已抓到的官方价格观察。",
            }
        ],
    })

    direct_section = text.split("\n\n##", 1)[0]
    pm_section = text.split("## 产品经理判断", 1)[1].split("\n\n##", 1)[0]
    evidence_section = text.split("## 关键证据", 1)[1].split("\n\n##", 1)[0]

    assert "3k 暂时只能作为验证假设" in direct_section
    assert "实际价格不能比 EV3 形成清晰低位" in pm_section
    assert "来源草稿1项" not in direct_section
    assert "来源草稿1项" not in pm_section
    assert "搜索候选和来源草稿" not in direct_section
    assert "搜索候选和来源草稿" not in pm_section
    assert "不能直接当作官方价格证据" not in direct_section
    assert "不能直接当作官方价格证据" not in pm_section
    assert "已定位 MSRP 来源草稿 = KIA EV3" in evidence_section


def test_business_review_answer_text_uses_business_meaning_for_pricing_pm_judgment() -> None:
    text = eval_service._business_review_answer_text({
        "answerPreview": (
            "目标价判断：O9 在瑞典 53,000-55,000 EUR 可以继续验证，但不能直接定案。 "
            "展示骨架：先看 Pricing corridor chart。 "
            "业务含义：53k-55k EUR 若落在走廊上沿，就必须用大尺寸/高配、质保、公司车或 leasing 价值解释溢价；"
            "如果这些证据补不回来，应回到走廊中段或用 campaign/RV 支撑成交。"
        ),
        "evidenceDigest": ["用户目标价下沿 = 53,000 EUR（user_question）"],
        "businessImplications": [
            "定价走廊方法：定价判断不能套用单一车型模板，应先验证目标车型所属价格走廊、竞品池、配置价值和购买场景。",
        ],
    })

    assert "## 产品经理判断" in text
    pm_section = text.split("## 产品经理判断", 1)[1].split("\n\n##", 1)[0]
    assert "53k-55k EUR 若落在走廊上沿" in pm_section
    assert "定价判断不能套用" not in pm_section


def test_business_review_answer_text_filters_report_structure_lines_from_key_evidence() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "报告判断：J7 HEV 定价逻辑应压成一页产品定位结构。",
        "evidenceDigest": [
            "价格样本最低值 = 39,121.7（jato_msrp_postgres）",
            "市场窗口、竞品池、主销高配价位、PVA 覆盖和可见配置必须放在同一页，",
        ],
        "businessImplications": [
            "J7 HEV 报告页的业务含义是低配做价格锚点、高配做主推版本，而不是只展示一个建议价。",
        ],
    })

    key_evidence = text.split("## 关键证据", 1)[1].split("\n\n##", 1)[0]
    assert "价格样本最低值" in key_evidence
    assert "必须放在同一页" not in key_evidence


def test_business_review_answer_text_strips_pricing_method_prefix_but_keeps_material_judgment() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "直接结论：瑞典 J7 HEV 定价应围绕核心竞争带中段 + 高配主推。",
        "evidenceDigest": ["J7 HEV user material main trim MSRP = 34,720 EUR（J7_HEV_V4.pptx）"],
        "businessImplications": [
            "定价走廊方法：J7 HEV 定价方法：核心竞争带中段 + 高配主推，先用市场窗口和竞品走廊定位，再用配置价值解释高配。",
            "竞品池应锁定 Corolla Cross、RAV4、C-HR、Qashqai，价格判断落在 30,000-40,000 EUR 核心竞争带。",
        ],
    })

    pm_section = text.split("## 产品经理判断", 1)[1].split("\n\n##", 1)[0]
    assert "J7 HEV 定价方法：核心竞争带中段 + 高配主推" in pm_section
    assert "定价走廊方法" not in pm_section
    assert "Corolla Cross、RAV4、C-HR、Qashqai" in pm_section


def test_business_review_answer_text_keeps_report_actions_out_of_key_evidence() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "直接结论：瑞典 BEV 渗透率变化需要转成产品定义检查项。",
        "evidenceDigest": [
            "contextSnapshot.动力类型Mix.BEV.sales = 25,235 units（jato_country_chart_deck）",
            "contextSnapshot.动力类型Mix.PHEV.sales = 15,028 units（jato_country_chart_deck）",
        ],
        "reportReadyBullets": [
            "Title：瑞典 BEV 渗透率对产品定义的影响",
            "Key message：BEV 增长正在改变 SUV A0/A 的配置门槛。",
            "Evidence：SUV A BEV 渗透率 = 40.0%",
            "Product implication：BEV 渗透率变化要先转成产品定义检查项。",
            "Next action：补齐 BEV 年/月度渗透率证据后再决定产品节奏。",
        ],
    })

    key_evidence_section = text.split("## 关键证据", 1)[1].split("\n\n##", 1)[0]

    assert "BEV 动力销量 = 25,235 units" in key_evidence_section
    assert "contextSnapshot.动力类型Mix" not in key_evidence_section
    assert "SUV A BEV 渗透率 = 40.0%" in key_evidence_section
    assert "Next action" not in key_evidence_section
    assert "补齐 BEV 年/月度渗透率证据" not in key_evidence_section
    assert "Product implication" not in key_evidence_section
    assert "产品定义检查项" not in key_evidence_section


def test_business_review_answer_text_rewrites_cross_country_market_context_labels() -> None:
    text = eval_service._business_review_answer_text({
        "direct": (
            "市场上下文：crossCountry.Sweden.动力类型Mix.BEV.sales = 25,235 units（JATO 跨国对比），"
            "crossCountry.Norway.powertrainMix.BEV.sales = 26,617 units（JATO 跨国对比）。"
        ),
        "evidenceDigest": [
            "crossCountry.Sweden.动力类型Mix.BEV.sales = 25,235 units（JATO 跨国对比）",
            "crossCountry.Norway.powertrainMix.BEV.sales = 26,617 units（JATO 跨国对比）",
        ],
        "businessImplications": [
            "北欧冬季配置判断要把市场结构转换成冬季包、热泵、座椅加热和续航冗余验证项。",
        ],
    })

    assert "瑞典 BEV 动力销量 = 25,235 units" in text
    assert "挪威 BEV 动力销量 = 26,617 units" in text
    assert "crossCountry." not in text
    assert "动力类型Mix" not in text


def test_business_review_answer_text_filters_stale_report_outline_gap_when_report_exists() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "直接结论：瑞典 J7 HEV 定价页应围绕核心竞争带中段 + 高配主推。",
        "evidenceDigest": [
            "J7 HEV user material main trim MSRP = 34,720 EUR（J7_HEV_V4.pptx）",
        ],
        "limitations": [
            "证据缺口：report outline（会削弱结论）。",
            "当前 query_msrp_pricing 返回 0 条 J7 HEV 的 MSRP 价格记录。",
        ],
        "reportReadyBullets": [
            "Title：瑞典 J7 HEV 定价逻辑",
            "Key message：核心竞争带中段 + 高配主推",
            "Evidence：主销高配 34,720 EUR 来自用户材料",
            "Product implication：低配做价格锚点，高配做主推版本",
            "Next action：补齐官网 MSRP 和竞品价格走廊",
        ],
    })

    assert "report outline" not in text.lower()
    assert "query_msrp_pricing" not in text
    assert "当前价格数据查询 返回 0 条" in text
    assert "## 汇报口径" in text


def test_business_review_answer_text_keeps_voc_fallback_actions_out_of_key_evidence() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "VOC 判断：瑞典当前不能把这些主题写成已验证高频吐槽。",
        "bullets": [
            "结论：当前缺少可追溯 VOC 来源，不能输出确定数字；下一步应补论坛/媒体/VOC 来源。",
            "证据：当前缺少可追溯 VOC 来源；JATO 市场数据只能作为背景，不能证明用户吐槽频次或高频主题。",
            "瑞典 VOC 判断要区分真实用户痛点、媒体观点、论坛噪音和可转化卖点（7 条可引用证据）。",
            "产品经理判断：没有可追溯来源时不能说高频吐槽，但可以先给主题假设。",
            "VOC 证据方法：当前不能输出确定数字，但可以先确定分析框架、竞品池、关键假设和补数优先级。",
            "证据有限但可推进：下一步执行：先按本轮外部研究检索线索补 VOC/媒体/论坛来源。",
            "展示：用来源表和主题表展示 VOC 来源、用户痛点和可转化卖点。",
            "证据安全检查：外部研究治理提醒。",
            "数据来源有限：仅依赖JATO工具可查询的公开销售数据，未覆盖瑞典本地社交媒体。",
            "OMODA/JAECOO在瑞典可能处于市场导入期，用户基数小，反馈样本不足。",
            "未获取到具体车型的瑞典市场配置细节或用户评价。",
            "JATO 销售数据中未包含 OMODA/JAECOO 的单独条目，可能因销量过低被归入其他类别。",
            "无法获取瑞典用户的一手吐槽数据，只能基于市场结构推断潜在问题。",
        ],
    })

    key_evidence_section = text.split("## 关键证据", 1)[1].split("\n\n##", 1)[0]

    assert "当前缺少可追溯 VOC 来源" in key_evidence_section
    assert "JATO 市场数据只能作为背景" in key_evidence_section
    assert "下一步应补论坛" not in key_evidence_section
    assert "下一步执行" not in key_evidence_section
    assert "产品经理判断" not in key_evidence_section
    assert "VOC 判断" not in key_evidence_section
    assert "VOC 证据方法" not in key_evidence_section
    assert "可引用证据" not in key_evidence_section
    assert "用来源表和主题表" not in key_evidence_section
    assert "证据安全检查" not in key_evidence_section
    assert "外部研究治理提醒" not in key_evidence_section
    assert "数据来源有限" not in key_evidence_section
    assert "用户基数小" not in key_evidence_section
    assert "未获取到具体车型" not in key_evidence_section
    assert "未包含 OMODA/JAECOO" not in key_evidence_section
    assert "无法获取瑞典用户" not in key_evidence_section


def test_business_review_answer_text_filters_competitor_method_lines_from_key_evidence() -> None:
    text = eval_service._business_review_answer_text({
        "direct": (
            "对标判断：J8 7座四驱打 Sorento 只能先作为场景型假设；"
            "当前缺少可引用的 Sorento/J8 价格、配置和销量证据。"
        ),
        "evidenceDigest": [],
        "keyTakeaways": [
            "瑞典市场的竞品判断应先锁定竞品池，再拆价格、尺寸/级别、动力、配置和用户场景（28 条可引用证据）。",
            "结论要落成定位话术：正面对抗、错位竞争或价格锚点，而不是只列车型名称；当前证据状态为部分对齐。",
            "Competitor table：待补可引用证据",
            "Feature delta：待补可引用证据",
            "Positioning statement：竞品定位方法：竞品对比先定义对标关系，再判断正面对抗、错位竞争或价格锚点。",
        ],
    })

    key_evidence_section = text.split("## 关键证据", 1)[1].split("\n\n##", 1)[0]

    assert "Competitor table：待补可引用证据" in key_evidence_section
    assert "Feature delta：待补可引用证据" in key_evidence_section
    assert "28 条可引用证据" not in key_evidence_section
    assert "竞品判断应先锁定" not in key_evidence_section
    assert "结论要落成定位话术" not in key_evidence_section
    assert "Positioning statement" not in key_evidence_section
    assert "竞品定位方法" not in key_evidence_section


def test_business_review_answer_text_filters_generic_method_lines_from_pm_judgment() -> None:
    text = eval_service._business_review_answer_text({
        "direct": (
            "对标判断：O5 BEV 应优先用 EX30 做主对标，EV3 做价格/配置校验锚点。"
            "产品动作：输出 O5 的可赢点、短板、价格边界和补证清单。"
        ),
        "evidenceDigest": ["EX30.sales = 1,518 units（jato_cross_reference）"],
        "businessImplications": [
            "竞品定位方法：竞品对比先定义对标关系，再判断正面对抗、错位竞争或价格锚点。",
            "结论要能转成配置、价格、销售话术和报告页。",
        ],
        "recommendedActions": [{"action": "补齐 O5 / EX30 / EV3 价格配置矩阵。"}],
    })

    assert "## 产品经理判断" in text
    pm_section = text.split("## 产品经理判断", 1)[1].split("\n\n##", 1)[0]
    assert "输出 O5 的可赢点、短板、价格边界和补证清单" in pm_section
    assert "竞品定位方法" not in pm_section
    assert "正面对抗、错位竞争" not in pm_section
    assert "结论要能转成" not in pm_section


def test_business_review_answer_text_filters_market_method_lines_from_pm_judgment() -> None:
    text = eval_service._business_review_answer_text({
        "direct": (
            "直接结论：瑞典 SUV A0/A 是主销结构，不是因为泛 SUV 热，而是覆盖家庭空间、城市通勤、冬季通过性和公司车/私人两用。"
            "对 OMODA/JAECOO 的动作不是泛泛上 SUV，而是把 BEV/HEV/PHEV 分别落到 SUV A0/A 的价格带、续航/冬季包和配置价值。"
        ),
        "evidenceDigest": [
            "contextSnapshot.crossTabs.driveBySegment.SUV A.sales = 7,544 units（jato_country_chart_deck）",
        ],
        "businessImplications": [
            "市场机会方法：市场数据要落到机会 segment 和动力路线选择，不能只复述销量或份额。",
            "对 OMODA/JAECOO 的价值在于识别优先进入的级别、动力和竞品锚点。",
            "输出视图：已生成 关键指标卡 和 市场决策表，把销量/份额、动力结构、级别结构和主销车型 转成机会 segment 与产品动作。",
        ],
    })

    assert "## 产品经理判断" in text
    pm_section = text.split("## 产品经理判断", 1)[1].split("\n\n##", 1)[0]
    assert "对 OMODA/JAECOO 的动作不是泛泛上 SUV" in pm_section
    assert "市场机会方法" not in pm_section
    assert "市场数据要落到机会 segment" not in pm_section
    assert "对 OMODA/JAECOO 的价值在于识别" not in pm_section
    assert "输出视图" not in pm_section
    assert "已生成 关键指标卡" not in pm_section


def test_business_review_answer_text_ignores_stale_preview_pm_display_bullets() -> None:
    text = eval_service._business_review_answer_text({
        "question": "北欧 BEV 增长是否会压缩 HEV 空间？",
        "answerPreview": (
            "直接结论：北欧 BEV 增长会压缩 HEV 空间，但不是把 HEV 一次性替代掉；"
            "BEV 会优先吃掉公司车、电动化 SUV 和政策敏感需求，HEV 仍保留无充电条件、价格敏感和低使用风险用户。\n\n"
            "## 产品经理判断\n"
            "- 输出视图：已生成 关键指标卡 和 市场决策表，把销量/份额、动力结构、级别结构和主销车型 转成机会 segment 与产品动作。\n\n"
            "## 下一步动作\n"
            "- P0 · 拆到车型/品牌"
        ),
        "evidenceDigest": [
            "瑞典 BEV 动力销量 = 25,235 units（JATO 跨国对比）",
            "瑞典 HEV 动力销量 = 5,051 units（JATO 跨国对比）",
        ],
        "businessImplications": [
            "市场机会方法：市场数据要落到机会 segment 和动力路线选择，不能只复述销量或份额。",
        ],
    })

    pm_section = text.split("## 产品经理判断", 1)[1].split("\n\n##", 1)[0]
    assert "BEV 增长会压缩 HEV 空间" in pm_section
    assert "HEV 仍保留无充电条件" in pm_section
    assert "输出视图" not in pm_section
    assert "关键指标卡" not in pm_section


def test_business_review_answer_text_filters_configuration_method_lines_from_pm_judgment() -> None:
    text = eval_service._business_review_answer_text({
        "direct": (
            "电池判断：A0 SUV BEV 的 80kWh 应定位为长续航/高配安全边界，低配仍要保留价格锚点。"
            "用户场景：私人家庭、郊区通勤、周末长途或公司车更需要 80kWh。"
        ),
        "evidenceDigest": ["configuration_delta.Battery size = 80kWh high-trim boundary（variant_compare）"],
        "businessImplications": [
            "配置价值方法：配置结论必须连接用户场景，例如冬季、拖车、充电、家庭出行和公司车使用。",
            "缺工程配置时先输出配置验证清单和主销配置假设。",
        ],
    })

    assert "## 产品经理判断" in text
    pm_section = text.split("## 产品经理判断", 1)[1].split("\n\n##", 1)[0]
    assert "A0 SUV BEV 的 80kWh 应定位为长续航/高配安全边界" in pm_section
    assert "配置价值方法" not in pm_section
    assert "配置结论必须连接用户场景" not in pm_section
    assert "缺工程配置时先输出" not in pm_section


def test_enrich_business_record_recomputes_stale_competitor_digest() -> None:
    record = {
        "comparisonId": "cmp_j8_sorento_stale_digest",
        "validationType": "business",
        "questionId": "biz-compare-003",
        "category": "competitor_compare",
        "country": "Sweden",
        "question": "J8 7 座四驱为什么能打 Sorento？",
        "expectedIntent": "competitor_compare",
        "expectedTools": ["compare_competitive_set"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": (
                "对标判断：J8 7座四驱打 Sorento 只能先作为场景型假设；"
                "当前缺少可引用的 Sorento/J8 价格、配置和销量证据。"
            ),
            "evidenceDigest": [
                "MODEL Y.sales = 2,412 units（jato_cross_reference）",
                "competitor.4.model = EX30（jato_cross_reference）",
            ],
            "keyTakeaways": [
                "瑞典市场的竞品判断应先锁定竞品池，再拆价格、尺寸/级别、动力、配置和用户场景（17 条可引用证据）。",
                "Competitor table：待补可引用证据",
                "Feature delta：待补可引用证据",
            ],
            "visualArtifacts": [
                {
                    "id": "artifact_competitor_evidence_chart",
                    "type": "chart",
                    "title": "Competitor sales chart",
                    "data": [{"label": "MODEL Y", "value": 2412}],
                },
                {
                    "id": "artifact_competitor_compare_table",
                    "type": "table",
                    "title": "Competitor comparison table",
                    "data": {"rows": [{"model": "EX30"}]},
                },
            ],
            "evidencePackage": {
                "intent": "competitor_compare",
                "entities": {"countries": ["Sweden"], "models": ["J8"], "competitors": []},
                "toolResults": [
                    {
                        "toolName": "compare_competitive_set",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_modely", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato_cross_reference"},
                            {"refId": "ev_ex30_model", "label": "competitor.4.model", "value": "EX30", "source": "jato_cross_reference"},
                        ],
                    }
                ],
                "missingEvidence": [
                    {
                        "name": "competitor_sales",
                        "reason": "No Sorento/J8 direct competitor metric rows.",
                        "impact": "weakens_answer",
                    }
                ],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    assert enriched["astrbot"]["evidenceDigest"] == []
    assert enriched["astrbotEvidenceDigest"] == []
    assert "MODEL Y" not in enriched["astrbotAnswer"]
    assert "EX30" not in enriched["astrbotAnswer"]
    refreshed_artifact_ids = [item["id"] for item in enriched["astrbot"]["visualArtifacts"]]
    assert "artifact_competitor_evidence_chart" not in refreshed_artifact_ids
    assert "artifact_competitor_compare_table" not in refreshed_artifact_ids
    assert "MODEL Y" not in str(enriched["astrbot"]["visualArtifacts"])
    assert "EX30" not in str(enriched["astrbot"]["visualArtifacts"])
    key_evidence_section = enriched["astrbotAnswer"].split("## 关键证据", 1)[1].split("\n\n##", 1)[0]
    assert "Competitor table：待补可引用证据" in key_evidence_section
    assert "Feature delta：待补可引用证据" in key_evidence_section
    assert "结论要能转成" not in key_evidence_section
    assert "用竞品矩阵展示" not in key_evidence_section
    assert "数据缺失" not in key_evidence_section
    assert "不能作为" not in key_evidence_section


def test_enrich_business_record_uses_market_context_for_j8_sorento() -> None:
    record = {
        "comparisonId": "cmp_j8_sorento_market_context",
        "validationType": "business",
        "questionId": "biz-compare-003",
        "category": "competitor_compare",
        "country": "Sweden",
        "question": "J8 7 座四驱为什么能打 Sorento？",
        "expectedIntent": "competitor_compare",
        "expectedTools": ["compare_competitive_set", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "对标判断：J8 7座四驱打 Sorento 只能先作为场景型假设。",
            "evidencePackage": {
                "intent": "competitor_compare",
                "entities": {"countries": ["Sweden"], "models": ["J8", "Sorento"], "competitors": ["Sorento"]},
                "toolResults": [
                    {
                        "toolName": "compare_competitive_set",
                        "query": {"country": "Sweden", "question": "J8 7 座四驱为什么能打 Sorento？"},
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_unrelated", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato_cross_reference"},
                        ],
                    },
                    {
                        "toolName": "build_market_chart",
                        "query": {"country": "Sweden", "question": "J8 7 座四驱为什么能打 Sorento？"},
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                            {"refId": "ev_suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "ev_suv_a_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct", "value": 60.1, "unit": "%", "source": "jato_country_chart_deck"},
                            {"refId": "ev_suv_a_phev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.PHEV_pct", "value": 38.2, "unit": "%", "source": "jato_country_chart_deck"},
                        ],
                    },
                ],
                "missingEvidence": [
                    {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "reason": "Need J8/Sorento MSRP.", "impact": "weakens_answer"},
                    {"name": "coverage_diagnostic:no_config_projects_for_country", "reason": "Need J8/Sorento configuration matrix.", "impact": "weakens_answer"},
                    {"name": "competitive_or_configuration_data_unavailable", "reason": "Need direct competitor configuration evidence.", "impact": "weakens_answer"},
                ],
                "confidence": "medium",
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    answer = enriched["astrbotAnswer"]
    digest = "\n".join(enriched["astrbot"]["evidenceDigest"])

    assert "市场场景支撑" in answer
    assert "SUV A 4WD 占比 60.1 %" in answer
    assert "SUV A 细分销量 7,544 units" in answer
    assert "PHEV 公司车注册占比 64.8 %" in answer
    assert "SUV A PHEV 渗透率 38.2 %" in answer
    assert "PHEV 公司车注册占比 = 64.8 %" in digest
    assert "SUV A 4WD 占比 = 60.1 %" in digest
    assert "SUV A PHEV 渗透率 = 38.2 %" in digest
    assert digest.index("SUV A 4WD 占比 = 60.1 %（JATO 图表数据）") < digest.index("本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证")
    table = next(
        item
        for item in enriched["astrbot"]["visualArtifacts"]
        if item["id"] == "artifact_competitor_compare_framework_table"
    )
    table_rows = table["data"]["rows"]
    serialized_table = str(table_rows)
    assert table_rows[0]["model"] == "市场场景证据"
    assert table_rows[0]["segment"] == "SUV A"
    assert table_rows[0]["powertrain"] == "4WD"
    assert "SUV A 4WD 占比 = 60.1 %" in table_rows[0]["keyAdvantage"]
    assert "PHEV 公司车注册占比 = 64.8 %" in serialized_table
    assert "SUV A 细分销量 = 7,544 units" in serialized_table
    assert "待补官方 MSRP / 月供 / RV" in serialized_table
    assert "不能只凭市场场景判定已胜出" in serialized_table
    assert "MODEL Y" not in answer
    assert "\n- 1 %" not in answer
    assert "\n- 2 %" not in answer


def test_enrich_business_record_refreshes_o5_competitor_role_matrix() -> None:
    record = {
        "comparisonId": "cmp_o5_competitor_roles",
        "validationType": "business",
        "questionId": "biz-compare-002",
        "category": "competitor_compare",
        "country": "Sweden",
        "question": "O5 BEV 应该对标 EX30 还是 EV3？",
        "expectedIntent": "competitor_compare",
        "expectedTools": ["compare_competitive_set"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": (
                "对标判断：O5 BEV 应优先用 EX30 做主对标，EV3 做价格/配置校验锚点。"
                "竞品角色：EX30 帮 O5 判断目标用户、品牌心智和产品定位；"
                "EV3 帮 O5 验证价格带、配置价值和购买替代理由。"
            ),
            "visualArtifacts": [
                {
                    "id": "artifact_competitor_compare_table",
                    "type": "table",
                    "title": "Competitor comparison table",
                    "data": {"rows": [{"model": "EX30", "keyAdvantage": "Sales 1518 units"}]},
                }
            ],
            "evidencePackage": {
                "intent": "competitor_compare",
                "entities": {"countries": ["Sweden"], "models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
                "toolResults": [
                    {
                        "toolName": "compare_competitive_set",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                        ],
                    }
                ],
                "missingEvidence": [
                    {"name": "current_msrp", "reason": "Need O5/EV3 MSRP.", "impact": "weakens_answer"},
                ],
                "confidence": "medium",
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    table = next(
        artifact
        for artifact in enriched["astrbot"]["visualArtifacts"]
        if artifact["id"] == "artifact_competitor_compare_table"
    )
    rows = table["data"]["rows"]
    ex30 = next(row for row in rows if row["model"] == "EX30")
    ev3 = next(row for row in rows if row["model"] == "EV3")

    assert ex30["segment"] == "主对标"
    assert "Sales 1,518 units" in ex30["keyAdvantage"]
    assert "目标用户、品牌心智和产品定位" in ex30["keyAdvantage"]
    assert ev3["segment"] == "价格/配置校验锚点"
    assert "价格带、配置价值和购买替代理由" in ev3["keyAdvantage"]
    assert "判断价差是否成立" in ev3["productImplication"]


def test_enrich_business_record_refreshes_market_overview_artifacts() -> None:
    record = {
        "comparisonId": "cmp_market_stale_artifacts",
        "validationType": "business",
        "questionId": "biz-market-001",
        "category": "market_overview",
        "country": "Sweden",
        "question": "瑞典 HEV 市场为什么适合 J7？",
        "expectedIntent": "market_overview",
        "expectedTools": ["query_country_snapshot", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "瑞典 HEV 市场判断应落到动力结构和机会 segment。",
            "visualArtifacts": [
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
                {"id": "artifact_metric_cards", "type": "metric_cards", "title": "Key metrics"},
            ],
            "evidencePackage": {
                "intent": "market_overview",
                "country": "Sweden",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "build_market_chart",
                        "success": True,
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "hev_sales", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "segment_hev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.HEV_pct", "value": 38.2, "unit": "%", "source": "jato_country_chart_deck"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    artifact_ids = [item["id"] for item in enriched["astrbot"]["visualArtifacts"]]

    assert artifact_ids[:2] == ["artifact_market_powertrain_mix_chart", "artifact_market_overview_table"]
    assert "artifact_metric_cards" in artifact_ids


def test_side_by_side_market_digest_prioritizes_requested_hev_for_j7_fit() -> None:
    evidence_package = {
        "intent": "market_overview",
        "country": "Sweden",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 6,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {
                        "refId": "ev_bev_sales",
                        "label": "contextSnapshot.powertrainMix.BEV.sales",
                        "value": 25235,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "ev_phev_sales",
                        "label": "contextSnapshot.powertrainMix.PHEV.sales",
                        "value": 15028,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "ev_mhev_sales",
                        "label": "contextSnapshot.powertrainMix.MHEV.sales",
                        "value": 8515,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "ev_hev_sales",
                        "label": "contextSnapshot.powertrainMix.HEV.sales",
                        "value": 5051,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "ev_reev_sales",
                        "label": "contextSnapshot.powertrainMix.REEV.sales",
                        "value": 2,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                ],
            }
        ],
        "missingEvidence": [],
    }

    digest = eval_service._side_by_side_evidence_digest_from_package(
        evidence_package,
        question="瑞典 HEV 市场为什么适合 J7？",
    )

    assert digest[0] == "HEV 动力销量 = 5,051 units（JATO 图表数据）"
    assert digest[1] == "BEV 动力销量 = 25,235 units（JATO 图表数据）"
    assert digest[2] == "PHEV 动力销量 = 15,028 units（JATO 图表数据）"
    assert digest.index("HEV 动力销量 = 5,051 units（JATO 图表数据）") < digest.index(
        "PHEV 动力销量 = 15,028 units（JATO 图表数据）"
    )
    assert digest.index("PHEV 动力销量 = 15,028 units（JATO 图表数据）") < digest.index(
        "MHEV 动力销量 = 8,515 units（JATO 图表数据）"
    )
    assert not any("REEV" in line for line in digest[:4])
    assert digest.index("HEV 动力销量 = 5,051 units（JATO 图表数据）") < digest.index(
        "MHEV 动力销量 = 8,515 units（JATO 图表数据）"
    )
    enriched = eval_service._enrich_business_record_for_read(
        {
            "comparisonId": "cmp_market_hev_digest",
            "validationType": "business",
            "questionId": "biz-market-001",
            "category": "market_overview",
            "country": "Sweden",
            "question": "瑞典 HEV 市场为什么适合 J7？",
            "expectedIntent": "market_overview",
            "humanScoring": {"status": "pending"},
            "astrbot": {
                "answerPreview": "瑞典 HEV 市场判断应落到动力结构和机会 segment。",
                "evidencePackage": evidence_package,
            },
            "countryCopilot": {"answerPreview": "Copilot answer"},
        }
    )
    answer_preview = enriched["astrbot"]["answerPreview"]
    assert answer_preview.startswith("直接结论：瑞典 HEV 市场对 J7 是值得继续验证的机会")
    assert "HEV 动力销量 5,051 units" in answer_preview
    assert "下一步应补 HEV + SUV A0/A 结构" in answer_preview


def test_side_by_side_market_digest_prioritizes_requested_bev_and_hev_over_phev() -> None:
    evidence_package = {
        "intent": "market_overview",
        "country": "Sweden",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "query_cross_country",
                "success": True,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {
                        "refId": "ev_se_bev",
                        "label": "crossCountry.Sweden.powertrainMix.BEV.sales",
                        "value": 25235,
                        "unit": "units",
                        "source": "jato_cross_country",
                    },
                    {
                        "refId": "ev_se_phev",
                        "label": "crossCountry.Sweden.powertrainMix.PHEV.sales",
                        "value": 15028,
                        "unit": "units",
                        "source": "jato_cross_country",
                    },
                    {
                        "refId": "ev_se_hev",
                        "label": "crossCountry.Sweden.powertrainMix.HEV.sales",
                        "value": 5051,
                        "unit": "units",
                        "source": "jato_cross_country",
                    },
                    {
                        "refId": "ev_fi_bev",
                        "label": "crossCountry.Finland.powertrainMix.BEV.sales",
                        "value": 8062,
                        "unit": "units",
                        "source": "jato_cross_country",
                    },
                    {
                        "refId": "ev_fi_phev",
                        "label": "crossCountry.Finland.powertrainMix.PHEV.sales",
                        "value": 2483,
                        "unit": "units",
                        "source": "jato_cross_country",
                    },
                    {
                        "refId": "ev_fi_hev",
                        "label": "crossCountry.Finland.powertrainMix.HEV.sales",
                        "value": 2267,
                        "unit": "units",
                        "source": "jato_cross_country",
                    },
                ],
            }
        ],
        "missingEvidence": [],
    }

    digest = eval_service._side_by_side_evidence_digest_from_package(
        evidence_package,
        question="北欧 BEV 增长是否会压缩 HEV 空间？",
    )

    joined_first_four = "\n".join(digest[:4])
    assert "瑞典 BEV 动力销量 = 25,235 units（JATO 跨国对比）" in joined_first_four
    assert "瑞典 HEV 动力销量 = 5,051 units（JATO 跨国对比）" in joined_first_four
    assert "芬兰 BEV 动力销量 = 8,062 units（JATO 跨国对比）" in joined_first_four
    assert "芬兰 HEV 动力销量 = 2,267 units（JATO 跨国对比）" in joined_first_four
    assert all("PHEV" not in line for line in digest[:4])


def test_side_by_side_market_digest_includes_j7_user_material_for_market_fit() -> None:
    evidence_package = {
        "intent": "market_overview",
        "country": "Sweden",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {
                        "refId": "ev_hev_sales",
                        "label": "contextSnapshot.powertrainMix.HEV.sales",
                        "value": 5051,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "ev_bev_sales",
                        "label": "contextSnapshot.powertrainMix.BEV.sales",
                        "value": 25235,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                ],
            },
            {
                "toolName": "business_method_material",
                "success": True,
                "sourceType": "generated",
                "evidenceRefs": [
                    {
                        "refId": "method_market_window",
                        "label": "J7 HEV user material market window",
                        "value": "瑞典 HEV 机会应先看 SUV A0 / SUV A 需求集中度、丰田系主导格局和竞品换代/交付窗口。",
                        "source": "J7_HEV_V4.pptx",
                        "table": "business_method_material",
                    },
                    {
                        "refId": "method_competitor_pool",
                        "label": "J7 HEV user material competitor pool",
                        "value": "Corolla Cross, RAV4, C-HR, Qashqai",
                        "source": "J7_HEV_V4.pptx",
                        "table": "business_method_material",
                    },
                ],
            },
        ],
        "missingEvidence": [],
    }

    digest = eval_service._side_by_side_evidence_digest_from_package(
        evidence_package,
        question="瑞典 HEV 市场为什么适合 J7？",
    )
    enriched = eval_service._enrich_business_record_for_read(
        {
            "comparisonId": "cmp_market_hev_user_material",
            "validationType": "business",
            "questionId": "biz-market-001",
            "category": "market_overview",
            "country": "Sweden",
            "question": "瑞典 HEV 市场为什么适合 J7？",
            "expectedIntent": "market_overview",
            "humanScoring": {"status": "pending"},
            "astrbot": {
                "answerPreview": "瑞典 HEV 市场判断应落到动力结构和机会 segment。",
                "evidencePackage": evidence_package,
            },
            "countryCopilot": {"answerPreview": "Copilot answer"},
        }
    )

    assert digest[0] == "HEV 动力销量 = 5,051 units（JATO 图表数据）"
    assert any(line.startswith("J7 HEV 市场窗口 = 瑞典 HEV 机会应先看") for line in digest)
    assert any("J7 HEV 竞品池 = Corolla Cross, RAV4, C-HR, Qashqai" in line for line in digest)
    assert "用户 J7_HEV 方法论补充了车型级进入窗口" in enriched["astrbot"]["answerPreview"]
    assert "Corolla Cross, RAV4, C-HR, Qashqai" in enriched["astrbot"]["answerPreview"]


def test_enrich_business_record_refreshes_stale_market_kpi_artifacts() -> None:
    record = {
        "comparisonId": "cmp_market_stale_kpis",
        "validationType": "business",
        "questionId": "biz-market-002",
        "category": "market_overview",
        "country": "Sweden",
        "question": "瑞典和芬兰销量差异为什么大？",
        "expectedIntent": "market_overview",
        "expectedTools": ["query_cross_country"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "瑞典和芬兰销量差异应先看市场体量和动力结构。",
            "visualArtifacts": [
                {"id": "artifact_market_powertrain_mix_chart", "type": "chart", "title": "Powertrain mix chart"},
                {
                    "id": "artifact_market_overview_table",
                    "type": "table",
                    "title": "Market decision table",
                    "data": {"rows": [{"signal": "totalRows"}, {"signal": "avgMsrp"}]},
                },
                {
                    "id": "artifact_metric_cards",
                    "type": "metric_cards",
                    "title": "Key metrics",
                    "data": {"rows": [{"label": "crossCountry.Sweden.kpis.totalRows"}, {"label": "crossCountry.Sweden.kpis.avgMsrp"}]},
                },
            ],
            "evidencePackage": {
                "intent": "market_overview",
                "country": "Sweden",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "query_cross_country",
                        "success": True,
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "se_sales", "label": "crossCountry.Sweden.kpis.cumulativeSales", "value": 1182452, "unit": "units", "source": "jato_cross_country"},
                            {"refId": "fi_sales", "label": "crossCountry.Finland.kpis.cumulativeSales", "value": 332237, "unit": "units", "source": "jato_cross_country"},
                            {"refId": "se_total", "label": "crossCountry.Sweden.kpis.totalRows", "value": 33327, "unit": "units", "source": "jato_cross_country"},
                            {"refId": "se_bev", "label": "crossCountry.Sweden.powertrainMix.BEV.value", "value": 25235, "unit": "units", "source": "jato_cross_country"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    artifacts_text = str(enriched["astrbot"]["visualArtifacts"])
    market_table = next(
        artifact
        for artifact in enriched["astrbot"]["visualArtifacts"]
        if artifact.get("id") == "artifact_market_overview_table"
    )
    table_signals = [row.get("signal") for row in market_table.get("data", {}).get("rows", [])]

    assert "totalRows" not in artifacts_text
    assert "avgMsrp" not in artifacts_text
    assert "Sweden 累计销量" in artifacts_text
    assert "Finland 累计销量" in table_signals
    assert "Sweden BEV 动力销量" in artifacts_text
    assert "瑞典 累计销量" in str(enriched["astrbot"]["evidenceDigest"])
    assert "芬兰 累计销量" in str(enriched["astrbot"]["evidenceDigest"])


def test_read_time_metric_cards_sanitize_internal_labels_and_sources() -> None:
    artifact = eval_service._sanitize_side_by_side_visual_artifact_for_read(
        {
            "id": "artifact_metric_cards",
            "type": "metric_cards",
            "title": "Key metrics",
            "data": {
                "rows": [
                    {
                        "label": "contextSnapshot.powertrainMix.BEV.sales",
                        "value": 25235,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                        "sourceEvidenceRef": "ev_bev_sales",
                    },
                    {
                        "label": "avgMsrp",
                        "value": 57954,
                        "unit": "EUR",
                        "source": "jato_country_snapshot",
                        "sourceEvidenceRef": "ev_avg_msrp",
                    },
                ],
            },
        },
        "report_generation",
    )
    rows = artifact["data"]["rows"]

    assert rows[0]["label"] == "BEV 动力销量"
    assert rows[0]["source"] == "JATO 图表数据"
    assert rows[0]["sourceEvidenceRef"] == "ev_bev_sales"
    assert rows[1]["label"] == "平均 MSRP"
    assert rows[1]["source"] == "JATO 市场快照"


def test_enrich_business_record_refreshes_segment_structure_chart_over_top_models() -> None:
    record = {
        "comparisonId": "cmp_market_segment_stale_chart",
        "validationType": "business",
        "questionId": "biz-market-004",
        "category": "market_overview",
        "country": "Sweden",
        "question": "SUV A0/A 级为什么是主销结构？",
        "expectedIntent": "market_overview",
        "expectedTools": ["query_country_snapshot", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "瑞典 SUV A0/A 主销结构应先用 segment cross-tab 验证销量和驱动形式。",
            "visualArtifacts": [
                {"id": "top_ranking", "type": "chart", "title": "Top Models"},
                {"id": "artifact_market_overview_table", "type": "table", "title": "Market decision table"},
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
            "evidencePackage": {
                "intent": "market_overview",
                "country": "Sweden",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "build_market_chart",
                        "success": True,
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "suv_a0_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 5416, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "suv_a_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct", "value": 60.1, "unit": "%", "source": "jato_country_chart_deck"},
                            {"refId": "suv_a0_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.4WD_pct", "value": 14.8, "unit": "%", "source": "jato_country_chart_deck"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    artifacts = enriched["astrbot"]["visualArtifacts"]
    artifact_ids = [item["id"] for item in artifacts]
    structure_chart = artifacts[0]

    assert artifact_ids[:2] == ["artifact_market_structure_chart", "artifact_market_overview_table"]
    assert "top_ranking" not in artifact_ids
    assert structure_chart["title"] == "市场结构图"
    assert [(row["label"], row["value"], row["series"]) for row in structure_chart["data"]] == [
        ("SUV A0", 5416.0, "级别销量"),
        ("SUV A", 7544.0, "级别销量"),
    ]
    assert structure_chart["sourceEvidenceRefs"] == ["suv_a_sales", "suv_a0_sales"]
    assert "SUV A 细分销量" in str(enriched["astrbot"]["evidenceDigest"])
    assert "contextSnapshot.crossTabs" not in str(enriched["astrbot"]["evidenceDigest"])


def test_enrich_business_record_refreshes_configuration_artifact_schema() -> None:
    record = {
        "comparisonId": "cmp_config_stale_artifacts",
        "validationType": "business",
        "questionId": "biz-config-001",
        "category": "configuration",
        "country": "Sweden",
        "question": "A0 SUV BEV 为什么需要 80kWh 电池？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["compare_vehicle_variants"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "电池判断：A0 SUV BEV 在北欧不是所有版本都必须 80kWh。",
            "direct": "电池判断：A0 SUV BEV 在北欧不是所有版本都必须 80kWh；80kWh 应定位为长续航/高配安全边界。",
            "visualArtifacts": [
                {
                    "id": "artifact_configuration_analysis_table",
                    "type": "table",
                    "title": "Configuration evidence table",
                    "spec": {"columns": ["feature", "targetModel", "competitor", "gap", "customerValue", "priority"]},
                    "data": {"rows": [{"feature": "80kWh long-range battery", "customerValue": "old"}]},
                }
            ],
            "evidencePackage": {
                "intent": "configuration_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "entities": {"models": ["A0 SUV BEV"]},
                "toolResults": [],
                "missingEvidence": [
                    {"name": "competitive_or_configuration_data_unavailable", "reason": "No trim/config matrix."}
                ],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    table = next(
        item
        for item in enriched["astrbot"]["visualArtifacts"]
        if item["id"] == "artifact_configuration_analysis_table"
    )
    columns = table["spec"]["columns"]
    first_row = table["data"]["rows"][0]

    assert columns == [
        "feature",
        "targetModel",
        "validationData",
        "sourceOrTool",
        "acceptanceCriteria",
        "currentStatus",
        "priority",
    ]
    assert first_row["feature"] == "80kWh long-range battery"
    assert "冬季真实续航" in first_row["validationData"]
    assert "compare_vehicle_variants" in first_row["sourceOrTool"]
    assert first_row["currentStatus"] == "待补竞品配置/价格证据"


def test_enrich_business_record_adds_tco_validation_artifact_for_leasing_gap() -> None:
    record = {
        "comparisonId": "cmp_tco_stale_artifacts",
        "validationType": "business",
        "questionId": "biz-policy-005",
        "category": "policy_news",
        "country": "Sweden",
        "question": "大客户 leasing 场景下，PHEV 还有没有理由？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "fleet leasing 判断：PHEV 必须用 TCO、月供、残值、公司车税和长途里程验证。",
            "businessImplications": [
                "大客户 leasing 下 PHEV 的理由要由 TCO、月供、残值、税费、长途里程和充电条件共同证明。",
                "Use this source to validate price corridor, monthly payment, or competitor positioning before making a firm pricing recommendation.",
            ],
            "visualArtifacts": [
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "entities": {"powertrains": ["PHEV"]},
                "toolResults": [
                    {
                        "toolName": "build_market_chart",
                        "success": True,
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                        ],
                    }
                ],
                "missingEvidence": [
                    {"name": "leasing_tco_or_company_car_evidence", "reason": "Missing monthly/RV/TCO evidence."}
                ],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    source_candidates = enriched["astrbot"]["sourceRepairCandidates"]
    external_table = next(
        item
        for item in enriched["businessValidation"]["astrbotVisualArtifacts"]
        if item["id"] == "artifact_external_source_repair_table"
    )
    tco_table = next(
        item
        for item in enriched["businessValidation"]["astrbotVisualArtifacts"]
        if item["id"] == "artifact_tco_validation_table"
    )
    external_rows = external_table["data"]["rows"]
    rows = tco_table["data"]["rows"]

    assert source_candidates["dataStatus"] == "leasing_tco_source_candidates"
    assert source_candidates["candidateCount"] >= 4
    assert external_table["title"] == "External source validation matrix"
    assert external_rows[0]["sourceNeed"] == "Leasing/TCO/company-car source"
    assert "monthly payment/RV/tax formula" in external_rows[0]["requiredFields"]
    assert "residual value" in external_rows[0]["evidenceUse"]
    assert tco_table["title"] == "TCO / company-car validation table"
    assert rows[0]["scenario"] == "Channel / fleet exposure"
    assert "PHEV 公司车注册占比" in rows[0]["currentStatus"]
    assert any(row["scenario"] == "Monthly payment / lease quote" for row in rows)
    assert any(row["currentStatus"] == "待补可引用证据" for row in rows)
    assert "Use this source" not in enriched["astrbotAnswer"]
    assert "先验证价格走廊、月供或竞品定位" not in enriched["astrbotAnswer"]
    assert "大客户 leasing 下 PHEV 的理由要由 TCO" in enriched["astrbotAnswer"]


def test_business_review_answer_text_rewrites_ex60_inference_limit() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "对标判断：O9 与 XC60 / EX60 需要补直接证据。",
        "evidenceDigest": ["XC60 销量 = 2,893 units（JATO 交叉引用）"],
        "limitations": [
            "EX60的具体销量数据未单独列出（可能被归入EX系列），但EX40（XC40纯电版）销量领先，可推断EX60需求旺盛。",
            "EX60尚未上市，无定价和销量数据，定位分析基于行业认知而非实际数据。",
            "定价数据中未直接返回XC60的MSRP，参考行业认知给出价格区间。",
        ],
    })

    key_section = text.split("## 关键证据", 1)[1].split("## 证据边界", 1)[0]
    limit_section = text.split("## 证据边界", 1)[1]

    assert "XC60 销量 = 2,893 units（JATO 交叉引用）" in key_section
    assert "不能用 EX40 替代或推断 EX60 需求" in limit_section
    assert "EX60 缺少可引用的瑞典上市状态、定价和销量证据" in limit_section
    assert "不能用行业认知给出确定价格区间" in limit_section
    assert "可推断EX60需求旺盛" not in limit_section
    assert "EX40（XC40纯电版）销量领先" not in limit_section
    assert "EX60尚未上市" not in limit_section
    assert "参考行业认知给出价格区间" not in limit_section


def test_business_review_answer_text_rewrites_j8_sorento_competitor_pool_limit() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "直接结论：J8 7 座四驱对 Sorento 的打法有市场场景支撑。",
        "evidenceDigest": [
            "SUV A 4WD 占比 = 60.1 %（jato_country_chart_deck）",
            "PHEV 公司车注册占比 = 64.8 %（jato_country_chart_deck）",
        ],
        "limitations": [
            "竞品池中未包含J8，无法确认J8是否被定义为Sorento的直接竞品。",
        ],
    })

    assert "竞品池中未包含J8" not in text
    assert "无法确认J8是否被定义为Sorento" not in text
    assert "当前缺少 J8/Sorento 直接对标关系" in text
    assert "车型级销量/MSRP、配置差异和 TCO 证据" in text


def test_business_review_answer_text_filters_cross_section_repetition() -> None:
    direct = (
        "电池判断：A0 SUV BEV 在北欧不是所有版本都必须 80kWh；"
        "80kWh 应定位为长续航/高配安全边界，低配仍要保留价格锚点。"
    )
    text = eval_service._business_review_answer_text({
        "direct": direct,
        "bullets": [
            direct,
            "配置证据：冬季包应覆盖热泵、座椅/方向盘加热和低温续航提示。",
        ],
        "businessImplications": [
            direct,
            "产品动作：80kWh 做高配，不把低配成本打穿。",
        ],
        "recommendedActions": [
            {
                "priority": "P0",
                "action": "生成 A0 SUV BEV 80kWh 续航-价格-重量验证表。",
            }
        ],
        "reportReadyBullets": [
            direct,
            "汇报口径：A0 SUV BEV 高配用 80kWh 建安全边界，低配保留价格锚点。",
        ],
    })

    assert text.count("A0 SUV BEV 在北欧不是所有版本都必须 80kWh") == 1
    assert text.count("80kWh 应定位为长续航/高配安全边界") == 1
    assert "## 关键证据" in text
    assert "冬季包应覆盖热泵" in text
    assert "## 产品经理判断" in text
    assert "80kWh 做高配" in text
    assert "## 下一步动作" in text


def test_business_review_answer_text_sanitizes_internal_research_and_ref_terms() -> None:
    text = eval_service._business_review_answer_text({
        "direct": (
            "直接结论：瑞典 政策/新闻分析必须确认来源；"
            "Use this source to decide what needs official-source confirmation before making policy claims。"
            "下一步执行 Next: confirm official source, publish date, and affected vehicle eligibility。"
            "证据对齐 partially_aligned，25 个 evidenceRef，置信度 high。"
        ),
        "bullets": ["Risk：coverage_diagnostic:no_current_prices_for_requested_models 会影响 own_model_price。"],
        "limitations": ["current_msrp: missing；mitigation: rerun."],
    })

    assert "Use this source" not in text
    assert "Next:" not in text
    assert "partially_aligned" not in text
    assert "evidenceRef" not in text
    assert "先确认官方来源" in text
    assert "25 条可引用证据" not in text
    assert "置信度高" not in text
    assert "价格覆盖诊断：缺少请求车型当前价格" in text


def test_business_review_answer_text_filters_runtime_lines_and_localizes_limits() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "直接结论：瑞典 VOC 结论要先区分真实用户痛点、媒体观点、论坛噪音和可转化卖点。",
        "bullets": [
            "Why：当前有 12 个 evidenceRef，置信度 medium。",
            "本轮工具链已经覆盖 external_research, query_country_snapshot，下一步应补齐缺失证据后再收敛结论。",
        ],
        "businessImplications": [
            "Sweden 汇报页应压成 Title / Key message / Evidence / Product implication / Next action 五块。",
        ],
        "limitations": [
            "External web sources are citation candidates and should be cross-checked against JATO structured data for numeric market claims.",
            "Tavily advanced research was unavailable; fallback search providers were used.",
            "No source URL was returned, so the answer must not claim current external facts.",
        ],
    })

    assert "本轮工具链" not in text
    assert "下一步应补齐缺失证据" not in text
    assert "External web sources" not in text
    assert "Tavily advanced research" not in text
    assert "No source URL" not in text
    assert "瑞典汇报页" in text
    assert "外部网页来源只是引用候选" in text
    assert "外部来源覆盖不足" in text
    assert "未返回来源 URL" in text


def test_business_review_answer_text_filters_generic_governance_boundaries() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "直接结论：O9 53k-55k 只能作为待验证价格假设。",
        "limitations": [
            "Business Composer: evidence alignment is partially_aligned.",
            "风险边界：决策边界：结论仍应随最新价格、政策和配置证据更新；建议：保留可引用证据和来源日期，进入人工业务验收。",
            "决策边界: 结论仍应随最新价格、政策和配置证据更新。；mitigation: 保留可引用证据和来源日期，进入人工业务验收。",
            "风险边界：mixed currency unit：会导致定价页把 EUR 价格误读成人民币或普通元。；mitigation: 人工确认价格单位。",
            "风险边界：multiple pva values：高配价差覆盖率会随 PVA 口径变化。；mitigation: 统一 PVA 定义。",
            "风险边界：non target market template residue：政策/税费分析可能被非目标市场模板污染。；mitigation: 替换为瑞典实际口径。",
            "风险边界：competitor price basis mismatch：MSRP、成交价和月供不能直接放在同一价格走廊里比较。；mitigation: 拆分价格列。",
            "query_msrp_pricing 未命中 O9、XC60、EX60 的当前 MSRP 记录，仅提供参考样本，不能作为官方价格。",
            "证据安全检查：支撑证据 missing。",
        ],
    })

    assert "证据对齐：部分对齐" not in text
    assert "风险边界：决策边界" not in text
    assert "决策边界:" not in text
    assert "缓解方式" not in text
    assert "query_msrp_pricing" not in text
    assert "支撑证据 missing" not in text
    assert "mixed currency unit" not in text
    assert "multiple pva values" not in text
    assert "non target market template residue" not in text
    assert "competitor price basis mismatch" not in text
    assert "价格单位风险" in text
    assert "PVA 口径风险" in text
    assert "非目标市场模板风险" in text
    assert "价格口径不一致风险" in text


def test_business_review_answer_text_localizes_source_and_field_codes() -> None:
    text = eval_service._business_review_answer_text({
        "direct": "直接结论：价格和 BOM 证据需要补齐后再给确定结论。",
        "evidenceDigest": [
            "本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证",
            "用户给定相对价差 = 3,000 EUR（user_question）",
            "背景价格样本最低值 = 39,121.7（jato_msrp_postgres）",
            "SUV A 细分销量 = 7,544 units（jato_country_chart_deck）",
            "瑞典累计销量 = 289,827（jato_cross_country）；竞品销量 = 2,893（jato_cross_reference）；BOM 字段 = material_number / available_units / lifecycle_status",
        ],
    })

    assert "current_price" not in text
    assert "user_question" not in text
    assert "jato_msrp_postgres" not in text
    assert "jato_country_chart_deck" not in text
    assert "jato_cross_country" not in text
    assert "jato_cross_reference" not in text
    assert "material_number" not in text
    assert "available_units" not in text
    assert "lifecycle_status" not in text
    assert "当前价格记录" in text
    assert "用户问题" in text
    assert "JATO MSRP 数据" in text
    assert "JATO 图表数据" in text
    assert "JATO 跨国对比" in text
    assert "JATO 交叉引用" in text
    assert "物料号" in text
    assert "可用数量" in text
    assert "生命周期状态" in text


def test_enrich_business_record_expands_legacy_short_astrbot_preview() -> None:
    record = {
        "comparisonId": "cmp_legacy_preview",
        "validationType": "business",
        "questionId": "biz-pricing-001",
        "category": "pricing",
        "country": "Sweden",
        "question": "瑞典 J7 HEV 应该怎么定价？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "直接结论：先按高配主推验证。",
            "bullets": ["Why：当前 MSRP 证据不足。"],
            "businessImplications": ["低配做价格锚点，高配做主推版本。"],
            "recommendedActions": [{"priority": "P0", "action": "补齐竞品 MSRP"}],
            "reportReadyBullets": ["J7 HEV 应围绕核心竞争带中段组织定价。"],
            "evidencePackage": {"toolResults": [], "missingEvidence": []},
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    assert enriched["astrbotAnswer"].startswith("直接结论：先按高配主推验证。")
    assert "市场层面先验证 HEV 需求" not in enriched["astrbotAnswer"]
    assert "竞品层面锁定 Corolla Cross" not in enriched["astrbotAnswer"]
    assert "## 关键证据" in enriched["astrbotAnswer"]
    assert "## 下一步动作" in enriched["astrbotAnswer"]
    assert enriched["astrbotAnswer"].count("## 下一步动作") == 1


def test_enrich_business_record_derives_evidence_digest_for_legacy_records() -> None:
    record = {
        "comparisonId": "cmp_legacy_digest",
        "validationType": "business",
        "questionId": "biz-pricing-004",
        "category": "pricing",
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "当前证据不足以确认瑞典 O9 官方 MSRP。",
            "evidencePackage": {
                "intent": "pricing_analysis",
                "toolResults": [
                    {
                        "toolName": "query_price_positioning",
                        "evidenceRefs": [
                            {"refId": "ev_row", "label": "row_count", "value": 1},
                            {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_price_positioning"},
                            {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_price_positioning"},
                            {"refId": "ev_avg", "label": "priceStats.avg", "value": 48467.39, "unit": "currency", "source": "jato_price_positioning"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
            "visualArtifacts": [{"type": "table", "title": "Pricing evidence table"}],
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    assert enriched["astrbot"]["evidenceDigest"][0].startswith("价格样本最低值 = 39,121.7")
    assert "row_count" not in " ".join(enriched["astrbot"]["evidenceDigest"])
    assert "Pricing evidence table" in enriched["astrbot"]["displayPlan"]
    assert "价格样本最低值" in enriched["astrbotAnswer"]
    assert enriched["astrbotEvidenceDigest"] == enriched["astrbot"]["evidenceDigest"]
    assert enriched["astrbotDisplayPlan"] == enriched["astrbot"]["displayPlan"]


def test_enrich_business_record_refreshes_pricing_digest_when_requested_msrp_is_missing() -> None:
    record = {
        "comparisonId": "cmp_stale_pricing_digest",
        "validationType": "business",
        "questionId": "biz-pricing-002",
        "category": "pricing",
        "country": "Sweden",
        "question": "J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "旧记录里把价格样本当成关键证据展示。",
            "evidenceDigest": ["价格样本最低值 = 39,121.7（jato_msrp_postgres）"],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "toolResults": [
                    {
                        "toolName": "query_price_positioning",
                        "evidenceRefs": [
                            {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_msrp_postgres"},
                            {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_msrp_postgres"},
                            {"refId": "ev_avg", "label": "priceStats.avg", "value": 48467.39, "unit": "currency", "source": "jato_msrp_postgres"},
                            {"refId": "ev_model_y_sales", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato_cross_reference"},
                        ],
                    }
                ],
                "missingEvidence": [
                    {
                        "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                        "reason": "Requested J7/Sportage current MSRP rows are missing.",
                        "impact": "weakens_answer",
                    }
                ],
            },
            "visualArtifacts": [{"type": "table", "title": "Pricing evidence table"}],
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    digest = enriched["astrbot"]["evidenceDigest"]

    assert digest[0] == "本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证"
    assert any(item.startswith("背景价格样本最低值 = 39,121.7") for item in digest)
    assert "MODEL Y.sales" not in " ".join(digest)
    assert not any(item.startswith("价格样本最低值") for item in digest)
    assert enriched["astrbotEvidenceDigest"] == digest
    assert "本题车型官方 MSRP = 待补" in enriched["astrbotAnswer"]


def test_pricing_digest_prefers_j7_material_value_refs_over_generic_market_avg_msrp() -> None:
    record = {
        "comparisonId": "cmp_j7_material_digest",
        "validationType": "business",
        "questionId": "biz-pricing-001",
        "category": "pricing",
        "country": "Sweden",
        "question": "瑞典 J7 HEV 应该怎么定价？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "J7 HEV 定价应围绕核心竞争带中段 + 高配主推。",
            "evidenceDigest": ["旧摘要"],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "toolResults": [
                    {
                        "toolName": "query_country_snapshot",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_avg_msrp", "label": "avgMsrp", "value": 57954.1, "unit": "currency", "source": "jato_country_snapshot"},
                        ],
                    },
                    {
                        "toolName": "business_method_material",
                        "sourceType": "generated",
                        "evidenceRefs": [
                            {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "source": "J7_HEV_V4.pptx"},
                            {"refId": "ev_j7_msrp", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                            {"refId": "ev_j7_gap", "label": "J7 HEV user material price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                            {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                        ],
                    },
                ],
                "missingEvidence": [],
            },
            "visualArtifacts": [{"type": "table", "title": "Pricing evidence table"}],
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    digest = enriched["astrbot"]["evidenceDigest"]

    assert digest == [
        "J7 HEV 主销高配价格 = 34,720 EUR（J7_HEV_V4.pptx）",
        "J7 HEV 竞品价格带 = 30,000-40,000 EUR（J7_HEV_V4.pptx）",
        "J7 HEV 高低配价差 = 3,230 EUR（J7_HEV_V4.pptx）",
        "J7 HEV 高配 PVA 覆盖率 = 118 %（J7_HEV_V4.pptx）",
    ]
    assert "avgMsrp" not in enriched["astrbotAnswer"]


def test_pricing_digest_puts_j7_material_before_missing_live_msrp_gap() -> None:
    record = {
        "comparisonId": "cmp_j7_material_with_live_price_gap",
        "validationType": "business",
        "questionId": "biz-pricing-002",
        "category": "pricing",
        "country": "Sweden",
        "question": "J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "business_method_material"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "J7 HEV 应比 Sportage HEV 保持价格吸引力。",
            "evidenceDigest": ["旧摘要"],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "toolResults": [
                    {
                        "toolName": "business_method_material",
                        "sourceType": "generated",
                        "evidenceRefs": [
                            {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "source": "J7_HEV_V4.pptx"},
                            {"refId": "ev_j7_msrp", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                            {"refId": "ev_j7_gap", "label": "J7 HEV user material price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                            {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                        ],
                    },
                ],
                "missingEvidence": [
                    {
                        "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                        "reason": "Need live J7/Sportage MSRP cross-check.",
                        "impact": "weakens_answer",
                    }
                ],
            },
            "visualArtifacts": [{"type": "table", "title": "Pricing evidence table"}],
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    digest = enriched["astrbot"]["evidenceDigest"]

    assert digest == [
        "J7 HEV 主销高配价格 = 34,720 EUR（J7_HEV_V4.pptx）",
        "J7 HEV 竞品价格带 = 30,000-40,000 EUR（J7_HEV_V4.pptx）",
        "J7 HEV 高低配价差 = 3,230 EUR（J7_HEV_V4.pptx）",
        "当前官方 MSRP 交叉验证 = 待补本车型/竞品当前价格记录",
    ]
    key_section = enriched["astrbotAnswer"].split("## 关键证据", 1)[1].split("## 产品经理判断", 1)[0]
    assert key_section.index("J7 HEV 主销高配价格") < key_section.index("当前官方 MSRP 交叉验证")
    assert "本题车型官方 MSRP = 待补" not in key_section


def test_enrich_business_record_does_not_use_market_kpis_as_voc_digest() -> None:
    record = {
        "comparisonId": "cmp_voc_digest",
        "validationType": "business",
        "questionId": "biz-voc-003",
        "category": "voc",
        "country": "Sweden",
        "question": "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research", "search_market_news"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "VOC 判断：当前不能把候选主题写成已验证高频吐槽。",
            "bullets": [
                "证据：当前缺少可追溯 VOC 来源；JATO 市场数据只能作为背景，不能证明用户吐槽频次或高频主题。"
            ],
            "evidencePackage": {
                "intent": "voc_analysis",
                "toolResults": [
                    {
                        "toolName": "query_country_snapshot",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_model", "label": "modelCount", "value": 539, "unit": "units", "source": "jato_country_snapshot"},
                            {"refId": "ev_sales", "label": "cumulativeSales", "value": 1182452, "unit": "units", "source": "jato_country_snapshot"},
                            {"refId": "ev_msrp", "label": "avgMsrp", "value": 57954.1, "source": "jato_country_snapshot"},
                        ],
                    },
                    {
                        "toolName": "external_research",
                        "sourceType": "web",
                        "evidenceRefs": [],
                    },
                    {
                        "toolName": "minirag_query_graph",
                        "sourceType": "voc",
                        "evidenceRefs": [
                            {"refId": "ev_meta_count", "label": "metadata.result_count", "value": 1, "unit": "units", "source": "minirag_fallback_multi_tool"},
                        ],
                    },
                ],
                "missingEvidence": [
                    {
                        "name": "external_research_claims_unavailable",
                        "reason": "External research returned no citation-ready VOC evidence.",
                        "impact": "weakens_answer",
                    }
                ],
                "confidence": "medium",
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    digest = enriched["astrbot"]["evidenceDigest"]

    assert "VOC 来源状态 = 待补可追溯媒体/论坛/用户原声" in digest
    assert "缺口 = 当前市场数据不能证明用户吐槽频次或高频主题" in digest
    assert any(item.startswith("本轮已查数据源 = ") for item in digest)
    assert enriched["astrbotEvidenceDigest"] == digest
    assert "cumulativeSales" not in enriched["astrbotAnswer"]
    assert "avgMsrp" not in enriched["astrbotAnswer"]
    assert "metadata.result_count" not in enriched["astrbotAnswer"]
    assert "本轮已查数据源" not in enriched["astrbotAnswer"]
    assert "VOC 来源状态 = 待补可追溯媒体/论坛/用户原声" in enriched["astrbotAnswer"]


def test_business_review_answer_text_filters_voc_direct_repetition() -> None:
    text = eval_service._business_review_answer_text({
        "question": "瑞典用户会不会把 V2H 当成真实购买卖点？",
        "direct": (
            "VOC 判断：瑞典 V2H 暂时不能定位为真实高频购买卖点，"
            "应定位为“高感知但待验证”的技术型加分项。"
            "用户价值：它更可能服务家庭能源、安全备份、冬季用车和科技形象叙事；"
            "如果没有用户原声，只能作为代理判断，不能当作消费者调研结论。"
            "产品动作：先把 V2H 测成家庭能源、冬季备份和科技形象三套话术，"
            "再用媒体测评、论坛评论和经销端反馈验证是否能转化购买。"
        ),
        "reportReadyBullets": [
            "瑞典 V2H 暂时应定位为高感知但待验证的技术加分项，不能直接写成高频购买卖点。",
            "验证重点是家庭能源、安全备份、冬季用车、科技形象和经销端话术是否能转化购买；当前证据状态为部分对齐。",
            "建议动作：抓取瑞典/北欧 V2H 用户原声和媒体测评证据。",
        ],
        "businessImplications": [
            "验证重点应放在家庭能源、安全备份、冬季用车、科技形象和经销端话术是否能转化购买。"
        ],
        "recommendedActions": [
            {
                "priority": "P0",
                "action": "抓取瑞典/北欧 V2H 用户原声和媒体测评证据",
                "rationale": "V2H 是否是真实购买卖点必须靠可追溯 VOC 来源验证。",
            }
        ],
        "evidenceDigest": ["VOC 来源状态 = 待补可追溯媒体/论坛/用户原声"],
        "businessSynthesisPlan": {"businessImplications": [], "reportReadyBullets": []},
    })

    assert text.count("高感知但待验证") == 1
    assert "## 关键证据" in text
    assert "## 下一步动作" in text
    assert "抓取瑞典/北欧 V2H 用户原声和媒体测评证据" in text


def test_business_review_answer_text_compacts_legacy_voc_preview_direct() -> None:
    text = eval_service._business_review_answer_text({
        "question": "瑞典用户会不会把 V2H 当成真实购买卖点？",
        "answerPreview": (
            "直接结论：瑞典 V2H 暂时不应被写成真实高频购买卖点，而应定位为“高感知但待验证”的技术型加分项。"
            "它更可能服务家庭能源、安全备份、冬季用车和科技形象叙事；若当前只有市场结构证据，这只能作为代理判断，不是消费者调研结论。"
            "是否能转化购买，仍需要用户原声、媒体测评和经销端话术验证。 "
            "瑞典 V2H 暂时应定位为高感知但待验证的技术加分项，不能直接写成高频购买卖点。 "
            "验证重点是家庭能源、安全备份、冬季用车、科技形象和经销端话术是否能转化购买；当前证据状态为部分对齐。 "
            "建议动作：抓取瑞典/北欧 V2H 用户原声和媒体测评证据。"
            "\n\n## 关键证据\n- VOC 来源状态 = 待补可追溯媒体/论坛/用户原声"
        ),
        "evidenceDigest": ["VOC 来源状态 = 待补可追溯媒体/论坛/用户原声"],
        "businessSynthesisPlan": {"businessImplications": [], "reportReadyBullets": []},
    })

    direct = text.split("\n\n## ", 1)[0]
    assert direct.count("高感知但待验证") == 1
    assert "验证重点是家庭能源" not in direct
    assert "建议动作：" not in direct
    assert "VOC 来源状态 = 待补可追溯媒体/论坛/用户原声" in text


def test_enrich_business_record_downgrades_voc_without_citation_ready_sources() -> None:
    record = {
        "comparisonId": "cmp_voc_no_sources",
        "validationType": "business",
        "questionId": "biz-voc-002",
        "category": "voc",
        "country": "Sweden",
        "question": "拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": (
                "直接结论：瑞典的用户声音判断应先验证来源可信度和主题聚类。"
                "瑞典 VOC 暂时有高感知但待验证的技术加分项（13 条可引用证据）。"
                "没有可追溯来源时不能声称高频，只能给验证假设。"
                "\n\n## 关键证据\n"
                "- 市场基础存在但未成熟：这是旧记录里的无来源推断。\n"
                "- 消费者决策因素排序：这是旧记录里的无来源推断。"
            ),
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "voc_analysis",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "sourceType": "web",
                        "rowCount": 2,
                        "summary": "Roof box crash-test source and brand marketing page, not user voice.",
                        "evidenceRefs": [
                            {
                                "refId": "ev_roof_box",
                                "label": "Several popular roof boxes fail Testfakta crash test",
                                "value": "Crash test article",
                                "source": "mynewsdesk.com",
                            },
                            {
                                "refId": "ev_marketing",
                                "label": "MHERO premium SUV Sweden.source",
                                "value": "https://example.test/mhero",
                                "source": "marketing_page",
                            },
                        ],
                    },
                    {
                        "toolName": "query_country_snapshot",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_sales", "label": "cumulativeSales", "value": 1182452, "source": "jato_country_snapshot"},
                        ],
                    },
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]

    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert enriched["astrbot"]["confidence"] == "medium"
    assert enriched["astrbot"]["evidencePackage"]["confidence"] == "medium"
    assert "VOC 来源状态 = 待补可追溯媒体/论坛/用户原声" in enriched["astrbot"]["evidenceDigest"]
    assert "缺口 = 当前市场数据不能证明用户吐槽频次或高频主题" in enriched["astrbot"]["evidenceDigest"]
    assert "external_research_claims_unavailable" in missing_names
    assert "minimum_external_sources" in missing_names
    assert enriched["astrbot"]["qualityScore"]["totalScore"] <= 0.88
    assert "13 条可引用证据" not in enriched["astrbotAnswer"]
    assert "市场基础存在但未成熟" not in enriched["astrbotAnswer"]
    assert "消费者决策因素排序" not in enriched["astrbotAnswer"]
    assert "VOC 来源状态 = 待补可追溯媒体/论坛/用户原声" in enriched["astrbotAnswer"]

    queue = eval_service._build_evidence_repair_queue([enriched])

    assert queue[0]["primaryGap"] == "external_research_claims_unavailable"
    assert queue[0]["repairTasks"][0]["title"] == "补齐 VOC/媒体/论坛来源"


def test_enrich_business_record_keeps_citation_ready_voc_source_and_marks_frequency_gap() -> None:
    record = {
        "comparisonId": "cmp_voc_with_source",
        "validationType": "business",
        "questionId": "biz-voc-002",
        "category": "voc",
        "country": "Sweden",
        "question": "拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "直接结论：已有来源支持 roof load / winter utility 是候选主题，但还不能说高频。",
            "evidencePackage": {
                "intent": "voc_analysis",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "sourceType": "web",
                        "rowCount": 1,
                        "summary": "Roof-box crash-test source with Nordic winter utility relevance.",
                        "evidenceRefs": [
                            {
                                "refId": "ev_roof_url",
                                "label": "Several popular roof boxes fail Testfakta crash test.source",
                                "value": "https://www.mynewsdesk.com/se/testfakta-ab/news/roof-box-crash-test",
                                "source": "jato_external_research_web",
                            },
                            {
                                "refId": "ev_roof_claim",
                                "label": "Several popular roof boxes fail Testfakta crash test.claim",
                                "value": "Roof boxes carrying skis and boots can fail in winter crash-test conditions.",
                                "source": "https://www.mynewsdesk.com/se/testfakta-ab/news/roof-box-crash-test",
                            },
                        ],
                    },
                ],
                "missingEvidence": [
                    {
                        "name": "external_research_claims_unavailable",
                        "reason": "Legacy stale gap.",
                        "impact": "weakens_answer",
                    },
                    {
                        "name": "minimum_external_sources",
                        "reason": "Legacy stale gap.",
                        "impact": "weakens_answer",
                    },
                ],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]

    assert "external_research_claims_unavailable" not in missing_names
    assert "minimum_external_sources" not in missing_names
    assert "voc_frequency_or_representativeness" in missing_names
    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert any("Roof boxes carrying skis and boots" in item for item in enriched["astrbot"]["evidenceDigest"])
    assert "Roof boxes carrying skis and boots" in enriched["astrbotAnswer"]
    direct = enriched["astrbotAnswer"].split("## 关键证据", 1)[0]
    assert "已有可追溯外部来源" in direct
    assert "不能证明北欧用户已经形成高频需求" in direct
    assert "没有可追溯来源" not in direct
    assert "当前缺少可追溯 VOC 来源" not in enriched["astrbotAnswer"]


def test_enrich_business_record_normalizes_stale_report_quality_failures() -> None:
    record = {
        "comparisonId": "cmp_report_quality",
        "validationType": "business",
        "questionId": "biz-report-001",
        "category": "report_generation",
        "country": "Sweden",
        "question": "把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        "expectedIntent": "report_generation",
        "expectedTools": ["query_msrp_pricing"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "直接结论：瑞典 J7 HEV 定价应围绕核心竞争带中段 + 高配主推。",
            "evidenceDigest": [
                "J7 HEV user material main trim MSRP = 34,720 EUR（J7_HEV_V4.pptx）",
            ],
            "limitations": ["证据缺口：report outline（会削弱结论）。"],
            "recommendedActions": [
                {
                    "priority": "P0",
                    "action": "补齐官网 MSRP 和竞品价格走廊",
                }
            ],
            "reportReadyBullets": [
                "Title：瑞典 J7 HEV 定价逻辑",
                "Key message：核心竞争带中段 + 高配主推",
                "Evidence：主销高配 34,720 EUR 来自用户材料",
                "Product implication：低配做价格锚点，高配做主推版本",
                "Next action：补齐官网 MSRP 和竞品价格走廊",
            ],
            "visualArtifacts": [{"type": "report_block", "title": "PPT-ready block"}],
            "evidencePackage": {
                "intent": "report_generation",
                "toolResults": [
                    {
                        "toolName": "user_material",
                        "evidenceRefs": [
                            {"refId": "ev_j7_msrp", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
            "qualityScore": {
                "intentScore": 1.0,
                "toolScore": 1.0,
                "groundingScore": 0.85,
                "followUpScore": 0.75,
                "safetyScore": 1.0,
                "businessCompletenessScore": 0.85,
                "actionabilityScore": 0.7,
                "totalScore": 0.88,
                "failures": [
                    "missing_supporting_evidence",
                    "followup_types_or_count_incomplete",
                    "business_missing_recommended_actions",
                ],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    quality = enriched["astrbot"]["qualityScore"]

    assert "report outline" not in enriched["astrbotAnswer"].lower()
    assert "missing_supporting_evidence" not in quality["failures"]
    assert "business_missing_recommended_actions" not in quality["failures"]
    assert quality["actionabilityScore"] == 1.0
    assert quality["totalScore"] > 0.88
    assert enriched["astrbotQualityScore"] == quality


def test_enrich_business_record_downgrades_answer_status_for_real_supporting_gap() -> None:
    record = {
        "comparisonId": "cmp_real_gap",
        "validationType": "business",
        "questionId": "biz-pricing-004",
        "category": "pricing",
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "直接结论：O9 53k-55k 需要配置和竞品价格支撑。",
            "answerStatus": "answered",
            "confidence": "high",
            "evidenceDigest": [
                "用户目标价下沿 = 53,000 EUR（user_question）",
                "价格样本最高值 = 53,165.2（jato_msrp_postgres）",
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Add or map current MSRP rows for O9 in Sweden.",
                    "impact": "weakens_answer",
                }
            ],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "query_msrp_pricing",
                        "evidenceRefs": [
                            {"refId": "ev_target", "label": "User supplied own-model target price min", "value": 53000, "unit": "EUR", "source": "user_question"},
                        ],
                    }
                ],
                "missingEvidence": [
                    {
                        "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                        "reason": "Add or map current MSRP rows for O9 in Sweden.",
                        "impact": "weakens_answer",
                    }
                ],
            },
            "qualityScore": {
                "intentScore": 1.0,
                "toolScore": 1.0,
                "groundingScore": 0.85,
                "followUpScore": 1.0,
                "safetyScore": 1.0,
                "businessCompletenessScore": 0.95,
                "totalScore": 0.963,
                "failures": ["missing_supporting_evidence"],
            },
            "followUps": [
                {"intent": "compare", "question": "对比竞品 MSRP 区间。"},
                {"intent": "data_check", "question": "查询 O9 官方 MSRP。"},
                {"intent": "action", "question": "分析配置价值。"},
                {"intent": "report", "question": "生成汇报页。"},
            ],
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    astrbot = enriched["astrbot"]
    quality = astrbot["qualityScore"]

    assert astrbot["answerStatus"] == "partially_answered"
    assert astrbot["confidence"] == "medium"
    assert astrbot["evidencePackage"]["confidence"] == "medium"
    assert quality["totalScore"] == 0.88
    assert quality["businessCompletenessScore"] <= 0.85
    assert "missing_supporting_evidence" in quality["failures"]


def test_business_review_answer_text_uses_question_context_for_clean_j7_lead() -> None:
    text = eval_service._business_review_answer_text(
        {
            "direct": (
                "直接结论：瑞典 J7 HEV 定价应围绕“核心竞争带中段 + 高配主推”：低配做价格锚点，"
                "高配做主推版本，用竞品价格走廊和可见配置价值支撑，而不是只报一个建议价。"
                "下一步执行 先补齐本车型官方 MSRP 来源，再审核竞品价格走廊候选；这些候选只是补数清单，不能直接当作官方价格证据。"
            ),
            "bullets": ["Why：已有部分证据可用，但内部/外部交叉验证还不完整。"],
            "businessImplications": ["低配做价格锚点，高配做主推版本。"],
            "recommendedActions": [{"priority": "P0", "action": "补齐竞品 MSRP"}],
            "reportReadyBullets": ["J7 HEV 应围绕核心竞争带中段组织定价。"],
            "limitations": ["Missing evidence: current_msrp."],
        },
        question="瑞典 J7 HEV 应该怎么定价？",
    )

    lead = text.split("\n\n", 1)[0]
    assert "瑞典 J7 HEV 定价应围绕“核心竞争带中段 + 高配主推”" in lead
    assert "市场层面先验证 HEV 需求" not in lead
    assert "补数清单" not in lead
    assert "## 下一步动作" in text
    assert "P0 · 补齐竞品 MSRP" in text


def test_enrich_business_record_downgrades_generic_market_review_without_evidence() -> None:
    record = {
        "comparisonId": "cmp_market_generic",
        "validationType": "business",
        "questionId": "biz-market-003",
        "category": "market_overview",
        "country": "Sweden",
        "question": "北欧 BEV 增长是否会压缩 HEV 空间？",
        "expectedIntent": "market_overview",
        "expectedTools": ["query_cross_country"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": (
                "直接结论：瑞典的 业务分析 已有可追溯证据支撑，当前最重要的业务含义是 "
                "把证据转成业务动作、风险边界和可复用汇报结构。\n\n"
                "## Key Takeaways\n- 当前有 12 个 evidenceRef，置信度 high。"
            ),
            "evidencePackage": {"toolResults": [], "missingEvidence": []},
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    text = enriched["astrbotAnswer"]

    assert "当前记录没有可引用证据支撑" in text
    assert "北欧 BEV 增长是否会压缩 HEV 空间" in text
    assert "BEV 增长会压缩 HEV 空间，但不是一次性替代" not in text
    assert "把证据转成业务动作" not in text.split("\n\n", 1)[0]
    assert "evidenceRef" not in text
    assert "置信度 high" not in text
    assert "12 条可引用证据" not in text


def test_enrich_business_record_downgrades_specific_review_directs_without_evidence() -> None:
    o5_record = {
        "comparisonId": "cmp_o5_generic",
        "validationType": "business",
        "questionId": "biz-pricing-003",
        "category": "pricing",
        "country": "Sweden",
        "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": (
                "直接结论：Sweden 的 定价分析 已有可追溯证据支撑，当前最重要的业务含义是 "
                "把证据转成业务动作、风险边界和可复用汇报结构。\n\n"
                "## Key Takeaways\n- 证据对齐 部分对齐，25 个 evidenceRef，置信度 high。"
            ),
            "evidencePackage": {"toolResults": [], "missingEvidence": []},
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }
    v2h_record = {
        **o5_record,
        "comparisonId": "cmp_v2h_generic",
        "questionId": "biz-voc-001",
        "category": "voc",
        "question": "瑞典用户会不会把 V2H 当成真实购买卖点？",
        "expectedIntent": "voc_analysis",
    }

    o5 = eval_service._enrich_business_record_for_read(o5_record)["astrbotAnswer"]
    v2h = eval_service._enrich_business_record_for_read(v2h_record)["astrbotAnswer"]

    assert "当前记录没有可引用证据支撑" in o5
    assert "O5 BEV 如果比 EV3 小电池便宜 3k" in o5
    assert "瑞典 O5 BEV 比 EV3 小电池便宜 3k" not in o5
    assert "Sweden" not in o5.split("\n\n", 1)[0]
    assert "25 条可引用证据" not in o5
    assert "证据状态：部分对齐" not in o5
    assert "当前记录没有可引用证据支撑" in v2h
    assert "瑞典 V2H 暂时应定位为高感知但待验证" not in v2h
    assert "Sweden 的 业务分析" not in v2h


def test_enrich_business_record_downgrades_company_car_and_phev_leasing_without_tco_evidence() -> None:
    generic = (
        "直接结论：Sweden 的 定价分析 已有可追溯证据支撑，当前最重要的业务含义是 "
        "建议围绕“核心竞争带中段 + 高配主推”组织定价逻辑：低配做价格锚点，高配承担销量和价值感。\n\n"
        "## Key Takeaways\n- 证据对齐 部分对齐，置信度 high。"
    )
    base_record = {
        "comparisonId": "cmp_company_car_generic",
        "validationType": "business",
        "questionId": "biz-policy-002",
        "category": "policy_news",
        "country": "Sweden",
        "question": "瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["external_research"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": generic,
            "evidencePackage": {"toolResults": [], "missingEvidence": []},
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }
    leasing_record = {
        **base_record,
        "comparisonId": "cmp_phev_leasing_generic",
        "questionId": "biz-policy-005",
        "question": "大客户 leasing 场景下，PHEV 还有没有理由？",
    }

    company_car = eval_service._enrich_business_record_for_read(base_record)["astrbotAnswer"]
    leasing = eval_service._enrich_business_record_for_read(leasing_record)["astrbotAnswer"]

    assert "当前记录没有可引用证据支撑" in company_car
    assert "company car benefit 对 BEV 和 PHEV 的影响" in company_car
    assert "PHEV 只在长途或无稳定充电场景保留理由" not in company_car
    assert "核心竞争带中段 + 高配主推" not in company_car.split("\n\n", 1)[0]
    assert "当前缺少 leasing/TCO/月供、残值或 company-car benefit 证据" in leasing
    assert "不能证明 PHEV 在大客户 leasing 场景下已经成立" in leasing
    assert "可油可电" not in leasing
    assert "核心竞争带中段 + 高配主推" not in leasing.split("\n\n", 1)[0]


def test_business_failure_tags_do_not_misclassify_no_evidence_as_tool_or_table_failure() -> None:
    record = {
        "questionId": "biz-pricing-001",
        "category": "pricing",
        "country": "Sweden",
        "question": "瑞典 J7 HEV 应该怎么定价？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "astrbot": {
            "answerPreview": "瑞典 J7 HEV 暂时不能给确定数字，但可以先推进定价框架。",
            "answerStatus": "insufficient_evidence",
            "selectedTool": "query_msrp_pricing",
            "evidenceRefCount": 0,
            "chartCount": 0,
            "visualArtifacts": [],
            "evidencePackage": {
                "toolResults": [
                    {"toolName": "query_msrp_pricing", "evidenceRefs": []},
                ],
            },
            "qualityScore": {
                "businessSynthesisScore": 1.0,
                "businessCompletenessScore": 0.825,
                "failures": ["grounding_incomplete"],
            },
            "recommendedActions": [{"action": "补齐竞品 MSRP / TP / 月供价格矩阵"}],
            "businessImplications": ["先按核心竞争带中段 + 高配主推组织定价逻辑。"],
            "reportReadyBullets": ["低配做价格锚点，高配做主推版本。"],
        },
        "comparison": {
            "astrbotAnswerChars": 180,
            "answerLengthDelta": -2300,
        },
        "humanScoring": {"status": "pending"},
    }

    tags = eval_service.infer_business_failure_tags(record)

    assert "answer_too_conservative" in tags
    assert "evidence_missing" in tags
    assert "tool_missing" not in tags
    assert "table_not_readable" not in tags
    assert "answer_too_generic" not in tags


def test_business_failure_tags_accept_se_fi_as_sweden_context() -> None:
    record = {
        "questionId": "biz-bom-004",
        "category": "inventory_bom",
        "country": "Sweden",
        "question": "SE/FI 合并 PI 但车辆分市场生成，逻辑是否正确？",
        "expectedIntent": "inventory_analysis",
        "expectedTools": ["query_cross_country", "query_with_filters"],
        "astrbot": {
            "answerPreview": (
                "SE/FI 合并 PI 可以做共用计划/产品信息层，但车辆生成、物料号、"
                "市场合规、价格、订单和库存生命周期必须按市场拆分。正确结构应是 "
                "PI header + market overlay + materialCode / vehicle generation mapping。"
            ),
            "answerStatus": "partially_answered",
            "selectedTool": "query_cross_country",
            "evidenceRefCount": 12,
            "evidencePackage": {
                "toolResults": [
                    {
                        "toolName": "query_cross_country",
                        "evidenceRefs": [
                            {"refId": "ev_se", "label": "crossCountry.Sweden.kpis.cumulativeSales", "value": 1182452},
                            {"refId": "ev_fi", "label": "crossCountry.Finland.kpis.cumulativeSales", "value": 332237},
                        ],
                    },
                    {
                        "toolName": "query_with_filters",
                        "evidenceRefs": [
                            {"refId": "ev_versions", "label": "results.kpis.versionCount", "value": 9204},
                        ],
                    },
                ],
            },
            "visualArtifacts": [{"type": "table", "title": "BOM / entity mapping validation table"}],
            "recommendedActions": [
                {"action": "定义 PI header + market overlay + vehicle/material generation mapping"},
            ],
            "businessImplications": [
                "合并 PI 只能解决共用计划/产品信息，车辆生成和物料/库存必须按市场保留可追溯映射。",
            ],
            "reportReadyBullets": [
                "SE/FI 共用计划层，执行层按 market overlay 生成车辆和物料。",
            ],
            "qualityScore": {"businessSynthesisScore": 1.0, "businessCompletenessScore": 0.85, "failures": []},
        },
        "comparison": {
            "astrbotAnswerChars": 520,
            "answerLengthDelta": 50,
        },
        "humanScoring": {"status": "pending"},
    }

    tags = eval_service.infer_business_failure_tags(record)

    assert "answer_too_generic" not in tags
    assert "tool_missing" not in tags


def test_business_review_pi_market_split_direct_is_not_repeated() -> None:
    question = "SE/FI 合并 PI 但车辆分市场生成，逻辑是否正确？"
    text = eval_service._business_review_answer_text(
        {
            "question": question,
            "intent": "inventory_analysis",
            "answerPreview": (
                "直接结论：SE/FI 合并 PI、车辆分市场生成的逻辑原则上可以成立，但前提是 PI 只承载共用计划/产品信息层，"
                "车辆生成、物料号、市场合规、价格、订单和库存生命周期必须保留 market-level overlay。"
                "SE/FI 合并 PI 可以做共用计划/产品信息层，但车辆生成、物料号、市场合规、价格、订单和库存生命周期必须按市场拆分。"
            ),
            "evidenceDigest": [
                "BOM 实体证据 = 待补车型版本、物料号、颜色、订单生命周期映射",
            ],
            "businessImplications": [
                "SE/FI 合并 PI 可以作为共用计划层，但车辆生成和物料/库存必须按市场保留可追溯映射。",
            ],
            "recommendedActions": [
                {"action": "定义 PI header + market overlay + vehicle/material generation mapping"},
            ],
            "reportReadyBullets": [
                "SE/FI 合并 PI 可以做共用计划/产品信息层，但车辆生成、物料号、市场合规、价格、订单和库存生命周期必须按市场拆分。",
                "正确结构应是 PI header + market overlay + materialCode / vehicle generation mapping；不能用合并 PI 覆盖 SE/FI 的市场差异，当前证据状态为部分对齐。",
            ],
        },
        question=question,
    )

    direct = text.split("## 关键证据", 1)[0]

    assert "PI header + market overlay + materialCode / vehicle generation mapping" in text
    assert direct.count("车辆生成、物料号") == 1
    assert direct.count("合并 PI") == 1
    assert "当前证据状态为" not in direct
    assert not direct.rstrip().endswith("，")


def test_business_review_rewrites_stale_competitor_chart_guidance_when_no_chart() -> None:
    answer = {
        "answerPreview": (
            "对标判断：J8 7座四驱打 Sorento 只能先作为场景型假设。 "
            "展示骨架：先看 Competitor sales chart 判断竞品量级，"
            "再用 Competitor comparison table 拆级别、动力类型、价格/配置差异和产品动作。\n\n"
            "## 汇报口径\n"
            "- 用竞品矩阵展示销量/份额、级别、动力、价格和配置差异；可用柱状图比较核心竞品销量或份额。"
        ),
        "visualArtifacts": [
            {"id": "artifact_msrp_source_repair_table", "type": "table", "title": "MSRP source validation table"},
            {
                "id": "artifact_competitor_compare_framework_table",
                "type": "table",
                "title": "Competitor comparison table",
            },
        ],
        "evidencePackage": {
            "intent": "competitor_compare",
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Add or map current MSRP rows for J8 in Sweden.",
                    "impact": "weakens_answer",
                }
            ],
        },
        "recommendedActions": [
            {"action": "先按官方价格搜索候选补齐本车型 MSRP 来源（J8）"},
        ],
        "reportReadyBullets": [
            "用竞品矩阵展示销量/份额、级别、动力、价格和配置差异；可用柱状图比较核心竞品销量或份额。",
        ],
    }

    text = eval_service._business_review_answer_text(answer)

    assert "Competitor sales chart" not in text
    assert "竞品销量图" not in text
    assert "可用柱状图比较" not in text
    assert "MSRP source repair table" not in text
    assert "MSRP 来源验证表" in text
    assert "竞品对比矩阵" in text
    assert "图表等 requested-model 销量或价格证据补齐后再生成" in text


def test_business_report_refreshes_competitor_display_plan_when_only_market_context_chart_exists() -> None:
    evidence_package = {
        "intent": "competitor_compare",
        "country": "Sweden",
        "missingEvidence": [
            {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
            {"name": "coverage_diagnostic:no_config_projects_for_country", "impact": "weakens_answer"},
        ],
    }
    astrbot_side = {
        "displayPlan": "用竞品矩阵展示销量/份额、级别、动力、价格和配置差异；可用柱状图比较核心竞品销量或份额。",
        "evidencePackage": evidence_package,
        "visualArtifacts": [
            {"id": "artifact_market_structure_chart", "type": "chart", "title": "Market structure chart"},
            {"id": "artifact_competitor_compare_framework_table", "type": "table", "title": "Competitor comparison table"},
            {"id": "artifact_msrp_source_repair_table", "type": "table", "title": "MSRP source validation table"},
            {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
        ],
    }

    assert eval_service._side_by_side_should_refresh_display_plan(astrbot_side)
    display_plan = eval_service._side_by_side_display_plan(astrbot_side, evidence_package)

    assert "Market structure chart" in display_plan
    assert "Competitor comparison table" in display_plan
    assert "MSRP source validation table" in display_plan
    assert "市场结构图不能当作车型胜负或价格证据" in display_plan
    assert "可用柱状图比较" not in display_plan


def test_business_review_display_line_uses_output_view_language() -> None:
    text = eval_service._sanitize_business_review_line(
        "展示骨架：先看 Pricing corridor chart 判断目标价位置，再用 Pricing evidence table 拆 MSRP 和竞品走廊。"
    )

    assert "展示骨架" not in text
    assert "先看" not in text
    assert "输出视图：已生成 价格走廊图 呈现目标价位置" in text
    assert "价格证据表" in text


def test_business_review_does_not_invent_question_specific_bom_directs_without_entity_evidence() -> None:
    base_answer = {
        "answerPreview": (
            "直接结论：瑞典 库存/BOM 问题应先建实体关系，再判断异常；"
            "车型版本、物料号、市场、颜色、PI、订单和生命周期必须分层建模。"
        ),
        "evidencePackage": {
            "intent": "inventory_analysis",
            "missingEvidence": [
                {
                    "name": "bom_entity_mapping_evidence",
                    "reason": "BOM/entity mapping evidence is incomplete.",
                    "impact": "weakens_answer",
                }
            ],
        },
        "recommendedActions": [
            {"action": "补齐 BOM/entity mapping 底表"},
        ],
        "businessImplications": [
            "库存/BOM 问题应先把车型版本、物料号、颜色、市场、PI 和订单生命周期分层。",
        ],
        "visualArtifacts": [
            {"id": "artifact_bom_entity_validation_table", "title": "BOM / entity mapping validation table"},
        ],
    }

    omoda9_text = eval_service._business_review_answer_text({
        **base_answer,
        "question": "OMODA9 一个版型多个物料号应该怎么解释？",
    })
    modeling_text = eval_service._business_review_answer_text({
        **base_answer,
        "question": "BOM、车型版本、内外饰颜色之间应该怎么建模？",
    })
    editable_quantity_text = eval_service._business_review_answer_text({
        **base_answer,
        "question": "当月选品表如何从物料号转成客户可编辑数量？",
    })

    assert "当前记录没有可引用证据支撑" in omoda9_text
    assert "OMODA9 一个版型多个物料号" in omoda9_text
    assert "不自动等于数据错误" not in omoda9_text
    assert "business variant、material code、color/interior" not in omoda9_text
    assert "建物料号解释矩阵" in omoda9_text
    assert "当前记录没有可引用证据支撑" in modeling_text
    assert "PI header -> market overlay -> business variant -> material code/SKU" not in modeling_text
    assert "物料号负责供应链和车辆生成" not in modeling_text
    assert "固定实体层级" in modeling_text
    assert "当前记录没有可引用证据支撑" in editable_quantity_text
    assert "扣除已分配、冻结订单、生命周期 phase-out" not in editable_quantity_text
    assert "定义可编辑数量公式" in editable_quantity_text


def test_business_failure_tags_include_blocking_missing_evidence() -> None:
    record = {
        "questionId": "biz-compare-001",
        "category": "competitor_compare",
        "country": "Sweden",
        "question": "J7 HEV 的核心竞品是谁？",
        "expectedIntent": "competitor_compare",
        "expectedTools": ["compare_competitive_set"],
        "astrbot": {
            "answerPreview": "瑞典 J7 HEV 可以先按 RAV4、Sportage、Corolla Cross 构建竞品池。",
            "answerStatus": "partially_answered",
            "selectedTool": "compare_competitive_set",
            "evidenceRefCount": 4,
            "evidencePackage": {
                "toolResults": [
                    {"toolName": "compare_competitive_set", "evidenceRefs": [{"refId": "ev_competitor_pool"}]},
                ],
            },
            "missingEvidence": [
                {
                    "name": "configuration_delta",
                    "reason": "Intent matrix requires configuration_delta.",
                    "impact": "blocking",
                }
            ],
            "recommendedActions": [{"action": "补齐配置差异矩阵后再定销售话术"}],
            "businessImplications": ["竞品池已可初步建立，但配置 gap 会影响可赢点判断。"],
            "reportReadyBullets": ["先定竞品池，再补配置价值差异。"],
        },
        "comparison": {
            "astrbotAnswerChars": 420,
            "answerLengthDelta": -120,
        },
        "humanScoring": {"status": "pending"},
    }

    tags = eval_service.infer_business_failure_tags(record)

    assert "evidence_missing" in tags
    assert "tool_missing" not in tags


def test_business_failure_tags_do_not_mark_handled_partial_tco_gap_as_failure(tmp_path, monkeypatch) -> None:
    record = {
        "comparisonId": "cmp_policy_tco_handled_gap",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-policy-005",
        "category": "policy_news",
        "country": "Sweden",
        "question": "大客户 leasing 场景下，PHEV 还有没有理由？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "build_market_chart"],
        "astrbot": {
            "answerPreview": (
                "直接结论：瑞典大客户 leasing 场景下 PHEV 已有公司车暴露信号：公司车注册占比 64.8 %，"
                "PHEV 注册量 6,498 units，私人注册占比 35.2 %。这支持把 PHEV 保留为 fleet/TCO 验证线，"
                "但当前仍缺月供、残值、税务 benefit 和大客户口径，不能直接证明 PHEV 应主推。\n\n"
                "## 下一步动作\n- P0 · 建立 PHEV fleet leasing TCO 表。"
            ),
            "answerStatus": "partially_answered",
            "selectedTool": "search_market_news",
            "evidenceRefCount": 12,
            "evidencePackage": {
                "country": "Sweden",
                "toolResults": [
                    {
                        "toolName": "build_market_chart",
                        "evidenceRefs": [
                            {
                                "refId": "phev_business",
                                "label": "PHEV 公司车注册占比",
                                "value": 64.8,
                                "unit": "%",
                            }
                        ],
                    }
                ],
            },
            "evidenceDigest": [
                "Sweden PHEV 公司车注册占比 64.8%，支持 fleet/TCO 验证线，但缺 monthly/RV/tax 证据。",
            ],
            "displayPlan": "Sweden PHEV company-car TCO validation table and PPT-ready report block.",
            "missingEvidence": [
                {
                    "name": "minimum_external_sources",
                    "reason": "pricing_analysis requires at least 2 external sources; 1 usable sources were kept.",
                    "impact": "blocking",
                },
                {
                    "name": "leasing_tco_or_company_car_evidence",
                    "reason": "Missing monthly/RV/TCO evidence.",
                    "impact": "weakens_answer",
                },
            ],
            "visualArtifacts": [
                {"id": "artifact_tco_validation_table", "type": "table", "title": "TCO / company-car validation table"},
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
            "recommendedActions": [{"action": "建立 PHEV fleet leasing TCO 表"}],
            "businessImplications": [
                "PHEV 可保留为 fleet/TCO 验证线，但不能在缺月供/RV/税费时直接主推。",
            ],
            "reportReadyBullets": [
                "PHEV 公司车暴露信号强，但 TCO 优势仍需月供、残值和税务口径验证。",
            ],
            "qualityScore": {
                "businessSynthesisScore": 1.0,
                "businessCompletenessScore": 0.72,
                "failures": ["missing_blocking_evidence"],
            },
        },
        "comparison": {
            "bothReturned": True,
            "errorCount": 0,
            "astrbotAnswerChars": 980,
            "answerLengthDelta": -100,
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }

    tags = eval_service.infer_business_failure_tags(record)

    assert "evidence_missing" not in tags
    assert "answer_too_conservative" not in tags

    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    summary = report["summary"]

    assert summary["failureTagCounts"] == {}
    assert summary["topFailureTags"] == []
    assert summary["repairGapCounts"] == {"leasing_tco_or_company_car_evidence": 1}
    assert summary["topRepairGaps"][0]["gap"] == "leasing_tco_or_company_car_evidence"


def test_business_report_normalizes_legacy_competitor_configuration_gap(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_legacy_empty_tags",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-compare-001",
        "category": "competitor_compare",
        "country": "Sweden",
        "question": "J7 HEV 的核心竞品是谁？",
        "expectedIntent": "competitor_compare",
        "expectedTools": ["compare_competitive_set"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "compare_competitive_set",
            "answerPreview": "Sweden J7 HEV 的竞品池可以初步定位，但配置差异仍缺证据。",
            "evidenceRefCount": 3,
            "visualArtifacts": [
                {"type": "table", "title": "Competitor matrix", "data": {"rows": []}},
            ],
            "missingEvidence": [
                {
                    "name": "configuration_delta",
                    "reason": "Intent matrix requires configuration_delta.",
                    "impact": "blocking",
                }
            ],
            "recommendedActions": [{"action": "生成配置差异矩阵"}],
            "businessImplications": ["缺配置差异会影响可赢点和销售话术。"],
            "reportReadyBullets": ["补齐配置差异后才能进入汇报。"],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0, "astrbotAnswerChars": 420, "answerLengthDelta": -120},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    queue = report["evidenceRepairQueue"]

    assert item["failureTags"] == []
    assert item["businessValidation"]["failureTags"] == []
    assert item["astrbot"]["missingEvidence"][0]["impact"] == "weakens_answer"
    assert report["summary"]["failureTagCounts"] == {}
    assert queue[0]["priority"] == "P1"
    assert queue[0]["failureTags"] == []


def test_business_report_drops_report_outline_gap_when_report_block_exists(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_report_outline_noise",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-report-001",
        "category": "report_generation",
        "country": "Sweden",
        "question": "把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        "expectedIntent": "report_generation",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "Sweden J7 HEV 定价页已生成 Title / Key message / Evidence / Product implication / Next action，并把低配价格锚点、高配主推、竞品 corridor 和下一步数据补齐动作放进一页汇报结构。",
            "evidenceRefCount": 4,
            "visualArtifacts": [
                {"type": "report_block", "title": "PPT-ready block", "data": {"title": "J7 HEV pricing"}},
            ],
            "missingEvidence": [
                {
                    "name": "report_outline",
                    "reason": "报告类问题需要清晰输出结构。",
                    "impact": "weakens_answer",
                }
            ],
            "reportReadyBullets": [
                "Title: J7 HEV pricing.",
                "Evidence: competitor corridor.",
                "Next action: build one-slide deck.",
            ],
            "businessImplications": ["低配作为价格锚点，高配作为主推版本。"],
            "recommendedActions": [{"action": "补齐竞品 MSRP 后直接生成一页 deck。"}],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0, "astrbotAnswerChars": 520, "answerLengthDelta": -120},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]

    assert item["astrbot"]["missingEvidence"] == []
    assert report["evidenceRepairQueue"] == []
    assert item["failureTags"] == []
    assert report["summary"]["failureTagCounts"] == {}


def test_business_report_orders_pricing_artifacts_for_review(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_pricing_stale_artifact_order",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-002",
        "category": "pricing",
        "country": "Sweden",
        "question": "J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "answerPreview": "J7 HEV 需要先看价格走廊，再看修复候选。",
            "visualArtifacts": [
                {"id": "artifact_metric_cards", "type": "metric_cards", "title": "Key metrics"},
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
                {"id": "artifact_msrp_source_repair_table", "type": "table", "title": "MSRP source validation table"},
                {"id": "artifact_pricing_analysis_table", "type": "table", "title": "Pricing evidence table"},
                {"id": "artifact_pricing_corridor_chart", "type": "chart", "title": "Pricing corridor chart"},
            ],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "toolResults": [],
                "missingEvidence": [
                    {"name": "current_msrp", "reason": "Missing requested-model MSRP.", "impact": "weakens_answer"}
                ],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    artifact_ids = [artifact["id"] for artifact in item["astrbot"]["visualArtifacts"]]

    assert artifact_ids == [
        "artifact_pricing_corridor_chart",
        "artifact_pricing_analysis_table",
        "artifact_msrp_source_repair_table",
        "artifact_report_block",
        "artifact_metric_cards",
    ]


def test_business_report_refreshes_stale_pricing_table_for_relative_delta(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_pricing_relative_delta_refresh",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-003",
        "category": "pricing",
        "country": "Sweden",
        "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "answerPreview": "3k 价差需要验证。",
            "visualArtifacts": [
                {
                    "id": "artifact_pricing_analysis_table",
                    "type": "table",
                    "title": "Pricing evidence table",
                    "data": {
                        "rows": [
                            {
                                "model": "Competitor corridor",
                                "powertrain": "BEV",
                                "msrp": "39121.74 EUR-53165.22 EUR",
                                "monthlyPayment": "待补月供/租赁方案",
                                "rv": "待补残值/RV",
                                "pricePosition": "样本最低-最高价格走廊",
                                "action": "先在 MSRP 来源验证表中验证搜索候选和来源草稿，确认 URL 后生成当前价格记录；这些候选只是补证线索。",
                            }
                        ]
                    },
                }
            ],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]},
                "toolResults": [
                    {
                        "toolName": "query_price_positioning",
                        "success": True,
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "EUR", "source": "jato_msrp_postgres"},
                            {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "EUR", "source": "jato_msrp_postgres"},
                            {"refId": "ev_delta", "label": "User supplied relative price delta", "value": 3000, "unit": "EUR", "source": "user_question"},
                        ],
                    }
                ],
                "missingEvidence": [
                    {"name": "current_msrp", "reason": "O5/EV3 official MSRP missing.", "impact": "weakens_answer"}
                ],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    pricing_table = next(
        artifact
        for artifact in item["astrbot"]["visualArtifacts"]
        if artifact["id"] == "artifact_pricing_analysis_table"
    )
    rows = pricing_table["data"]["rows"]
    delta_row = next(row for row in rows if row["model"] == "Relative price delta")

    assert delta_row["msrp"] == "3,000 EUR"
    assert "用户给定价差假设" in delta_row["pricePosition"]
    assert "搜索候选" not in str(rows)
    assert "来源草稿" not in str(rows)


def test_business_report_refreshes_stale_o5_ev3_report_block_key_message(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_pricing_o5_ev3_stale_report_block",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-003",
        "category": "pricing",
        "country": "Sweden",
        "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "answerPreview": (
                "价差判断：O5 BEV 比 EV3 小电池便宜 3k 暂时只能作为验证假设。"
                "当前价格样本显示：样本走廊 39,121.7-53,165.2，中位数 52,130.4。"
                "当前仍缺 O5/EV3 官方 MSRP 或版本价差证据。"
            ),
            "reportReadyBullets": [
                (
                    "Key message：价差判断：O5 BEV 比 EV3 小电池便宜 3k 暂时只能作为验证假设。"
                    "当前价格样本显示：样本走廊 39,121.7-53,165.2，中位数 52,130.4。"
                    "当前仍缺 O5/EV3 官方 MSRP 或版本价差证据。"
                ),
                "Evidence：本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证",
                "Next action：补齐本车型与竞品 MSRP / TP / 月供价格矩阵",
            ],
            "recommendedActions": [{"action": "补齐本车型与竞品 MSRP / TP / 月供价格矩阵"}],
            "visualArtifacts": [
                {
                    "id": "artifact_report_block",
                    "type": "report_block",
                    "title": "PPT-ready block",
                    "data": {
                        "keyMessage": (
                            "价差判断：O5 BEV 比 EV3 小电池便宜 3k 暂时只能作为验证假设。"
                            "当前价格样本显示：样本走廊 39,121.7-53,165.2，中位数 52,130.4。"
                            "当前仍缺 O5/EV3 官方 MSRP 或版本价差证据。"
                        )
                    },
                },
                {
                    "id": "artifact_pricing_analysis_table",
                    "type": "table",
                    "title": "Pricing evidence table",
                    "data": {"rows": [{"model": "Relative price delta", "msrp": "3,000 EUR", "pricePosition": "用户给定价差假设"}]},
                },
            ],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]},
                "toolResults": [
                    {
                        "toolName": "query_price_positioning",
                        "success": True,
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "EUR", "source": "jato_msrp_postgres"},
                            {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "EUR", "source": "jato_msrp_postgres"},
                            {"refId": "ev_delta", "label": "User supplied relative price delta", "value": 3000, "unit": "EUR", "source": "user_question"},
                        ],
                    }
                ],
                "missingEvidence": [
                    {"name": "current_msrp", "reason": "O5/EV3 official MSRP missing.", "impact": "weakens_answer"}
                ],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    report_block = next(
        artifact
        for artifact in item["astrbot"]["visualArtifacts"]
        if artifact["id"] == "artifact_report_block"
    )
    key_message = report_block["data"]["keyMessage"]

    assert "当前价格样本显示" not in key_message
    assert "非本题核心车型的已物化价格背景" in key_message
    assert "不能当作 O5/EV3 官方 MSRP 或竞品价格走廊" in key_message


def test_business_report_orders_bom_entity_artifacts_for_review(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_bom_artifact_order",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-bom-002",
        "category": "inventory_bom",
        "country": "Sweden",
        "question": "BOM、车型版本、内外饰颜色之间应该怎么建模？",
        "expectedIntent": "inventory_analysis",
        "expectedTools": ["query_with_filters"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "answerPreview": "直接结论：BOM 应先建立实体关系。",
            "displayPlan": "用 BOM/库存关系表展示车型版本、物料号、颜色、市场、订单和生命周期异常。",
            "visualArtifacts": [
                {"id": "artifact_metric_cards", "type": "metric_cards", "title": "Key metrics"},
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
                {"id": "artifact_inventory_analysis_table", "type": "table", "title": "Inventory / BOM evidence table"},
                {"id": "artifact_bom_entity_validation_table", "type": "table", "title": "BOM / entity mapping validation table"},
            ],
            "evidencePackage": {
                "intent": "inventory_analysis",
                "country": "Sweden",
                "toolResults": [],
                "missingEvidence": [
                    {"name": "bom_entity_mapping_evidence", "reason": "Need BOM entity mapping.", "impact": "weakens_answer"}
                ],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    artifact_ids = [artifact["id"] for artifact in item["astrbot"]["visualArtifacts"]]

    assert artifact_ids == [
        "artifact_bom_entity_validation_table",
        "artifact_inventory_analysis_table",
        "artifact_report_block",
        "artifact_metric_cards",
    ]
    assert "BOM/entity mapping validation table" in item["astrbot"]["displayPlan"]
    assert "material code" in item["astrbot"]["displayPlan"]


def test_business_report_orders_report_generation_artifacts_for_review(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_report_generation_stale_artifact_order",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-report-001",
        "category": "report_generation",
        "country": "Sweden",
        "question": "把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        "expectedIntent": "report_generation",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "answerPreview": "J7 HEV report answer.",
            "visualArtifacts": [
                {"id": "artifact_metric_cards", "type": "metric_cards", "title": "Key metrics"},
                {
                    "id": "artifact_report_generation_table",
                    "type": "table",
                    "title": "Report evidence appendix",
                    "data": {
                        "rows": [
                            {
                                "section": "Market evidence",
                                "evidence": "contextSnapshot.powertrainMix.BEV.sales: 25,235 units",
                                "source": "jato_country_chart_deck",
                            },
                            {
                                "section": "Pricing evidence",
                                "evidence": "priceStats.count: 4 EUR",
                                "source": "jato_msrp_postgres",
                            },
                        ],
                    },
                },
                {"id": "artifact_trend_series_chart", "type": "chart", "title": "Trend"},
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
            "evidencePackage": {
                "intent": "report_generation",
                "country": "Sweden",
                "toolResults": [],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    artifact_ids = [artifact["id"] for artifact in item["astrbot"]["visualArtifacts"]]

    assert artifact_ids == [
        "artifact_report_block",
        "artifact_report_generation_table",
        "artifact_trend_series_chart",
        "artifact_metric_cards",
    ]
    appendix = next(
        artifact for artifact in item["astrbot"]["visualArtifacts"]
        if artifact["id"] == "artifact_report_generation_table"
    )
    rows = appendix["data"]["rows"]
    assert rows[0]["evidence"] == "BEV 动力销量: 25,235 units"
    assert rows[0]["source"] == "JATO 图表数据"
    assert rows[1]["evidence"] == "价格样本数: 4"
    assert rows[1]["source"] == "JATO MSRP 数据"
    assert "contextSnapshot." not in str(rows)
    assert "priceStats." not in str(rows)
    assert "jato_" not in str(rows)


def test_business_report_review_mentions_pricing_artifacts_for_j7_report(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_report_generation_pricing_artifacts",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-report-001",
        "category": "report_generation",
        "country": "Sweden",
        "question": "把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        "expectedIntent": "report_generation",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "answerPreview": "直接结论：瑞典 J7 HEV 定价应围绕核心竞争带中段 + 高配主推。",
            "visualArtifacts": [
                {"id": "artifact_metric_cards", "type": "metric_cards", "title": "Key metrics"},
                {"id": "artifact_report_generation_table", "type": "table", "title": "Report evidence appendix"},
                {"id": "artifact_report_pricing_table", "type": "table", "title": "Pricing evidence table"},
                {"id": "artifact_pricing_corridor_chart", "type": "chart", "title": "Pricing corridor chart"},
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
            "reportReadyBullets": [
                "Title：瑞典 J7 HEV 定价逻辑：核心竞争带中段 + 高配主推",
                "Key message：低配做价格锚点，高配做主推版本。",
            ],
            "evidencePackage": {
                "intent": "report_generation",
                "country": "Sweden",
                "toolResults": [
                    {
                        "toolName": "business_method_material",
                        "evidenceRefs": [
                            {"refId": "ev_j7_msrp", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    artifact_ids = [artifact["id"] for artifact in item["astrbot"]["visualArtifacts"]]

    assert artifact_ids[:4] == [
        "artifact_report_block",
        "artifact_pricing_corridor_chart",
        "artifact_report_pricing_table",
        "artifact_report_generation_table",
    ]
    assert "价格走廊图" in item["astrbotAnswer"]
    assert "价格证据表" in item["astrbotAnswer"]
    assert "PVA" in item["astrbotAnswer"]


def test_business_report_display_plan_distinguishes_j7_user_material_price_from_current_msrp() -> None:
    evidence_package = {
        "intent": "report_generation",
        "country": "Sweden",
        "toolResults": [
            {
                "toolName": "business_method_material",
                "evidenceRefs": [
                    {"refId": "ev_j7_msrp", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                ],
            }
        ],
        "missingEvidence": [
            {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
        ],
    }
    astrbot_side = {
        "displayPlan": "用 report block 输出可复制的一页 PPT 结构，并附证据表或图表作为 appendix。",
        "evidencePackage": evidence_package,
        "visualArtifacts": [
            {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            {"id": "artifact_pricing_corridor_chart", "type": "chart", "title": "Pricing corridor chart"},
            {"id": "artifact_report_pricing_table", "type": "table", "title": "Pricing evidence table"},
        ],
    }

    assert eval_service._side_by_side_should_refresh_display_plan(astrbot_side)
    display_plan = eval_service._side_by_side_display_plan(astrbot_side, evidence_package)

    assert "PPT-ready block" in display_plan
    assert "用户材料价格锚点" in display_plan
    assert "当前官方 MSRP、竞品官方价格和月供/RV 仍需补源" in display_plan


def test_pricing_display_plan_distinguishes_j7_user_material_price_without_current_price_gap() -> None:
    evidence_package = {
        "intent": "pricing_analysis",
        "country": "Sweden",
        "toolResults": [
            {
                "toolName": "business_method_material",
                "evidenceRefs": [
                    {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_msrp", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                ],
            }
        ],
        "missingEvidence": [],
    }
    astrbot_side = {
        "displayPlan": (
            "先看 Pricing corridor chart 判断目标价或官方 MSRP 在价格走廊中的位置；"
            "再看 Pricing evidence table 拆 MSRP、价差、月供/RV 和证据边界。"
        ),
        "evidencePackage": evidence_package,
        "visualArtifacts": [
            {"id": "artifact_pricing_corridor_chart", "type": "chart", "title": "Pricing corridor chart"},
            {"id": "artifact_pricing_analysis_table", "type": "table", "title": "Pricing evidence table"},
            {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
        ],
    }

    assert eval_service._side_by_side_should_refresh_display_plan(astrbot_side)
    display_plan = eval_service._side_by_side_display_plan(astrbot_side, evidence_package)

    assert "用户材料价格锚点" in display_plan
    assert "用户材料价格不能直接当作当前官方 MSRP" in display_plan
    assert "官网 MSRP、月供/RV 和竞品官方价格" in display_plan
    assert "判断目标价或官方 MSRP" not in display_plan


def test_report_generation_recomputes_stale_requested_model_digest() -> None:
    record = {
        "comparisonId": "cmp_report_stale_requested_digest",
        "validationType": "business",
        "questionId": "biz-report-002",
        "category": "report_generation",
        "country": "Sweden",
        "question": "生成 O5 BEV 对标 EX30 和 EV3 的一页竞品汇报框架。",
        "expectedIntent": "report_generation",
        "expectedTools": ["compare_competitive_set"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerPreview": "O5 BEV report.",
            "evidenceDigest": [
                "MODEL Y.sales = 2,412 units（jato_cross_reference）",
                "competitor.5.model = ID.7（jato_cross_reference）",
            ],
            "evidencePackage": {
                "intent": "report_generation",
                "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
                "toolResults": [
                    {
                        "toolName": "compare_competitive_set",
                        "sourceType": "jato_cross_reference",
                        "evidenceRefs": [
                            {"refId": "ev_modely", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato_cross_reference"},
                            {"refId": "ev_ex30", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                            {"refId": "ev_ev3", "label": "EV3.pricePosition", "value": "price / configuration anchor", "source": "jato_cross_reference"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    assert enriched["astrbot"]["evidenceDigest"] == [
        "竞品汇报覆盖 = O5 BEV / EX30 / EV3 待补完整 MSRP、配置/电池/续航和来源日期",
        "缺口 = 单个竞品销量或市场背景不能支撑完整对标页结论",
        "EX30 销量 = 1,518 units（JATO 交叉引用）",
        "EV3.pricePosition = price / configuration anchor（JATO 交叉引用）",
    ]
    assert "MODEL Y" not in enriched["astrbotAnswer"]
    assert "ID.7" not in enriched["astrbotAnswer"]


def test_business_report_refreshes_missing_pricing_repair_table_from_answer_candidates(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_pricing_missing_repair_artifact",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-002",
        "category": "pricing",
        "country": "Sweden",
        "question": "J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "answerPreview": "J7 HEV 需要补官方 MSRP 来源。",
            "visualArtifacts": [
                {"id": "artifact_metric_cards", "type": "metric_cards", "title": "Key metrics"},
            ],
            "sourceRepairCandidates": {
                "dataStatus": "competitor_current_price_available_own_model_missing",
                "missingOwnModelSource": True,
                "candidateCount": 1,
                "ownModel": [
                    {
                        "brand": "",
                        "model": "J7 HEV",
                        "sourceCode": "msrp-source-sweden-j7-hev-1",
                        "draftStatus": "candidate_search_query",
                        "candidateSourceType": "generic_official_price_search",
                        "sourceSearchQuery": "Sweden J7 HEV official price MSRP",
                    }
                ],
                "competitorCorridor": [],
            },
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "toolResults": [],
                "missingEvidence": [
                    {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"}
                ],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    repair_table = next(
        artifact
        for artifact in item["astrbot"]["visualArtifacts"]
        if artifact["id"] == "artifact_msrp_source_repair_table"
    )

    assert repair_table["type"] == "table"
    assert repair_table["data"]["rows"][0]["model"] == "J7 HEV"
    assert repair_table["data"]["rows"][0]["searchQuery"] == "Sweden J7 HEV official price MSRP"


def test_business_report_accepts_nordic_context_for_sweden_configuration_answer(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_nordic_config_answer",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-config-001",
        "category": "configuration",
        "country": "Sweden",
        "question": "A0 SUV BEV 为什么需要 80kWh 电池？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["compare_vehicle_variants"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "compare_vehicle_variants",
            "answerPreview": (
                "电池判断：A0 SUV BEV 在北欧不是所有版本都必须 80kWh；"
                "80kWh 应定位为长续航/高配安全边界，低配仍要保留价格锚点。"
            ),
            "evidenceRefCount": 3,
            "missingEvidence": [
                {
                    "name": "competitive_or_configuration_data_unavailable",
                    "reason": "No full competitor/configuration matrix yet.",
                    "impact": "weakens_answer",
                }
            ],
            "visualArtifacts": [
                {"type": "table", "title": "80kWh validation matrix", "data": {"rows": []}},
            ],
            "recommendedActions": [{"action": "生成 A0 SUV BEV 80kWh 续航-价格-重量验证表。"}],
            "businessImplications": ["80kWh 做高配安全边界，低配保留价格锚点。"],
            "reportReadyBullets": [
                "配置判断：80kWh 解决冬季续航折损、跨城出行和用户里程焦虑。",
                "用户场景：私人家庭、郊区通勤、周末长途或公司车更需要 80kWh。",
                "产品动作：低配保价格锚点，高配/长续航版用 80kWh 打价值感。",
            ],
            "qualityScore": {"businessCompletenessScore": 0.92, "failures": []},
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0, "astrbotAnswerChars": 430, "answerLengthDelta": -120},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]

    assert "answer_too_generic" not in item["failureTags"]
    assert report["summary"]["failureTagCounts"] == {}


def test_business_report_restores_legacy_stringified_evidence_refs(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    legacy_refs = [
        {
            "refId": "ev_policy_1",
            "label": "policy_source",
            "value": "Swedish policy source",
            "source": "jato_external_research_web",
            "retrievedAt": "2026-06-12T00:00:00Z",
        }
    ]
    record = {
        "comparisonId": "cmp_legacy_refs",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-policy-003",
        "category": "policy_news",
        "country": "Sweden",
        "question": "CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["external_research"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "external_research",
            "answerPreview": "Policy answer with cited source.",
            "evidencePackage": {
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "success": True,
                        "evidenceRefs": repr(legacy_refs),
                    }
                ],
                "missingEvidence": [
                    {
                        "name": "published_date",
                        "reason": "Research policy requires publish dates.",
                        "impact": "weakens_answer",
                    }
                ],
            },
            "missingEvidence": [
                {
                    "name": "published_date",
                    "reason": "Research policy requires publish dates.",
                    "impact": "weakens_answer",
                }
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    refs = item["astrbotEvidencePackage"]["toolResults"][0]["evidenceRefs"]

    assert isinstance(refs, list)
    assert refs[0]["refId"] == "ev_policy_1"
    assert item["astrbot"]["evidenceRefCount"] == 1
    assert item["astrbotEvidencePackage"]["missingEvidence"][0]["name"] == "published_date"


def test_business_report_drops_published_date_gap_when_date_ref_exists(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    legacy_refs = [
        {
            "refId": "ev_policy_date",
            "label": "Swedish policy source.date",
            "value": "2026-05-01",
            "source": "jato_external_research_web",
            "retrievedAt": "2026-06-12T00:00:00Z",
        },
        {
            "refId": "ev_policy_claim",
            "label": "Swedish policy source.claim",
            "value": "CO2 band policy affects PHEV company-car economics.",
            "source": "jato_external_research_web",
            "retrievedAt": "2026-06-12T00:00:00Z",
        },
    ]
    record = {
        "comparisonId": "cmp_legacy_policy_date",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-policy-003",
        "category": "policy_news",
        "country": "Sweden",
        "question": "CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["external_research"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "external_research",
            "answerPreview": "Policy answer with dated cited source.",
            "evidencePackage": {
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "success": True,
                        "evidenceRefs": repr(legacy_refs),
                    }
                ],
                "missingEvidence": [
                    {
                        "name": "published_date",
                        "reason": "Research policy requires publish dates.",
                        "impact": "weakens_answer",
                    }
                ],
            },
            "missingEvidence": [
                {
                    "name": "published_date",
                    "reason": "Research policy requires publish dates.",
                    "impact": "weakens_answer",
                }
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]

    assert item["astrbotEvidencePackage"]["missingEvidence"] == []
    assert item["astrbot"]["missingEvidence"] == []
    assert report["evidenceRepairQueue"][0]["missingEvidence"] == []
    assert report["evidenceRepairQueue"][0]["repairSummary"]["primaryGap"] != "published_date"


def test_business_report_marks_truncated_legacy_evidence_refs(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    truncated_refs = "[{'refId': 'ev_policy_1', 'label': 'policy_source', 'value': 'truncated"
    record = {
        "comparisonId": "cmp_truncated_refs",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-policy-001",
        "category": "policy_news",
        "country": "Sweden",
        "question": "Elbilspremien 2026 会影响哪些车型？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["external_research"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "external_research",
            "answerPreview": "Policy answer with legacy refs.",
            "evidencePackage": {
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "success": True,
                        "evidenceRefs": truncated_refs,
                    }
                ],
                "missingEvidence": [],
            },
            "missingEvidence": [],
            "evidenceRefCount": 5,
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    refs = item["astrbotEvidencePackage"]["toolResults"][0]["evidenceRefs"]

    assert refs[0]["refId"] == "legacy_evidence_refs_truncated"
    assert refs[0]["legacyTruncated"] is True
    assert item["astrbot"]["evidenceRefCount"] == 1
    assert "evidence_missing" not in item["failureTags"]


def test_business_report_reinfers_blocking_price_tags_for_legacy_empty_failure_tags(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_legacy_price_tags",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-004",
        "category": "pricing",
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "Sweden O9 当前缺少官方 MSRP，不能确认 53k-55k 欧元是否合理。",
            "evidenceRefCount": 2,
            "missingEvidence": [
                {
                    "name": "current_msrp",
                    "reason": "No current official MSRP.",
                    "impact": "blocking",
                }
            ],
            "recommendedActions": [{"action": "补齐 O9 当前 MSRP 后重跑。"}],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0, "astrbotAnswerChars": 420, "answerLengthDelta": -120},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    queue = report["evidenceRepairQueue"]

    assert "evidence_missing" in item["failureTags"]
    assert "evidence_missing" in item["businessValidation"]["failureTags"]
    assert report["summary"]["failureTagCounts"]["evidence_missing"] == 1
    assert queue[0]["priority"] == "P0"
    assert "evidence_missing" in queue[0]["failureTags"]


def test_business_report_downgrades_current_msrp_when_user_target_price_is_present(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_target_price_policy",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-004",
        "category": "pricing",
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "query_price_positioning"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "53k-55k EUR 是用户给定目标价，需要验证竞品价格走廊。",
            "evidenceRefCount": 3,
            "evidencePackage": {
                "evidenceId": "evpkg_target_price_policy",
                "intent": "pricing_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "toolResults": [
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
                    }
                ],
                "missingEvidence": [
                    {"name": "current_msrp", "reason": "Official current MSRP not materialized.", "impact": "blocking"},
                    {"name": "competitor_price_range", "reason": "Need competitor price corridor.", "impact": "blocking"},
                ],
            },
            "missingEvidence": [
                {"name": "current_msrp", "reason": "Official current MSRP not materialized.", "impact": "blocking"},
                {"name": "competitor_price_range", "reason": "Need competitor price corridor.", "impact": "blocking"},
            ],
            "recommendedActions": [{"action": "补齐竞品 MSRP 后重跑。"}],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0, "astrbotAnswerChars": 420, "answerLengthDelta": -120},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    missing = {entry["name"]: entry for entry in item["astrbot"]["missingEvidence"]}
    queue = report["evidenceRepairQueue"]

    assert missing["current_msrp"]["impact"] == "weakens_answer"
    assert missing["competitor_price_range"]["impact"] == "blocking"
    assert item["astrbotEvidencePackage"]["missingEvidence"][0]["impact"] == "weakens_answer"
    assert queue[0]["primaryGap"] == "competitor_price_range"
    assert queue[0]["repairTasks"][0]["taskType"] == "competitor_price_corridor"
    assert "evidence_missing" in item["failureTags"]


def test_business_report_exposes_repair_gaps_before_human_scoring(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_unscored_repair_gap",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-voc-002",
        "category": "voc",
        "country": "Sweden",
        "question": "拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "external_research",
            "answerPreview": "VOC answer with a missing external source gap.",
            "missingEvidence": [
                {
                    "name": "external_research_claims_unavailable",
                    "reason": "No citation-ready external source evidence.",
                    "impact": "weakens_answer",
                }
            ],
            "recommendedActions": [{"action": "补论坛/媒体/VOC 来源。"}],
            "sourceRepairCandidates": {
                "dataStatus": "source_draft_only_not_price_evidence",
                "ownModel": [],
                "competitorCorridor": [
                    {"brand": "TOYOTA", "model": "RAV4", "sourceCode": "toyota_rav4_se"},
                    {"brand": "KIA", "model": "SPORTAGE", "sourceCode": "kia_sportage_se"},
                ],
                "candidateCount": 2,
                "materializedCandidateCount": 0,
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    summary = report["summary"]

    assert summary["repairGapCounts"] == {"external_research_claims_unavailable": 1}
    assert summary["topRepairGaps"][0]["gap"] == "external_research_claims_unavailable"
    assert summary["topRepairGaps"][0]["sampleCandidates"]
    assert any(
        "tow/roof-load" in str(candidate).lower() or "winter-tyre" in str(candidate).lower()
        for candidate in summary["topRepairGaps"][0]["sampleCandidates"]
    )
    assert summary["topRepairGaps"][0]["sampleQuestionIds"] == ["biz-voc-002"]
    assert summary["topRepairGaps"][0]["sampleQuestions"][0]["questionId"] == "biz-voc-002"
    repair_actions = [
        item
        for item in summary["recommendedNextActions"]
        if item["tag"] == "external_research_claims_unavailable"
    ]
    assert repair_actions
    assert repair_actions[0]["source"] == "repair_gap"
    assert repair_actions[0]["sampleCandidates"]
    assert repair_actions[0]["sampleQuestionIds"] == ["biz-voc-002"]
    assert "External Research Evidence" in repair_actions[0]["module"]
    assert "优先候选" in repair_actions[0]["recommendation"]
    assert "biz-voc-002" in repair_actions[0]["recommendation"]
    assert "external_research_claims_unavailable" in report["markdown"]


def test_business_report_refreshes_voc_external_source_repair_artifacts(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_voc_source_repair_artifact",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-voc-003",
        "category": "voc",
        "country": "Sweden",
        "question": "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "external_research",
            "answerPreview": "当前缺少可追溯 VOC 来源，不能写成高频吐槽。",
            "missingEvidence": [
                {
                    "name": "external_research_claims_unavailable",
                    "reason": "No citation-ready VOC source-backed claim evidence was available.",
                    "impact": "weakens_answer",
                },
                {
                    "name": "minimum_external_sources",
                    "reason": "voc_analysis requires at least 1 citation-ready external or VOC source.",
                    "impact": "weakens_answer",
                },
            ],
            "evidencePackage": {
                "intent": "voc_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "success": True,
                        "sourceType": "web",
                        "evidenceRefs": [],
                    }
                ],
                "missingEvidence": [
                    {"name": "external_research_claims_unavailable", "impact": "weakens_answer"},
                    {"name": "minimum_external_sources", "impact": "weakens_answer"},
                ],
            },
            "sourceRepairCandidates": {
                "dataStatus": "external_research_query_candidates",
                "candidateCount": 2,
                "materializedCandidateCount": 0,
                "queries": [
                    "OMODA JAECOO Sweden Sverige owner review complaint forum",
                    "OMODA JAECOO Sverige ägare recension problem forum klagomål",
                ],
                "competitorCorridor": [
                    {
                        "sourceCode": "voc-source-sweden-1",
                        "brand": "VOC",
                        "model": "OMODA JAECOO Sweden owner review",
                        "sourceUrl": "https://www.google.com/search?q=OMODA+JAECOO+Sweden+owner+review",
                        "draftStatus": "candidate_search_query",
                    }
                ],
            },
            "visualArtifacts": [
                {"id": "artifact_metric_cards", "type": "metric_cards", "title": "Key metrics"},
                {
                    "id": "artifact_external_source_repair_table",
                    "type": "table",
                    "title": "External source repair table",
                    "data": {
                        "rows": [
                            {
                                "candidateType": "VOC / media / forum query",
                                "queryOrSource": "OMODA JAECOO Sweden owner review",
                                "sourceStatus": "search query candidate",
                            }
                        ]
                    },
                    "spec": {"columns": ["candidateType", "queryOrSource", "sourceStatus"]},
                },
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    queue_action = report["evidenceRepairQueue"][0]["repairAction"]
    sample_action = report["summary"]["topRepairGaps"][0]["sampleQuestions"][0]["repairAction"]
    artifact_ids = [artifact["id"] for artifact in item["astrbot"]["visualArtifacts"]]
    repair_table = next(
        artifact
        for artifact in item["astrbot"]["visualArtifacts"]
        if artifact["id"] == "artifact_external_source_repair_table"
    )

    assert artifact_ids[:2] == ["artifact_external_source_repair_table", "artifact_voc_analysis_framework_table"]
    assert "External source validation matrix" in item["astrbot"]["displayPlan"]
    assert repair_table["spec"]["evidenceMode"] == "external_source_repair_candidates_not_citations"
    assert repair_table["title"] == "External source validation matrix"
    assert repair_table["data"]["rows"][0]["sourceNeed"] == "VOC owner/media source"
    assert repair_table["data"]["rows"][0]["validationStage"] == "search query candidate"
    assert repair_table["data"]["rows"][0]["canUseInAnswer"] == "No - validate first"
    assert "candidateType" not in repair_table["data"]["rows"][0]
    assert "外部来源验证矩阵" in queue_action
    assert "补证线索" in queue_action
    assert "外部来源修复表" not in queue_action
    assert "补证入口" not in queue_action
    assert sample_action == queue_action


def test_business_report_generates_v2h_external_research_candidates(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_v2h_missing_external_candidates",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-voc-001",
        "category": "voc",
        "country": "Sweden",
        "question": "瑞典用户会不会把 V2H 当成真实购买卖点？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "external_research",
            "answerPreview": "V2H 只能作为待验证卖点。",
            "missingEvidence": [
                {"name": "external_research_claims_unavailable", "impact": "weakens_answer"},
                {"name": "minimum_external_sources", "impact": "weakens_answer"},
            ],
            "evidencePackage": {
                "intent": "voc_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "toolResults": [
                    {"toolName": "external_research", "success": True, "sourceType": "web", "evidenceRefs": []},
                ],
                "missingEvidence": [
                    {"name": "external_research_claims_unavailable", "impact": "weakens_answer"},
                    {"name": "minimum_external_sources", "impact": "weakens_answer"},
                ],
            },
            "visualArtifacts": [
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    item = eval_service.get_business_validation_report()["items"][0]
    candidates = item["astrbot"]["sourceRepairCandidates"]
    repair_table = next(
        artifact
        for artifact in item["astrbot"]["visualArtifacts"]
        if artifact["id"] == "artifact_external_source_repair_table"
    )
    rows = repair_table["data"]["rows"]

    assert candidates["dataStatus"] == "external_research_query_candidates"
    assert candidates["candidateCount"] == 3
    assert candidates["sourceSearchPlan"][0]["sourceSearchQuery"] == "Sweden V2H EV purchase driver owner review forum"
    assert item["astrbot"]["visualArtifacts"][0]["id"] == "artifact_external_source_repair_table"
    assert rows[0]["sourceNeed"] == "VOC owner/media source"
    assert rows[0]["queryOrSource"] == "Sweden V2H EV purchase driver owner review forum"
    assert rows[0]["canUseInAnswer"] == "No - validate first"


def test_business_report_uses_pricing_source_candidates_for_price_external_gap(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_pricing_external_candidates",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-003",
        "category": "pricing",
        "country": "Sweden",
        "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "O5 BEV vs EV3 needs official MSRP validation.",
            "evidenceRefCount": 3,
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
                {"name": "minimum_external_sources", "impact": "blocking"},
            ],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "toolResults": [
                    {"toolName": "query_msrp_pricing", "success": True, "sourceType": "postgres", "evidenceRefs": []},
                    {"toolName": "external_research", "success": True, "sourceType": "web", "evidenceRefs": []},
                ],
                "missingEvidence": [
                    {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
                    {"name": "minimum_external_sources", "impact": "blocking"},
                ],
            },
            "visualArtifacts": [
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    item = eval_service.get_business_validation_report()["items"][0]
    candidates = item["astrbot"]["sourceRepairCandidates"]
    queries = candidates["queries"]

    assert candidates["dataStatus"] == "external_research_query_candidates"
    assert queries[0] == "O5 BEV Sweden official price MSRP"
    assert any(query == "Kia EV3 Sweden official price MSRP" for query in queries[:4])
    assert all("owner review complaint forum" not in query for query in queries[:3])
    assert candidates["competitorCorridor"][0]["brand"] == "Pricing"
    assert candidates["competitorCorridor"][0]["model"] == "O5 BEV official price source"
    repair_table = next(
        artifact
        for artifact in item["astrbot"]["visualArtifacts"]
        if artifact["id"] == "artifact_external_source_repair_table"
    )
    rows = repair_table["data"]["rows"]
    assert item["astrbot"]["visualArtifacts"][0]["id"] == "artifact_external_source_repair_table"
    assert rows[0]["sourceNeed"] == "Official price/MSRP source"
    assert rows[0]["queryOrSource"] == "O5 BEV Sweden official price MSRP"
    assert rows[0]["evidenceUse"].startswith("Validate official MSRP/current price")
    assert rows[0]["requiredFields"] == "URL, title, publish date, model/trim, currency, MSRP/current price"
    assert "官方价格/MSRP 来源" in item["astrbot"]["displayPlan"]
    backlog = eval_service.get_business_validation_report()["sourceRepairBacklog"]
    pricing_backlog = next(entry for entry in backlog if entry["label"] == "Pricing O5 BEV official price source")
    assert pricing_backlog["sourceType"] == "external_price_source"
    assert "官方价格/MSRP 来源" in pricing_backlog["recommendedAction"]
    assert "VOC/媒体/论坛" not in pricing_backlog["recommendedAction"]
    minimum_action = next(
        action
        for action in eval_service.get_business_validation_report()["summary"]["recommendedNextActions"]
        if action["tag"] == "minimum_external_sources"
    )
    assert minimum_action["module"] == "Pricing Source Materialization"
    assert minimum_action["priority"] == "P0"
    assert "citation-ready price evidence" in minimum_action["recommendation"]


def test_business_report_refreshes_pricing_display_plan_when_chart_is_not_generated() -> None:
    record = {
        "comparisonId": "cmp_pricing_no_chart_display_plan",
        "validationType": "business",
        "questionId": "biz-pricing-003",
        "category": "pricing",
        "country": "Sweden",
        "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "displayPlan": "用价格证据表展示本车型 MSRP、目标价、竞品走廊、月供/RV 缺口；若有价格区间，再用柱状图标出目标价在走廊中的位置。",
            "answerPreview": "O5 BEV vs EV3 needs official MSRP validation.",
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
            ],
            "reportReadyBullets": [
                "优先展示价格走廊图和价格证据表，把 O5/EV3 的 3,000 EUR 价差假设拆成证据验证。",
            ],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]},
                "confidence": "low",
                "toolResults": [
                    {
                        "toolName": "user_supplied_price_delta",
                        "success": True,
                        "sourceType": "generated",
                        "evidenceRefs": [
                            {
                                "refId": "ev_delta",
                                "label": "User supplied relative price delta",
                                "value": -3000,
                                "unit": "EUR",
                                "source": "user_question",
                            },
                        ],
                    }
                ],
                "missingEvidence": [
                    {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
                ],
            },
            "sourceRepairCandidates": {
                "dataStatus": "own_model_current_price_source_candidates",
                "missingOwnModelSource": True,
                "materializedCandidateCount": 0,
                "ownModel": [
                    {
                        "sourceCode": "msrp-source-sweden-o5-bev-1",
                        "brand": "",
                        "model": "O5 BEV",
                        "draftStatus": "candidate_search_query",
                        "candidateSourceType": "generic_official_price_search",
                        "sourceSearchQuery": "Sweden O5 BEV pris price MSRP official",
                    },
                    {
                        "sourceCode": "msrp-source-sweden-ev3-2",
                        "brand": "KIA",
                        "model": "EV3",
                        "draftStatus": "candidate_search_query",
                        "candidateSourceType": "brand_official_search",
                        "candidateDomain": "kia.com/se",
                        "sourceSearchQuery": "site:kia.com/se Sweden KIA EV3 pris price MSRP official",
                    },
                ],
                "competitorCorridor": [],
                "candidateCount": 2,
            },
            "visualArtifacts": [
                {"id": "artifact_pricing_analysis_table", "type": "table", "title": "Pricing evidence table"},
                {"id": "artifact_msrp_source_repair_table", "type": "table", "title": "MSRP source validation table"},
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }

    item = eval_service._enrich_business_record_for_read(record)
    display_plan = item["astrbot"]["displayPlan"]
    answer_text = item["astrbotAnswer"]

    assert "Pricing evidence table" in display_plan
    assert "MSRP source validation table" in display_plan
    assert "柱状图" not in display_plan
    assert "价格走廊图只在" in display_plan
    assert "优先展示价格走廊图" not in answer_text
    assert "未补齐官方价格前不生成价格走廊图" in answer_text


def test_business_report_refreshes_tco_display_plan_before_external_repair() -> None:
    evidence_package = {
        "intent": "pricing_analysis",
        "confidence": "medium",
        "missingEvidence": [
            {"name": "leasing_tco_or_company_car_evidence", "reason": "Missing monthly/RV/TCO evidence."}
        ],
    }
    astrbot_side = {
        "displayPlan": (
            "先看 External source validation matrix 核对官方价格/MSRP 来源、发布日期、车型/版本、币种和价格字段；"
            "验证后再把 citation-ready price evidence 接入价格走廊图、定价表和汇报块。"
        ),
        "evidencePackage": evidence_package,
        "visualArtifacts": [
            {"id": "artifact_external_source_repair_table", "type": "table", "title": "External source validation matrix"},
            {"id": "artifact_tco_validation_table", "type": "table", "title": "TCO / company-car validation table"},
            {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready summary"},
        ],
    }

    assert eval_service._side_by_side_should_refresh_display_plan(astrbot_side)
    display_plan = eval_service._side_by_side_display_plan(astrbot_side, evidence_package)

    assert "TCO / company-car validation table" in display_plan
    assert "月供/RV" in display_plan
    assert "benefit tax" in display_plan
    assert "不能把 PHEV 写成大客户主推结论" in display_plan
    assert "官方价格/MSRP" not in display_plan


def test_business_report_policy_price_cap_keeps_policy_pricing_before_tco() -> None:
    question = "BEV 补贴价格上限对 O5 BEV 定价有什么影响？"
    evidence_package = {
        "intent": "news_policy_search",
        "confidence": "medium",
        "missingEvidence": [],
    }
    astrbot_side = {
        "displayPlan": (
            "先看 TCO / company-car validation table 拆 benefit tax、月供、残值、年里程和充电条件；"
            "再用 External source validation matrix / policy/news evidence table 核对官方来源和适用对象。"
        ),
        "evidencePackage": evidence_package,
        "visualArtifacts": [
            {"id": "artifact_external_source_repair_table", "type": "table", "title": "External source validation matrix"},
            {"id": "artifact_news_policy_search_table", "type": "table", "title": "policy/news evidence table"},
            {"id": "artifact_policy_pricing_table", "type": "table", "title": "Pricing evidence table"},
            {"id": "artifact_tco_validation_table", "type": "table", "title": "TCO / company-car validation table"},
            {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready summary"},
            {"id": "artifact_pricing_corridor_chart", "type": "chart", "title": "Pricing corridor chart"},
        ],
    }

    assert eval_service._side_by_side_should_refresh_display_plan(astrbot_side, question=question)
    display_plan = eval_service._side_by_side_display_plan(astrbot_side, evidence_package, question=question)

    assert display_plan.startswith("先看 policy/news evidence table")
    assert "Pricing evidence table / Pricing corridor chart" in display_plan
    assert "政策价格上限" in display_plan
    assert "TCO / company-car validation table 只作月供/RV 补充验证" in display_plan
    assert "JATO channel mix 只能说明公司车暴露" not in display_plan


def test_business_review_company_car_benefit_uses_tco_display_language() -> None:
    text = eval_service._business_review_answer_text({
        "questionId": "biz-policy-002",
        "question": "瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        "answerStatus": "partially_answered",
        "confidence": "medium",
        "answerPreview": "政策边界：company car benefit 应拆 benefit tax、月供、残值和充电条件。",
        "displayPlan": (
            "先看 External source validation matrix 核对官方/可引用政策来源、发布日期、适用对象和限制；"
            "再看 policy/news evidence table 和 report block 输出车型、价格和渠道动作。"
        ),
        "visualArtifacts": [
            {"id": "artifact_external_source_repair_table", "type": "table", "title": "External source validation matrix"},
            {"id": "artifact_news_policy_search_table", "type": "table", "title": "policy/news evidence table"},
            {"id": "artifact_policy_market_context_table", "type": "table", "title": "Policy market context table"},
            {"id": "artifact_tco_validation_table", "type": "table", "title": "TCO / company-car validation table"},
            {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready summary"},
        ],
        "evidencePackage": {
            "intent": "news_policy_search",
            "confidence": "medium",
            "missingEvidence": [
                {"name": "leasing_tco_or_company_car_evidence", "reason": "Missing monthly/RV/TCO evidence."}
            ],
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {
                            "refId": "ev_bev_business",
                            "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Business_pct",
                            "value": 60.3,
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
        },
        "missingEvidence": [
            {"name": "leasing_tco_or_company_car_evidence", "reason": "Missing monthly/RV/TCO evidence."}
        ],
    })

    assert "TCO/company-car 验证表" in text
    assert "JATO channel mix 是暴露信号" in text
    assert "不是 TCO 结论" in text
    assert "判断 BEV/PHEV 谁更优" in text
    assert "policy/news evidence table 和 report block 输出车型、价格和渠道动作" not in text


def test_business_report_refreshes_configuration_external_source_candidates(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_config_winter_external_candidates",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-config-003",
        "category": "configuration",
        "country": "Sweden",
        "question": "北欧市场冬季包应该包含什么？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["query_cross_country", "search_market_news"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "search_market_news",
            "answerPreview": "冬季包需要外部配置来源验证。",
            "missingEvidence": [
                {"name": "external_research_claims_unavailable", "impact": "weakens_answer"},
                {"name": "competitive_or_configuration_data_unavailable", "impact": "weakens_answer"},
            ],
            "evidencePackage": {
                "intent": "configuration_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "toolResults": [
                    {
                        "toolName": "query_cross_country",
                        "success": True,
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "se_bev", "label": "crossCountry.Sweden.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_cross_country"},
                            {"refId": "no_bev", "label": "crossCountry.Norway.powertrainMix.BEV.sales", "value": 26617, "unit": "units", "source": "jato_cross_country"},
                        ],
                    },
                ],
                "missingEvidence": [
                    {"name": "external_research_claims_unavailable", "impact": "weakens_answer"},
                    {"name": "competitive_or_configuration_data_unavailable", "impact": "weakens_answer"},
                ],
            },
            "visualArtifacts": [
                {"id": "artifact_configuration_analysis_table", "type": "table", "title": "Configuration validation matrix"},
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    item = eval_service.get_business_validation_report()["items"][0]
    artifact_ids = [artifact["id"] for artifact in item["astrbot"]["visualArtifacts"]]
    candidates = item["astrbot"]["sourceRepairCandidates"]
    repair_table = next(
        artifact
        for artifact in item["astrbot"]["visualArtifacts"]
        if artifact["id"] == "artifact_external_source_repair_table"
    )
    first_row = repair_table["data"]["rows"][0]

    assert candidates["dataStatus"] == "external_research_query_candidates"
    assert candidates["queries"][0] == "Sweden Nordic EV winter package heat pump battery preconditioning test"
    assert artifact_ids[:2] == [
        "artifact_external_source_repair_table",
        "artifact_configuration_analysis_table",
    ]
    assert first_row["sourceNeed"] == "Configuration/media source"
    assert first_row["requiredFields"] == "URL, title, publish date, test condition, feature claim, model relevance"
    assert first_row["canUseInAnswer"] == "No - validate first"


def test_business_report_read_sanitizes_report_block_product_implication_tail() -> None:
    artifacts = eval_service._order_side_by_side_visual_artifacts_for_read(
        "competitor_compare",
        [
            {
                "id": "artifact_report_block",
                "type": "report_block",
                "title": "PPT-ready block",
                "data": {
                    "keyMessage": "O9 与 XC60 / EX60 应先写成错位定位判断。",
                    "productImplication": (
                        "把已验证的销量/价格/级别锚点先转成定位差异，再补目标车型价格、配置和用户场景；"
                        "不要只停在生成矩阵。 这些搜索候选只是补证线索，不能直接当作官方价格证据。"
                    ),
                    "nextAction": "生成竞品矩阵",
                },
            }
        ],
    )

    block = artifacts[0]
    product_implication = block["data"]["productImplication"]

    assert product_implication == "把已验证的销量/价格/级别锚点先转成定位差异，再补目标车型价格、配置和用户场景；不要只停在生成矩阵。"
    assert "补证线索" not in product_implication
    assert "搜索候选" not in product_implication


def test_business_report_sanitizes_legacy_internal_answer_preview(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_legacy_internal_preview",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-voc-002",
        "category": "voc",
        "country": "Sweden",
        "question": "拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "external_research",
            "answerPreview": (
                "Risk：external_research_claims_unavailable 会影响 External research returned only "
                "source/date/count refs; no supported claim or source-backed business finding was available.\n"
                "Evidence Limits\n"
                "Business Composer: evidence alignment is partially_aligned.\n"
                "Missing evidence: external_research_claims_unavailable (weakens_answer)."
            ),
            "missingEvidence": [
                {
                    "name": "external_research_claims_unavailable",
                    "reason": "No citation-ready external source evidence.",
                    "impact": "weakens_answer",
                }
            ],
            "recommendedActions": [{"action": "补论坛/媒体/VOC 来源。"}],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    preview = item["astrbot"]["answerPreview"]

    assert "外部来源结论不足" in preview
    assert "来源/日期/数量线索" in preview
    assert "尚未形成可引用的业务结论" in preview
    assert "证据对齐：部分对齐" not in preview
    assert "external_research_claims_unavailable" not in preview
    assert "source/date/count refs" not in preview
    assert "Business Composer" not in preview
    assert "weakens_answer" not in preview
    assert item["astrbot"]["missingEvidence"][0]["name"] == "external_research_claims_unavailable"
    assert report["summary"]["repairGapCounts"] == {"external_research_claims_unavailable": 1}


def test_business_report_repair_actions_name_source_candidates(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_price_source_candidates",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-001",
        "category": "pricing",
        "country": "Sweden",
        "question": "瑞典 J7 HEV 应该怎么定价？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "Pricing answer needs current MSRP coverage.",
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Requested model has no current price row.",
                    "impact": "weakens_answer",
                }
            ],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "toolResults": [
                    {
                        "toolName": "query_msrp_pricing",
                        "success": True,
                        "sourceType": "postgres",
                        "evidenceRefs": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_requested_models",
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
            },
            "sourceRepairCandidates": {
                "dataStatus": "source_draft_only_not_price_evidence",
                "missingOwnModelSource": True,
                "ownModel": [],
                "competitorCorridor": [
                    {"brand": "TOYOTA", "model": "COROLLA CROSS", "sourceCode": "toyota_corolla_cross_se"},
                    {"brand": "TOYOTA", "model": "RAV4", "sourceCode": "toyota_rav4_se"},
                    {"brand": "KIA", "model": "SPORTAGE", "sourceCode": "kia_sportage_se"},
                ],
                "candidateCount": 3,
                "materializedCandidateCount": 0,
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "businessValidation": {"failureTags": []},
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    summary = report["summary"]
    top_gap = summary["topRepairGaps"][0]
    action = next(
        item
        for item in summary["recommendedNextActions"]
        if item["tag"] == "coverage_diagnostic:no_current_prices_for_requested_models"
    )

    assert top_gap["sampleCandidates"] == ["TOYOTA COROLLA CROSS", "TOYOTA RAV4", "KIA SPORTAGE"]
    assert top_gap["sampleQuestionIds"] == ["biz-pricing-001"]
    assert action["sampleCandidates"] == top_gap["sampleCandidates"]
    assert action["sampleQuestionIds"] == top_gap["sampleQuestionIds"]
    assert "TOYOTA COROLLA CROSS" in action["recommendation"]
    assert "biz-pricing-001" in action["recommendation"]
    assert "KIA SPORTAGE" in action["recommendation"]


def test_business_validation_human_scores_and_report(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    record = eval_service.run_business_validation_question("biz-pricing-001")

    updated = eval_service.update_eval_side_by_side_human_score(
        record["comparisonId"],
        {
            "status": "scored",
            "winner": "astrbot",
            "notes": "AstrBot is more grounded.",
            "dimensions": record["humanScoring"]["dimensions"],
            "astrbotScores": {
                "intentAccuracy": 5,
                "toolSelection": 4,
                "grounding": 5,
                "pmInsight": 4,
                "actionability": 4,
                "artifactQuality": 4,
                "followUpValue": 5,
                "presentationReadiness": 4,
            },
            "countryCopilotScores": {
                "intentAccuracy": 3,
                "toolSelection": 2,
                "grounding": 2,
                "pmInsight": 3,
                "actionability": 3,
                "artifactQuality": 2,
                "followUpValue": 1,
                "presentationReadiness": 3,
            },
        },
    )
    report = eval_service.get_business_validation_report()

    assert updated["humanScoring"]["scoreTotals"]["astrbot"] > updated["humanScoring"]["scoreTotals"]["countryCopilot"]
    assert updated["humanScoring"]["source"] == "manual"
    assert updated["businessValidation"]["winner"] == "astrbot"
    assert isinstance(updated["failureTags"], list)
    assert report["summary"]["scoredCount"] == 1
    assert report["summary"]["baselineScoredCount"] == 1
    assert report["summary"]["replacementBaselineScoredCount"] == 1
    assert report["summary"]["pendingBaselineScoring"] == 0
    assert report["summary"]["pendingReplacementBaselineScoring"] == 0
    assert report["summary"]["humanScoreSourceCounts"] == {"manual": 1}
    assert report["summary"]["baselineSourceCounts"] == {"manual": 1}
    assert report["summary"]["replacementBaselineSourceCounts"] == {"manual": 1}
    assert report["summary"]["humanWins"]["astrbot"] == 1
    assert report["summary"]["replacementWins"]["astrbot"] == 1
    assert report["summary"]["astrbotWinRate"] == 1
    assert report["summary"]["replacementAstrbotWinRate"] == 1
    assert "replacementReadinessVerdict" in report["summary"]
    assert report["summary"]["replacementReadiness"]["sourceCounts"] == {"manual": 1}
    assert report["summary"]["replacementReadiness"]["scoredCount"] == 1
    assert report["summary"]["replacementReadiness"]["pendingCount"] == 0
    assert report["summary"]["replacementReadiness"]["businessBaselineReady"] is True
    assert report["summary"]["replacementReadiness"]["astrbotWinRate"] == 1
    assert "categoryLevelScore" in report["summary"]
    assert "failureTagCounts" in report["summary"]
    assert "# AstrBot Business Validation Report" in report["markdown"]
    assert "Human score sources" in report["markdown"]
    assert "Replacement readiness verdict" in report["markdown"]


def test_business_validation_preserves_codex_review_score_source(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    record = eval_service.run_business_validation_question("biz-pricing-001")
    dimensions = record["humanScoring"]["dimensions"]

    updated = eval_service.update_eval_side_by_side_human_score(
        record["comparisonId"],
        {
            "status": "scored",
            "source": "codex_review",
            "winner": "tie",
            "notes": "[Accepted Codex draft review] uiStatus=pass",
            "dimensions": dimensions,
            "astrbotScores": {key: 4 for key in dimensions},
            "countryCopilotScores": {key: 4 for key in dimensions},
        },
    )
    report = eval_service.get_business_validation_report()

    assert updated["humanScoring"]["source"] == "codex_review"
    assert updated["humanScoring"]["notes"].startswith("[Accepted Codex draft review]")
    assert report["summary"]["humanScoreSourceCounts"] == {"codex_review": 1}
    assert report["summary"]["baselineSourceCounts"] == {"codex_review": 1}
    assert report["summary"]["replacementBaselineSourceCounts"] == {}
    assert report["summary"]["replacementBaselineScoredCount"] == 0
    assert report["summary"]["pendingBaselineScoring"] == 0
    assert report["summary"]["pendingReplacementBaselineScoring"] == 1
    assert report["summary"]["scoredCount"] == 1
    assert report["summary"]["selfTestBaseline"]["sourceCounts"] == {"codex_review": 1}
    assert report["summary"]["selfTestBaseline"]["scoredCount"] == 1
    assert report["summary"]["selfTestBaseline"]["codexReviewedCount"] == 1
    assert report["summary"]["selfTestBaseline"]["trustedBaselineCount"] == 0
    assert report["summary"]["selfTestBaseline"]["selfTestReady"] is True
    assert report["summary"]["replacementReadinessVerdict"] == "not_enough_human_scores"
    assert report["summary"]["replacementReadiness"]["sourceCounts"] == {}
    assert report["summary"]["replacementReadiness"]["scoredCount"] == 0
    assert report["summary"]["replacementReadiness"]["pendingCount"] == 1
    assert report["summary"]["replacementReadiness"]["businessBaselineReady"] is False
    assert report["summary"]["replacementReadiness"]["recommendedNextAction"].startswith("Score 1 more")


def test_business_validation_self_test_counts_raw_codex_review_notes(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    notes_file = tmp_path / "codex_review_notes.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(eval_service, "_CODEX_REVIEW_NOTES_FILE", notes_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    record = eval_service.run_business_validation_question("biz-pricing-001")
    dimensions = record["humanScoring"]["dimensions"]
    notes_file.write_text(
        json.dumps(
            {
                "questionId": record["questionId"],
                "uiStatus": "warning",
                "suggestedWinner": "astrbot",
                "suggestedScores": {
                    "astrbot": {key: 5 for key in dimensions},
                    "countryCopilot": {key: 3 for key in dimensions},
                },
                "suggestedFailureTags": [],
                "reviewNotes": "Draft review only.",
                "createdAt": "2026-06-17T16:13:33.081Z",
                "source": "codex_review",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    report = eval_service.get_business_validation_report()
    summary = report["summary"]

    assert summary["scoredCount"] == 0
    assert summary["baselineSourceCounts"] == {}
    assert summary["replacementBaselineSourceCounts"] == {}
    assert summary["replacementBaselineScoredCount"] == 0
    assert summary["replacementReadiness"]["scoredCount"] == 0
    assert summary["replacementReadiness"]["businessBaselineReady"] is False
    assert summary["selfTestBaseline"]["sourceCounts"] == {"codex_review_draft": 1}
    assert summary["selfTestBaseline"]["scoredCount"] == 1
    assert summary["selfTestBaseline"]["pendingCount"] == 0
    assert summary["selfTestBaseline"]["codexReviewedCount"] == 1
    assert summary["selfTestBaseline"]["trustedBaselineCount"] == 0
    assert summary["selfTestBaseline"]["astrbotWinRate"] == 1
    assert summary["selfTestBaseline"]["avgAstrBotScore"] == 5
    assert summary["selfTestBaseline"]["avgCountryCopilotScore"] == 3
    assert summary["selfTestBaseline"]["selfTestReady"] is True


def test_business_validation_report_uses_latest_record_per_question(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )

    first = eval_service.run_business_validation_question("biz-pricing-001")
    second = eval_service.run_business_validation_question("biz-pricing-001")

    full_history = eval_service.list_eval_side_by_side_results(category="pricing")
    latest_queue = eval_service.list_eval_side_by_side_results(category="pricing", latest_per_question=True)
    report = eval_service.get_business_validation_report(category="pricing")

    assert full_history["total"] == 2
    assert latest_queue["total"] == 1
    assert latest_queue["items"][0]["comparisonId"] == second["comparisonId"]
    assert latest_queue["items"][0]["comparisonId"] != first["comparisonId"]
    assert report["total"] == 1
    assert report["items"][0]["comparisonId"] == second["comparisonId"]
    assert report["summary"]["count"] == 1


def test_business_validation_report_exposes_evidence_repair_queue(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_repair_1",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-004",
        "category": "pricing",
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "当前证据不足以确认瑞典 O9 官方 MSRP。",
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_country",
                    "reason": "Add current price observations for Sweden before numeric pricing claims.",
                    "impact": "blocking",
                },
                {
                    "name": "price_corridor",
                    "reason": "No competitor price corridor evidenceRefs were returned.",
                    "impact": "weakens_answer",
                },
            ],
            "recommendedActions": [
                {
                    "action": "补齐 Sweden current_prices 与竞品价格走廊后重跑 biz-pricing-004。",
                    "rationale": "Pricing validation cannot pass without country-level price evidence.",
                    "priority": "P0",
                }
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": ["evidence_missing"],
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    queue = report["evidenceRepairQueue"]

    assert len(queue) == 1
    assert queue[0]["questionId"] == "biz-pricing-004"
    assert queue[0]["priority"] == "P0"
    assert "当前价格" in queue[0]["repairAction"]
    assert "竞品价格走廊" in queue[0]["repairAction"]
    assert queue[0]["missingEvidence"][0]["name"] == "coverage_diagnostic:no_current_prices_for_country"
    assert queue[0]["primaryGap"] == "coverage_diagnostic:no_current_prices_for_country"
    assert "当前价格记录" in queue[0]["commandHint"]
    assert queue[0]["repairSummary"]["primaryGap"] == "coverage_diagnostic:no_current_prices_for_country"
    assert queue[0]["repairSummary"]["missingEvidenceCount"] == 2
    assert queue[0]["repairSummary"]["blockingEvidenceCount"] == 1
    assert queue[0]["repairSummary"]["weakEvidenceCount"] == 1
    assert queue[0]["repairTasks"][0]["taskType"] == "own_model_msrp_source"
    assert queue[0]["repairTasks"][0]["priority"] == "P0"
    assert queue[0]["repairTasks"][-1]["taskType"] == "rerun_business_validation"
    assert "## Evidence Repair Queue" in report["markdown"]
    assert "Primary Gap" in report["markdown"]
    assert "Command Hint" in report["markdown"]
    assert "Source Summary" in report["markdown"]
    assert "Repair Tasks" in report["markdown"]
    assert "coverage_diagnostic:no_current_prices_for_country" in report["markdown"]


def test_business_validation_repair_queue_uses_source_repair_candidates(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_source_repair_1",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-004",
        "category": "pricing",
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "当前证据不足以确认瑞典 O9 官方 MSRP。",
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_country",
                    "reason": "No Sweden current_prices rows.",
                    "impact": "blocking",
                }
            ],
            "sourceRepairCandidates": {
                "dataStatus": "competitor_current_price_available_own_model_missing",
                "missingOwnModelSource": True,
                "materializedCandidateCount": 2,
                "ownModel": [],
                "competitorCorridor": [
                    {
                        "sourceCode": "toyota_rav4_se_draft_scrapling",
                        "brand": "TOYOTA",
                        "model": "RAV4",
                        "sourceUrl": "https://www.toyota.se/new-cars/rav4",
                        "relativePath": "se/07_toyota_rav4_se.yaml",
                        "draftStatus": "current_price_materialized",
                        "currentPriceRows": 4,
                    },
                    {
                        "sourceCode": "kia_sportage_se_draft_scrapling",
                        "brand": "KIA",
                        "model": "SPORTAGE",
                        "sourceUrl": "https://www.kia.com/se/nya-bilar/sportage/upptack/",
                        "relativePath": "se/13_kia_sportage_se.yaml",
                        "draftStatus": "current_price_materialized",
                        "currentPriceRows": 3,
                    },
                ],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    queue = report["evidenceRepairQueue"]

    assert len(queue) == 1
    assert queue[0]["sourceRepairCandidates"]["candidateCount"] == 2
    assert queue[0]["sourceRepairCandidates"]["missingOwnModelSource"] is True
    assert queue[0]["sourceRepairCandidates"]["materializedCandidateCount"] == 2
    assert queue[0]["sourceRepairCandidates"]["competitorCorridor"][0]["currentPriceRows"] == 4
    assert queue[0]["repairSummary"]["sourceCandidateCount"] == 2
    assert queue[0]["repairSummary"]["competitorCandidateCount"] == 2
    assert queue[0]["repairSummary"]["materializedCandidateCount"] == 2
    assert queue[0]["repairSummary"]["missingOwnModelSource"] is True
    assert "2/2 个来源候选已生成价格行" in queue[0]["repairSummary"]["sourceSummary"]
    assert "本车型来源缺失" in queue[0]["repairSummary"]["sourceSummary"]
    assert queue[0]["primaryGap"] == "coverage_diagnostic:no_current_prices_for_country"
    assert "当前价格记录" in queue[0]["commandHint"]
    assert "MSRP 来源验证表" in queue[0]["repairAction"]
    assert "共2项" in queue[0]["repairAction"]
    assert queue[0]["repairTasks"][0]["taskType"] == "own_model_msrp_source"
    assert queue[0]["repairTasks"][0]["sourceCandidates"] == []
    assert "当前价格记录" in queue[0]["repairTasks"][0]["commandHint"]
    assert queue[0]["repairTasks"][1]["taskType"] == "competitor_price_corridor"
    assert "TOYOTA RAV4" in queue[0]["repairTasks"][1]["sourceCandidates"]
    assert "价格走廊" in queue[0]["repairTasks"][1]["commandHint"]
    assert "TOYOTA RAV4" in queue[0]["repairTasks"][1]["sourceCandidates"]
    assert "KIA SPORTAGE" in queue[0]["repairTasks"][1]["sourceCandidates"]
    assert "2/2 个来源候选已生成价格行" in report["markdown"]
    assert "Validate competitor price corridor" in report["markdown"]
    assert "TOYOTA RAV4" in report["markdown"]


def test_business_validation_repair_queue_splits_config_and_bom_gaps(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    records = [
        {
            "comparisonId": "cmp_config_gap",
            "runAt": "2026-06-12T00:00:00.000Z",
            "validationType": "business",
            "questionId": "biz-config-001",
            "category": "configuration",
            "country": "Sweden",
            "question": "A0 SUV BEV 为什么需要 80kWh 电池？",
            "astrbot": {
                "status": "ok",
                "answerStatus": "answered",
                "selectedTool": "compare_vehicle_variants",
                "missingEvidence": [
                    {
                        "name": "competitive_or_configuration_data_unavailable",
                        "reason": "No usable competitor or configuration evidence.",
                        "impact": "weakens_answer",
                    }
                ],
            },
            "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
            "comparison": {"bothReturned": True, "errorCount": 0},
            "failureTags": [],
            "humanScoring": {"status": "pending"},
        },
        {
            "comparisonId": "cmp_bom_gap",
            "runAt": "2026-06-12T00:00:00.000Z",
            "validationType": "business",
            "questionId": "biz-bom-001",
            "category": "inventory_bom",
            "country": "Sweden",
            "question": "OMODA9 一个版型多个物料号应该怎么解释？",
            "astrbot": {
                "status": "ok",
                "answerStatus": "answered",
                "selectedTool": "query_with_filters",
                "missingEvidence": [
                    {
                        "name": "query_with_filters_weak_evidence_refs",
                        "reason": "Tool returned only weak count/source/date refs.",
                        "impact": "weakens_answer",
                    }
                ],
            },
            "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
            "comparison": {"bothReturned": True, "errorCount": 0},
            "failureTags": [],
            "humanScoring": {"status": "pending"},
        },
    ]
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
        encoding="utf-8",
    )

    report = eval_service.get_business_validation_report()
    queue_by_id = {item["questionId"]: item for item in report["evidenceRepairQueue"]}

    config_task = queue_by_id["biz-config-001"]["repairTasks"][0]
    assert config_task["taskType"] == "competitive_config_matrix"
    assert config_task["title"] == "Build competitor/configuration evidence matrix"
    assert "配置矩阵" in config_task["output"]
    assert "核心竞品的配置矩阵" in config_task["commandHint"]

    bom_task = queue_by_id["biz-bom-001"]["repairTasks"][0]
    assert bom_task["taskType"] == "bom_entity_mapping_evidence"
    assert bom_task["title"] == "Map BOM/entity evidence refs"
    assert "实体映射" in bom_task["output"]
    assert "物料编码" in bom_task["commandHint"]


def test_enrich_business_record_downgrades_inventory_bom_without_entity_refs() -> None:
    record = {
        "comparisonId": "cmp_bom_market_only",
        "validationType": "business",
        "questionId": "biz-bom-002",
        "category": "inventory_bom",
        "country": "Sweden",
        "question": "BOM、车型版本、内外饰颜色之间应该怎么建模？",
        "expectedIntent": "inventory_analysis",
        "expectedTools": ["query_with_filters"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": (
                "直接结论：BOM 问题应先建立实体关系。\n\n"
                "## 关键证据\n"
                "- results.topModels.XC60.value = 2,893（jato_filtered_query）\n"
                "- results.kpis.avgMsrp = 57,954.1（jato_filtered_query）"
            ),
            "evidenceDigest": [
                "results.topModels.XC60.value = 2,893（jato_filtered_query）",
                "results.kpis.avgMsrp = 57,954.1（jato_filtered_query）",
            ],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "inventory_analysis",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "query_country_snapshot",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_xc60", "label": "results.topModels.XC60.value", "value": 2893, "source": "jato_filtered_query"},
                            {"refId": "ev_avg", "label": "results.kpis.avgMsrp", "value": 57954.1, "source": "jato_filtered_query"},
                        ],
                    },
                    {
                        "toolName": "query_with_filters",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_version", "label": "results.kpis.versionCount", "value": 9204, "source": "jato_filtered_query"},
                            {"refId": "ev_sales", "label": "results.kpis.cumulativeSales", "value": 1182452, "source": "jato_filtered_query"},
                        ],
                    },
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]

    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert enriched["astrbot"]["confidence"] == "medium"
    assert enriched["astrbot"]["evidencePackage"]["confidence"] == "medium"
    digest = enriched["astrbot"]["evidenceDigest"]
    assert "BOM 实体证据 = 待补车型版本、物料号、颜色、订单生命周期映射" in digest
    assert "缺口 = 当前市场销量/MSRP 不能证明 BOM 建模、生命周期或可编辑数量" in digest
    assert any(item.startswith("本轮已查数据源 = ") for item in digest)
    assert "bom_entity_mapping_evidence" in missing_names
    assert "results.topModels.XC60" not in enriched["astrbotAnswer"]
    assert "avgMsrp" not in enriched["astrbotAnswer"]
    assert "BOM 实体证据 = 待补车型版本、物料号、颜色、订单生命周期映射" in enriched["astrbotAnswer"]
    assert enriched["astrbot"]["qualityScore"]["totalScore"] <= 0.88
    artifacts = enriched["astrbot"]["visualArtifacts"]
    bom_table = next(item for item in artifacts if item["id"] == "artifact_bom_entity_validation_table")
    assert bom_table["spec"]["columns"] == [
        "entityLayer",
        "mappingNeeded",
        "sourceOrTool",
        "acceptanceCriteria",
        "currentStatus",
        "businessUse",
        "priority",
    ]
    assert bom_table["data"]["rows"][0]["entityLayer"] == "PI / shared header"

    queue = eval_service._build_evidence_repair_queue([enriched])

    assert queue[0]["primaryGap"] == "bom_entity_mapping_evidence"
    assert queue[0]["repairTasks"][0]["taskType"] == "bom_entity_mapping_evidence"


def test_enrich_business_record_downgrades_configuration_without_spec_refs() -> None:
    record = {
        "comparisonId": "cmp_config_market_only",
        "validationType": "business",
        "questionId": "biz-config-001",
        "category": "configuration",
        "country": "Sweden",
        "question": "A0 SUV BEV 为什么需要 80kWh 电池？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["compare_vehicle_variants"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": (
                "电池判断：A0 SUV BEV 在北欧不是所有版本都必须 80kWh。\n\n"
                "## 关键证据\n"
                "- MODEL Y.sales = 2,412 units（jato_cross_reference）\n"
                "- competitor.4.model = EX30（jato_cross_reference）"
            ),
            "evidenceDigest": [
                "MODEL Y.sales = 2,412 units（jato_cross_reference）",
                "competitor.4.model = EX30（jato_cross_reference）",
            ],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "configuration_analysis",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "compare_vehicle_variants",
                        "sourceType": "engineering",
                        "evidenceRefs": [
                            {"refId": "ev_row_count", "label": "row_count", "value": 1, "source": "jato_variant_diff_service"},
                        ],
                    },
                    {
                        "toolName": "compare_competitive_set",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_model_y_sales", "label": "MODEL Y.sales", "value": 2412, "source": "jato_cross_reference"},
                            {"refId": "ev_ex30_model", "label": "competitor.4.model", "value": "EX30", "source": "jato_cross_reference"},
                        ],
                    },
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]

    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert enriched["astrbot"]["confidence"] == "medium"
    assert enriched["astrbot"]["evidencePackage"]["confidence"] == "medium"
    digest = enriched["astrbot"]["evidenceDigest"]
    assert "配置验证项 = 80kWh 长续航/高配安全边界、热泵、电池预热、快充和冬季舒适配置" in digest
    assert "证据状态 = 待补竞品配置/价格证据" in digest
    assert "缺口 = 当前缺少可追溯配置矩阵或工程配置证据" in digest
    assert any(item.startswith("本轮已查数据源 = ") for item in digest)
    assert "competitive_or_configuration_data_unavailable" in missing_names
    assert "MODEL Y.sales" not in enriched["astrbotAnswer"]
    assert "competitor.4.model" not in enriched["astrbotAnswer"]
    assert "配置验证项 = 80kWh 长续航/高配安全边界" in enriched["astrbotAnswer"]
    assert enriched["astrbot"]["qualityScore"]["totalScore"] <= 0.88

    queue = eval_service._build_evidence_repair_queue([enriched])

    assert queue[0]["primaryGap"] == "competitive_or_configuration_data_unavailable"
    assert queue[0]["repairTasks"][0]["taskType"] == "competitive_config_matrix"


def test_enrich_business_record_adds_legacy_configuration_coverage_diagnostic(monkeypatch) -> None:
    monkeypatch.setattr(
        eval_service,
        "_configuration_coverage_diagnostics_for_read",
        lambda _country, _models: {
            "diagnosis": "no_config_projects_for_country",
            "requested": {"country": "Sweden", "queryModels": list(_models)},
            "availableProjectCount": 0,
            "nextActions": [
                "Import or activate engineering configuration projects for the requested market before comparing variants."
            ],
        },
    )
    record = {
        "comparisonId": "cmp_legacy_config_diag",
        "validationType": "business",
        "questionId": "biz-compare-003",
        "category": "configuration",
        "country": "Sweden",
        "question": "J8 7座四驱为什么能打 Sorento？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["compare_vehicle_variants"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "配置判断：J8 7座四驱应对标 Sorento 做配置验证。",
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "configuration_analysis",
                "country": "Sweden",
                "entities": {"models": ["J8", "Sorento"]},
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "compare_vehicle_variants",
                        "query": {"country": "Sweden", "models": ["J8", "Sorento"]},
                        "sourceType": "engineering",
                        "rowCount": 1,
                        "evidenceRefs": [
                            {"refId": "ev_row_count", "label": "row_count", "value": 1, "source": "jato_variant_diff_service"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]
    tool = enriched["astrbot"]["evidencePackage"]["toolResults"][0]
    digest = enriched["astrbot"]["evidenceDigest"]

    assert "competitive_or_configuration_data_unavailable" in missing_names
    assert "coverage_diagnostic:no_config_projects_for_country" in missing_names
    assert tool["rowCount"] == 0
    assert tool["coverageDiagnostics"]["diagnosis"] == "no_config_projects_for_country"
    assert "工程配置覆盖 = 当前国家未导入或未激活工程配置项目" in digest
    assert enriched["astrbot"]["qualityScore"]["totalScore"] <= 0.88

    queue = eval_service._build_evidence_repair_queue([enriched])

    task_types = [task["taskType"] for task in queue[0]["repairTasks"]]
    assert "engineering_config_project_coverage" in task_types


def test_enrich_business_record_keeps_configuration_gap_when_only_chart_context_exists() -> None:
    record = {
        "comparisonId": "cmp_config_chart_context_only",
        "validationType": "business",
        "questionId": "biz-config-001",
        "category": "configuration",
        "country": "Sweden",
        "question": "A0 SUV BEV 为什么需要 80kWh 电池？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["compare_vehicle_variants", "query_country_snapshot", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "电池判断：A0 SUV BEV 80kWh 应做高配安全边界。",
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "configuration_analysis",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "compare_vehicle_variants",
                        "sourceType": "engineering",
                        "evidenceRefs": [
                            {"refId": "ev_row_count", "label": "row_count", "value": 1, "source": "jato_variant_diff_service"},
                        ],
                    },
                    {
                        "toolName": "build_market_chart",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_suva0_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 5416, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "ev_suva_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "ev_suva_bev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.BEV_pct", "value": 40.0, "unit": "%", "source": "jato_country_chart_deck"},
                        ],
                    },
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]
    artifact_ids = [item["id"] for item in enriched["astrbot"]["visualArtifacts"]]

    assert "competitive_or_configuration_data_unavailable" in missing_names
    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert enriched["astrbot"]["confidence"] == "medium"
    assert enriched["astrbot"]["qualityScore"]["totalScore"] <= 0.88
    assert artifact_ids[:2] == ["artifact_market_structure_chart", "artifact_configuration_analysis_table"]
    assert "SUV A0" in str(enriched["astrbot"]["visualArtifacts"][0]["data"])
    assert "A0/A SUV BEV 不应全系强推 80kWh" in enriched["astrbotAnswer"]
    assert "证据支撑" in enriched["astrbotAnswer"]
    assert "SUV A BEV 渗透率 40 %" in enriched["astrbotAnswer"]
    assert "SUV A0 细分销量 = 5,416 units" in enriched["astrbotAnswer"]
    assert "SUV A 细分销量 = 7,544 units" in enriched["astrbotAnswer"]
    assert "低配继续保价格锚点" in enriched["astrbotAnswer"]
    assert "当前应输出可验证的产品定义和配置验证表" in enriched["astrbotAnswer"]
    assert "SUV A0.sales" not in enriched["astrbotAnswer"]
    assert "配置验证项 = 80kWh 长续航/高配安全边界" in enriched["astrbotAnswer"]


def test_enrich_business_record_uses_powertrain_mix_for_high_spec_configuration_context() -> None:
    record = {
        "comparisonId": "cmp_config_powertrain_context",
        "validationType": "business",
        "questionId": "biz-config-002",
        "category": "configuration",
        "country": "Sweden",
        "question": "4.7m A-SUV 为什么要 95kWh + 双电机 + 800V？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["compare_vehicle_variants", "compare_competitive_set", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "配置判断：95kWh + 双电机 + 800V 应作为高价值 BEV 架构。",
            "visualArtifacts": [
                {
                    "id": "artifact_configuration_analysis_table",
                    "type": "table",
                    "title": "Configuration validation matrix",
                    "spec": {
                        "columns": [
                            "feature",
                            "targetModel",
                            "validationData",
                            "sourceOrTool",
                            "acceptanceCriteria",
                            "currentStatus",
                            "priority",
                        ]
                    },
                    "data": {"rows": []},
                },
                {"id": "artifact_report_block", "type": "report_block", "title": "PPT-ready block"},
            ],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "configuration_analysis",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "build_market_chart",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "ev_phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "ev_hev_sales", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units", "source": "jato_country_chart_deck"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    artifact_ids = [item["id"] for item in enriched["astrbot"]["visualArtifacts"]]

    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert artifact_ids[:2] == ["artifact_market_powertrain_mix_chart", "artifact_configuration_analysis_table"]
    assert "95kWh + 双电机 + 800V 应定位为高价值家庭/公司车 BEV 架构" in enriched["astrbotAnswer"]
    assert "证据支撑" in enriched["astrbotAnswer"]
    assert "BEV 动力销量 = 25,235 units" in enriched["astrbotAnswer"]
    assert "PHEV 动力销量 = 15,028 units" in enriched["astrbotAnswer"]
    assert "HEV 动力销量 5,051 units" in enriched["astrbotAnswer"]
    assert "补能效率和 fleet 使用效率" in enriched["astrbotAnswer"]
    assert "不能单独证明电池、续航、充电、冬季包、价格或竞品配置矩阵已经成立" in enriched["astrbotAnswer"]
    assert "contextSnapshot.powertrainMix" not in enriched["astrbotAnswer"]


def test_enrich_business_record_labels_configuration_external_gap_without_voc_copy() -> None:
    record = {
        "comparisonId": "cmp_config_winter_external_gap",
        "validationType": "business",
        "questionId": "biz-config-003",
        "category": "configuration",
        "country": "Sweden",
        "question": "北欧市场冬季包应该包含什么？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["query_cross_country", "compare_vehicle_variants", "search_market_news", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "配置判断：北欧冬季包应先保证冬季可用性。",
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "configuration_analysis",
                "confidence": "medium",
                "toolResults": [
                    {
                        "toolName": "build_market_chart",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "ev_phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                        ],
                    },
                    {
                        "toolName": "search_market_news",
                        "sourceType": "web",
                        "evidenceRefs": [],
                    },
                ],
                "missingEvidence": [
                    {"name": "external_research_claims_unavailable", "reason": "No winter-package source evidence.", "impact": "weakens_answer"},
                    {"name": "competitive_or_configuration_data_unavailable", "reason": "No trim/config matrix.", "impact": "weakens_answer"},
                ],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    digest_text = " ".join(enriched["astrbot"]["evidenceDigest"])
    answer_text = enriched["astrbotAnswer"]

    assert "BEV 动力销量 = 25,235 units" in digest_text
    assert "配置验证项 = 热泵、电池预热、座椅/方向盘加热、冬季胎/TPMS 和真实冬季续航" in digest_text
    assert "VOC 来源状态" not in digest_text
    assert "用户吐槽频次" not in answer_text
    assert "外部配置来源状态" not in answer_text
    assert "北欧冬季包应按 must-have / visible value / optional 分层" in answer_text
    assert "证据支撑" in answer_text
    key_section = answer_text.split("## 关键证据", 1)[1].split("## 产品经理判断", 1)[0]
    assert "BEV 动力销量 = 25,235 units" in key_section
    assert "PHEV 动力销量 = 15,028 units" in key_section
    assert "配置验证项 = 热泵、电池预热" in answer_text


def test_enrich_business_record_uses_nordic_cross_country_configuration_refs_once() -> None:
    record = {
        "comparisonId": "cmp_config_winter_cross_country",
        "validationType": "business",
        "questionId": "biz-config-003",
        "category": "configuration",
        "country": "Sweden",
        "question": "北欧市场冬季包应该包含什么？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["query_cross_country", "compare_vehicle_variants", "search_market_news", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "配置判断：北欧冬季包应先保证冬季可用性。",
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "configuration_analysis",
                "confidence": "medium",
                "entities": {"countries": ["Sweden", "Finland", "Norway", "Denmark"]},
                "toolResults": [
                    {
                        "toolName": "query_cross_country",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "se_bev", "label": "crossCountry.Sweden.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_cross_country"},
                            {"refId": "fi_bev", "label": "crossCountry.Finland.powertrainMix.BEV.sales", "value": 8062, "unit": "units", "source": "jato_cross_country"},
                            {"refId": "no_bev", "label": "crossCountry.Norway.powertrainMix.BEV.sales", "value": 26617, "unit": "units", "source": "jato_cross_country"},
                            {"refId": "dk_bev", "label": "crossCountry.Denmark.powertrainMix.BEV.sales", "value": 13221, "unit": "units", "source": "jato_cross_country"},
                        ],
                    },
                    {"toolName": "search_market_news", "sourceType": "web", "evidenceRefs": []},
                ],
                "missingEvidence": [
                    {"name": "external_research_claims_unavailable", "reason": "No winter-package source evidence.", "impact": "weakens_answer"},
                    {"name": "competitive_or_configuration_data_unavailable", "reason": "No trim/config matrix.", "impact": "weakens_answer"},
                ],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    digest = enriched["astrbot"]["evidenceDigest"]
    answer_text = enriched["astrbotAnswer"]
    key_section = answer_text.split("## 关键证据", 1)[1].split("## 产品经理判断", 1)[0]

    assert digest[:3] == [
        "瑞典 BEV 动力销量 = 25,235 units（JATO 跨国对比）",
        "芬兰 BEV 动力销量 = 8,062 units（JATO 跨国对比）",
        "挪威 BEV 动力销量 = 26,617 units（JATO 跨国对比）",
    ]
    assert "配置验证项 = 热泵、电池预热" in " ".join(digest)
    assert "北欧冬季包应按 must-have / visible value / optional 分层" in answer_text
    assert "瑞典 BEV 动力销量 25,235 units" in answer_text
    assert "挪威 BEV 动力销量 26,617 units" in answer_text
    assert "拖车钩、roof load、远程预热" in answer_text
    assert "当前应输出可验证的产品定义和配置验证表" in answer_text
    assert key_section.count("瑞典 BEV 动力销量") == 1
    assert key_section.count("芬兰 BEV 动力销量") == 1
    assert key_section.count("挪威 BEV 动力销量") == 1


def test_enrich_business_record_refreshes_generic_market_followups() -> None:
    record = {
        "comparisonId": "cmp_generic_suv_followups",
        "validationType": "business",
        "questionId": "biz-market-004",
        "category": "market_overview",
        "country": "Sweden",
        "question": "SUV A0/A 级为什么是主销结构？",
        "expectedIntent": "market_overview",
        "expectedTools": ["query_country_snapshot", "query_segment_breakdown"],
        "expectedFollowUpTypes": ["drilldown", "compare", "action", "report"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "直接结论：SUV A0/A 需要用销量、动力结构和主销车型解释。",
            "actualTools": ["query_country_snapshot", "build_market_chart"],
            "followUps": [
                {"label": "继续深挖数据", "question": "继续深挖数据。", "intent": "drilldown"},
                {"label": "看竞品/邻国对比", "question": "看竞品/邻国对比。", "intent": "compare"},
                {"label": "转成业务动作", "question": "转成业务动作。", "intent": "action"},
                {"label": "生成汇报框架", "question": "生成汇报框架。", "intent": "report"},
            ],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "market_overview",
                "country": "Sweden",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "query_country_snapshot",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_suv_a", "label": "SUV A sales", "value": 12000, "source": "jato_country_snapshot"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    labels = [item["label"] for item in enriched["astrbot"]["followUps"]]
    questions = " ".join(item["question"] for item in enriched["astrbot"]["followUps"])
    assert labels == ["拆 SUV A0/A 数据", "对比竞品/邻国结构", "映射到车型机会", "生成机会页"]
    assert "继续深挖数据" not in labels
    assert "转成业务动作" not in labels
    assert "SUV A0/A 拆到销量、BEV/PHEV/HEV 渗透率" in questions
    assert enriched["astrbotFollowUps"] == enriched["astrbot"]["followUps"]


def test_enrich_business_record_refreshes_generic_bom_followups() -> None:
    record = {
        "comparisonId": "cmp_generic_bom_followups",
        "validationType": "business",
        "questionId": "biz-bom-004",
        "category": "inventory_bom",
        "country": "Sweden",
        "question": "SE/FI 合并 PI 但车辆分市场生成，逻辑是否正确？",
        "expectedIntent": "inventory_analysis",
        "expectedTools": ["query_country_snapshot", "query_with_filters"],
        "expectedFollowUpTypes": ["compare", "data_check", "action"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "直接结论：SE/FI 合并 PI 要拆 header 与 market overlay。",
            "actualTools": ["query_country_snapshot", "query_with_filters"],
            "followUps": [
                {"label": "继续深挖数据", "question": "继续深挖数据。", "intent": "drilldown"},
                {"label": "看竞品/邻国对比", "question": "看竞品/邻国对比。", "intent": "compare"},
                {"label": "转成业务动作", "question": "转成业务动作。", "intent": "action"},
            ],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "inventory_analysis",
                "country": "Sweden",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "query_with_filters",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_rows", "label": "filtered rows", "value": 20, "source": "jato_filtered_query"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    labels = [item["label"] for item in enriched["astrbot"]["followUps"]]
    questions = " ".join(item["question"] for item in enriched["astrbot"]["followUps"])
    assert labels[:3] == ["画实体关系图", "定义校验规则", "生成管理表"]
    assert "继续深挖数据" not in labels
    assert "看竞品/邻国对比" not in labels
    assert "PI header、market overlay、business variant、material code" in questions
    assert "phase-out 和跨市场混用" in questions
    assert enriched["astrbotFollowUps"] == enriched["astrbot"]["followUps"]


def test_enrich_business_record_refreshes_tool_instruction_followups() -> None:
    record = {
        "comparisonId": "cmp_tool_instruction_followups",
        "validationType": "business",
        "questionId": "biz-voc-003",
        "category": "voc",
        "country": "Sweden",
        "question": "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research", "search_market_news"],
        "expectedFollowUpTypes": ["why", "action", "report"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "medium",
            "answerPreview": "直接结论：需要补真实 VOC 来源。",
            "actualTools": ["external_research", "search_market_news"],
            "followUps": [
                {"label": "能否帮我读取瑞典汽车媒体", "question": "能否帮我读取瑞典汽车媒体 Teknikens Värld？", "intent": "why"},
                {"label": "请使用 query_country_snapshot", "question": "请使用 query_country_snapshot 查瑞典市场。", "intent": "external_search"},
                {"label": "能否搜索瑞典语关键词", "question": "能否搜索瑞典语关键词 OMODA problem？", "intent": "action"},
            ],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "voc_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "sourceType": "web",
                        "evidenceRefs": [
                            {"refId": "ev_source", "label": "VOC source count", "value": 3, "source": "external_research"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    labels = [item["label"] for item in enriched["astrbot"]["followUps"]]
    questions = " ".join(item["question"] for item in enriched["astrbot"]["followUps"])
    assert labels == ["解释吐槽触发场景", "验证真实用户来源", "聚类痛点到配置动作", "生成 VOC 验证表"]
    assert "query_country_snapshot" not in questions
    assert "请使用" not in questions
    assert "产生吐槽" in questions
    assert "购买阶段排序" in questions


def test_enrich_business_record_refreshes_truncated_configuration_followups() -> None:
    record = {
        "comparisonId": "cmp_truncated_config_followups",
        "validationType": "business",
        "questionId": "biz-config-001",
        "category": "configuration",
        "country": "Sweden",
        "question": "A0 SUV BEV 为什么需要 80kWh 电池？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["compare_vehicle_variants", "query_country_snapshot"],
        "expectedFollowUpTypes": ["compare", "why", "action"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "直接结论：80kWh 需要用北欧场景和竞品配置验证。",
            "actualTools": ["compare_vehicle_variants", "query_country_snapshot"],
            "followUps": [
                {"label": "瑞典A0 SUV BEV细分市场中，80kWh版本与60...", "question": "对比 80kWh 与 60kWh。", "intent": "compare"},
                {"label": "沃尔沃EX30（1,518辆）和EX40（2,945辆）...", "question": "给出配置动作。", "intent": "action"},
                {"label": "瑞典冬季续航实测数据：80kWh vs 60kWh电池的...", "question": "查冬季续航实测。", "intent": "data_check"},
            ],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "configuration_analysis",
                "country": "Sweden",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "compare_vehicle_variants",
                        "sourceType": "engineering",
                        "evidenceRefs": [
                            {"refId": "ev_config", "label": "configuration rows", "value": 2, "source": "jato_variant_diff_service"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)

    labels = [item["label"] for item in enriched["astrbot"]["followUps"]]
    questions = " ".join(item["question"] for item in enriched["astrbot"]["followUps"])
    assert labels[:3] == ["做配置价值矩阵", "解释北欧需求原因", "拆版本策略"]
    assert "..." not in " ".join(labels)
    assert "80kWh 电池" in questions
    assert "冬季续航、安全冗余、公司车使用" in questions


def test_enrich_business_record_downgrades_report_without_multi_model_coverage() -> None:
    record = {
        "comparisonId": "cmp_report_single_model_only",
        "validationType": "business",
        "questionId": "biz-report-002",
        "category": "report_generation",
        "country": "Sweden",
        "question": "生成 O5 BEV 对标 EX30 和 EV3 的一页竞品汇报框架。",
        "expectedIntent": "report_generation",
        "expectedTools": ["compare_competitive_set", "compare_vehicle_variants"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": (
                "直接结论：O5 BEV 对标 EX30 / EV3 的一页竞品汇报应给 positioning stance。\n\n"
                "## 关键证据\n"
                "- EX30.sales = 1,518 units（jato_cross_reference）"
            ),
            "evidenceDigest": [
                "EX30.sales = 1,518 units（jato_cross_reference）",
            ],
            "limitations": [
                "O5 BEV尚未在瑞典上市，无实际销量、定价和配置数据，所有推测基于品牌定位。",
                "EV3未进入top8模型榜单，具体销量数据需进一步查询。",
                "当前数据截至2025年3月，后续需更新上市后实际表现。",
            ],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "report_generation",
                "confidence": "high",
                "entities": {
                    "models": ["O5 BEV", "EX30", "EV3"],
                    "competitors": ["EX30", "EV3"],
                },
                "toolResults": [
                    {
                        "toolName": "compare_competitive_set",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "source": "jato_cross_reference"},
                            {"refId": "ev_xc60_sales", "label": "XC60.sales", "value": 2893, "source": "jato_cross_reference"},
                            {"refId": "ev_market_sales", "label": "cumulativeSales", "value": 1182452, "source": "jato_country_snapshot"},
                            {"refId": "ev_market_msrp", "label": "avgMsrp", "value": 57954, "source": "jato_country_snapshot"},
                        ],
                    },
                ],
                "missingEvidence": [],
            },
            "visualArtifacts": [
                {"id": "artifact_report_model_coverage_table", "type": "table", "title": "Competitor report coverage matrix"},
                {
                    "id": "artifact_report_generation_table",
                    "type": "table",
                    "title": "Report evidence appendix",
                    "data": {"rows": [{"section": "Market evidence", "evidence": "XC60.sales: 2893"}]},
                },
                {
                    "id": "artifact_metric_cards",
                    "type": "metric_cards",
                    "title": "Key metrics",
                    "data": {"rows": [{"label": "cumulativeSales", "value": 1182452}]},
                },
            ],
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]

    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert enriched["astrbot"]["confidence"] == "medium"
    assert enriched["astrbot"]["evidencePackage"]["confidence"] == "medium"
    assert enriched["astrbot"]["evidenceDigest"][:2] == [
        "竞品汇报覆盖 = O5 BEV / EX30 / EV3 待补完整 MSRP、配置/电池/续航和来源日期",
        "缺口 = 单个竞品销量或市场背景不能支撑完整对标页结论",
    ]
    assert "EX30 销量 = 1,518（JATO 交叉引用）" in enriched["astrbot"]["evidenceDigest"]
    assert "competitive_or_configuration_data_unavailable" in missing_names
    assert "EX30 销量" in enriched["astrbotAnswer"]
    assert "EX30.sales" not in enriched["astrbotAnswer"]
    assert "当前缺少多车型对标汇报所需的实体证据覆盖" in enriched["astrbotAnswer"]
    assert "当前缺少目标车型在瑞典的可引用上市状态、销量、官方 MSRP 和配置/版本证据" in enriched["astrbotAnswer"]
    assert "不能用 top model 榜单缺失判断其需求强弱" in enriched["astrbotAnswer"]
    assert "不写确定数据截止月份" in enriched["astrbotAnswer"]
    assert "O5 BEV尚未在瑞典上市" not in enriched["astrbotAnswer"]
    assert "EV3未进入top8模型榜单" not in enriched["astrbotAnswer"]
    assert "当前数据截至2025年3月" not in enriched["astrbotAnswer"]
    assert enriched["astrbot"]["qualityScore"]["totalScore"] <= 0.88
    artifact_ids = [item["id"] for item in enriched["astrbot"]["visualArtifacts"]]
    assert artifact_ids[:3] == [
        "artifact_report_model_coverage_chart",
        "artifact_report_model_coverage_table",
        "artifact_report_generation_table",
    ]
    coverage_chart = next(
        item for item in enriched["astrbot"]["visualArtifacts"]
        if item["id"] == "artifact_report_model_coverage_chart"
    )
    assert coverage_chart["title"] == "Competitor report evidence coverage"
    assert [row["label"] for row in coverage_chart["data"]] == ["O5 BEV", "EX30", "EV3"]
    assert [row["value"] for row in coverage_chart["data"]] == [0, 1, 0]
    assert "artifact_report_model_coverage_table" in artifact_ids
    coverage_table = next(
        item for item in enriched["astrbot"]["visualArtifacts"]
        if item["id"] == "artifact_report_model_coverage_table"
    )
    coverage_rows = coverage_table["data"]["rows"]
    assert coverage_rows[0]["model"] == "O5 BEV"
    assert coverage_rows[0]["coverageStatus"] == "待补"
    assert any(row["model"] == "EX30" and row["coverageStatus"] == "部分覆盖" for row in coverage_rows)
    assert any(row["model"] == "EV3" and row["coverageStatus"] == "待补" for row in coverage_rows)
    appendix = next(
        item for item in enriched["astrbot"]["visualArtifacts"]
        if item["id"] == "artifact_report_generation_table"
    )
    assert "XC60" not in str(appendix["data"]["rows"])
    assert "cumulativeSales" not in str(enriched["astrbot"]["visualArtifacts"])
    assert "avgMsrp" not in str(enriched["astrbot"]["visualArtifacts"])

    queue = eval_service._build_evidence_repair_queue([enriched])

    assert queue[0]["primaryGap"] == "competitive_or_configuration_data_unavailable"
    assert queue[0]["repairTasks"][0]["taskType"] == "competitive_config_matrix"


def test_enrich_business_record_downgrades_leasing_question_without_tco_refs() -> None:
    record = {
        "comparisonId": "cmp_leasing_price_only",
        "validationType": "business",
        "questionId": "biz-policy-005",
        "category": "policy_news",
        "country": "Sweden",
        "question": "大客户 leasing 场景下，PHEV 还有没有理由？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "policy_tax_lookup"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": (
                "直接结论：PHEV 仍可能有理由。\n\n"
                "## 关键证据\n"
                "- TAYRON.msrp = 39,121.7（jato_msrp_postgres）"
            ),
            "evidenceDigest": ["TAYRON.msrp = 39,121.7（jato_msrp_postgres）"],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "pricing_analysis",
                "confidence": "high",
                "entities": {"models": [], "competitors": []},
                "toolResults": [
                    {
                        "toolName": "query_msrp_pricing",
                        "sourceType": "postgres",
                        "evidenceRefs": [
                            {"refId": "ev_tayron_msrp", "label": "TAYRON.msrp", "value": 39121.7, "source": "jato_msrp_postgres"},
                        ],
                    }
                ],
                "missingEvidence": [],
            },
            "visualArtifacts": [
                {"id": "artifact_pricing_corridor_chart", "type": "chart", "title": "Pricing corridor chart"},
                {
                    "id": "artifact_pricing_analysis_table",
                    "type": "table",
                    "title": "Pricing evidence table",
                    "data": {"rows": [{"model": "TAYRON", "msrp": "39,121.7 EUR"}]},
                },
                {
                    "id": "artifact_metric_cards",
                    "type": "metric_cards",
                    "title": "Key metrics",
                    "data": {"rows": [{"label": "TAYRON.msrp", "value": 39121.7}]},
                },
                {"id": "artifact_tco_validation_table", "type": "table", "title": "TCO / company-car validation table"},
            ],
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]

    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert enriched["astrbot"]["confidence"] == "medium"
    assert enriched["astrbot"]["evidencePackage"]["confidence"] == "medium"
    assert "leasing_tco_or_company_car_evidence" in missing_names
    assert enriched["astrbot"]["evidenceDigest"] == [
        "leasing/TCO/company-car 证据 = 待补月供、残值、税务 benefit 或大客户口径"
    ]
    assert "当前缺少 leasing/TCO/残值或 company-car benefit 证据" in enriched["astrbotAnswer"]
    assert "TAYRON.msrp" not in enriched["astrbotAnswer"]
    assert "补齐本车型与竞品 MSRP" not in enriched["astrbotAnswer"]
    assert "生成价格矩阵" not in enriched["astrbotAnswer"]
    assert "建立 PHEV fleet leasing TCO 表" in enriched["astrbotAnswer"]
    assert "定义 PHEV 保留主推资格" in enriched["astrbotAnswer"]
    assert "定价逻辑应先验证" not in enriched["astrbotAnswer"]
    assert "泛 MSRP、竞品走廊或普通定价模板" in enriched["astrbotAnswer"]
    assert "TCO/company-car 验证表" in enriched["astrbotAnswer"]
    direct = enriched["astrbotAnswer"].split("\n\n", 1)[0]
    assert "leasing/TCO/月供、残值" in direct
    assert "定价逻辑" not in direct
    assert "竞品走廊" not in direct
    assert "配置估值" not in direct
    assert enriched["astrbot"]["qualityScore"]["totalScore"] <= 0.88
    artifact_ids = [item["id"] for item in enriched["astrbot"]["visualArtifacts"]]
    assert artifact_ids[:2] == ["artifact_external_source_repair_table", "artifact_tco_validation_table"]
    assert "artifact_pricing_corridor_chart" not in artifact_ids
    assert "artifact_pricing_analysis_table" not in artifact_ids
    assert "artifact_pricing_analysis_framework_table" not in artifact_ids
    assert "artifact_metric_cards" not in artifact_ids
    assert "TAYRON" not in str(enriched["astrbot"]["visualArtifacts"])

    queue = eval_service._build_evidence_repair_queue([enriched])

    assert queue[0]["primaryGap"] == "leasing_tco_or_company_car_evidence"
    assert queue[0]["sourceCandidateCount"] >= 4
    assert "leasing/TCO/company-car 来源候选待验证" in queue[0]["sourceSummary"]
    assert queue[0]["repairTasks"][0]["taskType"] == "leasing_tco_evidence"
    assert "月供、残值/RV、税务 benefit" in queue[0]["repairTasks"][0]["commandHint"]


def test_enrich_business_record_does_not_treat_generic_leasing_directory_as_tco_evidence() -> None:
    record = {
        "comparisonId": "cmp_leasing_directory_only",
        "validationType": "business",
        "questionId": "biz-policy-005",
        "category": "policy_news",
        "country": "Sweden",
        "question": "大客户 leasing 场景下，PHEV 还有没有理由？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "external_research", "search_market_news", "query_country_snapshot", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "直接结论：PHEV leasing 有外部来源。",
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "pricing_analysis",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "sourceType": "web",
                        "evidenceRefs": [
                            {
                                "refId": "leasing_directory_source",
                                "label": "Top 19 Car Leasing Companies in Sweden (2026) | ensun.source",
                                "value": "https://ensun.io/search/car-leasing/sweden",
                                "source": "https://ensun.io/search/car-leasing/sweden",
                            },
                            {
                                "refId": "leasing_directory_rank",
                                "label": "Top 19 Car Leasing Companies in Sweden (2026) | ensun.rankSeed",
                                "value": 0,
                                "source": "jato_external_research_web",
                            },
                        ],
                    }
                ],
                "missingEvidence": [{"name": "minimum_external_sources", "reason": "Only one weak external source.", "impact": "weakens_answer"}],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]
    source_candidates = enriched["astrbot"]["sourceRepairCandidates"]
    artifact_ids = [item["id"] for item in enriched["astrbot"]["visualArtifacts"]]

    assert "leasing_tco_or_company_car_evidence" in missing_names
    assert "leasing/TCO/company-car 证据 = 待补月供、残值、税务 benefit 或大客户口径" in enriched["astrbot"]["evidenceDigest"]
    assert "ensun" not in enriched["astrbotAnswer"].lower()
    assert enriched["astrbot"]["qualityScore"]["totalScore"] <= 0.88
    assert source_candidates["dataStatus"] == "leasing_tco_source_candidates"
    assert source_candidates["candidateCount"] >= 4
    assert "skatteverket" in str(source_candidates).lower()
    assert artifact_ids[:2] == ["artifact_external_source_repair_table", "artifact_tco_validation_table"]
    external_table = enriched["astrbot"]["visualArtifacts"][0]
    external_rows = external_table["data"]["rows"]
    assert external_rows[0]["sourceNeed"] == "Leasing/TCO/company-car source"
    assert "monthly payment/RV/tax formula" in external_rows[0]["requiredFields"]
    assert "VOC" not in str(external_rows)
    assert "forum" not in str(external_rows).lower()


def test_enrich_business_record_prioritizes_phev_channel_refs_for_leasing_digest() -> None:
    record = {
        "comparisonId": "cmp_leasing_channel_context",
        "validationType": "business",
        "questionId": "biz-policy-005",
        "category": "policy_news",
        "country": "Sweden",
        "question": "大客户 leasing 场景下，PHEV 还有没有理由？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "search_market_news", "query_country_snapshot", "build_market_chart"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "直接结论：PHEV leasing 需要 TCO 和渠道证据。",
            "evidencePackage": {
                "intent": "pricing_analysis",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "query_msrp_pricing",
                        "sourceType": "postgres",
                        "evidenceRefs": [
                            {"refId": "ev_enyaq", "label": "ENYAQ.msrp", "value": 52130.4, "source": "jato_msrp_postgres"},
                            {"refId": "ev_tayron", "label": "TAYRON.msrp", "value": 39121.7, "source": "jato_msrp_postgres"},
                        ],
                    },
                    {
                        "toolName": "build_market_chart",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {"refId": "ev_suva_sales", "label": "contextSnapshot.crossTabs.registrationBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "ev_suva_business", "label": "contextSnapshot.crossTabs.registrationBySegment.SUV A.Business_pct", "value": 60.3, "unit": "%", "source": "jato_country_chart_deck"},
                            {"refId": "ev_phev_sales", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.sales", "value": 6498, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "ev_phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                            {"refId": "ev_phev_private", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Private_pct", "value": 35.2, "unit": "%", "source": "jato_country_chart_deck"},
                            {"refId": "ev_bev_sales", "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.sales", "value": 10875, "unit": "units", "source": "jato_country_chart_deck"},
                            {"refId": "ev_bev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Business_pct", "value": 60.3, "unit": "%", "source": "jato_country_chart_deck"},
                        ],
                    },
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    digest = enriched["astrbot"]["evidenceDigest"]
    answer = enriched["astrbotAnswer"]

    assert digest[:4] == [
        "leasing/TCO/company-car 证据 = 待补月供、残值、税务 benefit 或大客户口径",
        "PHEV 公司车注册占比 = 64.8 %（JATO 图表数据）",
        "PHEV 注册量 = 6,498 units（JATO 图表数据）",
        "PHEV 私人注册占比 = 35.2 %（JATO 图表数据）",
    ]
    assert "PHEV 注册量 = 6,498 units（JATO 图表数据）" in answer
    assert "PHEV 公司车注册占比 = 64.8 %（JATO 图表数据）" in answer
    assert "PHEV 私人注册占比 = 35.2 %（JATO 图表数据）" in answer
    direct = answer.split("\n\n", 1)[0]
    assert "PHEV 已有公司车暴露信号" in direct
    assert "公司车注册占比 64.8 %" in direct
    assert "PHEV 注册量 6,498 units" in direct
    assert "不能直接证明 PHEV 应主推" in direct
    assert "TAYRON.msrp" not in answer
    assert "ENYAQ.msrp" not in answer
    assert "SUV A 注册量" not in answer


def test_enrich_business_record_downgrades_company_car_benefit_without_benefit_refs() -> None:
    record = {
        "comparisonId": "cmp_company_car_bonus_only",
        "validationType": "business",
        "questionId": "biz-policy-002",
        "category": "policy_news",
        "country": "Sweden",
        "question": "瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["external_research"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": (
                "政策边界：company car benefit 应拆 benefit tax、月供、残值和充电条件。\n\n"
                "## 关键证据\n"
                "- Bonus policy date = 2026-04-17（transportstyrelsen.se）"
            ),
            "evidenceDigest": ["Bonus policy date = 2026-04-17（transportstyrelsen.se）"],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "news_policy_search",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "sourceType": "web",
                        "evidenceRefs": [
                            {
                                "refId": "ev_bonus_date",
                                "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.date",
                                "value": "2026-04-17",
                                "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                            },
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]

    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert enriched["astrbot"]["confidence"] == "medium"
    assert "leasing_tco_or_company_car_evidence" in missing_names
    assert "leasing/TCO/company-car 证据 = 待补月供、残值、税务 benefit 或大客户口径" in enriched["astrbot"]["evidenceDigest"]
    assert enriched["astrbot"]["sourceRepairCandidates"]["dataStatus"] == "leasing_tco_source_candidates"
    assert enriched["astrbot"]["sourceRepairCandidates"]["candidateCount"] >= 4
    assert "当前缺少 leasing/TCO/残值或 company-car benefit 证据" in enriched["astrbotAnswer"]
    assert "不能证明 BEV/PHEV company car benefit 差异" in enriched["astrbotAnswer"]
    assert "大客户 leasing 场景下 PHEV" not in enriched["astrbotAnswer"]
    assert "Bonus - for low emission vehicles has ended - Transportstyrelsen.date" in enriched["astrbotAnswer"]

    queue = eval_service._build_evidence_repair_queue([enriched])
    assert queue[0]["primaryGap"] == "leasing_tco_or_company_car_evidence"
    assert "leasing/TCO/company-car 来源候选" in queue[0]["repairAction"]
    assert "company-car/TCO 结论" in queue[0]["repairAction"]
    assert "PHEV 大客户 TCO" not in queue[0]["repairAction"]


def test_enrich_business_record_labels_policy_context_refs_for_review() -> None:
    record = {
        "comparisonId": "cmp_company_car_context_refs",
        "validationType": "business",
        "questionId": "biz-policy-002",
        "category": "policy_news",
        "country": "Sweden",
        "question": "瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["external_research"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": "政策边界：company car benefit 应拆 benefit tax、月供、残值和充电条件。",
            "evidenceDigest": ["旧摘要"],
            "evidencePackage": {
                "intent": "news_policy_search",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "query_country_snapshot",
                        "sourceType": "jato_parquet",
                        "evidenceRefs": [
                            {
                                "refId": "ev_phev_sales",
                                "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.sales",
                                "value": 6498,
                                "unit": "units",
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
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    digest_text = " ".join(enriched["astrbot"]["evidenceDigest"])
    direct = enriched["astrbotAnswer"].split("\n\n", 1)[0]

    assert "PHEV 公司车注册占比 = 64.8 %" in digest_text
    assert "PHEV 注册量 = 6,498 units" in digest_text
    assert "contextSnapshot.crossTabs" not in digest_text
    assert "company car benefit 对 BEV/PHEV 的差异" in direct
    assert "PHEV 已有公司车暴露信号" not in direct
    assert "大客户 leasing 场景" not in direct


def test_policy_digest_keeps_bev_context_for_bev_subsidy_questions() -> None:
    evidence_package = {
        "intent": "news_policy_search",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "external_research",
                "sourceType": "web",
                "evidenceRefs": [
                    {
                        "refId": "bonus_date",
                        "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.date",
                        "value": "2026-04-17",
                        "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                    },
                    {
                        "refId": "bonus_rank",
                        "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.rank",
                        "value": "1",
                        "source": "jato_external_research_web",
                    },
                ],
            },
            {
                "toolName": "build_market_chart",
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {
                        "refId": "bev_sales",
                        "label": "contextSnapshot.powertrainMix.BEV.sales",
                        "value": 25235,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "phev_sales",
                        "label": "contextSnapshot.powertrainMix.PHEV.sales",
                        "value": 15028,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "mhev_sales",
                        "label": "contextSnapshot.powertrainMix.MHEV.sales",
                        "value": 8515,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "hev_sales",
                        "label": "contextSnapshot.powertrainMix.HEV.sales",
                        "value": 5051,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                ],
            },
            {
                "toolName": "query_msrp_pricing",
                "sourceType": "postgres",
                "evidenceRefs": [
                    {
                        "refId": "tayron_msrp",
                        "label": "TAYRON.msrp",
                        "value": 39121.74,
                        "unit": "currency",
                        "source": "jato_msrp_postgres",
                    },
                ],
            },
        ],
        "missingEvidence": [],
    }

    digest = eval_service._side_by_side_evidence_digest_from_package(
        evidence_package,
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
    )
    digest_text = " ".join(digest)

    assert "BEV 动力销量 = 25,235 units" in digest_text
    assert "Bonus - for low emission vehicles has ended - Transportstyrelsen.date = 2026-04-17" in digest_text
    assert "PHEV" not in digest_text
    assert "MHEV" not in digest_text
    assert "HEV 动力销量" not in digest_text
    assert "TAYRON" not in digest_text
    assert "rank" not in digest_text


def test_enrich_business_record_downgrades_named_policy_without_named_source() -> None:
    record = {
        "comparisonId": "cmp_elbilspremien_generic_policy_only",
        "validationType": "business",
        "questionId": "biz-policy-001",
        "category": "policy_news",
        "country": "Sweden",
        "question": "Elbilspremien 2026 会影响哪些车型？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["external_research"],
        "humanScoring": {"status": "pending"},
        "astrbot": {
            "answerStatus": "answered",
            "confidence": "high",
            "answerPreview": (
                "政策边界：Elbilspremien 2026 不能先点名确定受益车型。\n\n"
                "## 关键证据\n"
                "- Bonus - for low emission vehicles has ended - Transportstyrelsen（transportstyrelsen.se，2026-04-17）"
            ),
            "evidenceDigest": [
                "Bonus - for low emission vehicles has ended - Transportstyrelsen（transportstyrelsen.se，2026-04-17）"
            ],
            "qualityScore": {
                "intentScore": 1,
                "toolScore": 1,
                "groundingScore": 1,
                "followUpScore": 1,
                "safetyScore": 1,
                "businessCompletenessScore": 1,
                "totalScore": 1,
                "failures": [],
            },
            "evidencePackage": {
                "intent": "news_policy_search",
                "confidence": "high",
                "toolResults": [
                    {
                        "toolName": "external_research",
                        "sourceType": "web",
                        "evidenceRefs": [
                            {
                                "refId": "ev_bonus",
                                "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.date",
                                "value": "2026-04-17",
                                "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                            },
                            {
                                "refId": "ev_malus",
                                "label": "Malus - for high emission vehicles - Transportstyrelsen.date",
                                "value": "2026-02-25",
                                "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/malus/",
                            },
                            {
                                "refId": "bev_sales",
                                "label": "contextSnapshot.powertrainMix.BEV.sales",
                                "value": 25235,
                                "unit": "units",
                                "source": "jato_country_chart_deck",
                            },
                            {
                                "refId": "phev_sales",
                                "label": "contextSnapshot.powertrainMix.PHEV.sales",
                                "value": 15028,
                                "unit": "units",
                                "source": "jato_country_chart_deck",
                            },
                            {
                                "refId": "mhev_sales",
                                "label": "contextSnapshot.powertrainMix.MHEV.sales",
                                "value": 8515,
                                "unit": "units",
                                "source": "jato_country_chart_deck",
                            },
                            {
                                "refId": "hev_sales",
                                "label": "contextSnapshot.powertrainMix.HEV.sales",
                                "value": 5051,
                                "unit": "units",
                                "source": "jato_country_chart_deck",
                            },
                        ],
                    }
                ],
                "missingEvidence": [],
            },
        },
        "countryCopilot": {"answerPreview": "Copilot answer"},
    }

    enriched = eval_service._enrich_business_record_for_read(record)
    missing_names = [item["name"] for item in enriched["astrbot"]["missingEvidence"]]

    assert enriched["astrbot"]["answerStatus"] == "partially_answered"
    assert enriched["astrbot"]["confidence"] == "medium"
    assert enriched["astrbot"]["evidencePackage"]["confidence"] == "medium"
    assert "specific_policy_source_evidence" in missing_names
    digest_text = " ".join(enriched["astrbot"]["evidenceDigest"])
    assert "点名政策来源 = 待补 Elbilspremien 2026 官方或可引用来源" in digest_text
    assert "BEV 动力销量 = 25,235 units" in digest_text
    assert "PHEV" not in digest_text
    assert "MHEV" not in digest_text
    assert "HEV 动力销量" not in digest_text
    assert "当前缺少问题中点名政策的官方或可引用来源" in enriched["astrbotAnswer"]
    assert "bonus/malus、vehicle tax 或市场背景只能作为交叉验证" in enriched["astrbotAnswer"]
    assert "Bonus - for low emission vehicles" not in enriched["astrbotAnswer"]
    assert enriched["astrbot"]["qualityScore"]["totalScore"] <= 0.88
    candidates = enriched["astrbot"]["sourceRepairCandidates"]
    assert candidates["dataStatus"] == "external_policy_source_candidates"
    assert candidates["candidateCount"] == 3
    source_queries = [item["sourceSearchQuery"] for item in candidates["sourceSearchPlan"]]
    assert "site:regeringen.se elbilspremien 2026 elbilspremie elbilspremien" in source_queries
    assert "site:transportstyrelsen.se elbilspremien 2026 elbilspremie elbilspremien" in source_queries
    assert "site:skatteverket.se elbilspremien 2026 elbilspremie elbilspremien" in source_queries

    queue = eval_service._build_evidence_repair_queue([enriched])

    assert queue[0]["primaryGap"] == "specific_policy_source_evidence"
    assert queue[0]["repairTasks"][0]["taskType"] == "specific_policy_source_evidence"
    assert queue[0]["sourceSearchPlan"][0]["candidateDomain"] == "regeringen.se"
    assert queue[0]["sourceSearchPlan"][0]["sourceSearchQuery"].startswith("site:regeringen.se")
    assert "建议先查" in queue[0]["repairTasks"][0]["commandHint"]
    assert "site:regeringen.se" in queue[0]["repairTasks"][0]["commandHint"]

    backlog = eval_service._build_source_repair_backlog(queue)

    assert backlog[0]["sourceType"] == "policy_news_source"
    assert backlog[0]["primaryGaps"] == ["specific_policy_source_evidence"]
    assert backlog[0]["sourceSearchQuery"].startswith("site:regeringen.se")


def test_business_validation_report_filters_j7_source_repair_candidates(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = {
        "comparisonId": "cmp_j7_source_repair_filter",
        "runAt": "2026-06-15T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-001",
        "category": "pricing",
        "country": "Sweden",
        "question": "瑞典 J7 HEV 应该怎么定价？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "J7 needs source repair candidates.",
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Requested model has no current price row.",
                    "impact": "weakens_answer",
                }
            ],
            "sourceRepairCandidates": {
                "dataStatus": "source_draft_only_not_price_evidence",
                "missingOwnModelSource": True,
                "materializedCandidateCount": 0,
                "ownModel": [],
                "competitorCorridor": [
                    {"sourceCode": "toyota_corolla_cross_se", "brand": "TOYOTA", "model": "COROLLA CROSS"},
                    {"sourceCode": "toyota_corolla_se", "brand": "TOYOTA", "model": "COROLLA"},
                    {"sourceCode": "toyota_rav4_se", "brand": "TOYOTA", "model": "RAV4"},
                    {"sourceCode": "polestar_4_se", "brand": "POLESTAR", "model": "4"},
                    {"sourceCode": "kia_sportage_se", "brand": "KIA", "model": "SPORTAGE"},
                    {"sourceCode": "toyota_c_hr_se", "brand": "TOYOTA", "model": "C-HR"},
                ],
                "candidateCount": 6,
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")

    report = eval_service.get_business_validation_report()
    item = report["items"][0]
    queue_item = report["evidenceRepairQueue"][0]

    item_models = [
        entry["model"]
        for entry in item["astrbot"]["sourceRepairCandidates"]["competitorCorridor"]
    ]
    queue_models = [
        entry["model"]
        for entry in queue_item["sourceRepairCandidates"]["competitorCorridor"]
    ]
    assert item_models == ["COROLLA CROSS", "RAV4", "SPORTAGE", "C-HR"]
    assert queue_models == item_models
    assert "4" not in item_models
    assert "COROLLA" not in item_models
    assert item["astrbot"]["sourceRepairCandidates"]["candidateCount"] == 4
    assert queue_item["repairSummary"]["sourceCandidateCount"] == 4


def test_policy_source_repair_filter_prefers_co2_tax_sources_over_bev_subsidy() -> None:
    candidates = {
        "dataStatus": "external_policy_source_candidates",
        "missingOwnModelSource": False,
        "materializedCandidateCount": 0,
        "ownModel": [],
        "competitorCorridor": [
            {
                "sourceCode": "policy-official-sweden-1",
                "brand": "official",
                "model": "Sweden BEV subsidy source",
                "sourceSearchQuery": "site:regeringen.se elbilspremie 2026 elbil prisgrans",
                "candidateDomain": "regeringen.se",
            },
            {
                "sourceCode": "policy-official-sweden-2",
                "brand": "official",
                "model": "Sweden vehicle-tax/bonus official source",
                "sourceSearchQuery": "site:transportstyrelsen.se elbil bonus malus 2026 prisgrans",
                "candidateDomain": "transportstyrelsen.se",
            },
            {
                "sourceCode": "policy-official-sweden-3",
                "brand": "official",
                "model": "Sweden company-car/tax official source",
                "sourceSearchQuery": "site:skatteverket.se bilforman elbil laddhybrid 2026",
                "candidateDomain": "skatteverket.se",
            },
        ],
        "candidateCount": 3,
    }

    filtered = eval_service._filter_source_repair_candidates_for_question(
        candidates,
        question="CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        country="Sweden",
    )

    source_queries = [
        entry["sourceSearchQuery"]
        for entry in filtered["competitorCorridor"]
    ]
    assert filtered["candidateCount"] == 1
    assert source_queries == ["site:skatteverket.se bilforman elbil laddhybrid 2026"]
    assert all("elbilspremie" not in query for query in source_queries)
    assert all("prisgrans" not in query for query in source_queries)
    assert filtered["sourceSearchPlan"][0]["candidateDomain"] == "skatteverket.se"


def test_policy_source_repair_action_uses_external_source_language() -> None:
    candidates = {
        "dataStatus": "external_policy_source_candidates",
        "missingOwnModelSource": False,
        "ownModel": [],
        "competitorCorridor": [
            {
                "sourceCode": "policy-official-sweden-1",
                "brand": "official",
                "model": "Sweden government policy source",
                "sourceUrl": "https://www.google.com/search?q=site%3Aregeringen.se+elbilspremie",
            },
            {
                "sourceCode": "policy-official-sweden-2",
                "brand": "official",
                "model": "Sweden vehicle-tax/bonus official source",
            },
        ],
        "candidateCount": 2,
        "materializedCandidateCount": 0,
    }
    action = eval_service._source_repair_action_text(candidates)
    summary = eval_service._repair_summary(
        missing_evidence=[
            {
                "name": "minimum_external_sources",
                "reason": "Need external sources.",
                "impact": "blocking",
            }
        ],
        source_repair_candidates=candidates,
        repair_action=action,
        failure_tags=["evidence_missing"],
    )

    assert "政策/新闻官方来源候选" in action
    assert "发布日期、适用对象和限制条件" in action
    assert "不能直接当作政策事实" in action
    assert "MSRP" not in action
    assert "政策来源候选已确认" in summary["sourceSummary"]
    assert "价格行" not in summary["sourceSummary"]


def test_business_validation_repair_tasks_do_not_escalate_p1_hardening() -> None:
    record = {
        "comparisonId": "cmp_weak_repair_1",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-003",
        "category": "pricing",
        "country": "Sweden",
        "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "可以作为验证假设，但需要当前 MSRP 交叉验证。",
            "missingEvidence": [
                {
                    "name": "current_msrp",
                    "reason": "Need current MSRP before final numeric price claim.",
                    "impact": "weakens_answer",
                }
            ],
            "sourceRepairCandidates": {
                "dataStatus": "source_draft_only_not_price_evidence",
                "missingOwnModelSource": False,
                "materializedCandidateCount": 0,
                "ownModel": [
                    {
                        "sourceCode": "kia_ev3_se_draft_scrapling",
                        "brand": "KIA",
                        "model": "EV3",
                        "relativePath": "se/20_kia_ev3_se.yaml",
                        "draftStatus": "source_draft_only_not_price_evidence",
                    },
                ],
                "competitorCorridor": [
                    {
                        "sourceCode": "volvo_ex30_se_draft_scrapling",
                        "brand": "VOLVO",
                        "model": "EX30",
                        "relativePath": "se/10_volvo_ex30_se.yaml",
                        "draftStatus": "source_draft_only_not_price_evidence",
                    },
                ],
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }

    queue = eval_service._build_evidence_repair_queue([record])

    assert len(queue) == 1
    assert queue[0]["priority"] == "P1"
    assert queue[0]["primaryGap"] == "current_msrp"
    assert queue[0]["commandHint"]
    assert {task["priority"] for task in queue[0]["repairTasks"]} == {"P1"}
    assert queue[0]["repairTasks"][0]["title"] == "Promote own-model source drafts"


def test_business_validation_repair_queue_labels_msrp_search_candidates_as_search_not_drafts() -> None:
    record = {
        "comparisonId": "cmp_msrp_search_candidates",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-003",
        "category": "pricing",
        "country": "Sweden",
        "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "需要本车型当前 MSRP 作为判断锚点。",
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Requested model has no current price row.",
                    "impact": "weakens_answer",
                }
            ],
            "evidencePackage": {
                "intent": "pricing_analysis",
                "country": "Sweden",
                "confidence": "medium",
                "toolResults": [
                    {
                        "toolName": "query_msrp_pricing",
                        "success": True,
                        "sourceType": "postgres",
                        "evidenceRefs": [],
                        "coverageDiagnostics": {
                            "diagnosis": "no_current_prices_for_requested_models",
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
            },
            "sourceRepairCandidates": {
                "dataStatus": "own_model_current_price_source_candidates",
                "missingOwnModelSource": True,
                "materializedCandidateCount": 0,
                "ownModel": [
                    {
                        "sourceCode": "msrp-source-sweden-o5-bev-1",
                        "brand": "",
                        "model": "O5 BEV",
                        "draftStatus": "candidate_search_query",
                        "candidateSourceType": "generic_official_price_search",
                        "sourceSearchQuery": "Sweden O5 BEV pris price MSRP official",
                        "sourceUrl": "https://www.google.com/search?q=Sweden+O5+BEV+official+price+MSRP",
                    },
                    {
                        "sourceCode": "msrp-source-sweden-ev3-2",
                        "brand": "KIA",
                        "model": "EV3",
                        "draftStatus": "candidate_search_query",
                        "candidateSourceType": "brand_official_search",
                        "candidateDomain": "kia.com/se",
                        "sourceSearchQuery": "site:kia.com/se Sweden KIA EV3 pris price MSRP official",
                        "sourceUrl": "https://www.google.com/search?q=Sweden+EV3+official+price+MSRP",
                    },
                ],
                "competitorCorridor": [],
                "candidateCount": 2,
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }

    queue = eval_service._build_evidence_repair_queue([record])
    action = queue[0]["repairAction"]
    task_titles = [task["title"] for task in queue[0]["repairTasks"]]
    command_text = " ".join([
        queue[0]["commandHint"],
        *[str(task.get("commandHint") or "") for task in queue[0]["repairTasks"]],
    ])

    assert "MSRP 来源验证表" in action
    assert "官方价格候选" in action
    assert "共2项" in action
    assert "候选只是补证线索" in action
    assert "site:kia.com/se" not in action
    assert "KIA EV3" not in action
    assert "EV3" in action
    assert "来源草稿" not in action
    assert "来源草稿" not in command_text
    assert "确认本车型来源 URL、版本/配置、币种和发布日期" in command_text
    assert "O5 BEV pris price MSRP official" in command_text
    assert "site:kia.com/se Sweden KIA EV3 pris price MSRP official" in command_text
    assert queue[0]["sourceRepairCandidates"]["ownModel"][1]["candidateDomain"] == "kia.com/se"
    assert queue[0]["sourceRepairCandidates"]["ownModel"][1]["sourceSearchQuery"].startswith("site:kia.com/se")
    assert queue[0]["sourceSearchPlan"][0]["sourceSearchQuery"] == "Sweden O5 BEV pris price MSRP official"
    assert queue[0]["sourceSearchPlan"][1]["candidateDomain"] == "kia.com/se"
    assert queue[0]["repairTasks"][0]["sourceSearchPlan"][1]["sourceSearchQuery"].startswith("site:kia.com/se")
    assert "查找本车型官方 MSRP 价格候选" in task_titles
    assert "Promote own-model source drafts" not in task_titles


def test_business_validation_repair_queue_decodes_google_search_query_candidates() -> None:
    record = {
        "comparisonId": "cmp_google_query_decode",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-004",
        "category": "pricing",
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "需要本车型当前 MSRP 作为目标价判断的锚点。",
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Requested model has no current price row.",
                    "impact": "weakens_answer",
                }
            ],
            "sourceRepairCandidates": {
                "dataStatus": "own_model_current_price_source_candidates",
                "missingOwnModelSource": True,
                "materializedCandidateCount": 0,
                "ownModel": [
                    {
                        "sourceCode": "msrp-source-sweden-o9-1",
                        "model": "O9",
                        "draftStatus": "candidate_search_query",
                        "sourceUrl": "https://www.google.com/search?q=Sweden+O9+pris+price+MSRP+official",
                    }
                ],
                "competitorCorridor": [],
                "candidateCount": 1,
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }

    queue = eval_service._build_evidence_repair_queue([record])

    assert queue[0]["sourceSearchPlan"][0]["sourceSearchQuery"] == "Sweden O9 pris price MSRP official"
    assert queue[0]["sourceRepairCandidates"]["ownModel"][0]["sourceSearchQuery"] == "Sweden O9 pris price MSRP official"
    assert "瑞典 O9 pris price MSRP official" in queue[0]["repairTasks"][0]["commandHint"]


def test_business_validation_report_groups_source_repair_backlog(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    records = [
        {
            "comparisonId": "cmp_source_backlog_1",
            "runAt": "2026-06-12T00:00:00.000Z",
            "validationType": "business",
            "questionId": "biz-pricing-003",
            "category": "pricing",
            "country": "Sweden",
            "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
            "astrbot": {
                "status": "ok",
                "answerStatus": "partially_answered",
                "selectedTool": "query_msrp_pricing",
                "missingEvidence": [
                    {
                        "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                        "reason": "Requested model has no current price row.",
                        "impact": "weakens_answer",
                    }
                ],
                "sourceRepairCandidates": {
                    "dataStatus": "own_model_current_price_source_candidates",
                    "missingOwnModelSource": True,
                    "materializedCandidateCount": 0,
                    "ownModel": [
                        {
                            "sourceCode": "msrp-source-sweden-o5-bev-1",
                            "model": "O5 BEV",
                            "draftStatus": "candidate_search_query",
                            "sourceSearchQuery": "Sweden O5 BEV pris price MSRP official",
                            "sourceUrl": "https://www.google.com/search?q=Sweden+O5+BEV+pris+price+MSRP+official",
                        }
                    ],
                    "competitorCorridor": [],
                },
            },
            "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
            "comparison": {"bothReturned": True, "errorCount": 0},
            "failureTags": [],
            "humanScoring": {"status": "pending"},
        },
        {
            "comparisonId": "cmp_source_backlog_2",
            "runAt": "2026-06-12T00:01:00.000Z",
            "validationType": "business",
            "questionId": "biz-report-001",
            "category": "report_generation",
            "country": "Sweden",
            "question": "生成瑞典 O5 BEV 定价汇报摘要。",
            "astrbot": {
                "status": "ok",
                "answerStatus": "partially_answered",
                "selectedTool": "query_msrp_pricing",
                "missingEvidence": [
                    {
                        "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                        "reason": "Requested model has no current price row.",
                        "impact": "weakens_answer",
                    }
                ],
                "sourceRepairCandidates": {
                    "dataStatus": "own_model_current_price_source_candidates",
                    "missingOwnModelSource": True,
                    "materializedCandidateCount": 0,
                    "ownModel": [
                        {
                            "sourceCode": "msrp-source-sweden-o5-bev-2",
                            "model": "O5 BEV",
                            "draftStatus": "candidate_search_query",
                            "sourceUrl": "https://www.google.com/search?q=Sweden+O5+BEV+pris+price+MSRP+official",
                        }
                    ],
                    "competitorCorridor": [],
                },
            },
            "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
            "comparison": {"bothReturned": True, "errorCount": 0},
            "failureTags": ["evidence_missing"],
            "humanScoring": {"status": "pending"},
        },
        {
            "comparisonId": "cmp_source_backlog_voc",
            "runAt": "2026-06-12T00:02:00.000Z",
            "validationType": "business",
            "questionId": "biz-voc-003",
            "category": "voc",
            "country": "Sweden",
            "question": "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
            "astrbot": {
                "status": "ok",
                "answerStatus": "partially_answered",
                "selectedTool": "external_research",
                "missingEvidence": [
                    {
                        "name": "external_research_claims_unavailable",
                        "reason": "No citation-ready VOC claims were returned.",
                        "impact": "weakens_answer",
                    }
                ],
                "sourceRepairCandidates": {
                    "dataStatus": "external_research_query_candidates",
                    "missingOwnModelSource": False,
                    "materializedCandidateCount": 0,
                    "ownModel": [],
                    "competitorCorridor": [
                        {
                            "sourceCode": "voc-source-sweden-1",
                            "brand": "VOC",
                            "model": "OMODA JAECOO Sweden owner review complaint forum",
                            "draftStatus": "candidate_search_query",
                            "sourceSearchQuery": "OMODA JAECOO Sweden owner review complaint forum",
                        }
                    ],
                },
            },
            "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
            "comparison": {"bothReturned": True, "errorCount": 0},
            "failureTags": ["evidence_missing"],
            "humanScoring": {"status": "pending"},
        },
    ]
    side_by_side_file.parent.mkdir(parents=True, exist_ok=True)
    side_by_side_file.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
        encoding="utf-8",
    )

    report = eval_service.get_business_validation_report()
    backlog = [
        item
        for item in report["sourceRepairBacklog"]
        if item["sourceSearchQuery"] == "Sweden O5 BEV pris price MSRP official"
    ]

    assert len(backlog) == 1
    item = backlog[0]
    assert item["priority"] == "P0"
    assert item["sourceType"] == "msrp_current_price_source"
    assert item["affectedCount"] == 2
    assert set(item["questionIds"]) == {"biz-pricing-003", "biz-report-001"}
    assert set(item["categories"]) == {"pricing", "report_generation"}
    assert item["primaryGaps"] == ["coverage_diagnostic:no_current_prices_for_requested_models"]
    assert "current_price" not in item["recommendedAction"]
    assert "当前价格记录" in item["recommendedAction"]
    assert report["sourceRepairBacklog"][0]["sourceSearchQuery"] == "Sweden O5 BEV pris price MSRP official"
    assert report["summary"]["sourceRepairBacklogCount"] == 2
    assert "## Source Repair Backlog" in report["markdown"]
    assert "Sweden O5 BEV pris price MSRP official" in report["markdown"]
    assert "biz-pricing-003, biz-report-001" in report["markdown"]


def test_source_repair_backlog_preserves_source_draft_path() -> None:
    backlog = eval_service._build_source_repair_backlog([
        {
            "questionId": "biz-compare-002",
            "category": "competitor_compare",
            "country": "Sweden",
            "priority": "P1",
            "primaryGap": "coverage_diagnostic:no_current_prices_for_requested_models",
            "failureTags": [],
            "sourceRepairCandidates": {"dataStatus": "source_draft_candidate_not_price_evidence"},
            "sourceSearchPlan": [
                {
                    "role": "competitor_corridor",
                    "label": "VOLVO EX30",
                    "brand": "VOLVO",
                    "model": "EX30",
                    "candidateSourceType": "source_draft",
                    "candidateDomain": "volvocars.com",
                    "draftStatus": "source_draft_available",
                    "sourceDraftPath": "se/05_volvo_ex30_se.yaml",
                    "sourceUrl": "https://www.volvocars.com/se/build/ex30-electric/",
                }
            ],
        }
    ])

    assert backlog[0]["sourceDraftPath"] == "se/05_volvo_ex30_se.yaml"
    assert backlog[0]["sourceUrl"] == "https://www.volvocars.com/se/build/ex30-electric/"
    assert backlog[0]["candidateSourceType"] == "source_draft"


def test_source_repair_backlog_labels_leasing_tco_sources_without_msrp_materialization() -> None:
    backlog = eval_service._build_source_repair_backlog([
        {
            "questionId": "biz-policy-005",
            "category": "policy_news",
            "country": "Sweden",
            "priority": "P0",
            "primaryGap": "leasing_tco_or_company_car_evidence",
            "failureTags": [],
            "sourceRepairCandidates": {"dataStatus": "leasing_tco_source_candidates"},
            "sourceSearchPlan": [
                {
                    "role": "competitor_corridor",
                    "label": "TCO company-car tax / benefit formula source",
                    "brand": "TCO",
                    "model": "company-car tax / benefit formula source",
                    "candidateSourceType": "leasing_tco_search",
                    "candidateDomain": "skatteverket.se",
                    "sourceSearchQuery": "site:skatteverket.se bilförmån laddhybrid Sweden 2026",
                    "sourceUrl": "https://www.google.com/search?q=site%3Askatteverket.se+bilf%C3%B6rm%C3%A5n+laddhybrid+Sweden+2026",
                }
            ],
        }
    ])

    assert backlog[0]["sourceType"] == "leasing_tco_source"
    assert backlog[0]["primaryGaps"] == ["leasing_tco_or_company_car_evidence"]
    assert "月供、残值/RV、税务 benefit" in backlog[0]["recommendedAction"]
    assert "计算口径" in backlog[0]["recommendedAction"]
    assert "不要生成当前价格记录" in backlog[0]["recommendedAction"]
    assert "官方价格来源" not in backlog[0]["recommendedAction"]


def test_business_validation_repair_queue_keeps_voc_query_candidates_out_of_price_tasks() -> None:
    record = {
        "comparisonId": "cmp_voc_query_repair",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-voc-003",
        "category": "voc",
        "country": "Sweden",
        "question": "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "external_research",
            "answerPreview": "当前缺少可追溯 VOC 来源。",
            "missingEvidence": [
                {
                    "name": "external_research_claims_unavailable",
                    "reason": "No citation-ready VOC claims were returned.",
                    "impact": "weakens_answer",
                },
                {
                    "name": "minimum_external_sources",
                    "reason": "voc_analysis requires at least 1 external source.",
                    "impact": "weakens_answer",
                },
            ],
            "sourceRepairCandidates": {
                "dataStatus": "external_research_query_candidates",
                "missingOwnModelSource": False,
                "materializedCandidateCount": 0,
                "ownModel": [],
                "competitorCorridor": [
                    {
                        "sourceCode": "voc-source-sweden-1",
                        "brand": "VOC",
                        "model": "OMODA JAECOO Sweden Sverige owner review complaint forum",
                        "draftStatus": "candidate_search_query",
                    },
                    {
                        "sourceCode": "voc-source-sweden-2",
                        "brand": "VOC",
                        "model": "OMODA JAECOO Sverige ägare recension problem forum klagomål",
                        "draftStatus": "candidate_search_query",
                    },
                ],
                "candidateCount": 2,
            },
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }

    queue = eval_service._build_evidence_repair_queue([record])
    task_types = [task["taskType"] for task in queue[0]["repairTasks"]]
    joined_text = " ".join([
        queue[0]["repairSummary"]["sourceSummary"],
        queue[0]["commandHint"],
        *[task["title"] for task in queue[0]["repairTasks"]],
        *[task["commandHint"] for task in queue[0]["repairTasks"]],
    ])

    assert "external_source_repair" in task_types
    assert task_types.count("external_source_repair") == 1
    assert "competitor_price_corridor" not in task_types
    assert "外部研究检索候选待验证" in queue[0]["repairSummary"]["sourceSummary"]
    assert "VOC/媒体/论坛来源" in joined_text
    assert "MSRP" not in joined_text
    assert "价格行" not in joined_text


def test_business_validation_repair_queue_ignores_competitor_action_for_own_price_gap() -> None:
    record = {
        "comparisonId": "cmp_own_price_action_filter",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-004",
        "category": "pricing",
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "需要本车型当前 MSRP 作为目标价判断的锚点。",
            "missingEvidence": [
                {
                    "name": "current_msrp",
                    "reason": "Need own-model current MSRP before final numeric price claim.",
                    "impact": "weakens_answer",
                }
            ],
            "recommendedActions": [
                {
                    "action": "补齐竞品 MSRP",
                    "rationale": "定价问题需要价格走廊。",
                    "priority": "P0",
                },
                {
                    "action": "生成价格矩阵",
                    "rationale": "定价问题需要价格矩阵。",
                    "priority": "P1",
                },
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }

    queue = eval_service._build_evidence_repair_queue([record])

    assert len(queue) == 1
    assert queue[0]["primaryGap"] == "current_msrp"
    assert "本车型当前官方 MSRP" in queue[0]["repairAction"]
    assert queue[0]["recommendedActions"] == []
    assert queue[0]["repairTasks"][0]["taskType"] == "own_model_msrp_source"


def test_business_validation_repair_queue_classifies_pricing_data_gap_as_matrix() -> None:
    record = {
        "comparisonId": "cmp_pricing_matrix_gap",
        "runAt": "2026-06-12T00:00:00.000Z",
        "validationType": "business",
        "questionId": "biz-pricing-002",
        "category": "pricing",
        "country": "Sweden",
        "question": "J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing"],
        "astrbot": {
            "status": "ok",
            "answerStatus": "partially_answered",
            "selectedTool": "query_msrp_pricing",
            "answerPreview": "需要本车型和竞品价格矩阵。",
            "missingEvidence": [
                {
                    "name": "pricing_data_unavailable",
                    "reason": "Need own-model and competitor pricing matrix.",
                    "impact": "weakens_answer",
                }
            ],
            "recommendedActions": [
                {
                    "action": "补齐竞品 MSRP / TP / 月供价格矩阵",
                    "rationale": "J7 HEV pricing playbook requires a comparable pricing table.",
                    "priority": "P0",
                }
            ],
        },
        "countryCopilot": {"status": "ok", "answerPreview": "CountryCopilot answer."},
        "comparison": {"bothReturned": True, "errorCount": 0},
        "failureTags": [],
        "humanScoring": {"status": "pending"},
    }

    queue = eval_service._build_evidence_repair_queue([record])

    assert len(queue) == 1
    assert queue[0]["primaryGap"] == "pricing_data_unavailable"
    assert queue[0]["repairAction"].startswith("补齐本车型与竞品")
    assert queue[0]["repairTasks"][0]["taskType"] == "pricing_matrix_evidence"
    assert "MSRP、TP、月供" in queue[0]["repairTasks"][0]["commandHint"]


def test_business_validation_partial_scores_do_not_create_total(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    record = eval_service.run_business_validation_question("biz-pricing-001")

    updated = eval_service.update_eval_side_by_side_human_score(
        record["comparisonId"],
        {
            "status": "pending",
            "dimensions": record["humanScoring"]["dimensions"],
            "astrbotScores": {"intentAccuracy": 5},
            "countryCopilotScores": {"intentAccuracy": 3},
        },
    )
    totals = updated["humanScoring"]["scoreTotals"]

    assert totals["astrbot"] == 0
    assert totals["countryCopilot"] == 0
    assert totals["astrbotCompleted"] == 1
    assert totals["countryCopilotCompleted"] == 1
    assert totals["requiredDimensions"] == 8
    assert totals["complete"] is False

    cleared = eval_service.update_eval_side_by_side_human_score(
        record["comparisonId"],
        {
            "status": "pending",
            "dimensions": record["humanScoring"]["dimensions"],
            "astrbotScores": {},
            "countryCopilotScores": {},
        },
    )
    cleared_totals = cleared["humanScoring"]["scoreTotals"]

    assert cleared_totals["astrbotCompleted"] == 0
    assert cleared_totals["countryCopilotCompleted"] == 0


def test_business_validation_total_score_shortcut_fills_all_dimensions(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    record = eval_service.run_business_validation_question("biz-pricing-001")

    updated = eval_service.update_eval_side_by_side_human_score(
        record["comparisonId"],
        {
            "status": "scored",
            "dimensions": record["humanScoring"]["dimensions"],
            "astrbotTotal": 5,
            "countryCopilotTotal": 3,
        },
    )
    totals = updated["humanScoring"]["scoreTotals"]

    assert totals["astrbot"] == 5
    assert totals["countryCopilot"] == 3
    assert totals["complete"] is True
    assert updated["humanScoring"]["winner"] == "astrbot"
    assert set(updated["humanScoring"]["astrbotScores"].values()) == {5}
    assert set(updated["humanScoring"]["countryCopilotScores"].values()) == {3}


def test_business_validation_partial_scores_cannot_be_marked_scored(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    record = eval_service.run_business_validation_question("biz-pricing-001")

    with pytest.raises(ValueError, match="requires all 8 dimensions"):
        eval_service.update_eval_side_by_side_human_score(
            record["comparisonId"],
            {
                "status": "scored",
                "winner": "astrbot",
                "dimensions": record["humanScoring"]["dimensions"],
                "astrbotScores": {"intentAccuracy": 5},
                "countryCopilotScores": {"intentAccuracy": 3},
            },
        )


def test_business_validation_llm_judge_can_auto_score(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    monkeypatch.setattr(
        eval_service,
        "judge_side_by_side_with_llm",
        lambda **_kwargs: {
            "status": "ok",
            "provider": {
                "provider": "openai",
                "model": "gpt-judge-test",
                "keySource": "OPENAI_API_KEY",
            },
            "scores": {
                "winner": "countryCopilot",
                "notes": "CountryCopilot is more complete in this fixture.",
                "failureTags": ["pm_insight_weak", "presentation_not_ready"],
                "astrbotScores": {
                    "intentAccuracy": 4,
                    "toolSelection": 3,
                    "grounding": 4,
                    "pmInsight": 3,
                    "actionability": 3,
                    "artifactQuality": 3,
                    "followUpValue": 3,
                    "presentationReadiness": 3,
                },
                "countryCopilotScores": {
                    "intentAccuracy": 4,
                    "toolSelection": 4,
                    "grounding": 4,
                    "pmInsight": 5,
                    "actionability": 5,
                    "artifactQuality": 4,
                    "followUpValue": 4,
                    "presentationReadiness": 5,
                },
            },
        },
    )

    record = eval_service.run_business_validation_question("biz-pricing-001")

    assert record["llmJudge"]["status"] == "ok"
    assert record["humanScoring"]["status"] == "scored"
    assert record["humanScoring"]["source"] == "llm_judge"
    assert record["humanScoring"]["winner"] == "countryCopilot"
    assert record["humanScoring"]["scoreTotals"]["complete"] is True
    assert record["winner"] == "countryCopilot"
    assert "pm_insight_weak" in record["failureTags"]
    assert "presentation_not_ready" in record["failureTags"]
    report = eval_service.get_business_validation_report()
    assert report["summary"]["baselineScoredCount"] == 1
    assert report["summary"]["replacementBaselineScoredCount"] == 1
    assert report["summary"]["baselineSourceCounts"] == {"llm_judge": 1}
    assert report["summary"]["replacementBaselineSourceCounts"] == {"llm_judge": 1}
    assert report["summary"]["pendingBaselineScoring"] == 0
    assert report["summary"]["pendingReplacementBaselineScoring"] == 0
    assert report["summary"]["replacementReadiness"]["sourceCounts"] == {"llm_judge": 1}
    assert report["summary"]["replacementReadiness"]["scoredCount"] == 1
    assert report["summary"]["replacementReadiness"]["businessBaselineReady"] is True


def test_business_validation_judge_existing_missing_key_does_not_mutate_records(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = _business_side_by_side_record("cmp_existing_1", "biz-pricing-001")
    eval_service._write_side_by_side_results([record])
    monkeypatch.setattr(
        eval_service,
        "judge_side_by_side_with_llm",
        lambda **_kwargs: {
            "status": "missing_key",
            "scores": {},
            "reason": "OPENAI_API_KEY is not configured",
            "provider": {"provider": "openai", "model": "gpt-judge-test"},
        },
    )

    result = eval_service.run_business_validation_judge_existing(limit=1)
    stored = eval_service._read_side_by_side_results()[0]
    report = eval_service.get_business_validation_report()

    assert result["status"] == "provider_not_ready"
    assert result["candidateCount"] == 1
    assert result["attemptedCount"] == 1
    assert result["savedCount"] == 0
    assert result["statusCounts"] == {"missing_key": 1}
    assert "llmJudge" not in stored
    assert stored["humanScoring"]["status"] == "pending"
    assert report["summary"]["replacementBaselineScoredCount"] == 0
    assert report["summary"]["pendingReplacementBaselineScoring"] == 1


def test_business_validation_judge_existing_scores_pending_records(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    record = _business_side_by_side_record("cmp_existing_1", "biz-pricing-001")
    eval_service._write_side_by_side_results([record])
    monkeypatch.setattr(
        eval_service,
        "judge_side_by_side_with_llm",
        lambda **_kwargs: _ok_business_judge_result("astrbot"),
    )

    result = eval_service.run_business_validation_judge_existing(limit=1)
    stored = eval_service._read_side_by_side_results()[0]
    report = eval_service.get_business_validation_report()

    assert result["status"] == "scored"
    assert result["savedCount"] == 1
    assert result["judgedCount"] == 1
    assert result["results"][0]["winner"] == "astrbot"
    assert stored["llmJudge"]["status"] == "ok"
    assert stored["humanScoring"]["status"] == "scored"
    assert stored["humanScoring"]["source"] == "llm_judge"
    assert stored["humanScoring"]["judgeProvider"]["model"] == "gpt-judge-test"
    assert stored["humanScoring"]["scoreTotals"]["complete"] is True
    assert stored["businessValidation"]["winner"] == "astrbot"
    assert stored["scores"]["complete"] is True
    assert report["summary"]["baselineSourceCounts"] == {"llm_judge": 1}
    assert report["summary"]["replacementBaselineSourceCounts"] == {"llm_judge": 1}
    assert report["summary"]["replacementBaselineScoredCount"] == 1
    assert report["summary"]["pendingReplacementBaselineScoring"] == 0


def test_business_validation_judge_existing_can_target_score_ready_records(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    repair_first = _business_side_by_side_record("cmp_repair_first", "biz-pricing-001")
    repair_first["astrbot"]["missingEvidence"] = [
        {
            "name": "coverage_diagnostic:no_current_prices_for_requested_models",
            "reason": "Missing current MSRP rows.",
            "impact": "weakens_answer",
        }
    ]
    older_score_ready_same_question = _business_side_by_side_record("cmp_old_score_ready", "biz-pricing-001")
    score_ready = _business_side_by_side_record("cmp_score_ready", "biz-report-002", category="report_generation")
    eval_service._write_side_by_side_results([older_score_ready_same_question, repair_first, score_ready])
    monkeypatch.setattr(
        eval_service,
        "judge_side_by_side_with_llm",
        lambda **_kwargs: _ok_business_judge_result("astrbot"),
    )

    result = eval_service.run_business_validation_judge_existing(
        limit=30,
        latest_per_question=True,
        score_ready_only=True,
    )
    stored = eval_service._read_side_by_side_results()

    assert result["scoreReadyOnly"] is True
    assert result["candidateCount"] == 1
    assert result["selectedCount"] == 1
    assert result["savedCount"] == 1
    assert result["results"][0]["comparisonId"] == "cmp_score_ready"
    assert stored[0]["humanScoring"]["status"] == "pending"
    assert stored[1]["humanScoring"]["status"] == "pending"
    assert stored[2]["humanScoring"]["status"] == "scored"
    assert stored[2]["humanScoring"]["source"] == "llm_judge"


def test_business_validation_judge_existing_respects_limit_and_latest_order(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    older = _business_side_by_side_record("cmp_existing_1", "biz-pricing-001")
    newer = _business_side_by_side_record("cmp_existing_2", "biz-pricing-002")
    eval_service._write_side_by_side_results([older, newer])
    monkeypatch.setattr(
        eval_service,
        "judge_side_by_side_with_llm",
        lambda **_kwargs: _ok_business_judge_result("countryCopilot"),
    )

    result = eval_service.run_business_validation_judge_existing(limit=1, latest_per_question=False)
    stored = eval_service._read_side_by_side_results()

    assert result["candidateCount"] == 2
    assert result["selectedCount"] == 1
    assert result["savedCount"] == 1
    assert result["results"][0]["comparisonId"] == "cmp_existing_2"
    assert stored[0]["humanScoring"]["status"] == "pending"
    assert stored[1]["humanScoring"]["status"] == "scored"
    assert stored[1]["humanScoring"]["source"] == "llm_judge"


def test_business_validation_report_calibrates_gpt_judge_against_human_override(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)
    monkeypatch.setattr(
        eval_service,
        "call_jato_mcp_tool",
        lambda _tool, args: _fake_astrbot_result(args["country"], args["question"]),
    )
    monkeypatch.setattr(
        eval_service,
        "answer_country_question",
        lambda country, question: _fake_country_result(country, question),
    )
    monkeypatch.setattr(
        eval_service,
        "judge_side_by_side_with_llm",
        lambda **_kwargs: {
            "status": "ok",
            "provider": {"provider": "openai", "model": "gpt-judge-test"},
            "scores": {
                "winner": "countryCopilot",
                "notes": "GPT judge thinks Copilot is stronger.",
                "failureTags": ["pm_insight_weak"],
                "astrbotScores": {
                    "intentAccuracy": 3,
                    "toolSelection": 3,
                    "grounding": 3,
                    "pmInsight": 3,
                    "actionability": 3,
                    "artifactQuality": 3,
                    "followUpValue": 3,
                    "presentationReadiness": 3,
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
            },
        },
    )
    record = eval_service.run_business_validation_question("biz-pricing-001")
    dimensions = record["humanScoring"]["dimensions"]

    eval_service.update_eval_side_by_side_human_score(
        record["comparisonId"],
        {
            "status": "scored",
            "winner": "astrbot",
            "notes": "Human PM prefers AstrBot after checking evidence.",
            "dimensions": dimensions,
            "astrbotScores": {key: 5 for key in dimensions},
            "countryCopilotScores": {key: 3 for key in dimensions},
            "failureTags": ["pm_insight_weak", "answer_too_generic"],
        },
    )
    report = eval_service.get_business_validation_report()
    calibration = report["summary"]["judgeCalibration"]

    assert calibration["gptJudgedCount"] == 1
    assert calibration["humanReviewedCount"] == 1
    assert calibration["mismatchCount"] == 1
    assert calibration["agreementRate"] == 0
    assert calibration["mismatchExamples"][0]["gptJudgeWinner"] == "countryCopilot"
    assert calibration["mismatchExamples"][0]["humanWinner"] == "astrbot"
    assert any(item["tag"] == "pm_insight_weak" for item in report["summary"]["recommendedNextActions"])
    assert "GPT-Human agreement rate" in report["markdown"]
    assert "Recommended Next Engineering Actions" in report["markdown"]


def test_codex_review_notes_are_listed_without_human_scoring(tmp_path, monkeypatch) -> None:
    notes_file = tmp_path / "codex_review_notes.jsonl"
    monkeypatch.setattr(eval_service, "_CODEX_REVIEW_NOTES_FILE", notes_file)
    notes_file.write_text(
        "\n".join([
            '{"questionId":"biz-pricing-001","uiStatus":"warning","suggestedWinner":"countryCopilot","source":"codex_review"}',
            '{"questionId":"ignored","uiStatus":"warning","source":"human_review"}',
        ]),
        encoding="utf-8",
    )

    result = eval_service.list_codex_review_notes()

    assert result["total"] == 1
    assert result["items"][0]["questionId"] == "biz-pricing-001"
    assert result["latestByQuestionId"]["biz-pricing-001"]["source"] == "codex_review"


def test_latest_codex_review_scoring_artifacts_are_loaded_read_only(tmp_path, monkeypatch) -> None:
    artifact_root = tmp_path / "artifacts" / "astrbot-review"
    old_run = artifact_root / "2026-06-13T01-00-00-000Z"
    latest_run = artifact_root / "2026-06-13T02-00-00-000Z"
    old_run.mkdir(parents=True)
    latest_run.mkdir(parents=True)
    (old_run / "codex_review_report.md").write_text("# old", encoding="utf-8")
    (latest_run / "codex_review_report.md").write_text("# latest", encoding="utf-8")
    (latest_run / "manual_scoring_template.tsv").write_text(
        "question_id\tastrbot_total_1_to_5\tcopilot_total_1_to_5\n"
        "biz-pricing-001\t\t\n",
        encoding="utf-8",
    )
    (latest_run / "codex_draft_scoring_sheet.tsv").write_text(
        "question_id\tastrbot_total_1_to_5\tcopilot_total_1_to_5\twinner\n"
        "biz-pricing-001\t5\t3\tastrbot\n",
        encoding="utf-8",
    )
    (latest_run / "reference_judge_packet.json").write_text(
        json.dumps({
            "source": "astrbot_reference_judge_packet",
            "records": [{"questionId": "biz-pricing-001"}],
        }),
        encoding="utf-8",
    )
    (latest_run / "reference_judge_packet.md").write_text(
        "# AstrBot Reference Judge Packet\n\nRecords: 1\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(eval_service, "_CODEX_REVIEW_ARTIFACT_DIR", artifact_root)

    result = eval_service.get_latest_codex_review_scoring_artifacts()

    assert result["available"] is True
    assert result["hasManualTemplate"] is True
    assert result["hasDraft"] is True
    assert result["hasReferenceJudgePacket"] is True
    assert result["runId"] == latest_run.name
    assert result["rowCount"] == 1
    assert "biz-pricing-001\t5\t3\tastrbot" in result["codexDraftSheetText"]
    assert result["manualTemplateText"].startswith("question_id")
    assert result["referenceJudgePacketJsonText"].startswith('{"source":')
    assert "Reference Judge Packet" in result["referenceJudgePacketMdText"]
    assert result["referenceJudgePacketJsonPath"].endswith("reference_judge_packet.json")
    assert result["referenceJudgePacketMdPath"].endswith("reference_judge_packet.md")
    assert "manualTemplatePath" in result
    assert "Codex draft" in result["warning"]
    assert "llm_judge" in result["warning"]
