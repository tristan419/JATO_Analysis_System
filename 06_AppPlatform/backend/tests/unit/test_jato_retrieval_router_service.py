from __future__ import annotations

from app.services.jato_retrieval_router_service import (
    RETRIEVAL_PATHS,
    build_retrieval_tool_plan,
    classify_retrieval_intent,
    merge_evidence_pack,
)


def test_classify_chart_trend_question_as_structured_mcp() -> None:
    result = classify_retrieval_intent("Draw a 2025 BEV market trend chart")
    assert result["primary"]["path"] == "structured_mcp"
    assert result["primary"]["confidence"] in ("high", "medium")
    assert len(result["allPaths"]) >= 1


def test_classify_pricing_question_as_structured_mcp() -> None:
    result = classify_retrieval_intent("What is the MSRP positioning for XC60?")
    assert result["primary"]["path"] == "structured_mcp"
    assert "pricing" in result["primary"]["signals"]


def test_classify_policy_news_as_hybrid_rag() -> None:
    result = classify_retrieval_intent("What policy or subsidy news could affect PHEV demand in Sweden?")
    assert "hybrid_rag" in result["allPaths"]
    assert any(d["path"] == "hybrid_rag" for d in result["decisions"])


def test_classify_voc_sentiment_as_hybrid_rag() -> None:
    result = classify_retrieval_intent("VOC complaints about range anxiety in Norway")
    assert "hybrid_rag" in result["allPaths"]


def test_classify_multi_hop_as_minirag() -> None:
    result = classify_retrieval_intent(
        "How do Sweden BEV policies indirectly affect J7 PHEV positioning against Kodiaq?"
    )
    assert "minirag" in result["allPaths"]
    assert next(d for d in result["decisions"] if d["path"] == "minirag")["status"] == "active"


def test_classify_document_reference_as_pageindex() -> None:
    result = classify_retrieval_intent("What does clause 5.2 in the policy PDF say about PHEV subsidies?")
    assert "pageindex" in result["allPaths"]
    assert next(d for d in result["decisions"] if d["path"] == "pageindex")["status"] == "active"


def test_classify_explicit_web_request_as_web_search() -> None:
    result = classify_retrieval_intent("Search the web for latest Sweden EV mandate")
    assert "web_search" in result["allPaths"]


def test_classify_explicit_url_as_web_search_with_read_tool() -> None:
    result = classify_retrieval_intent("Summarize https://example.com/report")
    plan = build_retrieval_tool_plan(result, "Sweden", "Summarize https://example.com/report")

    assert result["primary"]["path"] == "web_search"
    assert result["primary"]["confidence"] == "high"
    assert "explicit_url" in result["primary"]["signals"]
    assert plan["steps"][0]["tool"] == "read_web_page"


def test_classify_vague_question_defaults_to_structured_mcp() -> None:
    result = classify_retrieval_intent("Hello, help me understand something")
    assert result["primary"]["path"] == "structured_mcp"
    assert result["primary"]["confidence"] == "low"


def test_build_retrieval_tool_plan_returns_steps() -> None:
    classification = classify_retrieval_intent("Draw a BEV trend chart for 2025 Sweden")
    plan = build_retrieval_tool_plan(classification, "Sweden", "Draw a BEV trend chart for 2025 Sweden")
    assert plan["primaryPath"] == "structured_mcp"
    assert plan["totalSteps"] >= 1
    assert len(plan["steps"]) >= 1
    for step in plan["steps"]:
        assert "tool" in step
        assert "path" in step
        assert "reason" in step
        assert "confidence" in step


def test_merge_evidence_pack_combines_multiple_paths() -> None:
    classification = classify_retrieval_intent("Sweden BEV share and latest policy news")
    results_by_path = {
        "structured_mcp": {
            "tool": "build_market_chart",
            "metadata": {"source": "jato_country_chart_deck", "truncated": False, "limitations": []},
            "data": {"country": "Sweden", "items": [{"year": 2025}]},
        },
        "hybrid_rag": {
            "tool": "search_market_news",
            "metadata": {"source": "jato_web_search_service", "truncated": False, "limitations": ["web freshness only"]},
            "data": {"items": [{"title": "Policy update"}]},
        },
    }
    pack = merge_evidence_pack(results_by_path, classification)
    assert pack["totalPaths"] == 2
    assert pack["sourceCount"] == 2
    assert len(pack["items"]) == 2
    assert "structured_mcp" in pack["pathsContributed"]
    assert "hybrid_rag" in pack["pathsContributed"]


def test_retrieval_paths_have_required_metadata() -> None:
    for path_id, path_def in RETRIEVAL_PATHS.items():
        assert "label" in path_def
        assert "description" in path_def
        assert "suitableFor" in path_def
        assert "unsuitableFor" in path_def
        assert "primaryTools" in path_def
        assert "priority" in path_def
        assert len(path_def["primaryTools"]) >= 1
