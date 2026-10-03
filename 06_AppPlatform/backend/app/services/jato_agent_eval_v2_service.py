from __future__ import annotations

from collections import Counter
from typing import Any

from app.services.jato_agent_deterministic_judge_service import score_deterministic_answer
from app.services.jato_agent_planning_service import build_evidence_plan
from app.services.jato_followup_service import generate_follow_ups
from app.services.jato_intent_tool_matrix_service import get_intent_tool_rule


_CATEGORY_TARGETS = {
    "pricing_analysis": 15,
    "competitor_compare": 15,
    "market_overview": 12,
    "configuration_analysis": 12,
    "inventory_analysis": 10,
    "voc_analysis": 10,
    "news_policy_search": 10,
    "report_generation": 8,
    "multi_turn_followup": 8,
}


def _seed_questions() -> list[dict[str, Any]]:
    return [
    _question(
        "astr-v2-001",
        "market_overview",
        "Sweden",
        "瑞典 2025 BEV 市场份额和车型排名怎么看？",
        "market_overview",
        ["query_country_snapshot"],
        ["BEV", "车型", "份额"],
        ["drilldown", "compare", "action", "report"],
    ),
    _question(
        "astr-v2-002",
        "pricing_analysis",
        "Sweden",
        "瑞典 J7 HEV 应该如何定价？",
        "pricing_analysis",
        ["query_msrp_pricing"],
        ["MSRP", "competitor corridor", "pricing stance"],
        ["compare", "drilldown", "action", "report"],
        optional_tools=["compare_competitive_set", "search_market_news"],
    ),
    _question(
        "astr-v2-003",
        "competitor_compare",
        "Germany",
        "Compare BMW iX1 vs Mercedes EQB vs Audi Q4 e-tron pricing and equipment.",
        "competitor_compare",
        ["compare_competitive_set"],
        ["price", "equipment", "competitor"],
        ["drilldown", "why", "action", "report"],
        optional_tools=["query_msrp_pricing", "compare_vehicle_variants"],
    ),
    _question(
        "astr-v2-004",
        "configuration_analysis",
        "Sweden",
        "O5 BEV 和 Kia EV3 的电池、续航、ADAS 配置差异是什么？",
        "configuration_analysis",
        ["compare_vehicle_variants"],
        ["battery", "range", "ADAS"],
        ["compare", "action", "data_check", "report"],
    ),
    _question(
        "astr-v2-005",
        "news_policy_search",
        "Sweden",
        "查询最近 30 天瑞典 BEV 政策新闻，对销量有什么影响？",
        "news_policy_search",
        ["search_market_news"],
        ["policy", "news", "impact"],
        ["why", "external_search", "action", "report"],
    ),
    _question(
        "astr-v2-006",
        "voc_analysis",
        "Norway",
        "Norway Tesla Model Y 车主最近在投诉什么？",
        "voc_analysis",
        ["search_market_news"],
        ["consumer", "complaint", "Model Y"],
        ["why", "external_search", "action", "report"],
    ),
    _question(
        "astr-v2-007",
        "report_generation",
        "Sweden",
        "把瑞典 BEV 市场做成一页 PPT 汇报框架。",
        "report_generation",
        ["build_market_chart"],
        ["conclusion", "evidence", "risk"],
        ["drilldown", "compare", "data_check", "action"],
        optional_tools=["query_country_snapshot"],
    ),
    _question(
        "astr-v2-008",
        "market_overview",
        "Germany",
        "Why does VW ID.7 sell better than expected?",
        "market_overview",
        ["query_country_snapshot"],
        ["sales", "pricing", "model"],
        ["drilldown", "compare", "action", "report"],
        optional_tools=["analyze_model_performance"],
    ),
    _question(
        "astr-v2-009",
        "pricing_analysis",
        "Netherlands",
        "What is the price corridor for EX30 vs Zeekr X in Netherlands?",
        "pricing_analysis",
        ["query_msrp_pricing"],
        ["price corridor", "EX30", "Zeekr X"],
        ["compare", "drilldown", "action", "report"],
    ),
    _question(
        "astr-v2-010",
        "news_policy_search",
        "EU",
        "Summarize this EV policy page https://example.com/policy",
        "news_policy_search",
        ["read_web_page"],
        ["policy", "source", "limitation"],
        ["why", "external_search", "action", "report"],
        optional_tools=["search_market_news"],
    ),
    _question("astr-v2-011", "market_overview", "Denmark", "Draw a top 10 brand sales chart for Denmark 2025.", "market_overview", ["query_country_snapshot"], ["chart", "brand", "sales"], ["drilldown", "compare", "action", "report"], optional_tools=["build_market_chart"]),
    _question("astr-v2-012", "competitor_compare", "France", "MG4 和 Renault 5 E-Tech 在法国应该怎么对比？", "competitor_compare", ["compare_competitive_set"], ["price", "configuration", "positioning"], ["drilldown", "why", "action", "report"], optional_tools=["query_msrp_pricing", "compare_vehicle_variants"]),
    _question("astr-v2-013", "configuration_analysis", "UK", "EV3 和 EX30 的续航、充电、ADAS 哪个更有优势？", "configuration_analysis", ["compare_vehicle_variants"], ["range", "charging", "ADAS"], ["compare", "action", "data_check", "report"]),
    _question("astr-v2-014", "inventory_analysis", "Sweden", "库存和订单信号会不会影响瑞典 O5 BEV 的上市节奏？", "inventory_analysis", ["query_country_snapshot"], ["inventory", "order", "market context"], ["drilldown", "compare", "action", "report"], optional_tools=["query_with_filters"]),
    _question("astr-v2-015", "voc_analysis", "Finland", "芬兰 EV 用户对冬季续航有什么主要抱怨？", "voc_analysis", ["search_market_news"], ["winter", "range", "consumer"], ["why", "external_search", "action", "report"]),
    _question("astr-v2-016", "market_overview", "Norway", "Norway BEV share by quarter in 2025, with chart.", "market_overview", ["query_country_snapshot"], ["BEV", "quarter", "chart"], ["drilldown", "compare", "action", "report"], optional_tools=["build_market_chart"]),
    _question("astr-v2-017", "news_policy_search", "Belgium", "公司车税制下 PHEV 和 BEV 的价格策略有什么不同？", "news_policy_search", ["search_market_news"], ["tax", "PHEV", "BEV"], ["why", "external_search", "action", "report"]),
    _question("astr-v2-018", "report_generation", "Germany", "Generate a markdown report outline for affordable BEV opportunity in Germany.", "report_generation", ["build_market_chart"], ["opportunity", "evidence", "next steps"], ["drilldown", "compare", "data_check", "action"], optional_tools=["query_country_snapshot"]),
    _question("astr-v2-019", "market_overview", "Sweden", "帮我解释 BEV、PHEV、HEV 的区别，并结合瑞典市场结构。", "market_overview", ["query_country_snapshot"], ["BEV", "PHEV", "HEV"], ["drilldown", "compare", "action", "report"]),
    _question("astr-v2-020", "multi_turn_followup", "Sweden", "继续上一轮，把这个结论拆到车型和价格带。", "pricing_analysis", ["query_msrp_pricing"], ["model", "price band"], ["compare", "drilldown", "action", "report"]),
    ]


def list_golden_questions_v2() -> dict[str, Any]:
    category_counts = Counter(str(item["category"]) for item in GOLDEN_QUESTIONS_V2)
    return {"items": GOLDEN_QUESTIONS_V2, "total": len(GOLDEN_QUESTIONS_V2), "categoryCounts": dict(category_counts)}


def check_golden_question_v2(question_id: str) -> dict[str, Any]:
    question = _find_question(question_id)
    plan = build_evidence_plan(question["country"], question["question"])
    follow_ups = generate_follow_ups(
        country=question["country"],
        question=question["question"],
        tools=[item.get("toolName", "") for item in plan.get("toolPlan", []) if isinstance(item, dict)],
        evidence_plan=plan,
    )
    actual_tools = [item.get("toolName", "") for item in plan.get("toolPlan", []) if isinstance(item, dict)]
    actual_follow_types = [str(item.get("intent") or "") for item in follow_ups]
    intent_ok = plan.get("intent") == question["expectedIntent"]
    tools_ok = all(tool in actual_tools or tool in plan.get("allowedTools", []) for tool in question["mustUseTools"])
    follow_ok = all(item in actual_follow_types for item in question["expectedFollowUpTypes"][:2])
    deterministic_score = score_deterministic_answer(
        expected=question,
        predicted_intent=str(plan.get("intent") or ""),
        tools_used=actual_tools,
        answer={"direct": "Plan-only deterministic eval.", "answerStatus": "answered"},
        evidence_package=_planned_evidence_package(question, plan, actual_tools),
        follow_ups=follow_ups,
    )
    return {
        "question": question,
        "plan": plan,
        "followUps": follow_ups,
        "deterministicScore": deterministic_score,
        "scores": {
            "intent": 1 if intent_ok else 0,
            "toolPrecision": 1 if tools_ok else 0,
            "followUpTypes": 1 if follow_ok else 0,
            "composite": round((int(intent_ok) + int(tools_ok) + int(follow_ok)) / 3, 3),
        },
    }


def run_eval_v2(limit: int | None = None) -> dict[str, Any]:
    questions = GOLDEN_QUESTIONS_V2[:limit] if limit else GOLDEN_QUESTIONS_V2
    results = [check_golden_question_v2(str(item["id"])) for item in questions]
    total = len(results)
    averages = {
        "intentScore": _average(results, "intentScore"),
        "toolScore": _average(results, "toolScore"),
        "groundingScore": _average(results, "groundingScore"),
        "followUpScore": _average(results, "followUpScore"),
        "safetyScore": _average(results, "safetyScore"),
        "totalScore": _average(results, "totalScore"),
    }
    confusion = _intent_confusion(results)
    failures = [
        {
            "id": item["question"]["id"],
            "category": item["question"]["category"],
            "failures": item["deterministicScore"]["failures"],
        }
        for item in results
        if item["deterministicScore"]["failures"]
    ]
    summary = {
        "total": total,
        "averages": averages,
        "intentConfusion": confusion,
        "failureCount": len(failures),
    }
    return {
        "summary": summary,
        "items": results,
        "markdown": _markdown_report(summary, failures[:20]),
    }


def _question(
    question_id: str,
    category: str,
    country: str,
    question: str,
    expected_intent: str,
    must_use_tools: list[str],
    must_mention: list[str],
    expected_follow_up_types: list[str],
    *,
    optional_tools: list[str] | None = None,
    must_not_do: list[str] | None = None,
    answer_mode: str = "analysis",
) -> dict[str, Any]:
    return {
        "id": question_id,
        "category": category,
        "country": country,
        "question": question,
        "expectedIntent": expected_intent,
        "mustUseTools": must_use_tools,
        "optionalTools": optional_tools if optional_tools is not None else get_intent_tool_rule(expected_intent)["optionalTools"],
        "mustMention": must_mention,
        "mustNotDo": must_not_do or ["invent exact sales data", "claim current official price without evidence"],
        "expectedFollowUpTypes": expected_follow_up_types,
        "answerMode": answer_mode,
    }


def _build_golden_questions() -> list[dict[str, Any]]:
    questions = _seed_questions()
    counts = Counter(str(item["category"]) for item in questions)
    next_index = len(questions) + 1
    for category, target in _CATEGORY_TARGETS.items():
        while counts[category] < target:
            questions.append(_generated_question(next_index, category, counts[category] + 1))
            counts[category] += 1
            next_index += 1
    return questions[:100]


def _generated_question(index: int, category: str, category_index: int) -> dict[str, Any]:
    countries = ["Sweden", "Norway", "Germany", "France", "Netherlands", "Denmark", "UK", "Belgium", "Finland", "Spain"]
    models = ["J7 HEV", "O5 BEV", "EX30", "EV3", "Model Y", "RAV4", "Sportage", "MG4", "Renault 5 E-Tech", "ID.7"]
    country = countries[(index + category_index) % len(countries)]
    model = models[(index + category_index * 2) % len(models)]
    other = models[(index + category_index * 3 + 1) % len(models)]
    question_id = f"astr-v2-{index:03d}"

    if category == "pricing_analysis":
        return _question(question_id, category, country, f"{country} {model} 的 MSRP 和竞品价格走廊应该怎么判断？", "pricing_analysis", ["query_msrp_pricing"], ["MSRP", "competitor corridor", "pricing stance"], ["compare", "drilldown", "action", "report"], optional_tools=["compare_competitive_set"])
    if category == "competitor_compare":
        return _question(question_id, category, country, f"Compare {model} vs {other} on price, equipment, and positioning in {country}.", "competitor_compare", ["compare_competitive_set"], ["price", "equipment", "positioning"], ["drilldown", "why", "action", "report"], optional_tools=["query_msrp_pricing", "compare_vehicle_variants"])
    if category == "market_overview":
        return _question(question_id, category, country, f"{country} BEV / PHEV 市场结构、份额和车型排名有什么变化？", "market_overview", ["query_country_snapshot"], ["share", "ranking", "powertrain"], ["drilldown", "compare", "action", "report"], optional_tools=["build_market_chart"])
    if category == "configuration_analysis":
        return _question(question_id, category, country, f"{model} 和 {other} 的电池、续航、ADAS、版型配置差异是什么？", "configuration_analysis", ["compare_vehicle_variants"], ["battery", "range", "ADAS"], ["compare", "action", "data_check", "report"])
    if category == "inventory_analysis":
        return _question(question_id, category, country, f"{country} {model} 的库存、订单和上市节奏风险怎么判断？", "inventory_analysis", ["query_country_snapshot"], ["inventory", "order", "market context"], ["drilldown", "compare", "action", "report"], optional_tools=["query_with_filters"])
    if category == "voc_analysis":
        return _question(question_id, category, country, f"{country} {model} 用户最近主要抱怨什么，是否影响购买决策？", "voc_analysis", ["search_market_news"], ["consumer", "complaint", model], ["why", "external_search", "action", "report"])
    if category == "news_policy_search":
        return _question(question_id, category, country, f"Search latest {country} EV policy or tax news and explain impact on {model}.", "news_policy_search", ["search_market_news"], ["policy", "date", "impact"], ["why", "external_search", "action", "report"])
    if category == "report_generation":
        return _question(question_id, category, country, f"Generate a one-page market analysis report outline for {model} opportunity in {country}.", "report_generation", ["build_market_chart"], ["conclusion", "evidence", "risk"], ["drilldown", "compare", "data_check", "action"], optional_tools=["query_country_snapshot"], answer_mode="report")
    return _question(question_id, category, country, f"继续上一轮，把 {model} 的结论拆到车型、价格带和下一步动作。", "pricing_analysis", ["query_msrp_pricing"], ["model", "price band", "next action"], ["compare", "drilldown", "action", "report"])


def _planned_evidence_package(question: dict[str, Any], plan: dict[str, Any], tools: list[str]) -> dict[str, Any]:
    return {
        "evidenceId": f"planned_{question['id']}",
        "sessionId": "eval_plan_only",
        "intent": plan.get("intent"),
        "country": question.get("country"),
        "entities": plan.get("entities", {}),
        "toolResults": [
            {
                "toolName": tool,
                "success": True,
                "sourceType": "generated",
                "summary": f"Plan-only evidence placeholder for {tool}.",
                "keyFindings": [f"{tool} is planned."],
                "evidenceRefs": [{"refId": f"planned_{idx + 1}", "label": tool, "value": 1, "retrievedAt": "eval"}],
            }
            for idx, tool in enumerate(tools)
        ],
        "missingEvidence": [],
        "confidence": "medium",
    }


def _average(results: list[dict[str, Any]], key: str) -> float:
    if not results:
        return 0.0
    return round(sum(float(item["deterministicScore"].get(key, 0)) for item in results) / len(results), 3)


def _intent_confusion(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str]] = Counter()
    for item in results:
        expected = str(item["question"].get("expectedIntent") or "")
        predicted = str(item["plan"].get("intent") or "")
        counts[(expected, predicted)] += 1
    return [
        {"expected": expected, "predicted": predicted, "count": count}
        for (expected, predicted), count in sorted(counts.items(), key=lambda entry: (-entry[1], entry[0]))
    ]


def _markdown_report(summary: dict[str, Any], failures: list[dict[str, Any]]) -> str:
    averages = summary["averages"]
    lines = [
        "# AstrBot Eval v2 Report",
        "",
        f"- Total questions: {summary['total']}",
        f"- Total score: {averages['totalScore']}",
        f"- Intent score: {averages['intentScore']}",
        f"- Tool score: {averages['toolScore']}",
        f"- Grounding score: {averages['groundingScore']}",
        f"- Follow-up score: {averages['followUpScore']}",
        f"- Safety score: {averages['safetyScore']}",
        "",
        "## Intent Confusion",
    ]
    for item in summary["intentConfusion"][:20]:
        lines.append(f"- {item['expected']} -> {item['predicted']}: {item['count']}")
    lines.extend(["", "## Failures"])
    if not failures:
        lines.append("- None")
    else:
        for item in failures:
            lines.append(f"- {item['id']} ({item['category']}): {', '.join(item['failures'])}")
    return "\n".join(lines)


def _find_question(question_id: str) -> dict[str, Any]:
    for question in GOLDEN_QUESTIONS_V2:
        if question["id"] == question_id:
            return question
    raise ValueError(f"Golden question not found: {question_id}")


GOLDEN_QUESTIONS_V2: list[dict[str, Any]] = _build_golden_questions()
