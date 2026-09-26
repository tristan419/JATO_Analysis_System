from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_HERMES_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "hermes"
_AGENT_USAGE_FILE = _HERMES_DIR / "agent_usage.jsonl"
_EVAL_USAGE_FILE = _HERMES_DIR / "eval" / "eval_usage.jsonl"
_AUDIT_FILE = _HERMES_DIR / "answer_audit.jsonl"
_PRICING_FILE = _HERMES_DIR / "model_pricing.yaml"
_FOLLOWUP_EVENTS_FILE = _HERMES_DIR / "agent_followup_events.jsonl"
_TOOL_CALL_EVENTS_FILE = _HERMES_DIR / "agent_tool_calls.jsonl"

# Default pricing (overridden by model_pricing.yaml if available)
_DEFAULT_PRICING = {
    "deepseek-v4-flash": {
        "inputPerM": 1.0,
        "outputPerM": 2.0,
    },
}
_CURRENCY = "CNY"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_pricing() -> dict[str, Any]:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError:
        return _DEFAULT_PRICING

    if not _PRICING_FILE.exists():
        return _DEFAULT_PRICING

    try:
        with open(_PRICING_FILE, "r") as fh:
            config = yaml.safe_load(fh) or {}
    except Exception:
        return _DEFAULT_PRICING

    models = config.get("models", {})
    pricing = {}
    for model_id, model_def in models.items():
        if isinstance(model_def, dict) and "pricing" in model_def:
            p = model_def["pricing"]
            discount = p.get("discount", {})
            input_price = p.get("inputCacheMissCnyPerMillionTokens", 1.0)
            output_price = p.get("outputCnyPerMillionTokens", 2.0)
            if discount.get("active") and discount.get("rate"):
                input_price *= float(discount["rate"])
                output_price *= float(discount["rate"])
            pricing[model_id] = {
                "inputPerM": float(input_price),
                "outputPerM": float(output_price),
                "discountActive": bool(discount.get("active")),
            }
    return pricing or _DEFAULT_PRICING


def estimate_tokens(text: str, token_type: str = "input") -> int:
    """Rough token estimation: ~2.5 chars per token for CN/EN mix."""
    if not text:
        return 0
    return max(1, int(len(text) / 2.5))


def estimate_cost(input_tokens: int, output_tokens: int, model: str = "deepseek-v4-flash") -> dict[str, Any]:
    pricing = _load_pricing()
    pricing_model = _pricing_model_id(model, pricing)
    model_pricing = pricing.get(pricing_model, _DEFAULT_PRICING.get("deepseek-v4-flash", {"inputPerM": 1.0, "outputPerM": 2.0}))
    input_cost = (input_tokens / 1_000_000) * model_pricing["inputPerM"]
    output_cost = (output_tokens / 1_000_000) * model_pricing["outputPerM"]
    total = round(input_cost + output_cost, 6)

    return {
        "model": model,
        "pricingModel": pricing_model,
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "inputCostCny": round(input_cost, 6),
        "outputCostCny": round(output_cost, 6),
        "totalCostCny": total,
        "currency": _CURRENCY,
        "discountActive": model_pricing.get("discountActive", False),
    }


def track_agent_answer_run(
    *,
    country: str,
    question: str,
    selected_tool: str,
    retrieval_paths: list[str],
    tools_used: list[str],
    model_usage: dict[str, Any],
    answer_mode: str = "astrbot_agent_chat",
) -> dict[str, Any]:
    """Record model usage for a normal JATO Agent chat answer."""
    input_tokens = _safe_int(model_usage.get("promptTokens") or model_usage.get("inputTokens"))
    output_tokens = _safe_int(model_usage.get("completionTokens") or model_usage.get("outputTokens"))
    total_tokens = _safe_int(model_usage.get("totalTokens")) or input_tokens + output_tokens
    model = str(model_usage.get("model") or "deepseek-chat")
    provider = str(model_usage.get("provider") or "deepseek")
    status = str(model_usage.get("status") or "unknown")
    cost = estimate_cost(input_tokens, output_tokens, model)
    usage_id = f"agent_usage_{uuid.uuid4().hex[:10]}"
    prompt_hit = _safe_int(model_usage.get("promptCacheHitTokens"))
    prompt_miss = _safe_int(model_usage.get("promptCacheMissTokens"))

    usage_record = {
        "usageId": usage_id,
        "recordedAt": _now_iso(),
        "country": country,
        "question": question[:300],
        "provider": provider,
        "model": model,
        "pricingModel": cost["pricingModel"],
        "status": status,
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "totalTokens": total_tokens,
        "promptCacheHitTokens": prompt_hit,
        "promptCacheMissTokens": prompt_miss,
        "estimatedCostCny": cost["totalCostCny"],
        "currency": cost["currency"],
        "selectedTool": selected_tool,
        "toolsUsed": tools_used,
        "retrievalPaths": retrieval_paths,
        "finishReason": str(model_usage.get("finishReason") or ""),
        "fallbackReason": str(model_usage.get("fallbackReason") or ""),
        "estimated": bool(model_usage.get("estimated")),
    }
    _append_jsonl(_AGENT_USAGE_FILE, usage_record)

    audit_record = {
        "answerId": f"astrbot.agent.{usage_id}",
        "question": question[:300],
        "answerMode": answer_mode,
        "modelUsed": cost["pricingModel"],
        "apiModel": model,
        "promptVersion": "jato_agent_dpv4_final_v1",
        "toolsUsed": tools_used,
        "evidenceIds": [f"jato.agent.{path}" for path in retrieval_paths],
        "groundednessScore": 0.75,
        "citationCoverageScore": 0.75,
        "hallucinationRiskScore": 0.25,
        "actionabilityScore": 0.75,
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "estimatedCostCny": cost["totalCostCny"],
        "shouldCache": input_tokens > 5000,
        "shouldEnterKnowledgeBase": bool(retrieval_paths),
        "createdAt": _now_iso(),
        "model": cost["pricingModel"],
        "cacheHitInputTokens": prompt_hit,
        "cacheMissInputTokens": prompt_miss or max(0, input_tokens - prompt_hit),
        "cacheHitRatio": (
            prompt_hit / (prompt_hit + prompt_miss)
            if prompt_hit + prompt_miss > 0
            else 0.0
        ),
        "discountActive": cost["discountActive"],
        "hasCacheSplit": bool(prompt_hit or prompt_miss),
    }
    _append_jsonl(_AUDIT_FILE, audit_record)
    return usage_record


def track_followup_impression(
    *,
    session_id: str,
    country: str,
    question: str,
    follow_ups: list[dict[str, Any]],
    intent: str = "",
    answer_id: str = "",
) -> dict[str, Any]:
    record = {
        "eventId": f"followup_imp_{uuid.uuid4().hex[:10]}",
        "eventType": "followup_impression",
        "recordedAt": _now_iso(),
        "sessionId": session_id,
        "answerId": answer_id,
        "country": country,
        "question": question[:300],
        "intent": intent,
        "count": len(follow_ups),
        "followUps": [
            _followup_event_payload(item, position=index + 1)
            for index, item in enumerate(follow_ups[:4])
        ],
    }
    _append_jsonl(_FOLLOWUP_EVENTS_FILE, record)
    return record


def track_followup_click(
    *,
    session_id: str,
    country: str,
    follow_up: dict[str, Any],
    source_question: str = "",
    user_id: str = "",
) -> dict[str, Any]:
    record = {
        "eventId": f"followup_click_{uuid.uuid4().hex[:10]}",
        "eventType": "followup_click",
        "recordedAt": _now_iso(),
        "sessionId": session_id,
        "country": country,
        "userId": user_id,
        "sourceQuestion": source_question[:300],
        "followUp": _followup_event_payload(follow_up, position=_safe_int(follow_up.get("priority"))),
    }
    _append_jsonl(_FOLLOWUP_EVENTS_FILE, record)
    return record


def track_followup_next_answer(
    *,
    session_id: str,
    country: str,
    follow_up: dict[str, Any],
    next_intent: str,
    next_tools_used: list[str],
    next_answer_success: bool,
    next_answer_eval_score: float,
) -> dict[str, Any]:
    record = {
        "eventId": f"followup_next_{uuid.uuid4().hex[:10]}",
        "eventType": "followup_next_answer",
        "recordedAt": _now_iso(),
        "sessionId": session_id,
        "country": country,
        "followUp": _followup_event_payload(follow_up, position=_safe_int(follow_up.get("priority"))),
        "nextIntent": next_intent,
        "nextToolsUsed": next_tools_used,
        "nextAnswerSuccess": next_answer_success,
        "nextAnswerEvalScore": round(float(next_answer_eval_score), 3),
    }
    _append_jsonl(_FOLLOWUP_EVENTS_FILE, record)
    return record


def get_followup_quality_summary(limit: int = 500) -> dict[str, Any]:
    records = _read_jsonl(_FOLLOWUP_EVENTS_FILE)[-max(1, min(limit, 5000)):]
    impressions = 0
    clicks = 0
    next_answers = 0
    next_success = 0
    by_intent: dict[str, dict[str, Any]] = {}

    for record in records:
        event_type = str(record.get("eventType") or "")
        if event_type == "followup_impression":
            follow_ups = record.get("followUps") if isinstance(record.get("followUps"), list) else []
            impressions += len(follow_ups)
            for item in follow_ups:
                if isinstance(item, dict):
                    _metric_bucket(by_intent, str(item.get("intent") or "unknown"))["impressions"] += 1
        elif event_type == "followup_click":
            clicks += 1
            follow_up = record.get("followUp") if isinstance(record.get("followUp"), dict) else {}
            _metric_bucket(by_intent, str(follow_up.get("intent") or "unknown"))["clicks"] += 1
        elif event_type == "followup_next_answer":
            next_answers += 1
            if record.get("nextAnswerSuccess") is True:
                next_success += 1
            follow_up = record.get("followUp") if isinstance(record.get("followUp"), dict) else {}
            bucket = _metric_bucket(by_intent, str(follow_up.get("intent") or "unknown"))
            bucket["nextAnswers"] += 1
            if record.get("nextAnswerSuccess") is True:
                bucket["nextSuccess"] += 1
            bucket["scoreTotal"] += float(record.get("nextAnswerEvalScore") or 0)

    for bucket in by_intent.values():
        bucket["ctr"] = round(bucket["clicks"] / bucket["impressions"], 3) if bucket["impressions"] else 0.0
        bucket["successRate"] = round(bucket["nextSuccess"] / bucket["nextAnswers"], 3) if bucket["nextAnswers"] else 0.0
        bucket["avgNextAnswerScore"] = round(bucket["scoreTotal"] / bucket["nextAnswers"], 3) if bucket["nextAnswers"] else 0.0
        bucket.pop("scoreTotal", None)

    return {
        "eventsScanned": len(records),
        "impressions": impressions,
        "clicks": clicks,
        "nextAnswers": next_answers,
        "nextSuccess": next_success,
        "ctr": round(clicks / impressions, 3) if impressions else 0.0,
        "successRate": round(next_success / next_answers, 3) if next_answers else 0.0,
        "byIntent": by_intent,
    }


def track_tool_call_event(
    *,
    session_id: str,
    country: str,
    question: str,
    tool_name: str,
    arguments: dict[str, Any],
    latency_ms: int,
    success: bool,
    cost_estimate: dict[str, Any] | None = None,
    tenant_id: str = "local",
    workspace_id: str = "default",
    error: str = "",
) -> dict[str, Any]:
    record = {
        "eventId": f"tool_call_{uuid.uuid4().hex[:10]}",
        "eventType": "tool_call",
        "recordedAt": _now_iso(),
        "tenantId": tenant_id,
        "workspaceId": workspace_id,
        "sessionId": session_id,
        "country": country,
        "question": question[:300],
        "toolName": tool_name,
        "input": _safe_tool_arguments(arguments),
        "latencyMs": max(0, int(latency_ms)),
        "success": success,
        "costEstimate": cost_estimate or {},
        "error": error[:300],
    }
    _append_jsonl(_TOOL_CALL_EVENTS_FILE, record)
    return record


def track_eval_run(
    *,
    eval_id: str,
    question_id: str,
    category: str,
    country: str,
    question: str,
    tool_calls: list[dict[str, Any]],
    total_input_tokens: int,
    total_output_tokens: int,
    model: str = "deepseek-v4-flash",
    scores: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Record token usage for an eval run. Writes to AstrBot eval_usage.jsonl AND Hermes answer_audit.jsonl."""
    cost = estimate_cost(total_input_tokens, total_output_tokens, model)

    # ── AstrBot eval usage record ──
    usage_record = {
        "usageId": f"usage_{uuid.uuid4().hex[:10]}",
        "recordedAt": _now_iso(),
        "evalId": eval_id,
        "questionId": question_id,
        "category": category,
        "country": country,
        "question": question[:200],
        "model": model,
        "inputTokens": total_input_tokens,
        "outputTokens": total_output_tokens,
        "estimatedCostCny": cost["totalCostCny"],
        "toolCalls": [t.get("tool", "") for t in tool_calls],
        "toolCallCount": len(tool_calls),
        "scores": scores or {},
    }
    _append_jsonl(_EVAL_USAGE_FILE, usage_record)

    # ── Hermes-compatible answer_audit record ──
    audit_record = {
        "answerId": f"astrbot.{eval_id}",
        "question": question[:300],
        "answerMode": f"astrbot_eval_{category}",
        "modelUsed": model,
        "promptVersion": "astrbot_eval_v1",
        "toolsUsed": [t.get("tool", "") for t in tool_calls],
        "evidenceIds": [f"astrbot.eval.{question_id}"],
        "groundednessScore": (scores or {}).get("evidenceTraceability", 0.5),
        "citationCoverageScore": (scores or {}).get("citationCoverage", 0.5),
        "hallucinationRiskScore": 0.3,
        "actionabilityScore": (scores or {}).get("toolSelectionRelevance", 0.5),
        "inputTokens": total_input_tokens,
        "outputTokens": total_output_tokens,
        "estimatedCostCny": cost["totalCostCny"],
        "shouldCache": total_input_tokens > 5000,
        "shouldEnterKnowledgeBase": (scores or {}).get("composite", 0) > 0.5,
        "createdAt": _now_iso(),
        "model": model,
        "cacheHitInputTokens": 0,
        "cacheMissInputTokens": total_input_tokens,
        "cacheHitRatio": 0.0,
        "discountActive": cost["discountActive"],
        "hasCacheSplit": False,
    }
    _append_jsonl(_AUDIT_FILE, audit_record)

    return usage_record


def get_eval_usage_summary() -> dict[str, Any]:
    """Aggregate usage stats for the eval dashboard."""
    records = _read_jsonl(_EVAL_USAGE_FILE)
    if not records:
        return {
            "totalRuns": 0,
            "totalInputTokens": 0,
            "totalOutputTokens": 0,
            "totalCostCny": 0,
            "avgCostPerRunCny": 0,
            "byCategory": {},
            "byModel": {},
        }

    total_input = sum(r.get("inputTokens", 0) for r in records)
    total_output = sum(r.get("outputTokens", 0) for r in records)
    total_cost = sum(r.get("estimatedCostCny", 0) for r in records)

    by_category: dict[str, dict[str, Any]] = {}
    by_model: dict[str, dict[str, Any]] = {}
    for r in records:
        cat = r.get("category", "unknown")
        model = r.get("model", "unknown")
        if cat not in by_category:
            by_category[cat] = {"runs": 0, "tokens": 0, "costCny": 0}
        if model not in by_model:
            by_model[model] = {"runs": 0, "tokens": 0, "costCny": 0}
        by_category[cat]["runs"] += 1
        by_category[cat]["tokens"] += r.get("inputTokens", 0) + r.get("outputTokens", 0)
        by_category[cat]["costCny"] += r.get("estimatedCostCny", 0)
        by_model[model]["runs"] += 1
        by_model[model]["tokens"] += r.get("inputTokens", 0) + r.get("outputTokens", 0)
        by_model[model]["costCny"] += r.get("estimatedCostCny", 0)

    for cat_data in by_category.values():
        cat_data["costCny"] = round(cat_data["costCny"], 4)
    for model_data in by_model.values():
        model_data["costCny"] = round(model_data["costCny"], 4)

    return {
        "totalRuns": len(records),
        "totalInputTokens": total_input,
        "totalOutputTokens": total_output,
        "totalTokens": total_input + total_output,
        "totalCostCny": round(total_cost, 4),
        "avgCostPerRunCny": round(total_cost / len(records), 6) if records else 0,
        "currency": _CURRENCY,
        "byCategory": {k: v for k, v in sorted(by_category.items())},
        "byModel": {k: v for k, v in sorted(by_model.items())},
    }


def get_agent_usage_summary(limit: int = 20) -> dict[str, Any]:
    """Aggregate usage stats for normal JATO Agent chat answers."""
    records = _read_jsonl(_AGENT_USAGE_FILE)
    if not records:
        return _empty_usage_summary()

    total_input = sum(_safe_int(r.get("inputTokens")) for r in records)
    total_output = sum(_safe_int(r.get("outputTokens")) for r in records)
    total_cost = sum(float(r.get("estimatedCostCny") or 0) for r in records)
    by_tool: dict[str, dict[str, Any]] = {}
    by_model: dict[str, dict[str, Any]] = {}
    by_status: dict[str, int] = {}

    for record in records:
        tool = str(record.get("selectedTool") or "unknown")
        model = str(record.get("pricingModel") or record.get("model") or "unknown")
        status = str(record.get("status") or "unknown")
        by_status[status] = by_status.get(status, 0) + 1
        _add_usage_bucket(by_tool, tool, record)
        _add_usage_bucket(by_model, model, record)

    recent = list(reversed(records))[: max(1, min(limit, 100))]
    return {
        "totalRuns": len(records),
        "totalInputTokens": total_input,
        "totalOutputTokens": total_output,
        "totalTokens": total_input + total_output,
        "totalCostCny": round(total_cost, 4),
        "avgCostPerRunCny": round(total_cost / len(records), 6) if records else 0,
        "currency": _CURRENCY,
        "byTool": {key: _round_usage_bucket(value) for key, value in sorted(by_tool.items())},
        "byModel": {key: _round_usage_bucket(value) for key, value in sorted(by_model.items())},
        "byStatus": dict(sorted(by_status.items())),
        "recent": recent,
    }


def estimate_tool_call_tokens(tool_name: str, arguments: dict[str, Any], result: dict[str, Any]) -> dict[str, int]:
    """Estimate tokens for a single tool call."""
    question = str(arguments.get("question", ""))
    country = str(arguments.get("country", ""))
    input_text = f"tool:{tool_name} country:{country} question:{question}"
    input_tokens = estimate_tokens(input_text, "input")

    result_json = json.dumps(result, ensure_ascii=False, default=str)
    output_tokens = estimate_tokens(result_json, "output")

    return {"inputTokens": input_tokens, "outputTokens": output_tokens}


# ── helpers ──


def _pricing_model_id(model: str, pricing: dict[str, Any]) -> str:
    normalized = str(model or "").strip()
    if normalized in pricing:
        return normalized
    lowered = normalized.lower()
    if lowered in {"deepseek-chat", "deepseek-v3", "deepseek-v4-flash"}:
        return "deepseek-v4-flash"
    if lowered in {"deepseek-reasoner", "deepseek-v4-pro"}:
        return "deepseek-v4-pro"
    return "deepseek-v4-flash"


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _followup_event_payload(item: dict[str, Any], *, position: int) -> dict[str, Any]:
    return {
        "id": str(item.get("id") or ""),
        "label": str(item.get("label") or "")[:160],
        "question": str(item.get("question") or "")[:300],
        "intent": str(item.get("intent") or ""),
        "position": max(0, position),
        "expectedTools": [
            str(tool)
            for tool in item.get("expectedTools", [])
            if str(tool or "").strip()
        ][:4]
        if isinstance(item.get("expectedTools"), list)
        else [],
        "expectedOutput": str(item.get("expectedOutput") or ""),
        "risk": str(item.get("risk") or ""),
    }


def _metric_bucket(target: dict[str, dict[str, Any]], intent: str) -> dict[str, Any]:
    if intent not in target:
        target[intent] = {
            "impressions": 0,
            "clicks": 0,
            "nextAnswers": 0,
            "nextSuccess": 0,
            "scoreTotal": 0.0,
        }
    return target[intent]


def _safe_tool_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
    blocked_keys = {"api_key", "apikey", "key", "token", "password", "secret", "authorization"}
    safe: dict[str, Any] = {}
    for key, value in (arguments or {}).items():
        normalized = str(key).lower()
        if normalized in blocked_keys or any(blocked in normalized for blocked in blocked_keys):
            safe[str(key)] = "***"
            continue
        safe[str(key)] = value
    return safe


def _empty_usage_summary() -> dict[str, Any]:
    return {
        "totalRuns": 0,
        "totalInputTokens": 0,
        "totalOutputTokens": 0,
        "totalTokens": 0,
        "totalCostCny": 0,
        "avgCostPerRunCny": 0,
        "currency": _CURRENCY,
        "byTool": {},
        "byModel": {},
        "byStatus": {},
        "recent": [],
    }


def _add_usage_bucket(
    target: dict[str, dict[str, Any]],
    key: str,
    record: dict[str, Any],
) -> None:
    bucket = target.setdefault(key, {"runs": 0, "tokens": 0, "costCny": 0.0})
    bucket["runs"] += 1
    bucket["tokens"] += _safe_int(record.get("inputTokens")) + _safe_int(record.get("outputTokens"))
    bucket["costCny"] += float(record.get("estimatedCostCny") or 0)


def _round_usage_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    return {
        "runs": bucket["runs"],
        "tokens": bucket["tokens"],
        "costCny": round(float(bucket["costCny"]), 4),
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    results: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                results.append(json.loads(stripped))
            except json.JSONDecodeError:
                continue
    return results


def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
