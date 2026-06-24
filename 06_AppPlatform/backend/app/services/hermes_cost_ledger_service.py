"""Hermes cost ledger aggregation.

The cost ledger is read-only. It normalizes Country Copilot audit records,
AstrBot agent usage, and AstrBot eval usage into one cost stream for Hermes
reports and UI summaries.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DEFAULT_PRICING: dict[str, Any] = {
    "currency": "CNY",
    "models": {
        "deepseek-v4-flash": {
            "pricing": {
                "inputCacheHitCnyPerMillionTokens": 0.02,
                "inputCacheMissCnyPerMillionTokens": 1.0,
                "outputCnyPerMillionTokens": 2.0,
                "discount": {"active": False},
            }
        },
        "deepseek-v4-pro": {
            "pricing": {
                "inputCacheHitCnyPerMillionTokens": 0.025,
                "inputCacheMissCnyPerMillionTokens": 3.0,
                "outputCnyPerMillionTokens": 6.0,
                "discount": {
                    "active": True,
                    "validUntil": "2026-05-31T23:59:00+08:00",
                },
            }
        },
    },
    "monthlyBudget": {"totalBudgetCny": 500, "warningThresholdRatio": 0.75},
}


def resolve_repo_path(repo_root: Path, path: str | Path) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return repo_root / target


def load_pricing(repo_root: Path, path: str | Path = "hermes/model_pricing.yaml") -> dict[str, Any]:
    target = resolve_repo_path(repo_root, path)
    if not target.is_file():
        return DEFAULT_PRICING
    try:
        import yaml

        data = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    except Exception:
        return DEFAULT_PRICING
    return data if isinstance(data, dict) else DEFAULT_PRICING


def load_cost_records(
    repo_root: Path,
    *,
    pricing_path: str | Path = "hermes/model_pricing.yaml",
    answer_audit_path: str | Path = "hermes/answer_audit.jsonl",
    agent_usage_path: str | Path = "hermes/agent_usage.jsonl",
    eval_usage_path: str | Path = "hermes/eval/eval_usage.jsonl",
) -> list[dict[str, Any]]:
    pricing = load_pricing(repo_root, pricing_path)
    records_by_key: dict[str, dict[str, Any]] = {}

    for record in _read_jsonl(resolve_repo_path(repo_root, agent_usage_path)):
        audit = _agent_usage_to_audit_record(record)
        records_by_key[_canonical_cost_key(audit)] = audit

    for record in _read_jsonl(resolve_repo_path(repo_root, eval_usage_path)):
        audit = _eval_usage_to_audit_record(record)
        records_by_key[_canonical_cost_key(audit)] = audit

    for record in _read_jsonl(resolve_repo_path(repo_root, answer_audit_path)):
        audit = dict(record)
        audit.setdefault("usageSource", infer_usage_source(audit))
        records_by_key[_canonical_cost_key(audit)] = audit

    records = [
        {**audit, **calculate_record_cost(audit, pricing)}
        for audit in records_by_key.values()
    ]
    records.sort(key=lambda item: str(item.get("createdAt") or ""), reverse=True)
    return records


def calculate_record_cost(audit: dict[str, Any], pricing: dict[str, Any]) -> dict[str, Any]:
    model = _pricing_model_id(
        str(
            audit.get("pricingModel")
            or audit.get("modelUsed")
            or audit.get("model")
            or "deepseek-v4-flash"
        ),
        pricing,
    )
    model_pricing = _model_pricing(pricing, model)

    input_tokens = _safe_int(_first_present(audit, "inputTokens", "promptTokens"))
    output_tokens = _safe_int(_first_present(audit, "outputTokens", "completionTokens"))
    cache_hit = _safe_int(_first_present(audit, "cacheHitInputTokens", "promptCacheHitTokens"))
    cache_miss = _safe_int(_first_present(audit, "cacheMissInputTokens", "promptCacheMissTokens"))
    if not cache_hit and not cache_miss and input_tokens > 0:
        cache_miss = input_tokens

    provided_cost = _optional_float(_first_present(audit, "estimatedCostCny", "totalCostCny"))
    if provided_cost is None:
        hit_price = float(model_pricing.get("inputCacheHitCnyPerMillionTokens") or 0)
        miss_price = float(model_pricing.get("inputCacheMissCnyPerMillionTokens") or 0)
        output_price = float(model_pricing.get("outputCnyPerMillionTokens") or 0)
        input_cost = (cache_hit / 1_000_000) * hit_price + (cache_miss / 1_000_000) * miss_price
        output_cost = (output_tokens / 1_000_000) * output_price
        estimated_cost = input_cost + output_cost
        cost_source = "calculated"
    else:
        estimated_cost = provided_cost
        cost_source = "provided"

    discount = model_pricing.get("discount", {}) if isinstance(model_pricing.get("discount"), dict) else {}
    return {
        "model": model,
        "modelUsed": model,
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "cacheHitInputTokens": cache_hit,
        "cacheMissInputTokens": cache_miss,
        "cacheHitRatio": round(cache_hit / max(input_tokens, 1), 3),
        "estimatedCostCny": round(estimated_cost, 6),
        "costSource": cost_source,
        "usageSource": infer_usage_source(audit),
        "discountActive": bool(discount.get("active")),
        "hasCacheSplit": bool(
            audit.get("cacheHitInputTokens")
            or audit.get("cacheMissInputTokens")
            or audit.get("promptCacheHitTokens")
            or audit.get("promptCacheMissTokens")
        ),
    }


def summarize_cost_records(
    records: list[dict[str, Any]],
    pricing: dict[str, Any],
) -> dict[str, Any]:
    budget = pricing.get("monthlyBudget", {}) if isinstance(pricing.get("monthlyBudget"), dict) else {}
    budget_cny = float(budget.get("totalBudgetCny") or 500)
    warn_ratio = float(budget.get("warningThresholdRatio") or 0.75)

    total_cost = sum(float(record.get("estimatedCostCny") or 0) for record in records)
    total_input = sum(_safe_int(record.get("inputTokens")) for record in records)
    total_output = sum(_safe_int(record.get("outputTokens")) for record in records)
    total_cache_hit = sum(_safe_int(record.get("cacheHitInputTokens")) for record in records)
    total_cache_miss = sum(_safe_int(record.get("cacheMissInputTokens")) for record in records)

    budget_status = "ok"
    if total_cost >= budget_cny:
        budget_status = "exceeded"
    elif total_cost >= budget_cny * warn_ratio:
        budget_status = "warning"

    by_model = _bucket_records(records, "model")
    by_mode = _bucket_records(records, "answerMode")
    by_source = _bucket_records(records, "usageSource")
    top = sorted(records, key=lambda item: float(item.get("estimatedCostCny") or 0), reverse=True)[:10]

    return {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "currency": pricing.get("currency", "CNY"),
        "summary": {
            "totalEstimatedCostCny": round(total_cost, 4),
            "totalInputTokens": total_input,
            "totalOutputTokens": total_output,
            "totalCacheHitInputTokens": total_cache_hit,
            "totalCacheMissInputTokens": total_cache_miss,
            "cacheHitRatio": round(total_cache_hit / max(total_input, 1), 3),
            "budgetCny": budget_cny,
            "budgetStatus": budget_status,
            "recordCount": len(records),
            "astrbotEstimatedCostCny": round(
                float(by_source.get("astrbot", {}).get("estimatedCostCny") or 0),
                4,
            ),
        },
        "byModel": by_model,
        "byAnswerMode": by_mode,
        "bySource": by_source,
        "topExpensiveRecords": top,
        "routingFindings": generate_routing_findings(records),
    }


def build_cost_report(repo_root: Path, **paths: Any) -> dict[str, Any]:
    pricing_path = paths.get("pricing_path", "hermes/model_pricing.yaml")
    pricing = load_pricing(repo_root, pricing_path)
    records = load_cost_records(repo_root, **paths)
    return summarize_cost_records(records, pricing)


def build_daily_cost_heatmap(
    repo_root: Path,
    *,
    days: int,
    daily_budget_cny: float,
    monthly_budget_cny: float,
) -> dict[str, Any]:
    records = load_cost_records(repo_root)
    daily_costs: dict[str, float] = {}
    by_model: dict[str, float] = {}
    by_source: dict[str, float] = {}
    today = datetime.now(timezone.utc).date()
    selected_dates = [
        (today - timedelta(days=days - 1 - index)).strftime("%Y-%m-%d")
        for index in range(days)
    ]
    selected_date_set = set(selected_dates)

    for record in records:
        date = str(record.get("createdAt") or record.get("recordedAt") or "")[:10]
        if date not in selected_date_set:
            continue
        cost = float(record.get("estimatedCostCny") or 0)
        daily_costs[date] = daily_costs.get(date, 0.0) + cost
        model = str(record.get("model") or "unknown")
        source = str(record.get("usageSource") or "hermes")
        by_model[model] = by_model.get(model, 0.0) + cost
        by_source[source] = by_source.get(source, 0.0) + cost

    days_list: list[dict[str, Any]] = []
    total = 0.0
    for key in selected_dates:
        cost = round(daily_costs.get(key, 0.0), 4)
        total += cost
        days_list.append({
            "date": key,
            "costCny": cost,
            "overDailyBudget": cost > daily_budget_cny,
        })

    monthly_status = "ok"
    alerts: list[str] = []
    if total > monthly_budget_cny:
        monthly_status = "exceeded"
        alerts.append(f"Monthly cost {total:.2f} CNY exceeds {monthly_budget_cny:.0f} CNY budget")
    elif total > monthly_budget_cny * 0.75:
        monthly_status = "warning"
        alerts.append(
            f"Monthly cost {total:.2f} CNY at "
            f"{total/monthly_budget_cny*100:.0f}% of {monthly_budget_cny:.0f} CNY budget"
        )

    return {
        "days": days_list,
        "totalCny": round(total, 4),
        "dailyBudgetCny": daily_budget_cny,
        "monthlyBudgetCny": monthly_budget_cny,
        "monthlyStatus": monthly_status,
        "byModelCny": {
            key: round(value, 4)
            for key, value in sorted(by_model.items(), key=lambda item: -item[1])
        },
        "bySourceCny": {
            key: round(value, 4)
            for key, value in sorted(by_source.items(), key=lambda item: -item[1])
        },
        "alerts": alerts,
    }


def generate_markdown_report(results: dict[str, Any]) -> str:
    summary = results["summary"]
    lines: list[str] = []
    lines.append("# Hermes Cost Report\n")
    lines.append(f"**Generated:** {results['generatedAt']}\n")
    lines.append("## 1. Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| Currency | {results['currency']} |")
    lines.append(f"| Records | {summary.get('recordCount', 0)} |")
    lines.append(f"| Total estimated cost | {summary['totalEstimatedCostCny']:.4f} CNY |")
    lines.append(f"| AstrBot estimated cost | {summary.get('astrbotEstimatedCostCny', 0):.4f} CNY |")
    lines.append(f"| Total input tokens | {summary['totalInputTokens']:,} |")
    lines.append(f"| Total output tokens | {summary['totalOutputTokens']:,} |")
    lines.append(f"| Cache-hit input tokens | {summary['totalCacheHitInputTokens']:,} |")
    lines.append(f"| Cache-miss input tokens | {summary['totalCacheMissInputTokens']:,} |")
    lines.append(f"| Cache hit ratio | {summary['cacheHitRatio']:.1%} |")
    lines.append(f"| Budget | {summary['budgetCny']:.0f} CNY |")
    lines.append(f"| Budget status | {summary['budgetStatus']} |")
    lines.append("")

    lines.append("## 2. Cost by Source\n")
    lines.append("| Source | Records | Input Tokens | Output Tokens | Estimated Cost (CNY) |")
    lines.append("|---|---:|---:|---:|---:|")
    for source, item in results["bySource"].items():
        lines.append(
            f"| {source} | {item['records']} | {item['inputTokens']:,} | "
            f"{item['outputTokens']:,} | {item['estimatedCostCny']:.4f} |"
        )
    lines.append("")

    lines.append("## 3. Cost by Model\n")
    lines.append("| Model | Records | Input Tokens | Output Tokens | Estimated Cost (CNY) |")
    lines.append("|---|---:|---:|---:|---:|")
    for model, item in results["byModel"].items():
        lines.append(
            f"| {model} | {item['records']} | {item['inputTokens']:,} | "
            f"{item['outputTokens']:,} | {item['estimatedCostCny']:.4f} |"
        )
    lines.append("")

    if results["byAnswerMode"]:
        lines.append("## 4. Cost by Answer Mode\n")
        lines.append("| Answer Mode | Records | Model Mix | Estimated Cost (CNY) |")
        lines.append("|---|---:|---|---:|")
        for mode, item in results["byAnswerMode"].items():
            lines.append(
                f"| {mode} | {item['records']} | {item.get('modelMix', '')} | "
                f"{item['estimatedCostCny']:.4f} |"
            )
        lines.append("")

    if results["topExpensiveRecords"]:
        lines.append("## 5. Top Expensive Records\n")
        lines.append("| Record ID | Source | Mode | Model | Input | Output | Cost (CNY) |")
        lines.append("|---|---|---|---|---:|---:|---:|")
        for item in results["topExpensiveRecords"][:10]:
            record_id = str(item.get("answerId") or item.get("usageId") or "")[-24:]
            lines.append(
                f"| `...{record_id}` | {item.get('usageSource', '')} | "
                f"{item.get('answerMode', '')} | {item.get('model', '')} "
                f"| {_safe_int(item.get('inputTokens')):,} | "
                f"{_safe_int(item.get('outputTokens')):,} | "
                f"{float(item.get('estimatedCostCny') or 0):.6f} |"
            )
        lines.append("")

    if results["routingFindings"]:
        lines.append("## 6. Routing Findings\n")
        lines.append("| Severity | Finding | Recommendation |")
        lines.append("|---|---|---|")
        for item in results["routingFindings"]:
            lines.append(f"| {item['severity']} | {item['finding'][:100]} | {item['recommendation'][:100]} |")
        lines.append("")

    lines.append("## 7. Notes\n")
    lines.append("- Pricing is loaded from `hermes/model_pricing.yaml`.")
    lines.append(
        "- AstrBot usage is included from `hermes/agent_usage.jsonl`, "
        "`hermes/eval/eval_usage.jsonl`, and compatible `answer_audit.jsonl` records."
    )
    lines.append(
        "- Matching AstrBot usage IDs are de-duplicated so agent/eval ledgers "
        "and answer audit records do not double-count."
    )
    lines.append("")
    return "\n".join(lines)


def infer_usage_source(record: dict[str, Any]) -> str:
    explicit = str(record.get("usageSource") or "").strip()
    if explicit:
        return explicit
    answer_id = str(record.get("answerId") or "")
    mode = str(record.get("answerMode") or "")
    if answer_id.startswith("astrbot.") or mode.startswith("astrbot_"):
        return "astrbot"
    source = str(record.get("source") or "").strip()
    return source or "hermes"


def generate_routing_findings(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for record in records:
        mode = str(record.get("answerMode") or "")
        model = str(record.get("model") or record.get("modelUsed") or "")
        if mode == "direct_lookup" and model == "deepseek-v4-pro":
            findings.append({
                "severity": "WARNING",
                "finding": f"direct_lookup using Pro: {str(record.get('question') or '')[:60]}",
                "recommendation": "direct_lookup should use Flash or no LLM.",
            })
        if mode == "insufficient_evidence" and model == "deepseek-v4-pro":
            findings.append({
                "severity": "WARNING",
                "finding": "insufficient_evidence using Pro",
                "recommendation": "No evidence available should not consume Pro budget.",
            })
        if mode == "hypothesis" and model == "deepseek-v4-pro":
            findings.append({
                "severity": "INFO",
                "finding": "hypothesis using Pro - review necessity",
                "recommendation": "Flash is sufficient for speculative answers.",
            })
    return findings


def _agent_usage_to_audit_record(record: dict[str, Any]) -> dict[str, Any]:
    tools_used = record.get("toolsUsed")
    if not tools_used and record.get("selectedTool"):
        tools_used = [record.get("selectedTool")]
    return {
        "answerId": f"astrbot.agent.{record.get('usageId', '')}",
        "usageId": record.get("usageId"),
        "question": record.get("question", ""),
        "answerMode": "astrbot_agent_chat",
        "usageSource": "astrbot",
        "modelUsed": record.get("pricingModel") or record.get("model") or "deepseek-v4-flash",
        "apiModel": record.get("model"),
        "toolsUsed": tools_used or [],
        "inputTokens": record.get("inputTokens", 0),
        "outputTokens": record.get("outputTokens", 0),
        "estimatedCostCny": record.get("estimatedCostCny"),
        "createdAt": record.get("recordedAt"),
        "cacheHitInputTokens": record.get("promptCacheHitTokens", 0),
        "cacheMissInputTokens": record.get("promptCacheMissTokens", 0),
        "discountActive": record.get("discountActive", False),
    }


def _eval_usage_to_audit_record(record: dict[str, Any]) -> dict[str, Any]:
    eval_id = str(record.get("evalId") or record.get("usageId") or "")
    return {
        "answerId": f"astrbot.{eval_id}",
        "usageId": record.get("usageId"),
        "evalId": eval_id,
        "question": record.get("question", ""),
        "answerMode": f"astrbot_eval_{record.get('category', 'unknown')}",
        "usageSource": "astrbot",
        "modelUsed": record.get("pricingModel") or record.get("model") or "deepseek-v4-flash",
        "toolsUsed": record.get("toolCalls") or [],
        "inputTokens": record.get("inputTokens", 0),
        "outputTokens": record.get("outputTokens", 0),
        "estimatedCostCny": record.get("estimatedCostCny"),
        "createdAt": record.get("recordedAt"),
        "cacheHitInputTokens": 0,
        "cacheMissInputTokens": record.get("inputTokens", 0),
    }


def _canonical_cost_key(record: dict[str, Any]) -> str:
    answer_id = str(record.get("answerId") or "")
    if answer_id.startswith("astrbot.agent."):
        return f"agent_usage:{answer_id.removeprefix('astrbot.agent.')}"
    if answer_id.startswith("astrbot.") and str(record.get("answerMode") or "").startswith(
        "astrbot_eval"
    ):
        return f"eval_usage:{answer_id.removeprefix('astrbot.')}"
    usage_id = str(record.get("usageId") or "")
    if usage_id.startswith("agent_usage_"):
        return f"agent_usage:{usage_id}"
    eval_id = str(record.get("evalId") or "")
    if eval_id:
        return f"eval_usage:{eval_id}"
    if answer_id:
        return f"answer_audit:{answer_id}"
    return f"record:{json.dumps(record, sort_keys=True, default=str)}"


def _bucket_records(records: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for record in records:
        bucket_key = str(record.get(key) or "unknown")
        bucket = buckets.setdefault(
            bucket_key,
            {
                "records": 0,
                "inputTokens": 0,
                "outputTokens": 0,
                "estimatedCostCny": 0.0,
                "models": set(),
            },
        )
        bucket["records"] += 1
        bucket["inputTokens"] += _safe_int(record.get("inputTokens"))
        bucket["outputTokens"] += _safe_int(record.get("outputTokens"))
        bucket["estimatedCostCny"] += float(record.get("estimatedCostCny") or 0)
        bucket["models"].add(str(record.get("model") or "unknown"))

    output: dict[str, dict[str, Any]] = {}
    for bucket_key, bucket in sorted(
        buckets.items(),
        key=lambda item: -float(item[1]["estimatedCostCny"]),
    ):
        output[bucket_key] = {
            "records": bucket["records"],
            "inputTokens": bucket["inputTokens"],
            "outputTokens": bucket["outputTokens"],
            "estimatedCostCny": round(float(bucket["estimatedCostCny"]), 6),
            "modelMix": ", ".join(sorted(bucket["models"])),
        }
    return output


def _model_pricing(pricing_data: dict[str, Any], model: str) -> dict[str, Any]:
    models = pricing_data.get("models", {}) if isinstance(pricing_data.get("models"), dict) else {}
    model_def = models.get(model, {}) if isinstance(models.get(model), dict) else {}
    pricing = model_def.get("pricing", {}) if isinstance(model_def.get("pricing"), dict) else {}
    return pricing


def _pricing_model_id(model: str, pricing: dict[str, Any]) -> str:
    models = pricing.get("models", {}) if isinstance(pricing.get("models"), dict) else {}
    normalized = str(model or "").strip()
    if normalized in models:
        return normalized
    lowered = normalized.lower()
    if lowered in {"deepseek-chat", "deepseek-v3", "deepseek-v4-flash"}:
        return "deepseek-v4-flash"
    if lowered in {"deepseek-reasoner", "deepseek-v4-pro"}:
        return "deepseek-v4-pro"
    return "deepseek-v4-flash"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _first_present(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] is not None and record[key] != "":
            return record[key]
    return None


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None
