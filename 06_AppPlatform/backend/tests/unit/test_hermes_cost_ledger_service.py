import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.services.hermes_cost_ledger_service import (
    build_cost_report,
    build_daily_cost_heatmap,
    load_cost_records,
)


def _append_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")


def test_astrbot_usage_ledgers_are_included_and_deduped(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    agent_usage_id = "agent_usage_chat_1"
    eval_id = "eval_retrieval_1"

    _append_jsonl(
        tmp_path / "hermes" / "agent_usage.jsonl",
        [
            {
                "usageId": agent_usage_id,
                "recordedAt": now,
                "model": "deepseek-chat",
                "pricingModel": "deepseek-v4-flash",
                "inputTokens": 1000,
                "outputTokens": 200,
                "promptCacheHitTokens": 300,
                "promptCacheMissTokens": 700,
                "estimatedCostCny": 0.1234,
            }
        ],
    )
    _append_jsonl(
        tmp_path / "hermes" / "eval" / "eval_usage.jsonl",
        [
            {
                "usageId": "eval_usage_1",
                "evalId": eval_id,
                "category": "retrieval",
                "recordedAt": now,
                "model": "deepseek-chat",
                "inputTokens": 100,
                "outputTokens": 50,
                "estimatedCostCny": 0.25,
            }
        ],
    )
    _append_jsonl(
        tmp_path / "hermes" / "answer_audit.jsonl",
        [
            {
                "answerId": f"astrbot.agent.{agent_usage_id}",
                "answerMode": "astrbot_agent_chat",
                "modelUsed": "deepseek-v4-flash",
                "inputTokens": 9999,
                "outputTokens": 999,
                "estimatedCostCny": 0.1234,
                "createdAt": now,
            },
            {
                "answerId": f"astrbot.{eval_id}",
                "answerMode": "astrbot_eval_retrieval",
                "modelUsed": "deepseek-v4-flash",
                "inputTokens": 100,
                "outputTokens": 50,
                "estimatedCostCny": 0.25,
                "createdAt": now,
            },
            {
                "answerId": "country_copilot_answer_1",
                "answerMode": "direct_lookup",
                "modelUsed": "deepseek-v4-flash",
                "inputTokens": 1_000_000,
                "outputTokens": 0,
                "createdAt": now,
            },
        ],
    )

    records = load_cost_records(tmp_path)
    report = build_cost_report(tmp_path)

    assert len(records) == 3
    assert report["summary"]["recordCount"] == 3
    assert report["summary"]["totalEstimatedCostCny"] == pytest.approx(1.3734)
    assert report["summary"]["astrbotEstimatedCostCny"] == pytest.approx(0.3734)
    assert report["bySource"]["astrbot"]["records"] == 2
    assert report["bySource"]["astrbot"]["estimatedCostCny"] == pytest.approx(0.3734)


def test_daily_cost_heatmap_breaks_out_astrbot_source(tmp_path: Path) -> None:
    now_dt = datetime.now(timezone.utc)
    now = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    yesterday = (now_dt - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _append_jsonl(
        tmp_path / "hermes" / "agent_usage.jsonl",
        [
            {
                "usageId": "agent_usage_today",
                "recordedAt": now,
                "pricingModel": "deepseek-v4-flash",
                "inputTokens": 1,
                "outputTokens": 1,
                "estimatedCostCny": 0.42,
            },
            {
                "usageId": "agent_usage_yesterday",
                "recordedAt": yesterday,
                "pricingModel": "deepseek-v4-flash",
                "inputTokens": 1,
                "outputTokens": 1,
                "estimatedCostCny": 9.99,
            }
        ],
    )

    heatmap = build_daily_cost_heatmap(
        tmp_path,
        days=1,
        daily_budget_cny=20,
        monthly_budget_cny=500,
    )

    assert heatmap["totalCny"] == pytest.approx(0.42)
    assert heatmap["bySourceCny"]["astrbot"] == pytest.approx(0.42)
    assert heatmap["days"][0]["costCny"] == pytest.approx(0.42)
