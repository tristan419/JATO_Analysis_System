from __future__ import annotations

import asyncio
import json
import time

import pytest

from app.services import jato_agent_stream_service as stream
from app.services.jato_evidence_package_service import build_evidence_package
from app.services.jato_evidence_package_service import evidence_ref_count


def test_stream_parse_answer_handles_fenced_json_block() -> None:
    parsed = stream._parse_answer(
        """```json
{"title": "JATO answer", "direct": "Streaming final answer", "bullets": ["A"], "limitations": []}
```"""
    )

    assert parsed == {
        "title": "JATO answer",
        "direct": "Streaming final answer",
        "bullets": ["A"],
        "limitations": [],
    }


def test_stream_parse_answer_preserves_follow_ups() -> None:
    parsed = stream._parse_answer(
        """```json
{"title": "JATO answer", "direct": "Streaming final answer", "bullets": ["A"], "limitations": [], "followUps": ["继续看车型？", "做竞品对比？"]}
```"""
    )

    assert parsed["followUps"] == ["继续看车型？", "做竞品对比？"]


def test_final_answer_instruction_uses_checked_evidence_not_tool_trace() -> None:
    instruction = stream._final_answer_instruction(
        question="匈牙利 J7 HEV 市场机会是什么？",
        evidence_plan={"intent": "market_overview", "country": "Hungary"},
        evidence_package={
            "country": "Hungary",
            "intent": "market_overview",
            "confidence": "medium",
            "entities": {"models": ["J7 HEV"]},
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "keyFindings": ["HEV keeps a practical role.", "totalRows: 0"],
                    "evidenceRefs": [
                        {"refId": "hev_sales", "label": "HEV sales", "value": 2687, "unit": "units", "source": "JATO"},
                        {"refId": "hev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 89.5, "unit": "%", "source": "JATO"},
                        {"refId": "technical", "label": "row_count", "value": 12},
                    ],
                },
            ],
            "missingEvidence": [
                {"name": "competitor_price", "reason": "Need current competitor MSRP.", "impact": "weakens_answer"},
            ],
        },
    )

    assert "checked Evidence Package" in instruction
    assert "HEV sales" in instruction
    assert "2687" in instruction
    assert "competitor_price" in instruction
    assert "row_count" not in instruction
    assert "totalRows: 0" not in instruction
    assert "HEV 2WD 占比" in instruction
    assert "Do not describe tools" in instruction
    assert "binary choice" in instruction


def test_final_answer_messages_isolate_composition_from_react_history() -> None:
    messages = stream._final_answer_messages(
        question="匈牙利 J7 HEV 应优先 2WD 还是 4WD？",
        evidence_plan={"intent": "market_overview", "country": "Hungary"},
        evidence_package={
            "country": "Hungary",
            "intent": "market_overview",
            "toolResults": [{
                "toolName": "query_segment_breakdown",
                "success": True,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {"refId": "hev_2wd", "label": "crossTabs.driveByFuel.HEV.2WD_pct", "value": 89.5, "unit": "%"},
                    {"refId": "hev_4wd", "label": "crossTabs.driveByFuel.HEV.4WD_pct", "value": 9.9, "unit": "%"},
                ],
            }],
            "missingEvidence": [],
        },
    )

    assert [message["role"] for message in messages] == ["system", "user"]
    assert "TOOL RESULT" not in "\n".join(message["content"] for message in messages)
    assert "HEV 2WD 占比" in messages[1]["content"]
    assert "89.5" in messages[1]["content"]


def test_visible_stream_answer_skips_repeated_direct_bullet_but_keeps_evidence() -> None:
    direct = "直接结论：匈牙利市场现阶段不要把 HEV/PHEV 简化成二选一；第一版更稳的是 HEV 做低风险主线。"
    visible = stream._visible_stream_answer_text({
        "direct": direct,
        "bullets": [
            "结论：匈牙利市场现阶段不要把 HEV/PHEV 简化成二选一；第一版更稳的是 HEV 做低风险主线。",
            "证据：PHEV = 969 units，SUV A0 = 7,303 units。",
            "下一步：建立 HEV vs PHEV 场景决策表。",
        ],
    })

    assert visible.count("匈牙利市场现阶段不要把 HEV/PHEV 简化成二选一") == 1
    assert "证据：PHEV = 969 units" in visible
    assert "下一步：建立 HEV vs PHEV 场景决策表" in visible


def test_visible_stream_answer_includes_grounding_boundaries_without_a_card_protocol() -> None:
    visible = stream._visible_stream_answer_text({
        "direct": "匈牙利 HEV 当前结构可以判断，但趋势不能下定论。",
        "bullets": ["当月 HEV 与 SUV 级别结构已有 JATO cross-tab。"],
        "limitations": [
            "月度序列工具返回 0 个数据点，不能把当月结构写成增长趋势。",
            "车型级价格证据尚未查询。",
        ],
    })

    assert "当前边界：" in visible
    assert "月度序列工具返回 0 个数据点" in visible
    assert "车型级价格证据尚未查询" in visible
    assert "TOOL:" not in visible


def test_build_developer_trace_uses_compact_tool_summaries() -> None:
    trace = stream._build_developer_trace(
        "Sweden",
        "Explain BEV market movement",
        [
            {
                "tool": "query_country_snapshot",
                "reason": "Need country evidence",
                "round": 1,
                "status": "ok",
                "hasData": True,
                "chartCount": 2,
                "summary": {"kpis": [{"label": "Sales", "value": 10}]},
                "chartSpecs": [{"large": "not exposed"}],
            },
            {
                "tool": "search_market_news",
                "reason": "Need policy context",
                "round": 2,
                "status": "error",
                "hasData": False,
                "error": "timeout",
            },
        ],
        [
            {
                "id": "fu_1",
                "label": "继续看品牌",
                "question": "继续看品牌？",
                "intent": "drilldown",
                "reason": "需要拆分",
                "expectedTools": ["query_country_snapshot"],
                "expectedOutput": "chart",
                "priority": 1,
            },
            {
                "id": "fu_2",
                "label": "做竞品对比",
                "question": "做竞品对比？",
                "intent": "compare",
                "reason": "需要对比",
                "expectedTools": ["query_msrp_pricing"],
                "expectedOutput": "table",
                "priority": 2,
            },
        ],
        {
            "intent": "market_overview",
            "allowedTools": ["query_country_snapshot"],
            "answerMode": "analysis",
        },
    )

    assert trace["toolCount"] == 2
    assert trace["intent"] == "market_overview"
    assert trace["followUps"][0]["label"] == "继续看品牌"
    assert trace["allowedTools"] == ["query_country_snapshot"]
    assert trace["tools"][0]["summary"] == {"kpis": [{"label": "Sales", "value": 10}]}
    assert trace["tools"][0]["chartCount"] == 2
    assert "chartSpecs" not in trace["tools"][0]
    assert trace["tools"][1]["status"] == "error"
    assert trace["tools"][1]["error"] == "timeout"


def test_stream_tool_coverage_guard_identifies_missing_required_tool() -> None:
    evidence_plan = {
        "intent": "pricing_analysis",
        "requiredTools": ["query_msrp_pricing"],
        "allowedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "toolPlan": [
            {
                "toolName": "query_msrp_pricing",
                "input": {"country": "Sweden", "model": "J7 HEV"},
            }
        ],
    }

    assert stream.missing_required_tools(evidence_plan, ["compare_competitive_set"], allowed_tools=evidence_plan["allowedTools"]) == ["query_msrp_pricing"]
    assert stream.required_tool_args(evidence_plan, "query_msrp_pricing", country="Sweden", question="J7 HEV pricing") == {
        "country": "Sweden",
        "model": "J7 HEV",
        "question": "J7 HEV pricing",
    }


def test_stream_coverage_guard_still_attempts_required_tool_after_react_budget() -> None:
    evidence_plan = {
        "intent": "pricing_analysis",
        "requiredTools": ["query_msrp_pricing"],
        "allowedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "toolPlan": [
            {
                "toolName": "query_msrp_pricing",
                "input": {"country": "Sweden", "model": "J7 HEV"},
            }
        ],
    }
    tool_history = [
        {
            "tool": "compare_competitive_set",
            "status": "ok",
            "round": 1,
        }
    ]

    assert stream._coverage_guard_tools(
        evidence_plan,
        tool_history,
        allowed_tools=evidence_plan["allowedTools"],
    ) == ["query_msrp_pricing"]


def test_stream_required_tool_preselector_runs_internal_market_tools_before_optional_research() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "requiredTools": ["query_country_snapshot", "build_market_chart"],
        "allowedTools": ["query_country_snapshot", "build_market_chart", "external_research"],
        "toolPlan": [
            {
                "toolName": "query_country_snapshot",
                "input": {"country": "Hungary", "question": "匈牙利 HEV 市场机会？"},
            },
            {
                "toolName": "build_market_chart",
                "input": {"country": "Hungary", "question": "匈牙利 HEV 市场机会？"},
            },
            {
                "toolName": "external_research",
                "input": {"country": "Hungary", "question": "匈牙利 HEV 市场机会？"},
            },
        ],
    }

    first = stream._required_tool_selection_content(
        evidence_plan,
        [],
        allowed_tools=evidence_plan["allowedTools"],
        country="Hungary",
        question="匈牙利 HEV 市场机会？",
    )
    second = stream._required_tool_selection_content(
        evidence_plan,
        [{"tool": "query_country_snapshot", "status": "ok"}],
        allowed_tools=evidence_plan["allowedTools"],
        country="Hungary",
        question="匈牙利 HEV 市场机会？",
    )
    third = stream._required_tool_selection_content(
        evidence_plan,
        [
            {"tool": "query_country_snapshot", "status": "ok"},
            {"tool": "build_market_chart", "status": "ok"},
        ],
        allowed_tools=evidence_plan["allowedTools"],
        country="Hungary",
        question="匈牙利 HEV 市场机会？",
    )

    assert "TOOL: query_country_snapshot" in first
    assert "TOOL: build_market_chart" in second
    assert "external_research" not in first
    assert "external_research" not in second
    assert third == ""


def test_stream_defers_answer_tokens_until_after_optional_external_research_by_default(monkeypatch) -> None:
    monkeypatch.setenv(stream.ASTRBOT_PROVIDER_KEY_ENV, "test-key")
    monkeypatch.delenv("APP_ASTRBOT_STREAM_PREFINAL_ANSWER", raising=False)

    evidence_plan = {
        "intent": "market_overview",
        "country": "Hungary",
        "entities": {"countries": ["Hungary"]},
        "requiredTools": ["query_country_snapshot"],
        "allowedTools": ["query_country_snapshot", "external_research"],
        "toolPlan": [
            {
                "toolName": "query_country_snapshot",
                "input": {"country": "Hungary", "question": "匈牙利 HEV 市场机会？"},
            },
            {
                "toolName": "external_research",
                "input": {"country": "Hungary", "question": "匈牙利 HEV 市场机会？"},
            },
        ],
        "evidenceNeeded": [{"name": "market_kpis"}, {"name": "trend_or_mix"}],
        "answerMode": "analysis",
    }

    monkeypatch.setattr(stream, "build_evidence_plan", lambda _country, _question: evidence_plan)
    monkeypatch.setattr(
        stream,
        "_call_llm",
        lambda *_args, **_kwargs: (
            'TOOL: external_research\n'
            'ARGS: {"country": "Hungary", "question": "匈牙利 HEV 市场机会？"}\n'
            "REASON: add source context"
        ),
    )

    async def fake_stream_llm(*_args, **_kwargs):
        yield '{"title":"Hungary HEV","direct":"匈牙利 HEV 机会需要结合内部数据和外部来源判断。","bullets":[],"limitations":[]}'

    def fake_tool(name, args):
        if name == "external_research":
            return {
                "tool": name,
                "metadata": {"source": "fake_external"},
                "data": {"items": [{"title": "Hungary HEV source", "claim": "Hybrid demand remains visible."}]},
            }
        return {
            "tool": name,
            "metadata": {"source": "jato_country_snapshot", "country": args.get("country", "Hungary")},
            "data": {
                "powertrainMix": [
                    {"label": "HEV", "value": 2687, "share": 0.18},
                    {"label": "PHEV", "value": 969, "share": 0.06},
                ],
            },
        }

    monkeypatch.setattr(stream, "_stream_llm", fake_stream_llm)
    monkeypatch.setattr(stream, "call_jato_mcp_tool", fake_tool)

    frames: list[dict[str, object]] = []

    async def collect() -> None:
        async for frame in stream.stream_agent_response(
            country="Sweden",
            question="匈牙利市场现在适合推 PHEV 还是 HEV？请不要回答瑞典。",
            max_rounds=2,
            session_id="",
            requested_mode="auto",
        ):
            frames.append(json.loads(frame.split("data: ", 1)[1].strip()))

    asyncio.run(collect())

    first_token_index = next(index for index, frame in enumerate(frames) if frame.get("_event") == "token")
    external_call_index = next(
        index
        for index, frame in enumerate(frames)
        if frame.get("_event") == "tool_call" and frame.get("tool") == "external_research"
    )
    done_frame = next(frame for frame in frames if frame.get("_event") == "done")

    assert first_token_index > external_call_index
    assert not any(frame.get("_event") == "answer_start" for frame in frames[:external_call_index])
    assert done_frame["country"] == "Hungary"
    assert "匈牙利" in "".join(str(frame.get("text") or "") for frame in frames if frame.get("_event") == "token")


def test_stream_tool_selection_timeout_uses_evidence_plan_fallback() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "allowedTools": ["query_country_snapshot", "external_research"],
        "requiredTools": ["query_country_snapshot"],
        "toolPlan": [
            {
                "toolName": "query_country_snapshot",
                "input": {"country": "Hungary", "question": "匈牙利 HEV 市场机会？"},
            },
            {
                "toolName": "external_research",
                "input": {"country": "Hungary", "question": "匈牙利 HEV 市场机会？"},
            },
        ],
    }

    content = stream._fallback_tool_selection_content(
        evidence_plan,
        [],
        allowed_tools=evidence_plan["allowedTools"],
        country="Hungary",
        question="匈牙利 HEV 市场机会？",
        error=TimeoutError("provider timeout"),
    )

    assert "TOOL: query_country_snapshot" in content
    assert '"country": "Hungary"' in content
    assert "evidence_plan_fallback_after_TimeoutError" in content


def test_stream_tool_selection_fallback_skips_already_executed_tools() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "allowedTools": ["query_country_snapshot", "external_research"],
        "requiredTools": ["query_country_snapshot"],
        "toolPlan": [
            {"toolName": "query_country_snapshot", "input": {"country": "Hungary"}},
            {"toolName": "external_research", "input": {"country": "Hungary"}},
        ],
    }

    content = stream._fallback_tool_selection_content(
        evidence_plan,
        [{"tool": "query_country_snapshot"}],
        allowed_tools=evidence_plan["allowedTools"],
        country="Hungary",
        question="匈牙利 HEV 市场机会？",
        error=TimeoutError("provider timeout"),
    )

    assert "TOOL: external_research" in content
    assert '"question": "匈牙利 HEV 市场机会？"' in content


def test_stream_grounded_local_draft_uses_evidence_package_country() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "country": "Hungary",
        "entities": {},
        "allowedTools": ["query_country_snapshot"],
        "requiredTools": ["query_country_snapshot"],
        "toolPlan": [{"toolName": "query_country_snapshot", "input": {"country": "Hungary"}}],
        "evidenceNeeded": [{"name": "market_kpis"}],
    }
    evidence_package = {
        "evidenceId": "ev_hu",
        "intent": "market_overview",
        "country": "Hungary",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Hungary"},
                "success": True,
                "rowCount": 8,
                "sourceType": "jato_parquet",
                "summary": "Hungary snapshot returned market KPIs.",
                "keyFindings": ["HEV SUV demand is visible in the sample."],
                "evidenceRefs": [
                    {
                        "refId": "hu_1",
                        "label": "SUV A HEV share",
                        "value": 38.2,
                        "unit": "%",
                        "source": "jato_snapshot",
                    }
                ],
            }
        ],
        "missingEvidence": [],
    }

    draft = stream._build_grounded_local_draft(
        country="Hungary",
        question="匈牙利 HEV 市场机会？不要回答瑞典。",
        tool_call_history=[{"tool": "query_country_snapshot", "status": "ok"}],
        evidence_plan=evidence_plan,
        evidence_package=evidence_package,
    )
    visible = stream._visible_stream_answer_text(draft)

    assert "Hungary" in draft["businessSynthesisPlan"]["country"]
    assert "瑞典" not in visible
    assert "已查数据和判断" in visible
    assert "query_country_snapshot" in str(draft.get("grounding", {})) or draft["grounding"]["evidenceRefCount"] == 1


def test_stream_early_draft_waits_when_only_source_repair_evidence_exists() -> None:
    evidence_plan = {
        "intent": "competitor_compare",
        "country": "Sweden",
        "requiredTools": ["compare_competitive_set", "build_market_chart", "query_msrp_pricing"],
    }
    evidence_package = {
        "evidenceId": "ev_source_repair_only",
        "intent": "competitor_compare",
        "country": "Sweden",
        "toolResults": [
            {
                "toolName": "compare_competitive_set",
                "success": True,
                "evidenceRefs": [
                    {
                        "refId": "repair_1",
                        "label": "source_repair_candidates.competitorCorridor.Sorento",
                        "value": "official price source search candidate",
                        "source": "source_draft",
                    }
                ],
            }
        ],
        "missingEvidence": [
            {"name": "current_price_gap", "impact": "weakens_answer"},
        ],
    }

    assert not stream._should_stream_early_evidence_draft(
        evidence_package=evidence_package,
        evidence_plan=evidence_plan,
        tool_call_history=[{"tool": "compare_competitive_set", "status": "ok"}],
    )


def test_stream_early_draft_can_stream_after_concrete_market_evidence() -> None:
    evidence_plan = {
        "intent": "competitor_compare",
        "country": "Sweden",
        "requiredTools": ["compare_competitive_set", "build_market_chart", "query_msrp_pricing"],
    }
    evidence_package = {
        "evidenceId": "ev_market_backed",
        "intent": "competitor_compare",
        "country": "Sweden",
        "toolResults": [
            {
                "toolName": "compare_competitive_set",
                "success": True,
                "evidenceRefs": [
                    {"refId": "cmp_1", "label": "competitors.Sorento.sales", "value": 1200, "unit": "units"},
                ],
            },
            {
                "toolName": "build_market_chart",
                "success": True,
                "evidenceRefs": [
                    {"refId": "mkt_1", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4wd_pct", "value": 60.1, "unit": "%"},
                    {"refId": "mkt_2", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.PHEV_pct", "value": 38.2, "unit": "%"},
                ],
            },
        ],
        "missingEvidence": [
            {"name": "current_price_gap", "impact": "weakens_answer"},
        ],
    }

    assert stream._should_stream_early_evidence_draft(
        evidence_package=evidence_package,
        evidence_plan=evidence_plan,
        tool_call_history=[
            {"tool": "compare_competitive_set", "status": "ok"},
            {"tool": "build_market_chart", "status": "ok"},
        ],
    )


def test_stream_emits_visible_answer_tokens_before_done(monkeypatch) -> None:
    monkeypatch.setenv(stream.ASTRBOT_PROVIDER_KEY_ENV, "test-key")
    llm_calls = {"count": 0}

    def fake_call_llm(_messages, _api_key, max_tokens=400, timeout_seconds=30):
        llm_calls["count"] += 1
        if llm_calls["count"] == 1:
            return "\n".join([
                "TOOL: query_country_snapshot",
                'ARGS: {"country": "Sweden", "question": "wrong country"}',
                "REASON: need market data",
            ])
        return 'FINAL_ANSWER: {"title":"Done","direct":"Hungary governed final answer.","bullets":["Evidence-backed"],"limitations":[]}'

    async def fake_stream_llm(_messages, _api_key, max_tokens=800):
        yield '{"title":"Done","direct":"Hungary governed final answer.","bullets":["Evidence-backed"],"limitations":[]}'

    def fake_tool(name, args):
        return {
            "tool": name,
            "metadata": {"source": "jato_snapshot", "resultCount": 2},
            "data": {
                "kpis": {"hevShare": 38.2},
                "topModels": [{"label": "J7 HEV", "value": 120, "unit": "units"}],
            },
        }

    monkeypatch.setattr(stream, "_call_llm", fake_call_llm)
    monkeypatch.setattr(stream, "_stream_llm", fake_stream_llm)
    monkeypatch.setattr(stream, "call_jato_mcp_tool", fake_tool)

    frames: list[dict[str, object]] = []

    async def collect() -> None:
        async for frame in stream.stream_agent_response(
            country="Sweden",
            question="匈牙利 HEV 市场机会？不要回答瑞典。",
            max_rounds=2,
            session_id="",
            requested_mode="auto",
        ):
            payload = frame.split("data: ", 1)[1].strip()
            frames.append(json.loads(payload))

    asyncio.run(collect())

    event_names = [str(frame.get("_event") or "") for frame in frames]
    assert "answer_start" in event_names
    assert "token" in event_names
    assert "done" in event_names
    assert event_names.index("answer_start") < event_names.index("done")
    token_text = "".join(str(frame.get("text") or "") for frame in frames if frame.get("_event") == "token")
    assert "Hungary" in token_text or "匈牙利" in token_text
    assert "瑞典" not in token_text
    done_frame = next(frame for frame in frames if frame.get("_event") == "done")
    assert done_frame["country"] == "Hungary"


def test_stream_provider_refinement_failure_finishes_with_local_draft(monkeypatch) -> None:
    monkeypatch.setenv(stream.ASTRBOT_PROVIDER_KEY_ENV, "test-key")

    def fake_call_llm(_messages, _api_key, max_tokens=400, timeout_seconds=30):
        raise OSError("ssl provider failure")

    async def fake_stream_llm(_messages, _api_key, max_tokens=800):
        raise OSError("ssl provider failure")
        yield ""  # pragma: no cover

    def fake_tool(name, args):
        return {
            "tool": name,
            "metadata": {"source": "jato_snapshot", "resultCount": 2},
            "data": {
                "kpis": {"hevSales": 2687},
                "topModels": [{"label": "Hungary HEV", "value": 2687, "unit": "units"}],
            },
        }

    monkeypatch.setattr(stream, "_call_llm", fake_call_llm)
    monkeypatch.setattr(stream, "_stream_llm", fake_stream_llm)
    monkeypatch.setattr(stream, "call_jato_mcp_tool", fake_tool)

    frames: list[dict[str, object]] = []

    async def collect() -> None:
        async for frame in stream.stream_agent_response(
            country="Sweden",
            question="匈牙利 HEV 市场机会是什么？",
            max_rounds=2,
            session_id="",
            requested_mode="auto",
        ):
            payload = frame.split("data: ", 1)[1].strip()
            frames.append(json.loads(payload))

    asyncio.run(collect())

    event_names = [str(frame.get("_event") or "") for frame in frames]
    assert "error" not in event_names
    assert "done" in event_names
    done_frame = next(frame for frame in frames if frame.get("_event") == "done")
    direct = str(done_frame.get("direct") or "")
    nested_answer = done_frame.get("answer")
    assert done_frame["country"] == "Hungary"
    assert not direct.startswith("Error:")
    assert isinstance(nested_answer, dict)
    assert nested_answer["direct"] == direct
    assert nested_answer["evidencePackage"] == done_frame["evidencePackage"]
    assert nested_answer["visualArtifacts"] == done_frame["visualArtifacts"]
    assert done_frame["intent"] == nested_answer["intent"]
    assert done_frame["intent"] == done_frame["evidencePlan"]["intent"]
    assert "ssl provider failure" not in direct
    assert "瑞典" not in direct
    assert any("模型精修暂时不可用" in str(item) for item in done_frame.get("limitations", []))


def test_stream_done_exposes_missing_evidence_from_normalized_package(monkeypatch) -> None:
    monkeypatch.setenv(stream.ASTRBOT_PROVIDER_KEY_ENV, "test-key")

    def fake_call_llm(_messages, _api_key, max_tokens=400, timeout_seconds=30):
        raise OSError("provider unavailable")

    async def fake_stream_llm(_messages, _api_key, max_tokens=800):
        raise OSError("provider unavailable")
        yield ""  # pragma: no cover

    def fake_tool(name, args):
        if name in {"compare_competitive_set", "query_msrp_pricing"}:
            return {
                "tool": name,
                "metadata": {"source": "mock_empty_model_level_evidence", "country": args.get("country", "Hungary")},
                "data": {"items": [], "rows": []},
            }
        return {
            "tool": name,
            "metadata": {"source": "jato_country_chart_deck", "country": args.get("country", "Hungary"), "chartCount": 0},
            "data": {
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
        }

    monkeypatch.setattr(stream, "_call_llm", fake_call_llm)
    monkeypatch.setattr(stream, "_stream_llm", fake_stream_llm)
    monkeypatch.setattr(stream, "call_jato_mcp_tool", fake_tool)

    frames: list[dict[str, object]] = []

    async def collect() -> None:
        async for frame in stream.stream_agent_response(
            country="Hungary",
            question="匈牙利 J7 HEV 市场机会是什么？",
            max_rounds=2,
            session_id="",
            requested_mode="auto",
        ):
            payload = frame.split("data: ", 1)[1].strip()
            frames.append(json.loads(payload))

    asyncio.run(collect())

    done_frame = next(frame for frame in frames if frame.get("_event") == "done")
    missing = done_frame.get("missingEvidence")
    package = done_frame.get("evidencePackage")

    assert done_frame["answerStatus"] == "partially_answered"
    assert isinstance(missing, list)
    assert any(item.get("name") == "model_level_market_opportunity_evidence" for item in missing if isinstance(item, dict))
    assert isinstance(package, dict)
    assert package.get("missingEvidence") == missing


def test_stream_done_uses_scoped_evidence_package_for_visual_artifacts(monkeypatch) -> None:
    monkeypatch.setenv(stream.ASTRBOT_PROVIDER_KEY_ENV, "test-key")

    evidence_plan = {
        "intent": "competitor_compare",
        "country": "Sweden",
        "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
        "requiredTools": ["query_msrp_pricing"],
        "allowedTools": ["query_msrp_pricing"],
        "toolPlan": [
            {
                "toolName": "query_msrp_pricing",
                "input": {
                    "country": "Sweden",
                    "question": "O5 BEV 应该对标 EX30 还是 EV3？",
                    "model": "O5 BEV",
                },
            }
        ],
        "evidenceNeeded": [{"name": "current_msrp", "priority": 1}],
        "answerMode": "analysis",
    }

    monkeypatch.setattr(stream, "build_evidence_plan", lambda _country, _question: evidence_plan)
    monkeypatch.setattr(
        stream,
        "_call_llm",
        lambda *_args, **_kwargs: (
            'TOOL: query_msrp_pricing\n'
            'ARGS: {"country": "Sweden", "question": "O5 BEV 应该对标 EX30 还是 EV3？", "model": "O5 BEV"}\n'
            "REASON: need price evidence"
        ),
    )

    async def fake_stream_llm(_messages, _api_key, max_tokens=800):
        yield '{"title":"O5 competitor","direct":"O5 BEV 对标需要用当前证据判断。","bullets":[],"limitations":[]}'

    def fake_tool(name, args):
        return {
            "tool": name,
            "metadata": {
                "source": "current_price",
                "country": args.get("country", "Sweden"),
                "resultCount": 2,
            },
            "data": {
                "summary": "J7 HEV and RAV4 current price rows returned from a stale lookup.",
                "priceStats": {"min": 34720, "max": 40200},
                "rows": [
                    {"model": "J7 HEV", "msrp": 34720},
                    {"model": "RAV4", "msrp": 40200},
                ],
            },
        }

    monkeypatch.setattr(stream, "_stream_llm", fake_stream_llm)
    monkeypatch.setattr(stream, "call_jato_mcp_tool", fake_tool)

    frames: list[dict[str, object]] = []

    async def collect() -> None:
        async for frame in stream.stream_agent_response(
            country="Sweden",
            question="O5 BEV 应该对标 EX30 还是 EV3？",
            max_rounds=2,
            session_id="",
            requested_mode="auto",
        ):
            payload = frame.split("data: ", 1)[1].strip()
            frames.append(json.loads(payload))

    asyncio.run(collect())

    done_frame = next(frame for frame in frames if frame.get("_event") == "done")
    package = done_frame.get("evidencePackage")
    artifacts = done_frame.get("visualArtifacts")
    serialized_package = json.dumps(package, ensure_ascii=False, sort_keys=True)
    serialized_artifacts = json.dumps(artifacts, ensure_ascii=False, sort_keys=True)

    assert done_frame["answerStatus"] == "insufficient_evidence"
    assert isinstance(package, dict)
    assert package.get("confidence") == "low"
    assert "requested_entity_evidence" in serialized_package
    assert "J7" not in serialized_package
    assert "RAV4" not in serialized_package
    assert "J7" not in serialized_artifacts
    assert "RAV4" not in serialized_artifacts


def test_blind_leasing_question_streams_grounded_refs_and_pricing_artifact(monkeypatch) -> None:
    from app.services import jato_conversation_store as conversation_store

    monkeypatch.setenv(stream.ASTRBOT_PROVIDER_KEY_ENV, "test-key")
    monkeypatch.setattr(conversation_store, "create_session", lambda: "sess_blind_leasing_stream")
    monkeypatch.setattr(conversation_store, "get_context", lambda _session_id: "")
    monkeypatch.setattr(conversation_store, "add_turn", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(stream, "track_tool_call_event", lambda **_kwargs: None)
    monkeypatch.setattr(stream, "track_followup_impression", lambda **_kwargs: None)

    async def fake_stream_llm(*_args, **_kwargs):
        yield json.dumps({
            "title": "Blind EV leasing comparison",
            "direct": "Nimbus E BEV 的已查有效月供低于 Solaris One BEV，但最终 TCO 仍需结合合同范围判断。",
            "bullets": ["比较 36 个月、15,000 km/year 的同口径 offer。"],
            "limitations": [],
            "followUps": [],
        }, ensure_ascii=False)

    def fake_tool(name, args):
        country = args.get("country", "Sweden")
        if name == "query_msrp_pricing":
            return {
                "tool": name,
                "metadata": {"source": "jato_msrp_postgres", "country": country, "resultCount": 2},
                "data": {
                    "country": country,
                    "items": [
                        {"model": "Nimbus E BEV", "msrp": 38900},
                        {"model": "Solaris One BEV", "msrp": 40500},
                    ],
                },
            }
        if name == "query_leasing_offers":
            return {
                "tool": name,
                "metadata": {"source": "jato_lease_offer_postgres", "country": country, "resultCount": 2},
                "data": {
                    "country": country,
                    "items": [
                        {
                            "modelName": "Nimbus E BEV",
                            "termMonths": 36,
                            "mileagePerYear": 15000,
                            "effectiveMonthlyEur": 529,
                            "residualValuePercent": 54,
                            "totalContractCostEur": 19044,
                            "sourceUrl": "https://example.test/nimbus-lease",
                        },
                        {
                            "modelName": "Solaris One BEV",
                            "termMonths": 36,
                            "mileagePerYear": 15000,
                            "effectiveMonthlyEur": 549,
                            "residualValuePercent": 56,
                            "totalContractCostEur": 19764,
                            "sourceUrl": "https://example.test/solaris-lease",
                        },
                    ],
                },
            }
        if name == "build_market_chart":
            return {
                "tool": name,
                "metadata": {"source": "jato_country_chart_deck", "country": country, "chartCount": 1},
                "data": {
                    "country": country,
                    "contextSnapshot": {"powertrainMix": [{"label": "BEV", "value": 25000, "share": 0.42}]},
                    "chartSpecs": {"chartCount": 0, "items": []},
                },
            }
        if name == "query_country_snapshot":
            return {
                "tool": name,
                "metadata": {"source": "jato_country_snapshot", "country": country},
                "data": {"country": country, "kpis": {"cumulativeSales": 60000}},
            }
        if name == "external_research":
            return {
                "tool": name,
                "metadata": {"source": "web_research", "country": country},
                "data": {
                    "country": country,
                    "items": [{
                        "title": "Sweden company car context",
                        "url": "https://example.test/company-car",
                        "claim": "Contract scope and tax assumptions must be aligned before comparing TCO.",
                    }],
                },
            }
        raise AssertionError(f"Unexpected tool: {name}")

    monkeypatch.setattr(stream, "_stream_llm", fake_stream_llm)
    monkeypatch.setattr(stream, "call_jato_mcp_tool", fake_tool)

    frames: list[dict[str, object]] = []

    async def collect() -> None:
        async for frame in stream.stream_agent_response(
            country="Sweden",
            question="Nimbus E BEV 和 Solaris One BEV 的 36 个月月供、RV 和总持有成本怎么比较？",
            max_rounds=5,
            session_id="",
            requested_mode="auto",
        ):
            frames.append(json.loads(frame.split("data: ", 1)[1].strip()))

    asyncio.run(collect())

    tool_calls = [str(frame.get("tool") or "") for frame in frames if frame.get("_event") == "tool_call"]
    done = next(frame for frame in frames if frame.get("_event") == "done")
    package_text = json.dumps(done.get("evidencePackage"), ensure_ascii=False)
    artifacts = done.get("visualArtifacts") if isinstance(done.get("visualArtifacts"), list) else []
    pricing_table = next(item for item in artifacts if item.get("id") == "artifact_pricing_analysis_table")
    pricing_text = json.dumps(pricing_table, ensure_ascii=False)

    assert tool_calls[:3] == ["query_msrp_pricing", "build_market_chart", "query_leasing_offers"]
    assert "answer_start" in [frame.get("_event") for frame in frames]
    assert "token" in [frame.get("_event") for frame in frames]
    assert "Nimbus E BEV.effectiveMonthlyEur" in package_text
    assert "Nimbus E BEV.residualValuePercent" in package_text
    assert "529 EUR/month" in pricing_text
    assert "54 %" in pricing_text


def test_stream_tool_call_with_timeout_returns_tool_result(monkeypatch) -> None:
    monkeypatch.setattr(
        stream,
        "call_jato_mcp_tool",
        lambda name, args: {"tool": name, "data": {"country": args["country"]}},
    )

    result = asyncio.run(
        stream._call_tool_with_timeout(
            "query_country_snapshot",
            {"country": "Hungary"},
            timeout_seconds=0.5,
        )
    )

    assert result["tool"] == "query_country_snapshot"
    assert result["data"]["country"] == "Hungary"


def test_stream_tool_call_with_timeout_raises_readable_timeout(monkeypatch) -> None:
    def slow_tool(_name, _args):
        time.sleep(0.05)
        return {"data": {"late": True}}

    monkeypatch.setattr(stream, "call_jato_mcp_tool", slow_tool)

    with pytest.raises(TimeoutError, match="query_country_snapshot timed out"):
        asyncio.run(
            stream._call_tool_with_timeout(
                "query_country_snapshot",
                {"country": "Hungary"},
                timeout_seconds=0.001,
            )
        )


def test_stream_coverage_guard_adds_market_news_when_internal_market_evidence_is_empty() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "requiredTools": ["query_country_snapshot"],
        "allowedTools": ["query_country_snapshot", "build_market_chart", "search_market_news"],
        "toolPlan": [
            {
                "toolName": "query_country_snapshot",
                "input": {"country": "Hungary", "question": "匈牙利市场现在适合推 PHEV 还是 HEV？"},
            }
        ],
        "evidenceNeeded": [{"name": "market_kpis"}, {"name": "trend_or_mix"}],
    }
    tool_history = [
        {
            "tool": "query_country_snapshot",
            "arguments": {"country": "Hungary", "question": "匈牙利市场现在适合推 PHEV 还是 HEV？"},
            "result": {"data": {"kpis": {"totalRows": 0, "countryCount": 0, "modelCount": 0}}},
            "status": "ok",
            "round": 1,
        }
    ]

    assert stream._coverage_guard_tools(
        evidence_plan,
        tool_history,
        allowed_tools=evidence_plan["allowedTools"],
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
    ) == ["search_market_news"]


def test_stream_iterative_coverage_guard_continues_after_guard_snapshot_is_empty() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "requiredTools": ["query_country_snapshot"],
        "allowedTools": ["query_country_snapshot", "build_market_chart", "search_market_news"],
        "toolPlan": [
            {
                "toolName": "query_country_snapshot",
                "input": {"country": "Hungary", "question": "匈牙利市场现在适合推 PHEV 还是 HEV？"},
            }
        ],
        "evidenceNeeded": [{"name": "market_kpis"}, {"name": "trend_or_mix"}],
    }
    seen: set[str] = set()

    first_tool = stream._next_coverage_guard_tool(
        evidence_plan,
        [],
        allowed_tools=evidence_plan["allowedTools"],
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
        seen_tools=seen,
    )
    seen.add(first_tool)
    tool_history = [
        {
            "tool": first_tool,
            "arguments": {"country": "Hungary", "question": "匈牙利市场现在适合推 PHEV 还是 HEV？"},
            "result": {"data": {"kpis": {"totalRows": 0, "countryCount": 0, "modelCount": 0}}},
            "status": "ok",
            "round": 1,
        }
    ]

    second_tool = stream._next_coverage_guard_tool(
        evidence_plan,
        tool_history,
        allowed_tools=evidence_plan["allowedTools"],
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
        seen_tools=seen,
    )

    assert first_tool == "query_country_snapshot"
    assert second_tool == "search_market_news"


def test_stream_coverage_guard_adds_market_news_when_snapshot_only_has_technical_counts() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "requiredTools": ["query_country_snapshot"],
        "allowedTools": ["query_country_snapshot", "build_market_chart", "search_market_news"],
        "toolPlan": [
            {
                "toolName": "query_country_snapshot",
                "input": {"country": "Hungary", "question": "匈牙利市场现在适合推 PHEV 还是 HEV？"},
            }
        ],
        "evidenceNeeded": [{"name": "market_kpis"}, {"name": "trend_or_mix"}],
    }
    tool_history = [
        {
            "tool": "query_country_snapshot",
            "arguments": {"country": "Hungary", "question": "匈牙利市场现在适合推 PHEV 还是 HEV？"},
            "result": {
                "data": {
                    "kpis": {
                        "totalRows": 12,
                        "countryCount": 1,
                        "modelCount": 4,
                    }
                }
            },
            "status": "ok",
            "round": 1,
        }
    ]

    probe_package = build_evidence_package(
        session_id="coverage_probe_test",
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
        evidence_plan=evidence_plan,
        tool_results=[
            {
                "toolName": "query_country_snapshot",
                "query": tool_history[0]["arguments"],
                "result": tool_history[0]["result"],
                "success": True,
            }
        ],
    )

    assert evidence_ref_count(probe_package) == 0
    assert stream._coverage_guard_tools(
        evidence_plan,
        tool_history,
        allowed_tools=evidence_plan["allowedTools"],
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
    ) == ["search_market_news"]


def test_stream_coverage_guard_skips_external_research_when_internal_market_evidence_is_usable() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "requiredTools": ["query_country_snapshot"],
        "allowedTools": ["query_country_snapshot", "build_market_chart", "external_research"],
        "toolPlan": [
            {
                "toolName": "query_country_snapshot",
                "input": {"country": "Sweden", "question": "瑞典 HEV 市场为什么适合 J7？"},
            }
        ],
        "evidenceNeeded": [{"name": "market_kpis"}, {"name": "trend_or_mix"}],
    }
    tool_history = [
        {
            "tool": "query_country_snapshot",
            "arguments": {"country": "Sweden", "question": "瑞典 HEV 市场为什么适合 J7？"},
            "result": {"data": {"kpis": {"market_share": 12, "sales_volume": 22816}}},
            "status": "ok",
            "round": 1,
        }
    ]

    assert stream._coverage_guard_tools(
        evidence_plan,
        tool_history,
        allowed_tools=evidence_plan["allowedTools"],
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
    ) == []


def test_stream_coverage_guard_adds_backup_research_when_market_news_is_weak() -> None:
    evidence_plan = {
        "intent": "news_policy_search",
        "requiredTools": ["search_market_news"],
        "allowedTools": ["search_market_news", "pageindex_search_documents", "minirag_query_graph"],
        "toolPlan": [
            {
                "toolName": "search_market_news",
                "input": {"country": "Sweden", "question": "Elbilspremien 2026 会影响哪些车型？"},
            }
        ],
        "evidenceNeeded": [{"name": "fresh_external_signal"}, {"name": "business_impact"}],
    }
    tool_history = [
        {
            "tool": "search_market_news",
            "arguments": {"country": "Sweden", "question": "Elbilspremien 2026 会影响哪些车型？"},
            "result": {
                "data": {
                    "status": "empty",
                    "items": [],
                    "citations": [],
                    "summary": "No citation-ready claims returned.",
                }
            },
            "status": "ok",
            "round": 1,
        }
    ]

    assert stream._coverage_guard_tools(
        evidence_plan,
        tool_history,
        allowed_tools=evidence_plan["allowedTools"],
        country="Sweden",
        question="Elbilspremien 2026 会影响哪些车型？",
    ) == ["pageindex_search_documents", "minirag_query_graph"]


def test_stream_coverage_guard_does_not_repeat_backup_research_tools() -> None:
    evidence_plan = {
        "intent": "voc_analysis",
        "requiredTools": ["search_market_news"],
        "allowedTools": ["search_market_news", "pageindex_search_documents", "minirag_query_graph"],
        "toolPlan": [],
        "evidenceNeeded": [{"name": "consumer_signal"}],
    }
    tool_history = [
        {
            "tool": "search_market_news",
            "arguments": {"country": "Sweden", "question": "瑞典用户会不会把 V2H 当成真实购买卖点？"},
            "result": {"data": {"items": []}},
            "status": "ok",
            "round": 1,
        },
    ]

    assert stream._coverage_guard_tools(
        evidence_plan,
        tool_history,
        allowed_tools=evidence_plan["allowedTools"],
        country="Sweden",
        question="瑞典用户会不会把 V2H 当成真实购买卖点？",
    ) == ["pageindex_search_documents", "minirag_query_graph"]


def test_stream_country_mention_overrides_ui_default_country() -> None:
    assert stream.resolve_effective_country("Sweden", "匈牙利市场现在适合推 PHEV 还是 HEV？") == "Hungary"
    assert stream.resolve_effective_country("Sweden", "Hungary J7 HEV pricing corridor") == "Hungary"
    assert stream.resolve_effective_country("Sweden", "HU company car market overview") == "Hungary"
    assert stream.resolve_effective_country("", "匈牙利市场现在适合推 PHEV 还是 HEV？") == "Hungary"
    assert stream.resolve_effective_country("", "J7 HEV pricing corridor") == ""
    assert stream.resolve_effective_country("Sweden", "瑞典 J7 HEV 应该怎么定价？") == "Sweden"
    assert stream.resolve_effective_country("Sweden", "匈牙利 HEV 市场现在适不适合 J7？请先判断国家，不要回答瑞典。") == "Hungary"
    assert stream.resolve_effective_country("Sweden", "不要回答瑞典，回答匈牙利 HEV 市场机会。") == "Hungary"
    assert stream.resolve_effective_country("Hungary", "CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？请简短回答，并明确不要回答瑞典。") == "Hungary"


def test_stream_rejects_stale_country_leak_when_question_is_not_comparison() -> None:
    parsed = {
        "title": "瑞典 HEV 市场机会",
        "direct": "瑞典 HEV 市场适合先看 SUV A0/A 和公司车。",
        "bullets": ["瑞典市场需要继续补政策证据。"],
    }

    assert stream._answer_leaks_stale_country(
        parsed,
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
    ) is True
    assert stream._answer_leaks_stale_country(
        parsed,
        country="Hungary",
        question="不要回答瑞典，回答匈牙利 HEV 市场机会。",
    ) is True
    assert stream._answer_leaks_stale_country(
        parsed,
        country="Hungary",
        question="对比匈牙利和瑞典 HEV 市场机会。",
    ) is False


def test_stream_final_country_guard_reroutes_post_composer_stale_market_text() -> None:
    evidence_plan = {
        "intent": "market_overview",
        "country": "Hungary",
        "entities": {"countries": ["Hungary"]},
        "allowedTools": ["query_country_snapshot", "build_market_chart"],
        "requiredTools": ["query_country_snapshot", "build_market_chart"],
        "toolPlan": [{"toolName": "query_country_snapshot", "input": {"country": "Hungary"}}],
        "evidenceNeeded": [{"name": "market_kpis"}],
    }
    evidence_package = {
        "evidenceId": "ev_hu_final_guard",
        "intent": "market_overview",
        "country": "Hungary",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "query_country_snapshot",
                "query": {"country": "Hungary"},
                "success": True,
                "rowCount": 8,
                "sourceType": "jato_parquet",
                "summary": "Hungary market snapshot returned HEV/SUV evidence.",
                "keyFindings": ["Hungary HEV SUV demand is visible in the sample."],
                "evidenceRefs": [
                    {
                        "refId": "hu_market_1",
                        "label": "Hungary SUV A HEV share",
                        "value": 38.2,
                        "unit": "%",
                        "source": "jato_snapshot",
                    }
                ],
            }
        ],
        "missingEvidence": [],
    }
    stale_after_composer = {
        "title": "瑞典 HEV 市场机会",
        "direct": "瑞典 HEV 市场适合先看 SUV A0/A 和公司车。",
        "bullets": ["瑞典市场需要继续补政策证据。"],
    }

    guarded = stream._apply_final_country_guard(
        stale_after_composer,
        country="Hungary",
        question="匈牙利市场现在适合推 PHEV 还是 HEV？不要回答瑞典。",
        tool_call_history=[{"tool": "query_country_snapshot", "status": "ok"}],
        evidence_plan=evidence_plan,
        evidence_package=evidence_package,
    )
    visible = stream._visible_answer_text(guarded)

    assert "瑞典" not in visible
    assert "Hungary" in visible or "匈牙利" in visible
    assert guarded["evidencePackage"]["country"] == "Hungary"


def test_stream_local_fallback_subject_removes_negated_country_instruction() -> None:
    subject = stream._read_evidence_subject(
        "Hungary",
        "匈牙利市场现在适合推 PHEV 还是 HEV？请不要回答瑞典。",
        {"entities": {}},
    )

    assert subject == "Hungary 相关问题：匈牙利市场现在适合推 PHEV 还是 HEV"
    assert "瑞典" not in subject
    assert "请" not in subject


def test_stream_coerces_model_tool_country_args_to_effective_country() -> None:
    args = stream.coerce_tool_country_args(
        {"country": "Sweden", "market": "Sweden", "model": "J7 HEV"},
        "Hungary",
        "匈牙利 J7 HEV 怎么定价？",
    )

    assert args["country"] == "Hungary"
    assert args["market"] == "Hungary"
    assert args["model"] == "J7 HEV"
    assert args["question"] == "匈牙利 J7 HEV 怎么定价？"


def test_stream_merges_planned_tool_args_before_execution() -> None:
    evidence_plan = {
        "toolPlan": [
            {
                "toolName": "external_research",
                "input": {
                    "country": "Sweden",
                    "question": "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
                    "brands": ["OMODA", "JAECOO"],
                    "features": ["V2H", "冬季包"],
                    "featureKeywords": "V2H 冬季包",
                    "intent": "voc_analysis",
                    "research_mode": "standard",
                },
            }
        ]
    }

    args = stream.merge_tool_args_with_evidence_plan(
        {"country": "Finland", "brands": ["OMODA"], "features": ["V2H"]},
        "external_research",
        country="Sweden",
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        evidence_plan=evidence_plan,
    )

    assert args["country"] == "Sweden"
    assert args["question"] == "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？"
    assert args["brands"] == ["OMODA", "JAECOO"]
    assert args["features"] == ["V2H", "冬季包"]
    assert args["featureKeywords"] == "V2H 冬季包"
    assert args["intent"] == "voc_analysis"
    assert args["research_mode"] == "standard"


def test_stream_merges_competitor_plan_into_minimal_tool_args() -> None:
    evidence_plan = {
        "toolPlan": [
            {
                "toolName": "query_msrp_pricing",
                "input": {
                    "country": "Hungary",
                    "question": "匈牙利 T7 HEV 应该对标 Corolla Cross 还是 Tucson？",
                    "models": ["T7 HEV", "Corolla Cross", "Tucson"],
                    "model": "T7 HEV",
                    "competitors": ["Corolla Cross", "Tucson"],
                },
            }
        ]
    }

    args = stream.merge_tool_args_with_evidence_plan(
        {
            "country": "Sweden",
            "model": "T7 HEV",
            "models": ["T7 HEV"],
            "competitors": ["Corolla Cross"],
        },
        "query_msrp_pricing",
        country="Hungary",
        question="匈牙利 T7 HEV 应该对标 Corolla Cross 还是 Tucson？",
        evidence_plan=evidence_plan,
    )

    assert args["country"] == "Hungary"
    assert args["model"] == "T7 HEV"
    assert args["models"] == ["T7 HEV", "Corolla Cross", "Tucson"]
    assert args["competitors"] == ["Corolla Cross", "Tucson"]


def test_current_request_context_marks_previous_country_as_background_only() -> None:
    context = stream.build_current_request_context(
        "Hungary",
        "Sweden",
        "Previous conversation:\nUser: 瑞典 J7 HEV 应该怎么定价？\nAssistant: Sweden market summary.",
    )

    assert context.startswith("Current request market/country: Hungary.")
    assert "UI requested country was Sweden" in context
    assert "use Hungary" in context
    assert "Previous conversation below is background only" in context
    assert context.index("Current request market/country: Hungary.") < context.index("Sweden market summary")


def test_stream_tool_history_citations_preserve_research_source_quality() -> None:
    citations = stream._tool_history_citations(
        [
            {
                "tool": "external_research",
                "result": {
                    "data": {
                        "items": [
                            {
                                "title": "Sweden EV policy update",
                                "source": "example.com",
                                "url": "https://example.com/policy",
                                "citationId": "R1",
                                "sourceScore": 91,
                                "sourceTier": "high",
                            }
                        ]
                    }
                },
            }
        ],
        {},
    )

    assert citations == [
        {
            "label": "[R1] Sweden EV policy update",
            "source": "example.com",
            "tool": "external_research",
            "citationId": "R1",
            "url": "https://example.com/policy",
            "sourceScore": 91,
            "sourceTier": "high",
        }
    ]


def test_stream_tool_history_citations_filter_low_relevance_policy_news() -> None:
    citations = stream._tool_history_citations(
        [
            {
                "tool": "search_market_news",
                "result": {
                    "data": {
                        "citations": [
                            {
                                "title": "EV maker Polestar's quarterly sales volumes slide amid US market ban - Reuters",
                                "source": "reuters.com",
                                "url": "https://www.reuters.com/business/autos-transportation/ev-maker-polestars-quarterly-sales-volumes-slide-amid-us-market-ban-2026-07-09/",
                                "citationId": "R1",
                                "sourceScore": 82,
                            },
                            {
                                "title": "Elbilspremien 2026 eligibility and price cap",
                                "source": "example.se",
                                "url": "https://example.se/elbilspremien-2026",
                                "citationId": "R2",
                                "sourceScore": 76,
                            },
                        ]
                    }
                },
            }
        ],
        {},
        intent="report_generation",
        question="Elbilspremien 2026 会影响哪些车型？请给出来源、JATO 数据交叉验证和一页汇报结构。",
    )

    assert len(citations) == 1
    assert citations[0]["source"] == "example.se"
    assert "polestar" not in str(citations).casefold()


def test_stream_parse_answer_handles_final_answer_bold_json() -> None:
    parsed = stream._parse_answer(
        'FINAL_ANSWER:\n**{"title": "Analysis", "direct": "瑞典 BEV 份额上升", "bullets": ["证据 A"], "limitations": []}**'
    )

    assert parsed["title"] == "Analysis"
    assert parsed["direct"] == "瑞典 BEV 份额上升"
    assert parsed["bullets"] == ["证据 A"]


def test_stream_parse_answer_uses_last_final_answer_marker() -> None:
    parsed = stream._parse_answer(
        """FORMAT EXAMPLE:
FINAL_ANSWER:
{"title": "..."}
FINAL_ANSWER:
{"title": "Actual", "direct": "最终答案", "bullets": [], "limitations": []}"""
    )

    assert parsed["title"] == "Actual"
    assert parsed["direct"] == "最终答案"


def test_stream_parse_answer_preserves_braces_inside_json_string() -> None:
    parsed = stream._parse_answer(
        '模型输出：{"title": "Analysis", "direct": "含有 {braces} 的回答", "bullets": [], "limitations": []}谢谢'
    )

    assert parsed["title"] == "Analysis"
    assert parsed["direct"] == "含有 {braces} 的回答"


def test_stream_parse_answer_prefers_answer_object_over_tool_args_without_marker() -> None:
    parsed = stream._parse_answer(
        """TOOL: query_country_snapshot
ARGS: {"country": "Sweden", "question": "BEV market"}
REASON: need market data
{"title": "Actual", "direct": "流式输出里应选择最终答案 JSON。", "bullets": ["A"], "limitations": []}"""
    )

    assert parsed["title"] == "Actual"
    assert parsed["direct"] == "流式输出里应选择最终答案 JSON。"
    assert "country" not in parsed


def test_stream_parse_answer_accepts_trailing_comma_json() -> None:
    parsed = stream._parse_answer(
        """FINAL_ANSWER:
{
  "title": "Trailing comma",
  "direct": "尾逗号不会泄漏原始文本。",
  "bullets": [],
  "limitations": [],
}"""
    )

    assert parsed["title"] == "Trailing comma"
    assert parsed["direct"] == "尾逗号不会泄漏原始文本。"


def test_stream_llm_yields_each_provider_sse_line(monkeypatch) -> None:
    class FakeResponse:
        def __init__(self) -> None:
            self.lines = iter([
                b"data: " + json.dumps({"choices": [{"delta": {"content": "A"}}]}).encode("utf-8") + b"\n",
                b"data: " + json.dumps({"choices": [{"delta": {"content": "B"}}]}).encode("utf-8") + b"\n",
                b"data: [DONE]\n",
            ])

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def readline(self) -> bytes:
            return next(self.lines, b"")

    monkeypatch.setattr(stream, "urlopen", lambda *_args, **_kwargs: FakeResponse())

    async def collect_tokens() -> list[str]:
        return [token async for token in stream._stream_llm([], "key")]

    assert asyncio.run(collect_tokens()) == ["A", "B"]


def test_stream_final_answer_chunks_preserve_text_for_visible_sse() -> None:
    text = (
        "直接结论：瑞典 J7 HEV 定价应围绕核心竞争带中段和高配主推展开。"
        "低配做价格锚点，高配做主推版本。"
        "下一步应补齐官方 MSRP 和竞品价格走廊。"
    )

    chunks = stream._chunk_text_for_sse(text, target_chars=18)

    assert len(chunks) > 1
    assert "".join(chunks) == text


def test_stream_final_answer_sse_emits_multiple_token_frames() -> None:
    text = (
        "直接结论：瑞典 J7 HEV 定价应围绕核心竞争带中段和高配主推展开。"
        "低配做价格锚点，高配做主推版本。"
    )

    async def collect_events() -> list[dict[str, object]]:
        frames = [frame async for frame in stream._stream_final_answer_sse(text, delay_seconds=0)]
        events: list[dict[str, object]] = []
        for frame in frames:
            assert frame.startswith("data: ")
            events.append(json.loads(frame.split("data: ", 1)[1]))
        return events

    events = asyncio.run(collect_events())

    assert len(events) > 1
    assert events[0]["_event"] == "answer_start"
    assert events[0]["chunkCount"] == len(events) - 1
    assert {event["_event"] for event in events[1:]} == {"token"}
    assert "".join(str(event["text"]) for event in events[1:]) == text


def test_stream_parse_answer_fallback_filters_control_lines() -> None:
    parsed = stream._parse_answer(
        """TOOL: query_country_snapshot
ARGS: {"country": "Sweden"}
REASON: need market data
FINAL_ANSWER:
工具没有返回足够证据，无法确认结论。"""
    )

    assert parsed["direct"] == "工具没有返回足够证据，无法确认结论。"
    assert parsed["bullets"] == []
    assert parsed["limitations"] == []
    assert "TOOL:" not in parsed["direct"]
    assert "ARGS:" not in parsed["direct"]


def test_stream_final_answer_fallback_rejects_control_protocol_only() -> None:
    parsed = stream._parse_answer(
        """TOOL: compare_competitive_set
ARGS: {"country": "Sweden"}
REASON: need competitor corridor"""
    )

    assert stream._answer_requires_local_fallback(
        "TOOL: compare_competitive_set\nARGS: {}",
        parsed,
    ) is True

    fallback = stream._build_local_final_answer(
        country="Sweden",
        question="瑞典 J7 HEV 应该如何定价？",
        tool_call_history=[
            {
                "tool": "query_msrp_pricing",
                "round": 1,
                "status": "ok",
                "hasData": True,
            }
        ],
        evidence_plan={
            "intent": "pricing_analysis",
            "allowedTools": ["query_msrp_pricing", "compare_competitive_set"],
            "entities": {"models": ["J7 HEV"]},
            "evidenceNeeded": [{"name": "current_msrp"}, {"name": "price_corridor"}],
        },
    )

    assert "TOOL:" not in fallback["direct"]
    assert fallback["title"] == "Sweden 的 J7 HEV · 定价分析"
    assert "query_msrp_pricing" not in fallback["direct"]
    assert "最终合成" not in " ".join(fallback.get("limitations", []))
    visible_bullets = " ".join(fallback["bullets"])
    assert "竞品池/价格走廊" in visible_bullets
    assert "compare_competitive_set" not in visible_bullets
    assert any("不能编造" in item for item in fallback["limitations"])


def test_local_final_fallback_uses_retrieved_facts_not_evidence_plan_prose() -> None:
    fallback = stream._build_local_final_answer(
        country="Sweden",
        question="瑞典 HEV 市场为什么适合 J7？",
        tool_call_history=[{"tool": "query_country_snapshot", "status": "ok", "hasData": True}],
        evidence_plan={
            "intent": "market_overview",
            "allowedTools": ["query_country_snapshot", "compare_competitive_set"],
            "entities": {"models": ["J7"]},
            "evidenceNeeded": [{"name": "market_kpis"}],
        },
        evidence_package={
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units"},
                        {"label": "contextSnapshot.powertrainMix.HEV.share", "value": "7.3%", "unit": "%"},
                    ],
                }
            ]
        },
    )

    assert "已按 market_overview 证据计划" not in fallback["direct"]
    assert "HEV 销量 5051 units" in fallback["direct"]
    assert "HEV 占比 7.3%" in fallback["direct"]
    assert "工具计划" not in fallback["direct"]


def test_local_pricing_fallback_labels_reference_samples_without_claiming_model_msrp() -> None:
    fallback = stream._build_local_final_answer(
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？",
        tool_call_history=[{"tool": "query_price_positioning", "status": "ok", "hasData": True}],
        evidence_plan={
            "intent": "pricing_analysis",
            "allowedTools": ["query_price_positioning"],
            "entities": {"models": ["J7 HEV"]},
            "evidenceNeeded": [{"name": "current_msrp"}],
        },
        evidence_package={
            "toolResults": [
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"label": "priceStats.min", "value": 39121.74},
                        {"label": "priceStats.max", "value": 53165.22},
                    ],
                }
            ]
        },
    )

    assert "瑞典 J7 HEV" in fallback["direct"]
    assert "市场参考价格样本最低值 39121.74" in fallback["direct"]
    assert "市场参考价格样本最高值 53165.22" in fallback["direct"]
    assert "不是本车型或核心竞品的当前官方 MSRP" in fallback["direct"]
    assert "priceStats" not in fallback["direct"]


def test_local_competitor_fallback_skips_metadata_and_zero_sales_before_real_competitor_evidence() -> None:
    fallback = stream._build_local_final_answer(
        country="Sweden",
        question="J8 7 座四驱为什么能打 Sorento？",
        tool_call_history=[{"tool": "compare_competitive_set", "status": "ok", "hasData": True}],
        evidence_plan={
            "intent": "competitor_compare",
            "allowedTools": ["compare_competitive_set"],
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "evidenceNeeded": [{"name": "competitor_pool"}],
        },
        evidence_package={
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"label": "row_count", "value": 1},
                        {"label": "competitor.1.model", "value": "Sorento"},
                        {"label": "competitor.1.sales", "value": 0, "unit": "units"},
                        {"label": "Sorento.priceEvidenceStatus", "value": "candidate_search_query"},
                        {"label": "Sorento.currentPriceRows", "value": 0, "unit": "units"},
                        {"label": "Sorento.priceEvidenceRole", "value": "current_price"},
                        {"label": "Sorento.sales", "value": 309, "unit": "units"},
                        {"label": "contextSnapshot.crossTabs.driveBySegment.SUV B.4WD_pct", "value": 65.9, "unit": "%"},
                    ],
                }
            ]
        },
    )

    assert "瑞典 J8" in fallback["direct"]
    assert "Sorento 销量 309 units" in fallback["direct"]
    assert "row count" not in fallback["direct"]
    assert "0 units" not in fallback["direct"]
    assert "competitor / 1 / model" not in fallback["direct"]
    assert "candidate_search_query" not in fallback["direct"]
    assert "currentPriceRows" not in fallback["direct"]
