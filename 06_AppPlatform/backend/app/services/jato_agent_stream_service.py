from __future__ import annotations

import asyncio
import json
import os
import re
import time
from typing import Any, AsyncGenerator
from urllib.request import Request, urlopen

from app.core.config import ASTRBOT_PROVIDER_API_BASE
from app.core.config import ASTRBOT_PROVIDER_KEY_ENV
from app.core.config import ASTRBOT_PROVIDER_MODEL
from app.services.jato_agent_deterministic_judge_service import score_deterministic_answer
from app.services.jato_agent_llm_judge_service import judge_answer_with_llm
from app.services.jato_agent_planning_service import build_evidence_plan
from app.services.jato_agent_provider_service import parse_agent_answer_content
from app.services.jato_answer_grounding_service import apply_answer_grounding_guard
from app.services.jato_business_playbook_service import build_business_playbook_context
from app.services.jato_country_resolution_service import COUNTRY_CODE_ALIASES
from app.services.jato_country_resolution_service import COUNTRY_MENTION_ALIASES
from app.services.jato_country_resolution_service import canonical_country
from app.services.jato_country_resolution_service import extract_country_mention as shared_extract_country_mention
from app.services.jato_country_resolution_service import resolve_effective_country as shared_resolve_effective_country
from app.services.jato_evidence_package_service import build_evidence_package
from app.services.jato_evidence_package_service import evidence_ref_count
from app.services.jato_evidence_package_service import is_usable_evidence_ref
from app.services.jato_followup_service import normalize_follow_ups
from app.services.jato_followup_service import serialize_follow_ups
from app.services.jato_mcp_tools_service import call_jato_mcp_tool
from app.services.jato_research_governance_service import filter_relevant_research_sources
from app.services.jato_tool_registry_service import filter_tool_descriptors_for_allowed
from app.services.jato_tool_coverage_guard_service import missing_required_tools
from app.services.jato_tool_coverage_guard_service import required_tool_args
from app.services.jato_tool_coverage_guard_service import tool_satisfies_required
from app.services.jato_usage_tracker import estimate_tool_call_tokens
from app.services.jato_usage_tracker import track_followup_impression
from app.services.jato_usage_tracker import track_followup_next_answer
from app.services.jato_usage_tracker import track_tool_call_event
from app.services.jato_visual_artifact_service import build_visual_artifacts


_FINAL_ANSWER_STREAM_CHARS = 10
_FINAL_ANSWER_STREAM_DELAY_SECONDS = 0.04
_TOOL_SELECTION_HEARTBEAT_SECONDS = 1.0
_TOOL_SELECTION_PROVIDER_TIMEOUT_SECONDS = 6
_TOOL_SELECTION_HARD_TIMEOUT_SECONDS = _TOOL_SELECTION_PROVIDER_TIMEOUT_SECONDS + 2
_TOOL_EXECUTION_TIMEOUT_SECONDS = 20
_MAX_COVERAGE_GUARD_TOOLS = 3
_STREAM_BREAK_CHARS = set("。！？；.!?;\n")
_TOOL_ARG_LIST_KEYS = {"models", "competitors", "brands", "features", "countries", "powertrains", "segments"}


def resolve_effective_country(requested_country: str, question: str) -> str:
    """Prefer an explicit country mention in the user question over UI defaults."""
    return shared_resolve_effective_country(requested_country, question)


def extract_country_mention(question: str) -> str:
    return shared_extract_country_mention(question)


def coerce_tool_country_args(args: dict[str, Any], country: str, question: str) -> dict[str, Any]:
    """Keep tool execution aligned with the effective market, not stale chat defaults."""
    next_args = dict(args) if isinstance(args, dict) else {}
    next_args["country"] = country
    if "market" in next_args:
        next_args["market"] = country
    if "question" not in next_args:
        next_args["question"] = question
    return next_args


def merge_tool_args_with_evidence_plan(
    args: dict[str, Any],
    tool_name: str,
    *,
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
) -> dict[str, Any]:
    """Fill missing tool args from EvidencePlan without letting stale args override market scope."""
    planned_args = required_tool_args(
        evidence_plan,
        tool_name,
        country=country,
        question=question,
    )
    merged = dict(planned_args)
    for key, value in (args if isinstance(args, dict) else {}).items():
        if _is_empty_tool_arg_value(value) and key in merged:
            continue
        if key in _TOOL_ARG_LIST_KEYS:
            merged[key] = _merge_tool_arg_list_values(merged.get(key), value)
            continue
        merged[key] = value
    return coerce_tool_country_args(merged, country, question)


def _merge_tool_arg_list_values(planned_value: Any, runtime_value: Any) -> list[str]:
    return _dedupe([*_tool_arg_list_items(planned_value), *_tool_arg_list_items(runtime_value)])


def _tool_arg_list_items(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item or "").strip()]
    return []


def _is_empty_tool_arg_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def build_current_request_context(country: str, requested_country: str, conv_context: str) -> str:
    """Make the current market override stale UI/session context before the LLM sees history."""
    current_country = str(country or "").strip() or "selected market"
    requested = str(requested_country or "").strip()
    lines = [
        f"Current request market/country: {current_country}.",
        "This current market/country overrides UI defaults and previous conversation history unless the user explicitly asks for a comparison.",
    ]
    if requested and requested != current_country:
        lines.append(
            f"UI requested country was {requested}, but the user question explicitly mentions {current_country}; use {current_country}."
        )
    if str(conv_context or "").strip():
        lines.extend([
            "",
            "Previous conversation below is background only. Do not reuse a prior country, market, model or evidence scope when it conflicts with the current request:",
            str(conv_context).strip(),
        ])
    return "\n".join(lines)


def _alias_position(alias: str, text: str, lower_text: str) -> int:
    if any("\u4e00" <= char <= "\u9fff" for char in alias):
        return text.find(alias)
    if len(alias) <= 3:
        match = re.search(rf"(?<![A-Za-z]){re.escape(alias)}(?![A-Za-z])", text, flags=re.IGNORECASE)
        return match.start() if match else -1
    return lower_text.find(alias.lower())


def _is_negated_country_mention(text: str, start: int) -> bool:
    prefix = text[max(0, start - 24):start].casefold()
    prefix = re.split(r"[，,。.;；!！?？]", prefix)[-1]
    negation_markers = (
        "不要回答",
        "别回答",
        "不是",
        "不要用",
        "别用",
        "not ",
        "not-",
        "do not",
        "don't",
        "dont",
        "not about",
    )
    return any(marker in prefix for marker in negation_markers)


async def stream_agent_response(
    country: str,
    question: str,
    max_rounds: int = 3,
    session_id: str = "",
    requested_mode: str = "auto",
    research_mode: str = "standard",
    skill_id: str = "",
    source_followup: dict[str, Any] | None = None,
) -> AsyncGenerator[str, None]:
    """SSE streaming agent: emit thinking → tool_calls → tokens → done.

    If session_id is provided, injects conversation history as context
    and saves turns after completion.
    """
    from app.services.jato_conversation_store import add_turn, create_session, get_context

    api_key = os.getenv(ASTRBOT_PROVIDER_KEY_ENV, "").strip()
    if not api_key:
        yield _sse("error", {"message": "DEEPSEEK_API_KEY not configured"})
        return

    requested_country = str(country or "").strip()
    country = resolve_effective_country(requested_country, question)

    # Session management
    sid = session_id.strip() if session_id else create_session()
    conv_context = get_context(sid) if session_id else ""

    # Save user turn
    add_turn(sid, "user", question)

    planning_question = _planning_question_for_mode(question, requested_mode)
    evidence_plan = build_evidence_plan(country, planning_question)
    business_playbook = build_business_playbook_context(
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    )
    allowed_tools = [
        str(tool)
        for tool in evidence_plan.get("allowedTools", [])
        if str(tool or "").strip()
    ]
    early_draft_streamed = False
    active_tools = filter_tool_descriptors_for_allowed(_active_tools(), allowed_tools)
    tools_desc = "\n".join(
        f"- {t['name']}: {t['description']}" for t in active_tools
    )

    current_request_context = build_current_request_context(country, requested_country, conv_context)
    yield _sse("thinking", {
        "round": 0,
        "message": f"Resolved current market: {country}. Building evidence plan and tool route...",
        "country": country,
        "requestedCountry": requested_country,
    })

    system = (
        "You are a JATO automotive market analyst. You MUST call tools to get real data. "
        f"{current_request_context}\n\n"
        "NEVER fabricate numbers. Follow this exact workflow:\n"
        "1. Call a tool using TOOL: <name>\\nARGS: {...}\\nREASON: ...\n"
        "2. Read the result\n"
        "3. Call another tool if needed (max 3 total)\n"
        "4. Give FINAL_ANSWER as JSON: {\\\"title\\\":...,\\\"direct\\\":...,\\\"bullets\\\":[...],\\\"limitations\\\":[...],\\\"followUps\\\":[...]}\n"
        "For direct, write a natural business conclusion first. Use the two or three most decision-relevant facts returned by tools; do not describe tools that already ran. "
        "Do not use boilerplate headings such as '直接结论', '证据状态', or '下一步执行'. Put unresolved evidence only in limitations, and keep next actions in followUps.\n\n"
        "When Business Playbook is present, follow its requiredSections and decisionFrame. "
        "If evidence is insufficient, do not stop at 'insufficient evidence'; explain what can still be judged, what evidence is missing, how the gap affects the conclusion, and which tool/data action comes next.\n"
        "The followUps field must contain 2-4 concrete next-step business paths. Prefer structured objects with label, question, intent, reason, expectedTools, expectedOutput, priority.\n"
        f"Evidence Plan: {json.dumps(evidence_plan, ensure_ascii=False)}\n"
        f"Business Playbook: {json.dumps(business_playbook, ensure_ascii=False)}\n"
        f"You may only call tools listed in this Evidence Plan allowedTools: {', '.join(allowed_tools)}.\n\n"
        "If the user asks to read or summarize a URL, call read_web_page with ARGS containing url and question. "
        "read_web_page is static and cannot click, type, log in, or submit forms.\n\n"
        "If the user asks for research, external sources, citations, or current web evidence without a specific URL, call search_market_news. "
        "Treat it as governed public-source search and report when source coverage is insufficient.\n\n"
        "If the user asks for a browser snapshot, rendered page, DOM, or screenshot of a URL, call browser_snapshot. "
        "browser_snapshot is read-only and cannot click, type, log in, or submit forms.\n\n"
        f"Available tools:\n{tools_desc}"
    )

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": f"Country: {country}\nQuestion: {question}"},
    ]

    tool_call_history: list[dict[str, Any]] = []

    for round_num in range(1, max_rounds + 1):
        yield _sse("thinking", {"round": round_num, "message": f"Analyzing and deciding next action (round {round_num}/{max_rounds})..."})

        content = _required_tool_selection_content(
            evidence_plan,
            tool_call_history,
            allowed_tools=allowed_tools,
            country=country,
            question=question,
        )
        if content:
            yield _sse("thinking", {
                "round": round_num,
                "message": "Using required Evidence Plan tool before optional research.",
            })
        else:
            # Get LLM response (non-streaming for tool selection)
            try:
                tool_selection_task = asyncio.create_task(
                    asyncio.to_thread(
                        _call_llm,
                        messages,
                        api_key,
                        max_tokens=400,
                        timeout_seconds=_TOOL_SELECTION_PROVIDER_TIMEOUT_SECONDS,
                    )
                )
                heartbeat_count = 0
                selection_started_at = time.perf_counter()
                while not tool_selection_task.done():
                    await asyncio.wait({tool_selection_task}, timeout=_TOOL_SELECTION_HEARTBEAT_SECONDS)
                    if tool_selection_task.done():
                        break
                    heartbeat_count += 1
                    yield _sse("thinking", {
                        "round": round_num,
                        "message": f"Selecting next evidence tool... {heartbeat_count}s",
                    })
                    if time.perf_counter() - selection_started_at >= _TOOL_SELECTION_HARD_TIMEOUT_SECONDS:
                        tool_selection_task.cancel()
                        raise TimeoutError(
                            f"tool selection timed out after {_TOOL_SELECTION_HARD_TIMEOUT_SECONDS}s"
                        )
                content = await tool_selection_task
            except Exception as exc:
                content = _fallback_tool_selection_content(
                    evidence_plan,
                    tool_call_history,
                    allowed_tools=allowed_tools,
                    country=country,
                    question=question,
                    error=exc,
                )
                if not content:
                    yield _sse("tool_error", {
                        "tool": "llm_tool_selection",
                        "error": f"Tool selection failed and no Evidence Plan fallback is available: {str(exc)[:160]}",
                        "round": round_num,
                    })
                    break
                yield _sse("thinking", {
                    "round": round_num,
                    "message": "Tool selection provider failed; using Evidence Plan fallback.",
                })
        messages.append({"role": "assistant", "content": content})

        # Parse tool call
        tool_match = re.search(r"TOOL:\s*(\S+)", content)
        args_match = re.search(r"ARGS:\s*(\{.*?\})", content, re.DOTALL)
        reason_match = re.search(r"REASON:\s*(.+?)(?:\n|$)", content)

        if tool_match and not ("FINAL_ANSWER:" in content and not tool_call_history):
            tool_name = tool_match.group(1).strip()
            try:
                args = json.loads(args_match.group(1)) if args_match else {"country": country, "question": question}
            except json.JSONDecodeError:
                args = {"country": country, "question": question}
            args = merge_tool_args_with_evidence_plan(
                args,
                tool_name,
                country=country,
                question=question,
                evidence_plan=evidence_plan,
            )
            reason = reason_match.group(1).strip()[:200] if reason_match else "agent-selected"

            if allowed_tools and tool_name not in allowed_tools:
                yield _sse("tool_error", {
                    "tool": tool_name,
                    "error": f"Tool is not allowed by Evidence Plan. Allowed: {', '.join(allowed_tools)}",
                    "round": round_num,
                })
                messages.append({"role": "user", "content": f"Tool {tool_name} is not allowed by the Evidence Plan. Choose one of: {', '.join(allowed_tools)}."})
                continue

            yield _sse("tool_call", {"tool": tool_name, "reason": reason, "round": round_num})

            try:
                started_at = time.perf_counter()
                result = await _call_tool_with_timeout(tool_name, args)
                latency_ms = int((time.perf_counter() - started_at) * 1000)
                result_summary = _summarize_for_sse(result)
                # Auto-generate chart specs from any snapshot/chart tool result
                chart_specs = _extract_chart_specs(result) if tool_name in ("build_market_chart", "query_country_snapshot", "query_with_filters", "query_powertrain_trend", "query_brand_deep_dive") else None
                if not chart_specs and tool_name == "query_country_snapshot":
                    # Generate charts from raw snapshot data
                    chart_specs = _build_charts_from_snapshot(result)
                yield _sse("tool_result", {"tool": tool_name, "summary": result_summary, "round": round_num})
                token_estimate = estimate_tool_call_tokens(tool_name, args, result)
                track_tool_call_event(
                    session_id=sid,
                    country=country,
                    question=question,
                    tool_name=tool_name,
                    arguments=args,
                    latency_ms=latency_ms,
                    success=True,
                    cost_estimate=token_estimate,
                )
                tool_call_history.append({
                    "tool": tool_name,
                    "arguments": args,
                    "result": result,
                    "reason": reason,
                    "round": round_num,
                    "status": "ok",
                    "hasData": bool(result.get("data")),
                    "summary": result_summary,
                    "chartCount": len(chart_specs or []),
                    "chartSpecs": chart_specs,
                })
                messages.append({"role": "user", "content": f"TOOL RESULT ({tool_name}):\n{json.dumps(result_summary, ensure_ascii=False)}"})
                if _pre_final_answer_streaming_enabled() and not early_draft_streamed:
                    early_draft = _build_early_evidence_draft(
                        session_id=sid,
                        country=country,
                        question=question,
                        tool_call_history=tool_call_history,
                        evidence_plan=evidence_plan,
                    )
                    early_text = _visible_stream_answer_text(early_draft) if early_draft else ""
                    if early_text:
                        early_draft_streamed = True
                        yield _sse("thinking", {"round": round_num, "message": "Streaming first evidence-backed draft while remaining tools continue..."})
                        async for event in _stream_final_answer_sse(early_text, delay_seconds=0.02):
                            yield event
            except Exception as exc:
                yield _sse("tool_error", {"tool": tool_name, "error": str(exc)[:200]})
                track_tool_call_event(
                    session_id=sid,
                    country=country,
                    question=question,
                    tool_name=tool_name,
                    arguments=args,
                    latency_ms=0,
                    success=False,
                    error=str(exc),
                )
                tool_call_history.append({
                    "tool": tool_name,
                    "arguments": args,
                    "result": {},
                    "reason": reason,
                    "round": round_num,
                    "status": "error",
                    "error": str(exc)[:200],
                    "hasData": False,
                    "chartCount": 0,
                })
                messages.append({"role": "user", "content": f"TOOL ERROR ({tool_name}): {exc}"})
        elif "FINAL_ANSWER:" in content:
            break
        elif not tool_call_history:
            # First round with no tool call — force it
            messages.append({"role": "user", "content": "You MUST call a TOOL first. Format: TOOL: tool_name\\nARGS: {...}"})
            continue
        else:
            break

    coverage_guard_attempts = 0
    coverage_guard_seen: set[str] = set()
    while coverage_guard_attempts < _MAX_COVERAGE_GUARD_TOOLS:
        tool_name = _next_coverage_guard_tool(
            evidence_plan,
            tool_call_history,
            allowed_tools=allowed_tools,
            country=country,
            question=question,
            seen_tools=coverage_guard_seen,
        )
        if not tool_name:
            break
        coverage_guard_seen.add(tool_name)
        coverage_guard_attempts += 1
        args = required_tool_args(evidence_plan, tool_name, country=country, question=question)
        reason = "tool_coverage_guard"
        round_num = len(tool_call_history) + 1
        yield _sse("thinking", {
            "round": round_num,
            "message": f"Tool coverage guard is filling required evidence with {tool_name}...",
        })
        yield _sse("tool_call", {"tool": tool_name, "reason": reason, "round": round_num})
        try:
            started_at = time.perf_counter()
            result = await _call_tool_with_timeout(tool_name, args)
            latency_ms = int((time.perf_counter() - started_at) * 1000)
            result_summary = _summarize_for_sse(result)
            chart_specs = _extract_chart_specs(result) if tool_name in ("build_market_chart", "query_country_snapshot", "query_with_filters", "query_powertrain_trend", "query_brand_deep_dive") else None
            if not chart_specs and tool_name == "query_country_snapshot":
                chart_specs = _build_charts_from_snapshot(result)
            yield _sse("tool_result", {"tool": tool_name, "summary": result_summary, "round": round_num})
            token_estimate = estimate_tool_call_tokens(tool_name, args, result)
            track_tool_call_event(
                session_id=sid,
                country=country,
                question=question,
                tool_name=tool_name,
                arguments=args,
                latency_ms=latency_ms,
                success=True,
                cost_estimate=token_estimate,
            )
            tool_call_history.append({
                "tool": tool_name,
                "arguments": args,
                "result": result,
                "reason": reason,
                "round": round_num,
                "status": "ok",
                "hasData": bool(result.get("data")),
                "summary": result_summary,
                "chartCount": len(chart_specs or []),
                "chartSpecs": chart_specs,
            })
            messages.append({"role": "user", "content": f"TOOL RESULT ({tool_name}):\n{json.dumps(result_summary, ensure_ascii=False)}"})
            if _pre_final_answer_streaming_enabled() and not early_draft_streamed:
                early_draft = _build_early_evidence_draft(
                    session_id=sid,
                    country=country,
                    question=question,
                    tool_call_history=tool_call_history,
                    evidence_plan=evidence_plan,
                )
                early_text = _visible_stream_answer_text(early_draft) if early_draft else ""
                if early_text:
                    early_draft_streamed = True
                    yield _sse("thinking", {"round": round_num, "message": "Streaming first evidence-backed draft while remaining tools continue..."})
                    async for event in _stream_final_answer_sse(early_text, delay_seconds=0.02):
                        yield event
        except Exception as exc:
            yield _sse("tool_error", {"tool": tool_name, "error": str(exc)[:200], "round": round_num})
            track_tool_call_event(
                session_id=sid,
                country=country,
                question=question,
                tool_name=tool_name,
                arguments=args,
                latency_ms=0,
                success=False,
                error=str(exc),
            )
            tool_call_history.append({
                "tool": tool_name,
                "arguments": args,
                "result": {},
                "reason": reason,
                "round": round_num,
                "status": "error",
                "error": str(exc)[:200],
                "hasData": False,
                "chartCount": 0,
            })
            messages.append({"role": "user", "content": f"TOOL ERROR ({tool_name}): {exc}"})

    evidence_package = build_evidence_package(
        session_id=sid,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        tool_results=[
            {
                "toolName": str(item.get("tool") or ""),
                "query": item.get("arguments") if isinstance(item.get("arguments"), dict) else {},
                "result": item.get("result") if isinstance(item.get("result"), dict) else {},
                "success": item.get("status") == "ok",
                "error": str(item.get("error") or ""),
            }
            for item in tool_call_history
        ],
    )
    draft_answer = _build_grounded_local_draft(
        country=country,
        question=question,
        tool_call_history=tool_call_history,
        evidence_plan=evidence_plan,
        evidence_package=evidence_package,
    )
    draft_text = _visible_stream_answer_text(draft_answer)
    if _pre_final_answer_streaming_enabled() and draft_text and not early_draft_streamed:
        yield _sse("thinking", {"message": "Writing evidence-backed draft from retrieved data..."})
        async for event in _stream_final_answer_sse(draft_text, delay_seconds=0.03):
            yield event

    # Final answer must not inherit ReAct tool-selection messages. Those traces
    # include diagnostic summaries (for example, a zero model filter result)
    # that are useful for planning but can distort a user-facing conclusion.
    # The final composer receives only its checked EvidencePackage.
    yield _sse("thinking", {"message": "Refining final answer with grounding guard..."})
    final_messages = _final_answer_messages(
        evidence_package=evidence_package,
        evidence_plan=evidence_plan,
        question=question,
    )

    full_answer = ""
    provider_refinement_failed = False
    try:
        async for token in _stream_llm(final_messages, api_key, max_tokens=800):
            full_answer += token
    except Exception:
        # Fallback: non-streaming
        try:
            full_answer = _call_llm(final_messages, api_key, max_tokens=800)
        except Exception:
            provider_refinement_failed = True

    # Parse final answer
    if provider_refinement_failed:
        parsed = dict(draft_answer)
        fallback_limitations = [
            str(item).strip()
            for item in parsed.get("limitations", [])
            if str(item).strip()
        ] if isinstance(parsed.get("limitations"), list) else []
        fallback_notice = "模型精修暂时不可用，当前答案直接基于已返回工具证据和证据边界生成。"
        if fallback_notice not in fallback_limitations:
            fallback_limitations.append(fallback_notice)
        parsed["limitations"] = fallback_limitations
    else:
        parsed = _parse_answer(full_answer)
    if (
        not provider_refinement_failed
        and (_answer_requires_local_fallback(full_answer, parsed) or _answer_leaks_stale_country(parsed, country=country, question=question))
    ):
        parsed = _build_local_final_answer(
            country=country,
            question=question,
            tool_call_history=tool_call_history,
            evidence_plan=evidence_plan,
            evidence_package=evidence_package,
        )
    parsed = apply_answer_grounding_guard(
        parsed,
        evidence_package,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    )
    parsed = _apply_final_country_guard(
        parsed,
        country=country,
        question=question,
        tool_call_history=tool_call_history,
        evidence_plan=evidence_plan,
        evidence_package=evidence_package,
    )
    if provider_refinement_failed:
        fallback_limitations = [
            str(item).strip()
            for item in parsed.get("limitations", [])
            if str(item).strip()
        ] if isinstance(parsed.get("limitations"), list) else []
        fallback_notice = "模型精修暂时不可用，当前答案直接基于已返回工具证据和证据边界生成。"
        if fallback_notice not in fallback_limitations:
            fallback_limitations.append(fallback_notice)
        parsed["limitations"] = fallback_limitations
    evidence_package = (
        parsed.get("evidencePackage")
        if isinstance(parsed.get("evidencePackage"), dict)
        else evidence_package
    )
    assistant_text = parsed.get("direct", full_answer[:500])
    if isinstance(assistant_text, str) and assistant_text.strip():
        async for event in _stream_final_answer_sse(assistant_text):
            yield event
    structured_follow_ups = normalize_follow_ups(
        parsed.get("followUps"),
        country=country,
        question=question,
        tools=[t["tool"] for t in tool_call_history],
        evidence_plan=evidence_plan,
        evidence_package=evidence_package,
    )
    follow_ups = serialize_follow_ups(structured_follow_ups)

    # Collect chart specs from tool results for live response and history replay.
    charts_data: list[dict[str, Any]] = []
    for tc in tool_call_history:
        if tc.get("chartSpecs"):
            charts_data.extend(tc["chartSpecs"])
    charts_data = charts_data[:4]
    visual_artifacts = build_visual_artifacts(
        question=question,
        answer=parsed,
        evidence_package=evidence_package,
        charts=charts_data,
    )
    missing_evidence = (
        evidence_package.get("missingEvidence")
        if isinstance(evidence_package.get("missingEvidence"), list)
        else []
    )
    answer_summary = str(parsed.get("summary") or assistant_text or "").strip()
    key_takeaways = _read_string_list(parsed.get("keyTakeaways")) or _read_string_list(parsed.get("bullets"))
    pm_insight = str(parsed.get("pmInsight") or _pm_insight_from_answer(parsed) or "").strip()
    answer_status = str(parsed.get("status") or parsed.get("answerStatus") or "answered")
    citations = _tool_history_citations(
        tool_call_history,
        parsed,
        intent=str(evidence_plan.get("intent") or ""),
        question=question,
    )
    developer_trace = _build_developer_trace(country, question, tool_call_history, structured_follow_ups, evidence_plan)
    quality_score = score_deterministic_answer(
        expected={
            "expectedIntent": evidence_plan.get("intent"),
            "mustUseTools": evidence_plan.get("requiredTools", []),
            "expectedFollowUpTypes": evidence_plan.get("followUpTypes", []),
        },
        predicted_intent=str(evidence_plan.get("intent") or ""),
        tools_used=[t["tool"] for t in tool_call_history],
        answer=parsed,
        evidence_package=evidence_package,
        follow_ups=structured_follow_ups,
    )
    llm_quality_score = judge_answer_with_llm(
        question=question,
        answer=parsed,
        evidence_package=evidence_package,
        follow_ups=structured_follow_ups,
    )
    try:
        track_followup_impression(
            session_id=sid,
            country=country,
            question=question,
            follow_ups=structured_follow_ups,
            intent=str(evidence_plan.get("intent") or ""),
        )
    except Exception:
        pass
    if source_followup:
        try:
            track_followup_next_answer(
                session_id=sid,
                country=country,
                follow_up=source_followup,
                next_intent=str(evidence_plan.get("intent") or ""),
                next_tools_used=[t["tool"] for t in tool_call_history],
                next_answer_success=quality_score["totalScore"] >= 0.6,
                next_answer_eval_score=float(quality_score["totalScore"]),
            )
        except Exception:
            pass

    answer_payload = {
        "title": parsed.get("title", "Analysis"),
        "direct": assistant_text,
        "intent": str(evidence_plan.get("intent") or evidence_package.get("intent") or ""),
        "bullets": parsed.get("bullets", []),
        "limitations": parsed.get("limitations", []),
        "citations": citations,
        "followUps": follow_ups,
        "structuredFollowUps": structured_follow_ups,
        "summary": answer_summary,
        "evidenceBackedLead": parsed.get("evidenceBackedLead", ""),
        "keyTakeaways": key_takeaways,
        "pmInsight": pm_insight,
        "visualArtifacts": visual_artifacts,
        "toolCalls": [t["tool"] for t in tool_call_history],
        "toolCount": len(tool_call_history),
        "charts": charts_data,
        "developerTrace": developer_trace,
        "evidencePlan": evidence_plan,
        "evidencePackage": evidence_package,
        "missingEvidence": missing_evidence,
        "qualityScore": quality_score,
        "llmQualityScore": llm_quality_score,
        "businessSynthesisPlan": parsed.get("businessSynthesisPlan", {}),
        "methodDistillation": parsed.get("methodDistillation", {}),
        "recommendedActions": parsed.get("recommendedActions", []),
        "reportReadyBullets": parsed.get("reportReadyBullets", []),
        "businessImplications": parsed.get("businessImplications", []),
        "confidence": parsed.get("confidence", evidence_package.get("confidence", "medium")),
        "status": answer_status,
        "answerStatus": answer_status,
        "grounding": parsed.get("grounding", {}),
        "sourceCount": len(citations),
        "tool": tool_call_history[-1]["tool"] if tool_call_history else "",
    }

    # Save assistant turn to conversation memory
    add_turn(sid, "assistant", assistant_text, {
        "country": country,
        "requestedCountry": requested_country,
        "intent": answer_payload["intent"],
        "answerTitle": answer_payload["title"],
        "bullets": answer_payload["bullets"],
        "limitations": answer_payload["limitations"],
        "citations": answer_payload["citations"],
        "followUps": answer_payload["followUps"],
        "structuredFollowUps": answer_payload["structuredFollowUps"],
        "summary": answer_payload["summary"],
        "evidenceBackedLead": answer_payload["evidenceBackedLead"],
        "keyTakeaways": answer_payload["keyTakeaways"],
        "pmInsight": answer_payload["pmInsight"],
        "visualArtifacts": answer_payload["visualArtifacts"],
        "toolCalls": answer_payload["toolCalls"],
        "toolCount": answer_payload["toolCount"],
        "chartCount": len(charts_data),
        "charts": answer_payload["charts"],
        "developerTrace": answer_payload["developerTrace"],
        "evidencePlan": answer_payload["evidencePlan"],
        "evidencePackage": answer_payload["evidencePackage"],
        "missingEvidence": answer_payload["missingEvidence"],
        "qualityScore": answer_payload["qualityScore"],
        "llmQualityScore": answer_payload["llmQualityScore"],
        "businessSynthesisPlan": answer_payload["businessSynthesisPlan"],
        "methodDistillation": answer_payload["methodDistillation"],
        "recommendedActions": answer_payload["recommendedActions"],
        "reportReadyBullets": answer_payload["reportReadyBullets"],
        "businessImplications": answer_payload["businessImplications"],
        "confidence": answer_payload["confidence"],
        "status": answer_payload["status"],
        "answerStatus": answer_payload["answerStatus"],
        "grounding": answer_payload["grounding"],
    })

    yield _sse("done", {
        "sessionId": sid,
        "country": country,
        "requestedCountry": requested_country,
        **answer_payload,
        "answer": answer_payload,
    })


# ── SSE helpers ──

def _sse(event: str, data: Any) -> str:
    """Emit SSE with event type embedded in data JSON for reliable parsing."""
    wrapped = {"_event": event, **(data if isinstance(data, dict) else {"value": data})}
    return f"data: {json.dumps(wrapped, ensure_ascii=False)}\n\n"


def _chunk_text_for_sse(text: str, *, target_chars: int = _FINAL_ANSWER_STREAM_CHARS) -> list[str]:
    """Split a governed final answer into visible SSE chunks without changing text."""
    if not text:
        return []
    target = max(8, target_chars)
    min_break = max(8, target // 2)
    max_break = max(target * 2, target + 8)
    chunks: list[str] = []
    current: list[str] = []
    for char in text:
        current.append(char)
        length = len(current)
        should_break = (
            (length >= target and (char.isspace() or char in _STREAM_BREAK_CHARS))
            or (length >= min_break and char == "\n")
            or length >= max_break
        )
        if should_break:
            chunks.append("".join(current))
            current = []
    if current:
        chunks.append("".join(current))
    return chunks


async def _stream_final_answer_sse(
    text: str,
    *,
    delay_seconds: float = _FINAL_ANSWER_STREAM_DELAY_SECONDS,
) -> AsyncGenerator[str, None]:
    """Emit the final grounded answer as multiple token frames for UI-visible streaming."""
    chunks = _chunk_text_for_sse(text)
    if not chunks:
        return
    yield _sse("answer_start", {"textLength": len(text), "chunkCount": len(chunks)})
    for chunk in chunks:
        if not chunk:
            continue
        yield _sse("token", {"text": chunk})
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)


async def _call_tool_with_timeout(
    tool_name: str,
    args: dict[str, Any],
    *,
    timeout_seconds: float = _TOOL_EXECUTION_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(call_jato_mcp_tool, tool_name, args),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        raise TimeoutError(f"{tool_name} timed out after {timeout_seconds:g}s") from exc
    return result if isinstance(result, dict) else {"data": result}


# ── LLM calls ──

def _call_llm(
    messages: list[dict[str, Any]],
    api_key: str,
    max_tokens: int = 400,
    timeout_seconds: float = 30,
) -> str:
    payload = {"model": ASTRBOT_PROVIDER_MODEL, "messages": messages, "temperature": 0.1, "max_tokens": max_tokens, "stream": False}
    req = Request(
        f"{ASTRBOT_PROVIDER_API_BASE.rstrip('/')}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urlopen(req, timeout=timeout_seconds) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        return body["choices"][0]["message"]["content"]


async def _stream_llm(messages: list[dict[str, Any]], api_key: str, max_tokens: int = 800) -> AsyncGenerator[str, None]:
    """Stream tokens from DeepSeek API."""
    payload = {"model": ASTRBOT_PROVIDER_MODEL, "messages": messages, "temperature": 0.2, "max_tokens": max_tokens, "stream": True}
    req = Request(
        f"{ASTRBOT_PROVIDER_API_BASE.rstrip('/')}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urlopen(req, timeout=60) as resp:
        while True:
            line = resp.readline()
            if not line:
                break
            line = line.strip()
            if not line or not line.startswith(b"data: "):
                continue
            data_str = line[6:].decode("utf-8", errors="ignore")
            if data_str == "[DONE]":
                return
            try:
                data = json.loads(data_str)
                choices = data.get("choices", [])
                if choices and choices[0].get("delta", {}).get("content"):
                    yield choices[0]["delta"]["content"]
            except json.JSONDecodeError:
                continue


# ── Tools ──

def _active_tools() -> list[dict[str, Any]]:
    return [
        {"name": "analyze_model_performance", "description": "Cross-reference: sales + pricing + variants + news for one model. Use for 'why X sells well'."},
        {"name": "compare_competitive_set", "description": "Compare model vs segment competitors on sales, price, features."},
        {"name": "analyze_market_dynamics", "description": "Market changes: trends + news + pricing cross-referenced."},
        {"name": "query_country_snapshot", "description": "Country KPIs, brand/model rankings, powertrain mix, sales trends."},
        {"name": "build_market_chart", "description": "Chart-ready market data with trend series. Returns chart specs."},
        {"name": "query_msrp_pricing", "description": "MSRP pricing records for specific models."},
        {"name": "compare_vehicle_variants", "description": "Variant/trim config differences."},
        {"name": "search_market_news", "description": "News, policy updates, consumer sentiment."},
        {"name": "read_web_page", "description": "Read a public HTTP/HTTPS page as static text. Requires url. No JavaScript, login, clicks, or forms."},
        {"name": "browser_snapshot", "description": "Read-only browser snapshot of a public URL when Playwright is available. Requires url. No clicks, typing, login, or forms."},
    ]


def _fallback_tool_selection_content(
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
    *,
    allowed_tools: list[str],
    country: str,
    question: str,
    error: Exception,
) -> str:
    """Use the deterministic Evidence Plan when provider tool-selection stalls."""
    selected = _next_evidence_plan_tool(
        evidence_plan,
        tool_call_history,
        allowed_tools=allowed_tools,
        country=country,
        question=question,
    )
    if not selected:
        return ""
    tool_name, args = selected
    reason = f"evidence_plan_fallback_after_{type(error).__name__}"
    return "\n".join([
        f"TOOL: {tool_name}",
        f"ARGS: {json.dumps(args, ensure_ascii=False)}",
        f"REASON: {reason}",
    ])


def _required_tool_selection_content(
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
    *,
    allowed_tools: list[str],
    country: str,
    question: str,
) -> str:
    selected = _next_required_evidence_plan_tool(
        evidence_plan,
        tool_call_history,
        allowed_tools=allowed_tools,
        country=country,
        question=question,
    )
    if not selected:
        return ""
    tool_name, args = selected
    return "\n".join([
        f"TOOL: {tool_name}",
        f"ARGS: {json.dumps(args, ensure_ascii=False)}",
        "REASON: required_evidence_plan_tool_before_optional_research",
    ])


def _next_required_evidence_plan_tool(
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
    *,
    allowed_tools: list[str],
    country: str,
    question: str,
) -> tuple[str, dict[str, Any]] | None:
    executed_tools = [str(item.get("tool") or "") for item in tool_call_history]
    missing = missing_required_tools(
        evidence_plan,
        executed_tools,
        allowed_tools=allowed_tools,
    )
    if not missing:
        return None
    tool_name = str(missing[0] or "").strip()
    if not tool_name:
        return None
    args = required_tool_args(
        evidence_plan,
        tool_name,
        country=country,
        question=question,
    )
    return tool_name, coerce_tool_country_args(args, country, question)


def _next_evidence_plan_tool(
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
    *,
    allowed_tools: list[str],
    country: str,
    question: str,
) -> tuple[str, dict[str, Any]] | None:
    executed = {str(item.get("tool") or "").strip() for item in tool_call_history}
    allowed = {str(tool or "").strip() for tool in allowed_tools if str(tool or "").strip()}
    candidates: list[str] = []
    for item in evidence_plan.get("toolPlan", []):
        if isinstance(item, dict):
            candidates.append(str(item.get("toolName") or "").strip())
    for key in ("requiredTools", "allowedTools"):
        for tool_name in evidence_plan.get(key, []):
            candidates.append(str(tool_name or "").strip())

    for tool_name in _dedupe(candidates):
        if not tool_name or tool_name in executed:
            continue
        if allowed and tool_name not in allowed:
            continue
        args = required_tool_args(
            evidence_plan,
            tool_name,
            country=country,
            question=question,
        )
        return tool_name, coerce_tool_country_args(args, country, question)
    return None


def _coverage_guard_tools(
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
    *,
    allowed_tools: list[str],
    country: str = "",
    question: str = "",
) -> list[str]:
    """Required-tool coverage is a quality gate, not part of the free ReAct budget."""
    executed_tools = [str(item.get("tool") or "") for item in tool_call_history]
    required = missing_required_tools(
        evidence_plan,
        executed_tools,
        allowed_tools=allowed_tools,
    )
    repair = _external_research_repair_tools(
        evidence_plan,
        tool_call_history,
        allowed_tools=allowed_tools,
        country=country,
        question=question,
    )
    return _dedupe([*required, *repair])


def _next_coverage_guard_tool(
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
    *,
    allowed_tools: list[str],
    country: str = "",
    question: str = "",
    seen_tools: set[str] | None = None,
) -> str:
    seen = {str(tool or "").strip() for tool in (seen_tools or set())}
    for tool_name in _coverage_guard_tools(
        evidence_plan,
        tool_call_history,
        allowed_tools=allowed_tools,
        country=country,
        question=question,
    ):
        normalized = str(tool_name or "").strip()
        if normalized and normalized not in seen:
            return normalized
    return ""


_EXTERNAL_RESEARCH_REPAIR_INTENTS = {
    "market_overview",
    "pricing_analysis",
    "competitor_compare",
    "configuration_analysis",
    "voc_analysis",
    "news_policy_search",
    "report_generation",
}


def _external_research_repair_tools(
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
    *,
    allowed_tools: list[str],
    country: str,
    question: str,
) -> list[str]:
    """Add source-backed research when governed internal tools produced no usable evidence."""
    allowed = {str(tool or "").strip() for tool in allowed_tools}
    if "search_market_news" not in allowed:
        return []
    intent = str(evidence_plan.get("intent") or "").strip()
    if intent not in _EXTERNAL_RESEARCH_REPAIR_INTENTS:
        return []
    executed_tools = [str(item.get("tool") or "").strip() for item in tool_call_history]
    source_backed_tools_executed = [
        tool
        for tool in executed_tools
        if any(tool_satisfies_required(required, tool) for required in ("search_market_news", "pageindex_search_documents", "minirag_query_graph"))
    ]
    if not _has_successful_internal_tool_attempt(tool_call_history):
        if source_backed_tools_executed:
            probe_package = _coverage_probe_package(
                evidence_plan=evidence_plan,
                tool_call_history=tool_call_history,
                country=country,
                question=question,
            )
            return _external_research_backup_tools(
                probe_package,
                allowed_tools=allowed_tools,
                executed_tools=executed_tools,
                question=question,
            )
        return []
    if source_backed_tools_executed:
        probe_package = _coverage_probe_package(
            evidence_plan=evidence_plan,
            tool_call_history=tool_call_history,
            country=country,
            question=question,
        )
        return _external_research_backup_tools(
            probe_package,
            allowed_tools=allowed_tools,
            executed_tools=executed_tools,
            question=question,
        )
    probe_package = build_evidence_package(
        session_id="coverage_guard_probe",
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        tool_results=[
            {
                "toolName": str(item.get("tool") or ""),
                "query": item.get("arguments") if isinstance(item.get("arguments"), dict) else {},
                "result": item.get("result") if isinstance(item.get("result"), dict) else {},
                "success": item.get("status") == "ok",
                "error": str(item.get("error") or ""),
            }
            for item in tool_call_history
        ],
    )
    if evidence_ref_count(probe_package) > 0:
        return []
    return ["search_market_news"]


def _coverage_probe_package(
    *,
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
    country: str,
    question: str,
) -> dict[str, Any]:
    return build_evidence_package(
        session_id="coverage_guard_probe",
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        tool_results=[
            {
                "toolName": str(item.get("tool") or ""),
                "query": item.get("arguments") if isinstance(item.get("arguments"), dict) else {},
                "result": item.get("result") if isinstance(item.get("result"), dict) else {},
                "success": item.get("status") == "ok",
                "error": str(item.get("error") or ""),
            }
            for item in tool_call_history
        ],
    )


def _external_research_backup_tools(
    probe_package: dict[str, Any],
    *,
    allowed_tools: list[str],
    executed_tools: list[str],
    question: str,
) -> list[str]:
    if not _needs_external_research_backup(probe_package):
        return []
    allowed = {str(tool or "").strip() for tool in allowed_tools}
    executed = {str(tool or "").strip() for tool in executed_tools}
    candidates = [
        "search_market_news",
        "pageindex_search_documents",
        "minirag_query_graph",
    ]
    if _extract_first_url(question):
        candidates.insert(0, "read_web_page")
    return [
        tool
        for tool in candidates
        if tool in allowed and tool not in executed
    ][:2]


def _needs_external_research_backup(probe_package: dict[str, Any]) -> bool:
    missing_names = {
        str(item.get("name") or "")
        for item in probe_package.get("missingEvidence", [])
        if isinstance(item, dict)
    }
    weak_source_gaps = {
        "external_research_claims_unavailable",
        "search_market_news_weak_evidence_refs",
        "pageindex_search_documents_weak_evidence_refs",
        "minirag_query_graph_weak_evidence_refs",
        "read_web_page_weak_evidence_refs",
    }
    return bool(missing_names & weak_source_gaps)


def _has_successful_internal_tool_attempt(tool_call_history: list[dict[str, Any]]) -> bool:
    for item in tool_call_history:
        if not isinstance(item, dict) or item.get("status") != "ok":
            continue
        tool = str(item.get("tool") or "").strip()
        if tool and not tool_satisfies_required("search_market_news", tool):
            return True
    return False


def _dedupe(items: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _extract_first_url(question: str) -> str:
    match = re.search(r"https?://[^\s)）]+", str(question or ""))
    return match.group(0) if match else ""


def _planning_question_for_mode(question: str, requested_mode: str) -> str:
    mode = str(requested_mode or "").strip().lower()
    if mode == "research":
        return f"{question}\n\nNeed external research with sources and citations."
    if mode == "news":
        return f"{question}\n\nNeed latest news, policy, sources, and citations."
    return question


def _summarize_for_sse(result: dict[str, Any]) -> dict[str, Any]:
    """Compact tool result for SSE display."""
    data = result.get("data", {}) if isinstance(result, dict) else {}
    if not isinstance(data, dict):
        return {"summary": str(result)[:300]}
    summary: dict[str, Any] = {}
    for key in ("kpis", "topModels", "topBrands", "powertrainMix"):
        val = data.get(key)
        if isinstance(val, list) and val:
            summary[key] = val[:5]
        elif isinstance(val, dict) and val:
            summary[key] = val
    items = data.get("items") or data.get("sections")
    if isinstance(items, list):
        summary["itemCount"] = len(items)
    findings = data.get("findings")
    if isinstance(findings, dict):
        summary["crossReferenceSources"] = list(findings.keys())
    dynamics = data.get("dynamics")
    if isinstance(dynamics, dict):
        summary["crossReferenceSources"] = list(dynamics.keys())
    if result.get("tool") in {"read_web_page", "browser_snapshot"}:
        summary["title"] = data.get("title", "")
        summary["status"] = data.get("status", "")
        summary["browserEngine"] = data.get("browserEngine", "")
        headings = data.get("headings") if isinstance(data.get("headings"), list) else []
        links = data.get("links") if isinstance(data.get("links"), list) else []
        summary["headingCount"] = len(headings)
        summary["linkCount"] = len(links)
        summary["textPreview"] = str(data.get("textPreview") or "")[:500]
    if not summary:
        summary["keys"] = list(data.keys())[:10]
    return summary


def _final_answer_instruction(
    *,
    evidence_package: dict[str, Any],
    evidence_plan: dict[str, Any],
    question: str,
) -> str:
    """Ask the provider to compose from checked evidence, not tool-call traces.

    The EvidencePackage remains compact enough for the final request while
    retaining the labels, values and source types required for a grounded
    business narrative.  It deliberately excludes diagnostics and raw payloads.
    """
    evidence_context = _compact_final_evidence_context(evidence_package, evidence_plan)
    return (
        "Write FINAL_ANSWER JSON now. Use the checked Evidence Package below as the only source for factual claims.\n"
        f"User question: {question}\n\n"
        "For direct: answer the business question first in natural Chinese, usually two compact paragraphs. "
        "State the conclusion, then the two or three decision-relevant facts and their implication. "
        "When the user asks an evidence-supported binary choice, lead with the selected option and explain it from both alternatives' evidence; "
        "do not replace that choice with a generic market-entry summary. "
        "Do not describe tools, evidence plans, agent steps, or generic next-step process. "
        "Do not use headings such as '直接结论', '证据状态', or '下一步执行'. "
        "Do not invent, estimate, or transform a number that is not present in the package. "
        "Do not infer a named model's segment, price, configuration, or commercial outcome unless that fact is explicitly present in the package. "
        "When a required fact is missing, say exactly which conclusion remains provisional and why; keep that boundary concise. "
        "Do not present user material, draft sources, or market samples as current official MSRP or verified competitor data.\n\n"
        "Return only JSON with title, direct, bullets, limitations and followUps. "
        "Bullets may add distinct evidence but must not repeat direct. Limitations must only contain real evidence gaps.\n\n"
        f"Checked Evidence Package:\n{json.dumps(evidence_context, ensure_ascii=False)}"
    )


def _final_answer_messages(
    *,
    evidence_package: dict[str, Any],
    evidence_plan: dict[str, Any],
    question: str,
) -> list[dict[str, str]]:
    """Isolate final composition from ReAct planning and raw tool summaries."""
    return [
        {
            "role": "system",
            "content": (
                "You are the final response composer for a governed automotive market analysis product. "
                "Return valid FINAL_ANSWER JSON only. The user-facing answer must be grounded in the checked "
                "Evidence Package supplied by the user message; ignore any knowledge not present there."
            ),
        },
        {
            "role": "user",
            "content": _final_answer_instruction(
                evidence_package=evidence_package,
                evidence_plan=evidence_plan,
                question=question,
            ),
        },
    ]


def _compact_final_evidence_context(
    evidence_package: dict[str, Any],
    evidence_plan: dict[str, Any],
) -> dict[str, Any]:
    """Return only business-safe evidence needed by the final composer."""
    tool_rows: list[dict[str, Any]] = []
    raw_tools = evidence_package.get("toolResults")
    tools = raw_tools if isinstance(raw_tools, list) else []
    for item in tools:
        if not isinstance(item, dict) or not item.get("success"):
            continue
        refs = item.get("evidenceRefs")
        compact_refs: list[dict[str, Any]] = []
        for ref in refs if isinstance(refs, list) else []:
            if not is_usable_evidence_ref(ref):
                continue
            label = str(ref.get("label") or "").strip()
            value = ref.get("value")
            if not label or value in (None, ""):
                continue
            compact_ref: dict[str, Any] = {
                "refId": str(ref.get("refId") or "").strip(),
                "label": _final_evidence_label(label)[:140],
                "value": value,
            }
            for key in ("unit", "source", "table", "evidenceStatus"):
                candidate = ref.get(key)
                if candidate not in (None, ""):
                    compact_ref[key] = str(candidate)[:180]
            compact_refs.append(compact_ref)
            if len(compact_refs) >= 8:
                break
        findings = [
            str(value).strip()[:220]
            for value in item.get("keyFindings", [])
            if _is_business_safe_final_finding(value)
        ] if isinstance(item.get("keyFindings"), list) else []
        if not compact_refs and not findings:
            continue
        tool_rows.append({
            "tool": str(item.get("toolName") or "").strip(),
            "sourceType": str(item.get("sourceType") or "").strip(),
            "findings": findings[:4],
            "refs": compact_refs,
        })
        if len(tool_rows) >= 4:
            break

    missing_rows = [
        {
            "name": str(item.get("name") or "").strip(),
            "reason": str(item.get("reason") or "").strip()[:240],
            "impact": str(item.get("impact") or "").strip(),
        }
        for item in evidence_package.get("missingEvidence", [])
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ][:5] if isinstance(evidence_package.get("missingEvidence"), list) else []

    return {
        "country": str(evidence_package.get("country") or evidence_plan.get("country") or "").strip(),
        "intent": str(evidence_package.get("intent") or evidence_plan.get("intent") or "").strip(),
        "entities": evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {},
        "confidence": str(evidence_package.get("confidence") or "").strip(),
        "evidence": tool_rows,
        "missingEvidence": missing_rows,
    }


def _is_business_safe_final_finding(value: Any) -> bool:
    """Exclude technical zero/row-count diagnostics from the final model context."""
    text = str(value or "").strip()
    if not text:
        return False
    return not bool(re.match(
        r"^(?:totalrows|countrycount|brandcount|modelcount|versioncount|row_count|result_count|chart_count)\s*:",
        text,
        flags=re.IGNORECASE,
    ))


def _final_evidence_label(label: str) -> str:
    """Turn stable evidence paths into readable labels without changing the fact."""
    raw = str(label or "").strip()
    match = re.match(r"(?:contextSnapshot\.)?crossTabs\.driveByFuel\.([^.]+)\.sales$", raw, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1).upper()} 动力销量"
    match = re.match(r"(?:contextSnapshot\.)?crossTabs\.driveByFuel\.([^.]+)\.(2WD|4WD|AWD)_pct$", raw, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1).upper()} {match.group(2).upper()} 占比"
    match = re.match(r"(?:contextSnapshot\.)?crossTabs\.driveBySegment\.([^.]+)\.sales$", raw, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 细分销量"
    match = re.match(r"(?:contextSnapshot\.)?crossTabs\.driveBySegment\.([^.]+)\.(2WD|4WD|AWD)_pct$", raw, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} 占比"
    return raw


def _tool_history_citations(
    tool_call_history: list[dict[str, Any]],
    parsed_answer: dict[str, Any],
    *,
    intent: str = "",
    question: str = "",
) -> list[dict[str, Any]]:
    citations = _normalized_citations(
        _filtered_citation_rows(parsed_answer.get("citations"), default_tool="", intent=intent, question=question),
        default_tool="",
    )
    if citations:
        return citations[:8]

    for call in tool_call_history:
        tool_name = str(call.get("tool") or "")
        result = call.get("result")
        if not isinstance(result, dict):
            continue
        data = result.get("data")
        if not isinstance(data, dict):
            continue
        citations.extend(
            _normalized_citations(
                _filtered_citation_rows(data.get("citations"), default_tool=tool_name, intent=intent, question=question),
                default_tool=tool_name,
            )
        )
        if not citations:
            citations.extend(
                _normalized_citations(
                    _filtered_citation_rows(data.get("items"), default_tool=tool_name, intent=intent, question=question),
                    default_tool=tool_name,
                )
            )
        if not citations:
            citations.extend(
                _normalized_citations(
                    _filtered_citation_rows(data.get("sections"), default_tool=tool_name, intent=intent, question=question),
                    default_tool=tool_name,
                )
            )
        if len(citations) >= 8:
            break
    return citations[:8]


def _filtered_citation_rows(value: Any, *, default_tool: str, intent: str, question: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    rows = [item for item in value if isinstance(item, dict)]
    if not rows:
        return []
    if not intent or not question:
        return rows
    source_tools = {"search_market_news", "read_web_page", "browser_snapshot", "pageindex_search_documents"}
    source_intents = {"news_policy_search", "report_generation", "pricing_analysis", "competitor_compare", "market_overview", "voc_analysis"}
    if default_tool not in source_tools and intent not in source_intents:
        return rows
    kept, _ = filter_relevant_research_sources(rows, intent=intent, question=question)
    return kept


def _normalized_citations(value: Any, *, default_tool: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    result: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        label = _citation_label(item)
        if not label:
            continue
        source = str(item.get("source") or item.get("provider") or "source").strip() or "source"
        citation: dict[str, Any] = {
            "label": label,
            "source": source,
            "tool": str(item.get("tool") or default_tool or "tool"),
        }
        citation_id = str(item.get("citationId") or "").strip()
        if citation_id:
            citation["citationId"] = citation_id
        url = item.get("url")
        if isinstance(url, str) and url.strip():
            citation["url"] = url.strip()
        score = item.get("sourceScore")
        if isinstance(score, int | float):
            citation["sourceScore"] = score
            tier = item.get("sourceTier")
            if isinstance(tier, str) and tier.strip():
                citation["sourceTier"] = tier.strip()
        for key in ("sourceTitle", "sourceCategory", "supportedClaim", "claimType"):
            text = item.get(key)
            if isinstance(text, str) and text.strip():
                citation[key] = text.strip()
        result.append(citation)
    return result


def _citation_label(item: dict[str, Any]) -> str:
    raw = item.get("label") or item.get("title") or item.get("name") or item.get("section") or item.get("model")
    label = str(raw or "").strip()
    if not label:
        return ""
    citation_id = str(item.get("citationId") or "").strip()
    if citation_id and not label.startswith("["):
        return f"[{citation_id}] {label[:160]}"
    return label[:180]


def _build_charts_from_snapshot(result: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Auto-generate Plotly charts from raw snapshot data when build_market_chart wasn't called."""
    data = result.get("data", {}) if isinstance(result, dict) else {}
    if not isinstance(data, dict):
        return None
    charts = []

    # Bar chart from top models
    top_models = data.get("topModels")
    if isinstance(top_models, list) and top_models:
        labels = [str(m.get("label", "")) for m in top_models[:10]]
        values = [float(m.get("value", 0)) for m in top_models[:10]]
        if labels and values:
            charts.append({
                "chartId": "auto_top_models",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": values, "y": labels, "type": "bar", "orientation": "h",
                          "marker": {"color": "#2563eb"},
                          "text": [f"{v:,.0f}" for v in values], "textposition": "outside"}],
                "layout": {"title": "Top Models", "height": 300, "margin": {"l": 140, "r": 20, "t": 40, "b": 50},
                           "xaxis": {"title": "Sales"}, "yaxis": {"autorange": "reversed"}},
            })

    # Pie chart from powertrain mix
    pm = data.get("powertrainMix")
    if isinstance(pm, list) and pm:
        labels = [str(p.get("label", "")) for p in pm[:8]]
        values = [float(p.get("value", 0)) for p in pm[:8]]
        if labels and values and sum(values) > 0:
            colors = ["#2563eb", "#0f766e", "#d97706", "#dc2626", "#7c3aed", "#db2777", "#0891b2", "#65a30d"]
            charts.append({
                "chartId": "auto_powertrain_mix",
                "chartType": "pie",
                "title": "Powertrain Mix",
                "data": [{"labels": labels, "values": values, "type": "pie", "hole": 0.45,
                          "marker": {"colors": colors[:len(labels)]}, "textinfo": "label+percent"}],
                "layout": {"title": "Powertrain Mix", "height": 350},
            })

    # Line chart from year series
    ys = data.get("yearSeries")
    if isinstance(ys, list) and ys:
        years = [str(y.get("time", y.get("year", ""))) for y in ys]
        vals = [float(y.get("value", 0)) for y in ys]
        if years and vals:
            charts.append({
                "chartId": "auto_year_trend",
                "chartType": "line",
                "title": "Yearly Trend",
                "data": [{"x": years, "y": vals, "type": "scatter", "mode": "lines+markers",
                          "line": {"color": "#2563eb", "width": 2}, "marker": {"size": 6}}],
                "layout": {"title": "Yearly Trend", "height": 320, "xaxis": {"title": "Year"}, "yaxis": {"title": "Sales"}},
            })

    return charts if charts else None


def _extract_chart_specs(result: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Extract Plotly chart specs from a tool result if available."""
    data = result.get("data", {}) if isinstance(result, dict) else {}
    if not isinstance(data, dict):
        return None
    chart_specs = data.get("chartSpecs")
    if isinstance(chart_specs, dict):
        charts = chart_specs.get("charts")
        if isinstance(charts, list) and charts:
            return [{"chartId": c.get("chartId", ""), "chartType": c.get("chartType", ""),
                     "title": c.get("title", ""), "data": c.get("data", []), "layout": c.get("layout", {})}
                    for c in charts[:4]]
    return None


def _parse_answer(content: str) -> dict[str, Any]:
    return parse_agent_answer_content(content)


def _answer_requires_local_fallback(raw_answer: str, parsed: dict[str, Any]) -> bool:
    direct = str(parsed.get("direct") or "").strip()
    if not direct:
        return True
    return _looks_like_control_protocol(direct) or _looks_like_control_protocol(raw_answer)


def _answer_leaks_stale_country(parsed: dict[str, Any], *, country: str, question: str) -> bool:
    """Guard against stale session/UI markets leaking into the visible answer."""
    expected_country = canonical_country(str(country or "").strip())
    if not expected_country:
        return False
    visible_text = _visible_answer_text(parsed)
    if not visible_text:
        return False
    stale_countries = {
        candidate
        for _alias, candidate in COUNTRY_MENTION_ALIASES
        if candidate and candidate != expected_country
    }
    for candidate in stale_countries:
        if not _text_mentions_country(visible_text, candidate):
            continue
        if _question_allows_country_mention(question, expected_country, candidate):
            continue
        return True
    return False


def _apply_final_country_guard(
    parsed: dict[str, Any],
    *,
    country: str,
    question: str,
    tool_call_history: list[dict[str, Any]],
    evidence_plan: dict[str, Any],
    evidence_package: dict[str, Any],
) -> dict[str, Any]:
    """Re-check visible text after grounding/composer rewrites before emitting done."""
    if not _answer_leaks_stale_country(parsed, country=country, question=question):
        return parsed
    fallback = _build_local_final_answer(
        country=country,
        question=question,
        tool_call_history=tool_call_history,
        evidence_plan=evidence_plan,
        evidence_package=evidence_package,
    )
    return apply_answer_grounding_guard(
        fallback,
        evidence_package,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    )


def _visible_answer_text(parsed: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("title", "direct", "summary", "pmInsight"):
        value = parsed.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value)
    for key in ("bullets", "limitations", "keyTakeaways", "businessImplications", "reportReadyBullets"):
        value = parsed.get(key)
        if isinstance(value, list):
            parts.extend(str(item) for item in value if str(item or "").strip())
    return "\n".join(parts)


def _question_allows_country_mention(question: str, expected_country: str, mentioned_country: str) -> bool:
    text = str(question or "")
    if not _text_mentions_country(text, mentioned_country):
        return False
    if not _text_mentions_country(text, expected_country):
        return False
    lowered = text.casefold()
    comparison_tokens = (
        "compare",
        "comparison",
        "versus",
        " vs ",
        "difference",
        "different",
        "对比",
        "比较",
        "相比",
        "相对",
        "差异",
        "区别",
        "和",
        "与",
    )
    negation_tokens = (
        "不要回答",
        "别回答",
        "不要用",
        "别用",
        "不是",
        "not about",
        "do not",
        "don't",
        "dont",
    )
    return any(token in lowered for token in comparison_tokens) and not any(token in lowered for token in negation_tokens)


def _text_mentions_country(text: str, country: str) -> bool:
    if not text or not country:
        return False
    lower_text = text.lower()
    for alias, candidate in COUNTRY_MENTION_ALIASES:
        if candidate == country and _alias_position(alias, text, lower_text) >= 0:
            return True
    for code, candidate in COUNTRY_CODE_ALIASES.items():
        if candidate != country:
            continue
        if re.search(rf"(?<![A-Za-z]){re.escape(code)}(?![A-Za-z])", text, flags=re.IGNORECASE):
            return True
    return False


def _looks_like_control_protocol(text: str) -> bool:
    compact = str(text or "").strip()
    if not compact:
        return False
    control_lines = 0
    content_lines = 0
    for raw_line in compact.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("TOOL:", "ARGS:", "REASON:")):
            control_lines += 1
        else:
            content_lines += 1
    return control_lines > 0 and content_lines == 0


def _build_stream_evidence_package(
    *,
    session_id: str,
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
) -> dict[str, Any]:
    return build_evidence_package(
        session_id=session_id,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        tool_results=[
            {
                "toolName": str(item.get("tool") or ""),
                "query": item.get("arguments") if isinstance(item.get("arguments"), dict) else {},
                "result": item.get("result") if isinstance(item.get("result"), dict) else {},
                "success": item.get("status") == "ok",
                "error": str(item.get("error") or ""),
            }
            for item in tool_call_history
        ],
    )


def _build_early_evidence_draft(
    *,
    session_id: str,
    country: str,
    question: str,
    tool_call_history: list[dict[str, Any]],
    evidence_plan: dict[str, Any],
) -> dict[str, Any] | None:
    evidence_package = _build_stream_evidence_package(
        session_id=session_id,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        tool_call_history=tool_call_history,
    )
    if not _should_stream_early_evidence_draft(
        evidence_package=evidence_package,
        evidence_plan=evidence_plan,
        tool_call_history=tool_call_history,
    ):
        return None
    return _build_grounded_local_draft(
        country=country,
        question=question,
        tool_call_history=tool_call_history,
        evidence_plan=evidence_plan,
        evidence_package=evidence_package,
    )


def _should_stream_early_evidence_draft(
    *,
    evidence_package: dict[str, Any],
    evidence_plan: dict[str, Any],
    tool_call_history: list[dict[str, Any]],
) -> bool:
    """Only stream an early answer when it already has business-usable evidence.

    The UI still receives thinking/tool events before this point. This guard keeps a
    gap-heavy first draft from appearing as a real answer and then being contradicted
    by the final evidence-backed answer a few seconds later.
    """
    if evidence_ref_count(evidence_package) <= 0:
        return False
    if not _has_stream_business_evidence_ref(evidence_package):
        return False
    required_tools = [
        str(tool or "").strip()
        for tool in evidence_plan.get("requiredTools", [])
        if str(tool or "").strip()
    ]
    if len(required_tools) > 1:
        executed = {
            str(item.get("tool") or "").strip()
            for item in tool_call_history
            if str(item.get("status") or "") == "ok" and str(item.get("tool") or "").strip()
        }
        required_executed = [tool for tool in required_tools if tool in executed]
        if len(required_executed) < min(2, len(required_tools)):
            return False
    missing = evidence_package.get("missingEvidence")
    missing_items = missing if isinstance(missing, list) else []
    blocking_or_weak = [
        item
        for item in missing_items
        if isinstance(item, dict) and str(item.get("impact") or "").strip() in {"blocking", "weakens_answer"}
    ]
    if blocking_or_weak and _stream_business_evidence_ref_count(evidence_package) < 2:
        return False
    return True


def _has_stream_business_evidence_ref(evidence_package: dict[str, Any]) -> bool:
    return _stream_business_evidence_ref_count(evidence_package) > 0


def _stream_business_evidence_ref_count(evidence_package: dict[str, Any]) -> int:
    count = 0
    for tool_result in evidence_package.get("toolResults", []):
        if not isinstance(tool_result, dict):
            continue
        for ref in tool_result.get("evidenceRefs", []):
            if isinstance(ref, dict) and _is_stream_business_evidence_ref(ref):
                count += 1
    return count


def _pre_final_answer_streaming_enabled() -> bool:
    value = os.getenv("APP_ASTRBOT_STREAM_PREFINAL_ANSWER", "").strip().casefold()
    return value in {"1", "true", "yes", "on"}


def _is_stream_business_evidence_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "")
    source = str(ref.get("source") or ref.get("table") or "")
    haystack = f"{label} {source}".casefold()
    if any(
        token in haystack
        for token in (
            "source_repair",
            "source repair",
            "source draft",
            "source_draft",
            "candidate",
            "review_pending",
            "review pending",
            "user supplied",
            "user material",
            "diagnostic",
            "missing",
            "coverage",
        )
    ):
        return False
    value = ref.get("value")
    if value in (None, ""):
        return False
    return True


def _build_local_final_answer(
    *,
    country: str,
    question: str,
    tool_call_history: list[dict[str, Any]],
    evidence_plan: dict[str, Any],
    evidence_package: dict[str, Any] | None = None,
) -> dict[str, Any]:
    intent = str(evidence_plan.get("intent") or "general_qa")
    executed_tools = [
        str(item.get("tool") or "")
        for item in tool_call_history
        if str(item.get("tool") or "").strip()
    ]
    allowed_tools = [
        str(item)
        for item in evidence_plan.get("allowedTools", [])
        if str(item or "").strip()
    ]
    pending_tools = [tool for tool in allowed_tools if tool not in executed_tools]
    evidence_needed = evidence_plan.get("evidenceNeeded")
    evidence_names = [
        str(item.get("name") or "")
        for item in evidence_needed
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ] if isinstance(evidence_needed, list) else []

    subject = _read_evidence_subject(country, question, evidence_plan)
    evidence_package = evidence_package or {}
    facts = _local_fallback_evidence_facts(evidence_package, limit=4)
    direct = _local_fallback_business_conclusion(
        intent=intent,
        country=country,
        question=question,
        subject=subject,
        facts=facts,
    )
    bullets: list[str] = [f"已查数据：{fact}。" for fact in facts[:3]]
    if not bullets:
        bullets.append("本轮没有取得可引用业务数据，不能把价格、销量、份额或政策影响写成确定事实。")
    if evidence_names:
        bullets.append(f"证据需求：{', '.join(_local_evidence_label(item) for item in evidence_names[:4])}。")
    if pending_tools:
        bullets.append(f"下一步补齐：{', '.join(_local_tool_label(item) for item in pending_tools[:3])}。")
    if pending_tools:
        bullets.append("补齐后应把新增数据直接更新到结论、图表和对比表，不重复解释工具计划。")

    limitations: list[str] = []
    if pending_tools:
        limitations.append("证据计划尚有未执行工具，不能编造未查到的数据。")

    return {
        "title": _local_fallback_title(intent, subject),
        "direct": direct,
        "bullets": bullets,
        "limitations": limitations,
    }


def _local_fallback_evidence_facts(evidence_package: dict[str, Any], *, limit: int) -> list[str]:
    facts: list[str] = []
    seen: set[str] = set()
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tool_results:
        if not isinstance(tool, dict) or tool.get("success") is False:
            continue
        refs = tool.get("evidenceRefs") if isinstance(tool.get("evidenceRefs"), list) else []
        for ref in refs:
            fact = _local_fallback_evidence_fact(ref)
            key = re.sub(r"\s+", "", fact).casefold()
            if not fact or key in seen:
                continue
            seen.add(key)
            facts.append(fact)
            if len(facts) >= limit:
                return facts
    return facts


def _local_fallback_evidence_fact(ref: Any) -> str:
    if not isinstance(ref, dict):
        return ""
    label = str(ref.get("label") or "").strip()
    value = ref.get("value")
    unit = str(ref.get("unit") or "").strip()
    if value in (None, "") or not label:
        return ""
    lowered = label.casefold()
    if any(token in lowered for token in (
        "source", "url", "rank", "retrieved", "candidate", "diagnostic",
        "row_count", "rowcount", "metadata", "result_count", "chart_count",
        "priceevidencestatus", "priceevidencerole", "currentpricerows",
        "reviewpendingrows", "sourcedraftpath", "candidatesourcetype",
        "materializationstatus", "materializationreadinessscore",
    )):
        return ""
    value_status = str(value or "").casefold()
    if any(token in value_status for token in ("candidate_search_query", "review_pending", "source_draft", "current_price")):
        return ""
    if re.search(r"(?:^|\.)competitor\.\d+\.model$", lowered) or lowered.endswith(".model"):
        return ""
    if _local_fallback_ref_is_zero_volume(label, value):
        return ""
    public_label = _local_fallback_evidence_label(label)
    if not public_label:
        return ""
    value_text = str(value).strip()
    if unit and unit.casefold() not in {"text", "string", "currency"} and unit not in value_text:
        value_text = f"{value_text} {unit}"
    return f"{public_label} {value_text}".strip()


def _local_fallback_ref_is_zero_volume(label: str, value: Any) -> bool:
    lowered = str(label or "").casefold()
    if not any(token in lowered for token in ("sales", "volume", "registrations", "count")):
        return False
    try:
        return float(str(value).replace(",", "").strip()) <= 0
    except (TypeError, ValueError):
        return False


def _local_fallback_evidence_label(label: str) -> str:
    text = str(label or "").strip()
    price_stat_labels = {
        "pricestats.min": "市场参考价格样本最低值",
        "pricestats.max": "市场参考价格样本最高值",
        "pricestats.avg": "市场参考价格样本均值",
        "pricestats.median": "市场参考价格样本中位数",
    }
    if text.casefold() in price_stat_labels:
        return price_stat_labels[text.casefold()]
    normalized = text.replace("_", " ")
    patterns = (
        (r"(?:contextSnapshot|marketSnapshot)\.powertrainMix\.([A-Za-z0-9]+)\.(sales|value)$", lambda match: f"{match.group(1).upper()} 销量"),
        (r"(?:contextSnapshot|marketSnapshot)\.powertrainMix\.([A-Za-z0-9]+)\.(share|mix)$", lambda match: f"{match.group(1).upper()} 占比"),
        (r"(?:contextSnapshot|marketSnapshot)\.cumulativeSales$", lambda _match: "市场累计销量"),
        (r"(?:contextSnapshot|marketSnapshot)\.avgMsrp$", lambda _match: "市场平均 MSRP"),
        (r"(?:driveBySegment|segmentByFuel)\.([^\.]+)\.(sales|value|share)$", lambda match: f"{match.group(1)} {('占比' if match.group(2) == 'share' else '销量')}"),
        (r"(?:driveBySegment|driveByFuel)\.([^\.]+)\.(2WD_pct|4WD_pct)$", lambda match: f"{match.group(1)} {match.group(2).replace('_pct', '')} 占比"),
        (r"(?:driveByFuel)\.([^\.]+)\.(2WD_pct|4WD_pct)$", lambda match: f"{match.group(1).upper()} {match.group(2).replace('_pct', '')} 占比"),
    )
    for pattern, render in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return render(match)
    model_metric = re.match(r"^(.+?)\.(sales|volume|share|msrp|price)$", text, flags=re.IGNORECASE)
    if model_metric:
        model = model_metric.group(1).replace("topModels.", "").replace("competitors.", "").strip()
        metric = model_metric.group(2).casefold()
        metric_labels = {"sales": "销量", "volume": "销量", "share": "份额", "msrp": "MSRP", "price": "价格"}
        if model:
            return f"{model} {metric_labels[metric]}"
    if len(normalized) > 80 or re.search(r"(?:^|[.])(?:source|url|rank|retrieved)(?:$|[.])", normalized, flags=re.IGNORECASE):
        return ""
    return normalized.replace(".", " / ")


def _local_fallback_business_conclusion(
    *,
    intent: str,
    country: str,
    question: str,
    subject: str,
    facts: list[str],
) -> str:
    fact_text = "；".join(facts[:3])
    normalized_question = str(question or "").casefold()
    market = _local_display_country(country)
    if not fact_text:
        return (
            f"{subject} 当前没有取得足够的可引用业务数据，因此不能给出确定的价格、销量、份额或政策结论。"
            "可以先明确需要补的数据范围，但不应把工具计划当作业务答案。"
        )
    if intent == "market_overview":
        if "j7" in normalized_question and "hev" in normalized_question:
            return (
                f"{market} J7 HEV 的首轮市场判断是：{fact_text}。"
                "这些数据可以支持把 HEV 作为待验证的场景入口，但还不足以直接确认车型进入或最终定价；下一步应把市场结构与车型级价格、配置证据连起来。"
            )
        return (
            f"{market} 市场的首轮结论应基于已查数据：{fact_text}。"
            "下一步应把这些结构信号转成目标车型、价格带和细分场景的产品动作。"
        )
    if intent == "pricing_analysis":
        reference_scope = (
            "这些是市场参考价格样本，不是本车型或核心竞品的当前官方 MSRP。"
            if any("市场参考价格样本" in fact for fact in facts)
            else ""
        )
        return (
            f"{_local_display_subject(subject, country)} 的定价判断目前可由这些已查数据支撑：{fact_text}。"
            f"{reference_scope}价格立场只能在本车型和竞品当前 MSRP、配置差异与月供/RV 同时具备后定稿。"
        )
    if intent == "competitor_compare":
        return (
            f"{_local_display_subject(subject, country)} 的对标结论应先落到已查数据：{fact_text}。"
            "这些事实可以界定市场场景，但车型胜负仍需要直接的价格、配置和销量对比。"
        )
    if intent in {"news_policy_search", "report_generation"}:
        return (
            f"{subject} 可先写入汇报的已查市场依据是：{fact_text}。"
            "政策或新闻影响仍须由来源日期、适用对象和官方原文确认，不能把市场背景替代为政策事实。"
        )
    return f"{subject} 的首轮业务判断基于：{fact_text}。后续结论应继续由新增证据更新，而不是重复工具说明。"


def _local_display_country(country: str) -> str:
    labels = {
        "sweden": "瑞典",
        "hungary": "匈牙利",
        "finland": "芬兰",
        "norway": "挪威",
        "denmark": "丹麦",
        "germany": "德国",
        "france": "法国",
        "poland": "波兰",
        "italy": "意大利",
        "austria": "奥地利",
        "netherlands": "荷兰",
    }
    value = str(country or "").strip()
    return labels.get(value.casefold(), value or "当前市场")


def _local_display_subject(subject: str, country: str) -> str:
    raw_country = str(country or "").strip()
    display_country = _local_display_country(raw_country)
    text = str(subject or "").strip()
    if raw_country and text.startswith(f"{raw_country} 的 "):
        return f"{display_country} {text[len(raw_country) + 3:]}"
    return text or display_country


def _build_grounded_local_draft(
    *,
    country: str,
    question: str,
    tool_call_history: list[dict[str, Any]],
    evidence_plan: dict[str, Any],
    evidence_package: dict[str, Any],
) -> dict[str, Any]:
    """Build the first visible answer from retrieved evidence before provider refinement."""
    local_answer = _build_local_final_answer(
        country=country,
        question=question,
        tool_call_history=tool_call_history,
        evidence_plan=evidence_plan,
        evidence_package=evidence_package,
    )
    return apply_answer_grounding_guard(
        local_answer,
        evidence_package,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    )


def _visible_stream_answer_text(answer: dict[str, Any]) -> str:
    """Return a compact, user-visible stream body for the chat bubble."""
    direct = str(answer.get("direct") or "").strip()
    bullets = _visible_stream_bullets(direct, answer.get("bullets"))
    limitations = _visible_stream_limitations(direct, bullets, answer.get("limitations"))
    lines: list[str] = []
    if direct:
        lines.append(direct)
    if bullets:
        lines.extend(["", "已查数据和判断："])
        lines.extend(f"- {item}" for item in bullets[:3])
    if limitations:
        lines.extend(["", "当前边界："])
        lines.extend(f"- {item}" for item in limitations[:2])
    return "\n".join(lines).strip()


def _visible_stream_bullets(direct: str, raw_bullets: Any) -> list[str]:
    if not isinstance(raw_bullets, list):
        return []
    direct_key = _stream_compare_key(direct)
    result: list[str] = []
    seen: set[str] = set()
    for raw_item in raw_bullets:
        item = str(raw_item or "").strip()
        if not item:
            continue
        item_key = _stream_compare_key(item)
        if not item_key or item_key in seen:
            continue
        if _stream_bullet_repeats_direct(item_key, direct_key):
            continue
        seen.add(item_key)
        result.append(item)
    return result


def _visible_stream_limitations(direct: str, bullets: list[str], raw_limitations: Any) -> list[str]:
    if not isinstance(raw_limitations, list):
        return []
    existing = {_stream_compare_key(direct)}
    existing.update(_stream_compare_key(item) for item in bullets)
    result: list[str] = []
    seen: set[str] = set()
    for raw_item in raw_limitations:
        item = str(raw_item or "").strip()
        key = _stream_compare_key(item)
        if not item or not key or key in seen or key in existing:
            continue
        seen.add(key)
        result.append(item)
    return result


def _stream_bullet_repeats_direct(item_key: str, direct_key: str) -> bool:
    if not item_key or not direct_key:
        return False
    direct_probe = direct_key[:80]
    item_probe = item_key[:80]
    return (
        len(direct_probe) >= 24
        and (item_key.startswith(direct_probe) or direct_key.startswith(item_probe))
    )


def _stream_compare_key(text: str) -> str:
    value = str(text or "").strip()
    value = re.sub(r"^\s*(?:直接结论|结论|核心判断|核心发现|判断|summary|conclusion)\s*[:：]\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", "", value)
    value = re.sub(r"[，,。.!！?？；;:：、（）()\\[\\]【】\"'“”‘’`]+", "", value)
    return value.casefold()


def _local_fallback_title(intent: str, subject: str) -> str:
    labels = {
        "pricing_analysis": "定价分析",
        "market_overview": "市场机会分析",
        "competitor_compare": "竞品定位分析",
        "configuration_analysis": "配置价值分析",
        "inventory_analysis": "库存/BOM 分析",
        "news_policy_search": "政策/外部研究分析",
        "report_generation": "汇报生成",
        "voc_analysis": "VOC 分析",
    }
    label = labels.get(str(intent or "").strip(), "业务分析")
    clean_subject = str(subject or "").strip()
    if clean_subject:
        return f"{clean_subject} · {label}"
    return label


def _local_evidence_label(name: str) -> str:
    value = str(name or "").strip()
    mapping = {
        "supporting_evidence": "支撑证据",
        "report_outline": "汇报结构",
        "current_msrp": "当前 MSRP",
        "price_corridor": "价格走廊",
        "competitor_price_range": "竞品价格带",
        "competitor_pool": "竞品池",
        "configuration_delta": "配置差异",
        "market_snapshot": "市场快照",
        "policy_effect": "政策影响",
        "source_date": "来源日期",
        "consumer_signal": "用户信号",
    }
    if value in mapping:
        return mapping[value]
    if value.startswith("missing_required_tool:"):
        return f"{_local_tool_label(value.replace('missing_required_tool:', '', 1))}工具结果"
    return value.replace("_", " ") or "证据"


def _local_tool_label(tool_name: str) -> str:
    value = str(tool_name or "").strip()
    mapping = {
        "query_msrp_pricing": "MSRP/当前价格",
        "compare_competitive_set": "竞品池/价格走廊",
        "query_competitive_landscape": "竞品格局",
        "query_price_positioning": "目标价格定位",
        "query_country_snapshot": "市场快照",
        "analyze_market_dynamics": "市场动态分析",
        "build_market_chart": "趋势图表",
        "compare_vehicle_variants": "配置差异",
        "external_research": "外部研究",
        "search_market_news": "新闻/政策搜索",
        "read_web_page": "网页来源读取",
        "pageindex_search_documents": "文档检索",
        "minirag_query_graph": "多跳知识检索",
        "query_with_filters": "筛选查询",
    }
    return mapping.get(value, value.replace("_", " ") or "必需")


def _read_evidence_subject(
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
) -> str:
    entities = evidence_plan.get("entities")
    models: list[str] = []
    if isinstance(entities, dict):
        raw_models = entities.get("models")
        if isinstance(raw_models, list):
            models = [str(item) for item in raw_models if str(item or "").strip()]
    if models:
        return f"{country} 的 {', '.join(models[:3])}"
    return f"{country} 相关问题：{_display_question_subject(question, max_chars=80)}"


def _display_question_subject(question: str, *, max_chars: int = 80) -> str:
    text = re.sub(r"\s+", " ", str(question or "")).strip()
    text = re.sub(r"请先判断国家[，,、\s]*", "", text)
    text = re.sub(r"(?:不要|别)回答[^，,。.!！?？；;]{1,32}[，,。.!！?？；;]?", "", text)
    text = re.sub(r"(?:不要|别)用[^，,。.!！?？；;]{1,32}[，,。.!！?？；;]?", "", text)
    text = re.sub(r"\b(?:do not|don't|dont)\s+(?:answer|use)\s+[^，,。.!！?？；;]{1,32}[，,。.!！?？；;]?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip().rstrip("。.!！?？；; ")
    text = re.sub(r"[，,、\s]*(?:请|麻烦|谢谢)$", "", text).strip().rstrip("。.!！?？；; ")
    return text[:max_chars] if text else "当前问题"


def _read_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _pm_insight_from_answer(answer: dict[str, Any]) -> str:
    implications = answer.get("businessImplications")
    for item in _read_string_list(implications):
        return item
    synthesis = answer.get("businessSynthesisPlan")
    if isinstance(synthesis, dict):
        for item in _read_string_list(synthesis.get("businessImplications")):
            return item
    bullets = _read_string_list(answer.get("bullets"))
    return bullets[0] if bullets else ""


def _build_developer_trace(
    country: str,
    question: str,
    tool_call_history: list[dict[str, Any]],
    follow_ups: list[dict[str, Any]],
    evidence_plan: dict[str, Any],
) -> dict[str, Any]:
    """Return a compact debug trace safe for developer-mode UI display."""
    tools: list[dict[str, Any]] = []
    for index, item in enumerate(tool_call_history):
        tool_trace: dict[str, Any] = {
            "index": index + 1,
            "tool": str(item.get("tool") or ""),
            "round": item.get("round") if isinstance(item.get("round"), int) else index + 1,
            "status": str(item.get("status") or "ok"),
            "reason": str(item.get("reason") or "")[:240],
            "hasData": bool(item.get("hasData")),
            "chartCount": int(item.get("chartCount") or 0),
        }
        summary = item.get("summary")
        if isinstance(summary, dict):
            tool_trace["summary"] = summary
        error = item.get("error")
        if error:
            tool_trace["error"] = str(error)[:240]
        tools.append(tool_trace)

    return {
        "mode": "developer",
        "country": country,
        "questionPreview": question[:240],
        "intent": str(evidence_plan.get("intent") or ""),
        "allowedTools": evidence_plan.get("allowedTools", []),
        "requiredTools": evidence_plan.get("requiredTools", []),
        "mustHaveEvidence": evidence_plan.get("mustHaveEvidence", []),
        "answerMode": str(evidence_plan.get("answerMode") or ""),
        "toolCount": len(tools),
        "tools": tools,
        "followUps": follow_ups[:4],
        "answerParser": "parse_agent_answer_content",
        "dataBoundary": "JATO MCP tool summaries only",
    }


def _fallback_follow_ups(country: str, question: str, tools: list[str]) -> list[str]:
    country_label = country or "当前市场"
    primary_tool = tools[-1] if tools else ""
    suggestions: list[str]
    if primary_tool == "build_market_chart":
        suggestions = [
            f"把 {country_label} 这个趋势拆到品牌和车型层面看。",
            f"对比 {country_label} 和挪威/丹麦同一指标。",
            "解释这个趋势背后的价格、政策或供给变化。",
        ]
    elif primary_tool == "query_msrp_pricing":
        suggestions = [
            "把这些价格和主要竞品 MSRP 区间做对比。",
            "找出这个价格带里销量最高的车型。",
            "按版本或配置差异解释价格分层。",
        ]
    elif primary_tool in {"search_market_news", "analyze_market_dynamics"}:
        suggestions = [
            f"把这些外部来源和 {country_label} 的销量/份额变化交叉验证。",
            "找出未来 3-6 个月最可能影响市场的事件。",
            "把影响拆到品牌或动力类型层面看。",
        ]
    elif primary_tool in {"analyze_model_performance", "compare_competitive_set"}:
        suggestions = [
            "继续拆解销量、价格、配置和新闻中哪个因素贡献最大。",
            "把这个车型和同级竞品做 side-by-side 对比。",
            "找出最值得追踪的风险或机会。",
        ]
    else:
        suggestions = [
            f"继续找 {country_label} 的结构化销量或份额证据。",
            "换一个工具交叉验证这个结论。",
            "把这个问题拆成趋势、竞品、价格和新闻四个维度分析。",
        ]
    if "为什么" in question or "原因" in question or "why" in question.lower():
        suggestions.insert(0, f"补充搜索 {country_label} 的政策、新闻或消费者反馈来解释原因。")
    return _read_string_list(suggestions)[:4]
