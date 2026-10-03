from __future__ import annotations

import re
import time
from typing import Any

from app.core.config import ASTRBOT_PROVIDER_MODEL
from app.db.session import get_session_factory
from app.services import engineering_variant_diff_service
from app.services import lease_comparison_service
from app.services import msrp_lookup_service
from app.services import web_search_service
from app.services.country_chat_service import (
    _build_country_chat_route,
    _enrich_snapshot_for_intents,
    build_country_chart_deck,
    build_country_snapshot,
    extract_user_params,
    infer_country_chat_intents,
)
from app.services.country_service import COUNTRY_ALIAS_GROUPS
from app.services.country_service import to_display_country
from app.services.jato_agent_profiles_service import get_active_agent_profile
from app.services.jato_agent_skills_service import get_agent_skill
from app.services.jato_agent_skills_service import infer_skill_id_from_mode
from app.services.jato_agent_memory_service import save_agent_run
from app.services.jato_agent_provider_service import agent_select_tools
from app.services.jato_agent_provider_service import compose_agent_final_answer
from app.services.jato_agent_provider_service import run_agent_loop
from app.services.jato_agent_planning_service import build_evidence_plan
from app.services.jato_answer_grounding_service import apply_answer_grounding_guard
from app.services.jato_agent_deterministic_judge_service import score_deterministic_answer
from app.services.jato_agent_llm_judge_service import judge_answer_with_llm
from app.services.jato_business_playbook_service import build_business_playbook_context
from app.services.jato_evidence_package_service import build_evidence_package
from app.services.jato_evidence_package_service import evidence_ref_count
from app.services.jato_followup_service import normalize_follow_ups
from app.services.jato_followup_service import serialize_follow_ups
from app.services.jato_retrieval_router_service import (
    build_retrieval_tool_plan,
    classify_retrieval_intent,
    merge_evidence_pack,
)
from app.services.jato_research_governance_service import apply_research_governance
from app.services.jato_research_governance_service import infer_research_intent
from app.services.jato_research_governance_service import normalize_research_mode
from app.services.jato_research_governance_service import standardize_jato_cross_check
from app.services.jato_tool_coverage_guard_service import missing_required_tools
from app.services.jato_tool_coverage_guard_service import required_tool_args
from app.services.jato_tool_registry_service import filter_tool_descriptors_for_allowed
from app.services.jato_usage_tracker import track_agent_answer_run
from app.services.jato_usage_tracker import estimate_tool_call_tokens
from app.services.jato_usage_tracker import track_followup_impression
from app.services.jato_usage_tracker import track_tool_call_event
from app.services.jato_chart_spec_builder import build_chart_spec_from_deck
from app.services.jato_llm_answer_service import synthesize_agent_answer
from app.services.jato_visual_artifact_service import build_visual_artifacts
from app.services.jato_query_tools import (
    query_with_filters,
    query_time_series,
    query_segment_breakdown,
    query_price_positioning,
    query_competitive_landscape,
)
from app.services.jato_pageindex_client import (
    is_configured as pageindex_configured,
    search_documents as pageindex_search,
    get_section as pageindex_section,
    list_documents as pageindex_list,
)
from app.services.jato_minirag_client import (
    is_configured as minirag_configured,
    query_graph as minirag_query,
    explain_entity as minirag_explain,
)
from app.services.jato_browser_readonly_service import read_web_page as browser_read_web_page
from app.services.jato_browser_snapshot_service import browser_snapshot as browser_capture_snapshot
from app.services.jato_browser_interaction_service import (
    browser_click_confirmed as browser_confirm_click,
)
from app.services.jato_browser_interaction_service import (
    browser_interaction_plan as browser_plan_interaction,
)
from app.services.jato_browser_interaction_service import (
    browser_type_confirmed as browser_confirm_type,
)
from app.services.jato_country_resolution_service import COUNTRY_CODE_ALIASES
from app.services.jato_country_resolution_service import resolve_effective_country


DEFAULT_SNAPSHOT_SECTIONS = [
    "country",
    "route",
    "periodLabel",
    "kpis",
    "analysisMeta",
    "metricScopes",
    "yearSeries",
    "monthSeries",
    "topBrands",
    "topModels",
    "powertrainMix",
    "newsDigest",
    "marketEvents",
]
DEFAULT_CHART_SECTIONS = [
    "country",
    "route",
    "periodLabel",
    "kpis",
    "analysisMeta",
    "metricScopes",
    "yearSeries",
    "monthSeries",
    "topBrands",
    "topModels",
    "powertrainMix",
    "crossTabs",
    "suvA",
    "suvB",
    "positioningMap",
    "priceDistribution",
    "modelVersionBubble",
    "marketScanScope",
    "newsDigest",
]
MAX_SECTION_ITEMS = 40
JATO_DATA_COUNTRY_ALIASES = {
    "sweden": "瑞典",
    "swedish": "瑞典",
    "sverige": "瑞典",
    "se": "瑞典",
    "swe": "瑞典",
    "finland": "芬兰",
    "finnish": "芬兰",
    "suomi": "芬兰",
    "fi": "芬兰",
    "fin": "芬兰",
    "norway": "挪威",
    "norwegian": "挪威",
    "norge": "挪威",
    "no": "挪威",
    "nor": "挪威",
    "denmark": "丹麦",
    "danish": "丹麦",
    "danmark": "丹麦",
    "dk": "丹麦",
    "dnk": "丹麦",
    "germany": "德国",
    "german": "德国",
    "deutschland": "德国",
    "de": "德国",
    "deu": "德国",
    "france": "法国",
    "french": "法国",
    "fr": "法国",
    "fra": "法国",
    "netherlands": "荷兰",
    "dutch": "荷兰",
    "holland": "荷兰",
    "nl": "荷兰",
    "nld": "荷兰",
    "uk": "英国",
    "united kingdom": "英国",
    "great britain": "英国",
    "gb": "英国",
    "gbr": "英国",
    "spain": "西班牙",
    "spanish": "西班牙",
    "es": "西班牙",
    "esp": "西班牙",
    "belgium": "比利时",
    "belgian": "比利时",
    "be": "比利时",
    "bel": "比利时",
}


JATO_MCP_TOOL_DESCRIPTORS: list[dict[str, Any]] = [
    {
        "name": "route_agent_request",
        "description": "Route one AstrBot/JATO agent request to the best governed JATO tool.",
        "required": ["country", "question"],
    },
    {
        "name": "query_country_snapshot",
        "description": "Return a governed JATO country market snapshot.",
        "required": ["country"],
    },
    {
        "name": "query_msrp_pricing",
        "description": "Return current MSRP pricing for one or more models.",
        "required": ["country"],
    },
    {
        "name": "query_leasing_offers",
        "description": "Return lease offers with monthly payment, term, mileage, residual value, and total contract cost.",
        "required": ["country"],
    },
    {
        "name": "search_market_news",
        "description": "Search market news with the existing JATO provider fallback.",
        "required": ["country", "question"],
    },
    {
        "name": "read_web_page",
        "description": "Read a public HTTP/HTTPS web page as static text with SSRF safeguards. No JavaScript, cookies, clicks, forms, or login state.",
        "required": ["url"],
        "status": "readonly",
    },
    {
        "name": "browser_snapshot",
        "description": "Capture a governed public-page browser snapshot when Playwright is available, otherwise fall back to static page text. No clicks, typing, forms, cookies, or login state.",
        "required": ["url"],
        "status": "readonly+fallback",
    },
    {
        "name": "browser_interaction_plan",
        "description": "Propose governed click/type actions for a public page and mint short-lived confirmation tokens. This does not execute actions.",
        "required": ["url"],
        "status": "requires_confirmation",
    },
    {
        "name": "browser_click_confirmed",
        "description": "Execute one previously approved browser click using a confirmation token from browser_interaction_plan. No cookies, login state, submits, uploads, or account changes.",
        "required": ["url", "action_id", "confirmation_token"],
        "status": "confirmed_action",
    },
    {
        "name": "browser_type_confirmed",
        "description": "Fill one previously approved browser field using a confirmation token from browser_interaction_plan. It does not press Enter or submit forms.",
        "required": ["url", "action_id", "confirmation_token", "text"],
        "status": "confirmed_action",
    },
    {
        "name": "compare_vehicle_variants",
        "description": "Compare selected market vehicle variants and features.",
        "required": ["country"],
    },
    {
        "name": "build_market_chart",
        "description": "Build chart-ready country market context with renderable chart specs.",
        "required": ["country"],
    },
    {
        "name": "pageindex_search_documents",
        "description": "Search long documents (PDFs, reports, manuals) via PageIndex tree index with web fallback.",
        "required": ["country", "question"],
        "status": "live+fallback",
    },
    {
        "name": "pageindex_get_section",
        "description": "Read a specific section or chapter from a PageIndex-indexed document with web fallback.",
        "required": ["country", "question"],
        "status": "live+fallback",
    },
    {
        "name": "pageindex_list_documents",
        "description": "List available PageIndex documents when connected, otherwise return fallback guidance.",
        "required": ["country"],
        "status": "live+fallback",
    },
    {
        "name": "minirag_query_graph",
        "description": "Query multi-hop entity relationships via MiniRAG heterogeneous graph with multi-tool fallback.",
        "required": ["country", "question"],
        "status": "live+fallback",
    },
    {
        "name": "minirag_explain_entity",
        "description": "Explain entity relationships within the JATO business domain with news fallback.",
        "required": ["country", "question"],
        "status": "live+fallback",
    },
    {
        "name": "minirag_update_corpus",
        "description": "Prepare MiniRAG corpus update metadata; live graph writes depend on MiniRAG storage setup.",
        "required": ["country"],
        "status": "fallback",
    },
    {
        "name": "analyze_model_performance",
        "description": "Deep-dive one model: sales + MSRP pricing + variant config + news cross-referenced. Use for 'why X sells well', root cause analysis.",
        "required": ["country"],
    },
    {
        "name": "compare_competitive_set",
        "description": "Compare a model against segment competitors across sales, price positioning, and features. Use for competitive landscape questions.",
        "required": ["country", "question"],
    },
    {
        "name": "analyze_market_dynamics",
        "description": "What's changing: new entries, price shifts, policy updates, consumer trends — cross-referenced. Use for 'what's driving X market'.",
        "required": ["country", "question"],
    },
    {
        "name": "query_with_filters",
        "description": "Query market data with full filter dimensions: country, powertrain, fuel_type, segment, brand, model, year. Returns KPIs, rankings, trend series, powertrain mix — all filterable like the UI sidebar.",
        "required": ["country"],
    },
    {
        "name": "query_time_series",
        "description": "Time-Series Lens: get trend data (monthly/yearly) filtered by powertrain, fuel_type, segment, year. Best for: 'show me the trend', 'how has X changed over time'.",
        "required": ["country"],
    },
    {
        "name": "query_segment_breakdown",
        "description": "Segment Lens: cross-tab analysis by segment × fuel × powertrain. Best for: 'which segment dominates', 'SUV vs Car breakdown', 'segment-fuel matrix'.",
        "required": ["country"],
    },
    {
        "name": "query_price_positioning",
        "description": "Price Positioning Lens: MSRP distribution, price stats, competitive price context. Best for: 'price range for X', 'how is X priced vs market'.",
        "required": ["country"],
    },
    {
        "name": "query_competitive_landscape",
        "description": "One-stop competitive intelligence: given a model, finds competitors and compares sales + pricing + features. Best for: 'who competes with X', 'competitive set analysis'.",
        "required": ["country", "model"],
    },
    {
        "name": "query_powertrain_trend",
        "description": "Get sales trends filtered by powertrain type (BEV/PHEV/HEV/ICE). Best for: 'BEV trend', 'how is PHEV doing', powertrain-specific analysis.",
        "required": ["country"],
    },
    {
        "name": "query_brand_deep_dive",
        "description": "Deep analysis of a specific brand: sales, top models, pricing, and market position. Best for: 'how is Volvo doing', 'Toyota performance analysis'.",
        "required": ["country", "brand"],
    },
    {
        "name": "query_cross_country",
        "description": "Compare the same metric across multiple countries. Best for: 'compare Sweden vs Norway BEV adoption', 'cross-country analysis'.",
        "required": ["countries", "question"],
    },
]


def list_jato_mcp_tools() -> dict[str, Any]:
    return {"items": JATO_MCP_TOOL_DESCRIPTORS}


def call_jato_mcp_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    tool_name = str(name or "").strip()
    args = arguments or {}
    if tool_name == "route_agent_request":
        return route_agent_request(args)
    if tool_name == "query_country_snapshot":
        return query_country_snapshot(args)
    if tool_name == "query_msrp_pricing":
        return query_msrp_pricing(args)
    if tool_name == "query_leasing_offers":
        return query_leasing_offers(args)
    if tool_name == "search_market_news":
        return search_market_news(args)
    if tool_name == "read_web_page":
        return read_web_page(args)
    if tool_name == "browser_snapshot":
        return browser_snapshot(args)
    if tool_name == "browser_interaction_plan":
        return browser_interaction_plan(args)
    if tool_name == "browser_click_confirmed":
        return browser_click_confirmed(args)
    if tool_name == "browser_type_confirmed":
        return browser_type_confirmed(args)
    if tool_name == "compare_vehicle_variants":
        return compare_vehicle_variants(args)
    if tool_name == "build_market_chart":
        return build_market_chart(args)
    if tool_name == "pageindex_search_documents":
        return pageindex_search_documents(args)
    if tool_name == "pageindex_get_section":
        return pageindex_get_section(args)
    if tool_name == "pageindex_list_documents":
        return pageindex_list_documents(args)
    if tool_name == "minirag_query_graph":
        return minirag_query_graph(args)
    if tool_name == "minirag_explain_entity":
        return minirag_explain_entity(args)
    if tool_name == "minirag_update_corpus":
        return minirag_update_corpus(args)
    if tool_name == "analyze_model_performance":
        return analyze_model_performance(args)
    if tool_name == "compare_competitive_set":
        return compare_competitive_set(args)
    if tool_name == "analyze_market_dynamics":
        return analyze_market_dynamics(args)
    if tool_name == "query_with_filters":
        return _call_query_with_filters(args)
    if tool_name == "query_time_series":
        return _call_query_time_series(args)
    if tool_name == "query_segment_breakdown":
        return _call_query_segment_breakdown(args)
    if tool_name == "query_price_positioning":
        return _call_query_price_positioning(args)
    if tool_name == "query_competitive_landscape":
        return _call_query_competitive_landscape(args)
    if tool_name == "query_powertrain_trend":
        return _call_query_powertrain_trend(args)
    if tool_name == "query_brand_deep_dive":
        return _call_query_brand_deep_dive(args)
    if tool_name == "query_cross_country":
        return _call_query_cross_country(args)
    raise ValueError(f"Unsupported JATO MCP tool: {tool_name}")


def route_agent_request(arguments: dict[str, Any]) -> dict[str, Any]:
    question = _required_text(arguments, "question")
    country = resolve_effective_country(_required_text(arguments, "country"), question)
    session_id = str(arguments.get("session_id") or arguments.get("sessionId") or "")
    requested_mode = _optional_text(arguments, "mode").lower()
    profile = get_active_agent_profile()
    requested_skill_id = _optional_text(arguments, "skill_id") or _optional_text(arguments, "skillId")
    skill = get_agent_skill(requested_skill_id)
    skill_mode = str(skill.get("routeMode") or "").lower()
    effective_mode = requested_mode or (skill_mode if skill_mode != "auto" else "")
    evidence_plan = build_evidence_plan(country, question)
    allowed_tool_descriptors = filter_tool_descriptors_for_allowed(
        JATO_MCP_TOOL_DESCRIPTORS,
        _text_list(evidence_plan.get("allowedTools")),
    )

    # ── Phase 6: Retrieval Router classification ──
    retrieval_classification = classify_retrieval_intent(question)
    retrieval_tool_plan = build_retrieval_tool_plan(retrieval_classification, country, question)

    # ── Agent-level LLM tool selection (AstrBot pattern) ──
    agent_selection: dict[str, Any] | None = None
    if not effective_mode and not _extract_first_url(question):
        try:
            agent_selection = agent_select_tools(
                country=country,
                question=question,
                available_tools=allowed_tool_descriptors,
                skill_context={"name": skill["name"], "description": skill.get("description", "")},
            )
        except Exception:
            agent_selection = None

    # ── Route selection: explicit mode/skill override wins, else LLM agent, else retrieval router ──
    if effective_mode:
        route = _select_agent_route(question, effective_mode)
        route_source = "skill_or_mode_override"
    elif _extract_first_url(question):
        selected_web_tool = "browser_snapshot" if _needs_browser_snapshot(question) else "read_web_page"
        route = {
            "mode": "web",
            "tool": selected_web_tool,
            "reason": "url_read_question",
            "retrievalPath": "web_search",
        }
        route_source = "url_router"
    elif agent_selection and agent_selection.get("source") == "llm_agent" and agent_selection.get("confidence") in ("high", "medium"):
        # LLM agent chose the tool — trust it
        selected_tool = str(agent_selection.get("primary_tool") or "")
        mode_label = str(agent_selection.get("mode") or "snapshot")
        if selected_tool not in {
            "query_country_snapshot",
            "query_msrp_pricing",
            "compare_vehicle_variants",
            "search_market_news",
            "read_web_page",
            "browser_snapshot",
            "build_market_chart",
            "pageindex_search_documents",
            "minirag_query_graph",
            "analyze_model_performance",
            "compare_competitive_set",
            "analyze_market_dynamics",
        }:
            selected_tool = "query_country_snapshot"
        route = {"mode": mode_label, "tool": selected_tool, "reason": f'llm_agent: {agent_selection.get("reasoning", "")[:100]}', "retrievalPath": retrieval_classification["primary"]["path"]}
        route_source = "llm_agent"
        # Override skill to match the LLM's chosen mode
        if not requested_skill_id:
            skill = get_agent_skill(infer_skill_id_from_mode(mode_label))
        # Also update retrieval classification primary to match LLM choice
        if selected_tool == "search_market_news":
            retrieval_classification["primary"] = {"path": "hybrid_rag", "confidence": "medium", "signals": ["llm_agent"], "reason": "LLM agent selected news search"}
        elif selected_tool == "read_web_page":
            retrieval_classification["primary"] = {"path": "web_search", "confidence": "medium", "signals": ["llm_agent_url"], "reason": "LLM agent selected static web page reader"}
        elif selected_tool == "browser_snapshot":
            retrieval_classification["primary"] = {"path": "web_search", "confidence": "medium", "signals": ["llm_agent_browser_snapshot"], "reason": "LLM agent selected browser snapshot"}
        elif selected_tool == "build_market_chart":
            retrieval_classification["primary"] = {"path": "structured_mcp", "confidence": "medium", "signals": ["llm_agent_chart"], "reason": "LLM agent selected chart"}
        elif selected_tool == "query_msrp_pricing":
            retrieval_classification["primary"] = {"path": "structured_mcp", "confidence": "medium", "signals": ["llm_agent_pricing"], "reason": "LLM agent selected pricing"}
        elif selected_tool == "compare_vehicle_variants":
            retrieval_classification["primary"] = {"path": "structured_mcp", "confidence": "medium", "signals": ["llm_agent_variant"], "reason": "LLM agent selected variant compare"}
        elif selected_tool == "pageindex_search_documents":
            retrieval_classification["primary"] = {"path": "pageindex", "confidence": "medium", "signals": ["llm_agent"], "reason": "LLM agent selected pageindex"}
        elif selected_tool == "minirag_query_graph":
            retrieval_classification["primary"] = {"path": "minirag", "confidence": "medium", "signals": ["llm_agent"], "reason": "LLM agent selected minirag"}
        elif selected_tool in {"analyze_model_performance", "compare_competitive_set", "analyze_market_dynamics"}:
            retrieval_classification["primary"] = {"path": "structured_mcp", "confidence": "medium", "signals": ["llm_agent_cross_reference"], "reason": f"LLM agent selected {selected_tool}"}
    else:
        # Use retrieval router's primary path + signals to select the best tool
        primary_path = retrieval_classification["primary"]["path"]
        primary_signals = retrieval_classification["primary"].get("signals", [])
        selected_tool, mode_label, reason = _select_tool_for_retrieval_path(primary_path, primary_signals)
        route = {"mode": mode_label, "tool": selected_tool, "reason": reason, "retrievalPath": primary_path}
        route_source = "retrieval_router"

    route = _enforce_intent_required_tool(route, evidence_plan, question, route_source)

    if route["tool"] in {"read_web_page", "browser_snapshot"}:
        web_decision = {
            "path": "web_search",
            "confidence": "high",
            "signals": ["explicit_url"],
            "reason": "Explicit URL read request",
        }
        other_decisions = [
            decision
            for decision in retrieval_classification.get("decisions", [])
            if decision.get("path") != "web_search"
        ]
        retrieval_classification["primary"] = web_decision
        retrieval_classification["secondary"] = [str(decision.get("path")) for decision in other_decisions if decision.get("path")]
        retrieval_classification["allPaths"] = ["web_search", *retrieval_classification["secondary"]]
        retrieval_classification["decisions"] = [web_decision, *other_decisions]
        retrieval_tool_plan = build_retrieval_tool_plan(retrieval_classification, country, question)

    if not requested_skill_id:
        skill = get_agent_skill(infer_skill_id_from_mode(route["mode"]))

    # ── Execute primary tool ──
    tool_arguments = _build_route_tool_arguments(route["tool"], arguments, country, question, evidence_plan)
    primary_started_at = time.perf_counter()
    primary_result = call_jato_mcp_tool(route["tool"], tool_arguments)
    primary_latency_ms = int((time.perf_counter() - primary_started_at) * 1000)
    try:
        track_tool_call_event(
            session_id=session_id,
            country=country,
            question=question,
            tool_name=route["tool"],
            arguments=tool_arguments,
            latency_ms=primary_latency_ms,
            success=True,
            cost_estimate=estimate_tool_call_tokens(route["tool"], tool_arguments, primary_result),
        )
    except Exception:
        pass
    primary_metadata = _dict_value(primary_result.get("metadata")) or {}
    display = _build_agent_display(route, primary_result, country, question, profile, skill)

    # ── Enhanced evidence pack with retrieval path metadata ──
    active_retrieval_path = route.get("retrievalPath", route["mode"])
    executed_results_by_path: dict[str, dict[str, Any]] = {active_retrieval_path: primary_result}
    executed_arguments_by_step: dict[tuple[str, str], dict[str, Any]] = {
        (active_retrieval_path, route["tool"]): tool_arguments
    }
    secondary_results: list[dict[str, Any]] = []
    executed_tools = {route["tool"]}

    if _should_execute_secondary_paths(arguments, route_source):
        # Priority: LLM agent secondary tool suggestion
        if agent_selection and agent_selection.get("secondary_tool"):
            llm_secondary = str(agent_selection["secondary_tool"])
            if llm_secondary and llm_secondary != route["tool"] and llm_secondary not in executed_tools:
                try:
                    secondary_args = _build_route_tool_arguments(llm_secondary, arguments, country, question, evidence_plan)
                    secondary_started_at = time.perf_counter()
                    sr = call_jato_mcp_tool(llm_secondary, secondary_args)
                    secondary_latency_ms = int((time.perf_counter() - secondary_started_at) * 1000)
                    try:
                        track_tool_call_event(
                            session_id=session_id,
                            country=country,
                            question=question,
                            tool_name=llm_secondary,
                            arguments=secondary_args,
                            latency_ms=secondary_latency_ms,
                            success=True,
                            cost_estimate=estimate_tool_call_tokens(llm_secondary, secondary_args, sr),
                        )
                    except Exception:
                        pass
                    secondary_results.append({"path": "llm_agent_secondary", "tool": llm_secondary, "status": "executed", "reason": f"LLM agent suggested cross-reference: {llm_secondary}", "arguments": secondary_args, "result": sr})
                    executed_tools.add(llm_secondary)
                    executed_results_by_path["llm_agent_secondary"] = sr
                    executed_arguments_by_step[("llm_agent_secondary", llm_secondary)] = secondary_args
                except Exception:
                    pass

        for decision in retrieval_classification.get("decisions", []):
            secondary_path = str(decision.get("path") or "").strip()
            if not secondary_path or secondary_path == active_retrieval_path:
                continue
            secondary_tool, _, secondary_reason = _select_tool_for_retrieval_path(
                secondary_path,
                decision.get("signals", []),
            )
            if secondary_tool in executed_tools:
                continue
            secondary_arguments = _build_route_tool_arguments(secondary_tool, arguments, country, question, evidence_plan)
            try:
                secondary_started_at = time.perf_counter()
                secondary_result = call_jato_mcp_tool(secondary_tool, secondary_arguments)
                secondary_latency_ms = int((time.perf_counter() - secondary_started_at) * 1000)
            except Exception as exc:
                try:
                    track_tool_call_event(
                        session_id=session_id,
                        country=country,
                        question=question,
                        tool_name=secondary_tool,
                        arguments=secondary_arguments,
                        latency_ms=0,
                        success=False,
                        error=str(exc),
                    )
                except Exception:
                    pass
                secondary_results.append({
                    "path": secondary_path,
                    "tool": secondary_tool,
                    "status": "failed",
                    "reason": secondary_reason,
                    "arguments": secondary_arguments,
                    "error": str(exc),
                })
                continue

            executed_tools.add(secondary_tool)
            try:
                track_tool_call_event(
                    session_id=session_id,
                    country=country,
                    question=question,
                    tool_name=secondary_tool,
                    arguments=secondary_arguments,
                    latency_ms=secondary_latency_ms,
                    success=True,
                    cost_estimate=estimate_tool_call_tokens(secondary_tool, secondary_arguments, secondary_result),
                )
            except Exception:
                pass
            executed_results_by_path[secondary_path] = secondary_result
            executed_arguments_by_step[(secondary_path, secondary_tool)] = secondary_arguments
            secondary_results.append({
                "path": secondary_path,
                "tool": secondary_tool,
                "status": "executed",
                "reason": secondary_reason,
                "arguments": secondary_arguments,
                "result": secondary_result,
            })
            if len([item for item in secondary_results if item.get("status") == "executed"]) >= 2:
                break

    coverage_tools = missing_required_tools(
        evidence_plan,
        list(executed_tools),
        allowed_tools=_text_list(evidence_plan.get("allowedTools")),
    )
    for coverage_tool in coverage_tools:
        if coverage_tool in executed_tools:
            continue
        coverage_arguments = required_tool_args(
            evidence_plan,
            coverage_tool,
            country=country,
            question=question,
        )
        try:
            coverage_started_at = time.perf_counter()
            coverage_result = call_jato_mcp_tool(coverage_tool, coverage_arguments)
            coverage_latency_ms = int((time.perf_counter() - coverage_started_at) * 1000)
        except Exception as exc:
            try:
                track_tool_call_event(
                    session_id=session_id,
                    country=country,
                    question=question,
                    tool_name=coverage_tool,
                    arguments=coverage_arguments,
                    latency_ms=0,
                    success=False,
                    error=str(exc),
                )
            except Exception:
                pass
            secondary_results.append({
                "path": "tool_coverage_guard",
                "tool": coverage_tool,
                "status": "failed",
                "reason": "required_tool_coverage_guard",
                "arguments": coverage_arguments,
                "error": str(exc),
            })
            continue

        executed_tools.add(coverage_tool)
        try:
            track_tool_call_event(
                session_id=session_id,
                country=country,
                question=question,
                tool_name=coverage_tool,
                arguments=coverage_arguments,
                latency_ms=coverage_latency_ms,
                success=True,
                cost_estimate=estimate_tool_call_tokens(coverage_tool, coverage_arguments, coverage_result),
            )
        except Exception:
            pass
        coverage_path = f"tool_coverage_guard:{coverage_tool}"
        executed_results_by_path[coverage_path] = coverage_result
        executed_arguments_by_step[(coverage_path, coverage_tool)] = coverage_arguments
        secondary_results.append({
            "path": coverage_path,
            "tool": coverage_tool,
            "status": "executed",
            "reason": "required_tool_coverage_guard",
            "arguments": coverage_arguments,
            "result": coverage_result,
        })

    if (
        _has_empty_variant_compare_execution(route["tool"], primary_result, secondary_results)
        and "compare_competitive_set" not in executed_tools
        and "compare_competitive_set" in _text_list(evidence_plan.get("allowedTools"))
    ):
        fallback_tool = "compare_competitive_set"
        fallback_arguments = _build_route_tool_arguments(fallback_tool, arguments, country, question, evidence_plan)
        try:
            fallback_started_at = time.perf_counter()
            fallback_result = call_jato_mcp_tool(fallback_tool, fallback_arguments)
            fallback_latency_ms = int((time.perf_counter() - fallback_started_at) * 1000)
        except Exception as exc:
            try:
                track_tool_call_event(
                    session_id=session_id,
                    country=country,
                    question=question,
                    tool_name=fallback_tool,
                    arguments=fallback_arguments,
                    latency_ms=0,
                    success=False,
                    error=str(exc),
                )
            except Exception:
                pass
            secondary_results.append({
                "path": "variant_empty_competitive_fallback",
                "tool": fallback_tool,
                "status": "failed",
                "reason": "variant_compare_returned_no_subject_or_feature_matrix",
                "arguments": fallback_arguments,
                "error": str(exc),
            })
        else:
            executed_tools.add(fallback_tool)
            try:
                track_tool_call_event(
                    session_id=session_id,
                    country=country,
                    question=question,
                    tool_name=fallback_tool,
                    arguments=fallback_arguments,
                    latency_ms=fallback_latency_ms,
                    success=True,
                    cost_estimate=estimate_tool_call_tokens(fallback_tool, fallback_arguments, fallback_result),
                )
            except Exception:
                pass
            fallback_path = "variant_empty_competitive_fallback"
            executed_results_by_path[fallback_path] = fallback_result
            executed_arguments_by_step[(fallback_path, fallback_tool)] = fallback_arguments
            secondary_results.append({
                "path": fallback_path,
                "tool": fallback_tool,
                "status": "executed",
                "reason": "variant_compare_returned_no_subject_or_feature_matrix",
                "arguments": fallback_arguments,
                "result": fallback_result,
            })

    evidence_item = {
        "tool": primary_result.get("tool"),
        "source": primary_metadata.get("source"),
        "retrievalPath": active_retrieval_path,
        "retrievalPathLabel": retrieval_tool_plan.get("primaryLabel", active_retrieval_path),
        "truncated": bool(primary_metadata.get("truncated")),
        "limitations": primary_metadata.get("limitations") if isinstance(primary_metadata.get("limitations"), list) else [],
    }
    evidence_pack = (
        merge_evidence_pack(executed_results_by_path, retrieval_classification)
        if route_source == "retrieval_router"
        else {
            "items": [evidence_item],
            "sourceCount": 1,
            "sources": [evidence_item["source"]],
            "pathsContributed": [active_retrieval_path],
            "totalPaths": 1,
            "limitations": evidence_item["limitations"],
            "classification": {
                "primaryPath": retrieval_classification["primary"]["path"],
                "allPaths": retrieval_classification["allPaths"],
            },
        }
    )
    tool_result_entries = _build_tool_result_entries(
        primary_tool=route["tool"],
        primary_arguments=tool_arguments,
        primary_result=primary_result,
        secondary_results=secondary_results,
    )
    evidence_package = build_evidence_package(
        session_id=session_id,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        tool_results=tool_result_entries,
    )
    evidence_package = _execute_external_source_repair_tools(
        evidence_package=evidence_package,
        evidence_plan=evidence_plan,
        arguments=arguments,
        country=country,
        question=question,
        session_id=session_id,
        primary_tool=route["tool"],
        primary_arguments=tool_arguments,
        primary_result=primary_result,
        secondary_results=secondary_results,
        executed_tools=executed_tools,
    )
    evidence_pack = {
        **evidence_pack,
        "evidencePackage": evidence_package,
        "businessPlaybook": build_business_playbook_context(
            country=country,
            question=question,
            evidence_plan=evidence_plan,
            evidence_package=evidence_package,
        ),
        "confidence": evidence_package["confidence"],
        "missingEvidence": evidence_package["missingEvidence"],
    }
    deterministic_answer = _compose_agent_answer(
        country=country,
        question=question,
        route=route,
        primary_result=primary_result,
        secondary_results=secondary_results,
        evidence_pack=evidence_pack,
        evidence_plan=evidence_plan,
    )

    # ── AstrBot-style iterative agent loop (ReAct: Think → Act → Observe → Repeat) ──
    agent_loop_result: dict[str, Any] | None = None
    agent_loop_tool_results: list[dict[str, Any]] = []

    def _agent_loop_tool_executor(name: str, args: dict[str, Any]) -> dict[str, Any]:
        safe_args = _safe_tool_arguments(args)
        try:
            result = call_jato_mcp_tool(name, args)
        except Exception as exc:
            agent_loop_tool_results.append({
                "tool": name,
                "arguments": safe_args,
                "status": "failed",
                "error": str(exc),
            })
            raise
        agent_loop_tool_results.append({
            "tool": name,
            "arguments": safe_args,
            "status": "executed",
            "result": result,
        })
        return result

    try:
        agent_loop_result = run_agent_loop(
            country=country,
            question=question,
            profile=profile,
            skill=skill,
            tool_executor=_agent_loop_tool_executor,
            allowed_tools=_text_list(evidence_plan.get("allowedTools")),
            max_rounds=3,
        )
    except Exception:
        agent_loop_result = None

    # Merge: prefer agent loop only after it actually completed a tool-grounded run.
    if _has_successful_agent_loop_answer(agent_loop_result):
        loop_answer = agent_loop_result["answer"]
        answer = {
            "title": loop_answer.get("title", deterministic_answer.get("title", "Agent Analysis")),
            "direct": loop_answer.get("direct", deterministic_answer.get("direct", "")),
            "bullets": loop_answer.get("bullets", deterministic_answer.get("bullets", [])),
            "limitations": loop_answer.get("limitations", deterministic_answer.get("limitations", [])),
            "followUps": normalize_follow_ups(
                loop_answer.get("followUps") or deterministic_answer.get("followUps"),
                country=country,
                question=question,
                tools=_text_list(loop_answer.get("retrievalPaths")) or [route["tool"]],
                evidence_plan=evidence_plan,
            ),
            "confidence": loop_answer.get("confidence", "medium"),
            "composer": "agent_loop",
            "retrievalPaths": loop_answer.get("retrievalPaths", []),
            "toolCount": loop_answer.get("toolCount", 0),
        }
        agent_usage = agent_loop_result.get("usage") or {}
        model_usage = {
            "provider": agent_usage.get("provider", "deepseek"),
            "model": agent_usage.get("model", ASTRBOT_PROVIDER_MODEL),
            "status": agent_usage.get("status", "ok"),
            "promptTokens": agent_usage.get("promptTokens", 0),
            "completionTokens": agent_usage.get("completionTokens", 0),
            "totalTokens": agent_usage.get("totalTokens", 0),
            "estimated": agent_usage.get("estimated", False),
            "agentRounds": agent_loop_result.get("rounds", 0),
        }
        # Merge agent loop tool calls into secondary_results
        for index, tc in enumerate(agent_loop_result.get("toolCalls", [])):
            if tc.get("tool") != route["tool"] and tc.get("status") == "ok":
                captured = agent_loop_tool_results[index] if index < len(agent_loop_tool_results) else {}
                secondary_results.append({
                    "path": "agent_loop",
                    "tool": tc["tool"],
                    "status": "executed",
                    "reason": tc.get("reason", ""),
                    "arguments": _dict_value(captured.get("arguments")) or _dict_value(tc.get("args")) or {},
                    "result": _dict_value(captured.get("result")) or _dict_value(tc.get("result")) or {},
                    "round": tc.get("round", 0),
                })
    else:
        final_answer_result = compose_agent_final_answer(
            country=country,
            question=question,
            profile=profile,
            skill=skill,
            deterministic_answer=deterministic_answer,
            primary_result=primary_result,
            secondary_results=secondary_results,
            evidence_pack=evidence_pack,
        )
        answer = _dict_value(final_answer_result.get("answer")) or deterministic_answer
        model_usage = _dict_value(final_answer_result.get("usage")) or {}

    tool_result_entries = _build_tool_result_entries(
        primary_tool=route["tool"],
        primary_arguments=tool_arguments,
        primary_result=primary_result,
        secondary_results=secondary_results,
    )
    evidence_package = build_evidence_package(
        session_id=session_id,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        tool_results=tool_result_entries,
    )
    evidence_pack = {
        **evidence_pack,
        "evidencePackage": evidence_package,
        "businessPlaybook": build_business_playbook_context(
            country=country,
            question=question,
            evidence_plan=evidence_plan,
            evidence_package=evidence_package,
        ),
        "confidence": evidence_package["confidence"],
        "missingEvidence": evidence_package["missingEvidence"],
    }

    answer = apply_answer_grounding_guard(
        answer,
        evidence_package,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    )
    answer = _with_direct_conclusion_prefix(answer)
    structured_follow_ups = normalize_follow_ups(
        answer.get("followUps") if isinstance(answer, dict) else [],
        country=country,
        question=question,
        tools=_executed_tool_names(route["tool"], secondary_results),
        evidence_plan=evidence_plan,
    )
    answer = {
        **answer,
        "followUps": serialize_follow_ups(structured_follow_ups),
        "structuredFollowUps": structured_follow_ups,
    }
    quality_score = score_deterministic_answer(
        expected={
            "expectedIntent": evidence_plan.get("intent"),
            "mustUseTools": evidence_plan.get("requiredTools", []),
            "expectedFollowUpTypes": evidence_plan.get("followUpTypes", []),
        },
        predicted_intent=str(evidence_plan.get("intent") or ""),
        tools_used=_evidence_package_tool_names(evidence_package),
        answer=answer,
        evidence_package=evidence_package,
        follow_ups=structured_follow_ups,
    )
    llm_quality_score = judge_answer_with_llm(
        question=question,
        answer=answer,
        evidence_package=evidence_package,
        follow_ups=structured_follow_ups,
    )
    charts_data = _chart_specs_from_results(primary_result, secondary_results)
    visual_artifacts = build_visual_artifacts(
        question=question,
        answer=answer,
        evidence_package=evidence_package,
        charts=charts_data,
    )
    answer = {
        **answer,
        "visualArtifacts": visual_artifacts,
    }
    try:
        track_followup_impression(
            session_id=session_id,
            country=country,
            question=question,
            follow_ups=structured_follow_ups,
            intent=str(evidence_plan.get("intent") or ""),
        )
    except Exception:
        pass

    usage_record: dict[str, Any] | None = None
    if _should_track_model_usage(model_usage):
        try:
            usage_record = track_agent_answer_run(
                country=country,
                question=question,
                selected_tool=route["tool"],
                retrieval_paths=_text_list(evidence_pack.get("pathsContributed")),
                tools_used=_executed_tool_names(route["tool"], secondary_results),
                model_usage=model_usage,
            )
            model_usage = {
                **model_usage,
                "usageId": usage_record.get("usageId"),
                "estimatedCostCny": usage_record.get("estimatedCostCny"),
                "currency": usage_record.get("currency"),
                "pricingModel": usage_record.get("pricingModel"),
            }
        except Exception as exc:
            model_usage = {**model_usage, "trackingError": str(exc)}

    # ── Build step-by-step tool plan for display ──
    tool_plan_steps = []
    for step in retrieval_tool_plan.get("steps", []):
        step_path = str(step.get("path") or "")
        step_tool = str(step.get("tool") or "")
        executed_arguments = executed_arguments_by_step.get((step_path, step_tool))
        is_executed = executed_arguments is not None
        tool_plan_steps.append({
            "step": step["step"],
            "tool": step["tool"],
            "path": step["path"],
            "pathLabel": step["pathLabel"],
            "reason": step["reason"],
            "confidence": step["confidence"],
            "status": step.get("status", "active"),
            "executed": is_executed,
            "arguments": _safe_tool_arguments(executed_arguments) if is_executed else step.get("arguments", {}),
        })

    data = {
        "country": country,
        "question": question,
        "profile": {
            "id": profile["id"],
            "name": profile["name"],
            "positioning": profile["positioning"],
        },
        "skill": {
            "id": skill["id"],
            "name": skill["name"],
            "domain": skill["domain"],
            "routeMode": skill["routeMode"],
            "outputContract": skill.get("outputContract", []),
        },
        "route": route,
        "routeSource": route_source,
        "retrievalClassification": {
            "primaryPath": retrieval_classification["primary"]["path"],
            "primaryLabel": retrieval_tool_plan["primaryLabel"],
            "primaryConfidence": retrieval_classification["primary"]["confidence"],
            "primaryReason": retrieval_classification["primary"]["reason"],
            "secondaryPaths": retrieval_classification["secondary"],
            "allPaths": retrieval_classification["allPaths"],
            "decisions": [
                {
                    "path": d["path"],
                    "confidence": d["confidence"],
                    "reason": d["reason"],
                    "signals": d.get("signals", []),
                }
                for d in retrieval_classification["decisions"]
            ],
        },
        "retrievalToolPlan": {
            "primaryPath": retrieval_tool_plan["primaryPath"],
            "primaryLabel": retrieval_tool_plan["primaryLabel"],
            "secondaryPaths": retrieval_tool_plan["secondaryPaths"],
            "totalSteps": retrieval_tool_plan["totalSteps"],
            "activeSteps": retrieval_tool_plan["activeSteps"],
            "plannedSteps": retrieval_tool_plan["plannedSteps"],
            "steps": tool_plan_steps,
        },
        "toolPlan": tool_plan_steps,  # backward-compat alias
        "display": display,
        "answer": answer,
        "visualArtifacts": visual_artifacts,
        "modelUsage": model_usage,
        "evidencePlan": evidence_plan,
        "evidencePackage": evidence_package,
        "qualityScore": quality_score,
        "llmQualityScore": llm_quality_score,
        "evidencePack": evidence_pack,
        "primaryResult": primary_result,
        "secondaryResults": secondary_results,
        "nextActions": _next_actions_for_route(route["tool"]),
    }

    # ── auto-save to agent memory ──
    try:
        primary_data = _dict_value(primary_result.get("data")) or {}
        data_items = _items_from_payload(primary_data)
        evidence_items = evidence_pack.get("items") if isinstance(evidence_pack.get("items"), list) else []
        evidence_count = max(len(evidence_items), len(data_items), 1)
        save_agent_run(
            profile_id=profile["id"],
            skill_id=skill["id"],
            skill_name=skill["name"],
            country=country,
            mode=route["mode"],
            question=question,
            selected_tool=route["tool"],
            route_reason=route["reason"],
            evidence_source=str(primary_metadata.get("source") or "jato"),
            evidence_count=evidence_count,
            display_cards=display.get("cards", []) if isinstance(display, dict) else [],
            result_summary=str(answer.get("direct") or display.get("summary", ""))
            if isinstance(display, dict)
            else str(answer.get("direct") or ""),
            limitations=evidence_pack.get("limitations", []) if isinstance(evidence_pack.get("limitations"), list) else [],
            truncated=bool(evidence_pack.get("truncated")) or bool(primary_metadata.get("truncated")),
            primary_result_tool=primary_result.get("tool") if isinstance(primary_result, dict) else route["tool"],
        )
    except Exception:
        pass  # memory save is best-effort, never block the agent response

    return _tool_payload(
        tool="route_agent_request",
        source="jato_agent_router",
        data=data,
        truncated=bool(primary_metadata.get("truncated")),
        metadata={
            "country": country,
            "question": question,
            "selectedTool": route["tool"],
            "mode": route["mode"],
            "reason": route["reason"],
            "profileId": profile["id"],
            "skillId": skill["id"],
            "retrievalPrimaryPath": retrieval_classification["primary"]["path"],
            "retrievalAllPaths": retrieval_classification["allPaths"],
            "secondaryTools": [
                item["tool"] for item in secondary_results if item.get("status") == "executed"
            ],
            "evidencePathCount": len(evidence_pack.get("pathsContributed", []))
            if isinstance(evidence_pack.get("pathsContributed"), list)
            else 1,
            "routeSource": route_source,
            "answerComposer": answer.get("composer") if isinstance(answer, dict) else None,
            "modelUsageStatus": model_usage.get("status") if isinstance(model_usage, dict) else None,
            "usageId": usage_record.get("usageId") if usage_record else None,
            "evidencePlanIntent": evidence_plan.get("intent"),
            "evidenceConfidence": evidence_package.get("confidence"),
            "qualityTotalScore": quality_score.get("totalScore"),
            "llmQualityStatus": llm_quality_score.get("status"),
        },
    )


def _execute_external_source_repair_tools(
    *,
    evidence_package: dict[str, Any],
    evidence_plan: dict[str, Any],
    arguments: dict[str, Any],
    country: str,
    question: str,
    session_id: str,
    primary_tool: str,
    primary_arguments: dict[str, Any],
    primary_result: dict[str, Any],
    secondary_results: list[dict[str, Any]],
    executed_tools: set[str],
) -> dict[str, Any]:
    current_package = evidence_package
    allowed_tools = set(_text_list(evidence_plan.get("allowedTools")))
    if "search_market_news" not in allowed_tools:
        return current_package

    source_tools = {"search_market_news", "pageindex_search_documents", "minirag_query_graph", "read_web_page"}
    repair_budget = max(0, 2 - len(executed_tools.intersection(source_tools)))
    for tool_name in _external_source_repair_tool_order(question):
        if repair_budget <= 0:
            break
        if not _needs_external_source_repair(current_package, evidence_plan):
            break
        if tool_name in executed_tools or (allowed_tools and tool_name not in allowed_tools):
            continue
        tool_arguments = _build_route_tool_arguments(tool_name, arguments, country, question, evidence_plan)
        try:
            started_at = time.perf_counter()
            tool_result = call_jato_mcp_tool(tool_name, tool_arguments)
            latency_ms = int((time.perf_counter() - started_at) * 1000)
        except Exception as exc:
            try:
                track_tool_call_event(
                    session_id=session_id,
                    country=country,
                    question=question,
                    tool_name=tool_name,
                    arguments=tool_arguments,
                    latency_ms=0,
                    success=False,
                    error=str(exc),
                )
            except Exception:
                pass
            secondary_results.append({
                "path": f"external_source_repair:{tool_name}",
                "tool": tool_name,
                "status": "failed",
                "reason": "external_source_repair",
                "arguments": tool_arguments,
                "error": str(exc),
            })
            executed_tools.add(tool_name)
            repair_budget -= 1
            continue

        executed_tools.add(tool_name)
        try:
            track_tool_call_event(
                session_id=session_id,
                country=country,
                question=question,
                tool_name=tool_name,
                arguments=tool_arguments,
                latency_ms=latency_ms,
                success=True,
                cost_estimate=estimate_tool_call_tokens(tool_name, tool_arguments, tool_result),
            )
        except Exception:
            pass
        secondary_results.append({
            "path": f"external_source_repair:{tool_name}",
            "tool": tool_name,
            "status": "executed",
            "reason": "external_source_repair",
            "arguments": tool_arguments,
            "result": tool_result,
        })
        repair_budget -= 1
        current_package = build_evidence_package(
            session_id=session_id,
            country=country,
            question=question,
            evidence_plan=evidence_plan,
            tool_results=_build_tool_result_entries(
                primary_tool=primary_tool,
                primary_arguments=primary_arguments,
                primary_result=primary_result,
                secondary_results=secondary_results,
            ),
        )
    return current_package


def _external_source_repair_tool_order(question: str) -> list[str]:
    tools = ["search_market_news", "pageindex_search_documents", "minirag_query_graph"]
    if _extract_first_url(question):
        tools.insert(0, "read_web_page")
    return tools[:4]


def _needs_external_source_repair(evidence_package: dict[str, Any], evidence_plan: dict[str, Any]) -> bool:
    missing_names = {
        str(item.get("name") or "")
        for item in evidence_package.get("missingEvidence", [])
        if isinstance(item, dict)
    }
    if any(
        name in {
            "external_research_claims_unavailable",
            "minimum_external_sources",
            "official_source",
            "published_date",
        }
        or name.startswith("target_policy_source:")
        for name in missing_names
    ):
        return True
    intent = str(evidence_plan.get("intent") or "").strip()
    if intent in {"pricing_analysis", "competitor_compare", "report_generation"} and any(
        name in {
            "current_msrp",
            "own_model_price",
            "competitor_price_range",
            "coverage_diagnostic:no_current_prices_for_requested_models",
            "coverage_diagnostic:no_current_prices_for_country",
        }
        for name in missing_names
    ):
        return not _has_source_tool_evidence(current_package=evidence_package)
    if intent not in {
        "market_overview",
        "pricing_analysis",
        "competitor_compare",
        "configuration_analysis",
        "voc_analysis",
        "news_policy_search",
        "report_generation",
    }:
        return False
    return evidence_ref_count(evidence_package) == 0


def _has_source_tool_evidence(*, current_package: dict[str, Any]) -> bool:
    source_tools = {"search_market_news", "pageindex_search_documents", "minirag_query_graph", "read_web_page"}
    tool_results = current_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    for item in tool_results:
        if not isinstance(item, dict):
            continue
        if str(item.get("toolName") or "") not in source_tools:
            continue
        evidence_refs = item.get("evidenceRefs")
        if isinstance(evidence_refs, list) and any(isinstance(ref, dict) for ref in evidence_refs):
            return True
    return False


def query_country_snapshot(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    question = _optional_text(arguments, "question")
    include_sections = _text_list(arguments.get("include_sections")) or DEFAULT_SNAPSHOT_SECTIONS
    intents = _text_list(arguments.get("intents"))
    user_params = extract_user_params(question) if question else {}
    raw_intents = intents or (infer_country_chat_intents(question) if question else [])
    route_plan = (
        _build_country_chat_route(question, user_params, raw_intents)
        if raw_intents
        else {"focusedIntents": [], "intentRoute": "snapshot"}
    )

    snapshot, jato_country = _build_country_snapshot_with_fallback(country, user_params=user_params)
    if route_plan["focusedIntents"]:
        _enrich_snapshot_for_intents(snapshot, route_plan["focusedIntents"], user_params)

    data, truncated = _select_sections(snapshot, include_sections)
    data["country"] = country
    if jato_country != country:
        data["jatoCountry"] = jato_country
    return _tool_payload(
        tool="query_country_snapshot",
        source="jato_country_snapshot",
        data=data,
        truncated=truncated,
        metadata={
            "country": country,
            "jatoCountry": jato_country,
            "question": question or None,
            "intentRoute": route_plan["intentRoute"],
            "focusedIntents": route_plan["focusedIntents"],
            "sections": include_sections,
        },
    )


def query_msrp_pricing(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    jato_country = _jato_data_country(country)
    brand = _optional_text(arguments, "brand") or None
    model = _optional_text(arguments, "model") or None
    models = _text_list(arguments.get("models")) or None
    powertrain = _optional_text(arguments, "powertrain") or None
    max_items = _clamp_int(arguments.get("max_items"), default=12, minimum=1, maximum=50)
    data = msrp_lookup_service.lookup_current_msrp_from_db(
        country=jato_country,
        brand=brand,
        model=model,
        models=models,
        powertrain=powertrain,
        max_items=max_items,
    )
    if isinstance(data, dict) and not data.get("items") and not data.get("coverageDiagnostics"):
        reference_sample = _build_reference_msrp_sample(
            country=jato_country,
            display_country=country,
            brand=brand,
            model=model,
            models=models,
            powertrain=powertrain,
            max_items=max_items,
        )
        data = {
            **data,
            **(
                {
                    "priceStats": reference_sample["priceStats"],
                    "referencePriceSample": reference_sample,
                }
                if reference_sample
                else {}
            ),
            "coverageDiagnostics": _build_empty_msrp_coverage_diagnostics(
                country=country,
                jato_country=jato_country,
                brand=brand,
                model=model,
                models=models,
                powertrain=powertrain,
                reference_sample=reference_sample,
            ),
        }
    if isinstance(data, dict):
        data["country"] = country
        if jato_country != country:
            data["jatoCountry"] = jato_country
    return _tool_payload(
        tool="query_msrp_pricing",
        source="jato_msrp_postgres",
        data=data,
        truncated=False,
        metadata={"country": country, "jatoCountry": jato_country},
    )


def query_leasing_offers(arguments: dict[str, Any]) -> dict[str, Any]:
    question = _optional_text(arguments, "question")
    country = resolve_effective_country(_required_text(arguments, "country"), question)
    country_code = _country_code_for_storage(country)
    brand = _optional_text(arguments, "brand") or None
    requested_models = _dedupe_texts([
        *_text_list(arguments.get("models")),
        *_text_list(arguments.get("competitors")),
        *([_optional_text(arguments, "model")] if _optional_text(arguments, "model") else []),
    ])
    lease_type = _optional_text(arguments, "lease_type") or None
    status = _optional_text(arguments, "status") or None
    term_months = _optional_int(arguments.get("term_months"))
    max_items = _clamp_int(arguments.get("max_items"), default=16, minimum=1, maximum=50)

    items: list[dict[str, Any]] = []
    database_error = ""
    try:
        session_factory = get_session_factory()
        with session_factory() as session:
            if requested_models:
                for requested_model in requested_models:
                    items.extend(lease_comparison_service.list_offers(
                        session,
                        country=country_code,
                        brand=brand,
                        model_name=requested_model,
                        lease_type=lease_type,
                        status=status,
                    ))
            else:
                items = lease_comparison_service.list_offers(
                    session,
                    country=country_code,
                    brand=brand,
                    lease_type=lease_type,
                    status=status,
                )
    except Exception as exc:
        database_error = type(exc).__name__

    items = _dedupe_leasing_offers(items)
    if term_months is not None:
        items = [item for item in items if _optional_int(item.get("termMonths")) == term_months]
    items = items[:max_items]
    coverage_diagnostics: dict[str, Any] = {
        "diagnosis": "leasing_offers_available" if items else (
            "leasing_store_unavailable" if database_error else "no_leasing_offers_for_requested_scope"
        ),
        "requested": {
            "country": country,
            "countryCode": country_code,
            "brand": brand,
            "models": requested_models,
            "leaseType": lease_type,
            "status": status,
            "termMonths": term_months,
        },
        "offerRows": len(items),
    }
    if database_error:
        coverage_diagnostics["errorType"] = database_error
    if not items:
        coverage_diagnostics["nextActions"] = [
            "Load current lease offers with source URL, effective/expiry dates, term, mileage, and monthly payment.",
            "Add residual value or total contract cost before making a firm TCO or company-car conclusion.",
        ]
    data = {
        "country": country,
        "countryCode": country_code,
        "items": items,
        "leasingStats": _leasing_offer_stats(items),
        "coverageDiagnostics": coverage_diagnostics,
        "summary": (
            f"{len(items)} leasing offers returned for {country}."
            if items
            else f"No citation-ready leasing offers were available for {country}."
        ),
    }
    return _tool_payload(
        tool="query_leasing_offers",
        source="jato_lease_offer_postgres",
        data=data,
        truncated=False,
        metadata={
            "country": country,
            "countryCode": country_code,
            "resultCount": len(items),
            "status": "ok" if items else "insufficient_data",
        },
    )


def _country_code_for_storage(country: str) -> str:
    normalized = resolve_effective_country(country, country)
    for code, name in COUNTRY_CODE_ALIASES.items():
        if len(code) == 2 and name.casefold() == normalized.casefold():
            return code
    value = str(country or "").strip().upper()
    return value if len(value) == 2 else normalized.upper()


def _dedupe_leasing_offers(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        key = str(item.get("offerId") or "").strip() or repr(sorted(item.items()))
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _leasing_offer_stats(items: list[dict[str, Any]]) -> dict[str, Any]:
    def values(key: str) -> list[float]:
        return [
            float(item[key])
            for item in items
            if isinstance(item.get(key), (int, float))
        ]

    monthly = values("effectiveMonthlyEur") or values("monthlyPaymentEur")
    residual_percent = values("residualValuePercent")
    total_cost = values("totalContractCostEur")
    stats: dict[str, Any] = {"offerCount": len(items)}
    for prefix, samples in (
        ("monthlyPaymentEur", monthly),
        ("residualValuePercent", residual_percent),
        ("totalContractCostEur", total_cost),
    ):
        if not samples:
            continue
        stats[f"{prefix}Min"] = min(samples)
        stats[f"{prefix}Max"] = max(samples)
        stats[f"{prefix}Average"] = round(sum(samples) / len(samples), 2)
    return stats


def _build_empty_msrp_coverage_diagnostics(
    *,
    country: str,
    jato_country: str,
    brand: str | None,
    model: str | None,
    models: list[str] | None,
    powertrain: str | None,
    reference_sample: dict[str, Any] | None = None,
) -> dict[str, Any]:
    requested_models = _dedupe_texts([*(models or []), *([model] if model else [])])
    diagnosis = "no_current_prices_for_requested_models" if requested_models else "no_current_prices_for_country"
    if requested_models:
        next_actions = [
            f"Add or map current MSRP rows for {', '.join(requested_models)} in {country}.",
            "Include trim/version, currency, source URL, and retrieved date before making final numeric price claims.",
        ]
    else:
        next_actions = [
            f"Load current MSRP rows for {country} or connect the pricing data source.",
            "Include model, trim/version, currency, source URL, and retrieved date before using MSRP as evidence.",
        ]
    diagnostics: dict[str, Any] = {
        "diagnosis": diagnosis,
        "requested": {
            "country": country,
            **({"jatoCountry": jato_country} if jato_country != country else {}),
            "brand": brand,
            "model": model,
            "models": requested_models,
            "powertrain": powertrain,
        },
        "currentPriceRows": {
            "requestedCountry": 0,
            "requestedModels": 0 if requested_models else None,
        },
        "nextActions": next_actions,
    }
    if reference_sample:
        diagnostics["referencePriceSample"] = {
            "dataStatus": "reference_price_sample",
            "scope": reference_sample.get("scope") or {},
            "sampleCount": reference_sample.get("sampleCount", 0),
            "sampleModels": reference_sample.get("sampleModels") or [],
            "priceStats": reference_sample.get("priceStats") or {},
            "limitations": reference_sample.get("limitations") or [],
        }
    return diagnostics


def _build_reference_msrp_sample(
    *,
    country: str,
    display_country: str,
    brand: str | None,
    model: str | None,
    models: list[str] | None,
    powertrain: str | None,
    max_items: int,
) -> dict[str, Any]:
    requested_models = _dedupe_texts([*(models or []), *([model] if model else [])])
    if not requested_models:
        return {}
    sample = msrp_lookup_service.lookup_current_msrp_from_db(
        country=country,
        brand=brand,
        model=None,
        models=None,
        powertrain=powertrain,
        max_items=max(6, min(max_items, 12)),
    )
    items = sample.get("items") if isinstance(sample, dict) else []
    rows = [item for item in items if isinstance(item, dict)]
    fallback_scope = ""
    price_stats = _price_stats_from_msrp_items(rows)
    if not price_stats and brand:
        market_sample = msrp_lookup_service.lookup_current_msrp_from_db(
            country=country,
            brand=None,
            model=None,
            models=None,
            powertrain=powertrain,
            max_items=max(6, min(max_items, 12)),
        )
        market_items = market_sample.get("items") if isinstance(market_sample, dict) else []
        market_rows = [item for item in market_items if isinstance(item, dict)]
        market_price_stats = _price_stats_from_msrp_items(market_rows)
        if market_price_stats:
            rows = market_rows
            price_stats = market_price_stats
            fallback_scope = "brand_sample_empty_market_reference_used"
    if not price_stats:
        return {}
    sample_models = _dedupe_texts([
        str(item.get("model") or "")
        for item in rows
        if str(item.get("model") or "").strip()
    ])
    return {
        "dataStatus": "reference_price_sample",
        "scope": {
            "country": display_country,
            **({"jatoCountry": country} if country != display_country else {}),
            "brand": brand if not fallback_scope else None,
            **({"requestedBrand": brand, "fallbackScope": fallback_scope} if fallback_scope else {}),
            "powertrain": powertrain,
        },
        "requestedModelsMissing": requested_models,
        "sampleCount": len(rows),
        "sampleModels": sample_models[:8],
        "priceStats": price_stats,
        "items": rows[:8],
        "limitations": [
            "Reference sample only: requested model MSRP rows are still missing.",
            "Use this corridor for preliminary positioning, not as the official price of the requested model.",
            *(
                [f"No current MSRP sample was available for brand {brand}; this is a market-level reference sample."]
                if fallback_scope and brand
                else []
            ),
        ],
    }


def _price_stats_from_msrp_items(items: list[dict[str, Any]]) -> dict[str, float | int | str]:
    prices = [
        float(item.get("msrp"))
        for item in items
        if isinstance(item.get("msrp"), (int, float)) and float(item.get("msrp") or 0) > 0
    ]
    if not prices:
        return {}
    ordered = sorted(prices)
    first_currency = next(
        (
            str(item.get("currency") or "").strip()
            for item in items
            if str(item.get("currency") or "").strip()
        ),
        "",
    )
    stats: dict[str, float | int | str] = {
        "min": ordered[0],
        "max": ordered[-1],
        "avg": round(sum(ordered) / len(ordered), 2),
        "median": ordered[len(ordered) // 2],
        "count": len(ordered),
    }
    if first_currency:
        stats["currency"] = first_currency
    return stats


def search_market_news(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    question = _required_text(arguments, "question")
    limit = _clamp_int(arguments.get("limit"), default=6, minimum=1, maximum=10)
    intent = infer_research_intent(question, _optional_text(arguments, "intent"))
    started_at = time.perf_counter()
    data = {
        "items": web_search_service.search_market_news(
            country=country,
            question=question,
            limit=limit,
        ),
    }
    latency_ms = round((time.perf_counter() - started_at) * 1000)
    data = apply_research_governance(
        data,
        intent=intent,
        question=question,
        research_mode="quick",
        latency_ms=latency_ms,
    )
    return _tool_payload(
        tool="search_market_news",
        source="jato_web_search_service",
        data=data,
        truncated=False,
        metadata={"country": country, "question": question, "limit": limit, "intent": intent},
    )


def _build_jato_cross_check(*, country: str, question: str) -> dict[str, Any]:
    try:
        user_params = extract_user_params(question)
        snapshot, _jato_country = _build_country_snapshot_with_fallback(
            country,
            user_params=user_params,
        )
    except Exception as exc:
        return {
            "status": "unavailable",
            "summary": f"JATO structured snapshot unavailable: {str(exc)[:160]}",
            "checks": [],
            "internalEvidence": {},
        }

    kpis = _dict_value(snapshot.get("kpis")) or {}
    top_models = snapshot.get("topModels") if isinstance(snapshot.get("topModels"), list) else []
    powertrain_mix = snapshot.get("powertrainMix") if isinstance(snapshot.get("powertrainMix"), list) else []
    checks: list[dict[str, Any]] = []
    if _has_usable_market_kpis(kpis):
        checks.append({
            "name": "market_kpis",
            "status": "available",
            "detail": f"{len(kpis)} KPI fields available for structured cross-check.",
        })
    if top_models:
        first = _dict_value(top_models[0]) or {}
        checks.append({
            "name": "top_models",
            "status": "available",
            "detail": (
                f"{len(top_models)} model ranking rows; top visible model: "
                f"{first.get('label') or first.get('model') or 'n/a'}."
            ),
        })
    if powertrain_mix:
        checks.append({
            "name": "powertrain_mix",
            "status": "available",
            "detail": f"{len(powertrain_mix)} powertrain mix rows available.",
        })

    internal_evidence = {
        "periodLabel": snapshot.get("periodLabel") or snapshot.get("resolvedPeriod") or "",
        "kpiKeys": list(kpis.keys())[:8],
        "topModelCount": len(top_models),
        "powertrainGroupCount": len(powertrain_mix),
    }
    if top_models:
        first = _dict_value(top_models[0]) or {}
        internal_evidence["topModel"] = (
            first.get("label") or first.get("model") or first.get("name") or ""
        )

    status = "matched" if checks else "not_available"
    summary = (
        "Public-source findings can be cross-checked against JATO market KPIs, top models, and powertrain mix."
        if checks
        else "No lightweight JATO market context was available for this source-search question."
    )
    return {
        "status": status,
        "summary": summary,
        "checks": checks,
        "internalEvidence": internal_evidence,
        "conflictRisk": "manual_review_required" if checks else "none",
    }


def _has_usable_market_kpis(kpis: dict[str, Any]) -> bool:
    if not kpis:
        return False
    weak_count_keys = {"totalrows", "countrycount", "brandcount", "modelcount", "versioncount"}
    for key, value in kpis.items():
        normalized_key = str(key or "").strip().replace("_", "").lower()
        numeric = _numeric_value(value)
        if normalized_key in weak_count_keys and numeric == 0:
            continue
        if isinstance(value, bool) or value is None:
            continue
        if numeric is not None:
            if numeric != 0:
                return True
            continue
        if str(value or "").strip():
            return True
    return False


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def read_web_page(arguments: dict[str, Any]) -> dict[str, Any]:
    url = _required_text(arguments, "url")
    question = _optional_text(arguments, "question")
    max_chars = _clamp_int(arguments.get("max_chars"), default=6000, minimum=1000, maximum=20_000)
    data = browser_read_web_page(url, question=question, max_chars=max_chars)
    return _tool_payload(
        tool="read_web_page",
        source="jato_browser_readonly",
        data=data,
        truncated=bool(data.get("truncated")),
        metadata={
            "url": data.get("url", url),
            "question": question,
            "status": data.get("status", "ok"),
            "httpStatus": data.get("httpStatus"),
            "contentType": data.get("contentType"),
            "readonly": True,
            "limitations": data.get("limitations") if isinstance(data.get("limitations"), list) else [],
        },
    )


def browser_snapshot(arguments: dict[str, Any]) -> dict[str, Any]:
    url = _required_text(arguments, "url")
    question = _optional_text(arguments, "question")
    max_chars = _clamp_int(arguments.get("max_chars"), default=6000, minimum=1000, maximum=20_000)
    timeout_ms = _clamp_int(arguments.get("timeout_ms"), default=12_000, minimum=1000, maximum=30_000)
    capture_screenshot = _bool_argument(arguments.get("capture_screenshot"), default=False)
    data = browser_capture_snapshot(
        url,
        question=question,
        max_chars=max_chars,
        capture_screenshot=capture_screenshot,
        timeout_ms=timeout_ms,
    )
    return _tool_payload(
        tool="browser_snapshot",
        source="jato_browser_snapshot",
        data=data,
        truncated=bool(data.get("truncated")),
        metadata={
            "url": data.get("url", url),
            "question": question,
            "status": data.get("status", "ok"),
            "browserEngine": data.get("browserEngine"),
            "readonly": True,
            "limitations": data.get("limitations") if isinstance(data.get("limitations"), list) else [],
        },
    )


def browser_interaction_plan(arguments: dict[str, Any]) -> dict[str, Any]:
    url = _required_text(arguments, "url")
    action_goal = _optional_text(arguments, "action_goal") or _optional_text(arguments, "actionGoal")
    max_actions = _clamp_int(arguments.get("max_actions"), default=6, minimum=1, maximum=12)
    timeout_ms = _clamp_int(arguments.get("timeout_ms"), default=12_000, minimum=1000, maximum=30_000)
    data = browser_plan_interaction(
        url,
        action_goal=action_goal,
        max_actions=max_actions,
        timeout_ms=timeout_ms,
    )
    return _tool_payload(
        tool="browser_interaction_plan",
        source="jato_browser_interaction",
        data=data,
        truncated=False,
        metadata={
            "url": data.get("url", url),
            "actionGoal": action_goal,
            "status": data.get("status", "ok"),
            "browserEngine": data.get("browserEngine"),
            "requiresUserApproval": True,
            "actionCount": len(data.get("actions", [])) if isinstance(data.get("actions"), list) else 0,
            "limitations": data.get("limitations") if isinstance(data.get("limitations"), list) else [],
        },
    )


def browser_click_confirmed(arguments: dict[str, Any]) -> dict[str, Any]:
    url = _required_text(arguments, "url")
    action_id = _required_text(arguments, "action_id")
    confirmation_token = _required_text(arguments, "confirmation_token")
    max_chars = _clamp_int(arguments.get("max_chars"), default=6000, minimum=1000, maximum=20_000)
    timeout_ms = _clamp_int(arguments.get("timeout_ms"), default=12_000, minimum=1000, maximum=30_000)
    data = browser_confirm_click(
        url,
        action_id=action_id,
        confirmation_token=confirmation_token,
        timeout_ms=timeout_ms,
        max_chars=max_chars,
    )
    return _tool_payload(
        tool="browser_click_confirmed",
        source="jato_browser_interaction",
        data=data,
        truncated=bool(data.get("truncated")),
        metadata={
            "url": data.get("url", url),
            "resultUrl": data.get("resultUrl"),
            "actionId": action_id,
            "status": data.get("status", "ok"),
            "confirmedAction": True,
            "limitations": data.get("limitations") if isinstance(data.get("limitations"), list) else [],
        },
    )


def browser_type_confirmed(arguments: dict[str, Any]) -> dict[str, Any]:
    url = _required_text(arguments, "url")
    action_id = _required_text(arguments, "action_id")
    confirmation_token = _required_text(arguments, "confirmation_token")
    text = _required_text(arguments, "text")
    max_chars = _clamp_int(arguments.get("max_chars"), default=6000, minimum=1000, maximum=20_000)
    timeout_ms = _clamp_int(arguments.get("timeout_ms"), default=12_000, minimum=1000, maximum=30_000)
    data = browser_confirm_type(
        url,
        action_id=action_id,
        confirmation_token=confirmation_token,
        text=text,
        timeout_ms=timeout_ms,
        max_chars=max_chars,
    )
    return _tool_payload(
        tool="browser_type_confirmed",
        source="jato_browser_interaction",
        data=data,
        truncated=bool(data.get("truncated")),
        metadata={
            "url": data.get("url", url),
            "resultUrl": data.get("resultUrl"),
            "actionId": action_id,
            "status": data.get("status", "ok"),
            "confirmedAction": True,
            "typedCharacters": data.get("typedCharacters"),
            "limitations": data.get("limitations") if isinstance(data.get("limitations"), list) else [],
        },
    )


def compare_vehicle_variants(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    data = engineering_variant_diff_service.compare_market_variants_from_db(
        country=country,
        brand=_optional_text(arguments, "brand") or None,
        model=_optional_text(arguments, "model") or None,
        models=_text_list(arguments.get("models")) or None,
        powertrain=_optional_text(arguments, "powertrain") or None,
        compare_subjects=_dict_list(arguments.get("compare_subjects")) or None,
        max_subjects=_clamp_int(arguments.get("max_subjects"), default=3, minimum=2, maximum=5),
        max_diff_features=_clamp_int(arguments.get("max_diff_features"), default=16, minimum=1, maximum=40),
        max_common_features=_clamp_int(arguments.get("max_common_features"), default=8, minimum=0, maximum=20),
    )
    return _tool_payload(
        tool="compare_vehicle_variants",
        source="jato_variant_diff_service",
        data=data,
        truncated=False,
        metadata={"country": country},
    )


def build_market_chart(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    question = _optional_text(arguments, "question")
    chart_sections = _text_list(arguments.get("include_sections")) or DEFAULT_CHART_SECTIONS
    deck, jato_country = _build_country_chart_deck_with_fallback(
        country=country,
        question=question,
        intents=_text_list(arguments.get("intents")) or None,
        extracted_params=_dict_value(arguments.get("extracted_params")),
        selected_year=_optional_int(arguments.get("selected_year")),
        selected_model=_optional_text(arguments, "selected_model") or None,
        model_top_n=_optional_int(arguments.get("model_top_n")),
    )
    snapshot = deck.get("contextSnapshot") if isinstance(deck, dict) else {}
    snapshot_data, truncated = _select_sections(
        snapshot if isinstance(snapshot, dict) else {},
        chart_sections,
    )

    # ── Phase 3: Build chart specs for frontend rendering ──
    chart_spec_result = build_chart_spec_from_deck(deck if isinstance(deck, dict) else {})

    data = {
        "country": country,
        "jatoCountry": jato_country,
        "question": deck.get("question"),
        "primaryIntent": deck.get("primaryIntent"),
        "intents": deck.get("intents"),
        "deckIntents": deck.get("deckIntents"),
        "intentRoute": deck.get("intentRoute"),
        "controls": deck.get("controls"),
        "extractedParams": deck.get("extractedParams"),
        "contextSnapshot": snapshot_data,
        "chartSpecs": chart_spec_result,
    }
    return _tool_payload(
        tool="build_market_chart",
        source="jato_country_chart_deck",
        data=data,
        truncated=truncated,
        metadata={
            "country": country,
            "jatoCountry": jato_country,
            "question": question or None,
            "sections": chart_sections,
            "chartCount": chart_spec_result.get("chartCount", 0),
            "primaryChart": chart_spec_result.get("primaryChart"),
        },
    )


# ── Phase 4: PageIndex tool stubs (with real hybrid_rag fallback) ──


def pageindex_search_documents(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    question = _required_text(arguments, "question")
    limit = _clamp_int(arguments.get("limit"), default=6, minimum=1, maximum=10)

    # Try real PageIndex client first
    if pageindex_configured():
        try:
            result = pageindex_search(query=question, top_k=limit)
            if result.get("status") == "ok":
                sections = result.get("sections", [])
                return _tool_payload(
                    tool="pageindex_search_documents",
                    source="pageindex_mcp",
                    data={
                        "status": "live",
                        "country": country,
                        "question": question,
                        "sections": sections,
                        "availableDocuments": len(sections),
                        "summary": f"PageIndex document search returned {len(sections)} sections.",
                    },
                    truncated=False,
                    metadata={
                        "country": country,
                        "question": question,
                        "status": "live",
                        "resultCount": len(sections),
                        "limitations": [],
                    },
                )
        except Exception:
            pass  # Fall through to fallback

    # Fallback: web search
    news_items = web_search_service.search_market_news(
        country=country, question=f"policy document regulation {question}", limit=limit,
    )
    sections = []
    for item in (news_items if isinstance(news_items, list) else []):
        sections.append({
            "title": item.get("title", ""),
            "source": item.get("provider") or item.get("source") or "web",
            "url": item.get("url", ""),
            "snippet": item.get("snippet") or item.get("summary", ""),
        })
    return _tool_payload(
        tool="pageindex_search_documents",
        source="pageindex_fallback_hybrid_rag",
        data={
            "status": "fallback",
            "country": country, "question": question,
            "sections": sections, "availableDocuments": len(sections),
            "fallback": "hybrid_rag",
            "summary": f"PageIndex not connected — web search fallback. {len(sections)} results.",
        },
        truncated=False,
        metadata={
            "country": country, "question": question,
            "status": "fallback", "fallbackPath": "hybrid_rag",
            "resultCount": len(sections),
            "limitations": ["PageIndex API key not configured — results from web search"],
        },
    )


def pageindex_get_section(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    question = _required_text(arguments, "question")
    section_id = _optional_text(arguments, "section_id") or _optional_text(arguments, "sectionId") or question

    if pageindex_configured():
        try:
            result = pageindex_section(section_id)
            if result.get("status") == "ok":
                citations = result.get("citations", [])
                return _tool_payload(
                    tool="pageindex_get_section",
                    source="pageindex_mcp",
                    data={
                        "status": "live",
                        "country": country,
                        "question": question,
                        "sectionId": section_id,
                        "text": result.get("text", ""),
                        "citations": citations,
                        "summary": result.get("summary") or "PageIndex section retrieved.",
                    },
                    truncated=False,
                    metadata={
                        "country": country,
                        "question": question,
                        "sectionId": section_id,
                        "status": "live",
                        "citationCount": len(citations) if isinstance(citations, list) else 0,
                        "limitations": [],
                    },
                )
        except Exception:
            pass

    # Fallback: search for specific clause/section content
    news_items = web_search_service.search_market_news(
        country=country,
        question=f"section clause {question}",
        limit=5,
    )
    citations = []
    for item in (news_items if isinstance(news_items, list) else []):
        citations.append({
            "title": item.get("title", ""),
            "url": item.get("url", ""),
            "source": item.get("provider") or item.get("source") or "web",
        })
    return _tool_payload(
        tool="pageindex_get_section",
        source="pageindex_fallback_hybrid_rag",
        data={
            "status": "fallback",
            "country": country,
            "question": question,
            "text": "",
            "citations": citations,
            "summary": f"PageIndex section retrieval not yet available — found {len(citations)} related web sources.",
        },
        truncated=False,
        metadata={
            "country": country,
            "question": question,
            "status": "fallback",
            "limitations": ["PageIndex server not yet connected — citations from web search"],
        },
    )


def pageindex_list_documents(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    if pageindex_configured():
        try:
            result = pageindex_list()
            if result.get("status") == "ok":
                documents = result.get("documents", [])
                return _tool_payload(
                    tool="pageindex_list_documents",
                    source="pageindex_mcp",
                    data={
                        "status": "live",
                        "country": country,
                        "documents": documents,
                        "summary": result.get("summary") or f"Found {len(documents)} indexed documents.",
                    },
                    truncated=False,
                    metadata={
                        "country": country,
                        "status": "live",
                        "documentCount": len(documents) if isinstance(documents, list) else 0,
                        "limitations": [],
                    },
                )
        except Exception:
            pass

    # Fallback: return placeholder — actual document index requires PageIndex server
    return _tool_payload(
        tool="pageindex_list_documents",
        source="pageindex_fallback",
        data={
            "status": "fallback",
            "country": country,
            "documents": [
                {"id": "placeholder", "title": "Document indexing requires PageIndex server connection", "pages": 0},
            ],
            "summary": "PageIndex document listing requires server connection. Use pageindex_search_documents to search for policy content via web fallback.",
        },
        truncated=False,
        metadata={
            "country": country,
            "status": "fallback",
            "limitations": ["PageIndex server not yet connected"],
        },
    )


# ── Phase 5: MiniRAG tool stubs (with real multi-tool fallback) ──


def minirag_query_graph(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    question = _required_text(arguments, "question")

    # Try real MiniRAG client first
    if minirag_configured():
        try:
            result = minirag_query(question=question, top_k=5)
            if result.get("status") == "ok":
                return _tool_payload(
                    tool="minirag_query_graph",
                    source="minirag_live",
                    data={
                        "status": "live",
                        "country": country, "question": question,
                        "paths": result.get("paths", []),
                        "entities": result.get("entities", []),
                        "supportingChunks": result.get("supportingChunks", []),
                        "summary": result.get("summary", ""),
                    },
                    truncated=False,
                    metadata={
                        "country": country, "question": question,
                        "status": "live",
                        "hopCount": len(result.get("paths", [])),
                        "entityCount": len(result.get("entities", [])),
                        "chunkCount": len(result.get("supportingChunks", [])),
                        "limitations": [],
                    },
                )
        except Exception:
            pass  # Fall through to fallback

    # Fallback: multi-tool chain — snapshot + news
    paths = []
    entities = []
    chunks = []
    try:
        snap = query_country_snapshot({"country": country, "question": question})
        snap_data = snap.get("data") if isinstance(snap.get("data"), dict) else {}
        snap_kpis = snap_data.get("kpis") if isinstance(snap_data.get("kpis"), dict) else {}
        entities.append({
            "type": "market_snapshot", "country": country,
            "kpis": {k: snap_kpis[k] for k in list(snap_kpis.keys())[:6]},
            "source": "jato_country_snapshot",
        })
        paths.append({"hop": 1, "from": "question", "to": "country_market_data", "tool": "query_country_snapshot"})
    except Exception:
        pass
    try:
        news_items = web_search_service.search_market_news(country=country, question=question, limit=5)
        for item in (news_items if isinstance(news_items, list) else []):
            chunks.append({"title": item.get("title", ""), "source": item.get("provider") or item.get("source") or "web", "url": item.get("url", "")})
            entities.append({"type": "news_evidence", "title": item.get("title", ""), "source": item.get("provider") or "web"})
        paths.append({"hop": 2, "from": "country_market_data", "to": "policy_news_evidence", "tool": "search_market_news"})
    except Exception:
        pass

    return _tool_payload(
        tool="minirag_query_graph",
        source="minirag_fallback_multi_tool",
        data={
            "status": "fallback", "country": country, "question": question,
            "paths": paths, "entities": entities, "supportingChunks": chunks,
            "fallback": "multi_tool_chain",
            "summary": f"MiniRAG not connected — {len(paths)}-hop fallback chain. {len(entities)} entities, {len(chunks)} chunks.",
        },
        truncated=False,
        metadata={
            "country": country, "question": question,
            "status": "fallback", "fallbackPath": "multi_tool_chain",
            "hopCount": len(paths), "entityCount": len(entities), "chunkCount": len(chunks),
            "limitations": ["MiniRAG not connected — multi-tool chain fallback"],
        },
    )


def minirag_explain_entity(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    question = _required_text(arguments, "question")
    entity = _optional_text(arguments, "entity") or _optional_text(arguments, "entity_name") or question

    if minirag_configured():
        try:
            result = minirag_explain(entity)
            if result.get("status") == "ok":
                related = result.get("relatedEntities", [])
                evidence = result.get("evidence", [])
                return _tool_payload(
                    tool="minirag_explain_entity",
                    source="minirag_live",
                    data={
                        "status": "live",
                        "country": country,
                        "question": question,
                        "entity": entity,
                        "relatedEntities": related,
                        "evidence": evidence,
                        "summary": result.get("summary") or f"MiniRAG returned {len(related)} related entities.",
                    },
                    truncated=False,
                    metadata={
                        "country": country,
                        "question": question,
                        "entity": entity,
                        "status": "live",
                        "relatedCount": len(related) if isinstance(related, list) else 0,
                        "evidenceCount": len(evidence) if isinstance(evidence, list) else 0,
                        "limitations": [],
                    },
                )
        except Exception:
            pass

    # Fallback: search for entity-specific context
    news_items = web_search_service.search_market_news(
        country=country, question=question, limit=5,
    )
    related = []
    for item in (news_items if isinstance(news_items, list) else []):
        related.append({
            "entity": item.get("title", ""),
            "relationship": "mentioned_in",
            "source": item.get("provider") or item.get("source") or "web",
            "url": item.get("url", ""),
        })
    return _tool_payload(
        tool="minirag_explain_entity",
        source="minirag_fallback_news",
        data={
            "status": "fallback",
            "country": country,
            "question": question,
            "relatedEntities": related,
            "evidence": [{"source": r["source"], "url": r["url"]} for r in related],
            "summary": f"MiniRAG entity explanation not yet available — found {len(related)} related references via web search.",
        },
        truncated=False,
        metadata={
            "country": country,
            "question": question,
            "status": "fallback",
            "limitations": ["MiniRAG server not yet connected — results from web search"],
        },
    )


def minirag_update_corpus(arguments: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(arguments, "country")
    return _tool_payload(
        tool="minirag_update_corpus",
        source="minirag_fallback",
        data={
            "status": "fallback",
            "country": country,
            "updateStats": {"added": 0, "updated": 0, "removed": 0},
            "summary": "MiniRAG corpus update requires server connection. Graph will be built when MiniRAG is connected.",
        },
        truncated=False,
        metadata={
            "country": country,
            "status": "fallback",
            "limitations": ["MiniRAG server not yet connected"],
        },
    )


# ── Cross-reference MCP tools ──


def analyze_model_performance(arguments: dict[str, Any]) -> dict[str, Any]:
    """Deep-dive: sales + MSRP + variant config + news for one model, cross-referenced."""
    country = _required_text(arguments, "country")
    question = _optional_text(arguments, "question")
    model_filter = _optional_text(arguments, "model") or _extract_model_from_question(question)

    findings: dict[str, Any] = {}
    sources: list[str] = []
    limitations: list[str] = []

    # 1. Sales data from snapshot
    try:
        snap = query_country_snapshot({"country": country, "question": question or f"{model_filter} sales {country}"})
        snap_data = snap.get("data") if isinstance(snap.get("data"), dict) else {}
        top_models = snap_data.get("topModels") if isinstance(snap_data.get("topModels"), list) else []
        findings["sales"] = {
            "rankings": [m for m in top_models if model_filter.lower() in str(m.get("label", "")).lower()][:5] if model_filter else top_models[:5],
            "totalModels": len(top_models),
        }
        sources.append("jato_country_snapshot")
    except Exception:
        limitations.append("Sales data unavailable")

    # 2. MSRP pricing
    try:
        msrp = query_msrp_pricing({"country": country, "model": model_filter} if model_filter else {"country": country})
        msrp_data = msrp.get("data") if isinstance(msrp.get("data"), dict) else {}
        msrp_items = msrp_data.get("items") if isinstance(msrp_data.get("items"), list) else []
        findings["pricing"] = {
            "records": msrp_items[:8],
            "count": len(msrp_items),
        }
        sources.append("jato_msrp_postgres")
    except Exception:
        limitations.append("Pricing data unavailable")

    # 3. Variant config
    try:
        variant_args = {"country": country}
        if model_filter:
            variant_args["model"] = model_filter
        variant = compare_vehicle_variants(variant_args)
        variant_data = variant.get("data") if isinstance(variant.get("data"), dict) else {}
        findings["variants"] = {
            "subjects": variant_data.get("subjects") or variant_data.get("compareSubjects") or [],
            "diffFeatures": variant_data.get("diffFeatures") or [],
            "commonFeatures": variant_data.get("commonFeatures") or [],
        }
        sources.append("jato_variant_diff_service")
    except Exception:
        limitations.append("Variant data unavailable")

    # 4. Relevant news
    try:
        news_query = f"{model_filter} {country} sales performance" if model_filter else f"automotive market {country}"
        news = search_market_news({"country": country, "question": news_query})
        news_data = news.get("data") if isinstance(news.get("data"), dict) else {}
        news_items = news_data.get("items") if isinstance(news_data.get("items"), list) else []
        findings["news"] = {
            "items": [{"title": n.get("title", ""), "source": n.get("provider") or n.get("source", ""), "url": n.get("url", "")} for n in news_items[:5]],
            "count": len(news_items),
        }
        sources.append("jato_web_search_service")
    except Exception:
        pass

    return _tool_payload(
        tool="analyze_model_performance",
        source="jato_cross_reference",
        data={
            "country": country,
            "question": question,
            "model": model_filter or "auto-detected",
            "findings": findings,
            "crossReference": {
                "sources": sources,
                "analyzable": len(sources) >= 2,
                "summary": f"Cross-referenced {len(sources)} data sources: {', '.join(sources)}. {'Ready for root cause analysis.' if len(sources) >= 2 else 'Limited cross-reference available.'}",
            },
        },
        truncated=False,
        metadata={
            "country": country, "question": question, "model": model_filter,
            "sources": sources, "limitations": limitations,
        },
    )


def compare_competitive_set(arguments: dict[str, Any]) -> dict[str, Any]:
    """Compare a model against its segment competitors across sales, price, and features."""
    country = _required_text(arguments, "country")
    question = _required_text(arguments, "question")
    explicit_models = _text_list(arguments.get("models")) or _extract_model_candidates_from_question(question)
    explicit_competitors = _text_list(arguments.get("competitors"))
    model_filter = _optional_text(arguments, "model") or (explicit_models[0] if explicit_models else _extract_model_from_question(question))

    competitors: list[dict[str, Any]] = []
    sources: list[str] = []
    top_models: list[dict[str, Any]] = []

    try:
        snap = query_country_snapshot({"country": country, "question": question})
        snap_data = snap.get("data") if isinstance(snap.get("data"), dict) else {}
        raw_top_models = snap_data.get("topModels") if isinstance(snap_data.get("topModels"), list) else []
        top_models = [item for item in raw_top_models if isinstance(item, dict)]
        if top_models:
            sources.append("jato_country_snapshot")
    except Exception:
        top_models = []

    nearby: list[dict[str, Any]] = []
    if explicit_competitors:
        nearby = [
            {
                "label": competitor,
                "value": _snapshot_model_value(top_models, competitor),
                "source": "explicit_competitor_pool",
                "basis": "competitor seed from question or user-material playbook",
                **_snapshot_model_rank_payload(top_models, competitor),
            }
            for competitor in explicit_competitors
            if competitor and not _same_model_name(competitor, model_filter)
        ][:8]
        if nearby:
            if "explicit_competitor_pool" not in sources:
                sources.insert(0, "explicit_competitor_pool")

    # 1. Get top models from snapshot to identify competitors when no explicit pool exists.
    if not nearby:
        try:
            # Find the target model and its neighbors
            target_rank = None
            for i, m in enumerate(top_models):
                if model_filter.lower() in str(m.get("label", "")).lower():
                    target_rank = i
                    break
            if target_rank is not None:
                start = max(0, target_rank - 2)
                end = min(len(top_models), target_rank + 5)
                nearby = top_models[start:end]
            else:
                nearby = top_models[:8]
        except Exception:
            nearby = []

    # 2. For each competitor, get pricing
    for model in nearby[:6]:
        model_name = str(model.get("label", ""))
        comp: dict[str, Any] = {"model": model_name, "sales": model.get("value", 0)}
        if model.get("source"):
            comp["source"] = model.get("source")
        if model.get("basis"):
            comp["basis"] = model.get("basis")
        if model.get("rank"):
            comp["rank"] = model.get("rank")
        if model.get("snapshotLabel"):
            comp["snapshotLabel"] = model.get("snapshotLabel")
        try:
            msrp = query_msrp_pricing({"country": country, "model": model_name})
            msrp_data = msrp.get("data") if isinstance(msrp.get("data"), dict) else {}
            msrp_items = msrp_data.get("items") if isinstance(msrp_data.get("items"), list) else []
            if msrp_items:
                prices = [item.get("msrp") or item.get("price") or item.get("retailPrice") for item in msrp_items if isinstance(item, dict)]
                prices_valid = [p for p in prices if isinstance(p, (int, float))]
                if prices_valid:
                    comp["avgPrice"] = sum(prices_valid) / len(prices_valid)
                    comp["minPrice"] = min(prices_valid)
                    comp["maxPrice"] = max(prices_valid)
                    comp["priceRecords"] = len(prices_valid)
            else:
                comp.update(_price_source_status_from_msrp_data(msrp_data, model_name))
        except Exception:
            pass
        competitors.append(comp)

    if not competitors and explicit_models:
        for model_name in explicit_models[:6]:
            if _same_model_name(model_name, model_filter):
                continue
            competitors.append({
                "model": model_name,
                "source": "user_question_model_candidate",
                "basis": "explicitly named in the user question",
            })
        if competitors:
            sources.append("user_question_model_candidates")

    if "jato_msrp_postgres" not in sources:
        sources.append("jato_msrp_postgres")

    return _tool_payload(
        tool="compare_competitive_set",
        source="jato_cross_reference",
        data={
            "country": country,
            "question": question,
            "targetModel": model_filter,
            "competitors": competitors,
            "analysis": {
                "sourceCount": len(sources),
                "totalCompared": len(competitors),
                "hasPricing": any("avgPrice" in c for c in competitors),
            },
        },
        truncated=False,
        metadata={"country": country, "model": model_filter, "competitorCount": len(competitors), "sources": sources},
    )


def _price_source_status_from_msrp_data(msrp_data: dict[str, Any], model_name: str) -> dict[str, Any]:
    diagnostics = (
        msrp_data.get("coverageDiagnostics")
        if isinstance(msrp_data.get("coverageDiagnostics"), dict)
        else {}
    )
    if not diagnostics:
        return {"priceEvidenceStatus": "current_price_missing"}

    candidate = _source_repair_candidate_for_model(diagnostics, model_name)
    if not candidate:
        return {
            "priceEvidenceStatus": str(diagnostics.get("diagnosis") or "current_price_missing"),
            "priceEvidenceNextStep": _first_text(diagnostics.get("nextActions")) or "Load current MSRP rows before making exact price claims.",
        }

    status = str(candidate.get("draftStatus") or "").strip() or (
        "current_price_available"
        if int(candidate.get("currentPriceRows") or 0) > 0
        else "current_price_missing"
    )
    result: dict[str, Any] = {
        "priceEvidenceStatus": status,
        "priceEvidenceRole": str(candidate.get("sourceCategory") or "").strip(),
        "priceEvidenceNextStep": str(
            candidate.get("materializationNextStep")
            or candidate.get("sourceSearchQuery")
            or _first_text(diagnostics.get("nextActions"))
            or "Validate official source, trim/version, currency and retrieved date before using MSRP."
        ).strip(),
    }
    for key in (
        "sourceDraftPath",
        "candidateDomain",
        "candidateSourceType",
        "sourceUrl",
        "sourceSearchQuery",
        "materializationStatus",
        "materializationReviewStatus",
        "materializationReadinessScore",
        "reviewPendingRows",
        "reviewPendingStatus",
        "currentPriceRows",
    ):
        value = candidate.get(key)
        if value not in (None, "", []):
            result[key] = value
    return result


def _source_repair_candidate_for_model(diagnostics: dict[str, Any], model_name: str) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    source_candidates = diagnostics.get("sourceRepairCandidates")
    if isinstance(source_candidates, dict):
        for key in ("ownModel", "competitorCorridor"):
            values = source_candidates.get(key)
            if isinstance(values, list):
                candidates.extend(item for item in values if isinstance(item, dict))
    matches = [
        candidate
        for candidate in candidates
        if _source_repair_candidate_matches_model(candidate, model_name)
    ]
    if not matches:
        return {}
    return sorted(matches, key=_source_repair_candidate_priority, reverse=True)[0]


def _source_repair_candidate_matches_model(candidate: dict[str, Any], model_name: str) -> bool:
    values = [
        str(candidate.get(key) or "")
        for key in ("model", "fixedModel", "fixedJatoModel", "sourceCode", "relativePath", "sourceDraftPath")
    ]
    return any(_model_name_matches_loose(value, model_name) for value in values if value.strip())


def _source_repair_candidate_priority(candidate: dict[str, Any]) -> tuple[int, int, int]:
    status = str(candidate.get("draftStatus") or "").strip()
    status_score = {
        "current_price_materialized": 40,
        "source_draft_available": 30,
        "candidate_search_query": 20,
    }.get(status, 10)
    pending = int(candidate.get("reviewPendingRows") or 0)
    readiness = candidate.get("materializationReadinessScore")
    readiness_score = int(readiness) if isinstance(readiness, (int, float)) else 0
    return status_score, pending, readiness_score


def _first_text(value: Any) -> str:
    if isinstance(value, list):
        for item in value:
            text = str(item or "").strip()
            if text:
                return text
        return ""
    return str(value or "").strip()


def _snapshot_model_value(top_models: list[dict[str, Any]], model_name: str) -> Any:
    match = _snapshot_model_match(top_models, model_name)
    if not match:
        return 0
    return match.get("value") or match.get("sales") or match.get("registrations") or 0


def _snapshot_model_rank_payload(top_models: list[dict[str, Any]], model_name: str) -> dict[str, Any]:
    match = _snapshot_model_match(top_models, model_name)
    if not match:
        return {}
    result: dict[str, Any] = {
        "snapshotLabel": str(match.get("label") or match.get("model") or "").strip(),
    }
    rank = match.get("rank")
    if isinstance(rank, (int, float)):
        result["rank"] = int(rank)
    return {key: value for key, value in result.items() if value not in ("", None)}


def _snapshot_model_match(top_models: list[dict[str, Any]], model_name: str) -> dict[str, Any]:
    for item in top_models:
        label = str(item.get("label") or item.get("model") or "").strip()
        if _model_name_matches_loose(label, model_name):
            return item
    return {}


def _model_name_matches_loose(left: str, right: str) -> bool:
    def normalize(value: str) -> str:
        return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())

    left_norm = normalize(left)
    right_norm = normalize(right)
    if not left_norm or not right_norm:
        return False
    if left_norm == right_norm:
        return True
    longer, shorter = (left_norm, right_norm) if len(left_norm) > len(right_norm) else (right_norm, left_norm)
    return len(shorter) >= 3 and longer.endswith(shorter)


def analyze_market_dynamics(arguments: dict[str, Any]) -> dict[str, Any]:
    """Cross-reference snapshot trends + news + pricing to identify what's changing."""
    country = _required_text(arguments, "country")
    question = _required_text(arguments, "question")

    dynamics: dict[str, Any] = {}
    sources: list[str] = []

    # 1. Snapshot for trend data (year-over-year changes)
    try:
        snap = query_country_snapshot({"country": country, "question": question})
        snap_data = snap.get("data") if isinstance(snap.get("data"), dict) else {}
        dynamics["marketSnapshot"] = {
            "kpis": snap_data.get("kpis", {}),
            "yearSeries": snap_data.get("yearSeries", [])[-3:],
            "monthSeries": snap_data.get("monthSeries", [])[-6:],
            "powertrainMix": snap_data.get("powertrainMix", []),
        }
        sources.append("jato_country_snapshot")
    except Exception:
        pass

    # 2. News/policy updates
    try:
        news = search_market_news({"country": country, "question": f"market changes policy update {question}"})
        news_data = news.get("data") if isinstance(news.get("data"), dict) else {}
        news_items = news_data.get("items") if isinstance(news_data.get("items"), list) else []
        dynamics["newsAndPolicy"] = {
            "items": [{"title": n.get("title", ""), "source": n.get("provider", ""), "url": n.get("url", "")} for n in news_items[:6]],
            "count": len(news_items),
        }
        sources.append("jato_web_search_service")
    except Exception:
        pass

    # 3. Pricing shifts (sample top models for price changes)
    try:
        msrp = query_msrp_pricing({"country": country, "max_items": 12})
        msrp_data = msrp.get("data") if isinstance(msrp.get("data"), dict) else {}
        msrp_items = msrp_data.get("items") if isinstance(msrp_data.get("items"), list) else []
        dynamics["priceSample"] = {
            "items": msrp_items[:8],
            "count": len(msrp_items),
        }
        if "jato_msrp_postgres" not in sources:
            sources.append("jato_msrp_postgres")
    except Exception:
        pass

    return _tool_payload(
        tool="analyze_market_dynamics",
        source="jato_cross_reference",
        data={
            "country": country,
            "question": question,
            "dynamics": dynamics,
            "crossReference": {
                "sources": sources,
                "summary": f"Cross-referenced {len(sources)} sources to identify market dynamics.",
            },
        },
        truncated=False,
        metadata={"country": country, "question": question, "sources": sources},
    )


# ── Filter/lens query tool wrappers ──


def _call_query_with_filters(args: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(args, "country")
    jato_country = _jato_data_country(country)
    data = query_with_filters(
        country=jato_country,
        powertrain=_optional_text(args, "powertrain"),
        fuel_type=_optional_text(args, "fuel_type"),
        segment=_optional_text(args, "segment"),
        brand=_optional_text(args, "brand"),
        model=_optional_text(args, "model"),
        year=_optional_int(args.get("year")),
        metric=_optional_text(args, "metric") or "sales",
        top_n=_clamp_int(args.get("top_n"), default=10, minimum=1, maximum=30),
    )
    data["country"] = country
    if jato_country != country:
        data["jatoCountry"] = jato_country
    return _tool_payload(
        tool="query_with_filters",
        source="jato_filtered_query",
        data=data,
        truncated=False,
        metadata={"country": country, "jatoCountry": jato_country, "filters": data.get("appliedFilters", {})},
    )


def _call_query_time_series(args: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(args, "country")
    jato_country = _jato_data_country(country)
    data = query_time_series(
        country=jato_country,
        metric=_optional_text(args, "metric") or "sales",
        powertrain=_optional_text(args, "powertrain"),
        fuel_type=_optional_text(args, "fuel_type"),
        segment=_optional_text(args, "segment"),
        year=_optional_int(args.get("year")),
        granularity=_optional_text(args, "granularity") or "monthly",
    )
    data["country"] = country
    if jato_country != country:
        data["jatoCountry"] = jato_country
    return _tool_payload(
        tool="query_time_series",
        source="jato_time_series",
        data=data,
        truncated=False,
        metadata={"country": country, "jatoCountry": jato_country, "metric": data.get("metric"), "granularity": data.get("granularity")},
    )


def _call_query_segment_breakdown(args: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(args, "country")
    jato_country = _jato_data_country(country)
    data = query_segment_breakdown(
        country=jato_country,
        segment=_optional_text(args, "segment"),
        powertrain=_optional_text(args, "powertrain"),
        year=_optional_int(args.get("year")),
    )
    data["country"] = country
    if jato_country != country:
        data["jatoCountry"] = jato_country
    return _tool_payload(
        tool="query_segment_breakdown",
        source="jato_segment_breakdown",
        data=data,
        truncated=False,
        metadata={"country": country, "jatoCountry": jato_country, "filters": data.get("appliedFilters", {})},
    )


def _call_query_price_positioning(args: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(args, "country")
    jato_country = _jato_data_country(country)
    data = query_price_positioning(
        country=jato_country,
        model=_optional_text(args, "model"),
        brand=_optional_text(args, "brand"),
        powertrain=_optional_text(args, "powertrain"),
        top_n=_clamp_int(args.get("top_n"), default=10, minimum=1, maximum=30),
    )
    data["country"] = country
    if jato_country != country:
        data["jatoCountry"] = jato_country
    return _tool_payload(
        tool="query_price_positioning",
        source="jato_price_positioning",
        data=data,
        truncated=False,
        metadata={"country": country, "jatoCountry": jato_country, "filters": data.get("appliedFilters", {})},
    )


def _call_query_competitive_landscape(args: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(args, "country")
    jato_country = _jato_data_country(country)
    model = _required_text(args, "model")
    data = query_competitive_landscape(
        country=jato_country,
        model=model,
        include_pricing=args.get("include_pricing", True) not in (False, "false", "0"),
        include_features=args.get("include_features", True) not in (False, "false", "0"),
        competitor_count=_clamp_int(args.get("competitor_count"), default=5, minimum=2, maximum=10),
    )
    data["country"] = country
    if jato_country != country:
        data["jatoCountry"] = jato_country
    return _tool_payload(
        tool="query_competitive_landscape",
        source="jato_competitive_landscape",
        data=data,
        truncated=False,
        metadata={"country": country, "jatoCountry": jato_country, "model": model, "competitorCount": data.get("competitorCount", 0)},
    )


def _call_query_powertrain_trend(args: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(args, "country")
    powertrain = _optional_text(args, "powertrain") or _optional_text(args, "type") or "BEV"
    question = f"{powertrain} sales trend {country}"
    deck, jato_country = _build_country_chart_deck_with_fallback(country=country, question=question)
    snapshot = deck.get("contextSnapshot", {}) if isinstance(deck, dict) else {}
    pm = snapshot.get("powertrainMix", []) if isinstance(snapshot, dict) else []
    pt_data = [p for p in pm if powertrain.lower() in str(p.get("label", "")).lower()] if isinstance(pm, list) else []
    year_series = snapshot.get("yearSeries", []) if isinstance(snapshot, dict) else []
    month_series = snapshot.get("monthSeries", []) if isinstance(snapshot, dict) else []
    return _tool_payload(
        tool="query_powertrain_trend", source="jato_powertrain_trend",
        data={"country": country, "jatoCountry": jato_country, "powertrain": powertrain, "powertrainData": pt_data[:5],
              "yearSeries": year_series[-5:] if isinstance(year_series, list) else [],
              "monthSeries": month_series[-12:] if isinstance(month_series, list) else [],
              "kpis": snapshot.get("kpis", {}) if isinstance(snapshot, dict) else {}},
        truncated=False, metadata={"country": country, "jatoCountry": jato_country, "powertrain": powertrain},
    )


def _call_query_brand_deep_dive(args: dict[str, Any]) -> dict[str, Any]:
    country = _required_text(args, "country")
    brand = _required_text(args, "brand")
    question = f"{brand} performance analysis {country}"
    snap = query_country_snapshot({"country": country, "question": question})
    snap_data = snap.get("data", {}) if isinstance(snap.get("data"), dict) else {}
    top_brands = snap_data.get("topBrands", []) if isinstance(snap_data.get("topBrands"), list) else []
    top_models = snap_data.get("topModels", []) if isinstance(snap_data.get("topModels"), list) else []
    brand_data = next((b for b in top_brands if brand.lower() in str(b.get("label", "")).lower()), None)
    brand_models = [m for m in top_models if brand.lower() in str(m.get("label", "")).lower()][:8]
    msrp_items: list[dict[str, Any]] = []
    try:
        msrp = query_msrp_pricing({"country": country, "brand": brand})
        msrp_data = msrp.get("data", {}) if isinstance(msrp.get("data"), dict) else {}
        msrp_items = msrp_data.get("items", []) if isinstance(msrp_data.get("items"), list) else []
    except Exception:
        pass
    return _tool_payload(
        tool="query_brand_deep_dive", source="jato_brand_deep_dive",
        data={"country": country, "brand": brand, "brandRanking": brand_data,
              "topModels": brand_models, "modelCount": len(brand_models),
              "pricing": msrp_items[:6],
              "kpis": snap_data.get("kpis", {})},
        truncated=False, metadata={"country": country, "brand": brand},
    )


def _call_query_cross_country(args: dict[str, Any]) -> dict[str, Any]:
    countries_str = _required_text(args, "countries")
    question = _required_text(args, "question")
    country_list = [c.strip() for c in countries_str.split(",") if c.strip()]
    if len(country_list) < 2:
        country_list = ["Sweden", "Norway"]
    if len(country_list) > 5:
        country_list = country_list[:5]
    results: dict[str, Any] = {}
    for c in country_list:
        try:
            snap = query_country_snapshot({"country": c, "question": question})
            snap_data = snap.get("data", {}) if isinstance(snap.get("data"), dict) else {}
            results[c] = {"kpis": snap_data.get("kpis", {}), "powertrainMix": snap_data.get("powertrainMix", [])[:5],
                          "topModels": snap_data.get("topModels", [])[:5]}
        except Exception:
            results[c] = {"error": "unavailable"}
    return _tool_payload(
        tool="query_cross_country", source="jato_cross_country",
        data={"countries": country_list, "question": question, "comparison": results,
              "countryCount": len(country_list)},
        truncated=False, metadata={"countries": country_list},
    )


def _extract_model_from_question(question: str) -> str:
    known_models = [
        "OMODA 9", "OMODA9", "OMODA 5", "OMODA5", "JAECOO J7", "JAECOO J8",
        "EX40", "EX30", "EX90", "XC60", "XC90", "XC40", "Model Y", "Model 3",
        "ID.7", "ID.4", "ID.3", "Kodiaq", "Tiguan", "RAV4", "i4", "Enyaq",
        "Sportage", "Sorento", "EV3", "EV9", "Corolla Cross", "C-HR", "Qashqai",
        "J8", "J7", "O9", "O5", "EX60", "e-tron", "EQB", "iX1", "500e", "Spring",
    ]
    for m in known_models:
        if m.upper() in question.upper():
            return m
    match = re.search(r"\b(?:O|J|EX|XC|ID|EQB|IX)\s?[-.]?\s?\d{1,2}\b", question, flags=re.IGNORECASE)
    if match:
        return re.sub(r"\s+", " ", match.group(0).replace("-", "").replace(".", "")).upper()
    return ""


def _extract_model_candidates_from_question(question: str) -> list[str]:
    known_models = [
        "OMODA 9", "OMODA9", "OMODA 5", "OMODA5", "JAECOO J7", "JAECOO J8",
        "Corolla Cross", "Model Y", "Model 3", "Kia Sportage", "Kia EV3", "Kia EV9",
        "Toyota RAV4", "Toyota C-HR", "Nissan Qashqai", "Volvo EX30", "Volvo EX60",
        "Volvo XC60", "Skoda Kodiaq", "Peugeot 5008", "Hyundai Tucson",
        "Sportage", "Sorento", "EX40", "EX30", "EX90", "XC60", "XC90", "XC40",
        "ID.7", "ID.4", "ID.3", "Kodiaq", "Tiguan", "RAV4", "Enyaq",
        "J8", "J7", "O9", "O5", "EX60", "EV3", "EV9", "Qashqai", "C-HR",
    ]
    result: list[str] = []
    upper = question.upper()
    for model in known_models:
        if model.upper() not in upper:
            continue
        if any(_same_model_name(model, item) for item in result):
            continue
        result.append(model)
    pattern = re.compile(r"\b(?:O|J|EX|XC|ID|EQB|IX|EV)\s?[-.]?\s?\d{1,2}\b", flags=re.IGNORECASE)
    for match in pattern.findall(question):
        value = re.sub(r"\s+", " ", str(match).replace("-", "").replace(".", "")).upper()
        if value and not any(_same_model_name(value, item) for item in result):
            result.append(value)
    return result[:8]


def _same_model_name(left: str, right: str) -> bool:
    def normalize(value: str) -> str:
        return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())

    left_norm = normalize(left)
    right_norm = normalize(right)
    return bool(left_norm and right_norm and left_norm == right_norm)


def _build_agent_display(
    route: dict[str, str],
    primary_result: dict[str, Any],
    country: str,
    question: str,
    profile: dict[str, Any],
    skill: dict[str, Any],
) -> dict[str, Any]:
    primary_data = _dict_value(primary_result.get("data")) or {}
    primary_metadata = _dict_value(primary_result.get("metadata")) or {}
    cards = [
        {"label": "Profile", "value": str(profile.get("id") or "default")},
        {"label": "Skill", "value": str(skill.get("id") or "auto_route")},
        {"label": "Country", "value": country},
        {"label": "Selected tool", "value": route["tool"]},
        {"label": "Evidence source", "value": str(primary_metadata.get("source") or "jato")},
    ]
    summary = f"Routed to {route['tool']} because {route['reason']}."

    if route["tool"] == "build_market_chart":
        snapshot = _dict_value(primary_data.get("contextSnapshot")) or {}
        kpis = _dict_value(snapshot.get("kpis")) or {}
        cards.extend(_cards_from_kpis(kpis))
        cards.append({"label": "Chart context", "value": _section_count(snapshot)})
        summary = "Chart-ready market context is available for rendering and explanation."
    elif route["tool"] == "query_country_snapshot":
        kpis = _dict_value(primary_data.get("kpis")) or {}
        cards.extend(_cards_from_kpis(kpis))
        cards.append({"label": "Snapshot sections", "value": _section_count(primary_data)})
        summary = "Country market snapshot is ready for grounded answer composition."
    elif route["tool"] == "query_msrp_pricing":
        items = _items_from_payload(primary_data)
        cards.append({"label": "Price records", "value": str(len(items))})
        first_item = _dict_value(items[0]) if items else None
        if first_item:
            cards.append({"label": "First match", "value": _join_nonempty([first_item.get("brand"), first_item.get("model")])})
        summary = "MSRP pricing records are ready for price positioning analysis."
    elif route["tool"] == "compare_vehicle_variants":
        subjects = primary_data.get("subjects") or primary_data.get("compareSubjects") or primary_data.get("items")
        cards.append({"label": "Variant subjects", "value": str(len(subjects) if isinstance(subjects, list) else 0)})
        summary = "Variant comparison evidence is ready for feature difference analysis."
    elif route["tool"] == "search_market_news":
        items = _items_from_payload(primary_data)
        cards.append({"label": "News results", "value": str(len(items))})
        first_item = _dict_value(items[0]) if items else None
        if first_item:
            cards.append({"label": "Top source", "value": str(first_item.get("provider") or first_item.get("source") or "news")})
        summary = "Market news evidence is ready for policy or event interpretation."

    elif route["tool"] == "read_web_page":
        links = primary_data.get("links") if isinstance(primary_data.get("links"), list) else []
        headings = primary_data.get("headings") if isinstance(primary_data.get("headings"), list) else []
        title = _short_text(primary_data.get("title"), 60)
        cards.append({"label": "HTTP status", "value": _display_value(primary_data.get("httpStatus") or "n/a")})
        cards.append({"label": "Headings", "value": str(len(headings))})
        cards.append({"label": "Links", "value": str(len(links))})
        if title:
            cards.append({"label": "Page title", "value": title})
        summary = "Static public page text is available for grounded summarization; no browser interaction was performed."

    elif route["tool"] == "browser_snapshot":
        links = primary_data.get("links") if isinstance(primary_data.get("links"), list) else []
        headings = primary_data.get("headings") if isinstance(primary_data.get("headings"), list) else []
        screenshot = primary_data.get("screenshot") if isinstance(primary_data.get("screenshot"), dict) else None
        cards.append({"label": "Snapshot status", "value": str(primary_data.get("status") or "unknown")})
        cards.append({"label": "Browser engine", "value": str(primary_data.get("browserEngine") or "n/a")})
        cards.append({"label": "Headings", "value": str(len(headings))})
        cards.append({"label": "Links", "value": str(len(links))})
        if screenshot:
            cards.append({"label": "Screenshot", "value": "captured"})
        summary = "Read-only browser snapshot evidence is available; no click, typing, login, or form action was performed."

    elif route["tool"] == "pageindex_search_documents":
        sections = primary_data.get("sections") if isinstance(primary_data.get("sections"), list) else []
        cards.append({"label": "Doc sections", "value": str(len(sections))})
        if sections:
            first = _dict_value(sections[0]) if sections else None
            if first:
                cards.append({"label": "Top result", "value": str(first.get("title") or "n/a")[:50]})
        summary = f"Document search via fallback: {len(sections)} policy/news sections found."

    elif route["tool"] == "minirag_query_graph":
        entities = primary_data.get("entities") if isinstance(primary_data.get("entities"), list) else []
        chunks = primary_data.get("supportingChunks") if isinstance(primary_data.get("supportingChunks"), list) else []
        paths_list = primary_data.get("paths") if isinstance(primary_data.get("paths"), list) else []
        cards.append({"label": "Entities found", "value": str(len(entities))})
        cards.append({"label": "Supporting chunks", "value": str(len(chunks))})
        cards.append({"label": "Multi-hop steps", "value": str(len(paths_list))})
        summary = f"Multi-hop query via fallback chain: {len(paths_list)} hops, {len(entities)} entities, {len(chunks)} chunks."



    return {
        "title": "Agent route ready",
        "summary": summary,
        "question": question,
        "skill": {
            "id": skill.get("id"),
            "name": skill.get("name"),
            "description": skill.get("description"),
            "outputContract": skill.get("outputContract", []),
        },
        "cards": cards[:8],
    }


def _compose_agent_answer(
    *,
    country: str,
    question: str,
    route: dict[str, str],
    primary_result: dict[str, Any],
    secondary_results: list[dict[str, Any]],
    evidence_pack: dict[str, Any],
    evidence_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    primary_tool = str(primary_result.get("tool") or route["tool"])
    primary_data = _dict_value(primary_result.get("data")) or {}
    primary_metadata = _dict_value(primary_result.get("metadata")) or {}
    primary_source = str(primary_metadata.get("source") or "jato")
    bullets = _answer_bullets_for_tool(primary_tool, primary_data, primary_metadata)
    secondary_bullets: list[str] = []

    for item in secondary_results:
        if item.get("status") != "executed":
            continue
        result = _dict_value(item.get("result")) or {}
        secondary_tool = str(result.get("tool") or item.get("tool") or "")
        result_data = _dict_value(result.get("data")) or {}
        result_metadata = _dict_value(result.get("metadata")) or {}
        secondary_bullets.extend(_answer_bullets_for_tool(secondary_tool, result_data, result_metadata)[:2])

    all_bullets = _dedupe_texts([*bullets, *secondary_bullets])[:6]
    if not all_bullets:
        all_bullets = [
            f"已将问题路由到 {primary_tool}，但当前返回的数据不足以形成更细的业务判断。",
        ]

    direct = _direct_answer_text(
        country=country,
        question=question,
        tool=primary_tool,
        source=primary_source,
        bullets=all_bullets,
        evidence_pack=evidence_pack,
    )

    citations = _answer_citations(primary_result)
    for item in secondary_results:
        if item.get("status") == "executed":
            citations.extend(_answer_citations(_dict_value(item.get("result")) or {}))

    limitations = _text_list(evidence_pack.get("limitations"))
    paths = _text_list(evidence_pack.get("pathsContributed"))
    return {
        "title": "Grounded answer",
        "direct": direct,
        "bullets": all_bullets,
        "citations": citations[:8],
        "limitations": limitations[:6],
        "followUps": _answer_follow_ups(
            country=country,
            question=question,
            tool=primary_tool,
            evidence_pack=evidence_pack,
            evidence_plan=evidence_plan or build_evidence_plan(country, question),
        ),
        "confidence": _answer_confidence(evidence_pack),
        "retrievalPaths": paths,
        "sourceCount": evidence_pack.get("sourceCount", 0),
        "tool": primary_tool,
        "question": question,
    }


def _direct_answer_text(
    *,
    country: str,
    question: str,
    tool: str,
    source: str,
    bullets: list[str],
    evidence_pack: dict[str, Any],
) -> str:
    source_count = evidence_pack.get("sourceCount", 0)
    paths = _text_list(evidence_pack.get("pathsContributed"))
    evidence_text = f"{source_count} 个来源" if source_count else source
    path_text = " + ".join(paths) if paths else tool
    first_bullet = bullets[0] if bullets else "当前证据不足，需要补充数据后再判断。"
    return (
        f"直接结论：针对 {country} 的问题“{question}”，当前应优先依据 {tool} 的结果判断。"
        f" 本次使用 {path_text} 路径，证据来自 {evidence_text}。{first_bullet}"
    )


def _answer_bullets_for_tool(
    tool: str,
    data: dict[str, Any],
    metadata: dict[str, Any],
) -> list[str]:
    if tool == "build_market_chart":
        return _chart_answer_bullets(data, metadata)
    if tool == "query_country_snapshot":
        return _snapshot_answer_bullets(data)
    if tool == "query_msrp_pricing":
        return _pricing_answer_bullets(data)
    if tool == "search_market_news":
        return _news_answer_bullets(data)
    if tool == "read_web_page":
        return _web_page_answer_bullets(data)
    if tool == "browser_snapshot":
        return _browser_snapshot_answer_bullets(data)
    if tool == "compare_vehicle_variants":
        return _variant_answer_bullets(data)
    if tool == "pageindex_search_documents":
        return _pageindex_answer_bullets(data)
    if tool == "minirag_query_graph":
        return _minirag_answer_bullets(data)
    return [str(data.get("summary") or metadata.get("source") or f"{tool} 已返回结果。")]


def _chart_answer_bullets(data: dict[str, Any], metadata: dict[str, Any]) -> list[str]:
    snapshot = _dict_value(data.get("contextSnapshot")) or {}
    kpis = _dict_value(snapshot.get("kpis")) or {}
    bullets = _kpi_bullets(kpis)
    chart_specs = _dict_value(data.get("chartSpecs")) or {}
    chart_count = chart_specs.get("chartCount") or metadata.get("chartCount")
    if chart_count:
        bullets.append(f"已生成 {chart_count} 个可重渲染图表 artifact，可在前端直接查看。")
    period_label = snapshot.get("periodLabel") or data.get("primaryIntent")
    if period_label:
        bullets.append(f"当前图表上下文范围：{period_label}。")
    return bullets


def _snapshot_answer_bullets(data: dict[str, Any]) -> list[str]:
    bullets = _kpi_bullets(_dict_value(data.get("kpis")) or {})
    top_brands = data.get("topBrands")
    if isinstance(top_brands, list) and top_brands:
        first_brand = _dict_value(top_brands[0]) or {}
        label = _join_nonempty([first_brand.get("brand"), first_brand.get("name")])
        if label != "n/a":
            bullets.append(f"品牌结构中首位可见对象是 {label}。")
    powertrain_mix = data.get("powertrainMix")
    if isinstance(powertrain_mix, list) and powertrain_mix:
        bullets.append(f"动力类型结构返回 {len(powertrain_mix)} 个分组，可继续追问 BEV/PHEV/HEV 差异。")
    return bullets or [f"国家快照返回 {_section_count(data)}，可用于市场概览判断。"]


def _pricing_answer_bullets(data: dict[str, Any]) -> list[str]:
    items = _items_from_payload(data)
    bullets = [f"当前命中 {len(items)} 条 MSRP 价格记录。"]
    if items:
        first = _dict_value(items[0]) or {}
        label = _join_nonempty([first.get("brand"), first.get("model"), first.get("version")])
        price = first.get("msrp") or first.get("price") or first.get("amount")
        if label != "n/a" and price not in (None, ""):
            bullets.append(f"首条可见价格记录：{label}，价格字段为 {_display_value(price)}。")
        elif label != "n/a":
            bullets.append(f"首条可见价格记录：{label}。")
    return bullets


def _news_answer_bullets(data: dict[str, Any]) -> list[str]:
    items = _items_from_payload(data)
    bullets = [f"当前召回 {len(items)} 条外部新闻/政策证据。"]
    for item in items[:3]:
        record = _dict_value(item) or {}
        title = _short_text(record.get("title"), 120)
        source = record.get("provider") or record.get("source")
        if title:
            bullets.append(f"{title}" + (f"（{source}）" if source else ""))
    return bullets


def _research_answer_bullets(data: dict[str, Any]) -> list[str]:
    items = _items_from_payload(data)
    coverage = _dict_value(data.get("sourceCoverage")) or {}
    cross_check = _dict_value(data.get("jatoCrossCheck")) or {}
    governance = _dict_value(data.get("researchGovernance")) or {}
    metrics = _dict_value(governance.get("metrics")) or {}
    summary = _short_text(data.get("summary") or data.get("answer"), 220)
    bullets = [
        f"External research returned {len(items)} governed source candidates across {coverage.get('domainCount') or 0} domains; source relevance avg {coverage.get('averageSourceScore') or 0}/100.",
    ]
    if governance:
        bullets.append(
            f"Research governance: {governance.get('policyStatus', 'unknown')} policy; mode {metrics.get('queryCount', 0)} queries / {metrics.get('sourcesUsed', len(items))} used sources / estimated cost {metrics.get('estimatedCost', 0)}."
        )
    if summary:
        bullets.append(summary)
    if cross_check:
        bullets.append(f"JATO cross-check: {cross_check.get('status', 'unknown')} — {_short_text(cross_check.get('summary'), 160)}")
    for item in items[:3]:
        record = _dict_value(item) or {}
        title = _short_text(record.get("title"), 120)
        source = record.get("source") or record.get("provider")
        citation_id = record.get("citationId") or f"R{record.get('rank') or ''}".strip()
        score = record.get("sourceScore")
        category = record.get("sourceCategory")
        claim = _short_text(record.get("supportedClaim"), 120)
        if title:
            score_text = f"，score {score}/100" if isinstance(score, int) else ""
            category_text = f"，{category}" if category else ""
            claim_text = f" Claim: {claim}" if claim else ""
            bullets.append(f"[{citation_id}] {title}" + (f"（{source}{category_text}{score_text}）" if source else score_text) + claim_text)
    insight_cards = data.get("insightCards") if isinstance(data.get("insightCards"), list) else []
    if insight_cards:
        bullets.append(f"Insight cards prepared: {len(insight_cards)} report-ready claims.")
    return bullets


def _web_page_answer_bullets(data: dict[str, Any]) -> list[str]:
    title = _short_text(data.get("title"), 120)
    headings = data.get("headings") if isinstance(data.get("headings"), list) else []
    links = data.get("links") if isinstance(data.get("links"), list) else []
    text_preview = _short_text(data.get("textPreview"), 180)
    bullets = [f"已读取公开网页：{title or data.get('url') or '未命名页面'}。"]
    if headings:
        bullets.append(f"页面包含 {len(headings)} 个 H1-H3 标题，首个标题是：{_short_text(headings[0], 100)}。")
    if text_preview:
        bullets.append(f"静态文本预览：{text_preview}")
    bullets.append(f"提取到 {len(links)} 个链接；该工具不会执行 JavaScript、登录、点击或表单提交。")
    return bullets


def _browser_snapshot_answer_bullets(data: dict[str, Any]) -> list[str]:
    bullets = _web_page_answer_bullets(data)
    status = str(data.get("status") or "unknown")
    engine = str(data.get("browserEngine") or "n/a")
    screenshot = data.get("screenshot")
    bullets.append(f"浏览器快照状态：{status}，引擎：{engine}。")
    if isinstance(screenshot, dict) and screenshot.get("path"):
        bullets.append(f"已保存页面截图：{screenshot['path']}。")
    else:
        bullets.append("未保存截图；当前结果只包含页面文本和链接摘要。")
    return bullets


def _variant_answer_bullets(data: dict[str, Any]) -> list[str]:
    subjects = data.get("subjects") or data.get("compareSubjects") or data.get("items")
    subject_count = len(subjects) if isinstance(subjects, list) else 0
    bullets = [f"当前返回 {subject_count} 个配置对比对象。"]
    diff_features = data.get("diffFeatures") or data.get("differences")
    if isinstance(diff_features, list):
        bullets.append(f"配置差异字段返回 {len(diff_features)} 项，可继续聚焦用户价值或成本影响。")
    return bullets


def _pageindex_answer_bullets(data: dict[str, Any]) -> list[str]:
    sections = data.get("sections")
    if not isinstance(sections, list):
        return [str(data.get("summary") or "PageIndex 返回了文档检索结果。")]
    bullets = [f"文档检索返回 {len(sections)} 个相关章节/结果。"]
    for section in sections[:3]:
        record = _dict_value(section) or {}
        title = _short_text(record.get("title"), 120)
        if title:
            bullets.append(f"相关文档结果：{title}。")
    return bullets


def _minirag_answer_bullets(data: dict[str, Any]) -> list[str]:
    paths = data.get("paths") if isinstance(data.get("paths"), list) else []
    entities = data.get("entities") if isinstance(data.get("entities"), list) else []
    chunks = data.get("supportingChunks") if isinstance(data.get("supportingChunks"), list) else []
    return [
        f"多跳检索返回 {len(paths)} 条关系路径、{len(entities)} 个实体、{len(chunks)} 个支撑片段。",
        str(data.get("summary") or "当前 MiniRAG 结果应作为关系线索，而不是结构化事实表。"),
    ]


def _kpi_bullets(kpis: dict[str, Any]) -> list[str]:
    bullets: list[str] = []
    for key in ("totalSales", "sales", "volume", "bevShare", "phevShare", "growthRate"):
        if key in kpis:
            bullets.append(f"{key}: {_display_value(kpis[key])}")
    return bullets[:4]


def _answer_citations(result: dict[str, Any]) -> list[dict[str, Any]]:
    tool = str(result.get("tool") or "")
    metadata = _dict_value(result.get("metadata")) or {}
    data = _dict_value(result.get("data")) or {}
    source = str(metadata.get("source") or "jato")
    citations: list[dict[str, Any]] = []

    for item in _items_from_payload(data)[:5]:
        record = _dict_value(item) or {}
        title = _short_text(record.get("title") or record.get("name") or record.get("model"), 120)
        if not title:
            continue
        citation_id = str(record.get("citationId") or "").strip()
        label = f"[{citation_id}] {title}" if citation_id else title
        citation: dict[str, Any] = {
            "label": label,
            "source": str(record.get("provider") or record.get("source") or source),
            "tool": tool,
        }
        if citation_id:
            citation["citationId"] = citation_id
        source_score = record.get("sourceScore")
        if isinstance(source_score, int | float):
            citation["sourceScore"] = source_score
            citation["sourceTier"] = record.get("sourceTier")
        for key in ("sourceTitle", "sourceCategory", "supportedClaim", "claimType"):
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                citation[key] = value.strip()
        url = record.get("url")
        if isinstance(url, str) and url.strip():
            citation["url"] = url.strip()
        citations.append(citation)

    sections = data.get("sections")
    if isinstance(sections, list):
        for section in sections[:5]:
            record = _dict_value(section) or {}
            title = _short_text(record.get("title") or record.get("section"), 120)
            if title:
                citation = {
                    "label": title,
                    "source": str(record.get("source") or source),
                    "tool": tool,
                }
                url = record.get("url")
                if isinstance(url, str) and url.strip():
                    citation["url"] = url.strip()
                citations.append(citation)

    if tool in {"read_web_page", "browser_snapshot"}:
        url = data.get("url")
        if isinstance(url, str) and url.strip():
            citations.append({
                "label": _short_text(data.get("title"), 120) or url.strip(),
                "source": source,
                "tool": tool,
                "url": url.strip(),
            })

    if not citations:
        citations.append({"label": source, "source": source, "tool": tool})
    return citations


def _answer_confidence(evidence_pack: dict[str, Any]) -> str:
    source_count = evidence_pack.get("sourceCount")
    item_count = len(evidence_pack.get("items")) if isinstance(evidence_pack.get("items"), list) else 0
    if isinstance(source_count, int) and source_count >= 2 and item_count >= 2:
        return "medium-high"
    if isinstance(source_count, int) and source_count >= 1:
        return "medium"
    return "low"


def _short_text(value: Any, max_len: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return f"{text[:max_len - 1]}…"


def _dedupe_texts(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _select_tool_for_retrieval_path(path: str, signals: Any) -> tuple[str, str, str]:
    signal_list = [str(signal) for signal in signals] if isinstance(signals, list) else []
    if path == "structured_mcp":
        if any("pricing" in signal for signal in signal_list):
            return "query_msrp_pricing", "pricing", "retrieval_router_pricing_signal"
        if any("variant" in signal for signal in signal_list):
            return "compare_vehicle_variants", "variant", "retrieval_router_variant_signal"
        if any("chart" in signal for signal in signal_list):
            return "build_market_chart", "chart", "retrieval_router_chart_signal"
        return "query_country_snapshot", "snapshot", "retrieval_router_structured_default"
    if path == "web_search" and any("explicit_url" in signal for signal in signal_list):
        return "read_web_page", "web", "retrieval_router_explicit_url"

    path_tool_map = {
        "hybrid_rag": ("search_market_news", "news", "retrieval_router_hybrid_rag_search"),
        "web_search": ("search_market_news", "research", "retrieval_router_web_search"),
        "pageindex": ("pageindex_search_documents", "news", "retrieval_router_pageindex"),
        "minirag": ("minirag_query_graph", "snapshot", "retrieval_router_minirag"),
    }
    return path_tool_map.get(path, ("query_country_snapshot", "snapshot", "retrieval_router_default"))


def _enforce_intent_required_tool(
    route: dict[str, str],
    evidence_plan: dict[str, Any],
    question: str,
    route_source: str,
) -> dict[str, str]:
    required_tools = _text_list(evidence_plan.get("requiredTools"))
    if not required_tools:
        return route
    if route_source == "skill_or_mode_override":
        return route

    current_tool = str(route.get("tool") or "")
    if current_tool in required_tools:
        return route

    allowed_tools = set(_text_list(evidence_plan.get("allowedTools")))
    if _extract_first_url(question) and current_tool in {"read_web_page", "browser_snapshot"}:
        return route
    if evidence_plan.get("answerMode") == "chart" and current_tool == "build_market_chart":
        return route

    forced_tool = next((tool for tool in required_tools if not allowed_tools or tool in allowed_tools), required_tools[0])
    forced_route = dict(route)
    forced_route["tool"] = forced_tool
    forced_route["mode"] = _mode_for_forced_tool(forced_tool)
    forced_route["reason"] = (
        f"intent_tool_matrix_required:{evidence_plan.get('intent')}: "
        f"{route.get('reason', '')}"
    )[:240]
    forced_route["retrievalPath"] = _retrieval_path_for_forced_tool(forced_tool)
    return forced_route


def _mode_for_forced_tool(tool_name: str) -> str:
    if tool_name in {"query_msrp_pricing", "query_price_positioning", "compare_competitive_set", "query_competitive_landscape"}:
        return "pricing"
    if tool_name in {"compare_vehicle_variants", "analyze_model_performance"}:
        return "variant"
    if tool_name in {"search_market_news", "pageindex_search_documents", "minirag_query_graph"}:
        return "news"
    if tool_name == "build_market_chart":
        return "chart"
    if tool_name in {"read_web_page", "browser_snapshot"}:
        return "web"
    return "snapshot"


def _retrieval_path_for_forced_tool(tool_name: str) -> str:
    if tool_name in {"search_market_news", "pageindex_search_documents"}:
        return "hybrid_rag"
    if tool_name == "minirag_query_graph":
        return "minirag"
    if tool_name in {"read_web_page", "browser_snapshot"}:
        return "web_search"
    return "structured_mcp"


def _should_execute_secondary_paths(arguments: dict[str, Any], route_source: str) -> bool:
    if route_source != "retrieval_router":
        return False
    explicit_value = (
        arguments.get("include_secondary_paths")
        if "include_secondary_paths" in arguments
        else arguments.get("includeSecondaryPaths")
        if "includeSecondaryPaths" in arguments
        else arguments.get("include_secondary")
    )
    return _bool_argument(explicit_value, default=True)


def _select_agent_route(question: str, requested_mode: str) -> dict[str, str]:
    allowed_modes = {
        "chart": ("build_market_chart", "requested_chart_mode"),
        "snapshot": ("query_country_snapshot", "requested_snapshot_mode"),
        "msrp": ("query_msrp_pricing", "requested_msrp_mode"),
        "pricing": ("query_msrp_pricing", "requested_pricing_mode"),
        "variant": ("compare_vehicle_variants", "requested_variant_mode"),
        "news": ("search_market_news", "requested_news_search_mode"),
        "research": ("search_market_news", "requested_research_search_mode"),
        "web": ("read_web_page", "requested_web_mode"),
        "browser": ("browser_snapshot", "requested_browser_mode"),
        "browser_snapshot": ("browser_snapshot", "requested_browser_snapshot_mode"),
        "screenshot": ("browser_snapshot", "requested_screenshot_mode"),
        "page": ("read_web_page", "requested_page_mode"),
    }
    if requested_mode in allowed_modes:
        selected_tool, reason = allowed_modes[requested_mode]
        return {"mode": requested_mode, "tool": selected_tool, "reason": reason}

    text = question.lower()
    if _extract_first_url(question):
        return {"mode": "web", "tool": "read_web_page", "reason": "url_read_question"}
    if _contains_any(text, ["chart", "plot", "graph", "trend", "走势", "趋势", "画图", "作图", "图表"]):
        return {"mode": "chart", "tool": "build_market_chart", "reason": "chart_or_trend_question"}
    if _contains_any(text, ["msrp", "price", "pricing", "价格", "定价", "售价", "报价"]):
        return {"mode": "pricing", "tool": "query_msrp_pricing", "reason": "pricing_question"}
    if _contains_any(text, ["variant", "trim", "feature", "configuration", "config", "配置", "版型", "差异"]):
        return {"mode": "variant", "tool": "compare_vehicle_variants", "reason": "variant_or_feature_question"}
    if _contains_any(text, ["research", "source", "sources", "citation", "citations", "search", "tavily", "联网", "来源", "引用", "检索"]):
        return {"mode": "research", "tool": "search_market_news", "reason": "governed_search_question"}
    if _contains_any(text, ["news", "policy", "subsidy", "regulation", "新闻", "政策", "补贴", "法规", "舆情"]):
        return {"mode": "news", "tool": "search_market_news", "reason": "news_or_policy_search_question"}
    return {"mode": "snapshot", "tool": "query_country_snapshot", "reason": "default_market_snapshot"}


def _build_route_tool_arguments(
    tool_name: str,
    arguments: dict[str, Any],
    country: str,
    question: str,
    evidence_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    planned_input = _planned_tool_input(evidence_plan, tool_name)
    base: dict[str, Any] = {
        "country": country,
        "question": question,
    }
    if tool_name == "query_country_snapshot":
        base["include_sections"] = _text_list(arguments.get("include_sections")) or DEFAULT_SNAPSHOT_SECTIONS
        return base
    if tool_name == "build_market_chart":
        base["include_sections"] = _text_list(arguments.get("include_sections")) or DEFAULT_CHART_SECTIONS
        selected_year = _optional_int(arguments.get("selected_year"))
        if selected_year is not None:
            base["selected_year"] = selected_year
        selected_model = _optional_text(arguments, "selected_model")
        if selected_model:
            base["selected_model"] = selected_model
        model_top_n = _optional_int(arguments.get("model_top_n"))
        if model_top_n is not None:
            base["model_top_n"] = model_top_n
        return base
    if tool_name == "query_cross_country":
        explicit_countries = _country_list_from_value(arguments.get("countries"))
        planned_countries = _country_list_from_value(planned_input.get("countries"))
        entity_countries = _country_list_from_value(
            (_dict_value((evidence_plan or {}).get("entities")) or {}).get("countries")
        )
        countries = explicit_countries if len(explicit_countries) >= 2 else []
        if not countries and len(planned_countries) >= 2:
            countries = planned_countries
        if not countries and len(entity_countries) >= 2:
            countries = entity_countries
        if not countries:
            countries = explicit_countries or planned_countries
        if not countries:
            countries = [country]
        base["countries"] = ", ".join(countries)
        return base
    if tool_name == "query_msrp_pricing":
        for key in ("brand", "powertrain"):
            value = _optional_text(arguments, key)
            if value:
                base[key] = value
        explicit_model = _optional_text(arguments, "model")
        if explicit_model:
            base["model"] = explicit_model
        models = _text_list(arguments.get("models"))
        planned_models = _text_list(planned_input.get("models"))
        if models:
            base.pop("model", None)
            base["models"] = models
        elif planned_models:
            combined_models = _dedupe_texts([
                *([str(base["model"])] if base.get("model") else []),
                *planned_models,
            ])
            if len(combined_models) > 1:
                base.pop("model", None)
                base["models"] = combined_models
            elif not base.get("model"):
                base["model"] = combined_models[0]
        elif not base.get("model"):
            model_from_question = _extract_model_from_question(question) or (planned_models[0] if len(planned_models) == 1 else "")
            if model_from_question:
                base["model"] = model_from_question
        base["max_items"] = _clamp_int(arguments.get("max_items"), default=12, minimum=1, maximum=50)
        return base
    if tool_name == "compare_vehicle_variants":
        for key in ("brand", "model", "powertrain"):
            value = _optional_text(arguments, key)
            if value:
                base[key] = value
        models = _text_list(arguments.get("models"))
        if not models:
            models = _text_list(planned_input.get("models"))
        if models:
            base["models"] = models
        compare_subjects = _dict_list(arguments.get("compare_subjects"))
        if compare_subjects:
            base["compare_subjects"] = compare_subjects
        return base
    if tool_name == "search_market_news":
        base["limit"] = _clamp_int(arguments.get("limit"), default=6, minimum=1, maximum=10)
        return base
    if tool_name == "read_web_page":
        url = _optional_text(arguments, "url") or _extract_first_url(question)
        if not url:
            raise ValueError("url is required for read_web_page")
        return {
            "url": url,
            "question": question,
            "max_chars": _clamp_int(arguments.get("max_chars"), default=6000, minimum=1000, maximum=20_000),
        }
    if tool_name == "browser_snapshot":
        url = _optional_text(arguments, "url") or _extract_first_url(question)
        if not url:
            raise ValueError("url is required for browser_snapshot")
        return {
            "url": url,
            "question": question,
            "max_chars": _clamp_int(arguments.get("max_chars"), default=6000, minimum=1000, maximum=20_000),
            "timeout_ms": _clamp_int(arguments.get("timeout_ms"), default=12_000, minimum=1000, maximum=30_000),
            "capture_screenshot": _bool_argument(arguments.get("capture_screenshot"), default=_needs_browser_snapshot(question)),
        }
    return base


def _next_actions_for_route(tool_name: str) -> list[str]:
    if tool_name == "build_market_chart":
        return ["render_chart_artifact", "summarize_chart_evidence"]
    if tool_name == "query_msrp_pricing":
        return ["compare_price_band", "cite_price_sources"]
    if tool_name == "compare_vehicle_variants":
        return ["summarize_diff_features", "cite_variant_sources"]
    if tool_name == "search_market_news":
        return ["rank_news_evidence", "cross_check_with_snapshot"]
    if tool_name == "read_web_page":
        return ["summarize_static_page", "cite_page_url"]
    if tool_name == "browser_snapshot":
        return ["summarize_browser_snapshot", "cite_page_url", "plan_confirmed_browser_action_if_needed"]
    if tool_name == "pageindex_search_documents":
        return ["review_policy_sections", "cite_document_sources"]
    if tool_name == "minirag_query_graph":
        return ["trace_entity_relationships", "verify_multi_hop_chain"]
    return ["compose_grounded_answer", "fact_check_numeric_claims"]


def _answer_follow_ups(
    *,
    country: str,
    question: str,
    tool: str,
    evidence_pack: dict[str, Any],
    evidence_plan: dict[str, Any],
) -> list[dict[str, Any]]:
    country_label = country or "当前市场"
    paths = _text_list(evidence_pack.get("pathsContributed"))
    suggestions: list[str] = []
    if tool == "build_market_chart":
        suggestions.extend([
            f"把 {country_label} 这个趋势拆到品牌和车型层面看，哪些品牌贡献最大？",
            f"对比 {country_label} 和挪威/丹麦同一指标，谁的增长质量更好？",
            f"这个趋势背后是否有价格、政策或车型供给变化？",
        ])
    elif tool == "query_country_snapshot":
        suggestions.extend([
            f"继续看 {country_label} 的 Top models，哪些车型拉动了这个结果？",
            f"按 BEV/PHEV/HEV/ICE 拆开看，结构变化最大的动力类型是什么？",
            f"把这个市场表现和相邻国家做 cross-country 对比。",
        ])
    elif tool == "query_msrp_pricing":
        suggestions.extend([
            f"把这些价格和主要竞品的 MSRP 区间做对比。",
            f"这个价格带对应哪些销量最高的车型？",
            f"按版本或配置差异解释为什么价格会分层。",
        ])
    elif tool == "compare_vehicle_variants":
        suggestions.extend([
            f"这些配置差异会影响哪个细分市场的购买决策？",
            f"把配置差异和 MSRP 一起看，哪一版性价比更高？",
            f"和同级竞品相比，这些配置是优势还是短板？",
        ])
    elif tool == "search_market_news":
        suggestions.extend([
            f"把这些外部来源和 {country_label} 的销量/份额变化交叉验证。",
            f"哪些政策或市场事件最可能影响未来 3-6 个月？",
            f"把来源影响拆到品牌或动力类型层面看。",
        ])
    elif tool == "read_web_page":
        suggestions.extend([
            "把这个网页内容提炼成可引用的证据点。",
            f"用这页信息和 {country_label} 的 JATO 市场数据交叉验证。",
            "继续读取页面里的关键链接或报告章节。",
        ])
    elif tool == "browser_snapshot":
        suggestions.extend([
            "根据这个页面快照生成可执行的下一步网页操作计划。",
            "把页面中的关键表格或链接提取出来。",
            f"把页面信息和 {country_label} 的 JATO 数据交叉验证。",
        ])
    elif tool == "pageindex_search_documents":
        suggestions.extend([
            "继续打开最相关文档章节，提取原文证据。",
            f"把文档结论和 {country_label} 的市场数据交叉验证。",
            "找出这份政策/报告对品牌或动力类型的具体影响。",
        ])
    elif tool == "minirag_query_graph":
        suggestions.extend([
            "展开这个关系链里的关键实体和证据来源。",
            f"把图谱关系和 {country_label} 的结构化销量数据交叉验证。",
            "继续追踪影响路径里最薄弱的一环。",
        ])
    else:
        suggestions.extend([
            f"继续找 {country_label} 相关的结构化销量或份额证据。",
            "换一个工具交叉验证这个结论。",
            "把这个问题拆成趋势、竞品、价格和新闻四个维度分析。",
        ])

    lowered_question = question.lower()
    if ("why" in lowered_question or "为什么" in question or "原因" in question) and "hybrid_rag" not in paths:
        suggestions.insert(0, f"补充搜索 {country_label} 的政策、新闻或消费者反馈来解释原因。")
    if ("compare" in lowered_question or "对比" in question or "竞品" in question) and "structured_mcp" in paths:
        suggestions.insert(0, "把同一指标拉到竞品/相邻国家做 side-by-side 对比。")

    return normalize_follow_ups(
        _dedupe_texts(suggestions)[:4],
        country=country,
        question=question,
        tools=[tool],
        evidence_plan=evidence_plan,
    )


def _safe_tool_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in arguments.items()
        if key.lower() not in {"api_key", "key", "token", "password", "secret"}
    }


def _planned_tool_input(evidence_plan: dict[str, Any] | None, tool_name: str) -> dict[str, Any]:
    if not isinstance(evidence_plan, dict):
        return {}
    for item in evidence_plan.get("toolPlan", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("toolName") or "").strip() != tool_name:
            continue
        input_value = item.get("input")
        return dict(input_value) if isinstance(input_value, dict) else {}
    return {}


def _country_list_from_value(value: Any) -> list[str]:
    if isinstance(value, list):
        return _text_list(value)
    if isinstance(value, str):
        return _dedupe_texts([item.strip() for item in value.split(",") if item.strip()])
    return []


def _should_track_model_usage(model_usage: dict[str, Any]) -> bool:
    if model_usage.get("status") != "ok":
        return False
    prompt_tokens = _optional_int(model_usage.get("promptTokens")) or 0
    completion_tokens = _optional_int(model_usage.get("completionTokens")) or 0
    total_tokens = _optional_int(model_usage.get("totalTokens")) or 0
    return prompt_tokens + completion_tokens + total_tokens > 0


def _has_successful_agent_loop_answer(agent_loop_result: dict[str, Any] | None) -> bool:
    result = _dict_value(agent_loop_result)
    if not result:
        return False
    usage = _dict_value(result.get("usage")) or {}
    if usage.get("status") != "ok":
        return False
    if (_optional_int(result.get("rounds")) or 0) <= 0:
        return False
    answer = _dict_value(result.get("answer")) or {}
    direct = str(answer.get("direct") or "").strip()
    return bool(direct or _text_list(answer.get("bullets")))


def _executed_tool_names(primary_tool: str, secondary_results: list[dict[str, Any]]) -> list[str]:
    tools = [primary_tool]
    for item in secondary_results:
        if item.get("status") != "executed":
            continue
        result = _dict_value(item.get("result")) or {}
        tool = str(result.get("tool") or item.get("tool") or "").strip()
        if tool:
            tools.append(tool)
    return _dedupe_texts(tools)


def _build_tool_result_entries(
    *,
    primary_tool: str,
    primary_arguments: dict[str, Any],
    primary_result: dict[str, Any],
    secondary_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = [
        {
            "toolName": primary_tool,
            "query": _safe_tool_arguments(primary_arguments),
            "result": primary_result,
            "success": True,
        }
    ]
    for item in secondary_results:
        tool_name = str(item.get("tool") or "").strip()
        if not tool_name:
            continue
        entries.append({
            "toolName": tool_name,
            "query": _safe_tool_arguments(_dict_value(item.get("arguments")) or {}),
            "result": _dict_value(item.get("result")) or {},
            "success": item.get("status") == "executed",
            "error": str(item.get("error") or ""),
        })
    return entries


def _variant_compare_returned_no_matrix(result: dict[str, Any]) -> bool:
    data = _dict_value(result.get("data")) or {}
    subjects = data.get("subjects") or data.get("compareSubjects")
    diff_features = data.get("differentFeatures") or data.get("diffFeatures") or data.get("differences")
    common_features = data.get("commonFeatures")
    selection_notes = data.get("selectionNotes")
    return not any(
        isinstance(value, list) and len(value) > 0
        for value in (subjects, diff_features, common_features, selection_notes)
    )


def _has_empty_variant_compare_execution(
    primary_tool: str,
    primary_result: dict[str, Any],
    secondary_results: list[dict[str, Any]],
) -> bool:
    if primary_tool == "compare_vehicle_variants" and _variant_compare_returned_no_matrix(primary_result):
        return True
    for item in secondary_results:
        if item.get("status") != "executed":
            continue
        if str(item.get("tool") or "") != "compare_vehicle_variants":
            continue
        result = _dict_value(item.get("result")) or {}
        if _variant_compare_returned_no_matrix(result):
            return True
    return False


def _chart_specs_from_results(
    primary_result: dict[str, Any],
    secondary_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    charts: list[dict[str, Any]] = []
    for result in [primary_result, *[
        _dict_value(item.get("result")) or {}
        for item in secondary_results
        if item.get("status") == "executed"
    ]]:
        data = _dict_value(result.get("data")) or {}
        chart_specs = _dict_value(data.get("chartSpecs")) or {}
        chart_items = chart_specs.get("charts")
        if isinstance(chart_items, list):
            charts.extend(dict(item) for item in chart_items if isinstance(item, dict))
    return charts[:4]


def _evidence_package_tool_names(evidence_package: dict[str, Any]) -> list[str]:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return []
    return _dedupe_texts([
        str(item.get("toolName") or "")
        for item in tool_results
        if isinstance(item, dict)
    ])


def _contains_any(text: str, needles: list[str]) -> bool:
    return any(needle in text for needle in needles)


def _extract_first_url(text: str) -> str:
    match = re.search(r"https?://[^\s<>)\"']+", str(text or ""))
    if not match:
        return ""
    return match.group(0).rstrip(".,，。;；:：!！?？)]】")


def _needs_browser_snapshot(text: str) -> bool:
    lowered = str(text or "").lower()
    return _contains_any(
        lowered,
        [
            "snapshot",
            "screenshot",
            "browser",
            "render",
            "rendered",
            "dom",
            "页面快照",
            "截图",
            "浏览器",
            "渲染",
        ],
    )


def _cards_from_kpis(kpis: dict[str, Any]) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    for key in ("totalSales", "sales", "volume", "bevShare", "phevShare", "growthRate"):
        if key in kpis:
            cards.append({"label": key, "value": _display_value(kpis[key])})
    return cards[:4]


def _items_from_payload(payload: dict[str, Any]) -> list[Any]:
    items = payload.get("items")
    return items if isinstance(items, list) else []


def _section_count(payload: dict[str, Any]) -> str:
    return f"{len([key for key, value in payload.items() if value not in (None, [], {})])} sections"


def _display_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return f"{len(value)} items"
    if isinstance(value, dict):
        return f"{len(value)} fields"
    return str(value)


def _join_nonempty(values: list[Any]) -> str:
    parts = [str(value).strip() for value in values if str(value or "").strip()]
    return " / ".join(parts) if parts else "n/a"


def _tool_payload(
    *,
    tool: str,
    source: str,
    data: dict[str, Any],
    truncated: bool,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "tool": tool,
        "metadata": {
            "source": source,
            "truncated": truncated,
            "limitations": [],
            **(metadata or {}),
        },
        "data": data,
    }


def _jato_data_country(country: str) -> str:
    value = str(country or "").strip()
    direct = JATO_DATA_COUNTRY_ALIASES.get(value.lower())
    if direct:
        return direct
    canonical = to_display_country(value)
    aliases = COUNTRY_ALIAS_GROUPS.get(canonical, set())
    chinese_alias = next(
        (
            alias
            for alias in sorted(aliases)
            if any("\u4e00" <= char <= "\u9fff" for char in alias)
        ),
        "",
    )
    return chinese_alias or value


def _build_country_snapshot_with_fallback(
    country: str,
    *,
    user_params: dict[str, Any] | None = None,
    news_payload_override: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], str]:
    snapshot = _call_build_country_snapshot(
        country,
        user_params=user_params,
        news_payload_override=news_payload_override,
    )
    if _has_market_snapshot_data(snapshot):
        return snapshot, country
    jato_country = _jato_data_country(country)
    if jato_country == country:
        return snapshot, country
    fallback = _call_build_country_snapshot(
        jato_country,
        user_params=user_params,
        news_payload_override=news_payload_override,
    )
    if _has_market_snapshot_data(fallback):
        return fallback, jato_country
    return snapshot, country


def _call_build_country_snapshot(
    country: str,
    *,
    user_params: dict[str, Any] | None,
    news_payload_override: dict[str, Any] | None,
) -> dict[str, Any]:
    if news_payload_override is None:
        return build_country_snapshot(country, user_params=user_params)
    return build_country_snapshot(
        country,
        user_params=user_params,
        news_payload_override=news_payload_override,
    )


def _build_country_chart_deck_with_fallback(
    *,
    country: str,
    question: str | None = None,
    intents: list[str] | None = None,
    extracted_params: dict[str, Any] | None = None,
    selected_year: int | None = None,
    selected_model: str | None = None,
    model_top_n: int | None = None,
) -> tuple[dict[str, Any], str]:
    deck = build_country_chart_deck(
        country=country,
        question=question,
        intents=intents,
        extracted_params=extracted_params,
        selected_year=selected_year,
        selected_model=selected_model,
        model_top_n=model_top_n,
    )
    if _has_market_snapshot_data(deck.get("contextSnapshot") if isinstance(deck, dict) else {}):
        return deck, country
    jato_country = _jato_data_country(country)
    if jato_country == country:
        return deck, country
    fallback = build_country_chart_deck(
        country=jato_country,
        question=question,
        intents=intents,
        extracted_params=extracted_params,
        selected_year=selected_year,
        selected_model=selected_model,
        model_top_n=model_top_n,
    )
    if _has_market_snapshot_data(fallback.get("contextSnapshot") if isinstance(fallback, dict) else {}):
        return fallback, jato_country
    return deck, country


def _has_market_snapshot_data(snapshot: Any) -> bool:
    if not isinstance(snapshot, dict):
        return False
    kpis = snapshot.get("kpis") if isinstance(snapshot.get("kpis"), dict) else {}
    for key in ("totalRows", "cumulativeSales", "sales", "volume", "avgMsrp"):
        value = kpis.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return True
    for key in ("topModels", "topBrands", "powertrainMix", "yearSeries", "monthSeries"):
        value = snapshot.get(key)
        if isinstance(value, list) and len(value) > 0:
            return True
    return False


def _required_text(arguments: dict[str, Any], key: str) -> str:
    value = str(arguments.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _optional_text(arguments: dict[str, Any], key: str) -> str:
    return str(arguments.get(key) or "").strip()


def _text_list(value: Any) -> list[str]:
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


def _with_direct_conclusion_prefix(answer: dict[str, Any]) -> dict[str, Any]:
    direct = str(answer.get("direct") or "").strip()
    if not direct or direct.startswith("直接结论"):
        return answer
    return {**answer, "direct": f"直接结论：{direct}"}


def _dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _dict_value(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None and str(value).strip() else None
    except (TypeError, ValueError):
        return None


def _bool_argument(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _clamp_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    parsed = _optional_int(value)
    if parsed is None:
        parsed = default
    return max(minimum, min(parsed, maximum))


def _select_sections(
    payload: dict[str, Any],
    sections: list[str],
) -> tuple[dict[str, Any], bool]:
    selected: dict[str, Any] = {}
    truncated = False
    for section in sections:
        if section not in payload:
            continue
        selected[section], was_truncated = _truncate_value(payload[section])
        truncated = truncated or was_truncated
    return selected, truncated


def _truncate_value(value: Any) -> tuple[Any, bool]:
    if isinstance(value, list):
        return value[:MAX_SECTION_ITEMS], len(value) > MAX_SECTION_ITEMS
    if isinstance(value, dict):
        truncated = False
        result: dict[str, Any] = {}
        for key, item in value.items():
            result[key], was_truncated = _truncate_value(item)
            truncated = truncated or was_truncated
        return result, truncated
    return value, False
