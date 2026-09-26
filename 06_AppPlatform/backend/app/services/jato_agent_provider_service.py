from __future__ import annotations

import json
import os
import re
import time
from typing import Any
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.request import Request
from urllib.request import urlopen

from app.core.config import ASTRBOT_PROVIDER_API_BASE
from app.core.config import ASTRBOT_PROVIDER_KEY_ENV
from app.core.config import ASTRBOT_PROVIDER_MODEL
from app.services.jato_agent_planning_service import build_evidence_plan
from app.services.jato_business_playbook_service import build_business_playbook_context
from app.services.jato_followup_service import normalize_follow_ups


DEFAULT_FINAL_COMPOSER_TIMEOUT_SECONDS = 25
DEFAULT_FINAL_COMPOSER_MAX_TOKENS = 1200
_JSON_BLOCK_PATTERN = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_ANSWER_CONTROL_PREFIXES = ("TOOL:", "ARGS:", "REASON:", "FINAL_ANSWER:", "```")


def compose_agent_final_answer(
    *,
    country: str,
    question: str,
    profile: dict[str, Any],
    skill: dict[str, Any],
    deterministic_answer: dict[str, Any],
    primary_result: dict[str, Any],
    secondary_results: list[dict[str, Any]],
    evidence_pack: dict[str, Any],
) -> dict[str, Any]:
    if not _composer_enabled():
        return _fallback_result(
            deterministic_answer,
            status="disabled",
            reason="APP_ASTRBOT_FINAL_COMPOSER_ENABLED=false",
        )

    api_key = os.getenv(ASTRBOT_PROVIDER_KEY_ENV, "").strip()
    if not api_key:
        return _fallback_result(
            deterministic_answer,
            status="missing_key",
            reason=f"{ASTRBOT_PROVIDER_KEY_ENV} is not configured",
        )

    prompt_payload = _build_prompt_payload(
        country=country,
        question=question,
        profile=profile,
        skill=skill,
        deterministic_answer=deterministic_answer,
        primary_result=primary_result,
        secondary_results=secondary_results,
        evidence_pack=evidence_pack,
    )
    messages = [
        {
            "role": "system",
            "content": _system_prompt(),
        },
        {
            "role": "user",
            "content": json.dumps(prompt_payload, ensure_ascii=False, separators=(",", ":")),
        },
    ]

    try:
        payload = _post_chat_completion(
            api_key=api_key,
            model=ASTRBOT_PROVIDER_MODEL,
            messages=messages,
        )
    except Exception as exc:
        return _fallback_result(
            deterministic_answer,
            status="failed",
            reason=str(exc),
        )

    content = _choice_content(payload)
    parsed = _parse_answer_json(content)
    if not parsed:
        parsed = {
            "title": "JATO answer",
            "direct": _clean_answer_text_fallback(content) or str(deterministic_answer.get("direct") or ""),
            "bullets": [],
            "limitations": [],
        }

    answer = _merge_provider_answer(deterministic_answer, parsed)
    usage = _read_usage(payload)
    return {
        "answer": answer,
        "usage": {
            "provider": "deepseek",
            "model": ASTRBOT_PROVIDER_MODEL,
            "status": "ok",
            "promptTokens": usage["promptTokens"],
            "completionTokens": usage["completionTokens"],
            "totalTokens": usage["totalTokens"],
            "promptCacheHitTokens": usage["promptCacheHitTokens"],
            "promptCacheMissTokens": usage["promptCacheMissTokens"],
            "finishReason": _choice_finish_reason(payload),
            "estimated": False,
        },
    }


def _composer_enabled() -> bool:
    raw = os.getenv("APP_ASTRBOT_FINAL_COMPOSER_ENABLED", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _system_prompt() -> str:
    return (
        "你是 JATO/Hermes 内置的汽车市场分析与代码工作助手。"
        "只能基于用户问题和给定 evidence JSON 回答；不要编造没有证据支持的政策、价格、份额、版本号或来源。"
        "默认用中文，先给直接结论，再给关键依据，最后给 2-4 个可以继续追问的问题。"
        "如果证据不足，明确说明缺口。不要提 AstrBot、开源社区、打赏或外部品牌入口。"
        "如果 payload.businessPlaybook 存在，必须按其中 requiredSections 和 decisionFrame 组织业务答案。"
        "证据不足时不要只回答“证据不足”；仍要输出当前能判断什么、缺什么证据、缺口影响和下一步工具/数据动作。"
        "必须只输出一个合法 JSON 对象，字段为 title、direct、bullets、limitations、followUps。"
        "bullets 和 limitations 必须是字符串数组。"
        "如果 evidencePack.evidencePackage 存在，价格、销量、市场份额、增长率、配置差异、税费、月供、政策日期、竞品结论等关键数字必须来自其中的 evidenceRefs。"
        "没有 evidenceRef 支撑时，只能写“当前证据不足，不能给出确定数字”，不得编造确定数字。"
        "followUps 可以是字符串数组，也可以是结构化对象数组；结构化对象字段包括 label、question、intent、reason、expectedTools、expectedOutput、priority。"
        "followUps 必须是可直接发送的业务追问，不能是泛泛的功能说明。"
        "\n汽车领域常识："
        "车长用米(m)表示，如4.5m=车长4.5米（紧凑型SUV级别）。"
        "MSRP价格用万(SEK/EUR/USD)或千(SEK)表示，如45k SEK=4.5万克朗。"
        "不要混淆车长(m)和价格(百万)。"
        "BEV=Battery Electric Vehicle, PHEV=Plug-in Hybrid, HEV=Hybrid, ICE=内燃机。"
        "瑞典市场主流品牌：Volvo, Volkswagen, Toyota, Kia, Tesla, Mercedes, BMW, Audi。"
    )


def _build_prompt_payload(
    *,
    country: str,
    question: str,
    profile: dict[str, Any],
    skill: dict[str, Any],
    deterministic_answer: dict[str, Any],
    primary_result: dict[str, Any],
    secondary_results: list[dict[str, Any]],
    evidence_pack: dict[str, Any],
) -> dict[str, Any]:
    evidence_package = evidence_pack.get("evidencePackage") if isinstance(evidence_pack.get("evidencePackage"), dict) else {}
    return {
        "task": "compose_final_grounded_answer",
        "country": country,
        "question": question,
        "businessPlaybook": build_business_playbook_context(
            country=country,
            question=question,
            evidence_package=evidence_package,
        ),
        "profile": {
            "id": profile.get("id"),
            "positioning": profile.get("positioning"),
            "style": profile.get("communicationStyle"),
        },
        "skill": {
            "id": skill.get("id"),
            "name": skill.get("name"),
            "outputContract": skill.get("outputContract"),
        },
        "deterministicAnswer": _shrink_prompt_value(deterministic_answer),
        "evidencePack": _shrink_prompt_value(evidence_pack),
        "primaryResult": _shrink_tool_result(primary_result),
        "secondaryResults": [
            _shrink_prompt_value(item)
            for item in secondary_results[:2]
        ],
        "responseContract": {
            "title": "short answer title",
            "direct": "one direct answer paragraph",
            "bullets": ["3-6 evidence-backed points"],
            "limitations": ["only evidence gaps or caveats"],
            "followUps": [
                {
                    "label": "short chip label",
                    "question": "clickable next user question",
                    "intent": "drilldown|compare|why|action|data_check|external_search|report",
                    "reason": "why this is a valuable next step",
                    "expectedTools": ["tool_name"],
                    "expectedOutput": "chart|table|analysis|report|summary|checklist|recommendation",
                    "priority": 1,
                }
            ],
        },
    }


def _shrink_tool_result(result: dict[str, Any]) -> dict[str, Any]:
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    return {
        "tool": result.get("tool"),
        "source": metadata.get("source"),
        "metadata": _shrink_prompt_value(metadata),
        "data": _shrink_prompt_value(data),
    }


def _shrink_prompt_value(value: Any, *, depth: int = 0) -> Any:
    if depth >= 4:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)[:400]
    if isinstance(value, list):
        return [_shrink_prompt_value(item, depth=depth + 1) for item in value[:8]]
    if isinstance(value, dict):
        shrunk: dict[str, Any] = {}
        for key, item in value.items():
            if key in {"raw", "html", "fullText", "debug"}:
                continue
            shrunk[str(key)] = _shrink_prompt_value(item, depth=depth + 1)
        return shrunk
    if isinstance(value, str):
        return value[:1500]
    return value


def _post_chat_completion(
    *,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    response_format: dict[str, str] | None = None,
) -> dict[str, Any]:
    request_body = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": DEFAULT_FINAL_COMPOSER_MAX_TOKENS,
        "stream": False,
    }
    if response_format:
        request_body["response_format"] = response_format
    request = Request(
        _chat_completions_url(),
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    max_retries = 1
    for attempt in range(max_retries + 1):
        try:
            with urlopen(request, timeout=DEFAULT_FINAL_COMPOSER_TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            if attempt >= max_retries:
                raise RuntimeError(_http_error_summary(exc)) from exc
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError("Provider returned empty response")


def _chat_completions_url() -> str:
    base = ASTRBOT_PROVIDER_API_BASE.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    return f"{base}/chat/completions"


def _choice_content(payload: dict[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message") if isinstance(first.get("message"), dict) else {}
    return str(message.get("content") or "").strip()


def _choice_finish_reason(payload: dict[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return ""
    return str(choices[0].get("finish_reason") or "")


def _parse_answer_json(content: str) -> dict[str, Any] | None:
    return _parse_json_object_from_answer(content)


def parse_agent_answer_content(content: str) -> dict[str, Any]:
    parsed = _parse_json_object_from_answer(content)
    if parsed is not None:
        return parsed
    return {
        "direct": _clean_answer_text_fallback(content),
        "bullets": [],
        "limitations": [],
        "title": "Analysis",
    }


def _parse_json_object_from_answer(content: str) -> dict[str, Any] | None:
    text = _strip_answer_markers(content)
    if not text:
        return None
    candidates: list[dict[str, Any]] = []

    for block_match in _JSON_BLOCK_PATTERN.finditer(text):
        block_text = _strip_markdown_wrappers(block_match.group(1))
        parsed = _loads_json_object(block_text)
        if parsed is not None:
            candidates.append(parsed)
        candidates.extend(_load_balanced_json_objects(block_text))

    parsed = _loads_json_object(_strip_markdown_wrappers(text))
    if parsed is not None:
        candidates.append(parsed)

    candidates.extend(_load_balanced_json_objects(text))
    if not candidates:
        return None

    return max(
        enumerate(candidates),
        key=lambda item: (_json_candidate_score(item[1]), item[0]),
    )[1]


def _loads_json_object(text: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        parsed = _loads_lenient_json_object(text)
    return parsed if isinstance(parsed, dict) else None


def _loads_lenient_json_object(text: str) -> dict[str, Any] | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None
    normalized = normalized.removeprefix("\ufeff").strip()
    normalized = re.sub(r"^json\s*", "", normalized, flags=re.IGNORECASE)
    normalized = normalized.translate(str.maketrans({
        "“": '"',
        "”": '"',
        "„": '"',
        "‟": '"',
    }))
    normalized = re.sub(r",\s*([}\]])", r"\1", normalized)
    try:
        parsed = json.loads(normalized)
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _load_balanced_json_objects(text: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for candidate in _iter_balanced_json_objects(text):
        parsed = _loads_json_object(candidate)
        if parsed is not None:
            objects.append(parsed)
    return objects


def _json_candidate_score(candidate: dict[str, Any]) -> int:
    score = 0
    if "primary_tool" in candidate:
        score += 20
    if "direct" in candidate:
        score += 16
    if "bullets" in candidate:
        score += 6
    if "limitations" in candidate:
        score += 6
    if "followUps" in candidate:
        score += 6
    if "title" in candidate:
        score += 4
    if "mode" in candidate and "reasoning" in candidate:
        score += 4
    if set(candidate).issubset({"country", "question", "url", "action_id", "confirmation_token", "text", "max_chars", "timeout_ms"}):
        score -= 8
    return score


def _iter_balanced_json_objects(text: str):
    for start, char in enumerate(text):
        if char != "{":
            continue
        depth = 0
        in_string = False
        escaped = False
        for index in range(start, len(text)):
            current = text[index]
            if in_string:
                if escaped:
                    escaped = False
                elif current == "\\":
                    escaped = True
                elif current == '"':
                    in_string = False
                continue
            if current == '"':
                in_string = True
            elif current == "{":
                depth += 1
            elif current == "}":
                depth -= 1
                if depth == 0:
                    yield text[start : index + 1]
                    break


def _strip_answer_markers(content: str) -> str:
    text = str(content or "").strip()
    marker_matches = list(re.finditer(r"FINAL_ANSWER:\s*", text, re.IGNORECASE))
    if marker_matches:
        text = text[marker_matches[-1].end() :].strip()
    return _strip_markdown_wrappers(text)


def _strip_markdown_wrappers(text: str) -> str:
    stripped = str(text or "").strip()
    while stripped.startswith("**") and stripped.endswith("**") and len(stripped) >= 4:
        stripped = stripped[2:-2].strip()
    if stripped.startswith("`") and stripped.endswith("`") and not stripped.startswith("```") and len(stripped) >= 2:
        stripped = stripped[1:-1].strip()
    return stripped


def _clean_answer_text_fallback(content: str) -> str:
    text = _strip_answer_markers(content)
    if not text:
        return ""
    block_match = _JSON_BLOCK_PATTERN.search(text)
    if block_match:
        text = block_match.group(1).strip()

    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _strip_markdown_wrappers(raw_line)
        if not line or line.startswith(_ANSWER_CONTROL_PREFIXES):
            continue
        lines.append(line)
    if not lines:
        return ""
    return " ".join(lines[:3])[:500]


def _merge_provider_answer(
    deterministic_answer: dict[str, Any],
    provider_answer: dict[str, Any],
) -> dict[str, Any]:
    direct = _read_text(provider_answer.get("direct")) or _read_text(deterministic_answer.get("direct"))
    bullets = _read_string_list(provider_answer.get("bullets")) or _read_string_list(deterministic_answer.get("bullets"))
    limitations = _read_string_list(provider_answer.get("limitations")) or _read_string_list(deterministic_answer.get("limitations"))
    country = _read_text(deterministic_answer.get("country"))
    question = _read_text(deterministic_answer.get("question"))
    tools = _read_string_list(deterministic_answer.get("retrievalPaths"))
    evidence_plan = (
        build_evidence_plan(country, question)
        if country and question
        else {}
    )
    follow_ups = normalize_follow_ups(
        provider_answer.get("followUps") or deterministic_answer.get("followUps"),
        country=country,
        question=question,
        tools=tools,
        evidence_plan=evidence_plan,
    )
    merged = dict(deterministic_answer)
    merged.update(
        {
            "title": _read_text(provider_answer.get("title")) or "JATO answer",
            "direct": direct,
            "bullets": bullets[:8],
            "limitations": limitations[:8],
            "followUps": follow_ups[:4],
            "composer": "dpv4",
        }
    )
    return merged


def _read_usage(payload: dict[str, Any]) -> dict[str, int]:
    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
    prompt_tokens = _safe_int(usage.get("prompt_tokens"))
    completion_tokens = _safe_int(usage.get("completion_tokens"))
    total_tokens = _safe_int(usage.get("total_tokens")) or prompt_tokens + completion_tokens
    prompt_hit = _safe_int(usage.get("prompt_cache_hit_tokens"))
    prompt_miss = _safe_int(usage.get("prompt_cache_miss_tokens"))
    return {
        "promptTokens": prompt_tokens,
        "completionTokens": completion_tokens,
        "totalTokens": total_tokens,
        "promptCacheHitTokens": prompt_hit,
        "promptCacheMissTokens": prompt_miss,
    }


def _fallback_result(answer: dict[str, Any], *, status: str, reason: str) -> dict[str, Any]:
    fallback_answer = dict(answer)
    fallback_answer.setdefault("composer", "deterministic")
    return {
        "answer": fallback_answer,
        "usage": {
            "provider": "deepseek",
            "model": ASTRBOT_PROVIDER_MODEL,
            "status": status,
            "promptTokens": 0,
            "completionTokens": 0,
            "totalTokens": 0,
            "promptCacheHitTokens": 0,
            "promptCacheMissTokens": 0,
            "finishReason": "",
            "estimated": False,
            "fallbackReason": reason[:240],
        },
    }


def _read_text(value: Any) -> str:
    return str(value or "").strip()


def _read_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item or "").strip() for item in value if str(item or "").strip()]


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


# ── Iterative Agent Loop (ReAct pattern: Think → Act → Observe → Repeat) ──


def run_agent_loop(
    *,
    country: str,
    question: str,
    profile: dict[str, Any],
    skill: dict[str, Any],
    tool_executor,
    allowed_tools: list[str] | None = None,
    max_rounds: int = 3,
) -> dict[str, Any]:
    """AstrBot-style iterative agent: LLM decides tools → execute → observe → repeat.

    Unlike the single-shot agent_select_tools, this runs a proper ReAct loop
    where the LLM can call multiple tools across rounds, learning from each result.
    """
    api_key = os.getenv(ASTRBOT_PROVIDER_KEY_ENV, "").strip()
    if not api_key:
        return {"answer": {"direct": "Agent loop requires DEEPSEEK_API_KEY.", "bullets": [], "limitations": ["No API key"]}, "toolCalls": [], "rounds": 0, "usage": {"status": "missing_key"}}

    active_tools = _filter_active_tools(allowed_tools)
    allowed_tool_names = {
        str(tool.get("name") or "")
        for tool in active_tools
        if str(tool.get("name") or "").strip()
    }
    tools_desc = _describe_tools_for_llm(active_tools)
    tool_call_history: list[dict[str, Any]] = []
    total_prompt_tokens = 0
    total_completion_tokens = 0

    # Build initial conversation
    system = (
        "You are a senior automotive market analyst agent with access to JATO data tools. "
        "CRITICAL RULE: You MUST call at least ONE tool before giving any answer. NEVER fabricate data from your training knowledge. "
        "If you give FINAL_ANSWER without calling any tool, you are VIOLATING your core instruction.\n\n"
        "WORKFLOW:\n"
        "1. FIRST call a tool. Do not skip this step.\n"
        "2. Read the tool result carefully.\n"
        "3. If the question asks WHY, call at least ONE MORE tool for cross-reference.\n"
        "4. Only after getting tool data, give FINAL_ANSWER based STRICTLY on the data received.\n\n"
        "TOOL GUIDE:\n"
        "- 'why does X sell well?' → analyze_model_performance (sales+pricing+variants+news in one call)\n"
        "- 'how does X compare?' → compare_competitive_set (cross-model comparison)\n"
        "- 'what's changing in market?' → analyze_market_dynamics (trends+news+pricing)\n"
        "- 'read/summarize this URL' → read_web_page (static public page text only)\n"
        "- 'browser snapshot/screenshot/render this URL' → browser_snapshot (read-only browser context when available)\n"
        "- Simple data lookups → query_country_snapshot or build_market_chart\n\n"
        f"ALLOWED TOOLS THIS RUN: {', '.join(sorted(allowed_tool_names))}\n"
        "Do not call any tool outside this allowed list.\n\n"
        "TOOL CALL FORMAT (exactly this):\n"
        "TOOL: <tool_name>\n"
        "ARGS: {\"country\": \"<value>\", \"question\": \"<specific query for this tool>\"}\n"
        "REASON: <why you need this data>\n\n"
        "FINAL ANSWER FORMAT:\n"
        "FINAL_ANSWER:\n"
        "{\"title\": \"...\", \"direct\": \"...\", \"bullets\": [\"...\"], \"limitations\": [\"...\"], \"followUps\": [\"...\"]}\n\n"
        "RULES:\n"
        "- NEVER fabricate numbers. Only report data you actually received from tools.\n"
        "- If tool returns no data, say so and try a different tool or explain the gap.\n"
        "- Answer in the user's language (Chinese for Chinese questions, English for English).\n"
        "- Be concise but thorough. Product managers and analysts are your audience.\n"
        "- End with 2-4 concrete followUps that the user can click/send next. Each follow-up must be a real question grounded in the tools/data you just used.\n"
        "- When comparing vehicles or analyzing why something sells well, gather BOTH sales data AND configuration/news context.\n"
        "- Default to Chinese unless the question is in English.\n"
        "- DOMAIN: '4.5m'=vehicle LENGTH (compact SUV). '45k'=45,000 money. '450万' or '4.5M SEK'=4.5 million. NEVER confuse meters(m) with millions.\n"
        "- Swedish BEV 40%+ share, Volvo #1. Chinese entrants: BYD/Zeekr/NIO/MG typically 4.5-5m long compact/mid-size segments.\n\n"
        f"Country: {country}\n"
        f"Available tools:\n{tools_desc}"
    )

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system},
        {"role": "user", "content": question},
    ]

    final_answer: dict[str, Any] | None = None

    for round_num in range(1, max_rounds + 1):
        try:
            response = _call_deepseek_messages(messages, api_key, max_tokens=800, temperature=0.1)
        except Exception:
            break

        content = response.get("content", "")
        total_prompt_tokens += response.get("prompt_tokens", 0)
        total_completion_tokens += response.get("completion_tokens", 0)

        # REJECT: LLM gave final answer without calling any tool first
        if "FINAL_ANSWER:" in content and not tool_call_history:
            # Force the LLM to call a tool by rejecting this answer
            messages.append({"role": "assistant", "content": content})
            messages.append({"role": "user", "content": "You MUST call a TOOL first before giving FINAL_ANSWER. Re-read the rules. Call analyze_model_performance or query_country_snapshot NOW. Format: TOOL: <tool_name>\\nARGS: {...}"})
            continue

        # Check if LLM gave a final answer
        if "FINAL_ANSWER:" in content:
            json_str = content.split("FINAL_ANSWER:", 1)[1].strip()
            # Strip markdown bold markers and other noise before JSON
            json_str = re.sub(r'^\*+\s*', '', json_str)
            parsed = _parse_agent_json(json_str)
            if parsed and (parsed.get("direct") or parsed.get("bullets")):
                final_answer = parsed
                messages.append({"role": "assistant", "content": content})
                break
            # If parsing fails, treat as direct text answer
            final_answer = {"direct": _clean_answer_text_fallback(json_str), "bullets": [], "limitations": []}
            messages.append({"role": "assistant", "content": content})
            break

        # Parse tool call
        tool_match = re.search(r"TOOL:\s*(\S+)", content)
        args_match = re.search(r"ARGS:\s*(\{.*?\})", content, re.DOTALL)
        reason_match = re.search(r"REASON:\s*(.+?)(?:\n|$)", content)

        if not tool_match:
            # No tool call found — treat as final answer attempt
            final_answer = {"direct": _clean_answer_text_fallback(content), "bullets": [], "limitations": ["LLM did not call a tool"]}
            messages.append({"role": "assistant", "content": content})
            break

        tool_name = tool_match.group(1).strip()
        try:
            args = json.loads(args_match.group(1)) if args_match else {"country": country, "question": question}
        except json.JSONDecodeError:
            args = {"country": country, "question": question}
        reason = reason_match.group(1).strip() if reason_match else "agent-selected tool"

        if allowed_tool_names and tool_name not in allowed_tool_names:
            messages.append({"role": "assistant", "content": content})
            messages.append({
                "role": "user",
                "content": (
                    f"Tool {tool_name} is not allowed by the Evidence Plan. "
                    f"Choose one of: {', '.join(sorted(allowed_tool_names))}."
                ),
            })
            continue

        # Execute the tool
        tool_result: dict[str, Any] = {}
        try:
            tool_result = tool_executor(tool_name, args)
            tool_status = "ok"
        except Exception as exc:
            tool_result = {"error": str(exc)}
            tool_status = "failed"

        tool_call_history.append({
            "round": round_num,
            "tool": tool_name,
            "args": {k: v for k, v in args.items() if k not in ("api_key", "key", "token", "password")},
            "reason": reason,
            "status": tool_status,
            "hasData": bool(tool_result.get("data")),
        })

        # Add assistant's tool call + tool result to conversation
        messages.append({"role": "assistant", "content": content})
        # Compact the tool result to fit context
        result_summary = _compact_tool_result(tool_result)
        messages.append({
            "role": "user",
            "content": f"TOOL RESULT ({tool_name}):\n{result_summary}\n\n"
                       f"You have {max_rounds - round_num} tool call(s) remaining. "
                       f"Call another tool if needed, or give FINAL_ANSWER now.",
        })

    # If no final answer was produced, force one
    if not final_answer:
        try:
            messages.append({"role": "user", "content": "Please give your FINAL_ANSWER now based on all the evidence above."})
            response = _call_deepseek_messages(messages, api_key, max_tokens=600, temperature=0.1)
            content = response.get("content", "")
            total_prompt_tokens += response.get("prompt_tokens", 0)
            total_completion_tokens += response.get("completion_tokens", 0)
            if "FINAL_ANSWER:" in content:
                json_str = content.split("FINAL_ANSWER:", 1)[1].strip()
                parsed = _parse_agent_json(json_str)
                final_answer = parsed or {"direct": _clean_answer_text_fallback(json_str)[:400], "bullets": [], "limitations": []}
            else:
                final_answer = {"direct": _clean_answer_text_fallback(content)[:400], "bullets": [], "limitations": []}
        except Exception:
            final_answer = {"direct": "Unable to produce final answer.", "bullets": [], "limitations": ["Agent loop failed"]}

    return {
        "answer": {
            "title": final_answer.get("title", "Agent Analysis"),
            "direct": final_answer.get("direct", ""),
            "bullets": final_answer.get("bullets", []),
            "limitations": final_answer.get("limitations", []),
            "followUps": normalize_follow_ups(
                final_answer.get("followUps"),
                country=country,
                question=question,
                tools=[tc["tool"] for tc in tool_call_history],
                evidence_plan=build_evidence_plan(country, question),
            ),
            "confidence": "medium" if final_answer.get("bullets") else "low",
            "composer": "agent_loop",
            "retrievalPaths": [tc["tool"] for tc in tool_call_history],
            "toolCount": len(tool_call_history),
        },
        "toolCalls": tool_call_history,
        "rounds": len(tool_call_history),
        "usage": {
            "provider": "deepseek",
            "model": ASTRBOT_PROVIDER_MODEL,
            "status": "ok",
            "promptTokens": total_prompt_tokens,
            "completionTokens": total_completion_tokens,
            "totalTokens": total_prompt_tokens + total_completion_tokens,
            "estimated": False,
        },
    }


def _get_active_tools() -> list[dict[str, Any]]:
    """Return the active MCP tools available to the agent."""
    return [
        {"name": "query_country_snapshot", "description": "Get country market KPIs, brand/model rankings, powertrain mix, sales trends. Best for: sales data, rankings, market structure, 'how is X doing', 'top models'.", "required": ["country"], "status": "active"},
        {"name": "build_market_chart", "description": "Get chart-ready market data with trend series, rankings, and powertrain breakdown. Best for: trends, charts, visual analysis, 'show me the trend'. Includes query_country_snapshot data plus chart specs.", "required": ["country"], "status": "active"},
        {"name": "query_msrp_pricing", "description": "Get MSRP pricing records for specific models. Best for: price comparison, pricing positioning, 'how much does X cost', 'compare prices'.", "required": ["country"], "status": "active"},
        {"name": "compare_vehicle_variants", "description": "Compare vehicle variants, features, and configuration differences. Best for: trim comparison, feature differences, 'which variant has X', 'compare specs'. Returns diff features and common features.", "required": ["country"], "status": "active"},
        {"name": "search_market_news", "description": "Search market news, policy updates, consumer sentiment, and forum discussions. Best for: latest news, policy changes, subsidies, 'what's new', consumer opinions.", "required": ["country", "question"], "status": "active"},
        {"name": "read_web_page", "description": "Read a public HTTP/HTTPS URL as static text with SSRF safeguards. Best for: summarize this web page, extract headings, cite a public page. No JavaScript, cookies, login, clicks, or forms.", "required": ["url"], "status": "active"},
        {"name": "browser_snapshot", "description": "Capture a read-only browser snapshot of a public URL when Playwright is available, otherwise return static fallback. Best for: screenshot, rendered page, browser snapshot. No clicks, typing, login, cookies, or forms.", "required": ["url"], "status": "active"},
        {"name": "pageindex_search_documents", "description": "Search policy documents, PDFs, and reports. Best for: specific policy clauses, regulation documents, official reports. Falls back to web search if PageIndex not connected.", "required": ["country", "question"], "status": "active"},
        {"name": "minirag_query_graph", "description": "Query multi-hop entity relationships. Best for: understanding connections between policies, brands, models, market segments. Uses multi-tool chain fallback if MiniRAG not connected.", "required": ["country", "question"], "status": "active"},
        {"name": "analyze_model_performance", "description": "DEEP CROSS-REFERENCE: sales + pricing + variants + news for one model. Use when asked WHY a model performs well/poorly, or for root cause analysis. Returns combined data from 3-4 sources.", "required": ["country"], "status": "active"},
        {"name": "compare_competitive_set", "description": "COMPETITIVE LANDSCAPE: compare a model against segment rivals across sales volume, price positioning, and features. Use when asked about competitive positioning or how a model stacks up.", "required": ["country", "question"], "status": "active"},
        {"name": "analyze_market_dynamics", "description": "MARKET CHANGE DETECTION: cross-references trends + news + pricing to identify what's shifting.", "required": ["country", "question"], "status": "active"},
        {"name": "query_with_filters", "description": "FILTERED QUERY: query with powertrain/fuel/segment/brand/model/year filters like the UI sidebar. Returns KPIs, rankings, trends, powertrain mix.", "required": ["country"], "status": "active"},
        {"name": "query_time_series", "description": "TIME-SERIES LENS: trend data (monthly/yearly) with powertrain/fuel/segment filters. Use for 'show trend', 'how changed over time'.", "required": ["country"], "status": "active"},
        {"name": "query_segment_breakdown", "description": "SEGMENT LENS: cross-tab analysis segment×fuel×powertrain. Use for 'which segment', 'SUV vs Car', 'segment-fuel matrix'.", "required": ["country"], "status": "active"},
        {"name": "query_price_positioning", "description": "PRICE LENS: MSRP distribution, price stats, competitive pricing context. Use for 'price range', 'how priced vs market'.", "required": ["country"], "status": "active"},
        {"name": "query_competitive_landscape", "description": "COMPETITIVE INTELLIGENCE: find competitors + compare sales/pricing/features. Use for 'who competes with X', 'competitive analysis'.", "required": ["country", "model"], "status": "active"},
        {"name": "query_powertrain_trend", "description": "POWERTRAIN TREND: sales trends for specific powertrain (BEV/PHEV/HEV/ICE). Use for 'BEV trend', 'PHEV growth', powertrain-specific KPI questions.", "required": ["country"], "status": "active"},
        {"name": "query_brand_deep_dive", "description": "BRAND DEEP DIVE: sales, models, pricing for one brand. Use for 'how is Volvo doing', 'Toyota market position'.", "required": ["country", "brand"], "status": "active"},
        {"name": "query_cross_country", "description": "CROSS-COUNTRY: compare metrics across multiple countries. Use for 'compare Sweden vs Norway', 'Nordic BEV comparison'. Args: countries='Sweden,Norway,Denmark'", "required": ["countries", "question"], "status": "active"},
    ]


def _filter_active_tools(allowed_tools: list[str] | None) -> list[dict[str, Any]]:
    tools = _get_active_tools()
    allowed = {
        str(tool or "").strip()
        for tool in (allowed_tools or [])
        if str(tool or "").strip()
    }
    if not allowed:
        return tools
    return [tool for tool in tools if str(tool.get("name") or "") in allowed]


def _compact_tool_result(result: dict[str, Any]) -> str:
    """Compact a tool result for inclusion in the LLM context."""
    data = result.get("data", {}) if isinstance(result, dict) else {}
    if not isinstance(data, dict):
        return str(result)[:2000]

    parts = []
    # Always include KPIs if present
    kpis = data.get("kpis")
    if isinstance(kpis, dict) and kpis and kpis.get("totalRows", 0) > 0:
        parts.append(f"KPIs: {json.dumps({k: v for k, v in kpis.items() if v}, ensure_ascii=False)}")

    # Rankings
    for key in ("topBrands", "topModels"):
        items = data.get(key)
        if isinstance(items, list) and items:
            parts.append(f"{key} (top {min(8, len(items))}): {json.dumps(items[:8], ensure_ascii=False)}")

    # Trends
    for key in ("yearSeries", "monthSeries"):
        items = data.get(key)
        if isinstance(items, list) and items:
            parts.append(f"{key} ({len(items)} points): {json.dumps(items[:6], ensure_ascii=False)}")

    # Powertrain
    pm = data.get("powertrainMix")
    if isinstance(pm, list) and pm:
        parts.append(f"powertrainMix: {json.dumps(pm[:8], ensure_ascii=False)}")

    # Pricing
    items = data.get("items")
    if isinstance(items, list) and items:
        parts.append(f"items ({len(items)}): {json.dumps(items[:10], ensure_ascii=False, default=str)}")

    # News/sections
    sections = data.get("sections")
    if isinstance(sections, list) and sections:
        parts.append(f"sections ({len(sections)}): {json.dumps([{'title': s.get('title',''), 'source': s.get('source','')} for s in sections[:5]], ensure_ascii=False)}")

    # Entities/chunks (MiniRAG)
    entities = data.get("entities")
    if isinstance(entities, list) and entities:
        parts.append(f"entities: {len(entities)} found")
    chunks = data.get("supportingChunks")
    if isinstance(chunks, list) and chunks:
        parts.append(f"supportingChunks: {len(chunks)} found")

    if result.get("tool") in {"read_web_page", "browser_snapshot"}:
        title = data.get("title")
        headings = data.get("headings") if isinstance(data.get("headings"), list) else []
        text_preview = data.get("textPreview")
        if title:
            parts.append(f"title: {title}")
        if headings:
            parts.append(f"headings: {json.dumps(headings[:5], ensure_ascii=False)}")
        if text_preview:
            parts.append(f"textPreview: {str(text_preview)[:1200]}")

    # Variant diff
    for key in ("diffFeatures", "differences", "commonFeatures"):
        val = data.get(key)
        if isinstance(val, list) and val:
            parts.append(f"{key}: {len(val)} items")

    if not parts:
        return json.dumps(data, ensure_ascii=False, default=str)[:1500]

    result_text = "\n".join(parts)
    if len(result_text) > 3000:
        result_text = result_text[:3000] + "\n...[truncated]"
    return result_text


def _call_deepseek_messages(messages: list[dict[str, Any]], api_key: str, max_tokens: int = 800, temperature: float = 0.1) -> dict[str, Any]:
    payload = {
        "model": ASTRBOT_PROVIDER_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "stream": False,
    }
    req = Request(
        _chat_completions_url(),
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        usage = body.get("usage", {}) if isinstance(body.get("usage"), dict) else {}
        return {
            "content": body["choices"][0]["message"]["content"],
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
        }


# ── Legacy single-shot tool selection (used as fallback) ──


def agent_select_tools(
    *,
    country: str,
    question: str,
    available_tools: list[dict[str, Any]],
    skill_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Ask the LLM to decide which JATO MCP tools to call for the given question.

    Returns a dict with:
      - primary_tool: str
      - primary_args: dict
      - secondary_tools: list[str]
      - reasoning: str
      - confidence: str
      - mode: str  (backward-compat mode label)
    """
    api_key = os.getenv(ASTRBOT_PROVIDER_KEY_ENV, "").strip()
    if not api_key:
        return _fallback_tool_selection(question, available_tools)

    tools_desc = _describe_tools_for_llm(available_tools)
    skill_hint = ""
    if skill_context and skill_context.get("name"):
        skill_hint = f"\nSkill hint: user selected skill '{skill_context['name']}' — {skill_context.get('description', '')}. Prefer tools aligned with this skill but override if the question clearly needs a different tool."

    system = (
        "You are a routing agent for the JATO automotive market analysis system. "
        "Your ONLY job is to decide which tool(s) to call to best answer the user's question. "
        "Do NOT answer the question. Only select tools.\n\n"
        "Rules:\n"
        "1. For questions about sales trends, market share, rankings, KPIs → use query_country_snapshot or build_market_chart\n"
        "2. For questions about prices, MSRP, pricing positioning → use query_msrp_pricing\n"
        "3. For questions about vehicle features, specs, configuration differences → use compare_vehicle_variants\n"
        "4. For questions about news, policies, regulations, subsidies, consumer sentiment → use search_market_news\n"
        "5. For questions about EXACT document clauses, PDFs, reports → use pageindex_search_documents\n"
        "6. For multi-hop questions about relationships between policies/countries/brands → use minirag_query_graph\n"
        "7. If the question contains a public URL to read or summarize, use read_web_page and include 'url' in arguments\n"
        "8. If the question asks for browser snapshot, screenshot, rendered page, or DOM context, use browser_snapshot and include 'url' in arguments\n"
        "9. When in doubt between chart data and raw data, prefer build_market_chart (it includes charts)\n"
        "10. You may select ONE primary tool and up to ONE secondary tool for cross-referencing\n"
        "11. ALWAYS include 'country' in arguments when the selected tool requires it\n"
        "12. For build_market_chart, use mode 'chart'. For query_msrp_pricing, use mode 'pricing'. For search_market_news, use mode 'news'. For read_web_page or browser_snapshot, use mode 'web'. For compare_vehicle_variants, use mode 'variant'. For others, use mode 'snapshot'.\n\n"
        "Respond with ONLY a JSON object:\n"
        '{"primary_tool": "tool_name", "secondary_tool": "tool_name or null", "mode": "chart|pricing|news|variant|web|snapshot", "reasoning": "why these tools", "confidence": "high|medium|low"}'
    )

    user_msg = (
        f"Country: {country}\nQuestion: {question}\n{skill_hint}\n\n"
        f"Available tools:\n{tools_desc}"
    )

    try:
        content = _call_deepseek_quick(system, user_msg, api_key, max_tokens=300)
        parsed = _parse_agent_json(content)
        if parsed and parsed.get("primary_tool"):
            return {
                "primary_tool": parsed.get("primary_tool", ""),
                "primary_args": {"country": country, "question": question},
                "secondary_tool": parsed.get("secondary_tool"),
                "reasoning": parsed.get("reasoning", ""),
                "confidence": parsed.get("confidence", "medium"),
                "mode": parsed.get("mode", "snapshot"),
                "source": "llm_agent",
            }
    except Exception:
        pass

    return _fallback_tool_selection(question, available_tools)


def _fallback_tool_selection(question: str, _available_tools: list[dict[str, Any]]) -> dict[str, Any]:
    """Keyword-based fallback when LLM is unavailable."""
    text = question.lower()
    if "http://" in text or "https://" in text:
        if any(w in text for w in ["snapshot", "screenshot", "browser", "render", "dom", "页面快照", "截图", "浏览器", "渲染"]):
            return {"primary_tool": "browser_snapshot", "primary_args": {}, "secondary_tool": None, "reasoning": "keyword: explicit URL + browser snapshot", "confidence": "medium", "mode": "web", "source": "keyword_fallback"}
        return {"primary_tool": "read_web_page", "primary_args": {}, "secondary_tool": None, "reasoning": "keyword: explicit URL", "confidence": "medium", "mode": "web", "source": "keyword_fallback"}
    if any(w in text for w in ["chart", "trend", "走势", "趋势", "画图", "share", "份额"]):
        return {"primary_tool": "build_market_chart", "primary_args": {}, "secondary_tool": None, "reasoning": "keyword: chart/trend", "confidence": "low", "mode": "chart", "source": "keyword_fallback"}
    if any(w in text for w in ["msrp", "price", "pricing", "价格", "定价"]):
        return {"primary_tool": "query_msrp_pricing", "primary_args": {}, "secondary_tool": None, "reasoning": "keyword: pricing", "confidence": "low", "mode": "pricing", "source": "keyword_fallback"}
    if any(w in text for w in ["variant", "config", "配置", "版型", "差异", "spec"]):
        return {"primary_tool": "compare_vehicle_variants", "primary_args": {}, "secondary_tool": None, "reasoning": "keyword: variant/config", "confidence": "low", "mode": "variant", "source": "keyword_fallback"}
    if any(w in text for w in ["news", "policy", "政策", "补贴", "regulation"]):
        return {"primary_tool": "search_market_news", "primary_args": {}, "secondary_tool": None, "reasoning": "keyword: news/policy", "confidence": "low", "mode": "news", "source": "keyword_fallback"}
    return {"primary_tool": "query_country_snapshot", "primary_args": {}, "secondary_tool": None, "reasoning": "default: snapshot", "confidence": "low", "mode": "snapshot", "source": "keyword_fallback"}


def _describe_tools_for_llm(tools: list[dict[str, Any]]) -> str:
    lines = []
    for t in tools:
        name = t.get("name", "")
        if name in ("route_agent_request", "pageindex_get_section", "pageindex_list_documents", "minirag_explain_entity", "minirag_update_corpus"):
            continue  # Skip meta/internal tools
        desc = t.get("description", "")
        required = t.get("required", [])
        status = t.get("status", "active")
        status_tag = " [planned]" if status == "planned" else ""
        lines.append(f"- {name}{status_tag}: {desc} (requires: {', '.join(required)})")
    return "\n".join(lines)


def _call_deepseek_quick(system: str, user: str, api_key: str, max_tokens: int = 300) -> str:
    payload = {
        "model": ASTRBOT_PROVIDER_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.0,
        "max_tokens": max_tokens,
        "stream": False,
    }
    req = Request(
        _chat_completions_url(),
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urlopen(req, timeout=15) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        return body["choices"][0]["message"]["content"]


def _parse_agent_json(content: str) -> dict[str, Any] | None:
    return parse_agent_answer_content(content)


def _http_error_summary(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        try:
            body = exc.read().decode("utf-8", errors="ignore")[:300]
        except Exception:
            body = ""
        return f"Provider HTTP {exc.code}: {body or exc.reason}"
    return str(exc)
