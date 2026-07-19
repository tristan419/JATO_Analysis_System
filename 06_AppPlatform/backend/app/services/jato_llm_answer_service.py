from __future__ import annotations

import json
import os
from typing import Any
from urllib.request import Request, urlopen

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_BASE = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat"


def synthesize_agent_answer(
    *,
    country: str,
    question: str,
    tool_name: str,
    tool_data: dict[str, Any],
    retrieval_path: str = "",
    skill_name: str = "",
) -> dict[str, Any]:
    """Call DeepSeek to produce a natural-language answer from tool data."""
    if not DEEPSEEK_API_KEY:
        return {
            "direct": "LLM API key not configured. Set DEEPSEEK_API_KEY to enable AI-powered answers.",
            "bullets": [],
            "citations": [],
            "limitations": ["No LLM available — answer is raw data only"],
            "confidence": "none",
            "modelUsed": "none",
        }

    system_prompt = (
        "You are a senior automotive market analyst. Answer the user's question based STRICTLY on the provided data. "
        "Do not fabricate numbers, trends, or policies not present in the data. "
        "If data is insufficient, say so explicitly. "
        "Structure your answer as: 1) Direct conclusion (1-2 sentences), 2) Key evidence bullets (3-5), 3) Limitations/caveats. "
        "Cite specific numbers from the data when available. "
        "Keep the answer concise and scannable — the user is a product manager or market analyst. "
        "Respond in the user's language (Chinese or English based on the question)."
    )

    # Compact the tool data to fit context window
    context = _compact_for_llm(tool_data)

    user_message = (
        f"Country: {country}\n"
        f"Question: {question}\n"
        f"Tool used: {tool_name}\n"
        f"Retrieval path: {retrieval_path}\n"
        f"Skill: {skill_name}\n\n"
        f"=== DATA ===\n{context}\n=== END DATA ===\n\n"
        f"Analyze the data above and answer the question. Be direct and evidence-based."
    )

    try:
        response = _call_deepseek(system_prompt, user_message)
        return _parse_structured_answer(response, tool_name)
    except Exception as exc:
        return {
            "direct": f"LLM call failed: {exc}. Raw data is available in the evidence section below.",
            "bullets": [],
            "citations": [],
            "limitations": [f"LLM error: {str(exc)[:200]}"],
            "confidence": "none",
            "modelUsed": "error",
        }


def _call_deepseek(system_prompt: str, user_message: str) -> str:
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.3,
        "max_tokens": 1500,
        "stream": False,
    }
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        f"{DEEPSEEK_BASE}/chat/completions",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        return body["choices"][0]["message"]["content"]


def _compact_for_llm(data: dict[str, Any], max_chars: int = 8000) -> str:
    """Compact tool data into a concise LLM-friendly summary."""
    parts = []

    # KPIs
    kpis = data.get("kpis")
    if isinstance(kpis, dict) and kpis:
        parts.append("KPIs: " + json.dumps(kpis, ensure_ascii=False, default=str))

    # Year series (trend data)
    ys = data.get("yearSeries")
    if isinstance(ys, list) and ys:
        parts.append(f"Yearly trends ({len(ys)} years): " + json.dumps(ys[:5], ensure_ascii=False, default=str))

    # Month series
    ms = data.get("monthSeries")
    if isinstance(ms, list) and ms:
        parts.append(f"Monthly trends ({len(ms)} months): " + json.dumps(ms[:12], ensure_ascii=False, default=str))

    # Top brands
    tb = data.get("topBrands")
    if isinstance(tb, list) and tb:
        parts.append(f"Top brands ({len(tb)}): " + json.dumps(tb[:10], ensure_ascii=False, default=str))

    # Top models
    tm = data.get("topModels")
    if isinstance(tm, list) and tm:
        parts.append(f"Top models ({len(tm)}): " + json.dumps(tm[:10], ensure_ascii=False, default=str))

    # Powertrain mix
    pm = data.get("powertrainMix")
    if isinstance(pm, (dict, list)) and pm:
        parts.append("Powertrain mix: " + json.dumps(pm, ensure_ascii=False, default=str))

    # Pricing items
    items = data.get("items")
    if isinstance(items, list) and items:
        parts.append(f"Price records ({len(items)}): " + json.dumps(items[:10], ensure_ascii=False, default=str))

    # News items
    news_items = data.get("items") if tool_has_news(data) else None
    if not news_items:
        # Check if this is a news/search result
        sections = data.get("sections")
        if isinstance(sections, list) and sections:
            parts.append(f"Document sections ({len(sections)}): " + json.dumps(sections[:6], ensure_ascii=False, default=str))

    # Variant diff
    subjects = data.get("subjects") or data.get("compareSubjects")
    if isinstance(subjects, list) and subjects:
        parts.append(f"Compared variants ({len(subjects)}): " + json.dumps(subjects, ensure_ascii=False, default=str))
    diff = data.get("diffFeatures") or data.get("differences")
    if isinstance(diff, list) and diff:
        parts.append(f"Key differences ({len(diff)}): " + json.dumps(diff[:10], ensure_ascii=False, default=str))

    # Entities/chunks from MiniRAG
    entities = data.get("entities")
    if isinstance(entities, list) and entities:
        parts.append(f"Entities ({len(entities)}): " + json.dumps(entities[:10], ensure_ascii=False, default=str))
    chunks = data.get("supportingChunks")
    if isinstance(chunks, list) and chunks:
        parts.append(f"Evidence chunks ({len(chunks)}): " + json.dumps(chunks[:5], ensure_ascii=False, default=str))

    # Chart metadata (don't include full chart specs)
    chart_specs = data.get("chartSpecs")
    if isinstance(chart_specs, dict):
        parts.append(f"Charts available: {chart_specs.get('chartCount', 0)} charts")

    # Fallback: include raw but truncated
    if not parts:
        raw = json.dumps(data, ensure_ascii=False, default=str)
        parts.append(raw[:max_chars])

    result = "\n\n".join(parts)
    if len(result) > max_chars:
        result = result[:max_chars] + "\n... [truncated]"
    return result


def _parse_structured_answer(text: str, tool_name: str) -> dict[str, Any]:
    """Parse LLM response into structured answer format."""
    lines = text.strip().split("\n")
    # Extract a direct conclusion (first substantive paragraph)
    direct = ""
    bullets = []
    limitations = []
    current_section = "conclusion"

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        # Detect section markers
        lower = stripped.lower()
        if any(m in lower for m in ["limitation", "caveat", "note:", "注意", "限制", "不足"]):
            current_section = "limitations"
            continue
        if any(m in lower for m in ["evidence", "key", "bullet", "证据", "关键", "要点"]):
            current_section = "bullets"
            continue

        if current_section == "conclusion" and not direct:
            direct = stripped
        elif current_section == "bullets" and (stripped.startswith(("-", "•", "*", "1.", "2.", "3.", "4.", "5."))):
            bullets.append(stripped.lstrip("-•* 0123456789."))
        elif current_section == "limitations":
            limitations.append(stripped)

    # If parsing failed, use whole text as direct answer
    if not direct:
        direct = text[:300]

    return {
        "direct": direct,
        "bullets": bullets[:6],
        "citations": [],
        "limitations": limitations[:4],
        "confidence": "medium" if bullets else "low",
        "modelUsed": "deepseek-chat",
        "rawResponse": text,
    }


def tool_has_news(data: dict[str, Any]) -> bool:
    """Check if data looks like a news/search result."""
    items = data.get("items")
    if isinstance(items, list) and items:
        first = items[0] if items else {}
        return isinstance(first, dict) and ("title" in first or "provider" in first)
    return False
