from __future__ import annotations

from typing import Any


DEFAULT_SKILL_ID = "auto_route"

_SKILLS: dict[str, dict[str, Any]] = {
    "auto_route": {
        "id": "auto_route",
        "name": "Auto Route",
        "domain": "general",
        "routeMode": "auto",
        "description": "Let JATO choose the best governed tool from the question.",
        "defaultCountry": "Sweden",
        "defaultQuestion": "Draw a 2025 BEV market trend chart and explain the key movement.",
        "outputContract": ["selected tool", "evidence pack", "display cards", "debug payload"],
    },
    "market_chart_analysis": {
        "id": "market_chart_analysis",
        "name": "Market Chart Analysis",
        "domain": "automotive_market",
        "routeMode": "chart",
        "description": "Build chart-ready market context for trend and share questions.",
        "defaultCountry": "Sweden",
        "defaultQuestion": "Draw a 2025 BEV market trend chart and explain the key movement.",
        "outputContract": ["chart context", "market KPIs", "evidence source", "next chart action"],
    },
    "policy_news_scan": {
        "id": "policy_news_scan",
        "name": "Policy / News Scan",
        "domain": "automotive_market",
        "routeMode": "news",
        "description": "Search current market, policy and subsidy news with Tavily-first provider order.",
        "defaultCountry": "Sweden",
        "defaultQuestion": "What policy or subsidy news could affect PHEV demand?",
        "outputContract": ["news results", "top provider", "source list", "cross-check action"],
    },
    "pricing_msrp_analysis": {
        "id": "pricing_msrp_analysis",
        "name": "Pricing / MSRP Analysis",
        "domain": "pricing",
        "routeMode": "pricing",
        "description": "Look up MSRP pricing records for price positioning and competitor comparison.",
        "defaultCountry": "Germany",
        "defaultQuestion": "What is the current MSRP positioning for XC60?",
        "outputContract": ["price records", "first match", "price evidence", "source limitations"],
    },
    "variant_config_compare": {
        "id": "variant_config_compare",
        "name": "Variant / Config Compare",
        "domain": "product_definition",
        "routeMode": "variant",
        "description": "Compare vehicle variants and feature differences using JATO engineering data.",
        "defaultCountry": "Sweden",
        "defaultQuestion": "Compare the key configuration differences for selected PHEV variants.",
        "outputContract": ["variant subjects", "diff features", "common features", "source limitations"],
    },
    "country_snapshot_brief": {
        "id": "country_snapshot_brief",
        "name": "Country Snapshot Brief",
        "domain": "automotive_market",
        "routeMode": "snapshot",
        "description": "Build a governed country market snapshot for grounded summary answers.",
        "defaultCountry": "Sweden",
        "defaultQuestion": "Summarize the 2025 Sweden automotive market with key powertrain signals.",
        "outputContract": ["snapshot sections", "KPIs", "rankings", "limitations"],
    },
    "code_debugging": {
        "id": "code_debugging",
        "name": "Code Debugging",
        "domain": "engineering",
        "routeMode": "auto",
        "description": "Structure code and log debugging work before using repository or shell tools.",
        "defaultCountry": "Sweden",
        "defaultQuestion": "This API is failing. Help me identify the likely root cause and validation steps.",
        "outputContract": ["symptom", "likely root cause", "fix steps", "validation commands"],
        "safety": "No destructive shell or database action without explicit confirmation.",
    },
}


def list_agent_skills() -> dict[str, Any]:
    return {
        "defaultSkillId": DEFAULT_SKILL_ID,
        "items": [dict(skill) for skill in _SKILLS.values()],
    }


def get_agent_skill(skill_id: str | None) -> dict[str, Any]:
    normalized = str(skill_id or "").strip()
    return dict(_SKILLS.get(normalized) or _SKILLS[DEFAULT_SKILL_ID])


def infer_skill_id_from_mode(mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    for skill in _SKILLS.values():
        if skill.get("routeMode") == normalized:
            return str(skill["id"])
    return DEFAULT_SKILL_ID
