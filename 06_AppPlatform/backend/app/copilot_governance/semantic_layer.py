"""Semantic Layer — resolves user terms to canonical business entities."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SemanticEntity(BaseModel):
    raw_text: str
    entity_type: str
    canonical_value: str | None = None
    aliases: list[str] = Field(default_factory=list)
    confidence: float = 0.0


class SemanticParseResult(BaseModel):
    country: str | None = None
    brands: list[str] = Field(default_factory=list)
    models: list[str] = Field(default_factory=list)
    powertrains: list[str] = Field(default_factory=list)
    segments: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    time_hint: str | None = None
    business_goal: str | None = None
    entities: list[SemanticEntity] = Field(default_factory=list)
    missing_slots: list[str] = Field(default_factory=list)


# ── Canonical aliases ────────────────────────────────────────────

_POWERTRAIN_ALIASES: dict[str, list[str]] = {
    "BEV": ["电动车", "纯电", "纯电动", "ev", "bev", "battery electric", "电动", "电池"],
    "PHEV": ["插混", "插电混动", "插电", "phev", "plug-in", "plug in"],
    "HEV": ["混动", "油电混动", "全混", "hev", "hybrid", "混合动力"],
    "MHEV": ["轻混", "48v", "mhev", "mild hybrid"],
    "ICE": ["燃油", "汽油", "柴油", "内燃机", "ice", "gasoline", "diesel", "petrol"],
}

_SEGMENT_ALIASES: dict[str, list[str]] = {
    "SUV-A": ["suv a", "紧凑suv", "小型suv"],
    "SUV-B": ["suv b", "中型suv", "中型suv"],
    "SUV-C": ["suv c", "大型suv", "大型suv"],
    "SUV-A0": ["小型suv", "小suv"],
    "SD-B": ["轿车b", "中型轿车"],
    "SD-C": ["轿车c", "大型轿车", "豪华轿车"],
}

_METRIC_ALIASES: dict[str, list[str]] = {
    "销量": ["销量", "销售", "卖了多少", "卖得", "sales", "volume"],
    "份额": ["份额", "占比", "比例", "share", "percentage"],
    "价格": ["价格", "定价", "价位", "多少钱", "price", "msrp", "cost"],
    "趋势": ["趋势", "变化", "增长", "下降", "trend", "growth", "decline"],
}

_BUSINESS_GOAL_ALIASES: dict[str, list[str]] = {
    "pricing_strategy": ["定价", "定价策略", "价格策略", "定价建议"],
    "market_entry": ["能不能卖", "能不能进入", "进入市场", "市场可行性"],
    "competitive_analysis": ["对比", "竞品", "竞争对手", "比较"],
    "cost_analysis": ["成本", "税费", "税负", "补贴", "碳税"],
}


def resolve_powertrain(text: str) -> list[str]:
    lowered = text.lower()
    results = []
    for canonical, aliases in _POWERTRAIN_ALIASES.items():
        if any(alias in lowered for alias in aliases):
            results.append(canonical)
    return results


def resolve_segment(text: str) -> list[str]:
    lowered = text.lower()
    results = []
    for canonical, aliases in _SEGMENT_ALIASES.items():
        if any(alias in lowered for alias in aliases):
            results.append(canonical)
    return results


def resolve_metrics(text: str) -> list[str]:
    lowered = text.lower()
    results = []
    for canonical, aliases in _METRIC_ALIASES.items():
        if any(alias in lowered for alias in aliases):
            results.append(canonical)
    return results


def resolve_business_goal(text: str) -> str | None:
    lowered = text.lower()
    for goal, aliases in _BUSINESS_GOAL_ALIASES.items():
        if any(alias in lowered for alias in aliases):
            return goal
    return None


def parse_semantic(
    question: str,
    country: str = "",
    user_params: dict[str, Any] | None = None,
) -> SemanticParseResult:
    params = user_params or {}
    brands = list(params.get("brands", []) or [])
    if params.get("brand"):
        brands.append(str(params["brand"]))
    models = list(params.get("models", []) or [])
    if params.get("model"):
        models.append(str(params["model"]))

    powertrains = resolve_powertrain(question)
    if params.get("powertrain"):
        powertrains.append(str(params["powertrain"]))

    segments = resolve_segment(question)
    if params.get("segment"):
        segments.append(str(params["segment"]))

    metrics = resolve_metrics(question)
    goal = resolve_business_goal(question)
    missing: list[str] = []
    if not brands:
        missing.append("brand")
    if not metrics:
        missing.append("metric")

    return SemanticParseResult(
        country=country or None,
        brands=brands,
        models=models,
        powertrains=powertrains,
        segments=segments,
        metrics=metrics,
        business_goal=goal,
        missing_slots=missing,
    )
