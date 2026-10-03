from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Literal, TypedDict
from urllib.parse import urlparse


ResearchIntent = Literal[
    "news_policy_search",
    "pricing_analysis",
    "market_overview",
    "competitor_compare",
    "configuration_analysis",
    "voc_analysis",
    "report_generation",
    "general_qa",
]
ResearchMode = Literal["quick", "standard", "deep"]
SourceCategory = Literal["official", "industry_data", "media", "dealer", "forum_social", "unknown"]
CrossCheckStatus = Literal["matched", "partially_matched", "conflicting", "not_available", "not_applicable"]
ClaimType = Literal["fact", "inference", "recommendation"]


class ResearchPolicy(TypedDict):
    intent: str
    minSources: int
    preferredSourceTiers: list[str]
    requireOfficialSource: bool
    requirePublishDate: bool
    requireJatoCrossCheck: bool
    allowAnswerWithoutExternalSource: bool


class ResearchModeConfig(TypedDict):
    mode: str
    queryLimit: int
    sourceLimit: int
    description: str


class ResearchGovernance(TypedDict, total=False):
    policy: ResearchPolicy
    policyStatus: Literal["passed", "warning", "blocking"]
    policyWarnings: list[str]
    missingEvidence: list[dict[str, str]]
    mode: ResearchModeConfig
    metrics: dict[str, Any]
    rejectedSources: list[dict[str, Any]]


@dataclass(frozen=True)
class _SourceIdentity:
    canonical_url: str
    domain: str
    title_tokens: frozenset[str]
    title_key: str


_AUTOMOTIVE_RESEARCH_TERMS = {
    "auto",
    "automotive",
    "bev",
    "phev",
    "hev",
    "ev",
    "vehicle",
    "vehicles",
    "car",
    "cars",
    "electric",
    "electric car",
    "hybrid",
    "elbil",
    "elbilar",
    "emission",
    "emissions",
    "co2",
    "co₂",
    "tax",
    "benefit",
    "subsidy",
    "incentive",
    "policy",
    "climate",
    "transport",
    "mobility",
    "leasing",
    "fleet",
    "company",
    "msrp",
    "price",
    "pricing",
    "market",
    "sales",
    "share",
    "车型",
    "汽车",
    "车辆",
    "电动车",
    "纯电",
    "插混",
    "混动",
    "补贴",
    "税",
    "税费",
    "政策",
    "排放",
    "价格",
    "月供",
    "租赁",
    "公司车",
    "市场",
}
_IRRELEVANT_RESEARCH_TERMS = {
    "fifa",
    "football",
    "soccer",
    "world cup",
    "tunisia",
    "goal",
    "goals",
    "scores",
    "scored",
    "match",
    "league",
    "team",
}


_POLICIES: dict[str, ResearchPolicy] = {
    "news_policy_search": {
        "intent": "news_policy_search",
        "minSources": 3,
        "preferredSourceTiers": ["official", "industry_data", "media"],
        "requireOfficialSource": True,
        "requirePublishDate": True,
        "requireJatoCrossCheck": False,
        "allowAnswerWithoutExternalSource": False,
    },
    "pricing_analysis": {
        "intent": "pricing_analysis",
        "minSources": 2,
        "preferredSourceTiers": ["official", "dealer", "industry_data"],
        "requireOfficialSource": False,
        "requirePublishDate": False,
        "requireJatoCrossCheck": True,
        "allowAnswerWithoutExternalSource": False,
    },
    "market_overview": {
        "intent": "market_overview",
        "minSources": 0,
        "preferredSourceTiers": ["industry_data", "official", "media"],
        "requireOfficialSource": False,
        "requirePublishDate": False,
        "requireJatoCrossCheck": True,
        "allowAnswerWithoutExternalSource": True,
    },
    "competitor_compare": {
        "intent": "competitor_compare",
        "minSources": 2,
        "preferredSourceTiers": ["official", "dealer", "industry_data"],
        "requireOfficialSource": False,
        "requirePublishDate": False,
        "requireJatoCrossCheck": True,
        "allowAnswerWithoutExternalSource": True,
    },
    "configuration_analysis": {
        "intent": "configuration_analysis",
        "minSources": 1,
        "preferredSourceTiers": ["official", "industry_data", "dealer"],
        "requireOfficialSource": False,
        "requirePublishDate": False,
        "requireJatoCrossCheck": True,
        "allowAnswerWithoutExternalSource": True,
    },
    "voc_analysis": {
        "intent": "voc_analysis",
        "minSources": 1,
        "preferredSourceTiers": ["forum_social", "media", "industry_data", "official"],
        "requireOfficialSource": False,
        "requirePublishDate": False,
        "requireJatoCrossCheck": True,
        "allowAnswerWithoutExternalSource": True,
    },
    "report_generation": {
        "intent": "report_generation",
        "minSources": 2,
        "preferredSourceTiers": ["official", "industry_data", "media"],
        "requireOfficialSource": False,
        "requirePublishDate": False,
        "requireJatoCrossCheck": True,
        "allowAnswerWithoutExternalSource": True,
    },
    "general_qa": {
        "intent": "general_qa",
        "minSources": 0,
        "preferredSourceTiers": ["official", "industry_data", "media", "dealer"],
        "requireOfficialSource": False,
        "requirePublishDate": False,
        "requireJatoCrossCheck": False,
        "allowAnswerWithoutExternalSource": True,
    },
}

_MODE_CONFIGS: dict[str, ResearchModeConfig] = {
    "quick": {
        "mode": "quick",
        "queryLimit": 1,
        "sourceLimit": 3,
        "description": "Low-latency research: one query and at most three cited sources.",
    },
    "standard": {
        "mode": "standard",
        "queryLimit": 2,
        "sourceLimit": 5,
        "description": "Default research: balanced query count, source quality, and cost.",
    },
    "deep": {
        "mode": "deep",
        "queryLimit": 4,
        "sourceLimit": 8,
        "description": "Report-grade research: more queries and more source candidates.",
    },
}

_CATEGORY_RANK = {
    "official": 0,
    "industry_data": 1,
    "dealer": 2,
    "media": 3,
    "forum_social": 4,
    "unknown": 5,
}

_CLAIM_WORDS = re.compile(r"[\w\u4e00-\u9fff]+", flags=re.UNICODE)


def infer_research_intent(question: str, explicit_intent: str | None = None) -> ResearchIntent:
    candidate = str(explicit_intent or "").strip()
    if candidate in _POLICIES:
        return candidate  # type: ignore[return-value]
    text = str(question or "").casefold()
    if any(token in text for token in ("report", "deck", "ppt", "slide", "报告", "汇报", "大纲")):
        return "report_generation"
    if any(token in text for token in ("policy", "tax", "subsidy", "incentive", "news", "latest", "recent", "政策", "税", "补贴", "新闻", "最近", "最新")):
        return "news_policy_search"
    if any(token in text for token in ("price", "pricing", "msrp", "lease", "leasing", "monthly", "月供", "定价", "价格")):
        return "pricing_analysis"
    if any(token in text for token in ("compare", "competitor", " versus ", " vs ", "对比", "竞品", "相比")):
        return "competitor_compare"
    if any(token in text for token in (
        "voc",
        "voice of customer",
        "forum",
        "review",
        "complaint",
        "sentiment",
        "consumer",
        "owner",
        "customer",
        "用户",
        "车主",
        "消费者",
        "吐槽",
        "抱怨",
        "投诉",
        "论坛",
        "口碑",
        "差评",
        "用户声音",
        "用户反馈",
    )):
        return "voc_analysis"
    if any(token in text for token in ("configuration", "variant", "trim", "battery", "range", "bom", "配置", "版型", "物料", "电池", "续航")):
        return "configuration_analysis"
    if any(token in text for token in ("market", "share", "sales", "volume", "overview", "segment", "市场", "销量", "份额", "结构")):
        return "market_overview"
    return "general_qa"


def get_research_policy(intent: str) -> ResearchPolicy:
    policy = _POLICIES.get(str(intent or "").strip(), _POLICIES["general_qa"])
    return {**policy, "preferredSourceTiers": list(policy["preferredSourceTiers"])}


def normalize_research_mode(value: Any) -> ResearchMode:
    text = str(value or "").strip().casefold()
    if text in {"quick", "fast", "light", "cheap"}:
        return "quick"
    if text in {"deep", "thorough", "report", "report_grade"}:
        return "deep"
    return "standard"


def research_mode_config(mode: str) -> ResearchModeConfig:
    normalized = normalize_research_mode(mode)
    return dict(_MODE_CONFIGS[normalized])  # type: ignore[return-value]


def classify_source_category(*, url: Any = "", source: Any = "") -> SourceCategory:
    domain = _domain(url=url, source=source)
    if not domain:
        return "unknown"
    dealer_tokens = (
        "dealer",
        "leasing",
        "leaseplan",
        "arval",
        "aldautomotive",
        "wayke",
        "bilweb",
        "mobile.de",
        "autoscout",
        "autotrader",
    )
    if any(token in domain for token in dealer_tokens):
        return "dealer"
    official_tokens = (
        ".gov",
        "europa.eu",
        "transportstyrelsen",
        "skatteverket",
        "traficom",
        "ofv.no",
        "kba.de",
        "bafa.de",
        "bmf.gv",
        "rdw.nl",
        "volvocars",
        "toyota.",
        "kia.",
        "hyundai.",
        "tesla.",
        "bmw.",
        "mercedes-benz",
        "volkswagen.",
        "skoda.",
        "omodaauto",
        "jaecoo",
        "chery",
    )
    if any(token in domain for token in official_tokens):
        return "official"
    industry_tokens = (
        "jato",
        "mobilitysweden",
        "acea.auto",
        "eafo",
        "icct",
        "marklines",
        "statista",
    )
    if any(token in domain for token in industry_tokens):
        return "industry_data"
    forum_tokens = ("reddit", "facebook", "x.com", "twitter", "youtube", "tiktok", "forum", "flashback")
    if any(token in domain for token in forum_tokens):
        return "forum_social"
    media_tokens = (
        "reuters",
        "automotive-news",
        "autonews",
        "insideevs",
        "electrive",
        "carscoops",
        "autocar",
        "carwow",
        "motor1",
        "thelocal",
        "bloomberg",
    )
    if any(token in domain for token in media_tokens):
        return "media"
    return "unknown"


def dedupe_research_sources(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kept: list[dict[str, Any]] = []
    identities: list[_SourceIdentity] = []
    rejected: list[dict[str, Any]] = []

    for raw in items:
        item = dict(raw)
        item["sourceCategory"] = classify_source_category(url=item.get("url"), source=item.get("source"))
        identity = _source_identity(item)
        duplicate_index = _find_duplicate_index(identity, identities)
        if duplicate_index is None:
            kept.append(item)
            identities.append(identity)
            continue

        current = kept[duplicate_index]
        if _source_priority(item) < _source_priority(current):
            rejected.append(_rejected_source(current, "duplicate_replaced_by_better_source"))
            kept[duplicate_index] = item
            identities[duplicate_index] = identity
        else:
            rejected.append(_rejected_source(item, "duplicate_source"))

    ranked = sorted(
        kept,
        key=lambda item: (
            _CATEGORY_RANK.get(str(item.get("sourceCategory") or "unknown"), 5),
            -float(item.get("sourceScore") or 0),
            int(item.get("rankSeed") or item.get("rank") or 999),
        ),
    )
    return ranked, rejected


def filter_relevant_research_sources(
    items: list[dict[str, Any]],
    *,
    intent: str,
    question: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kept: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for item in items:
        if _is_weak_citation_source(item):
            rejected.append(_rejected_source(item, "weak_citation_source"))
            continue
        if _is_relevant_source(item, intent=intent, question=question):
            kept.append(item)
        else:
            rejected.append(_rejected_source(item, "low_question_relevance"))
    return kept, rejected


def standardize_jato_cross_check(raw: dict[str, Any], *, intent: str, question: str) -> dict[str, Any]:
    result = dict(raw)
    raw_status = str(raw.get("status") or "").strip()
    checks = raw.get("checks") if isinstance(raw.get("checks"), list) else []
    conflict_risk = str(raw.get("conflictRisk") or "").casefold()
    if "conflict" in conflict_risk and "manual" not in conflict_risk:
        status: CrossCheckStatus = "conflicting"
    elif _jato_not_applicable(intent=intent, question=question):
        status = "not_applicable"
    elif raw_status in {"unavailable", "not_available"}:
        status = "not_available"
    elif not checks or raw_status in {"no_structured_context", "empty"}:
        status = "not_available"
    elif len(checks) >= 2:
        status = "matched"
    else:
        status = "partially_matched"

    result["rawStatus"] = raw_status or "unknown"
    result["status"] = status
    result["summary"] = _cross_check_summary(status, raw.get("summary"))
    result["checkedAt"] = datetime.now(timezone.utc).isoformat()
    return result


def apply_research_governance(
    data: dict[str, Any],
    *,
    intent: str,
    question: str,
    research_mode: str,
    latency_ms: int,
) -> dict[str, Any]:
    policy = get_research_policy(intent)
    mode = research_mode_config(research_mode)
    source_limit = int(mode["sourceLimit"])

    original_items = _dict_list(data.get("items"))
    deduped, rejected = dedupe_research_sources(original_items)
    relevant, relevance_rejected = filter_relevant_research_sources(deduped, intent=intent, question=question)
    rejected.extend(relevance_rejected)
    deduped = relevant[:source_limit]
    deduped = [_with_citation_claims(item, intent=intent, question=question, index=index) for index, item in enumerate(deduped)]
    citations = [
        _citation_from_item(item)
        for item in deduped
        if str(item.get("title") or item.get("url") or "").strip()
    ]

    data["items"] = deduped
    data["citations"] = citations
    data["intent"] = intent
    coverage = data.get("sourceCoverage") if isinstance(data.get("sourceCoverage"), dict) else {}
    source_count = len(deduped)
    categories = _category_counts(deduped)
    metrics = {
        "queryCount": _query_count(data),
        "sourcesReturned": len(original_items),
        "sourcesUsed": source_count,
        "avgSourceScore": _average_source_score(deduped),
        "latencyMs": max(0, int(latency_ms)),
        "estimatedCost": _estimated_cost(mode=str(mode["mode"]), query_count=_query_count(data), sources_used=source_count),
    }
    coverage.update({
        "sourceCount": source_count,
        "domainCount": len({str(item.get("source") or "").strip() for item in deduped if str(item.get("source") or "").strip()}),
        "domains": sorted({str(item.get("source") or "").strip() for item in deduped if str(item.get("source") or "").strip()})[:8],
        "officialSourceCount": categories.get("official", 0),
        "sourceCategories": categories,
        "researchMode": mode["mode"],
        "queryCount": metrics["queryCount"],
        "sourcesReturned": metrics["sourcesReturned"],
        "sourcesUsed": metrics["sourcesUsed"],
        "latencyMs": metrics["latencyMs"],
        "estimatedCost": metrics["estimatedCost"],
        "averageSourceScore": metrics["avgSourceScore"],
    })
    data["sourceCoverage"] = coverage

    missing, warnings = _policy_gaps(policy=policy, items=deduped, data=data)
    policy_status = _policy_status(missing)
    governance: ResearchGovernance = {
        "policy": policy,
        "policyStatus": policy_status,
        "policyWarnings": warnings,
        "missingEvidence": missing,
        "mode": mode,
        "metrics": metrics,
        "rejectedSources": rejected[:10],
    }
    data["researchGovernance"] = governance
    data["confidence"] = _research_confidence(policy_status=policy_status, items=deduped, data=data)
    data["insightCards"] = build_insight_cards(data, intent=intent)
    data["limitations"] = _governed_limitations(data.get("limitations"), governance)

    research_plan = data.get("researchPlan") if isinstance(data.get("researchPlan"), list) else []
    research_plan.append({
        "step": len(research_plan) + 1,
        "name": "apply_research_policy",
        "status": policy_status,
        "detail": f"{intent} policy checked: {source_count} sources, {categories.get('official', 0)} official, mode {mode['mode']}.",
    })
    data["researchPlan"] = research_plan
    return data


def build_insight_cards(data: dict[str, Any], *, intent: str) -> list[dict[str, Any]]:
    if intent not in {"news_policy_search", "market_overview", "pricing_analysis", "report_generation"}:
        return []
    citations = _dict_list(data.get("citations"))
    if not citations:
        return []

    cards: list[dict[str, Any]] = []
    for index, citation in enumerate(citations[:4]):
        citation_id = str(citation.get("citationId") or f"R{index + 1}")
        supported_claim = str(citation.get("supportedClaim") or citation.get("sourceTitle") or "").strip()
        title = _insight_title(intent=intent, index=index, citation=citation)
        cards.append({
            "title": title,
            "claim": supported_claim or "External source provides a citation candidate for the research question.",
            "evidence": [f"[{citation_id}] {citation.get('sourceTitle') or citation.get('label') or citation.get('source') or 'source'}"],
            "implication": _insight_implication(intent),
            "recommendedAction": _insight_action(intent),
            "citations": [citation_id],
            "confidence": _card_confidence(citation),
        })
    return cards[:4]


def _dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _with_citation_claims(item: dict[str, Any], *, intent: str, question: str, index: int) -> dict[str, Any]:
    result = dict(item)
    if not str(result.get("citationId") or "").strip():
        result["citationId"] = f"R{index + 1}"
    result["sourceTitle"] = str(result.get("title") or result.get("sourceTitle") or "").strip()
    result["sourceCategory"] = classify_source_category(url=result.get("url"), source=result.get("source"))
    if not str(result.get("publishedAt") or "").strip():
        result["publishedAt"] = _infer_published_at(result)
    result["supportedClaim"] = _supported_claim(result, question=question)
    result["claimType"] = _claim_type(intent=intent, item=result)
    return result


def _citation_from_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": item.get("title", ""),
        "sourceTitle": item.get("sourceTitle") or item.get("title") or "",
        "url": item.get("url", ""),
        "source": item.get("source", ""),
        "publishedAt": item.get("publishedAt", ""),
        "snippet": item.get("snippet", ""),
        "citationId": item.get("citationId", ""),
        "sourceScore": item.get("sourceScore", 0),
        "sourceTier": item.get("sourceTier", ""),
        "sourceCategory": item.get("sourceCategory", "unknown"),
        "supportedClaim": item.get("supportedClaim", ""),
        "claimType": item.get("claimType", "fact"),
    }


def _supported_claim(item: dict[str, Any], *, question: str) -> str:
    snippet = str(item.get("snippet") or "").strip()
    title = str(item.get("title") or "").strip()
    source = str(item.get("source") or "").strip()
    if snippet:
        sentence = re.split(r"(?<=[.!?。！？])\s+", snippet)[0].strip()
        if sentence:
            return sentence[:260]
    if title:
        return f"{title} is a source candidate for: {str(question or '').strip()[:120]}".strip()
    return f"Source candidate from {source or 'external research'}."


def _claim_type(*, intent: str, item: dict[str, Any]) -> ClaimType:
    if intent == "report_generation":
        return "recommendation"
    category = str(item.get("sourceCategory") or "")
    if category in {"official", "industry_data", "media", "dealer"} and item.get("url"):
        return "fact"
    return "inference"


def _infer_published_at(item: dict[str, Any]) -> str:
    text = " ".join(str(item.get(key) or "") for key in ("url", "publishedAt", "date", "published"))
    for pattern in (
        r"(?<!\d)(20\d{2})[/-](0?[1-9]|1[0-2])[/-](0?[1-9]|[12]\d|3[01])(?!\d)",
        r"(?<!\d)(20\d{2})/(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])(?:/|$)",
    ):
        match = re.search(pattern, text)
        if match:
            year, month, day = match.groups()
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return ""


def _policy_gaps(
    *,
    policy: ResearchPolicy,
    items: list[dict[str, Any]],
    data: dict[str, Any],
) -> tuple[list[dict[str, str]], list[str]]:
    missing: list[dict[str, str]] = []
    warnings: list[str] = []
    min_sources = int(policy.get("minSources") or 0)
    allow_without_external = bool(policy.get("allowAnswerWithoutExternalSource"))
    if len(items) < min_sources:
        missing.append({
            "name": "minimum_external_sources",
            "reason": f"{policy['intent']} requires at least {min_sources} external sources; {len(items)} usable sources were kept.",
            "impact": "weakens_answer" if allow_without_external else "blocking",
        })
        warnings.append("minimum_external_sources_not_met")
    if policy.get("requireOfficialSource") and not any(item.get("sourceCategory") == "official" for item in items):
        missing.append({
            "name": "official_source",
            "reason": "Policy/news claims require at least one official source before stating current rules as fact.",
            "impact": "blocking" if not allow_without_external else "weakens_answer",
        })
        warnings.append("official_source_missing")
    if policy.get("requirePublishDate") and items:
        dated = [item for item in items if str(item.get("publishedAt") or "").strip()]
        if len(dated) < min(len(items), max(1, min_sources)):
            missing.append({
                "name": "published_date",
                "reason": "Research policy requires publish dates for time-sensitive policy or news claims.",
                "impact": "weakens_answer",
            })
            warnings.append("publish_date_missing")
    cross_check = data.get("jatoCrossCheck") if isinstance(data.get("jatoCrossCheck"), dict) else {}
    cross_status = str(cross_check.get("status") or "")
    if policy.get("requireJatoCrossCheck"):
        if cross_status == "conflicting":
            missing.append({
                "name": "jato_cross_check_conflict",
                "reason": "External source direction conflicts with internal JATO context.",
                "impact": "blocking",
            })
            warnings.append("jato_cross_check_conflicting")
        elif cross_status in {"not_available", ""}:
            missing.append({
                "name": "jato_cross_check",
                "reason": "This intent should be cross-checked against internal JATO data, but no matching structured context was available.",
                "impact": "weakens_answer",
            })
            warnings.append("jato_cross_check_not_available")
    if not items and not allow_without_external:
        warnings.append("external_research_required_but_empty")
    return missing, warnings


def _policy_status(missing: list[dict[str, str]]) -> Literal["passed", "warning", "blocking"]:
    if any(item.get("impact") == "blocking" for item in missing):
        return "blocking"
    if missing:
        return "warning"
    return "passed"


def _research_confidence(*, policy_status: str, items: list[dict[str, Any]], data: dict[str, Any]) -> Literal["high", "medium", "low"]:
    cross_check = data.get("jatoCrossCheck") if isinstance(data.get("jatoCrossCheck"), dict) else {}
    if policy_status == "blocking" or cross_check.get("status") == "conflicting":
        return "low"
    if not items:
        return "low"
    official_or_industry = [item for item in items if item.get("sourceCategory") in {"official", "industry_data"}]
    if policy_status == "passed" and len(items) >= 3 and official_or_industry:
        return "high"
    return "medium"


def _governed_limitations(value: Any, governance: ResearchGovernance) -> list[str]:
    result = [str(item).strip() for item in value if str(item).strip()] if isinstance(value, list) else []
    result.append("Source score is relevance/authority, not fact confidence; fact confidence also depends on policy fit and JATO cross-check.")
    for warning in governance.get("policyWarnings", [])[:4]:
        result.append(f"Research policy warning: {warning}.")
    return _dedupe_strings(result)


def _query_count(data: dict[str, Any]) -> int:
    value = data.get("sourceCoverage") if isinstance(data.get("sourceCoverage"), dict) else {}
    query_count = value.get("queryCount")
    if isinstance(query_count, int) and query_count > 0:
        return query_count
    queries = data.get("queriesRun") if isinstance(data.get("queriesRun"), list) else data.get("queriesTried")
    if isinstance(queries, list) and queries:
        return len(queries)
    return 1 if data.get("query") else 0


def _estimated_cost(*, mode: str, query_count: int, sources_used: int) -> float:
    mode_factor = {"quick": 1.0, "standard": 1.7, "deep": 3.0}.get(mode, 1.7)
    return round((query_count * 0.006 + sources_used * 0.0015) * mode_factor, 4)


def _average_source_score(items: list[dict[str, Any]]) -> int:
    scores = [int(float(item.get("sourceScore") or 0)) for item in items]
    if not scores:
        return 0
    return round(sum(scores) / len(scores))


def _category_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        category = str(item.get("sourceCategory") or "unknown")
        counts[category] = counts.get(category, 0) + 1
    return counts


def _source_identity(item: dict[str, Any]) -> _SourceIdentity:
    canonical_url = _canonical_url(item.get("url"))
    domain = _domain(url=item.get("url"), source=item.get("source"))
    tokens = _title_tokens(item.get("title"))
    title_key = " ".join(sorted(tokens)[:10])
    return _SourceIdentity(canonical_url=canonical_url, domain=domain, title_tokens=frozenset(tokens), title_key=title_key)


def _find_duplicate_index(identity: _SourceIdentity, identities: list[_SourceIdentity]) -> int | None:
    for index, existing in enumerate(identities):
        if identity.canonical_url and identity.canonical_url == existing.canonical_url:
            return index
        if identity.domain and identity.domain == existing.domain and _token_similarity(identity.title_tokens, existing.title_tokens) >= 0.72:
            return index
        if identity.title_key and identity.title_key == existing.title_key:
            return index
    return None


def _source_priority(item: dict[str, Any]) -> tuple[int, int, int]:
    category = str(item.get("sourceCategory") or "unknown")
    score = int(float(item.get("sourceScore") or 0))
    published = 1 if str(item.get("publishedAt") or "").strip() else 0
    return (_CATEGORY_RANK.get(category, 5), -score, -published)


def _is_relevant_source(item: dict[str, Any], *, intent: str, question: str) -> bool:
    text = _source_search_text(item)
    if not text:
        return False
    if _looks_clearly_irrelevant(text):
        return False
    effective_intent = _effective_relevance_intent(intent=intent, question=question)
    if str(item.get("sourceCategory") or "") in {"official", "industry_data", "dealer"}:
        return True
    topic_tokens = _meaningful_query_tokens(question)
    overlap = sum(1 for token in topic_tokens if token in text)
    auto_hits = sum(1 for token in _AUTOMOTIVE_RESEARCH_TERMS if _research_term_present(text, token))
    if effective_intent == "news_policy_search":
        policy_hits = any(
            _research_term_present(text, token)
            for token in (
                "policy",
                "subsid",
                "tax",
                "co2",
                "co₂",
                "incentive",
                "benefit",
                "climate",
                "elbilspremien",
                "bonus",
                "bidrag",
                "premie",
                "regeringen",
                "transportstyrelsen",
                "补贴",
                "政策",
                "税",
            )
        )
        specific_policy_terms = _specific_policy_query_terms(question)
        if specific_policy_terms:
            specific_hit = any(_research_term_present(text, token) for token in specific_policy_terms)
            return auto_hits >= 1 and (specific_hit or policy_hits)
        return auto_hits >= 1 and (overlap >= 2 or policy_hits)
    if effective_intent in {"pricing_analysis", "competitor_compare", "configuration_analysis"}:
        return auto_hits >= 1 or overlap >= 2
    if effective_intent in {"market_overview", "report_generation", "voc_analysis"}:
        return auto_hits >= 1 or overlap >= 1
    return True


def _effective_relevance_intent(*, intent: str, question: str) -> str:
    """Treat policy/report prompts as policy relevance checks, not generic report searches."""
    text = str(question or "").casefold()
    if intent == "report_generation" and any(
        token in text
        for token in (
            "policy",
            "subsid",
            "incentive",
            "tax",
            "co2",
            "co₂",
            "elbilspremien",
            "bonus",
            "bidrag",
            "premie",
            "补贴",
            "政策",
            "税",
        )
    ):
        return "news_policy_search"
    return intent


def _specific_policy_query_terms(question: str) -> set[str]:
    text = str(question or "").casefold()
    terms: set[str] = set()
    known_terms = (
        "elbilspremien",
        "bonus malus",
        "company car benefit",
        "bilförmån",
        "bilmån",
        "klimatbonus",
        "transportstyrelsen",
        "regeringen",
        "co2",
        "co₂",
    )
    for term in known_terms:
        if term in text:
            terms.add(term)
    return terms


def _is_weak_citation_source(item: dict[str, Any]) -> bool:
    domain = _domain(url=item.get("url"), source=item.get("source"))
    if not domain:
        return False
    weak_domains = (
        "grokipedia",
        "wikipedia.org",
        "wikidata.org",
        "wikiwand.com",
        "fandom.com",
    )
    return any(token in domain for token in weak_domains)


def _source_search_text(item: dict[str, Any]) -> str:
    values = [
        item.get("label"),
        item.get("name"),
        item.get("title"),
        item.get("sourceTitle"),
        item.get("claim"),
        item.get("supportedClaim"),
        item.get("snippet"),
        item.get("source"),
        item.get("url"),
    ]
    return " ".join(str(value or "") for value in values).casefold()


def _looks_clearly_irrelevant(text: str) -> bool:
    if not any(_term_present(text, token) for token in _IRRELEVANT_RESEARCH_TERMS):
        return False
    return not any(_research_term_present(text, token) for token in _AUTOMOTIVE_RESEARCH_TERMS)


def _research_term_present(text: str, token: str) -> bool:
    needle = str(token or "").casefold()
    if needle in {"policy", "政策"}:
        text = re.sub(r"\bprivacy\s+policy\b", "", text)
        text = re.sub(r"\bterms\s+of\s+use\b", "", text)
    return _term_present(text, token)


def _term_present(text: str, token: str) -> bool:
    needle = str(token or "").casefold()
    if not needle:
        return False
    if re.fullmatch(r"[a-z0-9]{1,3}", needle):
        return re.search(rf"\b{re.escape(needle)}\b", text) is not None
    return needle in text


def _meaningful_query_tokens(question: str) -> set[str]:
    text = str(question or "").casefold()
    stopwords = {
        "what",
        "which",
        "where",
        "when",
        "why",
        "how",
        "will",
        "would",
        "should",
        "does",
        "have",
        "the",
        "and",
        "for",
        "with",
        "about",
        "影响",
        "什么",
        "是否",
        "应该",
        "怎么",
        "哪些",
        "有什么",
    }
    return {token for token in _CLAIM_WORDS.findall(text) if len(token) >= 2 and token not in stopwords}


def _rejected_source(item: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "title": item.get("title", ""),
        "url": item.get("url", ""),
        "source": item.get("source", ""),
        "sourceCategory": item.get("sourceCategory", "unknown"),
        "reason": reason,
    }


def _canonical_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    host = parsed.netloc.casefold().removeprefix("www.")
    path = re.sub(r"/+$", "", parsed.path or "/")
    return f"{host}{path}".casefold()


def _domain(*, url: Any = "", source: Any = "") -> str:
    raw_url = str(url or "").strip()
    if raw_url:
        parsed = urlparse(raw_url)
        if parsed.netloc:
            return parsed.netloc.casefold().removeprefix("www.")
    return str(source or "").strip().casefold().removeprefix("www.")


def _title_tokens(value: Any) -> set[str]:
    text = str(value or "").casefold()
    return {token for token in _CLAIM_WORDS.findall(text) if len(token) >= 2}


def _token_similarity(left: frozenset[str], right: frozenset[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _jato_not_applicable(*, intent: str, question: str) -> bool:
    text = str(question or "").casefold()
    if intent == "news_policy_search" and any(token in text for token in ("policy", "subsidy", "tax", "政策", "补贴", "税")):
        return True
    return intent in {"general_qa", "report_generation"} and not any(token in text for token in ("market", "sales", "share", "price", "销量", "份额", "价格"))


def _cross_check_summary(status: CrossCheckStatus, raw_summary: Any) -> str:
    raw = str(raw_summary or "").strip()
    prefix = {
        "matched": "External research direction is supported by available JATO market context.",
        "partially_matched": "External research has partial JATO support, but the measurement basis may differ.",
        "conflicting": "External research conflicts with internal JATO context and needs manual review.",
        "not_available": "No matching JATO structured context was available for this external claim.",
        "not_applicable": "JATO historical data is not a direct validator for this policy/news claim.",
    }[status]
    return f"{prefix} {raw}".strip()[:360]


def _insight_title(*, intent: str, index: int, citation: dict[str, Any]) -> str:
    if intent == "pricing_analysis":
        return "Pricing evidence checkpoint" if index == 0 else "Pricing corridor input"
    if intent == "market_overview":
        return "Market signal" if index == 0 else "Market context"
    if intent == "report_generation":
        return "Report-ready source" if index == 0 else "Supporting report evidence"
    return "Policy/news signal" if index == 0 else "External research signal"


def _insight_implication(intent: str) -> str:
    mapping = {
        "pricing_analysis": "Use this source to validate price corridor, monthly payment, or competitor positioning before making a firm pricing recommendation.",
        "market_overview": "将该来源作为外部背景，最终结论仍需回到 JATO 内部销量、份额和细分结构证据验证。",
        "report_generation": "This source can become one cited insight in the market report or product deck.",
        "news_policy_search": "Use this source to decide what needs official-source confirmation before making policy claims.",
    }
    return mapping.get(intent, "Use this source as a citation candidate and cross-check with internal evidence.")


def _insight_action(intent: str) -> str:
    mapping = {
        "pricing_analysis": "对比官方/经销商/租赁价格与 JATO 价格走廊。",
        "market_overview": "量化受影响细分市场，并生成市场机会视图。",
        "report_generation": "把引用结论整理成一页汇报并写明限制。",
        "news_policy_search": "确认官方来源、发布日期和受影响车型资格。",
    }
    return mapping.get(intent, "验证结论并附上最佳可用来源。")


def _card_confidence(citation: dict[str, Any]) -> Literal["high", "medium", "low"]:
    category = citation.get("sourceCategory")
    score = citation.get("sourceScore")
    numeric_score = int(float(score or 0)) if isinstance(score, int | float) else 0
    if category in {"official", "industry_data"} and numeric_score >= 65:
        return "high"
    if numeric_score >= 45:
        return "medium"
    return "low"


def _dedupe_strings(items: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
