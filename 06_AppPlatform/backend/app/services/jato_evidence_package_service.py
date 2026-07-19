from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Literal, TypedDict

from app.services.jato_business_method_distillation_service import get_active_pricing_method
from app.services.jato_research_governance_service import filter_relevant_research_sources
from app.services.jato_tool_coverage_guard_service import missing_required_tools


EvidenceConfidence = Literal["high", "medium", "low"]
EvidenceSourceType = Literal[
    "jato_parquet",
    "postgres",
    "web",
    "voc",
    "policy",
    "engineering",
    "generated",
    "user_material",
]


class EvidenceRef(TypedDict, total=False):
    refId: str
    label: str
    value: str | int | float
    unit: str
    source: str
    table: str
    rowCount: int
    retrievedAt: str
    evidenceStatus: Literal["verified", "hypothesis", "candidate"]
    sourceType: EvidenceSourceType
    claimType: str
    entityIds: list[str]
    country: str
    scopeKey: str
    periodType: Literal["month", "ytd", "rolling_12", "full_year", "custom", "unknown"]
    periodLabel: str
    periodStart: str
    periodEnd: str


class ToolEvidence(TypedDict, total=False):
    toolName: str
    query: dict[str, Any]
    success: bool
    rowCount: int
    freshness: str
    sourceType: EvidenceSourceType
    summary: str
    keyFindings: list[str]
    evidenceRefs: list[EvidenceRef]
    error: str
    coverageDiagnostics: dict[str, Any]


class MissingEvidence(TypedDict):
    name: str
    reason: str
    impact: Literal["blocking", "weakens_answer", "optional"]


class EvidencePackage(TypedDict, total=False):
    evidenceId: str
    sessionId: str
    intent: str
    country: str
    entities: dict[str, Any]
    toolResults: list[ToolEvidence]
    missingEvidence: list[MissingEvidence]
    confidence: EvidenceConfidence
    researchGovernance: dict[str, Any]
    jatoCrossCheck: dict[str, Any]
    insightCards: list[dict[str, Any]]
    scopeDiagnostics: dict[str, Any]


def build_evidence_package(
    *,
    session_id: str,
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
    tool_results: list[dict[str, Any]],
) -> EvidencePackage:
    retrieved_at = _now_iso()
    intent = str(evidence_plan.get("intent") or "general_qa")
    evidences = [
        _tool_result_to_evidence(
            item,
            index=index,
            retrieved_at=retrieved_at,
            intent=intent,
            question=question,
            requested_country=country,
        )
        for index, item in enumerate(tool_results)
    ]
    method_evidence = _business_method_material_evidence(
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        index=len(evidences),
        retrieved_at=retrieved_at,
    )
    if method_evidence:
        evidences.append(method_evidence)
    target_price_evidence = _user_supplied_target_price_evidence(
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        index=len(evidences),
        retrieved_at=retrieved_at,
    )
    if target_price_evidence:
        evidences.append(target_price_evidence)
    target_delta_evidence = _user_supplied_price_delta_evidence(
        country=country,
        question=question,
        evidence_plan=evidence_plan,
        index=len(evidences),
        retrieved_at=retrieved_at,
    )
    if target_delta_evidence:
        evidences.append(target_delta_evidence)
    missing = _missing_evidence(evidence_plan, evidences, question=question)
    scope_diagnostics = _evidence_scope_diagnostics(evidences)
    for conflict in scope_diagnostics.get("conflicts", []):
        metric = str(conflict.get("metric") or "market_metric")
        period_label = str(conflict.get("periodLabel") or "same period")
        missing.append({
            "name": f"evidence_scope_conflict:{metric}",
            "reason": (
                f"Conflicting values were returned for {metric} under the same evidence scope "
                f"({period_label}); the composer must not select one value until the source is reconciled."
            ),
            "impact": "blocking",
        })
    if method_evidence and not _has_non_method_evidence_refs(evidences):
        method_model = str((method_evidence.get("query") or {}).get("model") or "target model")
        missing.append({
            "name": "current_official_msrp_cross_check",
            "reason": f"{method_model} pricing method is backed by user material, but current official MSRP / competitor corridor still needs live source cross-check.",
            "impact": "weakens_answer",
        })
    research_governance = _research_governance_from_tool_results(tool_results)
    if research_governance:
        missing.extend(_governance_missing_evidence(research_governance, evidences))
    jato_cross_check = _jato_cross_check_from_tool_results(tool_results)
    insight_cards = _insight_cards_from_tool_results(tool_results)
    confidence = _confidence(evidences, missing)
    if method_evidence and not _has_non_method_evidence_refs(evidences):
        confidence = "medium"
    confidence = _governed_confidence(confidence, research_governance, jato_cross_check)
    evidence_id = _evidence_id(session_id=session_id, country=country, question=question, intent=intent, retrieved_at=retrieved_at)
    entities = evidence_plan.get("entities") if isinstance(evidence_plan.get("entities"), dict) else {}
    package: EvidencePackage = {
        "evidenceId": evidence_id,
        "sessionId": session_id,
        "intent": intent,
        "country": country,
        "entities": dict(entities),
        "toolResults": evidences,
        "missingEvidence": missing,
        "confidence": confidence,
    }
    if research_governance:
        package["researchGovernance"] = research_governance
    if jato_cross_check:
        package["jatoCrossCheck"] = jato_cross_check
    if insight_cards:
        package["insightCards"] = insight_cards
    if scope_diagnostics.get("parallelScopes") or scope_diagnostics.get("conflicts"):
        package["scopeDiagnostics"] = scope_diagnostics
    return package


def evidence_ref_count(evidence_package: dict[str, Any]) -> int:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return 0
    total = 0
    for item in tool_results:
        if isinstance(item, dict) and isinstance(item.get("evidenceRefs"), list):
            total += sum(1 for ref in item["evidenceRefs"] if is_usable_evidence_ref(ref))
    return total


def is_usable_evidence_ref(ref: Any) -> bool:
    """Return True for refs that can support business conclusions, not technical counts."""
    return isinstance(ref, dict) and not _is_weak_ref(ref)


def evidence_tool_names(evidence_package: dict[str, Any]) -> list[str]:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return []
    names: list[str] = []
    for item in tool_results:
        if isinstance(item, dict):
            name = str(item.get("toolName") or "").strip()
            if name:
                names.append(name)
    return names


def _append_business_method_ref(
    refs: list[EvidenceRef],
    index: int,
    *,
    model: str,
    country: str,
    label_suffix: str,
    value: Any,
    source: str,
    claim_type: str,
    retrieved_at: str,
    unit: str = "",
) -> None:
    previous_count = len(refs)
    _append_ref_if_value(
        refs,
        index,
        f"{model} user material {label_suffix}",
        value,
        source,
        "business_method_material",
        1,
        retrieved_at,
    )
    if len(refs) == previous_count:
        return
    ref = refs[-1]
    ref["evidenceStatus"] = "hypothesis"
    ref["sourceType"] = "user_material"
    ref["claimType"] = claim_type
    ref["entityIds"] = [model]
    ref["country"] = _canonical_country(country) or country
    if unit:
        ref["unit"] = unit


def _business_method_material_evidence(
    *,
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
    index: int,
    retrieved_at: str,
) -> ToolEvidence | None:
    intent = str(evidence_plan.get("intent") or "")
    if intent not in {"pricing_analysis", "report_generation", "competitor_compare", "market_overview"}:
        return None
    if not _should_include_business_method_material(
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    ):
        return None
    model = _model_hint(evidence_plan)
    method = get_active_pricing_method(country=country, model=model, question=question)
    if not method:
        return None
    method_model = str(method.get("model") or model or "target model").strip()
    source = str(method.get("sourceName") or method.get("deckTitle") or "business_method_material")
    refs: list[EvidenceRef] = []
    _append_business_method_ref(
        refs,
        index,
        model=method_model,
        country=country,
        label_suffix="positioning",
        value=method["priceCorridor"]["positioning"],
        source=source,
        claim_type="pricing_positioning",
        retrieved_at=retrieved_at,
    )
    _append_business_method_ref(
        refs,
        index,
        model=method_model,
        country=country,
        label_suffix="competitor corridor",
        value=method["priceCorridor"]["coreCorridor"],
        source=source,
        claim_type="competitor_price_corridor",
        retrieved_at=retrieved_at,
    )
    _append_business_method_ref(
        refs,
        index,
        model=method_model,
        country=country,
        label_suffix="main trim MSRP",
        value=_number_from_text(method["priceCorridor"]["mainTrimPrice"]),
        source=source,
        claim_type="main_trim_msrp_hypothesis",
        unit="EUR",
        retrieved_at=retrieved_at,
    )
    _append_business_method_ref(
        refs,
        index,
        model=method_model,
        country=country,
        label_suffix="price gap",
        value=_number_from_text(method["versionStrategy"]["priceGap"]),
        source=source,
        claim_type="trim_price_gap_hypothesis",
        unit="EUR",
        retrieved_at=retrieved_at,
    )
    _append_business_method_ref(
        refs,
        index,
        model=method_model,
        country=country,
        label_suffix="PVA coverage",
        value=_number_from_text(method["versionStrategy"]["pvaCoverage"]),
        source=source,
        claim_type="pva_coverage_hypothesis",
        unit="%",
        retrieved_at=retrieved_at,
    )
    _append_business_method_ref(
        refs,
        index,
        model=method_model,
        country=country,
        label_suffix="competitor pool",
        value=", ".join(method.get("competitorPool", [])[:6]),
        source=source,
        claim_type="competitor_pool_hypothesis",
        retrieved_at=retrieved_at,
    )
    _append_business_method_ref(
        refs,
        index,
        model=method_model,
        country=country,
        label_suffix="market window",
        value=method["pricingPlaybook"]["market_window"],
        source=source,
        claim_type="market_window_hypothesis",
        retrieved_at=retrieved_at,
    )
    for feature in method.get("featureValueClaims", [])[:4]:
        if not isinstance(feature, dict):
            continue
        _append_business_method_ref(
            refs,
            index,
            model=method_model,
            country=country,
            label_suffix=f"visible feature value.{feature.get('featureName', 'feature')}",
            value=str(feature.get("customerValue") or feature.get("businessUse") or ""),
            source=source,
            claim_type="visible_feature_value_hypothesis",
            retrieved_at=retrieved_at,
        )
    if not refs:
        return None
    return {
        "toolName": "business_method_material",
        "query": {"country": country, "question": question, "model": method_model},
        "success": True,
        "rowCount": len(refs),
        "freshness": "user_material",
        "sourceType": "user_material",
        "summary": f"{method_model} pricing hypothesis distilled from user material {source}.",
        "keyFindings": method.get("coreClaims", [])[:5],
        "evidenceRefs": refs[:12],
    }


def _should_include_business_method_material(
    *,
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
) -> bool:
    if _question_requests_business_method_material(question):
        return True
    return _question_requests_scoped_pricing_method_material(
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    )


def _question_requests_business_method_material(question: str) -> bool:
    """Surface local method samples when the user explicitly asks for material/template logic."""
    text = str(question or "").casefold()
    if not text:
        return False
    return any(
        marker in text
        for marker in (
            "ppt",
            "deck",
            "slide",
            "material",
            "template",
            "playbook",
            "method",
            "j7_hev_v4",
            "材料",
            "模板",
            "原稿",
            "方案",
            "方法",
            "方法论",
            "我给",
            "我的",
        )
    )


def _question_requests_scoped_pricing_method_material(
    *,
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
) -> bool:
    """Use scoped J7 material as a pricing/config hypothesis, not as a global template."""
    intent = str(evidence_plan.get("intent") or "")
    if intent not in {"pricing_analysis", "competitor_compare", "report_generation"}:
        return False
    entities = evidence_plan.get("entities") if isinstance(evidence_plan.get("entities"), dict) else {}
    entity_countries = _canonical_country_list(entities.get("countries"))
    requested_country = _canonical_country(country)
    if entity_countries and requested_country and requested_country not in entity_countries:
        return False
    text = str(question or "").casefold()
    if not _model_hint(evidence_plan).strip():
        return False
    return any(
        marker in text
        for marker in (
            "price",
            "pricing",
            "msrp",
            "corridor",
            "configuration",
            "config",
            "competitor",
            "compare",
            "report",
            "定价",
            "价格",
            "价差",
            "走廊",
            "配置",
            "竞品",
            "对标",
            "汇报",
            "定位",
            "高配",
            "低配",
            "月供",
        )
    )


def _user_supplied_target_price_evidence(
    *,
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
    index: int,
    retrieved_at: str,
) -> ToolEvidence | None:
    if str(evidence_plan.get("intent") or "") != "pricing_analysis":
        return None
    target = _target_price_range_from_question(question)
    if not target:
        return None
    model = _model_hint(evidence_plan) or "own model"
    refs: list[EvidenceRef] = []
    _append_ref_if_value(
        refs,
        index,
        "User supplied own-model target price min",
        target["min"],
        "user_question",
        "user_supplied_target_price",
        1,
        retrieved_at,
    )
    if refs:
        refs[-1]["unit"] = target["currency"]
    _append_ref_if_value(
        refs,
        index,
        "User supplied own-model target price max",
        target["max"],
        "user_question",
        "user_supplied_target_price",
        1,
        retrieved_at,
    )
    if refs:
        refs[-1]["unit"] = target["currency"]
    _append_ref_if_value(
        refs,
        index,
        "User supplied own-model target price midpoint",
        target["midpoint"],
        "user_question",
        "user_supplied_target_price",
        1,
        retrieved_at,
    )
    if refs:
        refs[-1]["unit"] = target["currency"]
    return {
        "toolName": "user_supplied_target_price",
        "query": {"country": country, "question": question, "model": model},
        "success": True,
        "rowCount": 1,
        "freshness": "user_supplied",
        "sourceType": "generated",
        "summary": (
            f"{model} target price range supplied by the user question; "
            "use as a scenario input, not as official current MSRP."
        ),
        "keyFindings": [
            f"target_price_range:{target['min']}-{target['max']} {target['currency']}",
            "official_current_msrp_still_requires_cross_check",
        ],
        "evidenceRefs": refs,
    }


def _user_supplied_price_delta_evidence(
    *,
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
    index: int,
    retrieved_at: str,
) -> ToolEvidence | None:
    if str(evidence_plan.get("intent") or "") != "pricing_analysis":
        return None
    delta = _price_delta_from_question(question)
    if not delta:
        return None
    model = _model_hint(evidence_plan) or "own model"
    refs: list[EvidenceRef] = []
    _append_ref_if_value(
        refs,
        index,
        "User supplied relative price delta",
        delta["amount"],
        "user_question",
        "user_supplied_price_delta",
        1,
        retrieved_at,
    )
    if refs:
        refs[-1]["unit"] = delta["currency"]
    _append_ref_if_value(
        refs,
        index,
        "User supplied price-delta direction",
        delta["direction"],
        "user_question",
        "user_supplied_price_delta",
        1,
        retrieved_at,
    )
    return {
        "toolName": "user_supplied_price_delta",
        "query": {"country": country, "question": question, "model": model},
        "success": True,
        "rowCount": 1,
        "freshness": "user_supplied",
        "sourceType": "generated",
        "summary": (
            f"{model} relative price delta supplied by the user question; "
            "use as a scenario input, not as official current MSRP."
        ),
        "keyFindings": [
            f"price_delta:{delta['direction']} {delta['amount']} {delta['currency']}",
            "official_current_msrp_still_requires_cross_check",
        ],
        "evidenceRefs": refs,
    }


def _tool_result_to_evidence(
    item: dict[str, Any],
    *,
    index: int,
    retrieved_at: str,
    intent: str,
    question: str,
    requested_country: str,
) -> ToolEvidence:
    result = item.get("result") if isinstance(item.get("result"), dict) else {}
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    raw_data = result.get("data") if isinstance(result.get("data"), dict) else {}
    tool_name = str(item.get("toolName") or result.get("tool") or "").strip()
    source = str(metadata.get("source") or result.get("source") or "").strip()
    data = _filter_external_tool_data(raw_data, tool_name=tool_name, intent=intent, question=question)
    query = item.get("query") if isinstance(item.get("query"), dict) else {}
    scope_mismatch = _country_scope_mismatch(
        tool_name=tool_name,
        query=query,
        metadata=metadata,
        data=data,
        requested_country=requested_country,
    )
    success = item.get("success") is not False and not str(item.get("error") or "").strip()
    if scope_mismatch:
        success = False
    row_count = _row_count(data, metadata)
    refs: list[EvidenceRef] = []
    if not scope_mismatch:
        refs = _extract_evidence_refs(
            tool_name=tool_name,
            source=source,
            data=data,
            metadata=metadata,
            row_count=row_count,
            retrieved_at=retrieved_at,
            index=index,
            question=question,
        )
        refs = _filter_external_evidence_refs(refs, tool_name=tool_name)
        refs = _annotate_market_metric_scopes(refs, data=data)
    coverage_diagnostics = (
        data.get("coverageDiagnostics")
        if isinstance(data.get("coverageDiagnostics"), dict)
        else {}
    )
    if scope_mismatch:
        coverage_diagnostics = {
            **coverage_diagnostics,
            "diagnosis": "country_scope_mismatch",
            "requestedCountry": scope_mismatch["requestedCountry"],
            "returnedCountry": scope_mismatch["returnedCountry"],
        }
    key_findings = _key_findings(data, refs)
    if coverage_diagnostics:
        key_findings.extend(_coverage_diagnostic_findings(coverage_diagnostics))
    if scope_mismatch:
        key_findings.insert(
            0,
            f"country_scope_mismatch:{scope_mismatch['returnedCountry']}!=requested:{scope_mismatch['requestedCountry']}",
        )
    evidence: ToolEvidence = {
        "toolName": tool_name,
        "query": query,
        "success": bool(success),
        "rowCount": row_count,
        "freshness": _freshness(data, metadata, retrieved_at),
        "sourceType": _source_type(tool_name, source),
        "summary": _summary(tool_name, source, data, row_count),
        "keyFindings": key_findings[:6],
        "evidenceRefs": refs,
    }
    if coverage_diagnostics:
        evidence["coverageDiagnostics"] = dict(coverage_diagnostics)
    error = str(item.get("error") or "").strip()
    if scope_mismatch:
        error = (
            f"{tool_name or 'tool'} returned {scope_mismatch['returnedCountry']} evidence "
            f"for requested country {scope_mismatch['requestedCountry']}."
        )
    if error:
        evidence["error"] = error[:300]
    return evidence


def _country_scope_mismatch(
    *,
    tool_name: str,
    query: dict[str, Any],
    metadata: dict[str, Any],
    data: dict[str, Any],
    requested_country: str,
) -> dict[str, str]:
    if tool_name not in _COUNTRY_SCOPED_TOOLS:
        return {}
    requested = _canonical_country(requested_country)
    if not requested:
        return {}
    returned = _tool_result_country(query=query, metadata=metadata, data=data)
    if not returned or returned == requested:
        return {}
    return {"requestedCountry": requested, "returnedCountry": returned}


_COUNTRY_SCOPED_TOOLS: set[str] = {
    "query_country_snapshot",
    "build_market_chart",
    "query_segment_breakdown",
    "query_with_filters",
    "query_time_series",
    "query_msrp_pricing",
    "query_leasing_offers",
    "query_price_positioning",
    "compare_competitive_set",
    "compare_vehicle_variants",
    "analyze_market_dynamics",
    "analyze_model_performance",
}


def _tool_result_country(*, query: dict[str, Any], metadata: dict[str, Any], data: dict[str, Any]) -> str:
    for value in (
        query.get("country"),
        metadata.get("country"),
        data.get("country"),
        data.get("market"),
    ):
        country = _canonical_country(str(value or ""))
        if country:
            return country
    extracted = data.get("extractedParams") if isinstance(data.get("extractedParams"), dict) else {}
    for value in (extracted.get("country"), extracted.get("market")):
        country = _canonical_country(str(value or ""))
        if country:
            return country
    context = data.get("contextSnapshot") if isinstance(data.get("contextSnapshot"), dict) else {}
    for value in (context.get("country"), context.get("market")):
        country = _canonical_country(str(value or ""))
        if country:
            return country
    return ""


def _filter_external_tool_data(data: dict[str, Any], *, tool_name: str, intent: str, question: str) -> dict[str, Any]:
    if tool_name not in {"external_research", "search_market_news", "pageindex_search_documents", "minirag_query_graph"}:
        return data
    filtered = dict(data)
    total_before = 0
    total_after = 0
    for list_key in ("items", "citations", "documents", "sections"):
        values = data.get(list_key)
        if not isinstance(values, list):
            continue
        rows = [row for row in values if isinstance(row, dict)]
        kept, rejected = filter_relevant_research_sources(rows, intent=intent, question=question)
        hard_kept: list[dict[str, Any]] = []
        hard_rejected: list[dict[str, Any]] = []
        for row in kept:
            if _external_row_is_obviously_irrelevant(row):
                hard_rejected.append({
                    "title": row.get("title", ""),
                    "url": row.get("url", ""),
                    "source": row.get("source", ""),
                    "reason": "obvious_irrelevant_external_row",
                })
            else:
                hard_kept.append(row)
        kept = hard_kept
        rejected = [*rejected, *hard_rejected]
        total_before += len(rows)
        total_after += len(kept)
        filtered[list_key] = kept
        if rejected:
            existing_rejected = filtered.get("rejectedSources") if isinstance(filtered.get("rejectedSources"), list) else []
            filtered["rejectedSources"] = [*existing_rejected, *rejected][:12]
    if total_before:
        diagnostics = filtered.get("coverageDiagnostics") if isinstance(filtered.get("coverageDiagnostics"), dict) else {}
        filtered["coverageDiagnostics"] = {
            **diagnostics,
            "externalRowsReturned": total_before,
            "externalRowsKept": total_after,
            "externalRowsFiltered": max(0, total_before - total_after),
        }
        if total_after != total_before:
            filtered["summary"] = (
                f"{tool_name} kept {total_after}/{total_before} question-relevant external rows; "
                f"filtered {max(0, total_before - total_after)} low-relevance rows."
            )
    return filtered


def _external_row_is_obviously_irrelevant(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key) or "")
        for key in (
            "label",
            "name",
            "title",
            "sourceTitle",
            "snippet",
            "summary",
            "content",
            "claim",
            "url",
            "source",
        )
    ).casefold()
    if not any(token in text for token in ("fifa", "world cup", "tunisia", "football", "soccer", "goal", "fox sports")):
        return False
    business_text = re.sub(r"\bprivacy\s+policy\b", "", text)
    business_text = re.sub(r"\bterms\s+of\s+use\b", "", business_text)
    strong_auto_pattern = (
        r"\b(bev|phev|hev|ev|car|cars|vehicle|vehicles|automotive|electric|hybrid|subsidy|tax|policy|"
        r"emission|emissions|co2|market|pricing|owner|owners|configuration|config|winter|tyre|tire|"
        r"towing|roof|omoda|jaecoo)\b"
    )
    return re.search(strong_auto_pattern, business_text) is None and not any(
        token in text
        for token in ("汽车", "车辆", "电动车", "纯电", "插混", "混动", "补贴", "政策", "税费", "配置", "冬季胎", "拖车")
    )


def _filter_external_evidence_refs(refs: list[EvidenceRef], *, tool_name: str) -> list[EvidenceRef]:
    if tool_name not in {"external_research", "search_market_news", "pageindex_search_documents", "minirag_query_graph"}:
        return refs
    return [ref for ref in refs if not _external_ref_is_obviously_irrelevant(ref)]


def _annotate_market_metric_scopes(
    refs: list[EvidenceRef],
    *,
    data: dict[str, Any],
) -> list[EvidenceRef]:
    snapshot = data.get("contextSnapshot") if isinstance(data.get("contextSnapshot"), dict) else {}
    metric_scopes = snapshot.get("metricScopes") if isinstance(snapshot.get("metricScopes"), dict) else {}
    if not metric_scopes:
        return refs
    for ref in refs:
        label = str(ref.get("label") or "")
        scope_key = _market_metric_scope_key(label)
        scope = metric_scopes.get(scope_key) if scope_key else None
        if not isinstance(scope, dict):
            continue
        ref["scopeKey"] = scope_key
        for key in ("periodType", "periodLabel", "periodStart", "periodEnd"):
            value = str(scope.get(key) or "").strip()
            if value:
                ref[key] = value  # type: ignore[literal-required]
    return refs


def _market_metric_scope_key(label: str) -> str:
    text = str(label or "").casefold()
    if ".crosstabs." in text:
        return "crossTabs"
    for key in ("powertrainMix", "topModels", "topBrands"):
        if f".{key.casefold()}." in text:
            return key
    return ""


def _evidence_scope_diagnostics(evidences: list[ToolEvidence]) -> dict[str, Any]:
    grouped: dict[str, list[EvidenceRef]] = {}
    for evidence in evidences:
        if not evidence.get("success"):
            continue
        for ref in evidence.get("evidenceRefs", []):
            metric = _canonical_scoped_metric(ref)
            if metric:
                grouped.setdefault(metric, []).append(ref)

    parallel_scopes: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for metric, refs in grouped.items():
        scoped = [ref for ref in refs if _ref_scope_identity(ref) is not None]
        if len(scoped) < 2:
            continue
        by_scope: dict[tuple[str, str, str], list[EvidenceRef]] = {}
        for ref in scoped:
            scope_identity = _ref_scope_identity(ref)
            if scope_identity is not None:
                by_scope.setdefault(scope_identity, []).append(ref)
        if len(by_scope) > 1:
            parallel_scopes.append({
                "metric": metric,
                "scopes": [
                    {
                        "periodType": scope[0],
                        "periodStart": scope[1],
                        "periodEnd": scope[2],
                        "periodLabel": str(scope_refs[0].get("periodLabel") or ""),
                        "values": _distinct_scope_values(scope_refs),
                        "refIds": [str(ref.get("refId") or "") for ref in scope_refs],
                    }
                    for scope, scope_refs in by_scope.items()
                ],
            })
        for scope, scope_refs in by_scope.items():
            values = _distinct_scope_values(scope_refs)
            if len(values) <= 1:
                continue
            conflicts.append({
                "metric": metric,
                "periodType": scope[0],
                "periodStart": scope[1],
                "periodEnd": scope[2],
                "periodLabel": str(scope_refs[0].get("periodLabel") or ""),
                "values": values,
                "refIds": [str(ref.get("refId") or "") for ref in scope_refs],
            })
    return {
        "parallelScopes": parallel_scopes,
        "conflicts": conflicts,
        "hasBlockingConflict": bool(conflicts),
    }


def _canonical_scoped_metric(ref: EvidenceRef) -> str:
    label = str(ref.get("label") or "")
    patterns = (
        r"contextSnapshot\.powertrainMix\.([^.]+)\.(?:sales|value)$",
        r"contextSnapshot\.crossTabs\.(?:driveByFuel|registrationByFuel)\.([^.]+)\.sales$",
    )
    for pattern in patterns:
        match = re.match(pattern, label, flags=re.IGNORECASE)
        if match:
            return f"powertrain:{match.group(1).upper()}:sales"
    return ""


def _ref_scope_identity(ref: EvidenceRef) -> tuple[str, str, str] | None:
    period_type = str(ref.get("periodType") or "").strip()
    period_start = str(ref.get("periodStart") or "").strip()
    period_end = str(ref.get("periodEnd") or "").strip()
    if not period_type or period_type == "unknown" or not period_start or not period_end:
        return None
    return period_type, period_start, period_end


def _distinct_scope_values(refs: list[EvidenceRef]) -> list[str | int | float]:
    values: list[str | int | float] = []
    seen: set[str] = set()
    for ref in refs:
        value = ref.get("value")
        if isinstance(value, bool) or value is None:
            continue
        key = (
            f"number:{float(value):.12g}"
            if isinstance(value, (int, float))
            else str(value).strip().casefold()
        )
        if not key or key in seen:
            continue
        seen.add(key)
        values.append(value)
    return values


def _external_ref_is_obviously_irrelevant(ref: EvidenceRef) -> bool:
    if str(ref.get("label") or "") == "row_count":
        return False
    return _external_row_is_obviously_irrelevant({
        "label": ref.get("label", ""),
        "claim": ref.get("value", ""),
        "url": ref.get("source", ""),
        "source": ref.get("table", ""),
    })


def _extract_evidence_refs(
    *,
    tool_name: str,
    source: str,
    data: dict[str, Any],
    metadata: dict[str, Any],
    row_count: int,
    retrieved_at: str,
    index: int,
    question: str = "",
) -> list[EvidenceRef]:
    refs: list[EvidenceRef] = []

    if row_count > 0 and tool_name != "query_time_series":
        refs.append(_ref(index, len(refs), "row_count", row_count, "", source, _table_name(tool_name, source), row_count, retrieved_at))

    _append_cross_country_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=data,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )
    if len(refs) >= 12:
        return refs[:12]

    _append_segment_breakdown_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=data,
        question=question,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )
    if len(refs) >= 12:
        return refs[:12]

    kpis = data.get("kpis")
    if isinstance(kpis, dict):
        for key, value in kpis.items():
            _append_ref_if_value(refs, index, str(key), value, source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return refs

    price_stats = data.get("priceStats")
    if isinstance(price_stats, dict):
        for key, value in price_stats.items():
            _append_ref_if_value(refs, index, f"priceStats.{key}", value, source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return refs

    _append_top_level_trend_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=data,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )
    if len(refs) >= 12:
        return refs[:12]

    _append_competitive_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=data,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )
    if len(refs) >= 12:
        return refs[:12]

    _append_variant_diff_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=data,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )
    if len(refs) >= 12:
        return refs[:12]

    _append_chart_snapshot_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=data,
        question=question,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )
    if len(refs) >= 12:
        return refs[:12]

    _append_cross_reference_finding_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=data,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )
    if len(refs) >= 12:
        return refs[:12]

    _append_nested_market_snapshot_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=data,
        question=question,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )
    if len(refs) >= 12:
        return refs[:12]

    _append_filtered_query_result_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=data,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )
    if len(refs) >= 12:
        return refs[:12]

    for list_key in ("topModels", "topBrands", "powertrainMix", "items", "priceRecords", "sections", "documents", "citations"):
        values = data.get(list_key)
        if not isinstance(values, list):
            continue
        for row_index, row in enumerate(values[:6]):
            if not isinstance(row, dict):
                continue
            label = str(row.get("label") or row.get("model") or row.get("modelName") or row.get("brand") or row.get("title") or row.get("name") or f"{list_key}_{row_index + 1}")
            source_value = row.get("url") or row.get("sourceUrl") or row.get("source")
            row_source = str(source_value or source).strip() or source
            if list_key in {"items", "citations", "sections", "documents"} and source_value:
                _append_ref_if_value(refs, index, f"{label}.source", source_value, row_source, tool_name, row_count, retrieved_at)
            supported_claim = _external_row_supported_claim(row)
            if list_key in {"items", "citations"} and supported_claim:
                _append_ref_if_value(refs, index, f"{label}.claim", supported_claim, row_source, tool_name, row_count, retrieved_at)
            published_at = row.get("publishedAt") or row.get("published_at") or row.get("date")
            if list_key in {"items", "citations", "sections", "documents"} and published_at:
                _append_ref_if_value(refs, index, f"{label}.date", published_at, row_source, tool_name, row_count, retrieved_at)
            if "value" in row:
                _append_ref_if_value(refs, index, label, row.get("value"), source, tool_name, row_count, retrieved_at)
            for key, value in row.items():
                key_lower = str(key).lower()
                if any(token in key_lower for token in (
                    "price",
                    "msrp",
                    "sales",
                    "share",
                    "rank",
                    "volume",
                    "count",
                    "range",
                    "battery",
                    "monthly",
                    "residual",
                    "term",
                    "mileage",
                    "contractcost",
                    "capcost",
                    "apr",
                )):
                    _append_ref_if_value(refs, index, f"{label}.{key}", value, source, tool_name, row_count, retrieved_at)
                if len(refs) >= 12:
                    return refs

    chart_specs = data.get("chartSpecs")
    if isinstance(chart_specs, dict):
        chart_count = chart_specs.get("chartCount")
        _append_ref_if_value(refs, index, "chart_count", chart_count, source, tool_name, row_count, retrieved_at)

    result_count = metadata.get("resultCount") or metadata.get("chartCount") or metadata.get("entityCount")
    _append_ref_if_value(refs, index, "metadata.result_count", result_count, source, tool_name, row_count, retrieved_at)
    return refs[:12]


def _append_top_level_trend_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    data: dict[str, Any],
    row_count: int,
    retrieved_at: str,
) -> None:
    for list_key in ("yearSeries", "monthSeries"):
        values = data.get(list_key)
        if not isinstance(values, list):
            continue
        _append_trend_series_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            values=values,
            prefix=list_key,
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        if len(refs) >= 12:
            return


def _append_trend_series_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    values: list[Any],
    prefix: str,
    row_count: int,
    retrieved_at: str,
) -> None:
    selected_values = values[-12:] if "monthseries" in prefix.lower() else values[-6:]
    for row_index, row in enumerate(selected_values):
        if not isinstance(row, dict):
            continue
        label = str(
            row.get("month")
            or row.get("year")
            or row.get("time")
            or row.get("period")
            or row.get("date")
            or row.get("label")
            or f"{prefix}_{row_index + 1}"
        )
        for key, value in row.items():
            key_lower = str(key).lower()
            if key_lower in {"label", "month", "year", "time", "period", "date", "name"}:
                continue
            if any(
                token in key_lower
                for token in (
                    "sales",
                    "share",
                    "volume",
                    "count",
                    "value",
                    "mix",
                    "penetration",
                    "registration",
                )
            ):
                _append_ref_if_value(refs, index, f"{prefix}.{label}.{key}", value, source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return


def _append_cross_country_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    data: dict[str, Any],
    row_count: int,
    retrieved_at: str,
) -> None:
    comparison = data.get("comparison")
    if not isinstance(comparison, dict):
        return
    comparison_items = [
        (str(country_name or "").strip(), country_data)
        for country_name, country_data in comparison.items()
        if str(country_name or "").strip() and isinstance(country_data, dict) and not country_data.get("error")
    ]
    if len(comparison_items) > 2:
        _append_compact_cross_country_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            comparison_items=comparison_items,
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        return
    for country_name, country_data in comparison_items:
        if len(refs) >= 12:
            return
        country_label = country_name
        country_start_count = len(refs)
        kpis = country_data.get("kpis") if isinstance(country_data.get("kpis"), dict) else {}
        kpi_keys = _prioritized_keys(
            kpis,
            ("totalSales", "cumulativeSales", "totalVolume", "sales", "bevShare", "marketShare"),
            max_items=4,
        )
        for key in kpi_keys:
            value = kpis.get(key)
            _append_ref_if_value(refs, index, f"crossCountry.{country_label}.kpis.{key}", value, source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return
            if len(refs) - country_start_count >= 6:
                break
        for list_key in ("powertrainMix", "topModels"):
            if len(refs) - country_start_count >= 6:
                break
            values = country_data.get(list_key)
            if not isinstance(values, list):
                continue
            for row_index, row in enumerate(values[:4]):
                if len(refs) - country_start_count >= 6:
                    break
                if not isinstance(row, dict):
                    continue
                label = str(row.get("label") or row.get("model") or row.get("brand") or row.get("name") or f"{list_key}_{row_index + 1}").strip()
                if not label:
                    continue
                for key, value in row.items():
                    if len(refs) - country_start_count >= 6:
                        break
                    key_lower = str(key).lower()
                    if key_lower in {"label", "model", "brand", "name"}:
                        continue
                    if any(token in key_lower for token in ("sales", "share", "volume", "count", "rank", "value", "mix")):
                        _append_ref_if_value(refs, index, f"crossCountry.{country_label}.{list_key}.{label}.{key}", value, source, tool_name, row_count, retrieved_at)
                    if len(refs) >= 12:
                        return


def _append_compact_cross_country_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    comparison_items: list[tuple[str, dict[str, Any]]],
    row_count: int,
    retrieved_at: str,
) -> None:
    for metric in ("sales", "share"):
        for fuel in ("BEV", "HEV"):
            for country_label, country_data in comparison_items:
                if len(refs) >= 12:
                    return
                row = _cross_country_powertrain_row(country_data, fuel)
                value = _cross_country_powertrain_value(row, metric) if row else None
                _append_ref_if_value(
                    refs,
                    index,
                    f"crossCountry.{country_label}.powertrainMix.{fuel}.{metric}",
                    value,
                    source,
                    tool_name,
                    row_count,
                    retrieved_at,
                )
    for country_label, country_data in comparison_items:
        if len(refs) >= 12:
            return
        row = _cross_country_powertrain_row(country_data, "PHEV")
        value = _cross_country_powertrain_value(row, "sales") if row else None
        _append_ref_if_value(
            refs,
            index,
            f"crossCountry.{country_label}.powertrainMix.PHEV.sales",
            value,
            source,
            tool_name,
            row_count,
            retrieved_at,
        )


def _cross_country_powertrain_row(country_data: dict[str, Any], fuel: str) -> dict[str, Any] | None:
    values = country_data.get("powertrainMix")
    if not isinstance(values, list):
        return None
    fuel_folded = fuel.casefold()
    for row in values:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label") or row.get("powertrain") or row.get("name") or "").casefold()
        if label == fuel_folded:
            return row
    return None


def _cross_country_powertrain_value(row: dict[str, Any], metric: str) -> Any:
    if metric == "share":
        return row.get("share") or row.get("mix") or row.get("penetration")
    return row.get("sales") or row.get("value") or row.get("volume") or row.get("registrations") or row.get("count")


def _prioritized_keys(values: dict[str, Any], preferred: tuple[str, ...], *, max_items: int) -> list[str]:
    result: list[str] = []
    for key in preferred:
        if key in values and key not in result:
            result.append(key)
    for key in values:
        if key in result:
            continue
        result.append(str(key))
        if len(result) >= max_items:
            break
    return result[:max_items]


def _append_competitive_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    data: dict[str, Any],
    row_count: int,
    retrieved_at: str,
) -> None:
    competitors = data.get("competitors")
    if isinstance(competitors, list):
        competitor_rows = [
            row
            for row in competitors[:6]
            if isinstance(row, dict)
            and str(row.get("model") or row.get("name") or "").strip()
        ]
        core_keys = (
            "sales",
            "avgPrice",
            "minPrice",
            "maxPrice",
            "priceRecords",
            "segment",
            "powertrain",
        )
        status_keys = (
            "priceEvidenceStatus",
            "reviewPendingRows",
            "currentPriceRows",
            "candidateDomain",
            "sourceDraftPath",
            "priceEvidenceRole",
            "candidateSourceType",
            "materializationStatus",
            "materializationReadinessScore",
        )
        for row_index, row in enumerate(competitor_rows):
            model = str(row.get("model") or row.get("name") or f"competitor_{row_index + 1}").strip()
            _append_ref_if_value(refs, index, f"competitor.{row_index + 1}.model", model, source, tool_name, row_count, retrieved_at)
            for key in core_keys:
                if key in row:
                    _append_ref_if_value(refs, index, f"{model}.{key}", row.get(key), source, tool_name, row_count, retrieved_at)
        # Balance source-status refs across requested competitors so the first model does not
        # consume the whole evidence budget before later competitors are represented.
        for key in status_keys:
            for row in competitor_rows:
                model = str(row.get("model") or row.get("name") or "").strip()
                if key in row:
                    _append_ref_if_value(refs, index, f"{model}.{key}", row.get(key), source, tool_name, row_count, retrieved_at)
                if len(refs) >= 24:
                    return
        return
    analysis = data.get("analysis")
    if isinstance(analysis, dict):
        for key in ("totalCompared", "sourceCount"):
            _append_ref_if_value(refs, index, f"competitive_analysis.{key}", analysis.get(key), source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return
    target_model = data.get("targetModel")
    _append_ref_if_value(refs, index, "target_model", target_model, source, tool_name, row_count, retrieved_at)


def _append_variant_diff_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    data: dict[str, Any],
    row_count: int,
    retrieved_at: str,
) -> None:
    subjects = data.get("subjects")
    if isinstance(subjects, list):
        for subject_index, subject in enumerate(subjects[:4]):
            if not isinstance(subject, dict):
                continue
            label = str(subject.get("model") or subject.get("name") or subject.get("version") or f"subject_{subject_index + 1}").strip()
            _append_ref_if_value(refs, index, f"variant_subject.{subject_index + 1}", label, source, tool_name, row_count, retrieved_at)
            for key in ("brand", "model", "version", "powertrain", "trim"):
                if key in subject:
                    _append_ref_if_value(refs, index, f"{label}.{key}", subject.get(key), source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return
    for list_key, prefix in (
        ("differentFeatures", "configuration_delta"),
        ("commonFeatures", "common_feature"),
        ("selectionNotes", "selection_note"),
    ):
        values = data.get(list_key)
        if not isinstance(values, list):
            continue
        for row_index, row in enumerate(values[:6]):
            if isinstance(row, dict):
                label = str(row.get("feature") or row.get("name") or row.get("label") or f"{prefix}_{row_index + 1}").strip()
                value = row.get("delta") or row.get("value") or row.get("customerValue") or row.get("description") or row.get("note")
                _append_ref_if_value(refs, index, f"{prefix}.{label}", value or label, source, tool_name, row_count, retrieved_at)
                for key in ("targetValue", "competitorValue", "gap", "priority"):
                    if key in row:
                        _append_ref_if_value(refs, index, f"{label}.{key}", row.get(key), source, tool_name, row_count, retrieved_at)
            else:
                _append_ref_if_value(refs, index, f"{prefix}.{row_index + 1}", row, source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return


def _append_cross_reference_finding_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    data: dict[str, Any],
    row_count: int,
    retrieved_at: str,
) -> None:
    findings = data.get("findings")
    if not isinstance(findings, dict):
        return
    sales = findings.get("sales") if isinstance(findings.get("sales"), dict) else {}
    rankings = sales.get("rankings") if isinstance(sales.get("rankings"), list) else []
    for row_index, row in enumerate(rankings[:5]):
        if not isinstance(row, dict):
            continue
        label = str(row.get("model") or row.get("label") or row.get("name") or f"ranking_{row_index + 1}").strip()
        if not label:
            continue
        for key in ("value", "sales", "share", "rank", "volume", "count"):
            if key in row:
                _append_ref_if_value(refs, index, f"sales.rankings.{label}.{key}", row.get(key), source, tool_name, row_count, retrieved_at)
        if len(refs) >= 12:
            return

    pricing = findings.get("pricing") if isinstance(findings.get("pricing"), dict) else {}
    records = pricing.get("records") if isinstance(pricing.get("records"), list) else []
    for row_index, row in enumerate(records[:5]):
        if not isinstance(row, dict):
            continue
        label = str(row.get("model") or row.get("label") or row.get("version") or f"pricing_{row_index + 1}").strip()
        if not label:
            continue
        for key in ("msrp", "price", "avgPrice", "minPrice", "maxPrice", "currency", "powertrain"):
            if key in row:
                _append_ref_if_value(refs, index, f"pricing.records.{label}.{key}", row.get(key), source, tool_name, row_count, retrieved_at)
        if len(refs) >= 12:
            return

    variants = findings.get("variants") if isinstance(findings.get("variants"), dict) else {}
    nested_variant_data = {
        "subjects": variants.get("subjects"),
        "differentFeatures": variants.get("diffFeatures") or variants.get("differentFeatures"),
        "commonFeatures": variants.get("commonFeatures"),
        "selectionNotes": variants.get("selectionNotes"),
    }
    _append_variant_diff_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        data=nested_variant_data,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )


def _append_nested_market_snapshot_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    data: dict[str, Any],
    question: str = "",
    row_count: int,
    retrieved_at: str,
) -> None:
    dynamics = data.get("dynamics") if isinstance(data.get("dynamics"), dict) else {}
    snapshot = dynamics.get("marketSnapshot")
    if not isinstance(snapshot, dict):
        snapshot = data.get("marketSnapshot")
    if not isinstance(snapshot, dict):
        return
    kpis = snapshot.get("kpis")
    if isinstance(kpis, dict):
        for key, value in kpis.items():
            _append_ref_if_value(refs, index, f"marketSnapshot.kpis.{key}", value, source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return
    for list_key in ("topModels", "topBrands", "powertrainMix", "yearSeries", "monthSeries"):
        values = snapshot.get(list_key)
        if not isinstance(values, list):
            continue
        preferred = _market_requested_signals(data, question=question) if list_key == "powertrainMix" else set()
        rows = _prioritized_snapshot_rows(values, preferred_signals=preferred)
        for row_index, row in enumerate(rows[:6]):
            if not isinstance(row, dict):
                continue
            label = str(row.get("label") or row.get("model") or row.get("brand") or row.get("year") or row.get("month") or f"{list_key}_{row_index + 1}")
            for key, value in row.items():
                key_lower = str(key).lower()
                if key_lower in {"label", "model", "brand", "name", "year", "month"}:
                    continue
                if any(token in key_lower for token in ("sales", "share", "volume", "count", "rank", "value", "mix")):
                    _append_ref_if_value(refs, index, f"marketSnapshot.{list_key}.{label}.{key}", value, source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return


def _append_filtered_query_result_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    data: dict[str, Any],
    row_count: int,
    retrieved_at: str,
) -> None:
    """Extract refs from query_with_filters-style nested `results` payloads."""
    results = data.get("results")
    if not isinstance(results, dict):
        return
    kpis = results.get("kpis")
    if isinstance(kpis, dict):
        for key, value in kpis.items():
            _append_ref_if_value(refs, index, f"results.kpis.{key}", value, source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return
    for list_key in ("topModels", "topBrands", "powertrainMix", "yearSeries", "monthSeries"):
        values = results.get(list_key)
        if not isinstance(values, list):
            continue
        for row_index, row in enumerate(values[:6]):
            if not isinstance(row, dict):
                continue
            label = str(
                row.get("label")
                or row.get("model")
                or row.get("brand")
                or row.get("name")
                or row.get("year")
                or row.get("month")
                or f"{list_key}_{row_index + 1}"
            )
            for key, value in row.items():
                key_lower = str(key).lower()
                if key_lower in {"label", "model", "brand", "name", "year", "month"}:
                    continue
                if any(token in key_lower for token in ("sales", "share", "volume", "count", "rank", "value", "mix")):
                    _append_ref_if_value(refs, index, f"results.{list_key}.{label}.{key}", value, source, tool_name, row_count, retrieved_at)
                if len(refs) >= 12:
                    return


def _append_chart_snapshot_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    data: dict[str, Any],
    question: str = "",
    row_count: int,
    retrieved_at: str,
) -> None:
    snapshot = data.get("contextSnapshot")
    if not isinstance(snapshot, dict):
        return
    cross_tabs = snapshot.get("crossTabs")
    has_cross_tab_rows = _market_cross_tabs_have_rows(cross_tabs)
    large_suv_competitor_first = _is_large_suv_competitor_context_question(question)
    channel_first = _is_channel_context_question(question)
    segment_first = _is_segment_structure_question(question)
    powertrain_pricing_first = _is_powertrain_pricing_context_question(question)

    if large_suv_competitor_first:
        large_suv_ref_count = len(refs)
        _append_large_suv_panel_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            snapshot=snapshot,
            question=question,
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        if len(refs) >= 12:
            return
        _append_large_suv_context_cross_tab_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            cross_tabs=cross_tabs,
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        if len(refs) >= 12:
            return
        if len(refs) > large_suv_ref_count:
            return
        for list_keys, preferred, row_limit in (
            (("driveBySegment",), {"SUV B"}, 1),
            (("registrationByFuel",), {"PHEV"}, 1),
            (("segmentByFuel",), {"SUV B", "SUV A"}, 2),
            (("registrationBySegment",), {"SUV B"}, 1),
        ):
            _append_market_cross_tab_refs(
                refs,
                index=index,
                source=source,
                tool_name=tool_name,
                cross_tabs=cross_tabs,
                prefix="contextSnapshot.crossTabs",
                preferred_signals=preferred,
                list_keys=list_keys,
                row_limit_override=row_limit,
                row_count=row_count,
                retrieved_at=retrieved_at,
            )
            if len(refs) >= 12:
                return

    if channel_first:
        _append_market_cross_tab_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            cross_tabs=cross_tabs,
            prefix="contextSnapshot.crossTabs",
            preferred_signals=_market_cross_tab_preferred_signals(data),
            list_keys=("registrationByFuel", "registrationBySegment", "driveByFuel", "driveBySegment", "segmentByFuel"),
            row_limit_override=2,
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        if len(refs) >= 12:
            return

    if segment_first:
        _append_market_cross_tab_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            cross_tabs=cross_tabs,
            prefix="contextSnapshot.crossTabs",
            preferred_signals={"SUV A0", "SUV A", *_market_cross_tab_preferred_signals(data)},
            list_keys=("driveBySegment", "segmentByFuel", "registrationBySegment", "fuelBySegment"),
            row_limit_override=2,
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        if len(refs) >= 12:
            return

    if powertrain_pricing_first:
        _append_powertrain_pricing_cross_tab_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            cross_tabs=cross_tabs,
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        if len(refs) >= 12:
            return

    for list_key in ("powertrainMix",):
        values = snapshot.get(list_key)
        if not isinstance(values, list):
            continue
        _append_snapshot_list_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            list_key=list_key,
            values=values,
            preferred_signals=_market_requested_signals(data, question=question),
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        if len(refs) >= 12:
            return

    for list_key in ("yearSeries", "monthSeries"):
        values = snapshot.get(list_key)
        if not isinstance(values, list):
            continue
        _append_trend_series_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            values=values,
            prefix=f"contextSnapshot.{list_key}",
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        if len(refs) >= 12:
            return

    for list_key in ("topModels", "topBrands"):
        values = snapshot.get(list_key)
        if not isinstance(values, list):
            continue
        _append_snapshot_list_refs(
            refs,
            index=index,
            source=source,
            tool_name=tool_name,
            list_key=list_key,
            values=values,
            row_count=row_count,
            retrieved_at=retrieved_at,
        )
        if len(refs) >= 12:
            return

    kpis = snapshot.get("kpis")
    if isinstance(kpis, dict):
        for key, value in kpis.items():
            if has_cross_tab_rows and _is_zero_technical_kpi(key, value):
                continue
            _append_ref_if_value(refs, index, f"contextSnapshot.kpis.{key}", value, source, tool_name, row_count, retrieved_at)
            if len(refs) >= 12:
                return
    _append_market_cross_tab_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        cross_tabs=cross_tabs,
        prefix="contextSnapshot.crossTabs",
        preferred_signals=_market_cross_tab_preferred_signals(data),
        list_keys=("driveByFuel", "driveBySegment", "segmentByFuel", "registrationBySegment")
        if not channel_first
        else ("driveBySegment", "segmentByFuel"),
        row_count=row_count,
        retrieved_at=retrieved_at,
    )


def _append_large_suv_panel_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    snapshot: dict[str, Any],
    question: str,
    row_count: int,
    retrieved_at: str,
) -> None:
    tokens = _large_suv_requested_model_tokens(question)
    if not tokens:
        return
    for section_key in ("suvB", "suvA"):
        panel = snapshot.get(section_key)
        if not isinstance(panel, dict):
            continue
        section_segment = _large_suv_panel_segment(section_key, panel)
        matched = False
        for row in _large_suv_panel_rows(panel):
            if not isinstance(row, dict):
                continue
            model = str(row.get("model") or row.get("label") or row.get("name") or "").strip()
            if not model or not any(_large_suv_model_matches(token, model) for token in tokens):
                continue
            matched = True
            sales = row.get("volume", row.get("sales", row.get("value")))
            _append_ref_if_value(refs, index, f"{model}.sales", sales, source, tool_name, row_count, retrieved_at)
            _append_ref_if_value(
                refs,
                index,
                f"{model}.segment",
                row.get("segment") or section_segment,
                source,
                tool_name,
                row_count,
                retrieved_at,
            )
            powertrain = _dominant_mix_label(row.get("fuelMix") or row.get("powertrainMix"))
            _append_ref_if_value(refs, index, f"{model}.powertrain", powertrain, source, tool_name, row_count, retrieved_at)
            drive_mix = row.get("driveMix") if isinstance(row.get("driveMix"), dict) else {}
            _append_ref_if_value(refs, index, f"{model}.4WD_sales", drive_mix.get("4WD"), source, tool_name, row_count, retrieved_at)
            registration_mix = row.get("registrationMix") if isinstance(row.get("registrationMix"), dict) else {}
            _append_ref_if_value(
                refs,
                index,
                f"{model}.Business_sales",
                registration_mix.get("Business"),
                source,
                tool_name,
                row_count,
                retrieved_at,
            )
            if len(refs) >= 12:
                return
        if matched:
            return


def _append_large_suv_context_cross_tab_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    cross_tabs: Any,
    row_count: int,
    retrieved_at: str,
) -> None:
    wanted = (
        ("driveBySegment", "SUV B", "_total", "sales"),
        ("driveBySegment", "SUV B", "4WD_pct", "4WD_pct"),
        ("driveByFuel", "PHEV", "4WD_pct", "4WD_pct"),
        ("segmentByFuel", "SUV B", "PHEV_pct", "PHEV_pct"),
        ("registrationByFuel", "PHEV", "Business_pct", "Business_pct"),
    )
    for table, row_name, source_key, metric_name in wanted:
        value = _cross_tab_row_value(cross_tabs, table=table, row_name=row_name, source_key=source_key)
        _append_ref_if_value(
            refs,
            index,
            f"contextSnapshot.crossTabs.{table}.{row_name}.{metric_name}",
            value,
            source,
            tool_name,
            row_count,
            retrieved_at,
        )
        if len(refs) >= 12:
            return


def _large_suv_panel_rows(panel: dict[str, Any]) -> list[Any]:
    ranking = panel.get("totalRanking")
    if isinstance(ranking, dict):
        rows = ranking.get("items")
        if isinstance(rows, list):
            return rows
    if isinstance(ranking, list):
        return ranking
    for key in ("items", "models", "topModels"):
        rows = panel.get(key)
        if isinstance(rows, list):
            return rows
    return []


def _large_suv_requested_model_tokens(question: str) -> list[str]:
    text = _large_suv_match_key(question)
    candidates = (
        "Sorento",
        "Kodiaq",
        "Tayron",
        "XC60",
        "EX60",
        "XC90",
        "EX90",
        "EV9",
        "Model Y",
        "J8",
        "JAECOO 8",
        "JAECOO8",
    )
    return [candidate for candidate in candidates if _large_suv_match_key(candidate) in text]


def _large_suv_model_matches(token: str, model: str) -> bool:
    token_key = _large_suv_match_key(token)
    model_key = _large_suv_match_key(model)
    return bool(token_key and model_key and (token_key == model_key or token_key in model_key or model_key in token_key))


def _large_suv_match_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold()).strip()


def _large_suv_panel_segment(section_key: str, panel: dict[str, Any]) -> str:
    for key in ("segment", "label", "title"):
        value = panel.get(key)
        if isinstance(value, str) and value.strip().upper().startswith("SUV"):
            return value.strip()
    if section_key == "suvB":
        return "SUV B"
    if section_key == "suvA":
        return "SUV A"
    return ""


def _dominant_mix_label(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    numeric_items = [
        (str(key), item)
        for key, item in value.items()
        if isinstance(item, (int, float)) and not isinstance(item, bool) and item > 0
    ]
    if not numeric_items:
        return ""
    numeric_items.sort(key=lambda item: item[1], reverse=True)
    return numeric_items[0][0]


def _append_segment_breakdown_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    data: dict[str, Any],
    question: str = "",
    row_count: int,
    retrieved_at: str,
) -> None:
    if tool_name != "query_segment_breakdown":
        return
    list_keys = ("driveByFuel", "driveBySegment", "segmentByFuel", "registrationByFuel", "registrationBySegment")
    if not any(isinstance(data.get(key), list) and data.get(key) for key in list_keys):
        return
    preferred = _segment_breakdown_preferred_signals(data, question=question)
    _append_market_cross_tab_refs(
        refs,
        index=index,
        source=source,
        tool_name=tool_name,
        cross_tabs=data,
        prefix="contextSnapshot.crossTabs",
        preferred_signals=preferred,
        list_keys=list_keys,
        row_limit_override=2,
        row_count=row_count,
        retrieved_at=retrieved_at,
    )


def _append_powertrain_pricing_cross_tab_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    cross_tabs: Any,
    row_count: int,
    retrieved_at: str,
) -> None:
    wanted = (
        ("driveByFuel", "HEV", "_total", "sales"),
        ("driveByFuel", "HEV", "2WD_pct", "2WD_pct"),
        ("registrationByFuel", "HEV", "Business_pct", "Business_pct"),
        ("registrationByFuel", "HEV", "Private_pct", "Private_pct"),
        ("driveBySegment", "SUV A0", "_total", "sales"),
        ("driveBySegment", "SUV A", "_total", "sales"),
        ("segmentByFuel", "SUV A0", "HEV_pct", "HEV_pct"),
        ("segmentByFuel", "SUV A", "HEV_pct", "HEV_pct"),
    )
    for table, row_name, source_key, metric_name in wanted:
        value = _cross_tab_row_value(cross_tabs, table=table, row_name=row_name, source_key=source_key)
        _append_ref_if_value(
            refs,
            index,
            f"contextSnapshot.crossTabs.{table}.{row_name}.{metric_name}",
            value,
            source,
            tool_name,
            row_count,
            retrieved_at,
        )
        if len(refs) >= 12:
            return


def _cross_tab_row_value(cross_tabs: Any, *, table: str, row_name: str, source_key: str) -> Any:
    if not isinstance(cross_tabs, dict):
        return None
    rows = cross_tabs.get(table)
    if not isinstance(rows, list):
        return None
    target = str(row_name or "").casefold()
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("_index") or row.get("label") or row.get("name") or "").casefold()
        if label == target:
            return row.get(source_key)
    return None


def _append_snapshot_list_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    list_key: str,
    values: list[Any],
    preferred_signals: set[str] | None = None,
    row_count: int,
    retrieved_at: str,
) -> None:
    rows = _prioritized_snapshot_rows(values, preferred_signals=preferred_signals if list_key == "powertrainMix" else None)
    for row_index, row in enumerate(rows[:6]):
        if not isinstance(row, dict):
            continue
        label = str(
            row.get("label")
            or row.get("model")
            or row.get("brand")
            or row.get("powertrain")
            or row.get("name")
            or f"{list_key}_{row_index + 1}"
        )
        for key, value in row.items():
            key_lower = str(key).lower()
            if key_lower in {"label", "model", "brand", "name", "powertrain"}:
                continue
            metric = _snapshot_list_metric_name(list_key, key_lower)
            if not metric:
                continue
            _append_ref_if_value(
                refs,
                index,
                f"contextSnapshot.{list_key}.{label}.{metric}",
                value,
                source,
                tool_name,
                row_count,
                retrieved_at,
            )
            if len(refs) >= 12:
                return


def _snapshot_list_metric_name(list_key: str, key_lower: str) -> str:
    if "share" in key_lower or "mix" in key_lower or "penetration" in key_lower:
        return "share"
    if key_lower in {"value", "sales", "volume", "registrations", "registration"}:
        return "sales"
    if any(token in key_lower for token in ("sales", "volume", "count", "rank")):
        return key_lower
    if str(list_key) in {"topModels", "topBrands", "powertrainMix"} and key_lower == "value":
        return "sales"
    return ""


def _append_market_cross_tab_refs(
    refs: list[EvidenceRef],
    *,
    index: int,
    source: str,
    tool_name: str,
    cross_tabs: Any,
    prefix: str,
    preferred_signals: set[str] | None = None,
    list_keys: tuple[str, ...] | None = None,
    row_limit_override: int | None = None,
    row_count: int,
    retrieved_at: str,
) -> None:
    if not isinstance(cross_tabs, dict):
        return
    preferred = {item.casefold() for item in (preferred_signals or set()) if item}
    for list_key in (list_keys or ("driveByFuel", "driveBySegment", "segmentByFuel", "registrationBySegment")):
        values = cross_tabs.get(list_key)
        if not isinstance(values, list):
            continue
        rows = [row for row in values if isinstance(row, dict)]
        rows.sort(key=lambda row: (0 if str(row.get("_index") or row.get("label") or row.get("name") or "").casefold() in preferred else 1))
        preferred_in_rows = bool(preferred) and any(
            str(row.get("_index") or row.get("label") or row.get("name") or "").casefold() in preferred
            for row in rows
        )
        row_limit = row_limit_override if row_limit_override is not None else 1 if preferred_in_rows else 2
        for row_index, row in enumerate(rows[:row_limit]):
            label = str(row.get("_index") or row.get("label") or row.get("name") or f"{list_key}_{row_index + 1}").strip()
            if not label:
                continue
            total_value = row.get("_total")
            if isinstance(total_value, (int, float)) and not isinstance(total_value, bool):
                _append_ref_if_value(refs, index, f"{prefix}.{list_key}.{label}.sales", total_value, source, tool_name, row_count, retrieved_at)
                if len(refs) >= 12:
                    return
            for key, value in row.items():
                key_text = str(key)
                if key_text in {"_index", "label", "name", "_total"}:
                    continue
                if key_text.endswith("_pct") or key_text.lower() in {"share", "mix", "penetration"}:
                    _append_ref_if_value(refs, index, f"{prefix}.{list_key}.{label}.{key_text}", value, source, tool_name, row_count, retrieved_at)
                if len(refs) >= 12:
                    return


def _market_cross_tabs_have_rows(cross_tabs: Any) -> bool:
    if not isinstance(cross_tabs, dict):
        return False
    for list_key in ("driveByFuel", "driveBySegment", "segmentByFuel", "registrationByFuel", "registrationBySegment"):
        values = cross_tabs.get(list_key)
        if isinstance(values, list) and any(isinstance(row, dict) and row.get("_index") for row in values):
            return True
    return False


def _is_channel_context_question(question: str) -> bool:
    text = str(question or "").casefold()
    if "phev" in text and any(
        token in text
        for token in (
            "co2",
            "co₂",
            "tax",
            "taxation",
            "benefit",
            "emission",
            "emissions",
            "税",
            "税率",
            "税费",
            "排放",
            "阶梯",
        )
    ):
        return True
    return any(
        token in text
        for token in (
            "company car",
            "fleet",
            "business",
            "private",
            "retail",
            "channel",
            "benefit",
            "leasing",
            "公司车",
            "大客户",
            "私人",
            "零售",
            "渠道",
            "月供",
            "残值",
        )
    )


def _is_segment_structure_question(question: str) -> bool:
    text = str(question or "").casefold()
    has_segment = "suv" in text and any(
        token in text
        for token in (
            "a0",
            "a 级",
            "a级",
            "suv a",
            "segment",
            "细分",
            "级别",
        )
    )
    if not has_segment:
        return False
    return any(
        token in text
        for token in (
            "主销",
            "结构",
            "为什么",
            "原因",
            "driver",
            "drivers",
            "集中",
            "机会",
        )
    )


def _is_large_suv_competitor_context_question(question: str) -> bool:
    text = str(question or "").casefold()
    if "j8" in text and "sorento" in text:
        return True
    return any(token in text for token in ("7座", "7 座", "四驱", "4wd", "awd", "sorento")) and any(
        token in text
        for token in (
            "能打",
            "对标",
            "相比",
            "定位",
            "竞品",
            "compare",
            "competitor",
            "versus",
            "vs",
        )
    )


def _is_powertrain_pricing_context_question(question: str) -> bool:
    text = str(question or "").casefold()
    if not any(token in text for token in ("价格", "定价", "便宜", "贵", "价差", "msrp", "price", "pricing", "cheaper", "expensive")):
        return False
    return any(
        token in text
        for token in (
            "hev",
            "phev",
            "bev",
            "混动",
            "插混",
            "纯电",
            "sportage",
            "j7",
            "o5",
            "ev3",
        )
    )


def _is_zero_technical_kpi(key: str, value: Any) -> bool:
    is_zero = value in {0, 0.0} if isinstance(value, (int, float)) and not isinstance(value, bool) else str(value).strip() in {"0", "0.0"}
    if not is_zero:
        return False
    return str(key or "").lower() in {"totalrows", "countrycount", "brandcount", "modelcount", "versioncount"}


def _market_cross_tab_preferred_signals(data: dict[str, Any]) -> set[str]:
    extracted = data.get("extractedParams") if isinstance(data.get("extractedParams"), dict) else {}
    result: set[str] = set()
    for key in ("powertrain", "fuelType", "fuel", "segment"):
        value = extracted.get(key)
        if isinstance(value, str) and value.strip():
            result.add(value.strip())
    return result


def _market_requested_signals(data: dict[str, Any], *, question: str = "") -> set[str]:
    result = set(_market_cross_tab_preferred_signals(data))
    text = str(question or "").casefold()
    for fuel in ("PHEV", "MHEV", "BEV", "HEV", "ICE", "REEV"):
        token = fuel.casefold()
        if re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", text):
            result.add(fuel)
    return result


def _segment_breakdown_preferred_signals(data: dict[str, Any], *, question: str = "") -> set[str]:
    result = set(_market_requested_signals(data, question=question))
    text = str(question or "").casefold()
    applied = data.get("appliedFilters") if isinstance(data.get("appliedFilters"), dict) else {}
    for key in ("segment", "powertrain"):
        value = applied.get(key)
        if isinstance(value, str) and value.strip():
            result.add(value.strip())
    if "j7" in text or "suv" in text or "a0" in text:
        result.update({"SUV A0", "SUV A"})
    if any(token in text for token in ("business", "fleet", "leasing", "company car", "大客户", "公司车")):
        result.add("Business")
    if any(token in text for token in ("private", "retail", "私人", "零售")):
        result.add("Private")
    return result


def _prioritized_snapshot_rows(values: list[Any], *, preferred_signals: set[str] | None = None) -> list[Any]:
    preferred = {str(item or "").strip().casefold() for item in (preferred_signals or set()) if str(item or "").strip()}
    if not preferred:
        return list(values)

    indexed = list(enumerate(values))

    def rank(item: tuple[int, Any]) -> tuple[int, int]:
        original_index, row = item
        if not isinstance(row, dict):
            return 2, original_index
        label = str(row.get("label") or row.get("powertrain") or row.get("name") or row.get("model") or row.get("brand") or "").strip()
        return (0 if label.casefold() in preferred else 1, original_index)

    return [row for _, row in sorted(indexed, key=rank)]


def _append_ref_if_value(
    refs: list[EvidenceRef],
    index: int,
    label: str,
    value: Any,
    source: str,
    tool_name: str,
    row_count: int,
    retrieved_at: str,
) -> None:
    if isinstance(value, bool) or value is None:
        return
    if not isinstance(value, (str, int, float)):
        return
    if isinstance(value, str) and not value.strip():
        return
    refs.append(_ref(index, len(refs), label, value, _unit_for_label(label), source, _table_name(tool_name, source), row_count, retrieved_at))


def _external_row_supported_claim(row: dict[str, Any]) -> str:
    explicit = row.get("supportedClaim") or row.get("claim")
    if isinstance(explicit, str) and explicit.strip():
        return _compact_claim_text(explicit)
    source_value = str(row.get("url") or row.get("source") or "").strip()
    if not source_value:
        return ""
    for key in ("snippet", "summary", "content", "title", "sourceTitle"):
        value = row.get(key)
        if not isinstance(value, str):
            continue
        text = _compact_claim_text(value)
        if _is_usable_external_claim_text(text):
            return text
    return ""


def _compact_claim_text(value: str) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    sentence = re.split(r"(?<=[.!?。！？])\s+", text, maxsplit=1)[0].strip()
    return (sentence or text)[:260]


def _is_usable_external_claim_text(value: str) -> bool:
    text = str(value or "").strip()
    if len(text) < 40 and len(re.findall(r"[\u4e00-\u9fff]", text)) < 12:
        return False
    lower = text.lower()
    if lower in {"n/a", "none", "no data", "empty"}:
        return False
    return True


def _ref(
    tool_index: int,
    ref_index: int,
    label: str,
    value: str | int | float,
    unit: str,
    source: str,
    table: str,
    row_count: int,
    retrieved_at: str,
) -> EvidenceRef:
    ref: EvidenceRef = {
        "refId": f"ev_{tool_index + 1}_{ref_index + 1}",
        "label": label[:120],
        "value": value,
        "source": source or table,
        "table": table,
        "rowCount": row_count,
        "retrievedAt": retrieved_at,
    }
    if unit:
        ref["unit"] = unit
    return ref


def _missing_evidence(
    evidence_plan: dict[str, Any],
    evidences: list[ToolEvidence],
    *,
    question: str = "",
) -> list[MissingEvidence]:
    names: list[tuple[str, str, int]] = []
    for item in evidence_plan.get("evidenceNeeded", []):
        if isinstance(item, dict):
            names.append((str(item.get("name") or ""), str(item.get("reason") or ""), int(item.get("priority") or 99)))
    for name in evidence_plan.get("mustHaveEvidence", []):
        names.append((str(name or ""), f"Intent matrix requires {name}.", 1))

    missing: list[MissingEvidence] = []
    seen: set[str] = set()
    target_price_available = _has_user_supplied_target_price_evidence(evidences)
    target_price_with_corridor = target_price_available and (
        _is_covered("price_corridor", evidences)
        or _is_covered("competitor_price_range", evidences)
    )
    target_delta_available = _has_user_supplied_price_delta_evidence(evidences)
    target_delta_with_context = target_delta_available and (
        _is_covered("competitor_price_range", evidences)
        or _is_covered("competitor_pool", evidences)
        or _is_covered("configuration_delta", evidences)
        or _is_covered("feature_diff", evidences)
    )
    method_price_with_corridor = _has_business_method_price_corridor(evidences)
    competitor_gap_context = _is_covered("price_or_config_gap", evidences)
    successful_tool_names = [
        str(item.get("toolName") or "")
        for item in evidences
        if item.get("success") and str(item.get("toolName") or "").strip()
    ]
    attempted_tool_names = _required_coverage_attempted_tool_names(evidences, successful_tool_names)
    for tool_name in missing_required_tools(evidence_plan, attempted_tool_names):
        missing.append({
            "name": f"missing_required_tool:{tool_name}",
            "reason": f"Intent requires {tool_name}, but the executed tool set did not satisfy that coverage rule.",
            "impact": "blocking",
        })
        seen.add(f"missing_required_tool:{tool_name}")
    target_policy_gap = _missing_target_policy_source_evidence(question, evidence_plan, evidences)
    if target_policy_gap and target_policy_gap["name"] not in seen:
        seen.add(target_policy_gap["name"])
        missing.append(target_policy_gap)
    for name, reason, priority in names:
        if not name or name in seen:
            continue
        seen.add(name)
        if _is_covered(name, evidences):
            continue
        impact: Literal["blocking", "weakens_answer", "optional"] = "blocking" if priority <= 1 else "weakens_answer"
        if name == "current_msrp" and (target_price_available or target_delta_with_context or method_price_with_corridor):
            impact = "weakens_answer"
            reason = (
                "已有目标价、价差假设或价格走廊样本可做初步判断，但仍缺本车型官方 MSRP、版本价差和来源日期交叉验证。"
            )
        if name == "own_model_price" and (target_delta_with_context or method_price_with_corridor):
            impact = "weakens_answer"
            reason = (
                "当前只有用户给定价差/价格走廊上下文，仍缺本车型官方价格，不能把价差判断写成最终定价结论。"
            )
        missing.append({
            "name": name,
            "reason": reason or f"No evidence found for {name}.",
            "impact": impact,
        })
    for evidence in evidences:
        if not evidence.get("success"):
            missing.append({
                "name": f"{evidence.get('toolName', 'tool')}_failed",
                "reason": evidence.get("error", "Tool did not return usable evidence."),
                "impact": "weakens_answer",
            })
    successful = [item for item in evidences if item.get("success")]
    for evidence in successful:
        if str(evidence.get("toolName") or "") != "compare_vehicle_variants":
            continue
        if _has_usable_competitive_or_configuration_evidence(successful):
            continue
        name = "competitive_or_configuration_data_unavailable"
        if name in seen:
            continue
        seen.add(name)
        missing.append({
            "name": name,
            "reason": (
                "compare_vehicle_variants returned no citation-ready subjects, feature deltas, common features, "
                "or selection notes; verify vehicle model mapping and engineering configuration source coverage."
            ),
            "impact": "weakens_answer",
        })
    if successful and not any(item.get("evidenceRefs") for item in successful):
        missing.append({
            "name": "evidence_refs",
            "reason": "Tools completed but returned no citation-ready evidence refs for grounded numeric or competitive conclusions.",
            "impact": "weakens_answer",
        })
    has_external_claim_evidence = _has_external_claim_evidence(successful)
    for evidence in successful:
        tool_name = str(evidence.get("toolName") or "")
        if tool_name not in {
            "external_research",
            "search_market_news",
            "pageindex_search_documents",
            "minirag_query_graph",
            "read_web_page",
        }:
            continue
        if not _requires_external_claim_evidence(evidence_plan, question=question):
            continue
        if evidence.get("evidenceRefs") or has_external_claim_evidence:
            continue
        name = "external_research_claims_unavailable"
        if name in seen:
            continue
        seen.add(name)
        missing.append({
            "name": name,
            "reason": "External research was required or attempted, but it returned no citation-ready source-backed claim evidence.",
            "impact": "weakens_answer",
        })
    for evidence in successful:
        refs = evidence.get("evidenceRefs")
        if not refs or any(not _is_weak_ref(ref) for ref in refs):
            continue
        name, reason = _weak_ref_missing_evidence(evidence)
        if name == "market_snapshot_data_unavailable" and _has_usable_market_evidence(successful):
            continue
        if (
            name in {
                "competitive_or_configuration_data_unavailable",
                "compare_vehicle_variants_weak_evidence_refs",
                "compare_competitive_set_weak_evidence_refs",
                "query_competitive_landscape_weak_evidence_refs",
            }
            and _has_usable_competitive_or_configuration_evidence(successful)
        ):
            continue
        if name in {
            "external_research_claims_unavailable",
            "read_web_page_weak_evidence_refs",
            "search_market_news_weak_evidence_refs",
            "pageindex_search_documents_weak_evidence_refs",
            "minirag_query_graph_weak_evidence_refs",
        }:
            if not _requires_external_claim_evidence(evidence_plan, question=question):
                continue
            if _has_external_claim_evidence(successful):
                continue
        if (
            name in {
                "analyze_model_performance_weak_evidence_refs",
                "analyze_market_dynamics_weak_evidence_refs",
            }
            and _has_usable_market_evidence(successful)
        ):
            continue
        if name in seen:
            continue
        seen.add(name)
        missing.append({
            "name": name,
            "reason": reason,
            "impact": "weakens_answer",
        })
    for evidence in evidences:
        diagnostics = evidence.get("coverageDiagnostics")
        if not isinstance(diagnostics, dict):
            continue
        diagnosis = str(diagnostics.get("diagnosis") or "").strip()
        if not diagnosis:
            continue
        name = f"coverage_diagnostic:{diagnosis}"
        if name in seen:
            continue
        seen.add(name)
        impact: Literal["blocking", "weakens_answer", "optional"] = "blocking" if diagnosis in {
            "current_price_table_empty",
            "no_current_prices_for_country",
            "no_current_prices_for_requested_models",
            "country_scope_mismatch",
        } else "weakens_answer"
        if (
            diagnosis in {
                "current_price_table_empty",
                "no_current_prices_for_country",
                "no_current_prices_for_requested_models",
            }
            and _market_overview_price_gap_can_weaken(
                evidence_plan=evidence_plan,
                question=question,
                successful_evidences=successful,
            )
        ):
            impact = "weakens_answer"
        if diagnosis == "no_current_prices_for_requested_models" and (
            target_price_with_corridor
            or target_delta_with_context
            or method_price_with_corridor
            or (
                str(evidence_plan.get("intent") or "") == "competitor_compare"
                and competitor_gap_context
            )
            or _policy_price_cap_context_allows_partial_answer(
                evidence_plan=evidence_plan,
                question=question,
                successful_evidences=successful,
            )
        ):
            impact = "weakens_answer"
        missing.append({
            "name": name,
            "reason": _coverage_diagnostic_reason(diagnostics),
            "impact": impact,
        })
    model_level_gap = _missing_model_level_market_opportunity_evidence(
        evidence_plan,
        evidences,
        question=question,
    )
    if model_level_gap and model_level_gap["name"] not in seen:
        seen.add(model_level_gap["name"])
        missing.append(model_level_gap)
    for item in _missing_cross_country_evidence(evidence_plan, evidences):
        if item["name"] in seen:
            continue
        seen.add(item["name"])
        missing.append(item)
    return missing


def _market_overview_price_gap_can_weaken(
    *,
    evidence_plan: dict[str, Any],
    question: str,
    successful_evidences: list[ToolEvidence],
) -> bool:
    if str(evidence_plan.get("intent") or "") != "market_overview":
        return False
    if _question_mentions_price_evidence(question):
        return False
    return _has_usable_market_evidence(successful_evidences)


def _question_mentions_price_evidence(question: str) -> bool:
    text = str(question or "").casefold()
    return any(
        token in text
        for token in (
            "价格",
            "定价",
            "价格带",
            "价格走廊",
            "价差",
            "便宜",
            "贵",
            "月供",
            "残值",
            "msrp",
            "price",
            "pricing",
            "corridor",
            "cheaper",
            "expensive",
            "leasing",
            "lease",
            "rv",
        )
    )


def _required_coverage_attempted_tool_names(
    evidences: list[ToolEvidence],
    successful_tool_names: list[str],
) -> list[str]:
    """Count same-country failed attempts as attempted coverage; keep scope mismatches strict."""
    result: list[str] = []
    seen: set[str] = set()
    for name in successful_tool_names:
        value = str(name or "").strip()
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    for evidence in evidences:
        tool_name = str(evidence.get("toolName") or "").strip()
        if not tool_name or tool_name in seen or evidence.get("success"):
            continue
        diagnostics = evidence.get("coverageDiagnostics") if isinstance(evidence.get("coverageDiagnostics"), dict) else {}
        if str(diagnostics.get("diagnosis") or "") == "country_scope_mismatch":
            continue
        result.append(tool_name)
        seen.add(tool_name)
    return result


def _policy_price_cap_context_allows_partial_answer(
    *,
    evidence_plan: dict[str, Any],
    question: str,
    successful_evidences: list[ToolEvidence],
) -> bool:
    """Policy price-cap reports can answer impact paths even when current MSRP rows are absent."""
    intent = str(evidence_plan.get("intent") or "").strip()
    if intent not in {"news_policy_search", "report_generation"}:
        return False
    entities = evidence_plan.get("entities") if isinstance(evidence_plan.get("entities"), dict) else {}
    entity_text = " ".join(
        str(item or "")
        for value in entities.values()
        for item in (value if isinstance(value, list) else [value])
    )
    text = f"{question} {entity_text}".casefold()
    if not (
        ("价格上限" in text or "price cap" in text or "eligibility threshold" in text)
        and ("补贴" in text or "subsidy" in text or "incentive" in text or "elbil" in text)
        and ("bev" in text or "纯电" in text or "电动" in text)
    ):
        return False
    return _has_external_claim_evidence(successful_evidences) or _has_usable_market_evidence(successful_evidences)


def _has_non_method_evidence_refs(evidences: list[ToolEvidence]) -> bool:
    return any(
        item.get("toolName") != "business_method_material"
        and bool(item.get("evidenceRefs"))
        for item in evidences
    )


def _model_hint(evidence_plan: dict[str, Any]) -> str:
    entities = evidence_plan.get("entities") if isinstance(evidence_plan.get("entities"), dict) else {}
    models = entities.get("models") if isinstance(entities.get("models"), list) else []
    for item in models:
        text = str(item or "").strip()
        if text:
            return text
    return ""


def _missing_model_level_market_opportunity_evidence(
    evidence_plan: dict[str, Any],
    evidences: list[ToolEvidence],
    *,
    question: str,
) -> MissingEvidence | None:
    if str(evidence_plan.get("intent") or "") != "market_overview":
        return None
    if not _question_requires_model_level_market_opportunity_evidence(evidence_plan, question):
        return None
    if not _has_usable_market_evidence(evidences):
        return None
    if _has_model_level_market_opportunity_evidence(evidence_plan, evidences):
        return None
    return {
        "name": "model_level_market_opportunity_evidence",
        "reason": (
            "Market snapshot evidence can support a country/segment opportunity read, "
            "but the named model still lacks model-level competitor, current price, configuration, "
            "or positioning evidence for an entry/fit verdict."
        ),
        "impact": "weakens_answer",
    }


def _question_requires_model_level_market_opportunity_evidence(
    evidence_plan: dict[str, Any],
    question: str,
) -> bool:
    entities = evidence_plan.get("entities") if isinstance(evidence_plan.get("entities"), dict) else {}
    models = _string_list(entities.get("models"))
    text = str(question or "").casefold()
    has_named_model = bool(models) or any(
        token in text
        for token in (
            "j7",
            "jaecoo 7",
            "jaecoo7",
            "o5",
            "omoda 5",
            "omoda5",
            "o9",
            "omoda 9",
            "omoda9",
            "j8",
            "jaecoo 8",
            "jaecoo8",
        )
    )
    if not has_named_model:
        return False
    return any(
        token in text
        for token in (
            "market",
            "opportunity",
            "fit",
            "validation",
            "validate",
            "outlook",
            "市场",
            "机会",
            "适合",
            "值得",
            "验证",
            "进入",
            "推",
        )
    )


def _has_model_level_market_opportunity_evidence(
    evidence_plan: dict[str, Any],
    evidences: list[ToolEvidence],
) -> bool:
    if _has_usable_competitive_or_configuration_evidence(evidences):
        return True
    return _has_usable_price_or_method_evidence(evidences)


def _has_usable_price_or_method_evidence(evidences: list[ToolEvidence]) -> bool:
    text = " ".join(
        f"{ref.get('label', '')} {ref.get('value', '')} {ref.get('source', '')} {ref.get('table', '')}"
        for item in evidences
        if item.get("success")
        and str(item.get("toolName") or "") in {
            "query_msrp_pricing",
            "query_price_positioning",
            "business_method_material",
            "user_supplied_target_price",
        }
        for ref in item.get("evidenceRefs", [])
        if not _is_weak_ref(ref)
    ).lower()
    return bool(text) and any(
        token in text
        for token in (
            "msrp",
            "price",
            "pricing",
            "corridor",
            "target price",
            "main trim",
            "competitor pool",
            "configuration",
            "pva",
        )
    )


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _missing_cross_country_evidence(evidence_plan: dict[str, Any], evidences: list[ToolEvidence]) -> list[MissingEvidence]:
    entities = evidence_plan.get("entities") if isinstance(evidence_plan.get("entities"), dict) else {}
    countries = _canonical_country_list(entities.get("countries"))
    if len(countries) < 2:
        return []
    covered = _covered_country_names(evidences)
    result: list[MissingEvidence] = []
    for country in countries:
        if country in covered:
            continue
        result.append({
            "name": f"missing_country_snapshot:{country}",
            "reason": f"Cross-country question requires citation-ready market evidence for {country}, but no usable snapshot or cross-country refs were returned.",
            "impact": "blocking",
        })
    return result


def _covered_country_names(evidences: list[ToolEvidence]) -> set[str]:
    covered: set[str] = set()
    for evidence in evidences:
        if not evidence.get("success") or not evidence.get("evidenceRefs"):
            continue
        tool_name = str(evidence.get("toolName") or "")
        if tool_name in {"query_country_snapshot", "build_market_chart", "query_with_filters"}:
            query = evidence.get("query") if isinstance(evidence.get("query"), dict) else {}
            query_country = _canonical_country(str(query.get("country") or ""))
            if query_country:
                covered.add(query_country)
        for ref in evidence.get("evidenceRefs", []):
            if not isinstance(ref, dict) or _is_weak_ref(ref):
                continue
            label = str(ref.get("label") or "")
            match = re.search(r"crossCountry\.([^.]+)\.", label)
            if match:
                covered.add(_canonical_country(match.group(1)))
    return {item for item in covered if item}


def _canonical_country_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        country = _canonical_country(str(item or ""))
        if country and country not in result:
            result.append(country)
    return result


def _canonical_country(value: str) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    mapping = {
        "sweden": "Sweden",
        "瑞典": "Sweden",
        "finland": "Finland",
        "芬兰": "Finland",
        "norway": "Norway",
        "挪威": "Norway",
        "denmark": "Denmark",
        "丹麦": "Denmark",
        "hungary": "Hungary",
        "匈牙利": "Hungary",
        "germany": "Germany",
        "德国": "Germany",
    }
    return mapping.get(token.casefold(), token)


def _number_from_text(value: Any) -> int | float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value or "")
    match = re.search(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?", text)
    if not match:
        return None
    raw = match.group(0).replace(",", "")
    try:
        number = float(raw)
    except ValueError:
        return None
    return int(number) if number.is_integer() else number


def _target_price_range_from_question(question: str) -> dict[str, Any] | None:
    text = str(question or "")
    range_pattern = re.compile(
        r"(?P<low>\d+(?:[.,]\d+)?)\s*(?P<low_suffix>k|K|万)?\s*"
        r"(?:-|–|—|~|至|到)\s*"
        r"(?P<high>\d+(?:[.,]\d+)?)\s*(?P<high_suffix>k|K|万)?\s*"
        r"(?P<currency>eur|euro|€|sek|kr|欧元|瑞典克朗)?",
        re.IGNORECASE,
    )
    match = range_pattern.search(text)
    if not match:
        return None
    currency = _normalize_currency(match.group("currency") or text)
    low = _scaled_price(match.group("low"), match.group("low_suffix") or match.group("high_suffix"))
    high = _scaled_price(match.group("high"), match.group("high_suffix") or match.group("low_suffix"))
    if low is None or high is None:
        return None
    minimum = min(low, high)
    maximum = max(low, high)
    return {
        "min": minimum,
        "max": maximum,
        "midpoint": int((minimum + maximum) / 2) if (minimum + maximum).is_integer() else round((minimum + maximum) / 2, 2),
        "currency": currency,
    }


def _scaled_price(value: str | None, suffix: str | None) -> float | None:
    if value is None:
        return None
    try:
        number = float(str(value).replace(",", "."))
    except ValueError:
        return None
    suffix_normalized = str(suffix or "").lower()
    if suffix_normalized == "k":
        number *= 1000
    elif suffix_normalized == "万":
        number *= 10000
    return number


def _normalize_currency(value: str) -> str:
    text = str(value or "").lower()
    if "sek" in text or "瑞典克朗" in text or re.search(r"\bkr\b", text):
        return "SEK"
    if "eur" in text or "euro" in text or "€" in text or "欧元" in text:
        return "EUR"
    return "EUR"


def _has_user_supplied_target_price_evidence(evidences: list[ToolEvidence]) -> bool:
    return any(
        item.get("success")
        and str(item.get("toolName") or "") == "user_supplied_target_price"
        and bool(item.get("evidenceRefs"))
        for item in evidences
    )


def _has_user_supplied_price_delta_evidence(evidences: list[ToolEvidence]) -> bool:
    return any(
        item.get("success")
        and str(item.get("toolName") or "") == "user_supplied_price_delta"
        and bool(item.get("evidenceRefs"))
        for item in evidences
    )


def _has_business_method_price_corridor(evidences: list[ToolEvidence]) -> bool:
    text = " ".join(
        f"{ref.get('label', '')} {ref.get('value', '')}"
        for item in evidences
        if item.get("success") and str(item.get("toolName") or "") == "business_method_material"
        for ref in item.get("evidenceRefs", [])
        if not _is_weak_ref(ref)
    ).lower()
    return bool(text) and "main trim msrp" in text and "competitor corridor" in text


def _has_usable_competitive_or_configuration_evidence(evidences: list[ToolEvidence]) -> bool:
    text = " ".join(
        f"{ref.get('label', '')} {ref.get('value', '')} {ref.get('source', '')} {ref.get('table', '')}"
        for item in evidences
        if item.get("success")
        and str(item.get("toolName") or "") in {"compare_vehicle_variants", "compare_competitive_set", "analyze_model_performance"}
        for ref in item.get("evidenceRefs", [])
        if not _is_weak_ref(ref)
    ).lower()
    return bool(text) and any(
        token in text
        for token in (
            "competitor",
            "configuration",
            "feature",
            "variant",
            "sales",
            "volume",
            "rank",
            "share",
            "avgprice",
            "minprice",
            "maxprice",
            "msrp",
            "price",
        )
    )


def _has_usable_market_evidence(evidences: list[ToolEvidence]) -> bool:
    text = " ".join(
        f"{ref.get('label', '')} {ref.get('value', '')} {ref.get('source', '')} {ref.get('table', '')}"
        for item in evidences
        if item.get("success")
        and str(item.get("toolName") or "") != "business_method_material"
        for ref in item.get("evidenceRefs", [])
        if not _is_weak_ref(ref)
    ).lower()
    return bool(text) and any(
        token in text
        for token in (
            "sales",
            "share",
            "volume",
            "market",
            "topmodel",
            "topbrand",
            "powertain",
            "powertrain",
            "monthseries",
            "yearseries",
            "cumulativesales",
        )
    )


def _has_external_claim_evidence(evidences: list[ToolEvidence]) -> bool:
    for evidence in evidences:
        if not evidence.get("success"):
            continue
        if str(evidence.get("toolName") or "") not in {
            "external_research",
            "search_market_news",
            "pageindex_search_documents",
            "minirag_query_graph",
            "read_web_page",
        }:
            continue
        for ref in evidence.get("evidenceRefs", []):
            if not isinstance(ref, dict) or _is_weak_ref(ref):
                continue
            label = str(ref.get("label") or "").lower()
            value = str(ref.get("value") or "").strip()
            if not value:
                continue
            if label.endswith(".claim") or "supportedclaim" in label:
                return True
    return False


def _requires_external_claim_evidence(evidence_plan: dict[str, Any], *, question: str = "") -> bool:
    intent = str(evidence_plan.get("intent") or "").strip()
    if intent in {"news_policy_search", "voc_analysis"}:
        return True
    external_tools = {
        "external_research",
        "search_market_news",
        "pageindex_search_documents",
        "minirag_query_graph",
        "read_web_page",
        "browser_snapshot",
    }
    required_tools = evidence_plan.get("requiredTools")
    if isinstance(required_tools, list) and any(str(tool) in external_tools for tool in required_tools):
        return True
    if intent == "report_generation":
        return _report_question_requires_external_claim(question)
    evidence_text_parts: list[str] = []
    for item in evidence_plan.get("evidenceNeeded", []):
        if not isinstance(item, dict):
            continue
        evidence_text_parts.append(str(item.get("name") or ""))
        evidence_text_parts.append(str(item.get("reason") or ""))
    for item in evidence_plan.get("mustHaveEvidence", []):
        evidence_text_parts.append(str(item or ""))
    evidence_text = " ".join(evidence_text_parts).lower()
    return any(
        token in evidence_text
        for token in (
            "external",
            "fresh",
            "source",
            "citation",
            "policy",
            "news",
            "consumer",
            "voc",
            "published",
            "date",
            "联网",
            "来源",
            "引用",
            "新闻",
            "政策",
            "用户声音",
        )
    )


def _report_question_requires_external_claim(question: str) -> bool:
    text = str(question or "").casefold()
    return any(
        token in text
        for token in (
            "source",
            "sources",
            "citation",
            "citations",
            "research",
            "search",
            "tavily",
            "web",
            "policy",
            "news",
            "subsidy",
            "tax",
            "voc",
            "forum",
            "review",
            "complaint",
            "来源",
            "引用",
            "检索",
            "联网",
            "政策",
            "新闻",
            "补贴",
            "税",
            "舆情",
            "论坛",
            "口碑",
            "投诉",
            "用户声音",
        )
    )


def _missing_target_policy_source_evidence(
    question: str,
    evidence_plan: dict[str, Any],
    evidences: list[ToolEvidence],
) -> MissingEvidence | None:
    target = _target_policy_source_from_question(question, evidence_plan)
    if not target:
        return None
    ref_text = _external_ref_search_text(evidences)
    if _target_policy_source_is_covered(target, ref_text):
        return None
    return {
        "name": f"target_policy_source:{target['slug']}",
        "reason": (
            f"The question asks for {target['label']} with a specific policy name/year, "
            "but the retrieved external evidence does not cite a source that matches that target policy."
        ),
        "impact": "blocking",
    }


def _target_policy_source_from_question(
    question: str,
    evidence_plan: dict[str, Any],
) -> dict[str, str] | None:
    if str(evidence_plan.get("intent") or "").strip() != "news_policy_search":
        return None
    text = str(question or "")
    folded = text.casefold()
    if "elbilspremien" not in folded:
        return None
    year_match = re.search(r"\b(20\d{2})\b", text)
    year = year_match.group(1) if year_match else ""
    label = f"Elbilspremien {year}".strip()
    slug = "elbilspremien"
    if year:
        slug = f"{slug}_{year}"
    return {"label": label, "slug": slug, "name": "elbilspremien", "year": year}


def _external_ref_search_text(evidences: list[ToolEvidence]) -> str:
    parts: list[str] = []
    for evidence in evidences:
        if not evidence.get("success"):
            continue
        if str(evidence.get("toolName") or "") not in {
            "external_research",
            "search_market_news",
            "pageindex_search_documents",
            "minirag_query_graph",
            "read_web_page",
        }:
            continue
        parts.append(str(evidence.get("summary") or ""))
        parts.extend(str(item or "") for item in evidence.get("keyFindings", []))
        for ref in evidence.get("evidenceRefs", []):
            parts.append(str(ref.get("label") or ""))
            parts.append(str(ref.get("value") or ""))
            parts.append(str(ref.get("source") or ""))
            parts.append(str(ref.get("table") or ""))
    return " ".join(parts).casefold()


def _target_policy_source_is_covered(target: dict[str, str], ref_text: str) -> bool:
    if not ref_text:
        return False
    name = target.get("name") or ""
    if name and name not in ref_text:
        return False
    year = target.get("year") or ""
    if year and year not in ref_text:
        return False
    return True


def _price_delta_from_question(question: str) -> dict[str, Any] | None:
    text = str(question or "")
    direction = ""
    if re.search(r"便宜|低|lower|cheaper|below|under", text, flags=re.IGNORECASE):
        direction = "cheaper"
    elif re.search(r"贵|高|higher|above|premium", text, flags=re.IGNORECASE):
        direction = "more_expensive"
    if not direction:
        return None
    match = re.search(
        r"(?<![A-Za-z])(?P<amount>\d+(?:[.,]\d+)?)\s*(?P<suffix>k|K|千|万)?\s*(?P<currency>eur|euro|€|sek|kr|欧元|瑞典克朗)?",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    amount = _scaled_price(match.group("amount"), match.group("suffix"))
    if amount is None or amount <= 0:
        return None
    return {
        "amount": int(amount) if float(amount).is_integer() else amount,
        "currency": _normalize_currency(match.group("currency") or text),
        "direction": direction,
    }


def _is_covered(name: str, evidences: list[ToolEvidence]) -> bool:
    key = name.lower()
    successful = [item for item in evidences if item.get("success")]
    if not successful:
        return False
    tool_names = " ".join(str(item.get("toolName") or "") for item in successful)
    source_types = " ".join(str(item.get("sourceType") or "") for item in successful)
    ref_text = " ".join(
        f"{ref.get('label', '')} {ref.get('value', '')} {ref.get('source', '')} {ref.get('table', '')}"
        for item in successful
        for ref in item.get("evidenceRefs", [])
        if not _is_weak_ref(ref)
    ).lower()
    non_target_ref_text = " ".join(
        f"{ref.get('label', '')} {ref.get('value', '')} {ref.get('source', '')} {ref.get('table', '')}"
        for item in successful
        if str(item.get("toolName") or "") != "user_supplied_target_price"
        for ref in item.get("evidenceRefs", [])
        if not _is_weak_ref(ref)
    ).lower()
    own_model_ref_text = " ".join(
        f"{ref.get('label', '')} {ref.get('value', '')} {ref.get('source', '')} {ref.get('table', '')}"
        for item in successful
        if str(item.get("toolName") or "") in {"query_msrp_pricing", "business_method_material"}
        for ref in item.get("evidenceRefs", [])
        if not _is_weak_ref(ref)
    ).lower()
    target_price_ref_text = " ".join(
        f"{ref.get('label', '')} {ref.get('value', '')} {ref.get('source', '')} {ref.get('table', '')}"
        for item in successful
        if str(item.get("toolName") or "") == "user_supplied_target_price"
        for ref in item.get("evidenceRefs", [])
        if not _is_weak_ref(ref)
    ).lower()
    searchable = f"{tool_names} {source_types} {ref_text}".lower()

    if "supporting_evidence" in key:
        return bool(ref_text)
    if "price_or_config_gap" in key:
        return bool(ref_text) and any(
            token in ref_text
            for token in (
                "price",
                "msrp",
                "pricing",
                "avgprice",
                "minprice",
                "maxprice",
                "configuration",
                "config",
                "feature",
                "variant",
                "battery",
                "range",
                "sales",
                "volume",
                "rank",
                "share",
                "competitor",
            )
        )
    if "report" in key or "outline" in key:
        return "report_block" in searchable or "report" in ref_text or "market_chart" in searchable or "country_snapshot" in searchable
    if "corridor" in key or "competitor_price_range" in key:
        return bool(non_target_ref_text) and (
            "corridor" in non_target_ref_text
            or "pricestats" in non_target_ref_text
            or "msrp" in non_target_ref_text
            or "price" in non_target_ref_text
            or "range" in non_target_ref_text
        )
    if "competitor" in key or "competitor_pool" in key or "competitor_set" in key:
        return bool(non_target_ref_text) and (
            "competitor." in non_target_ref_text
            or ".model" in non_target_ref_text
            or "competitor pool" in non_target_ref_text
            or "competitor" in non_target_ref_text
        )
    if "current_msrp" in key or ("current" in key and "msrp" in key):
        return bool(own_model_ref_text) and ("msrp" in own_model_ref_text or "own model" in own_model_ref_text or "main trim" in own_model_ref_text)
    if "own_model_price" in key:
        return (
            bool(own_model_ref_text)
            and ("msrp" in own_model_ref_text or "own model" in own_model_ref_text or "main trim" in own_model_ref_text)
        ) or (
            bool(target_price_ref_text)
            and "target price" in target_price_ref_text
        )
    if "msrp" in key:
        return bool(own_model_ref_text) and ("msrp" in own_model_ref_text or "main trim" in own_model_ref_text)
    if "trend" in key or "series" in key or any(token in key for token in ("market", "mix", "kpi", "share")):
        return bool(ref_text) and (
            "sales" in ref_text
            or "share" in ref_text
            or "volume" in ref_text
            or "mix" in ref_text
            or "market" in ref_text
            or "topmodel" in ref_text
            or "topbrand" in ref_text
            or "monthseries" in ref_text
            or "yearseries" in ref_text
            or "penetration" in ref_text
        )
    if "price" in key or (
        "monthly" in key
        and any(token in key for token in ("payment", "leasing", "lease", "installment"))
    ):
        return bool(ref_text) and ("msrp" in ref_text or "price" in ref_text or "pricing" in ref_text)
    if any(token in key for token in ("feature", "trim", "configuration", "powertrain", "battery")):
        return bool(ref_text) and ("variant" in searchable or "config" in searchable or "engineering" in searchable or "feature" in ref_text or "battery" in ref_text)
    if "published_date" in key or ("published" in key and "date" in key):
        return _has_published_date_evidence(evidences)
    if any(token in key for token in ("source", "policy", "fresh", "consumer", "voc", "date")):
        return bool(ref_text) and ("web" in searchable or "policy" in searchable or "minirag" in searchable or "pageindex" in searchable)
    if any(token in key for token in ("inventory", "stock", "order", "available", "version")):
        return bool(ref_text) and ("available" in ref_text or "stock" in ref_text or "inventory" in ref_text or "material" in ref_text or "version" in ref_text)
    if "basic" in key:
        return bool(successful)
    return bool(successful and any(item.get("evidenceRefs") for item in successful))


def _is_weak_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    raw_value = ref.get("value")
    value = "" if raw_value is None else str(raw_value).strip().lower()
    is_zero_count = value in {"0", "0.0"} and any(token in label for token in ("count", "rows", "row_count"))
    return (
        label in {"row_count", "metadata.result_count", "chart_count"}
        or _is_technical_count_label(label)
        or is_zero_count
        or label.endswith(".source")
        or label.endswith(".sourc")
        or label.endswith(" source")
        or label.endswith(" sourc")
        or label.endswith(".date")
        or value.startswith(("http://", "https://"))
    )


def _is_technical_count_label(label: str) -> bool:
    """Separate tool coverage metadata from market evidence counts."""
    normalized = re.sub(r"[^a-z0-9]+", "", str(label or "").lower())
    if not normalized:
        return False
    technical_count_labels = {
        "totalrows",
        "rowcount",
        "countrycount",
        "marketcount",
        "brandcount",
        "modelcount",
        "resultcount",
        "documentcount",
        "entitycount",
        "chunkcount",
        "sectioncount",
        "itemcount",
        "citationcount",
        "chartcount",
        "metadatacount",
        "metadataresultcount",
    }
    if normalized in technical_count_labels:
        return True
    return bool(
        normalized.startswith(("kpis", "marketsnapshotkpis", "contextsnapshotkpis"))
        and normalized.endswith(("count", "rows", "resultcount"))
        and not any(token in normalized for token in ("sales", "volume", "share", "registration", "penetration", "mix"))
    )


def _weak_ref_missing_evidence(evidence: ToolEvidence) -> tuple[str, str]:
    tool_name = str(evidence.get("toolName") or "tool").strip()
    if tool_name in {"query_country_snapshot", "build_market_chart"}:
        return (
            "market_snapshot_data_unavailable",
            "Market snapshot tools returned only weak count/source refs; no usable sales, share, mix, top-model, or segment evidence was available.",
        )
    if tool_name in {"compare_vehicle_variants", "compare_competitive_set"}:
        return (
            "competitive_or_configuration_data_unavailable",
            "Comparison tools returned only weak refs; no usable competitor, configuration, feature, price, or positioning evidence was available.",
        )
    if tool_name in {"external_research", "search_market_news", "pageindex_search_documents", "minirag_query_graph"}:
        return (
            "external_research_claims_unavailable",
            "External research returned only source/date/count refs; no supported claim or source-backed business finding was available.",
        )
    if tool_name in {"query_msrp_pricing", "query_price_positioning"}:
        return (
            "pricing_data_unavailable",
            "Pricing tools returned only weak refs; no usable MSRP, price corridor, or price-stat evidence was available.",
        )
    if tool_name == "query_leasing_offers":
        return (
            "leasing_tco_or_company_car_evidence",
            "Leasing tool returned no usable monthly payment, term, mileage, residual value, or total contract cost evidence.",
        )
    return (
        f"{tool_name or 'tool'}_weak_evidence_refs",
        "Tool completed but returned only weak count/source/date refs, so the answer still needs citation-ready evidence.",
    )


def _confidence(evidences: list[ToolEvidence], missing: list[MissingEvidence]) -> EvidenceConfidence:
    successful = [item for item in evidences if item.get("success")]
    ref_total = sum(
        1
        for item in successful
        for ref in item.get("evidenceRefs", [])
        if not _is_weak_ref(ref)
    )
    blocking_missing = any(item["impact"] == "blocking" for item in missing)
    external_claim_gap = any(
        item.get("name") == "external_research_claims_unavailable"
        for item in missing
    )
    target_policy_gap = any(
        str(item.get("name") or "").startswith("target_policy_source:")
        for item in missing
    )
    model_level_opportunity_gap = any(
        item.get("name") == "model_level_market_opportunity_evidence"
        for item in missing
    )
    market_snapshot_gap = any(
        item.get("name") == "market_snapshot_data_unavailable"
        for item in missing
    )
    scope_conflict_gap = any(
        str(item.get("name") or "").startswith("evidence_scope_conflict:")
        for item in missing
    )
    if not successful or ref_total == 0:
        return "low"
    if market_snapshot_gap and not _has_usable_market_evidence(successful):
        return "low"
    if scope_conflict_gap:
        return "low"
    if target_policy_gap:
        return "medium" if not blocking_missing else "low"
    if model_level_opportunity_gap and not blocking_missing:
        return "medium"
    if external_claim_gap and not blocking_missing:
        return "medium"
    if not blocking_missing and ref_total >= 3:
        return "high"
    return "medium"


def _research_governance_from_tool_results(tool_results: list[dict[str, Any]]) -> dict[str, Any]:
    for item in tool_results:
        result = item.get("result") if isinstance(item.get("result"), dict) else {}
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        governance = data.get("researchGovernance") if isinstance(data.get("researchGovernance"), dict) else {}
        if governance:
            return _sanitize_research_governance(governance)
    return {}


def _sanitize_research_governance(governance: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(governance)
    rejected = governance.get("rejectedSources") if isinstance(governance.get("rejectedSources"), list) else []
    if not rejected:
        return sanitized
    reason_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    for item in rejected:
        if not isinstance(item, dict):
            continue
        reason = str(item.get("reason") or "unknown").strip() or "unknown"
        category = str(item.get("sourceCategory") or "unknown").strip() or "unknown"
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        category_counts[category] = category_counts.get(category, 0) + 1
    sanitized["rejectedSourceCount"] = len([item for item in rejected if isinstance(item, dict)])
    sanitized["rejectedSourceReasons"] = dict(sorted(reason_counts.items()))
    sanitized["rejectedSourceCategories"] = dict(sorted(category_counts.items()))
    sanitized["rejectedSources"] = []
    return sanitized


def _jato_cross_check_from_tool_results(tool_results: list[dict[str, Any]]) -> dict[str, Any]:
    for item in tool_results:
        result = item.get("result") if isinstance(item.get("result"), dict) else {}
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        cross_check = data.get("jatoCrossCheck") if isinstance(data.get("jatoCrossCheck"), dict) else {}
        if cross_check:
            return dict(cross_check)
    return {}


def _insight_cards_from_tool_results(tool_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for item in tool_results:
        result = item.get("result") if isinstance(item.get("result"), dict) else {}
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        cards = data.get("insightCards") if isinstance(data.get("insightCards"), list) else []
        if cards:
            return [dict(card) for card in cards if isinstance(card, dict)]
    return []


def _has_published_date_evidence(evidences: list[ToolEvidence]) -> bool:
    for evidence in evidences:
        if not evidence.get("success"):
            continue
        for ref in evidence.get("evidenceRefs", []):
            if not isinstance(ref, dict):
                continue
            label = str(ref.get("label") or "").lower()
            value = str(ref.get("value") or "").strip()
            if not value:
                continue
            if (
                "published" in label
                or label.endswith(".date")
                or label.endswith("_date")
                or label in {"date", "source_date"}
            ):
                return True
    return False


def _governance_missing_evidence(
    governance: dict[str, Any],
    evidences: list[ToolEvidence],
) -> list[MissingEvidence]:
    values = governance.get("missingEvidence")
    if not isinstance(values, list):
        return []
    result: list[MissingEvidence] = []
    seen: set[str] = set()
    for item in values:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name or name in seen:
            continue
        if name == "published_date" and _has_published_date_evidence(evidences):
            continue
        if (
            name == "external_research_claims_unavailable"
            and _has_external_claim_evidence(evidences)
        ):
            continue
        seen.add(name)
        impact = str(item.get("impact") or "weakens_answer").strip()
        result.append(
            {
                "name": name,
                "reason": str(
                    item.get("reason")
                    or "Research governance policy marked this evidence as missing."
                ).strip(),
                "impact": (
                    "blocking"
                    if impact == "blocking"
                    else "optional"
                    if impact == "optional"
                    else "weakens_answer"
                ),
            }
        )
    return result


def _governed_confidence(
    confidence: EvidenceConfidence,
    governance: dict[str, Any],
    jato_cross_check: dict[str, Any],
) -> EvidenceConfidence:
    if jato_cross_check.get("status") == "conflicting":
        return "low"
    policy_status = str(governance.get("policyStatus") or "")
    if policy_status == "blocking":
        return "low"
    if policy_status == "warning" and confidence == "high":
        return "medium"
    research_confidence = str(governance.get("confidence") or "")
    if research_confidence in {"low", "medium"} and confidence == "high":
        return "medium" if research_confidence == "medium" else "low"
    return confidence


def _row_count(data: dict[str, Any], metadata: dict[str, Any]) -> int:
    variant_matrix_count = _variant_matrix_row_count(data)
    if variant_matrix_count is not None:
        return variant_matrix_count
    comparison = data.get("comparison")
    if isinstance(comparison, dict) and comparison:
        return len(comparison)
    for key in ("items", "sections", "documents", "citations", "topModels", "topBrands", "powertrainMix", "yearSeries", "monthSeries"):
        value = data.get(key)
        if isinstance(value, list):
            return len(value)
    results = data.get("results")
    if isinstance(results, dict):
        nested_total = 0
        for key in ("topModels", "topBrands", "powertrainMix", "yearSeries", "monthSeries"):
            value = results.get(key)
            if isinstance(value, list):
                nested_total += len(value)
        if nested_total > 0:
            return nested_total
    cross_tab_total = 0
    for key in ("driveByFuel", "driveBySegment", "segmentByFuel", "fuelBySegment", "registrationByFuel", "registrationBySegment"):
        value = data.get(key)
        if isinstance(value, list):
            cross_tab_total += len(value)
    if cross_tab_total > 0:
        return cross_tab_total
    for key in ("resultCount", "documentCount", "entityCount", "chunkCount", "chartCount"):
        value = metadata.get(key)
        if isinstance(value, int):
            return value
    return 1 if data else 0


def _variant_matrix_row_count(data: dict[str, Any]) -> int | None:
    if not any(
        key in data
        for key in ("subjects", "compareSubjects", "differentFeatures", "diffFeatures", "commonFeatures", "selectionNotes")
    ):
        return None
    total = 0
    for key in ("subjects", "compareSubjects", "differentFeatures", "diffFeatures", "commonFeatures", "selectionNotes"):
        value = data.get(key)
        if isinstance(value, list):
            total += len(value)
    return total


def _freshness(data: dict[str, Any], metadata: dict[str, Any], retrieved_at: str) -> str:
    for value in (data.get("latestUpdatedAt"), metadata.get("latestUpdatedAt"), data.get("updatedAt"), metadata.get("date")):
        if isinstance(value, str) and value.strip():
            return value.strip()
    return retrieved_at


def _source_type(tool_name: str, source: str) -> EvidenceSourceType:
    combined = f"{tool_name} {source}".lower()
    if "msrp" in combined or "postgres" in combined:
        return "postgres"
    if "variant" in combined or "engineering" in combined or "bom" in combined:
        return "engineering"
    if "news" in combined or "browser" in combined or "web" in combined:
        return "web"
    if "policy" in combined or "pageindex" in combined:
        return "policy"
    if "voc" in combined or "minirag" in combined:
        return "voc"
    if "chart" in combined:
        return "generated"
    return "jato_parquet"


def _summary(tool_name: str, source: str, data: dict[str, Any], row_count: int) -> str:
    explicit = data.get("summary")
    if isinstance(explicit, str) and explicit.strip():
        return explicit[:300]
    if tool_name == "compare_vehicle_variants" and _variant_matrix_row_count(data) == 0:
        query_models = data.get("queryModels") if isinstance(data.get("queryModels"), list) else []
        model_text = ", ".join(str(item) for item in query_models[:4] if str(item).strip())
        suffix = f" for {model_text}" if model_text else ""
        return (
            f"{tool_name} returned no variant/configuration matrix rows from {source or 'jato'}{suffix}; "
            "model mapping or engineering configuration source coverage needs repair."
        )
    keys = ", ".join(list(data.keys())[:6]) if data else "no data"
    return f"{tool_name} returned {row_count} evidence rows from {source or 'jato'} ({keys})."


def _key_findings(data: dict[str, Any], refs: list[EvidenceRef]) -> list[str]:
    findings: list[str] = []
    for ref in refs[:5]:
        label = ref.get("label", "evidence")
        value = ref.get("value", "")
        unit = f" {ref.get('unit')}" if ref.get("unit") else ""
        findings.append(f"{label}: {value}{unit}")
    if not findings and data:
        if _variant_matrix_row_count(data) == 0:
            findings.append("variant_matrix_unavailable: no subjects, feature deltas, common features, or selection notes returned")
        else:
            findings.append(f"Available fields: {', '.join(list(data.keys())[:5])}")
    return findings


def _coverage_diagnostic_findings(diagnostics: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    diagnosis = str(diagnostics.get("diagnosis") or "").strip()
    if diagnosis:
        findings.append(f"coverage_diagnosis: {diagnosis}")
    if diagnosis == "country_scope_mismatch":
        requested_country = str(diagnostics.get("requestedCountry") or "").strip()
        returned_country = str(diagnostics.get("returnedCountry") or "").strip()
        if requested_country or returned_country:
            findings.append(f"country_scope: requested {requested_country or 'unknown'}, returned {returned_country or 'unknown'}")
    current_rows = (
        diagnostics.get("currentPriceRows")
        if isinstance(diagnostics.get("currentPriceRows"), dict)
        else {}
    )
    requested_country = current_rows.get("requestedCountry")
    total = current_rows.get("total")
    if isinstance(total, int) and isinstance(requested_country, int):
        findings.append(
            f"current_price_rows: requested country {requested_country}, total {total}"
        )
    next_actions = diagnostics.get("nextActions")
    if isinstance(next_actions, list) and next_actions:
        action = str(next_actions[0] or "").strip()
        if action:
            findings.append(f"repair_action: {action[:180]}")
    return findings


def _coverage_diagnostic_reason(diagnostics: dict[str, Any]) -> str:
    diagnosis = str(diagnostics.get("diagnosis") or "coverage_gap").strip()
    if diagnosis == "country_scope_mismatch":
        requested_country = str(diagnostics.get("requestedCountry") or "").strip()
        returned_country = str(diagnostics.get("returnedCountry") or "").strip()
        return (
            f"Tool returned {returned_country or 'another country'} evidence while the user requested "
            f"{requested_country or 'the requested country'}; those refs were excluded from grounded conclusions."
        )
    next_actions = diagnostics.get("nextActions")
    if isinstance(next_actions, list):
        for item in next_actions:
            action = str(item or "").strip()
            if action:
                return action[:300]
    requested = (
        diagnostics.get("requested")
        if isinstance(diagnostics.get("requested"), dict)
        else {}
    )
    country = str(requested.get("country") or "").strip()
    return f"MSRP coverage diagnostic reported {diagnosis} for {country or 'the requested scope'}."


def _unit_for_label(label: str) -> str:
    lower = label.lower()
    if any(
        token in lower
        for token in (
            "priceevidencestatus",
            "priceevidencerole",
            "sourcedraftpath",
            "candidatedomain",
            "candidatesourcetype",
            "materializationstatus",
        )
    ):
        return ""
    if any(token in lower for token in ("currentpricerows", "reviewpendingrows", "pricerecords")):
        return "units"
    if "share" in lower or "percent" in lower or "pct" in lower:
        return "%"
    if "apr" in lower:
        return "%"
    if "monthlypayment" in lower or "effectivemonthly" in lower:
        return "EUR/month"
    if any(token in lower for token in ("residualvalueeur", "totalcontractcosteur", "capcosteur", "downpaymenteur")):
        return "EUR"
    if "termmonths" in lower:
        return "months"
    if "mileageperyear" in lower:
        return "km/year"
    if "price" in lower or "msrp" in lower:
        return "currency"
    if "sales" in lower or "volume" in lower or "count" in lower:
        return "units"
    if "range" in lower:
        return "km"
    if "battery" in lower:
        return "kWh"
    return ""


def _table_name(tool_name: str, source: str) -> str:
    if source:
        return source
    return tool_name or "unknown_tool"


def _evidence_id(*, session_id: str, country: str, question: str, intent: str, retrieved_at: str) -> str:
    seed = f"{session_id}|{country}|{intent}|{question}|{retrieved_at}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]
    return f"evpkg_{digest}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
