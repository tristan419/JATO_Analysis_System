from __future__ import annotations

import re
from typing import Any, Literal, TypedDict
from urllib.parse import parse_qs, urlparse


VisualArtifactType = Literal["chart", "table", "metric_cards", "report_block"]
ChartType = Literal["bar", "line", "stacked_bar", "scatter"]


class ChartSpec(TypedDict, total=False):
    chartType: ChartType
    xField: str
    yField: str
    seriesField: str
    data: list[dict[str, Any]]
    note: str
    plotlyData: list[dict[str, Any]]
    plotlyLayout: dict[str, Any]


class VisualArtifact(TypedDict, total=False):
    id: str
    type: VisualArtifactType
    title: str
    subtitle: str
    data: Any
    spec: dict[str, Any]
    fallbackReason: str
    sourceEvidenceRefs: list[str]


def build_visual_artifacts(
    *,
    question: str,
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    charts: list[dict[str, Any]],
) -> list[VisualArtifact]:
    """Build product-ready visual artifacts from governed answer evidence."""
    intent = str(evidence_package.get("intent") or "")
    refs = _scoped_visual_evidence_refs(_evidence_refs(evidence_package), evidence_package, question=question)
    usable_refs = [ref for ref in refs if not _is_weak_visual_ref(ref)]
    display_refs = _visual_refs_for_intent(
        intent,
        question=question,
        evidence_package=evidence_package,
        refs=usable_refs,
    )
    ref_ids = [str(ref.get("refId") or "") for ref in usable_refs if str(ref.get("refId") or "").strip()]
    artifacts: list[VisualArtifact] = []

    metric_cards = _metric_card_rows(
        _metric_refs_for_intent(evidence_package, display_refs, question=question),
        preserve_order=intent in {"market_overview", "competitor_compare"},
    )
    if metric_cards:
        artifacts.append({
            "id": "artifact_metric_cards",
            "type": "metric_cards",
            "title": _metric_title(intent),
            "subtitle": _metric_subtitle(str(evidence_package.get("intent") or "")),
            "data": {
                "rows": metric_cards[:6],
                "intentAnalysis": _intent_analysis_block(evidence_package, answer, display_refs),
            },
            "sourceEvidenceRefs": [str(row["sourceEvidenceRef"]) for row in metric_cards if row.get("sourceEvidenceRef")],
        })

    chart_artifacts = _chart_artifacts(
        _charts_for_intent(intent, charts, question=question, evidence_package=evidence_package),
        ref_ids,
    )
    intent_chart = _intent_chart_from_evidence_refs(evidence_package, display_refs, question=question)
    if intent in {"market_overview", "pricing_analysis", "competitor_compare"} and intent_chart:
        artifacts.append(intent_chart)
    artifacts.extend(chart_artifacts)
    if not chart_artifacts and intent_chart and not _artifact_id_exists(artifacts, str(intent_chart.get("id") or "")):
        artifacts.append(intent_chart)

    if _wants_trend_chart(question) and not _has_line_chart(charts):
        trend_chart = _trend_chart_from_evidence_refs(usable_refs)
        if trend_chart:
            artifacts.append(trend_chart)
        else:
            fallback = _snapshot_fallback_chart(usable_refs)
            if fallback:
                _ensure_missing_evidence(
                    evidence_package,
                    name="monthly_trend_series",
                    reason="The user requested a trend chart, but the executed tools did not return a year or month trend series.",
                    impact="weakens_answer",
                )
                artifacts.append(fallback)

    powertrain_route_table = _powertrain_route_table_artifact(question, display_refs)
    if powertrain_route_table:
        artifacts.append(powertrain_route_table)

    table = _table_artifact(
        question,
        evidence_package,
        _table_refs_for_intent(evidence_package, refs, display_refs, question=question),
        answer,
    )
    if table:
        artifacts.append(table)

    configuration_competitor_table = _configuration_competitor_context_table_artifact(evidence_package, refs)
    if configuration_competitor_table:
        artifacts.append(configuration_competitor_table)

    pricing_market_table = _pricing_market_structure_table_artifact(question, evidence_package, display_refs, answer)
    if pricing_market_table:
        artifacts.append(pricing_market_table)

    report_coverage_table = _report_model_coverage_artifact(question, evidence_package, refs, answer)
    if report_coverage_table:
        artifacts.append(report_coverage_table)
    report_coverage_chart = _report_model_coverage_chart_artifact(question, evidence_package, refs, answer)
    if report_coverage_chart:
        artifacts.append(report_coverage_chart)

    bom_entity_artifact = _bom_entity_validation_artifact(question, evidence_package, refs, answer)
    if bom_entity_artifact:
        artifacts.append(bom_entity_artifact)

    repair_table = _msrp_source_repair_table_artifact(question, evidence_package, answer)
    if repair_table:
        artifacts.append(repair_table)
    pending_msrp_table = _pending_msrp_review_table_artifact(question, evidence_package, answer)
    if pending_msrp_table:
        artifacts.append(pending_msrp_table)
    pending_msrp_chart = _pending_msrp_review_chart_artifact(question, evidence_package, answer)
    if pending_msrp_chart:
        artifacts.append(pending_msrp_chart)
    external_repair_table = _external_source_repair_table_artifact(evidence_package, question=question)
    if external_repair_table:
        artifacts.append(external_repair_table)

    artifacts.extend(_supplemental_policy_pricing_artifacts(evidence_package, usable_refs, answer))
    artifacts.extend(_supplemental_report_pricing_artifacts(question, evidence_package, usable_refs, answer))
    artifacts.extend(_supplemental_policy_market_artifacts(question, evidence_package, usable_refs, answer))
    tco_artifact = _tco_validation_artifact(question, evidence_package, refs, answer)
    if tco_artifact:
        artifacts.append(tco_artifact)

    report_block = _report_block_artifact(
        answer,
        evidence_package,
        _report_block_ref_ids(intent, ref_ids, display_refs),
        question=question,
    )
    if report_block:
        artifacts.append(report_block)

    return _ordered_artifacts(intent, artifacts)[:8]


def _ordered_artifacts(intent: str, artifacts: list[VisualArtifact]) -> list[VisualArtifact]:
    if intent not in {
        "news_policy_search",
        "voc_analysis",
        "inventory_analysis",
        "pricing_analysis",
        "competitor_compare",
        "report_generation",
        "market_overview",
        "configuration_analysis",
    }:
        return artifacts
    return sorted(artifacts, key=lambda item: _artifact_priority(intent, item))


def _artifact_priority(intent: str, artifact: VisualArtifact) -> int:
    artifact_type = str(artifact.get("type") or "")
    artifact_id = str(artifact.get("id") or "")
    if intent == "news_policy_search":
        if artifact_id == "artifact_news_policy_search_table":
            return 0
        if artifact_id == "artifact_policy_market_context_table":
            return 1
        if artifact_id in {"artifact_tco_validation_table", "artifact_policy_pricing_table"}:
            return 2
        if artifact_type == "table" and artifact_id != "artifact_news_policy_search_framework_table":
            return 3
        if artifact_id == "artifact_external_source_repair_table":
            return 4
        if artifact_id == "artifact_news_policy_search_framework_table":
            return 5
        if artifact_type == "report_block":
            return 6
        if artifact_type == "chart":
            return 7
        if artifact_type == "metric_cards":
            return 8
    if intent == "voc_analysis":
        if artifact_id == "artifact_external_source_repair_table":
            return 0
        if artifact_id.endswith("_table"):
            return 1
        if artifact_type == "report_block":
            return 2
        if artifact_type == "metric_cards":
            return 3
    if intent == "inventory_analysis":
        if artifact_id == "artifact_bom_entity_validation_table":
            return 0
        if artifact_id == "artifact_inventory_analysis_table":
            return 1
        if artifact_id.endswith("_table"):
            return 2
        if artifact_type == "report_block":
            return 3
        if artifact_type == "metric_cards":
            return 5
    if intent == "pricing_analysis":
        if artifact_id == "artifact_tco_validation_table":
            return 0
        if artifact_id == "artifact_pricing_corridor_chart":
            return 1
        if artifact_id == "artifact_pending_msrp_review_chart":
            return 2
        if artifact_id == "artifact_pricing_analysis_table":
            return 3
        if artifact_id == "artifact_pricing_market_structure_table":
            return 4
        if artifact_id == "artifact_pending_msrp_review_table":
            return 5
        if artifact_type == "metric_cards":
            return 6
        if artifact_id == "artifact_msrp_source_repair_table":
            return 7
        if artifact_id == "artifact_external_source_repair_table":
            return 7
        if artifact_type == "report_block":
            return 8
        if artifact_id == "artifact_pricing_analysis_framework_table":
            return 10
        if artifact_type == "chart":
            return 11
    if intent == "competitor_compare":
        if artifact_id == "artifact_market_structure_chart":
            return 0
        if artifact_id == "artifact_competitor_evidence_chart":
            return 1
        if artifact_id == "artifact_competitor_compare_table":
            return 2
        if artifact_id == "artifact_pricing_corridor_chart":
            return 3
        if artifact_id == "artifact_pending_msrp_review_chart":
            return 4
        if artifact_id == "artifact_pending_msrp_review_table":
            return 5
        if artifact_id == "artifact_msrp_source_repair_table":
            return 6
        if artifact_type == "report_block":
            return 7
        if artifact_type == "metric_cards":
            return 8
    if intent == "report_generation":
        if artifact_type == "report_block":
            return 0
        if artifact_id == "artifact_report_model_coverage_chart":
            return 1
        if artifact_id == "artifact_pricing_corridor_chart":
            return 1
        if artifact_id == "artifact_report_model_coverage_table":
            return 2
        if artifact_id == "artifact_report_pricing_table":
            return 3
        if artifact_id == "artifact_report_generation_table":
            return 4
        if artifact_type == "chart":
            return 5
        if artifact_type == "metric_cards":
            return 6
    if intent == "market_overview":
        if artifact_type == "chart":
            return 0
        if artifact_id == "artifact_powertrain_route_table":
            return 1
        if artifact_id == "artifact_market_overview_table":
            return 2
        if artifact_type == "report_block":
            return 3
        if artifact_type == "metric_cards":
            return 4
    if intent == "configuration_analysis":
        if artifact_id in {"artifact_market_structure_chart", "artifact_market_powertrain_mix_chart"}:
            return 0
        if artifact_id == "artifact_external_source_repair_table":
            return 1
        if artifact_id == "artifact_configuration_analysis_table":
            return 2
        if artifact_id == "artifact_configuration_competitor_context_table":
            return 3
        if artifact_type == "report_block":
            return 4
        if artifact_type == "metric_cards":
            return 5
    return 4


def _intent_chart_from_evidence_refs(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    *,
    question: str = "",
) -> VisualArtifact | None:
    intent = str(evidence_package.get("intent") or "")
    if intent == "market_overview":
        drive_mix_chart = _market_drive_mix_chart_from_refs(refs, question=question)
        if drive_mix_chart:
            return drive_mix_chart
        if _question_prioritizes_powertrain_mix(question):
            return _market_powertrain_mix_chart_from_refs(refs) or _market_structure_chart_from_refs(refs)
        return _market_structure_chart_from_refs(refs) or _market_powertrain_mix_chart_from_refs(refs)
    if intent == "pricing_analysis":
        return _pricing_corridor_chart_from_evidence_refs(evidence_package, refs)
    if intent == "competitor_compare":
        competitor_chart = _competitor_evidence_chart_from_refs(refs)
        if competitor_chart:
            return competitor_chart
        if _question_mentions_price_comparison(question):
            pricing_chart = _pricing_corridor_chart_from_evidence_refs(
                evidence_package,
                refs,
                allow_reference_sample=True,
            )
            if pricing_chart:
                return pricing_chart
        return _competitor_evidence_chart_from_refs(refs) or _market_structure_chart_from_refs(refs, include_percent=True)
    if intent == "configuration_analysis":
        return _market_structure_chart_from_refs(refs) or _market_powertrain_mix_chart_from_refs(refs)
    return None


def _question_prioritizes_powertrain_mix(question: str) -> bool:
    text = str(question or "").casefold()
    mentioned_fuels = sum(1 for token in ("bev", "phev", "hev", "mhev", "ice", "动力", "燃油", "插混", "混动", "纯电") if token in text)
    route_tokens = ("route", "powertrain", "mix", "路线", "动力路线", "主推", "优先", "适合推", "压缩", "替代")
    return mentioned_fuels >= 2 or (mentioned_fuels >= 1 and any(token in text for token in route_tokens))


def _question_prioritizes_drive_mix(question: str) -> bool:
    """Identify questions that require a 2WD versus 4WD evidence view."""
    text = str(question or "").casefold()
    mentions_two_wd = any(token in text for token in ("2wd", "两驱"))
    mentions_four_wd = any(token in text for token in ("4wd", "awd", "四驱"))
    return mentions_two_wd and mentions_four_wd


def _artifact_id_exists(artifacts: list[VisualArtifact], artifact_id: str) -> bool:
    return bool(artifact_id) and any(str(item.get("id") or "") == artifact_id for item in artifacts)


def _report_block_ref_ids(intent: str, ref_ids: list[str], display_refs: list[dict[str, Any]]) -> list[str]:
    if intent != "competitor_compare":
        return ref_ids
    result: list[str] = []
    seen: set[str] = set()
    for ref in display_refs:
        ref_id = str(ref.get("refId") or "").strip()
        if not ref_id or ref_id in seen:
            continue
        seen.add(ref_id)
        result.append(ref_id)
    return result


def _table_refs_for_intent(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    usable_refs: list[dict[str, Any]],
    *,
    question: str,
) -> list[dict[str, Any]]:
    intent = str(evidence_package.get("intent") or "")
    if intent == "voc_analysis":
        return _voc_table_refs(refs)
    if intent == "market_overview":
        return _market_overview_focus_refs(question, usable_refs)
    if intent in {"news_policy_search", "report_generation"}:
        if intent == "report_generation":
            return _report_generation_display_refs(evidence_package, question, refs)
        return refs
    return usable_refs


def _metric_refs_for_intent(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    *,
    question: str,
) -> list[dict[str, Any]]:
    intent = str(evidence_package.get("intent") or "")
    if intent == "pricing_analysis":
        pricing_refs = [ref for ref in refs if _is_pricing_metric_ref(ref)]
        requested_models = _pricing_relevant_model_labels(evidence_package, {}, question=question)
        if requested_models and _pricing_should_suppress_generic_price_stat_metric_cards(evidence_package):
            pricing_refs = [
                ref
                for ref in pricing_refs
                if not _pricing_metric_ref_is_generic_price_stat(ref)
            ]
        if requested_models and not _pricing_refs_have_chart_anchor(pricing_refs, requested_models):
            return [
                ref
                for ref in pricing_refs
                if not _pricing_metric_ref_is_generic_price_stat(ref)
            ]
        return pricing_refs
    if intent == "market_overview":
        return sorted(_market_overview_focus_refs(question, refs), key=_market_metric_ref_priority)
    if intent == "voc_analysis":
        return [ref for ref in refs if _is_voc_metric_ref(ref)]
    if intent == "news_policy_search":
        return [ref for ref in refs if _is_policy_metric_ref(ref)]
    if intent == "inventory_analysis":
        return [ref for ref in refs if _is_inventory_metric_ref(ref)]
    if intent == "configuration_analysis":
        return [ref for ref in refs if _is_configuration_metric_ref(ref)]
    if intent == "report_generation":
        return _report_generation_metric_refs(evidence_package, question, refs)
    return refs


def _pricing_metric_ref_is_generic_price_stat(ref: dict[str, Any]) -> bool:
    return str(ref.get("label") or "").lower() in {
        "pricestats.min",
        "pricestats.max",
        "pricestats.avg",
        "pricestats.median",
    }


def _pricing_should_suppress_generic_price_stat_metric_cards(evidence_package: dict[str, Any]) -> bool:
    missing = {str(name or "").casefold() for name in _missing_names(evidence_package)}
    return bool(missing & {
        "coverage_diagnostic:no_current_prices_for_requested_models",
        "no_current_prices_for_requested_models",
        "current_msrp",
        "own_model_price",
        "target_model_price",
    })


def _market_overview_focus_refs(question: str, refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    market_size_refs = [ref for ref in refs if _is_market_overview_size_ref(ref)]
    structural_refs = [ref for ref in refs if _is_market_structural_ref(ref)]
    cross_country_size_refs = [
        ref for ref in refs
        if _is_cross_country_size_question(question) and _is_cross_country_cumulative_sales_ref(ref)
    ]
    if market_size_refs or structural_refs or cross_country_size_refs:
        return _dedupe_refs_by_id(market_size_refs + cross_country_size_refs + structural_refs)
    focused = [ref for ref in refs if not _is_market_generic_snapshot_ref(ref)]
    return focused or refs


def _is_market_overview_size_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    if not any(token in source for token in ("jato_country_snapshot", "jato_country_chart_deck", "jato_filtered_query")):
        return False
    return label in {"cumulativesales", "marketsnapshot.kpis.cumulativesales", "results.kpis.cumulativesales"}


def _is_market_structural_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    if _is_user_method_material_ref(ref):
        return False
    haystack = f"{label} {source}"
    if _is_market_generic_snapshot_ref(ref):
        return False
    return any(
        token in haystack
        for token in (
            "powertrainmix",
            "drivebysegment",
            "drivebyfuel",
            "segmentbyfuel",
            "registrationbyfuel",
            "registrationbysegment",
            "crosscountry.",
            "topmodels.",
            "bev",
            "phev",
            "hev",
            "mhev",
            "reev",
            "ice",
            "suv",
            "segment",
        )
    )


def _is_user_method_material_ref(ref: dict[str, Any] | None) -> bool:
    if not isinstance(ref, dict):
        return False
    haystack = " ".join(
        str(ref.get(key) or "")
        for key in ("label", "source", "table")
    ).casefold()
    return (
        "business_method_material" in haystack
        or "user material" in haystack
        or "j7_hev_v4" in haystack
        or "j7_hev_method" in haystack
    )


def _scoped_visual_evidence_refs(
    refs: list[dict[str, Any]],
    evidence_package: dict[str, Any],
    *,
    question: str,
) -> list[dict[str, Any]]:
    return [
        ref
        for ref in refs
        if _visual_ref_matches_material_scope(ref, evidence_package, question=question)
    ]


def _visual_ref_matches_material_scope(
    ref: dict[str, Any],
    evidence_package: dict[str, Any],
    *,
    question: str = "",
) -> bool:
    if not _is_user_method_material_ref(ref):
        return True
    label = str(ref.get("label") or "")
    source = str(ref.get("source") or ref.get("table") or "")
    value = str(ref.get("value") or "")
    package_country = _visual_canonical_country(str(evidence_package.get("country") or ""))
    question_country = _visual_user_material_country_hint(question)
    target_country = package_country or question_country
    material_country = _visual_user_material_country_hint(" ".join([label, source, value]))
    source_key = _visual_model_key(source)
    if "j7hevv4" in source_key or "j7hevmethod" in source_key:
        material_country = "Sweden"
    if target_country and material_country and target_country != material_country:
        return False
    requested_models = _requested_competitor_visual_models(evidence_package, question=question)
    if not requested_models:
        return True
    material_model_key = _visual_model_key(" ".join([label, source]))
    if not material_model_key:
        return True
    return any(
        _visual_model_tokens_match(material_model_key, _visual_model_key(model))
        for model in requested_models
    )


def _visual_model_tokens_match(material_key: str, requested_key: str) -> bool:
    if not material_key or not requested_key:
        return False
    if material_key == requested_key:
        return True
    if len(requested_key) >= 2 and requested_key in material_key:
        return True
    if len(material_key) >= 3 and material_key in requested_key:
        return True
    return False


def _visual_canonical_country(value: str) -> str:
    token = str(value or "").strip().casefold()
    if not token:
        return ""
    mapping = {
        "sweden": "Sweden",
        "sverige": "Sweden",
        "se": "Sweden",
        "swe": "Sweden",
        "瑞典": "Sweden",
        "hungary": "Hungary",
        "magyarország": "Hungary",
        "hu": "Hungary",
        "匈牙利": "Hungary",
        "finland": "Finland",
        "fi": "Finland",
        "芬兰": "Finland",
        "norway": "Norway",
        "no": "Norway",
        "挪威": "Norway",
        "denmark": "Denmark",
        "dk": "Denmark",
        "丹麦": "Denmark",
        "germany": "Germany",
        "de": "Germany",
        "德国": "Germany",
    }
    return mapping.get(token, "")


def _visual_user_material_country_hint(value: str) -> str:
    text = str(value or "").casefold()
    negative_sweden_markers = ("不要回答瑞典", "不是瑞典", "非瑞典", "not sweden", "not about sweden", "do not answer sweden")
    if any(marker in text for marker in negative_sweden_markers):
        text = text.replace("瑞典", "").replace("sweden", "")
    for token, country in {
        "sweden": "Sweden",
        "sverige": "Sweden",
        "瑞典": "Sweden",
        "hungary": "Hungary",
        "magyarország": "Hungary",
        "匈牙利": "Hungary",
        "finland": "Finland",
        "芬兰": "Finland",
        "norway": "Norway",
        "挪威": "Norway",
        "denmark": "Denmark",
        "丹麦": "Denmark",
        "germany": "Germany",
        "德国": "Germany",
    }.items():
        if token in text:
            return country
    return ""


def _is_market_context_ref_for_competitor_table(ref: dict[str, Any]) -> bool:
    raw_label = str(ref.get("label") or "").strip()
    label = raw_label.casefold()
    if _is_direct_competitor_model_metric_label(raw_label):
        return False
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    haystack = f"{label} {source}"
    return any(
        token in haystack
        for token in (
            "contextsnapshot.crosstabs",
            "contextsnapshot.cross_tabs",
            "contextsnapshot.cross-tabs",
            "crosstabs.",
            "cross_tabs.",
            "cross-tabs.",
            "powertrainmix.",
            "marketsnapshot.",
            "country_snapshot",
            "jato_country_snapshot",
            "jato_country_chart_deck",
        )
    )


def _is_direct_competitor_model_metric_label(label: str) -> bool:
    text = str(label or "").strip()
    lower = text.casefold()
    if lower.startswith(("contextsnapshot.", "marketsnapshot.", "results.", "pricestats.", "crosscountry.")):
        return False
    metric = lower.split(".")[-1] if "." in lower else ""
    return metric in {
        "sales",
        "share",
        "volume",
        "count",
        "rank",
        "segment",
        "powertrain",
        "4wd_sales",
        "business_sales",
        "private_sales",
        "price",
        "msrp",
        "minprice",
        "maxprice",
        "avgprice",
        "priceevidencestatus",
        "sourcedraftpath",
        "candidatedomain",
        "candidatesourcetype",
    }


def _is_market_generic_snapshot_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    generic_tokens = (
        "avgmsrp",
        "totalrows",
        "countrycount",
        "brandcount",
        "modelcount",
        "versioncount",
        "yearseries",
        "metadata.",
        "result_count",
    )
    if any(token in label for token in generic_tokens):
        return True
    if "cumulativesales" in label:
        return not label.startswith("crosscountry.")
    return False


def _market_metric_ref_priority(ref: dict[str, Any]) -> tuple[int, int, str]:
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    haystack = f"{label} {source}"
    if "powertrainmix" in haystack or re.search(r"\b(?:bev|phev|hev|mhev|ice|reev)\b", label):
        bucket = 0
    elif any(token in haystack for token in ("drivebysegment", "segmentbyfuel", "suv", "segment")):
        bucket = 1
    elif any(token in haystack for token in ("registrationbyfuel", "registrationbysegment", "business", "private", "fleet")):
        bucket = 2
    elif "topmodels." in haystack:
        bucket = 3
    elif "cumulativesales" in label:
        bucket = 4
    else:
        bucket = 5
    return (bucket, _market_signal_sort_priority(_market_signal_from_ref(str(ref.get("label") or ""), ref)), label)


def _is_cross_country_size_question(question: str) -> bool:
    text = str(question or "").casefold()
    return any(token in text for token in ("cross-country", "cross country", "finland", "芬兰", "挪威", "丹麦", "北欧", "对比", "差异"))


def _is_cross_country_cumulative_sales_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    return label.startswith("crosscountry.") and label.endswith(".kpis.cumulativesales")


def _dedupe_refs_by_id(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        key = str(ref.get("refId") or ref.get("label") or id(ref))
        if key in seen:
            continue
        seen.add(key)
        result.append(ref)
    return result


def _is_pricing_metric_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    source = str(ref.get("source") or ref.get("table") or "").lower()
    haystack = f"{label} {source}"
    if any(token in source for token in ("jato_country_snapshot", "jato_country_chart_deck")):
        return False
    if label.endswith(".count") or "row_count" in label:
        return False
    return any(
        token in haystack
        for token in (
            "pricestats.",
            "msrp",
            "target price",
            "price delta",
            "relative price",
            "price corridor",
            "competitor corridor",
            "monthly",
            "leasing",
            "rv",
            "residual",
            "pva",
            "价差",
            "价格",
        )
    )


def _visual_refs_for_intent(
    intent: str,
    *,
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if intent == "pricing_analysis":
        return _pricing_visual_refs(evidence_package, question=question, refs=refs)
    if intent != "competitor_compare":
        return refs
    requested = _requested_competitor_visual_models(evidence_package, question=question)
    market_context_refs = _competitor_market_context_visual_refs(question, refs)
    price_context_refs = _competitor_price_context_visual_refs(question, refs)
    if not requested:
        filtered = [ref for ref in refs if not _is_competitor_model_only_ref(ref)]
        contextual = [*market_context_refs, *price_context_refs]
        return _dedupe_refs_by_id([*contextual, *filtered]) if contextual else filtered
    matched = [ref for ref in refs if _ref_mentions_any_requested_model(ref, requested)]
    matched_metrics = [ref for ref in matched if not _is_competitor_model_only_ref(ref)]
    contextual = [*market_context_refs, *price_context_refs]
    if contextual:
        return _dedupe_refs_by_id([*matched_metrics, *contextual])
    return matched_metrics


def _competitor_price_context_visual_refs(question: str, refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not _question_mentions_price_comparison(question):
        return []
    return [
        ref
        for ref in refs
        if _pricing_metric_ref_is_generic_price_stat(ref)
    ]


def _competitor_market_context_visual_refs(question: str, refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not _is_competitor_market_context_question(question):
        return []
    focused = [
        ref
        for ref in refs
        if _competitor_market_context_visual_priority(ref)[0] > 0
    ]
    return sorted(focused, key=_competitor_market_context_visual_priority, reverse=True)


def _is_competitor_market_context_question(question: str) -> bool:
    text = str(question or "").casefold()
    if "j8" in text and "sorento" in text:
        return True
    return any(token in text for token in ("7座", "7 座", "四驱", "4wd", "awd", "sorento")) and any(
        token in text
        for token in ("能打", "对标", "相比", "定位", "竞品", "compare", "competitor", "versus", "vs")
    )


def _competitor_market_context_visual_priority(ref: dict[str, Any]) -> tuple[int, str]:
    label = str(ref.get("label") or "")
    normalized = label.casefold()
    order = [
        ("drivebysegment.suv b.sales", 100),
        ("drivebysegment.suv b.4wd_pct", 98),
        ("drivebyfuel.phev.4wd_pct", 96),
        ("segmentbyfuel.suv b.phev_pct", 94),
        ("registrationbyfuel.phev.business_pct", 92),
        ("drivebysegment.suv a.sales", 80),
        ("drivebysegment.suv a.4wd_pct", 78),
        ("registrationbyfuel.phev.sales", 76),
        ("segmentbyfuel.suv a.sales", 74),
        ("segmentbyfuel.suv a.phev_pct", 72),
    ]
    for token, score in order:
        if token in normalized:
            return score, label
    return 0, label


def _pricing_visual_refs(
    evidence_package: dict[str, Any],
    *,
    question: str,
    refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    requested_models = _pricing_relevant_model_labels(evidence_package, {}, question=question)
    if not requested_models and _pricing_question_should_prioritize_tco(question, evidence_package):
        return []
    if not requested_models:
        return refs
    result: list[dict[str, Any]] = []
    for ref in refs:
        model = _pricing_record_model_from_ref(str(ref.get("label") or ""))
        if not model:
            result.append(ref)
            continue
        group = {
            "model": model,
            "source": str(ref.get("source") or ref.get("table") or ""),
        }
        if _pricing_record_is_relevant(group, requested_models):
            result.append(ref)
    return result


def _pricing_question_should_prioritize_tco(question: str, evidence_package: dict[str, Any]) -> bool:
    text = str(question or "").casefold()
    if not any(
        token in text
        for token in (
            "leasing",
            "lease",
            "tco",
            "company car",
            "fleet",
            "大客户",
            "公司车",
            "月供",
            "残值",
        )
    ):
        return False
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    if not any(
        isinstance(item, dict) and str(item.get("name") or "") == "leasing_tco_or_company_car_evidence"
        for item in missing
    ):
        return False
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    has_explicit_models = any(
        isinstance(entities.get(key), list) and any(str(item or "").strip() for item in entities.get(key, []))
        for key in ("models", "competitors")
    )
    return not has_explicit_models


def _requested_competitor_visual_models(evidence_package: dict[str, Any], *, question: str) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    result: list[str] = []
    for key in ("models", "competitors"):
        values = entities.get(key) if isinstance(entities.get(key), list) else []
        for value in values:
            text = str(value or "").strip()
            if text:
                result.append(text)
    result.extend(_model_mentions_from_question(question))
    return _dedupe_strings(result)


def _model_mentions_from_question(question: str) -> list[str]:
    text = str(question or "")
    patterns = [
        r"\b[A-Z][A-Za-z0-9.-]{1,12}\s+(?:HEV|PHEV|BEV|EV|SUV|Recharge|E-Tech|e-tron)\b",
        r"\b(?:OMODA\s?9|OMODA9|OMODA\s?5|OMODA5|JAECOO\s?J7|JAECOO\s?J8|J8|J7|O9|O5|EX30|EX40|EX60|EX90|XC40|XC60|XC90|RAV4|MODEL Y|Sportage|Sorento|EV3|EV9|Enyaq|ID\.4|ID\.7|Kodiaq|Tayron)\b",
    ]
    result: list[str] = []
    for pattern in patterns:
        for match in re.findall(pattern, text, flags=re.IGNORECASE):
            value = " ".join(str(match).strip().split())
            if value:
                result.append(value)
    return _dedupe_strings(result)


def _ref_mentions_any_requested_model(ref: dict[str, Any], models: list[str]) -> bool:
    haystack = _visual_model_key(
        " ".join(
            str(ref.get(key) or "")
            for key in ("label", "value", "source", "table")
        )
    )
    return bool(haystack) and any(
        _visual_model_key(model) and _visual_model_key(model) in haystack
        for model in models
    )


def _is_competitor_model_only_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").strip().lower()
    return label.startswith("competitor.") and label.endswith(".model")


def _visual_model_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _is_policy_metric_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    source = str(ref.get("source") or ref.get("table") or "").lower()
    if any(token in source for token in ("jato_country_snapshot", "jato_country_chart_deck")):
        return False
    return any(
        token in label
        for token in (
            "tax",
            "fee",
            "subsidy",
            "bonus",
            "malus",
            "benefit",
            "co2",
            "co₂",
            "emission",
            "pricecap",
            "price_cap",
            "price cap",
            "政策",
            "补贴",
            "税",
            "价格上限",
        )
    )


def _is_voc_metric_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    source = str(ref.get("source") or ref.get("table") or "").lower()
    return (
        "voc" in label
        or "complaint" in label
        or "sentiment" in label
        or "consumer" in label
        or "user_signal" in label
        or "voc" in source
    )


def _is_inventory_metric_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    source = str(ref.get("source") or ref.get("table") or "").lower()
    haystack = f"{label} {source}"
    if any(
        token in haystack
        for token in (
            "cumulativesales",
            "avgmsrp",
            "powertrainmix",
            "topmodels",
            "yearseries",
            "versioncount",
            "modelcount",
            "row_count",
            "result_count",
            "filtered rows",
            "results.kpis",
            "crosscountry.",
        )
    ):
        return False
    record_like = label.startswith((
        "inventory.records.",
        "material.records.",
        "bom.records.",
        "stock.records.",
        "order.records.",
    ))
    source_like = any(token in source for token in ("inventory", "stock", "bom", "material", "order"))
    if not record_like and not source_like:
        return False
    return any(
        token in haystack
        for token in (
            "availableunits",
            "available_units",
            "stock",
            "inventory",
            "bom",
            "material",
            "materialcode",
            "version",
            "variant",
            "lifecycle",
            "duplicate",
            "conflict",
        )
    )


def _is_configuration_metric_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    source = str(ref.get("source") or ref.get("table") or "").lower()
    haystack = f"{label} {source}"
    if _is_technical_count_label(label):
        return False
    if any(token in haystack for token in ("cumulativesales", "avgmsrp", "topmodels", "yearseries", "country_snapshot")):
        return False
    return any(
        token in haystack
        for token in (
            "configuration_delta",
            "variant_compare",
            "variant_diff",
            "engineering",
            "battery",
            "range",
            "charging",
            "charge",
            "heat",
            "winter",
            "tow",
            "roof",
            "seat",
            "camera",
            "hud",
            "adas",
            "trim",
            "feature",
            "equipment",
        )
    )


def _voc_table_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ref in refs:
        if _is_voc_noise_ref(ref):
            continue
        label = str(ref.get("label") or "").lower()
        source = str(ref.get("source") or ref.get("table") or "").lower()
        value = str(ref.get("value") or "").lower()
        haystack = f"{label} {source} {value}"
        is_source_backed = any(token in source for token in ("external", "web", "http", "review", "forum", "voc"))
        is_voc_like = any(
            token in haystack
            for token in (
                "voc",
                "user",
                "consumer",
                "review",
                "complaint",
                "claim",
                "summary",
                "topic",
                "sentiment",
                "forum",
                "media",
                "omoda",
                "jaecoo",
                "dealer",
                "service",
                "warranty",
                "winter",
                "range",
                "charging",
                "adas",
                "software",
                "roof",
                "tow",
            )
        )
        if is_source_backed or is_voc_like:
            rows.append(ref)
    return rows


def _chart_artifacts(charts: list[dict[str, Any]], ref_ids: list[str]) -> list[VisualArtifact]:
    artifacts: list[VisualArtifact] = []
    for index, chart in enumerate(charts[:3]):
        if not isinstance(chart, dict):
            continue
        chart_type = str(chart.get("chartType") or "chart").strip() or "chart"
        title = _localize_chart_title(str(chart.get("title") or f"Chart {index + 1}").strip())
        plotly_data = chart.get("data") if isinstance(chart.get("data"), list) else []
        plotly_layout = chart.get("layout") if isinstance(chart.get("layout"), dict) else {}
        if not plotly_data:
            continue
        spec: ChartSpec = {
            "chartType": _normalized_chart_type(chart_type),
            "xField": "x",
            "yField": "y",
            "data": _chart_rows_from_plotly(plotly_data),
            "plotlyData": plotly_data,
            "plotlyLayout": plotly_layout,
        }
        note = chart.get("note")
        if isinstance(note, str) and note.strip():
            spec["note"] = note.strip()
        artifact: VisualArtifact = {
            "id": str(chart.get("chartId") or f"artifact_chart_{index + 1}"),
            "type": "chart",
            "title": title,
            "subtitle": _chart_subtitle_for_title(title),
            "data": spec["data"],
            "spec": dict(spec),
            "sourceEvidenceRefs": ref_ids[:6],
        }
        artifacts.append(artifact)
    return artifacts


def _localize_chart_title(title: str) -> str:
    mapping = {
        "Powertrain Mix": "动力结构图",
        "Yearly Trend": "年度趋势图",
        "Top Models": "主销车型图",
        "Market Structure": "市场结构图",
        "Pricing hypothesis corridor chart": "定价假设走廊图",
        "Pricing evidence + hypothesis chart": "价格证据与假设图",
        "Pricing reference sample chart": "参考价格样本图",
        "Pricing corridor chart": "价格走廊图",
    }
    return mapping.get(str(title or "").strip(), str(title or "").strip())


def _chart_subtitle_for_title(title: str) -> str:
    if title == "动力结构图":
        return "按动力类型展示当前市场销量结构，用于判断产品路线和定价压力。"
    if title == "年度趋势图":
        return "按年份展示市场规模变化，用于判断趋势背景；不替代月度趋势。"
    if title == "主销车型图":
        return "展示主销车型证据，用于确定竞品池和机会入口。"
    if title == "市场结构图":
        return "展示市场结构证据，用于把规模、级别和动力路线转成业务判断。"
    return "由 Agent 工具链基于本轮证据生成。"


def _charts_for_intent(
    intent: str,
    charts: list[dict[str, Any]],
    *,
    question: str = "",
    evidence_package: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if intent == "configuration_analysis":
        return [chart for chart in charts if _is_configuration_chart(chart)]
    if intent == "news_policy_search":
        return [chart for chart in charts if _is_policy_relevant_chart(chart)]
    if intent == "voc_analysis":
        return [chart for chart in charts if _is_voc_relevant_chart(chart, question=question)]
    if intent == "pricing_analysis":
        return [
            chart
            for chart in charts
            if _is_pricing_relevant_chart(chart, question=question, evidence_package=evidence_package or {})
        ]
    return charts


def _is_pricing_relevant_chart(
    chart: dict[str, Any],
    *,
    question: str,
    evidence_package: dict[str, Any],
) -> bool:
    requested_models = _pricing_relevant_model_labels(evidence_package, {}, question=question)
    if not requested_models:
        return True
    haystack = _chart_search_text(chart)
    if _is_generic_top_model_chart(chart):
        return _text_mentions_any_model(haystack, requested_models)
    if _is_generic_pricing_snapshot_chart(chart):
        return _text_mentions_any_model(haystack, requested_models)
    return True


def _is_generic_top_model_chart(chart: dict[str, Any]) -> bool:
    haystack = _chart_search_text(chart)
    return any(token in haystack for token in ("top models", "top_model", "top ranking", "top_ranking", "ranking"))


def _is_generic_pricing_snapshot_chart(chart: dict[str, Any]) -> bool:
    haystack = _chart_search_text(chart)
    return any(token in haystack for token in ("price sample", "pricing sample", "current evidence snapshot", "snapshot"))


def _chart_search_text(chart: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("chartId", "title", "note", "chartType"):
        parts.append(str(chart.get(key) or ""))
    data = chart.get("data")
    if isinstance(data, list):
        for trace in data[:4]:
            if not isinstance(trace, dict):
                continue
            for key in ("name", "type"):
                parts.append(str(trace.get(key) or ""))
            for key in ("x", "y", "labels"):
                values = trace.get(key)
                if isinstance(values, list):
                    parts.extend(str(item or "") for item in values[:24])
    return " ".join(parts).casefold()


def _text_mentions_any_model(text: str, models: list[str]) -> bool:
    key = _visual_model_key(text)
    return bool(key) and any(
        _visual_model_key(model) and _visual_model_key(model) in key
        for model in models
    )


def _is_configuration_chart(chart: dict[str, Any]) -> bool:
    haystack = " ".join(
        str(chart.get(key) or "")
        for key in ("chartId", "title", "note", "chartType")
    ).lower()
    return any(
        token in haystack
        for token in (
            "configuration",
            "config",
            "battery",
            "range",
            "winter",
            "charging",
            "charge",
            "feature",
            "trim",
            "variant",
            "80kwh",
            "95kwh",
            "800v",
        )
    )


def _is_voc_relevant_chart(chart: dict[str, Any], *, question: str) -> bool:
    haystack = _chart_search_text(chart)
    if any(
        token in haystack
        for token in (
            "top models",
            "top_model",
            "top ranking",
            "top_ranking",
            "ranking",
            "powertrain",
            "powertrain_mix",
            "year trend",
            "year_trend",
            "yearly trend",
            "market structure",
        )
    ):
        return False
    if any(
        token in haystack
        for token in (
            "voc",
            "voice",
            "source",
            "review",
            "forum",
            "sentiment",
            "complaint",
            "theme",
            "user",
            "用户",
            "媒体",
            "论坛",
            "吐槽",
            "痛点",
            "主题",
            "情绪",
        )
    ):
        return True
    question_tokens = [
        token
        for token in ("v2h", "v2l", "winter", "冬季", "拖车", "roof", "车机", "售后")
        if token in str(question or "").casefold()
    ]
    return bool(question_tokens) and any(token in haystack for token in question_tokens)


def _is_policy_relevant_chart(chart: dict[str, Any]) -> bool:
    haystack = " ".join(
        str(chart.get(key) or "")
        for key in ("chartId", "title", "note", "chartType")
    ).lower()
    if any(token in haystack for token in ("top model", "top_models", "ranking", "yearly trend", "year_trend")):
        return False
    return any(
        token in haystack
        for token in (
            "policy",
            "tax",
            "benefit",
            "company",
            "fleet",
            "business",
            "private",
            "channel",
            "tco",
            "leasing",
            "co2",
            "co₂",
        )
    )


def _pricing_corridor_chart_from_evidence_refs(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    *,
    allow_reference_sample: bool = False,
) -> VisualArtifact | None:
    rows: list[dict[str, Any]] = []
    source_refs: list[str] = []
    model = _entity_label(evidence_package, fallback="Target model")
    requested_models = _pricing_relevant_model_labels(evidence_package, {}, question="")
    chart_refs = _dedupe_refs_by_id([*refs, *_reference_price_sample_refs_from_coverage(evidence_package)])
    has_chart_anchor = _pricing_refs_have_chart_anchor(chart_refs, requested_models)

    for ref in chart_refs:
        label = str(ref.get("label") or "").strip()
        lower = label.lower()
        if _pricing_chart_ref_is_noise(lower):
            continue
        row_label = _pricing_chart_label(label, model)
        if not row_label:
            continue
        if "competitor corridor" in lower or "price corridor" in lower:
            values = _number_values(ref.get("value"))
            if len(values) >= 2:
                prefix = f"{model} 用户材料竞品价格带" if _is_user_method_material_ref(ref) else "竞品价格带"
                _append_chart_row(rows, source_refs, label=f"{prefix}下沿", value=values[0], unit=str(ref.get("unit") or "EUR"), ref=ref, series="corridor")
                _append_chart_row(rows, source_refs, label=f"{prefix}上沿", value=values[1], unit=str(ref.get("unit") or "EUR"), ref=ref, series="corridor")
                continue
        value = _number_value(ref.get("value"))
        if value is None:
            continue
        unit = str(ref.get("unit") or "")
        _append_chart_row(rows, source_refs, label=row_label, value=value, unit=unit, ref=ref, series=_pricing_chart_series(lower))

    rows = _sort_pricing_chart_rows(_dedupe_chart_rows(rows))
    if len(rows) < 2:
        return None
    if requested_models and not has_chart_anchor and _pricing_rows_are_only_generic_stats(rows) and not allow_reference_sample:
        return None
    reference_sample_only = _pricing_rows_use_reference_sample_stats(rows)
    material_hypothesis = _pricing_refs_include_user_material_anchor(refs)
    verified_price_anchor = _pricing_refs_include_verified_price_anchor(refs, requested_models)
    mixed_verified_and_material = material_hypothesis and verified_price_anchor
    chart_title = (
        "参考价格样本图"
        if reference_sample_only
        else "价格证据与假设图"
        if mixed_verified_and_material
        else "定价假设走廊图"
        if material_hypothesis
        else "价格走廊图"
    )
    chart_subtitle = (
        "来自已物化 MSRP 记录的参考样本；需要补目标车型和核心竞品官方 MSRP 后才能当作竞品走廊。"
        if reference_sample_only
        else "同时展示已验证价格锚点和用户材料价格假设；官方证据和定位假设必须分开使用。"
        if mixed_verified_and_material
        else "用户材料价格假设图；使用前必须补官方 MSRP、竞品价格、月供和 RV。"
        if material_hypothesis
        else "由 MSRP、目标价和竞品价格走廊证据生成的价格图。"
    )
    chart_note = (
        "仅为参考样本；请用价格表区分目标价、用户材料价、官方 MSRP 缺口、月供和 RV。"
        if reference_sample_only
        else "混合证据：官方价格锚点可引用，用户材料点在 MSRP、leasing/RV 和配置证据完整前仍是假设。"
        if mixed_verified_and_material
        else "仅为用户材料假设，不是当前官方 MSRP 或已验证竞品价。"
        if material_hypothesis
        else "基于证据生成；月供、RV 和价值覆盖细节请看价格表。"
    )
    return {
        "id": "artifact_pricing_corridor_chart",
        "type": "chart",
        "title": chart_title,
        "subtitle": chart_subtitle,
        "data": rows[:10],
        "spec": {
            "chartType": "bar",
            "xField": "label",
            "yField": "value",
            "seriesField": "series",
            "data": rows[:10],
            "note": chart_note,
        },
        "sourceEvidenceRefs": source_refs[:6],
    }


def _question_mentions_price_comparison(question: str) -> bool:
    text = str(question or "").casefold()
    return any(
        token in text
        for token in (
            "price",
            "pricing",
            "msrp",
            "cheaper",
            "expensive",
            "monthly",
            "leasing",
            "rv",
            "定价",
            "价格",
            "售价",
            "便宜",
            "贵",
            "价差",
            "月供",
            "残值",
        )
    )


def _reference_price_sample_refs_from_coverage(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for tool_index, tool in enumerate(_tool_results(evidence_package), start=1):
        diagnostics = tool.get("coverageDiagnostics") if isinstance(tool.get("coverageDiagnostics"), dict) else {}
        sample = diagnostics.get("referencePriceSample") if isinstance(diagnostics.get("referencePriceSample"), dict) else {}
        stats = sample.get("priceStats") if isinstance(sample.get("priceStats"), dict) else {}
        if not stats:
            continue
        currency = str(stats.get("currency") or "").strip() or "EUR"
        for key in ("min", "max", "avg", "median"):
            value = stats.get(key)
            if _number_value(value) is None:
                continue
            refs.append({
                "refId": f"coverage_{tool_index}_priceStats_{key}",
                "label": f"priceStats.{key}",
                "value": value,
                "unit": currency,
                "source": "reference_price_sample",
            })
    return refs


def _pricing_refs_include_user_material_anchor(refs: list[dict[str, Any]]) -> bool:
    for ref in refs:
        if not _is_user_method_material_ref(ref):
            continue
        label = str(ref.get("label") or "").casefold()
        if "main trim msrp" in label or "competitor corridor" in label or "price corridor" in label:
            return True
    return False


def _pricing_refs_include_verified_price_anchor(refs: list[dict[str, Any]], requested_models: list[str]) -> bool:
    for ref in refs:
        if _is_user_method_material_ref(ref):
            continue
        label = str(ref.get("label") or "").strip()
        lower = label.casefold()
        if not label or lower.startswith("pricestats."):
            continue
        source = " ".join(str(ref.get(key) or "") for key in ("source", "table")).casefold()
        if "user_question" in source:
            continue
        from_verified_source = any(token in source for token in ("pricing", "current_price", "jato_msrp", "postgres"))
        has_price_metric = any(token in lower for token in ("pricing.records.", ".msrp", ".price", "current msrp", "own-model msrp", "premium msrp"))
        if not from_verified_source and not lower.startswith("pricing.records."):
            continue
        if not has_price_metric:
            continue
        model = _pricing_record_model_from_ref(label)
        if model and requested_models and not _pricing_model_is_relevant(model, requested_models):
            continue
        return True
    return False


def _sort_pricing_chart_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(indexed_row: tuple[int, dict[str, Any]]) -> tuple[int, int, int, str]:
        index, row = indexed_row
        label = str(row.get("label") or "").casefold()
        series = str(row.get("series") or "").casefold()
        if "user-material" in label or "用户材料主销价假设" in label or series == "own model":
            return (0, 0, index, label)
        if series == "target":
            return (1, 0, index, label)
        if "corridor low" in label or "价格带下沿" in label:
            return (2, 0, index, label)
        if "corridor high" in label or "价格带上沿" in label:
            return (2, 1, index, label)
        if series == "corridor":
            return (2, 2, index, label)
        if series == "competitor":
            return (3, 0, index, label)
        if series == "reference sample":
            stat_order = 4
            if "min" in label:
                stat_order = 0
            elif "max" in label:
                stat_order = 1
            elif "avg" in label:
                stat_order = 2
            elif "median" in label:
                stat_order = 3
            return (4, stat_order, index, label)
        return (5, 0, index, label)

    return [row for _, row in sorted(enumerate(rows), key=sort_key)]


def _pricing_refs_have_chart_anchor(refs: list[dict[str, Any]], requested_models: list[str]) -> bool:
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        lower = label.lower()
        if _pricing_chart_ref_is_noise(lower):
            continue
        if any(token in lower for token in ("target price", "main trim msrp", "own-model msrp", "current msrp", "premium msrp")):
            return True
        if "competitor corridor" in lower or "price corridor" in lower:
            return True
        model = _pricing_record_model_from_ref(label)
        if model and (not requested_models or _pricing_model_is_relevant(model, requested_models)):
            return True
    return False


def _pricing_rows_are_only_generic_stats(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    allowed = {
        "Corridor min",
        "Corridor max",
        "Corridor avg",
        "Corridor median",
        "Reference sample min",
        "Reference sample max",
        "Reference sample avg",
        "Reference sample median",
        "参考样本下沿",
        "参考样本上沿",
        "参考样本均值",
        "参考样本中位数",
    }
    return all(str(row.get("label") or "") in allowed for row in rows)


def _pricing_rows_use_reference_sample_stats(rows: list[dict[str, Any]]) -> bool:
    labels = {str(row.get("label") or "") for row in rows}
    if not labels:
        return False
    has_reference_sample = any(label.startswith("Reference sample ") or label.startswith("参考样本") for label in labels)
    has_explicit_corridor = any(label.startswith("Competitor corridor ") or label.startswith("竞品价格带") for label in labels)
    has_non_sample_anchor = any(
        str(row.get("series") or "").casefold() in {"target", "own model", "competitor"}
        for row in rows
    )
    return has_reference_sample and not has_explicit_corridor and not has_non_sample_anchor


_POWERTRAIN_ORDER = ("BEV", "PHEV", "HEV", "MHEV", "ICE", "REEV")


def _market_drive_mix_chart_from_refs(
    refs: list[dict[str, Any]],
    *,
    question: str,
) -> VisualArtifact | None:
    """Build the requested drivetrain comparison from market cross-tab evidence.

    This does not infer a vehicle's drivetrain strategy. It only visualises
    the fuel-specific 2WD/4WD mix returned by the executed market tool.
    """
    if not _question_prioritizes_drive_mix(question):
        return None

    question_text = str(question or "").casefold()
    requested_fuel = "PHEV" if "phev" in question_text else "HEV" if "hev" in question_text or "混动" in question_text else ""
    grouped: dict[str, list[tuple[str, float, str, str]]] = {}
    pattern = re.compile(
        r"(?:contextSnapshot\.)?crossTabs\.driveByFuel\.([^.]+)\.(2WD|4WD|AWD)_pct$",
        flags=re.IGNORECASE,
    )
    for ref in refs:
        match = pattern.match(str(ref.get("label") or "").strip())
        if not match:
            continue
        fuel = match.group(1).upper()
        value = _number_value(ref.get("value"))
        if value is None:
            continue
        drive = match.group(2).upper()
        if drive == "AWD":
            drive = "4WD"
        grouped.setdefault(fuel, []).append((drive, value, str(ref.get("unit") or "%"), str(ref.get("refId") or "")))

    if requested_fuel and requested_fuel in grouped:
        fuel = requested_fuel
    elif len(grouped) == 1:
        fuel = next(iter(grouped))
    else:
        return None

    drive_rows = grouped[fuel]
    rows: list[dict[str, Any]] = []
    source_refs: list[str] = []
    seen_drives: set[str] = set()
    for drive, value, unit, ref_id in sorted(drive_rows, key=lambda item: (item[0] != "2WD", item[0])):
        if drive in seen_drives:
            continue
        seen_drives.add(drive)
        rows.append({
            "label": drive,
            "value": value,
            "unit": _chart_unit_label(unit),
            "series": fuel,
        })
        if ref_id:
            source_refs.append(ref_id)
    if len(rows) < 2:
        return None
    return {
        "id": "artifact_market_drive_mix_chart",
        "type": "chart",
        "title": f"{fuel} 2WD/4WD 市场结构",
        "subtitle": "来自本轮市场交叉表，直接对应两驱与四驱主销选择。",
        "data": rows,
        "spec": {
            "chartType": "bar",
            "xField": "label",
            "yField": "value",
            "seriesField": "series",
            "data": rows,
            "note": "仅展示已查到的市场驱动形式占比；最终版型配比仍需要车型价格、配置和渠道证据。",
        },
        "sourceEvidenceRefs": source_refs[:4],
    }


def _market_powertrain_mix_chart_from_refs(refs: list[dict[str, Any]]) -> VisualArtifact | None:
    grouped: dict[str, dict[str, Any]] = {}
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        powertrain, metric = _market_powertrain_metric_from_label(label)
        if not powertrain or metric not in {"sales", "share"}:
            continue
        value = _number_value(ref.get("value"))
        if value is None:
            continue
        group = grouped.setdefault(powertrain, {"powertrain": powertrain})
        if metric not in group:
            group[metric] = value
            group[f"{metric}Unit"] = str(ref.get("unit") or ("%" if metric == "share" else "units"))
            group[f"{metric}Ref"] = str(ref.get("refId") or "")

    metric_key = "sales" if sum(1 for item in grouped.values() if "sales" in item) >= 2 else "share"
    rows: list[dict[str, Any]] = []
    source_refs: list[str] = []
    for powertrain in _POWERTRAIN_ORDER:
        group = grouped.get(powertrain)
        if not group or metric_key not in group:
            continue
        ref_id = str(group.get(f"{metric_key}Ref") or "")
        if ref_id and ref_id not in source_refs:
            source_refs.append(ref_id)
        rows.append({
            "label": powertrain,
            "value": group[metric_key],
            "unit": _chart_unit_label(str(group.get(f"{metric_key}Unit") or "")),
            "series": "powertrain sales" if metric_key == "sales" else "powertrain share",
        })
    if len(rows) < 2:
        return None
    return {
        "id": "artifact_market_powertrain_mix_chart",
        "type": "chart",
        "title": "Powertrain mix chart",
        "subtitle": "Evidence-derived chart comparing BEV, PHEV, HEV and adjacent powertrain scale.",
        "data": rows,
        "spec": {
            "chartType": "bar",
            "xField": "label",
            "yField": "value",
            "seriesField": "series",
            "data": rows,
            "note": "Use this chart to judge whether BEV pressure is replacing, segmenting, or coexisting with HEV/PHEV demand.",
        },
        "sourceEvidenceRefs": source_refs[:6],
    }


def _market_structure_chart_from_refs(refs: list[dict[str, Any]], *, include_percent: bool = False) -> VisualArtifact | None:
    rows: list[dict[str, Any]] = []
    source_refs: list[str] = []
    seen: set[tuple[str, str]] = set()
    for ref in refs:
        parsed = _direct_competitor_chart_metric_from_ref(ref) if include_percent else None
        if parsed is None:
            parsed = _market_cross_tab_sales_metric_from_ref(ref, include_percent=include_percent)
        if parsed is None:
            continue
        group, label, value, unit = parsed
        key = (group.lower(), label.lower())
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "label": label,
            "value": value,
            "unit": _chart_unit_label(unit),
            "series": group,
        })
        ref_id = str(ref.get("refId") or "").strip()
        if ref_id and ref_id not in source_refs:
            source_refs.append(ref_id)

    rows = _sort_market_structure_chart_rows(rows)
    if len(rows) < 2:
        return None
    return {
        "id": "artifact_market_structure_chart",
        "type": "chart",
        "title": "市场结构图",
        "subtitle": "来自市场快照的交叉表信号，用于判断级别、动力和渠道场景。",
        "data": rows[:8],
        "spec": {
            "chartType": "bar",
            "xField": "label",
            "yField": "value",
            "seriesField": "series",
            "data": rows[:8],
            "note": "交叉表柱状图按维度展示有证据支撑的市场信号，不代表可相加的市场总量。",
        },
        "sourceEvidenceRefs": source_refs[:6],
    }


def _direct_competitor_chart_metric_from_ref(ref: dict[str, Any]) -> tuple[str, str, float, str] | None:
    label = str(ref.get("label") or "").strip()
    if not _is_direct_competitor_model_metric_label(label):
        return None
    model = _competitor_model_from_ref(label, ref.get("value"))
    if not model or _looks_like_non_model_competitor_group(model):
        return None
    value = _number_value(ref.get("value"))
    if value is None or value <= 0:
        return None
    metric = label.lower().split(".")[-1]
    unit = str(ref.get("unit") or "")
    if metric in {"sales", "value", "volume", "count"}:
        return "车型销量", f"{model} 销量", value, unit or "units"
    if metric == "4wd_sales":
        return "车型销量", f"{model} 4WD", value, unit or "units"
    if metric == "business_sales":
        return "车型销量", f"{model} 公司车", value, unit or "units"
    if metric == "private_sales":
        return "车型销量", f"{model} 私人", value, unit or "units"
    if metric == "share":
        return "车型占比", f"{model} share", value, unit or "%"
    return None


def _market_cross_tab_sales_metric_from_ref(ref: dict[str, Any], *, include_percent: bool = False) -> tuple[str, str, float, str] | None:
    label = str(ref.get("label") or "").strip()
    parts = [part.strip() for part in re.split(r"[.>/|]", label) if part.strip()]
    lowered = [part.lower() for part in parts]
    if "crosstabs" not in lowered:
        return None
    metric = parts[-1].lower() if parts else ""
    metric_kind = _market_cross_tab_metric_kind(metric)
    if not metric_kind:
        return None
    if metric_kind == "percentage" and not include_percent:
        return None
    value = _number_value(ref.get("value"))
    if value is None:
        return None
    cross_tab_index = lowered.index("crosstabs")
    dimension_key = parts[cross_tab_index + 1] if len(parts) > cross_tab_index + 1 else ""
    signal_parts = parts[cross_tab_index + 2 : -1]
    signal = _market_cross_tab_signal_label(signal_parts, metric, metric_kind)
    if not signal:
        return None
    group = _market_cross_tab_group_label(dimension_key, metric_kind=metric_kind)
    unit = str(ref.get("unit") or "units")
    return group, signal, value, unit


def _market_cross_tab_metric_kind(metric: str) -> str:
    normalized = str(metric or "").strip().lower()
    if normalized in {"sales", "volume", "registrations", "registration", "value", "count", "_total"}:
        return "sales"
    if normalized in {"share", "mix", "penetration"} or normalized.endswith("_pct") or normalized.endswith("_share"):
        return "percentage"
    return ""


def _market_cross_tab_signal_label(signal_parts: list[str], metric: str, metric_kind: str) -> str:
    signal = " ".join(part.replace("_", " ").strip() for part in signal_parts if part.strip())
    if metric_kind != "percentage":
        return signal
    metric_label = _market_cross_tab_metric_display_label(metric)
    if metric_label and metric_label not in {"share", "mix", "penetration"}:
        return f"{signal} {metric_label}".strip()
    return signal


def _market_cross_tab_metric_display_label(metric: str) -> str:
    text = str(metric or "").replace("_pct", "").replace("_share", "").replace("_", " ").strip()
    normalized = text.lower()
    upper_tokens = {"bev", "phev", "hev", "mhev", "ice", "awd", "4wd", "2wd"}
    if normalized in upper_tokens:
        return normalized.upper()
    title_tokens = {"business", "private", "retail"}
    if normalized in title_tokens:
        return normalized.title()
    return text


def _market_cross_tab_group_label(dimension_key: str, *, metric_kind: str = "sales") -> str:
    normalized = re.sub(r"[^a-z0-9]+", "", str(dimension_key or "").lower())
    suffix = "销量" if metric_kind == "sales" else "占比"
    if "segment" in normalized or "body" in normalized:
        return f"级别{suffix}"
    if "fuel" in normalized or "powertrain" in normalized or "drive" in normalized:
        return f"动力{suffix}"
    if "registration" in normalized or "channel" in normalized or "business" in normalized:
        return f"渠道{suffix}"
    return f"市场{suffix}"


def _sort_market_structure_chart_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    group_priority = {
        "车型销量": 0,
        "车型占比": 1,
        "动力销量": 2,
        "级别销量": 3,
        "渠道销量": 4,
        "市场销量": 5,
        "动力占比": 6,
        "级别占比": 7,
        "渠道占比": 8,
        "市场占比": 9,
    }
    return sorted(
        rows,
        key=lambda row: (
            group_priority.get(str(row.get("series") or ""), 99),
            _direct_competitor_chart_label_priority(str(row.get("label") or ""), str(row.get("series") or "")),
            _market_signal_sort_priority(str(row.get("label") or "")),
            -float(row.get("value") or 0),
            str(row.get("label") or ""),
        ),
    )


def _direct_competitor_chart_label_priority(label: str, series: str) -> int:
    if str(series or "") not in {"车型销量", "车型占比"}:
        return 0
    text = str(label or "").casefold()
    if "销量" in text or text.endswith(" sales"):
        return 0
    if "4wd" in text or "awd" in text:
        return 1
    if "business" in text or "公司车" in text:
        return 2
    if "private" in text or "私人" in text:
        return 3
    return 4


def _market_powertrain_metric_from_label(label: str) -> tuple[str, str]:
    parts = [part.strip() for part in re.split(r"[.>/|]", str(label or "")) if part.strip()]
    if not parts:
        return "", ""
    powertrain = ""
    for part in parts:
        upper = part.upper()
        if upper in _POWERTRAIN_ORDER:
            powertrain = upper
            break
    if not powertrain and parts[0].upper() in _POWERTRAIN_ORDER:
        powertrain = parts[0].upper()
    if not powertrain:
        return "", ""
    metric = parts[-1].lower()
    if metric in {"sales", "volume", "registrations", "registration", "value", "count", "_total"}:
        return powertrain, "sales"
    if metric in {"share", "mix", "penetration"} or metric.endswith("_pct"):
        return powertrain, "share"
    return "", ""


def _powertrain_route_table_artifact(question: str, refs: list[dict[str, Any]]) -> VisualArtifact | None:
    if not _is_powertrain_route_artifact_question(question):
        return None
    stats = _powertrain_route_stats(refs)
    requested = _powertrain_route_requested_fuels(question)
    if len(requested) < 2:
        requested = ["HEV", "PHEV"]
    if not any(stats.get(fuel) for fuel in requested):
        return None

    columns = [
        "powertrain",
        "sales",
        "share",
        "twoWheelDrive",
        "fourWheelDrive",
        "routeRole",
        "productAction",
    ]
    rows = [_powertrain_route_row(fuel, stats.get(fuel, {})) for fuel in requested]
    source_refs = _dedupe_strings([
        str(ref_id)
        for fuel in requested
        for ref_id in stats.get(fuel, {}).get("sourceRefs", [])
        if str(ref_id).strip()
    ])
    return {
        "id": "artifact_powertrain_route_table",
        "type": "table",
        "title": "HEV / PHEV route comparison table",
        "subtitle": "Evidence-backed route table for deciding whether HEV should be the low-risk main line and PHEV the TCO/company-car validation line.",
        "data": {
            "rows": _display_table_rows(rows, columns, max_columns=len(columns)),
        },
        "spec": {
            "columns": columns,
            "maxRows": len(rows),
            "sortBy": "powertrain",
            "businessExplanation": "This table consolidates powertrain evidence into product-route decisions instead of scattering HEV/PHEV metrics across generic evidence rows.",
            "evidenceMode": "powertrain_route_comparison",
        },
        "sourceEvidenceRefs": source_refs[:10],
    }


def _is_powertrain_route_artifact_question(question: str) -> bool:
    text = str(question or "").casefold()
    fuels = _powertrain_route_requested_fuels(question)
    if len(fuels) < 2:
        return False
    return any(
        token in text
        for token in (
            "还是",
            "or",
            "vs",
            "versus",
            "对比",
            "相比",
            "适合",
            "优先",
            "主推",
            "推",
            "路线",
            "route",
            "which",
        )
    )


def _powertrain_route_requested_fuels(question: str) -> list[str]:
    text = str(question or "").casefold()
    result: list[str] = []
    for fuel in _POWERTRAIN_ORDER:
        if re.search(rf"(?<![a-z0-9]){fuel.casefold()}(?![a-z0-9])", text):
            result.append(fuel)
    if "插混" in text and "PHEV" not in result:
        result.append("PHEV")
    if "混动" in text and "HEV" not in result:
        result.append("HEV")
    if "纯电" in text and "BEV" not in result:
        result.append("BEV")
    return _dedupe_strings(result)


def _powertrain_route_stats(refs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    for ref in refs:
        label = str(ref.get("label") or "")
        fuel, metric = _powertrain_route_metric_from_label(label)
        if fuel not in _POWERTRAIN_ORDER or not metric:
            continue
        formatted = _format_powertrain_route_value(ref)
        if not formatted:
            continue
        bucket = stats.setdefault(fuel, {"sourceRefs": []})
        if metric not in bucket or _route_ref_preferred(ref, bucket.get(f"{metric}Source", "")):
            bucket[metric] = formatted
            bucket[f"{metric}Source"] = str(ref.get("source") or ref.get("table") or "")
        ref_id = str(ref.get("refId") or "").strip()
        if ref_id and ref_id not in bucket["sourceRefs"]:
            bucket["sourceRefs"].append(ref_id)
    return stats


def _format_powertrain_route_value(ref: dict[str, Any]) -> str:
    formatted = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
    if str(ref.get("unit") or "").strip() == "%":
        return formatted.replace(" %", "%")
    return formatted


def _powertrain_route_metric_from_label(label: str) -> tuple[str, str]:
    parts = [part.strip() for part in re.split(r"[.>/|]", str(label or "")) if part.strip()]
    fuel = next((part.upper() for part in parts if part.upper() in _POWERTRAIN_ORDER), "")
    if not fuel:
        return "", ""
    lower = str(label or "").casefold()
    metric = str(parts[-1] if parts else "").casefold()
    if metric in {"sales", "volume", "registrations", "registration", "value", "count", "_total"}:
        return fuel, "sales"
    if metric in {"share", "mix", "penetration"}:
        return fuel, "share"
    if "business_pct" in lower or "business" in lower or "fleet" in lower or "company" in lower:
        return fuel, "business"
    if "private_pct" in lower or "retail_pct" in lower or "private" in lower or "retail" in lower:
        return fuel, "private"
    if "2wd_pct" in lower or "2wd" in lower:
        return fuel, "twoWheelDrive"
    if "4wd_pct" in lower or "4wd" in lower or "awd" in lower:
        return fuel, "fourWheelDrive"
    return "", ""


def _route_ref_preferred(ref: dict[str, Any], current_source: str) -> bool:
    current = str(current_source or "").casefold()
    candidate = str(ref.get("source") or ref.get("table") or "").casefold()
    if "jato_segment_breakdown" in candidate and "jato_segment_breakdown" not in current:
        return True
    return False


def _powertrain_route_row(fuel: str, stats: dict[str, Any]) -> dict[str, Any]:
    role, action = _powertrain_route_role_action(fuel)
    return {
        "powertrain": fuel,
        "sales": stats.get("sales") or "待补",
        "share": stats.get("share") or "待补",
        "twoWheelDrive": stats.get("twoWheelDrive") or "待补",
        "fourWheelDrive": stats.get("fourWheelDrive") or "待补",
        "routeRole": role,
        "productAction": action,
    }


def _powertrain_route_role_action(fuel: str) -> tuple[str, str]:
    if fuel == "HEV":
        return (
            "低风险主线",
            "验证价格敏感、无稳定充电和低使用风险场景；继续补车型级价格/竞品池。",
        )
    if fuel == "PHEV":
        return (
            "公司车/TCO 验证线",
            "补月供、残值/RV、税费 benefit、里程和充电条件后再决定是否主推。",
        )
    if fuel == "BEV":
        return (
            "政策/公司车压力源",
            "验证补贴、TCO、续航/冬季包和主销价格带。",
        )
    return (
        "待判定路线",
        "补销量、份额、渠道和车型级价格配置证据。",
    )


def _pricing_chart_ref_is_noise(lower_label: str) -> bool:
    return any(
        token in lower_label
        for token in (
            "monthly",
            "leasing",
            "leasepayment",
            "residual",
            "rv",
            "pva",
            "coverage",
            "gap",
            ".powertrain",
            ".fuel",
        )
    )


def _pricing_chart_label(label: str, model: str) -> str:
    lower = label.lower()
    if "target price midpoint" in lower:
        return f"{model} target midpoint"
    if "target price min" in lower:
        return ""
    if "target price max" in lower:
        return ""
    if "user material" in lower and "main trim msrp" in lower:
        return f"{model} 用户材料主销价假设"
    if any(token in lower for token in ("main trim msrp", "own-model msrp", "current msrp", "premium msrp")):
        return model
    if lower == "pricestats.min":
        return "参考样本下沿"
    if lower == "pricestats.max":
        return "参考样本上沿"
    if lower == "pricestats.avg":
        return "参考样本均值"
    if lower == "pricestats.median":
        return "参考样本中位数"
    if lower.startswith("pricing.records."):
        parts = label.split(".")
        if len(parts) >= 4:
            record_model = ".".join(parts[2:-1]).strip()
            metric = parts[-1].strip()
            if metric.lower() in {"msrp", "price", "avgprice", "medianprice"}:
                return record_model
            if metric.lower() == "minprice":
                return f"{record_model} min"
            if metric.lower() == "maxprice":
                return f"{record_model} max"
    parts = label.split(".")
    if len(parts) >= 2:
        metric = parts[-1].strip().lower()
        record_model = ".".join(parts[:-1]).strip()
        if record_model and metric in {"msrp", "price", "avgprice", "medianprice"}:
            return record_model
        if record_model and metric == "minprice":
            return f"{record_model} min"
        if record_model and metric == "maxprice":
            return f"{record_model} max"
    if "competitor corridor" in lower or "price corridor" in lower:
        return "Competitor corridor"
    return ""


def _pricing_chart_series(lower_label: str) -> str:
    if "target price" in lower_label:
        return "target"
    if "pricestats" in lower_label:
        return "reference sample"
    if "corridor" in lower_label:
        return "corridor"
    if "pricing.records" in lower_label:
        return "competitor"
    return "own model"


def _competitor_evidence_chart_from_refs(refs: list[dict[str, Any]]) -> VisualArtifact | None:
    groups: dict[str, dict[str, Any]] = {}
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        if _is_market_context_ref_for_competitor_table(ref):
            continue
        model = _competitor_model_from_ref(label, ref.get("value"))
        if not model or _looks_like_non_model_competitor_group(model):
            continue
        value = _number_value(ref.get("value"))
        if value is None:
            continue
        metric = label.lower().split(".")[-1]
        group = groups.setdefault(model, {"model": model})
        if metric in {"sales", "value", "volume", "count"} and "sales" not in group:
            group.update({"sales": value, "salesRef": str(ref.get("refId") or ""), "salesUnit": str(ref.get("unit") or "units")})
        elif metric == "share" and "share" not in group:
            group.update({"share": value, "shareRef": str(ref.get("refId") or ""), "shareUnit": str(ref.get("unit") or "%")})
        elif metric in {"avgprice", "price", "msrp", "minprice", "maxprice"} and "price" not in group:
            group.update({"price": value, "priceRef": str(ref.get("refId") or ""), "priceUnit": str(ref.get("unit") or "EUR")})

    metric_key, metric_label = _best_competitor_chart_metric(groups)
    if not metric_key:
        return None
    rows: list[dict[str, Any]] = []
    source_refs: list[str] = []
    for group in groups.values():
        value = group.get(metric_key)
        if not isinstance(value, (int, float)):
            continue
        ref_id = str(group.get(f"{metric_key}Ref") or "")
        if ref_id and ref_id not in source_refs:
            source_refs.append(ref_id)
        rows.append({
            "label": str(group.get("model") or ""),
            "value": value,
            "unit": str(group.get(f"{metric_key}Unit") or ""),
            "series": metric_label,
        })
    rows = sorted(rows, key=lambda row: float(row.get("value") or 0), reverse=True)
    if len(rows) < 2:
        return None
    return {
        "id": "artifact_competitor_evidence_chart",
        "type": "chart",
        "title": f"Competitor {metric_label} chart",
        "subtitle": "Bar chart generated from competitor evidence refs.",
        "data": rows[:10],
        "spec": {
            "chartType": "bar",
            "xField": "label",
            "yField": "value",
            "seriesField": "series",
            "data": rows[:10],
            "note": "Evidence-derived chart; use the competitor table for segment, powertrain and implication details.",
        },
        "sourceEvidenceRefs": source_refs[:6],
    }


def _best_competitor_chart_metric(groups: dict[str, dict[str, Any]]) -> tuple[str, str]:
    candidates = (("sales", "sales"), ("share", "share"), ("price", "price"))
    for key, label in candidates:
        count = sum(1 for group in groups.values() if isinstance(group.get(key), (int, float)))
        if count >= 2:
            return key, label
    return "", ""


def _append_chart_row(
    rows: list[dict[str, Any]],
    source_refs: list[str],
    *,
    label: str,
    value: float,
    unit: str,
    ref: dict[str, Any],
    series: str,
) -> None:
    rows.append({"label": label, "value": value, "unit": _chart_unit_label(unit), "series": series})
    ref_id = str(ref.get("refId") or "").strip()
    if ref_id and ref_id not in source_refs:
        source_refs.append(ref_id)


def _chart_unit_label(unit: str) -> str:
    text = str(unit or "").strip()
    if text.lower() == "currency":
        return "EUR"
    return text


def _dedupe_chart_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row.get("label") or ""), str(row.get("series") or ""))
        if not key[0] or key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _trend_chart_from_evidence_refs(refs: list[dict[str, Any]]) -> VisualArtifact | None:
    rows: list[dict[str, Any]] = []
    source_ref_ids: list[str] = []
    for ref in refs:
        label = str(ref.get("label") or "")
        lower = label.lower()
        if "monthseries" not in lower and "yearseries" not in lower:
            continue
        value = _number_value(ref.get("value"))
        if value is None:
            continue
        metric = _trend_metric_from_label(label)
        period = _trend_period_from_label(label)
        ref_id = str(ref.get("refId") or "").strip()
        rows.append({"series": metric, "x": period, "y": value})
        if ref_id:
            source_ref_ids.append(ref_id)
    if len(rows) < 2:
        return None
    return {
        "id": "artifact_trend_series_chart",
        "type": "chart",
        "title": "Trend series",
        "subtitle": "Line chart generated from year/month evidence refs.",
        "data": rows[:48],
        "spec": {
            "chartType": "line",
            "xField": "x",
            "yField": "y",
            "seriesField": "series",
            "data": rows[:48],
        },
        "sourceEvidenceRefs": source_ref_ids[:6],
    }


def _trend_metric_from_label(label: str) -> str:
    parts = [part for part in str(label or "").split(".") if part]
    if not parts:
        return "trend"
    metric = parts[-1]
    if metric.lower() == "value" and len(parts) >= 2:
        parent = parts[-2]
        if "monthseries" not in parent.lower() and "yearseries" not in parent.lower():
            metric = parent
    return metric


def _trend_period_from_label(label: str) -> str:
    parts = [part for part in str(label or "").split(".") if part]
    for part in parts:
        if re.search(r"\d{4}(?:-\d{1,2})?$", part):
            return part
    for part in parts:
        if "monthseries" in part.lower() or "yearseries" in part.lower():
            continue
        if re.search(r"series_\d+$", part.lower()):
            return part
    return parts[-2] if len(parts) >= 2 else "period"


def _snapshot_fallback_chart(refs: list[dict[str, Any]]) -> VisualArtifact | None:
    rows = _metric_card_rows(refs)
    if not rows:
        return None
    chart_rows = [
        {
            "label": str(row.get("label") or ""),
            "value": row.get("value"),
            "unit": str(row.get("unit") or ""),
        }
        for row in rows[:6]
        if isinstance(row.get("value"), (int, float))
    ]
    if not chart_rows:
        return None
    return {
        "id": "artifact_snapshot_fallback_chart",
        "type": "chart",
        "title": "Current evidence snapshot",
        "subtitle": "Trend series unavailable; showing current evidence snapshot instead.",
        "data": chart_rows,
        "spec": {
            "chartType": "bar",
            "xField": "label",
            "yField": "value",
            "data": chart_rows,
            "note": "Trend series unavailable; showing current snapshot instead.",
        },
        "fallbackReason": "monthly trend series missing",
        "sourceEvidenceRefs": [
            str(row.get("sourceEvidenceRef") or "")
            for row in rows[:6]
            if str(row.get("sourceEvidenceRef") or "").strip()
        ],
    }


def _table_artifact(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> VisualArtifact | None:
    intent = str(evidence_package.get("intent") or "")
    if intent not in {
        "market_overview",
        "pricing_analysis",
        "competitor_compare",
        "configuration_analysis",
        "inventory_analysis",
        "news_policy_search",
        "voc_analysis",
        "report_generation",
    }:
        return None
    raw_columns = _business_table_columns(intent)
    columns = _display_table_columns(intent, raw_columns)
    rows = _business_table_rows(intent, evidence_package, refs, answer, question=question)
    if not rows:
        if intent == "pricing_analysis" and _pricing_question_should_prioritize_tco(question, evidence_package):
            return None
        return _fallback_business_table_artifact(intent, evidence_package, answer, raw_columns)
    source_refs = _source_evidence_refs_from_rows(rows)
    display_rows = _display_table_rows(rows, columns)
    intent_analysis = _intent_analysis_block(evidence_package, {}, refs)
    artifact_id = f"artifact_{intent}_table"
    if intent == "competitor_compare" and any(
        str(row.get("source") or "").strip() == "framework"
        for row in rows
    ):
        artifact_id = "artifact_competitor_compare_framework_table"
    return {
        "id": artifact_id,
        "type": "table",
        "title": _table_title(intent),
        "subtitle": _table_explanation(intent),
        "data": {
            "rows": display_rows,
            "intentAnalysis": intent_analysis,
        },
        "spec": {
            "columns": columns,
            "rawColumns": raw_columns,
            "maxRows": 10,
            "sortBy": _table_sort_by(intent),
            "businessExplanation": _table_explanation(intent),
            "columnPolicy": "Main table is capped at seven business columns; raw redundant fields remain in evidencePackage / Analysis Path.",
        },
        "sourceEvidenceRefs": source_refs,
    }


def _configuration_competitor_context_table_artifact(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> VisualArtifact | None:
    if str(evidence_package.get("intent") or "") != "configuration_analysis":
        return None
    rows = _configuration_competitor_context_rows(refs)
    if not rows:
        return None
    columns = ["model", "sales", "priceEvidence", "source", "configurationUse", "nextAction"]
    display_rows = _display_table_rows(rows, columns)
    if not display_rows:
        return None
    return {
        "id": "artifact_configuration_competitor_context_table",
        "type": "table",
        "title": "Competitor context for configuration",
        "subtitle": "Competitor pool and market signals already retrieved; configuration matrix remains a separate evidence gap.",
        "data": {
            "rows": display_rows,
            "intentAnalysis": {
                "intent": "configuration_analysis",
                "evidenceMode": "competitor_context_not_configuration_delta",
                "businessUse": "Use this table to decide which competitors should enter the battery/range/configuration matrix.",
            },
        },
        "spec": {
            "columns": columns,
            "rawColumns": columns,
            "maxRows": len(display_rows),
            "sortBy": "sales",
            "businessExplanation": "Shows the retrieved competitor pool and price-evidence status so configuration validation has concrete comparison targets.",
        },
        "sourceEvidenceRefs": _source_evidence_refs_from_rows(rows),
    }


def _configuration_competitor_context_rows(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        if not label:
            continue
        model, metric = _configuration_competitor_ref_parts(label, ref.get("value"))
        if not model or not metric:
            continue
        group = groups.setdefault(model, {
            "model": model,
            "sales": "",
            "salesValue": None,
            "priceEvidence": "",
            "source": "",
            "configurationUse": "竞品池已识别；配置差异仍需工程/官网配置矩阵验证",
            "nextAction": "补电池、续航、充电、价格和版本配置矩阵",
            "evidenceRef": "",
        })
        if not group.get("evidenceRef"):
            group["evidenceRef"] = str(ref.get("refId") or "")
        _merge_configuration_competitor_ref(group, metric, ref)
    rows = [row for row in groups.values() if str(row.get("model") or "").strip()]
    rows.sort(key=_configuration_competitor_row_sort_key)
    return rows[:6]


def _configuration_competitor_ref_parts(label: str, value: Any) -> tuple[str, str]:
    text = str(label or "").strip()
    if text.startswith("competitor.") and text.endswith(".model"):
        return str(value or "").strip(), "model"
    if "." not in text:
        return "", ""
    model, metric = text.rsplit(".", 1)
    model = model.strip()
    metric = metric.strip()
    if not model or model.startswith(("contextSnapshot", "pricing", "marketSnapshot")):
        return "", ""
    if metric in {
        "sales",
        "priceEvidenceStatus",
        "priceEvidenceRole",
        "candidateDomain",
        "candidateSourceType",
        "sourceUrl",
        "sourceSearchQuery",
        "currentPriceRows",
        "reviewPendingRows",
    }:
        return model, metric
    return "", ""


def _merge_configuration_competitor_ref(group: dict[str, Any], metric: str, ref: dict[str, Any]) -> None:
    value = ref.get("value")
    if metric == "sales":
        ref_id = str(ref.get("refId") or "").strip()
        if ref_id:
            group["evidenceRef"] = ref_id
        group["sales"] = format_artifact_value(value, str(ref.get("unit") or "units"))
        numeric = _number_value(value)
        if numeric is not None:
            group["salesValue"] = numeric
        return
    if metric == "priceEvidenceStatus":
        group["priceEvidence"] = _configuration_competitor_price_evidence(value)
        return
    if metric in {"candidateDomain", "candidateSourceType", "sourceUrl", "sourceSearchQuery"}:
        source = _configuration_competitor_source_label(metric, value)
        if source and (not group.get("source") or metric == "sourceUrl"):
            group["source"] = source
        return
    if metric == "currentPriceRows":
        try:
            if int(value or 0) > 0:
                group["priceEvidence"] = "正式价格记录可用"
        except (TypeError, ValueError):
            pass
        return
    if metric == "reviewPendingRows":
        try:
            if int(value or 0) > 0 and not group.get("priceEvidence"):
                group["priceEvidence"] = f"{int(value)} 条待审核价格观察"
        except (TypeError, ValueError):
            pass


def _configuration_competitor_price_evidence(value: Any) -> str:
    label = _pricing_status_value_label(str(value or ""))
    if label:
        return label
    raw = str(value or "").strip()
    return raw.replace("_", " ") if raw else "价格证据待补"


def _configuration_competitor_source_label(metric: str, value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if metric == "sourceUrl":
        try:
            domain = urlparse(text).netloc.lower()
        except ValueError:
            domain = ""
        return (domain[4:] if domain.startswith("www.") else domain) or text
    if metric == "sourceSearchQuery":
        return "official price search"
    return text


def _configuration_competitor_row_sort_key(row: dict[str, Any]) -> tuple[int, float, str]:
    sales = row.get("salesValue")
    return (
        0 if isinstance(sales, (int, float)) else 1,
        -float(sales) if isinstance(sales, (int, float)) else 0.0,
        str(row.get("model") or "").casefold(),
    )


def _pricing_market_structure_table_artifact(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> VisualArtifact | None:
    if str(evidence_package.get("intent") or "") != "pricing_analysis":
        return None
    structure_refs = _pricing_market_structure_refs(question, evidence_package, refs)
    if not structure_refs:
        return None
    rows = _pricing_market_structure_rows(question, evidence_package, structure_refs)
    if not rows:
        return None
    columns = ["dimension", "evidence", "businessUse", "nextValidation"]
    display_rows = _display_table_rows(rows, columns)
    source_refs = _source_evidence_refs_from_rows(rows)
    if not display_rows or not source_refs:
        return None
    return {
        "id": "artifact_pricing_market_structure_table",
        "type": "table",
        "title": "定价相关市场结构证据表",
        "subtitle": "把动力、渠道和级别结构转成定价立场；不替代官方 MSRP、月供/RV 和配置差异验证。",
        "data": {
            "rows": display_rows,
            "intentAnalysis": _intent_analysis_block(evidence_package, answer, structure_refs),
        },
        "spec": {
            "columns": columns,
            "rawColumns": columns,
            "maxRows": 6,
            "sortBy": "businessUse",
            "businessExplanation": "用这张表把 HEV/BEV/PHEV 需求池、渠道和级别结构连接到定价姿态，再补官方 MSRP、月供/RV 和配置差异。",
            "columnPolicy": "市场结构支撑表；不能替代官方 MSRP、月供/RV 或配置差异验证。",
        },
        "sourceEvidenceRefs": source_refs,
    }


def _pricing_market_structure_refs(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    fuels = _pricing_market_structure_fuels(question, evidence_package, refs)
    if not fuels:
        return []
    selected: list[dict[str, Any]] = []
    for ref in refs:
        label = str(ref.get("label") or "").casefold()
        if "contextsnapshot.crosstabs." not in label:
            continue
        is_segment_pool_ref = "drivebysegment." in label
        if not is_segment_pool_ref and not any(fuel.casefold() in label for fuel in fuels):
            continue
        if any(
            token in label
            for token in (
                ".sales",
                ".2wd_pct",
                ".4wd_pct",
                ".awd_pct",
                ".business_pct",
                ".private_pct",
                ".suv a0.",
                ".suv a.",
                ".suv b.",
            )
        ):
            selected.append(ref)
    return _dedupe_refs_by_id(selected)


def _pricing_market_structure_fuels(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> list[str]:
    text_parts = [str(question or "")]
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    for key in ("models", "competitors", "powertrains"):
        values = entities.get(key)
        if isinstance(values, list):
            text_parts.extend(str(item or "") for item in values)
    combined = " ".join(text_parts).casefold()
    fuels = [
        fuel
        for fuel in ("BEV", "PHEV", "HEV", "MHEV", "ICE")
        if fuel.casefold() in combined
    ]
    if fuels:
        return fuels
    labels = " ".join(str(ref.get("label") or "") for ref in refs).casefold()
    return [
        fuel
        for fuel in ("BEV", "PHEV", "HEV", "MHEV", "ICE")
        if f".{fuel.casefold()}." in labels or f".{fuel.casefold()}_" in labels
    ]


def _pricing_market_structure_rows(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fuel = _pricing_market_structure_fuels(question, evidence_package, refs)[0]
    demand_ref = _pricing_structure_ref(refs, f"drivebyfuel.{fuel}.sales") or _pricing_structure_ref(refs, f"registrationbyfuel.{fuel}.sales")
    two_wd_ref = _pricing_structure_ref(refs, f"drivebyfuel.{fuel}.2wd_pct")
    awd_ref = _pricing_structure_ref(refs, f"drivebyfuel.{fuel}.4wd_pct") or _pricing_structure_ref(refs, f"drivebyfuel.{fuel}.awd_pct")
    business_ref = _pricing_structure_ref(refs, f"registrationbyfuel.{fuel}.business_pct")
    private_ref = _pricing_structure_ref(refs, f"registrationbyfuel.{fuel}.private_pct")
    segment_refs = _pricing_structure_segment_refs(refs, fuel)

    if demand_ref:
        rows.append({
            "dimension": f"{fuel}需求池",
            "evidence": f"{fuel} {format_artifact_value(demand_ref.get('value'), str(demand_ref.get('unit') or ''))}",
            "businessUse": "先确认定价问题背后有可量化需求池，再讨论 MSRP 位置。",
            "nextValidation": "补齐同一市场下目标车型和核心竞品的当前官方 MSRP。",
            "evidenceRef": str(demand_ref.get("refId") or ""),
        })
    drive_parts = []
    if two_wd_ref:
        drive_parts.append(f"2WD {format_artifact_value(two_wd_ref.get('value'), str(two_wd_ref.get('unit') or '%'))}")
    if awd_ref:
        drive_parts.append(f"4WD/AWD {format_artifact_value(awd_ref.get('value'), str(awd_ref.get('unit') or '%'))}")
    if drive_parts:
        rows.append({
            "dimension": f"{fuel}驱动结构",
            "evidence": " / ".join(drive_parts),
            "businessUse": "判断主战场是价值导向的两驱，还是高成本四驱/AWD 定位。",
            "nextValidation": "把驱动形式和版本、公司车场景、配置价值交叉验证。",
            "evidenceRef": str((two_wd_ref or awd_ref or {}).get("refId") or ""),
        })
    channel_parts = []
    if business_ref:
        channel_parts.append(f"Business {format_artifact_value(business_ref.get('value'), str(business_ref.get('unit') or '%'))}")
    if private_ref:
        channel_parts.append(f"Private {format_artifact_value(private_ref.get('value'), str(private_ref.get('unit') or '%'))}")
    if channel_parts:
        rows.append({
            "dimension": f"{fuel}渠道结构",
            "evidence": " / ".join(channel_parts),
            "businessUse": "区分 fleet/company car 定价逻辑和私人零售价格敏感性。",
            "nextValidation": "补月供、RV、税费/company car 口径后再定最终价格姿态。",
            "evidenceRef": str((business_ref or private_ref or {}).get("refId") or ""),
        })
    if segment_refs:
        rows.append({
            "dimension": f"{fuel} SUV级别深度",
            "evidence": "；".join(_pricing_structure_segment_text(segment, sales_ref, mix_ref, fuel=fuel) for segment, sales_ref, mix_ref in segment_refs),
            "businessUse": "把相对价格判断落到目标车型和竞品真正竞争的 SUV 级别。",
            "nextValidation": "用级别池排序竞品价格/配置矩阵的优先级。",
            "evidenceRef": str((segment_refs[0][1] or segment_refs[0][2] or {}).get("refId") or ""),
        })
    return rows


def _pricing_structure_ref(refs: list[dict[str, Any]], token: str) -> dict[str, Any] | None:
    normalized_token = token.casefold()
    for ref in refs:
        if normalized_token in str(ref.get("label") or "").casefold():
            return ref
    return None


def _pricing_structure_segment_refs(
    refs: list[dict[str, Any]],
    fuel: str,
) -> list[tuple[str, dict[str, Any] | None, dict[str, Any] | None]]:
    rows: list[tuple[str, dict[str, Any] | None, dict[str, Any] | None]] = []
    for segment in ("SUV A0", "SUV A", "SUV B"):
        key = segment.casefold()
        sales_ref = _pricing_structure_ref(refs, f"drivebysegment.{key}.sales")
        mix_ref = _pricing_structure_ref(refs, f"segmentbyfuel.{key}.{fuel}_pct")
        if sales_ref or mix_ref:
            rows.append((segment, sales_ref, mix_ref))
    return rows


def _pricing_structure_segment_text(
    segment: str,
    sales_ref: dict[str, Any] | None,
    mix_ref: dict[str, Any] | None,
    *,
    fuel: str = "",
) -> str:
    parts = [segment]
    if sales_ref:
        parts.append(format_artifact_value(sales_ref.get("value"), str(sales_ref.get("unit") or "units")))
    if mix_ref:
        mix_label = f"{fuel}占比" if fuel else "占比"
        parts.append(f"{mix_label} {format_artifact_value(mix_ref.get('value'), str(mix_ref.get('unit') or '%'))}")
    return " ".join(parts)


def _supplemental_policy_pricing_artifacts(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[VisualArtifact]:
    if str(evidence_package.get("intent") or "") != "news_policy_search":
        return []
    pricing_refs = _policy_pricing_refs(refs)
    if not pricing_refs:
        return []
    artifacts: list[VisualArtifact] = []
    rows = _pricing_decision_rows(
        evidence_package,
        pricing_refs,
        answer,
        question="",
        filter_requested_models=False,
    )
    if rows:
        raw_columns = _business_table_columns("pricing_analysis")
        columns = _display_table_columns("pricing_analysis", raw_columns)
        display_rows = _display_table_rows(rows[:10], columns)
        pricing_package = dict(evidence_package)
        pricing_package["intent"] = "pricing_analysis"
        artifacts.append({
            "id": "artifact_policy_pricing_table",
            "type": "table",
            "title": "价格证据表",
            "subtitle": "用于把政策影响转成车型、竞品价格和定价动作的补充价格证据。",
            "data": {
                "rows": display_rows,
                "intentAnalysis": _intent_analysis_block(pricing_package, answer, pricing_refs),
            },
            "spec": {
                "columns": columns,
                "rawColumns": raw_columns,
                "maxRows": 10,
                "sortBy": _table_sort_by("pricing_analysis"),
                "businessExplanation": _table_explanation("pricing_analysis"),
                "columnPolicy": "Supplemental policy-pricing table; official policy sources remain in the primary policy table.",
            },
            "sourceEvidenceRefs": _source_evidence_refs_from_rows(rows),
        })
    chart = _pricing_corridor_chart_from_evidence_refs(evidence_package, pricing_refs)
    if chart:
        chart["subtitle"] = "Supplemental chart for policy questions that also require current price or competitor price evidence."
        artifacts.append(chart)
    return artifacts


def _supplemental_report_pricing_artifacts(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[VisualArtifact]:
    if str(evidence_package.get("intent") or "") != "report_generation":
        return []
    pricing_refs = _report_generation_pricing_refs(question, evidence_package, refs)
    if not pricing_refs:
        return []

    pricing_package = dict(evidence_package)
    pricing_package["intent"] = "pricing_analysis"
    artifacts: list[VisualArtifact] = []

    rows = _pricing_decision_rows(
        pricing_package,
        pricing_refs,
        answer,
        question=question,
    )
    if rows:
        raw_columns = _business_table_columns("pricing_analysis")
        columns = _display_table_columns("pricing_analysis", raw_columns)
        display_rows = _display_table_rows(rows[:10], columns)
        artifacts.append({
            "id": "artifact_report_pricing_table",
            "type": "table",
            "title": "价格证据表",
            "subtitle": "支撑 PPT 定位页的价格和价值证据。",
            "data": {
                "rows": display_rows,
                "intentAnalysis": _intent_analysis_block(pricing_package, answer, pricing_refs),
            },
            "spec": {
                "columns": columns,
                "rawColumns": raw_columns,
                "maxRows": 10,
                "sortBy": _table_sort_by("pricing_analysis"),
                "businessExplanation": _table_explanation("pricing_analysis"),
                "columnPolicy": "Supplemental report-pricing table; it supports the PPT block but does not replace official MSRP repair requirements.",
            },
            "sourceEvidenceRefs": _source_evidence_refs_from_rows(rows),
        })

    chart = _pricing_corridor_chart_from_evidence_refs(pricing_package, pricing_refs)
    if chart:
        chart["subtitle"] = "Pricing corridor chart generated from the report's MSRP, competitor corridor and user-material price evidence."
        artifacts.append(chart)
    return artifacts


def _report_generation_pricing_refs(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not _report_generation_is_pricing_report(question, evidence_package, refs):
        return []
    result: list[dict[str, Any]] = []
    for ref in refs:
        if _is_report_generation_noise_ref(ref) or _is_report_generation_generic_market_ref(ref):
            continue
        label = str(ref.get("label") or "").strip()
        lower = label.casefold()
        source = str(ref.get("source") or ref.get("table") or "").casefold()
        unit = str(ref.get("unit") or "").casefold()
        if lower == "avgmsrp":
            continue
        from_price_source = any(token in source for token in ("jato_msrp", "price", "pricing"))
        pricing_label = any(
            token in lower
            for token in (
                "pricestats.",
                "pricing.records.",
                "msrp",
                "target price",
                "relative price",
                "price delta",
                "price corridor",
                "competitor corridor",
                "monthly",
                "leasing",
                "residual",
                "rv",
                "pva",
                "coverage",
                "price gap",
                "价差",
                "价格",
                "价格带",
            )
        )
        currency_unit = unit in {"currency", "eur", "sek"} or "eur" in unit or "sek" in unit or "currency" in unit
        if from_price_source or pricing_label or currency_unit:
            result.append(ref)
    return result


def _report_generation_is_pricing_report(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> bool:
    text = str(question or "").casefold()
    if any(
        token in text
        for token in (
            "pricing",
            "price",
            "msrp",
            "corridor",
            "定价",
            "价格",
            "价位",
            "价格带",
            "价差",
        )
    ):
        return True
    requested_models = _report_requested_models(evidence_package, question)
    if requested_models:
        return any(_is_user_material_pricing_ref_for_models(ref, requested_models) for ref in refs)
    return False


def _is_user_material_pricing_ref_for_models(ref: dict[str, Any], requested_models: list[str]) -> bool:
    label = str(ref.get("label") or "")
    lower = label.casefold()
    if "user material" not in lower:
        return False
    if not any(token in lower for token in ("msrp", "price", "corridor", "pva", "gap", "价格", "价差")):
        return False
    normalized_label = _compact_match_text(label)
    return any(
        model_token and model_token in normalized_label
        for model_token in (_compact_match_text(model) for model in requested_models)
    )


def _compact_match_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _msrp_source_repair_table_artifact(question: str, evidence_package: dict[str, Any], answer: dict[str, Any]) -> VisualArtifact | None:
    intent = str(evidence_package.get("intent") or "")
    if intent not in {"pricing_analysis", "competitor_compare"}:
        return None
    candidates = _msrp_source_repair_candidates(evidence_package, answer)
    if not candidates:
        return None
    display_candidates = _display_msrp_source_repair_candidates(
        candidates,
        _pricing_relevant_model_labels(evidence_package, answer, question=question),
    )
    rows = _msrp_source_repair_rows(
        display_candidates,
        target_models=_pricing_requested_target_models(evidence_package),
        competitor_models=_pricing_requested_competitor_models(evidence_package),
    )
    if not rows:
        return None
    columns = [
        "candidateRole",
        "model",
        "sourceType",
        "sourceStatus",
        "reviewPendingRows",
        "readiness",
        "reviewStatus",
        "requiredFields",
        "draftPath",
        "searchQuery",
        "nextStep",
    ]
    return {
        "id": "artifact_msrp_source_repair_table",
        "type": "table",
        "title": "MSRP 来源验证表",
        "subtitle": "用于验证官方价格来源；生成当前价格记录前不能写成确定 MSRP 或竞品价。",
        "data": {
            "rows": _display_table_rows(rows, columns, max_columns=len(columns)),
            "intentAnalysis": {
                "template": "msrp_source_repair",
                "recommendation": "先确认官方 URL、版本/配置、币种和发布日期，再生成当前价格记录；确认前不能写成确定 MSRP。",
                "coverage": _msrp_source_repair_summary(display_candidates),
                "materializationCommands": _msrp_source_materialization_commands(display_candidates),
            },
        },
        "spec": {
            "columns": columns,
            "rawColumns": columns,
            "maxRows": 8,
            "sortBy": "candidateRole",
            "businessExplanation": "用于修复本车型或竞品 MSRP 覆盖缺口的官方来源验证表。",
            "columnPolicy": "候选来源在审核并生成当前价格记录之前，不能作为确定价格证据。",
            "evidenceMode": "source_repair_candidates_not_price_evidence",
        },
        "fallbackReason": "msrp_source_repair_candidates",
        "sourceEvidenceRefs": [],
    }


def _pending_msrp_review_table_artifact(question: str, evidence_package: dict[str, Any], answer: dict[str, Any]) -> VisualArtifact | None:
    intent = str(evidence_package.get("intent") or "")
    if intent not in {"pricing_analysis", "competitor_compare"}:
        return None
    rows = _pending_msrp_review_rows(
        _display_msrp_source_repair_candidates(
            _msrp_source_repair_candidates(evidence_package, answer),
            _pricing_relevant_model_labels(evidence_package, answer, question=question),
        )
    )
    if not rows:
        return None
    columns = [
        "candidateRole",
        "model",
        "trim",
        "localMsrp",
        "eurMsrp",
        "source",
        "reviewStatus",
        "decisionUse",
        "nextStep",
    ]
    return {
        "id": "artifact_pending_msrp_review_table",
        "type": "table",
        "title": "Pending MSRP review table",
        "subtitle": "Official-source price observations captured by the scraper but not yet approved as current MSRP.",
        "data": {
            "rows": _display_table_rows(rows, columns, max_columns=len(columns)),
            "intentAnalysis": {
                "template": "pending_msrp_review",
                "recommendation": "Use this as a review queue and provisional price-ladder view only. Confirm trim/version, currency, date and source before converting to current price.",
                "coverage": _pending_msrp_review_summary(rows),
            },
        },
        "spec": {
            "columns": columns,
            "rawColumns": columns,
            "maxRows": 10,
            "sortBy": "model",
            "businessExplanation": "Pending official-source MSRP observations that can guide review and table layout, but cannot support final price claims yet.",
            "columnPolicy": "Pending observations are not accepted current price evidence until human approval or deterministic override is applied.",
            "evidenceMode": "review_pending_not_current_price",
        },
        "fallbackReason": "pending_msrp_review_observations",
        "sourceEvidenceRefs": [],
    }


def _pending_msrp_review_chart_artifact(question: str, evidence_package: dict[str, Any], answer: dict[str, Any]) -> VisualArtifact | None:
    intent = str(evidence_package.get("intent") or "")
    if intent not in {"pricing_analysis", "competitor_compare"}:
        return None
    rows = _pending_msrp_review_rows(
        _display_msrp_source_repair_candidates(
            _msrp_source_repair_candidates(evidence_package, answer),
            _pricing_relevant_model_labels(evidence_package, answer, question=question),
        )
    )
    chart_rows: list[dict[str, Any]] = []
    for row in rows:
        value = _number_value(row.get("localMsrpValue"))
        currency = str(row.get("localCurrency") or "").strip()
        if value is None or not currency:
            continue
        chart_rows.append({
            "label": " ".join(
                part
                for part in (str(row.get("model") or "").strip(), str(row.get("trim") or "").strip())
                if part
            ),
            "value": value,
            "unit": currency,
            "series": "review pending MSRP",
        })
    if len(chart_rows) < 2:
        return None
    currencies = {str(row.get("unit") or "") for row in chart_rows if str(row.get("unit") or "").strip()}
    if len(currencies) != 1:
        return None
    chart_rows = _dedupe_chart_rows(chart_rows)
    if len(chart_rows) < 2:
        return None
    return {
        "id": "artifact_pending_msrp_review_chart",
        "type": "chart",
        "title": "Pending MSRP ladder chart",
        "subtitle": "Bar chart from review-pending official-source observations; not accepted current MSRP yet.",
        "data": chart_rows[:10],
        "spec": {
            "chartType": "bar",
            "xField": "label",
            "yField": "value",
            "seriesField": "series",
            "data": chart_rows[:10],
            "note": "Review-pending chart only: verify trim/version, currency, date and source before writing exact MSRP conclusions.",
            "evidenceMode": "review_pending_not_current_price",
        },
        "sourceEvidenceRefs": [],
    }


def _display_msrp_source_repair_candidates(
    candidates: dict[str, Any],
    requested_models: list[str],
) -> dict[str, Any]:
    if not requested_models:
        return candidates
    filtered = dict(candidates)
    kept: list[dict[str, Any]] = []
    for key in ("ownModel", "competitorCorridor"):
        values = candidates.get(key) if isinstance(candidates.get(key), list) else []
        filtered_values = [
            entry
            for entry in values
            if isinstance(entry, dict)
            and _pricing_model_is_relevant(_source_repair_model_label(entry), requested_models)
        ]
        filtered[key] = filtered_values
        kept.extend(filtered_values)
    if not kept:
        return candidates
    filtered["candidateCount"] = len(kept)
    filtered["materializedCandidateCount"] = sum(
        1
        for entry in kept
        if _source_repair_current_price_rows(entry) > 0
    )
    filtered["displayFilteredByQuestion"] = True
    return filtered


def _pending_msrp_review_rows(candidates: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for role, key in (("请求车型", "ownModel"), ("竞品/价格走廊", "competitorCorridor")):
        entries = candidates.get(key) if isinstance(candidates.get(key), list) else []
        for entry in entries:
            if isinstance(entry, dict):
                rows.extend(_pending_msrp_review_rows_for_candidate(entry, role=role))
    return _dedupe_pending_msrp_review_rows(rows)[:10]


def _pending_msrp_review_rows_for_candidate(entry: dict[str, Any], *, role: str) -> list[dict[str, Any]]:
    observations = entry.get("reviewPendingObservations")
    if not isinstance(observations, list):
        return []
    candidate_model = _source_repair_model_label(entry)
    rows: list[dict[str, Any]] = []
    for observation in observations:
        if not isinstance(observation, dict):
            continue
        model = " ".join(
            part
            for part in (
                str(observation.get("brand") or "").strip(),
                str(observation.get("model") or observation.get("officialModel") or "").strip(),
            )
            if part
        ).strip() or candidate_model
        trim = str(observation.get("trim") or observation.get("officialTrim") or "").strip()
        local_value = _number_value(observation.get("sourceMsrpValue"))
        local_currency = str(observation.get("sourceCurrency") or "").strip()
        eur_value = _number_value(observation.get("msrpValue"))
        eur_currency = str(observation.get("currency") or "").strip()
        rows.append({
            "candidateRole": role,
            "model": model,
            "trim": trim or "Pending trim review",
            "localMsrp": format_artifact_value(local_value, local_currency) if local_value is not None else "",
            "localMsrpValue": local_value,
            "localCurrency": local_currency,
            "eurMsrp": format_artifact_value(eur_value, eur_currency) if eur_value is not None and eur_currency else "",
            "source": _pending_msrp_source_label(observation),
            "reviewStatus": str(observation.get("reviewStatus") or observation.get("matchStatus") or "review_required").strip(),
            "decisionUse": "待审核：可用于 review/价格阶梯骨架，不能当确定 MSRP",
            "nextStep": "确认 trim、币种、发布日期和来源后再生成 current price",
            "reviewCaseId": str(observation.get("reviewCaseId") or "").strip(),
            "observationId": str(observation.get("observationId") or "").strip(),
            "evidenceStatus": str(observation.get("evidenceStatus") or "review_pending_not_current_price").strip(),
        })
    return rows


def _pending_msrp_source_label(observation: dict[str, Any]) -> str:
    source_url = str(observation.get("sourceUrl") or "").strip()
    if not source_url:
        return "official source pending review"
    try:
        domain = urlparse(source_url).netloc.lower()
    except ValueError:
        domain = ""
    domain = domain[4:] if domain.startswith("www.") else domain
    return domain or source_url


def _dedupe_pending_msrp_review_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = (
            str(row.get("reviewCaseId") or ""),
            str(row.get("observationId") or ""),
            str(row.get("model") or "").casefold(),
            str(row.get("trim") or "").casefold(),
        )
        fallback_key = (
            str(row.get("model") or "").casefold(),
            str(row.get("trim") or "").casefold(),
            str(row.get("localMsrp") or "").casefold(),
            str(row.get("source") or "").casefold(),
        )
        unique_key = key if any(key[:2]) else fallback_key
        if unique_key in seen:
            continue
        seen.add(unique_key)
        result.append(row)
    return result


def _pending_msrp_review_summary(rows: list[dict[str, Any]]) -> str:
    models = _dedupe_strings(str(row.get("model") or "").strip() for row in rows if str(row.get("model") or "").strip())
    currencies = _dedupe_strings(str(row.get("localCurrency") or "").strip() for row in rows if str(row.get("localCurrency") or "").strip())
    parts = [f"{len(rows)} pending observations"]
    if models:
        parts.append(f"models: {', '.join(models[:4])}")
    if currencies:
        parts.append(f"currencies: {', '.join(currencies[:3])}")
    parts.append("not accepted current price")
    return "; ".join(parts)


def _msrp_source_repair_candidates(evidence_package: dict[str, Any], answer: dict[str, Any]) -> dict[str, Any]:
    for direct_candidates in (answer.get("sourceRepairCandidates"), evidence_package.get("sourceRepairCandidates")):
        if not isinstance(direct_candidates, dict):
            continue
        if not _is_msrp_source_repair_candidates(direct_candidates):
            continue
        if _source_repair_candidate_count(direct_candidates) > 0:
            return direct_candidates
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for item in tool_results:
        if not isinstance(item, dict):
            continue
        diagnostics = item.get("coverageDiagnostics") if isinstance(item.get("coverageDiagnostics"), dict) else {}
        candidates = diagnostics.get("sourceRepairCandidates") if isinstance(diagnostics.get("sourceRepairCandidates"), dict) else {}
        if not candidates:
            continue
        if not _is_msrp_source_repair_candidates(candidates):
            continue
        if _source_repair_candidate_count(candidates) > 0:
            return candidates
    return {}


def _is_msrp_source_repair_candidates(candidates: dict[str, Any]) -> bool:
    data_status = str(candidates.get("dataStatus") or "").strip().casefold()
    if any(
        token in data_status
        for token in (
            "policy",
            "external_research",
            "voc",
            "leasing",
            "tco",
            "company_car",
            "company-car",
        )
    ):
        return False
    if not data_status:
        return True
    return any(
        token in data_status
        for token in (
            "current_price",
            "price_source",
            "msrp",
            "source_draft",
            "competitor_current_price",
            "own_model_current_price",
        )
    )


def _source_repair_candidate_count(candidates: dict[str, Any]) -> int:
    raw_count = candidates.get("candidateCount")
    if raw_count is not None:
        try:
            return int(raw_count)
        except (TypeError, ValueError):
            pass
    own_model = candidates.get("ownModel") if isinstance(candidates.get("ownModel"), list) else []
    competitor_corridor = (
        candidates.get("competitorCorridor")
        if isinstance(candidates.get("competitorCorridor"), list)
        else []
    )
    queries = candidates.get("queries") if isinstance(candidates.get("queries"), list) else []
    return len(own_model) + len(competitor_corridor) + len([query for query in queries if str(query or "").strip()])


def _msrp_source_repair_rows(
    candidates: dict[str, Any],
    *,
    target_models: list[str] | None = None,
    competitor_models: list[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    own_model = candidates.get("ownModel") if isinstance(candidates.get("ownModel"), list) else []
    competitor_corridor = (
        candidates.get("competitorCorridor")
        if isinstance(candidates.get("competitorCorridor"), list)
        else []
    )
    targets = target_models or []
    competitors = competitor_models or []
    for entry in own_model:
        if isinstance(entry, dict):
            rows.append(_msrp_source_repair_row(entry, _source_repair_candidate_role(entry, targets=targets, competitors=competitors, fallback="请求车型")))
    for entry in competitor_corridor:
        if isinstance(entry, dict):
            rows.append(_msrp_source_repair_row(entry, _source_repair_candidate_role(entry, targets=targets, competitors=competitors, fallback="竞品/价格走廊")))
    return _dedupe_source_repair_rows(rows)[:8]


def _source_repair_candidate_role(
    entry: dict[str, Any],
    *,
    targets: list[str],
    competitors: list[str],
    fallback: str,
) -> str:
    model = _source_repair_model_label(entry)
    if competitors and _pricing_model_is_relevant(model, competitors):
        return "竞品/价格走廊"
    if targets and _pricing_model_is_relevant(model, targets):
        return "请求车型"
    return fallback


def _msrp_source_repair_row(entry: dict[str, Any], role: str) -> dict[str, Any]:
    model_label = _source_repair_model_label(entry)
    status = _source_repair_status(entry)
    return {
        "candidateRole": role,
        "model": model_label,
        "sourceType": _source_repair_type(entry, status),
        "sourceStatus": status,
        "reviewPendingRows": _source_repair_review_pending_rows(entry),
        "readiness": _source_repair_readiness(entry),
        "reviewStatus": _source_repair_review_status(entry),
        "requiredFields": _source_repair_required_fields(entry),
        "draftPath": _source_repair_draft_path(entry),
        "sourceScope": _source_repair_scope(entry, status),
        "searchQuery": _source_repair_search_query(entry),
        "nextStep": _source_repair_next_step(entry, status),
    }


def _source_repair_model_label(entry: dict[str, Any]) -> str:
    brand = str(entry.get("brand") or "").strip()
    model = str(entry.get("model") or "").strip()
    return " ".join(part for part in (brand, model) if part).strip() or "Model source candidate"


def _source_repair_current_price_rows(entry: dict[str, Any]) -> int:
    try:
        return int(entry.get("currentPriceRows") or 0)
    except (TypeError, ValueError):
        return 0


def _source_repair_review_pending_rows(entry: dict[str, Any]) -> int:
    try:
        return int(entry.get("reviewPendingRows") or 0)
    except (TypeError, ValueError):
        return 0


def _source_repair_status(entry: dict[str, Any]) -> str:
    current_price_rows = _source_repair_current_price_rows(entry)
    pending_rows = _source_repair_review_pending_rows(entry)
    draft_status = str(entry.get("draftStatus") or "").strip()
    if current_price_rows > 0:
        return f"已有当前价格记录：{current_price_rows}"
    if pending_rows > 0:
        return f"待审核价格观察：{pending_rows}（非正式当前价）"
    if draft_status == "candidate_search_query":
        return "官方价格搜索候选"
    if draft_status == "source_draft_available":
        return "来源草稿待审核"
    if draft_status:
        return draft_status.replace("_", " ")
    return "来源候选"


def _source_repair_type(entry: dict[str, Any], status: str) -> str:
    current_price_rows = _source_repair_current_price_rows(entry)
    pending_rows = _source_repair_review_pending_rows(entry)
    if current_price_rows > 0:
        return "当前价格记录"
    if pending_rows > 0:
        return "待审核价格观察"
    candidate_type = str(entry.get("candidateSourceType") or "").strip()
    if candidate_type == "source_draft":
        return "来源草稿"
    if candidate_type in {"brand_official_search", "generic_official_price_search"}:
        return "品牌官网搜索" if candidate_type == "brand_official_search" else "官方价格搜索"
    if str(entry.get("draftStatus") or "").strip() == "candidate_search_query":
        return "官方价格搜索"
    return candidate_type.replace("_", " ") if candidate_type else "来源候选"


def _source_repair_draft_path(entry: dict[str, Any]) -> str:
    if str(entry.get("candidateSourceType") or "").strip() != "source_draft":
        return ""
    return str(entry.get("sourceDraftPath") or entry.get("relativePath") or "").strip()


def _source_repair_hint(entry: dict[str, Any]) -> str:
    source_code = str(entry.get("sourceCode") or "").strip()
    relative_path = str(entry.get("relativePath") or "").strip()
    source_url = str(entry.get("sourceUrl") or "").strip()
    return source_code or relative_path or source_url or "source candidate"


def _source_repair_scope(entry: dict[str, Any], status: str) -> str:
    current_price_rows = _source_repair_current_price_rows(entry)
    if current_price_rows > 0:
        return "已有当前价格记录"
    candidate_type = str(entry.get("candidateSourceType") or "").strip()
    if candidate_type == "source_draft":
        source_url = str(entry.get("sourceUrl") or "").strip()
        if source_url:
            domain = _source_domain_label(source_url)
            return f"来源草稿：{domain}" if domain else "来源草稿待审核"
        return "来源草稿待审核"
    domain = str(entry.get("candidateDomain") or "").strip()
    if domain:
        return f"品牌官网搜索：{domain}"
    if candidate_type == "generic_official_price_search":
        return "通用官方价格搜索"
    if candidate_type:
        return candidate_type.replace("_", " ")
    if str(entry.get("draftStatus") or "").strip() == "candidate_search_query" and _source_repair_google_query(entry):
        return "通用官方价格搜索"
    return _source_repair_hint(entry)


def _source_repair_search_query(entry: dict[str, Any]) -> str:
    query = str(entry.get("sourceSearchQuery") or "").strip()
    if query:
        return query
    google_query = _source_repair_google_query(entry)
    if google_query:
        return google_query
    source_url = str(entry.get("sourceUrl") or "").strip()
    if source_url:
        return source_url
    return str(entry.get("relativePath") or "").strip() or _source_repair_hint(entry)


def _source_repair_google_query(entry: dict[str, Any]) -> str:
    source_url = str(entry.get("sourceUrl") or "").strip()
    if not source_url:
        return ""
    parsed = urlparse(source_url)
    if "google." not in parsed.netloc.lower():
        return ""
    query_values = parse_qs(parsed.query).get("q") or []
    if not query_values:
        return ""
    return str(query_values[0] or "").strip()


def _source_domain_label(source_url: str) -> str:
    try:
        domain = urlparse(str(source_url or "")).netloc.lower()
    except ValueError:
        return ""
    return domain[4:] if domain.startswith("www.") else domain


def _source_repair_next_step(entry: dict[str, Any], status: str) -> str:
    current_price_rows = _source_repair_current_price_rows(entry)
    if current_price_rows > 0:
        return "使用当前价格记录后重跑 Business Validation。"
    if _source_repair_review_pending_rows(entry) > 0:
        return "审核版本/配置、币种、发布日期和来源；人工确认后再生成当前价格记录。"
    materialization_next_step = str(entry.get("materializationNextStep") or "").strip()
    if materialization_next_step:
        return _public_source_repair_next_step(materialization_next_step)
    if str(entry.get("draftStatus") or "").strip() == "candidate_search_query":
        return "打开候选，确认 URL、版本/配置、币种和发布日期后创建当前价格记录。"
    return "审核来源草稿，确认版本/配置、币种和日期后生成当前价格记录。"


def _public_source_repair_next_step(value: str) -> str:
    text = str(value or "").strip()
    lowered = text.casefold()
    if not text:
        return ""
    if "dry-run" in lowered or "dry run" in lowered or "ingest" in lowered:
        return "先做抽取前审核，确认版本/配置、币种、日期和价格合理性；通过后再写入当前价格记录。"
    if "fix selector" in lowered or "selector" in lowered:
        return "先修正价格/版本选择器并做抽取前审核；通过后再写入当前价格记录。"
    if "review trim" in lowered or "current_prices" in lowered or "materialization" in lowered:
        return "先审核版本/配置、币种、日期和来源；确认后再生成当前价格记录。"
    replacements = {
        "dry-run": "抽取前审核",
        "current_prices": "当前价格记录",
        "current price": "当前价格记录",
        "ingestion": "写入",
        "ingest": "写入",
        "trim": "版本/配置",
    }
    for raw, replacement in replacements.items():
        text = re.sub(raw, replacement, text, flags=re.IGNORECASE)
    return text


def _source_repair_readiness(entry: dict[str, Any]) -> str:
    if _source_repair_review_pending_rows(entry) > 0:
        return "价格观察待审核"
    status = str(entry.get("materializationStatus") or "").strip()
    score = entry.get("materializationReadinessScore")
    if status and score not in (None, ""):
        return f"{_public_source_repair_status(status)} · {score}"
    if status:
        return _public_source_repair_status(status)
    if str(entry.get("candidateSourceType") or "").strip() == "source_draft":
        return "来源草稿待审核"
    return ""


def _public_source_repair_status(value: str) -> str:
    normalized = str(value or "").strip().casefold()
    mapping = {
        "ready_for_extraction": "可进入抽取前审核",
        "selector_review_required": "选择器需审核",
        "dry_run_review_required": "抽取前需审核",
        "review_pending_not_current_price": "价格观察待审核",
        "source_draft_available": "来源草稿待审核",
        "current_price_materialized": "已有当前价格记录",
        "candidate_search_query": "官方价格搜索候选",
    }
    return mapping.get(normalized, str(value or "").replace("_", " "))


def _public_source_repair_risk_flag(value: str) -> str:
    normalized = str(value or "").strip().casefold()
    mapping = {
        "price_selector_too_broad": "价格选择器过宽",
        "vehicle_container_too_broad": "车型容器过宽",
        "powertrain_mapping_requires_trim_review": "动力类型需按版本复核",
        "missing_price_selector": "缺价格选择器",
        "missing_source_url": "缺官方来源 URL",
        "missing_currency": "缺币种",
    }
    return mapping.get(normalized, str(value or "").replace("_", " "))


def _public_source_repair_field(value: str) -> str:
    normalized = str(value or "").strip().casefold()
    mapping = {
        "source": "来源",
        "source_url": "官方来源 URL",
        "trim": "版本/配置",
        "official_trim": "官方版本/配置",
        "msrp": "MSRP",
        "price": "价格",
        "currency": "币种",
        "observed_at": "抓取日期",
        "retrieved_at": "读取日期",
        "match_status": "匹配状态",
        "price_selector": "价格选择器",
        "vehicle_container": "车型容器",
    }
    return mapping.get(normalized, str(value or "").replace("_", " "))


def _source_repair_required_fields(entry: dict[str, Any]) -> str:
    pending = entry.get("reviewPendingObservations")
    if isinstance(pending, list) and pending:
        trims = [
            str(item.get("trim") or item.get("officialTrim") or "").strip()
            for item in pending
            if isinstance(item, dict)
        ]
        examples = ", ".join([trim for trim in trims if trim][:3])
        return f"待审核观察：{examples}" if examples else "待审核 MSRP 观察"
    risk_flags = entry.get("materializationRiskFlags")
    if isinstance(risk_flags, list) and risk_flags:
        return "需审核：" + "、".join(_public_source_repair_risk_flag(str(item)) for item in risk_flags[:4])
    missing = entry.get("materializationMissingFields")
    if isinstance(missing, list) and missing:
        return "缺少：" + "、".join(_public_source_repair_field(str(item)) for item in missing[:5])
    required = entry.get("materializationRequiredFields")
    if isinstance(required, list) and required:
        return "、".join(_public_source_repair_field(str(item)) for item in required[:5])
    if str(entry.get("candidateSourceType") or "").strip() == "source_draft":
        return "来源、版本/配置、MSRP、币种、抓取日期、匹配状态"
    return ""


def _source_repair_review_status(entry: dict[str, Any]) -> str:
    status = str(entry.get("reviewPendingStatus") or "").strip()
    if status:
        return _public_source_repair_status(status)
    status = str(entry.get("materializationReviewStatus") or "").strip()
    if status:
        return _public_source_repair_status(status)
    flags = entry.get("materializationRiskFlags")
    if isinstance(flags, list) and flags:
        return "选择器需审核"
    if str(entry.get("candidateSourceType") or "").strip() == "source_draft":
        return "抽取前需审核"
    return ""


def _msrp_source_materialization_commands(candidates: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("ownModel", "competitorCorridor"):
        values = candidates.get(key) if isinstance(candidates.get(key), list) else []
        for entry in values:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("candidateSourceType") or "").strip() != "source_draft":
                continue
            dry_run_command = str(entry.get("dryRunCommand") or "").strip()
            submit_command = str(entry.get("submitCommand") or "").strip()
            if not dry_run_command and not submit_command:
                continue
            rows.append({
                "model": _source_repair_model_label(entry),
                "sourceDraftPath": _source_repair_draft_path(entry),
                "dryRunCommand": dry_run_command,
                "submitCommand": submit_command,
                "ingestApiPath": str(entry.get("ingestApiPath") or "").strip(),
                "materializationGate": str(entry.get("materializationGate") or "").strip(),
                "materializationReviewStatus": str(entry.get("materializationReviewStatus") or "").strip(),
                "materializationRiskFlags": [
                    str(item)
                    for item in entry.get("materializationRiskFlags", [])
                    if str(item).strip()
                ][:8] if isinstance(entry.get("materializationRiskFlags"), list) else [],
                "priceSanityRules": entry.get("priceSanityRules") if isinstance(entry.get("priceSanityRules"), dict) else {},
                "safeToAutoMaterialize": bool(entry.get("safeToAutoMaterialize")),
                "reviewChecklist": [
                    str(item)
                    for item in entry.get("reviewChecklist", [])
                    if str(item).strip()
                ][:8] if isinstance(entry.get("reviewChecklist"), list) else [],
            })
    return rows[:8]


def _dedupe_source_repair_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row.get("candidateRole") or ""), str(row.get("model") or "").lower())
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _msrp_source_repair_summary(candidates: dict[str, Any]) -> str:
    candidate_count = _source_repair_candidate_count(candidates)
    materialized = candidates.get("materializedCandidateCount")
    try:
        materialized_count = int(materialized or 0)
    except (TypeError, ValueError):
        materialized_count = 0
    missing_own_model = bool(candidates.get("missingOwnModelSource"))
    pending_count = sum(
        _source_repair_review_pending_rows(entry)
        for key in ("ownModel", "competitorCorridor")
        for entry in (candidates.get(key) if isinstance(candidates.get(key), list) else [])
        if isinstance(entry, dict)
    )
    parts = [f"{materialized_count}/{candidate_count} candidates materialized"]
    if pending_count > 0:
        parts.append(f"{pending_count} pending review observations")
    if missing_own_model:
        parts.append("requested model source missing")
    data_status = str(candidates.get("dataStatus") or "").strip()
    if data_status:
        parts.append(data_status.replace("_", " "))
    return "; ".join(parts)


def _external_source_repair_table_artifact(
    evidence_package: dict[str, Any],
    *,
    question: str = "",
) -> VisualArtifact | None:
    intent = str(evidence_package.get("intent") or "")
    candidates = _external_source_repair_candidates(evidence_package, question=question)
    if (
        intent == "pricing_analysis"
        and str(candidates.get("dataStatus") or "") != "leasing_tco_source_candidates"
        and not _is_pricing_external_source_repair_candidates(candidates)
    ):
        return None
    if intent not in {"voc_analysis", "news_policy_search", "pricing_analysis", "configuration_analysis"}:
        return None
    if not candidates:
        return None
    rows = _external_source_repair_rows(candidates, intent=intent)
    if not rows:
        return None
    columns = [
        "sourceNeed",
        "queryOrSource",
        "validationStage",
        "evidenceUse",
        "requiredFields",
        "nextStep",
        "canUseInAnswer",
    ]
    return {
        "id": "artifact_external_source_repair_table",
        "type": "table",
        "title": "External source validation matrix",
        "subtitle": "Shows which external sources must be validated before converting research/VOC/configuration hypotheses into answer claims.",
        "data": {
            "rows": _display_table_rows(rows, columns),
            "intentAnalysis": {
                "template": "external_source_validation",
                "recommendation": "Use these candidates as a research checklist; do not cite them until URL, title, publish date and claim text are captured.",
                "coverage": _external_source_repair_summary(candidates),
            },
        },
        "spec": {
            "columns": columns,
            "rawColumns": columns,
            "maxRows": 8,
            "sortBy": "sourceNeed",
            "businessExplanation": "Validation matrix for turning search queries or source candidates into citation-ready VOC/news/configuration evidence.",
            "columnPolicy": "Search queries and source candidates are displayed as validation tasks, not as evidence-backed facts.",
            "evidenceMode": "external_source_repair_candidates_not_citations",
        },
        "fallbackReason": "external_source_repair_candidates",
        "sourceEvidenceRefs": [],
    }


def _external_source_repair_candidates(
    evidence_package: dict[str, Any],
    *,
    question: str = "",
) -> dict[str, Any]:
    missing_items = evidence_package.get("missingEvidence")
    if not isinstance(missing_items, list):
        missing_items = []
    missing_names = {
        str(item.get("name") or "").strip()
        for item in missing_items
        if isinstance(item, dict)
    }
    intent = str(evidence_package.get("intent") or "")
    direct_candidates = evidence_package.get("sourceRepairCandidates")
    if isinstance(direct_candidates, dict) and _is_external_source_repair_candidates(direct_candidates):
        if _needs_leasing_tco_source_repair(intent=intent, missing_names=missing_names, question=question):
            return _leasing_tco_source_repair_candidates(
                evidence_package,
                queries=_external_source_candidate_queries(direct_candidates),
                question=question,
            )
        return _filter_external_policy_source_candidates(
            direct_candidates,
            question=question,
            country=str(evidence_package.get("country") or ""),
        )
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for item in tool_results:
        if not isinstance(item, dict):
            continue
        diagnostics = item.get("coverageDiagnostics") if isinstance(item.get("coverageDiagnostics"), dict) else {}
        candidates = diagnostics.get("sourceRepairCandidates") if isinstance(diagnostics.get("sourceRepairCandidates"), dict) else {}
        if _is_external_source_repair_candidates(candidates):
            if _needs_leasing_tco_source_repair(intent=intent, missing_names=missing_names, question=question):
                return _leasing_tco_source_repair_candidates(
                    evidence_package,
                    queries=_external_source_candidate_queries(candidates),
                    question=question,
                )
            return _filter_external_policy_source_candidates(
                candidates,
                question=question,
                country=str(evidence_package.get("country") or ""),
            )
        queries = diagnostics.get("externalResearchQueries") if isinstance(diagnostics.get("externalResearchQueries"), list) else []
        query_texts = _dedupe_strings(str(query or "").strip() for query in queries if str(query or "").strip())
        if query_texts:
            if _needs_leasing_tco_source_repair(intent=intent, missing_names=missing_names, question=question):
                return _leasing_tco_source_repair_candidates(evidence_package, queries=query_texts, question=question)
            if intent == "news_policy_search":
                query_texts = _filter_policy_source_queries(query_texts, _policy_source_repair_topic(question)) or query_texts
            return {
                "dataStatus": "external_research_query_candidates",
                "queries": query_texts[:8],
                "candidateCount": len(query_texts[:8]),
                "materializedCandidateCount": 0,
            }
    if _needs_leasing_tco_source_repair(intent=intent, missing_names=missing_names, question=question):
        return _leasing_tco_source_repair_candidates(evidence_package, question=question)
    if intent == "voc_analysis" and missing_names.intersection({"external_research_claims_unavailable", "minimum_external_sources", "consumer_signal"}):
        queries = _default_external_repair_queries(evidence_package, question=question, intent=intent)
        return {
            "dataStatus": "external_research_required",
            "queries": queries,
            "candidateCount": len(queries),
            "materializedCandidateCount": 0,
        }
    if intent == "news_policy_search" and missing_names.intersection({"specific_policy_source_evidence", "minimum_external_sources", "official_source", "source_date"}):
        queries = _default_external_repair_queries(evidence_package, question=question, intent=intent)
        return {
            "dataStatus": "external_policy_source_required",
            "queries": queries,
            "candidateCount": len(queries),
            "materializedCandidateCount": 0,
        }
    if intent == "configuration_analysis" and missing_names.intersection({
        "external_research_claims_unavailable",
        "minimum_external_sources",
        "configuration_delta",
        "feature_diff",
        "key_features",
        "user_value_impact",
        "consumer_signal",
    }):
        queries = _default_external_repair_queries(evidence_package, question=question, intent=intent)
        return {
            "dataStatus": "external_configuration_source_required",
            "queries": queries,
            "candidateCount": len(queries),
            "materializedCandidateCount": 0,
        }
    return {}


def _is_pricing_external_source_repair_candidates(candidates: dict[str, Any]) -> bool:
    data_status = str(candidates.get("dataStatus") or "").strip()
    if data_status not in {"external_research_query_candidates", "external_research_required"}:
        return False
    values: list[str] = []
    queries = candidates.get("queries") if isinstance(candidates.get("queries"), list) else []
    values.extend(str(query or "") for query in queries)
    for key in ("ownModel", "competitorCorridor", "sourceSearchPlan"):
        entries = candidates.get(key) if isinstance(candidates.get(key), list) else []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            values.extend(
                str(entry.get(field) or "")
                for field in ("brand", "model", "sourceSearchQuery", "candidateSourceType", "sourceUrl")
            )
    haystack = " ".join(values).casefold()
    if not haystack:
        return False
    if any(token in haystack for token in ("owner review", "complaint", "forum", "klagomål", "omdöme")):
        return False
    return any(token in haystack for token in ("official price", "msrp", "pris officiell", "price list", "pricing"))


def _filter_external_policy_source_candidates(
    candidates: dict[str, Any],
    *,
    question: str,
    country: str,
) -> dict[str, Any]:
    data_status = str(candidates.get("dataStatus") or "").strip()
    if data_status not in {"external_policy_source_candidates", "external_policy_source_required"}:
        return candidates
    topic = _policy_source_repair_topic(question)
    if not topic:
        return candidates
    result = dict(candidates)
    own_model = [entry for entry in result.get("ownModel", []) if isinstance(entry, dict)] if isinstance(result.get("ownModel"), list) else []
    competitor_corridor = [
        entry for entry in result.get("competitorCorridor", []) if isinstance(entry, dict)
    ] if isinstance(result.get("competitorCorridor"), list) else []
    queries = [str(query or "").strip() for query in result.get("queries", []) if str(query or "").strip()] if isinstance(result.get("queries"), list) else []
    filtered_own = _filter_policy_source_candidate_entries(own_model, topic)
    filtered_corridor = _filter_policy_source_candidate_entries(competitor_corridor, topic)
    filtered_queries = _filter_policy_source_queries(queries, topic)
    if (own_model or competitor_corridor or queries) and not (filtered_own or filtered_corridor or filtered_queries):
        return _topic_policy_source_required_candidates(country=country, topic=topic) or candidates
    if own_model:
        result["ownModel"] = filtered_own
    if competitor_corridor:
        result["competitorCorridor"] = filtered_corridor
    if queries:
        result["queries"] = filtered_queries
    candidate_count = len(result.get("ownModel", []) if isinstance(result.get("ownModel"), list) else [])
    candidate_count += len(result.get("competitorCorridor", []) if isinstance(result.get("competitorCorridor"), list) else [])
    candidate_count += len(result.get("queries", []) if isinstance(result.get("queries"), list) else [])
    result["candidateCount"] = candidate_count
    return result


def _policy_source_repair_topic(question: str) -> str:
    text = str(question or "").casefold()
    if "elbilspremien" in text or "elbilspremie" in text:
        return "bev_subsidy"
    if any(token in text for token in ("补贴", "subsidy", "price cap", "价格上限", "prisgrans", "prisgräns")):
        return "bev_subsidy"
    if any(token in text for token in ("co₂", "co2", "税率", "税费", "company car", "benefit", "公司车", "bilförmån", "bilforman")):
        return "co2_tax"
    if "phev" in text and any(token in text for token in ("税", "tax", "benefit", "fleet", "leasing", "大客户")):
        return "co2_tax"
    return ""


def _filter_policy_source_candidate_entries(entries: list[dict[str, Any]], topic: str) -> list[dict[str, Any]]:
    scored: list[tuple[int, int, dict[str, Any]]] = []
    for index, entry in enumerate(entries):
        score = _policy_source_text_score(_policy_source_candidate_haystack(entry), topic)
        if score > 0:
            scored.append((score, index, entry))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [dict(entry) for _, _, entry in scored]


def _filter_policy_source_queries(queries: list[str], topic: str) -> list[str]:
    if not topic:
        return queries
    scored: list[tuple[int, int, str]] = []
    for index, query in enumerate(queries):
        score = _policy_source_text_score(query, topic)
        if score > 0:
            scored.append((score, index, query))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [query for _, _, query in scored]


def _policy_source_text_score(value: str, topic: str) -> int:
    text = " ".join(str(value or "").casefold().replace("%20", " ").replace("+", " ").split())
    if not text:
        return 0
    excluded = _policy_source_excluded_terms(topic)
    if excluded and any(term in text for term in excluded):
        return 0
    score = sum(1 for term in _policy_source_preferred_terms(topic) if term in text)
    if topic == "co2_tax" and "skatteverket" in text:
        score += 4
    if topic == "co2_tax" and ("transportstyrelsen" in text or "fordonsskatt" in text):
        score += 2
    if topic == "bev_subsidy" and ("regeringen" in text or "transportstyrelsen" in text):
        score += 3
    if topic == "bev_subsidy" and "skatteverket" in text and ("elbilspremie" in text or "elbilspremien" in text):
        score += 1
    return score


def _policy_source_preferred_terms(topic: str) -> tuple[str, ...]:
    if topic == "co2_tax":
        return (
            "skatteverket",
            "bilförmån",
            "bilforman",
            "förmån",
            "forman",
            "company car",
            "benefit",
            "co2",
            "co₂",
            "koldioxid",
            "laddhybrid",
            "phev",
            "fordonsskatt",
            "malus",
            "tjänstebil",
            "tjanstebil",
            "tax",
            "税率",
        )
    if topic == "bev_subsidy":
        return (
            "elbilspremie",
            "elbilspremien",
            "subsidy",
            "incentive",
            "price cap",
            "prisgrans",
            "prisgräns",
            "bonus",
            "regeringen",
            "transportstyrelsen",
        )
    return ()


def _policy_source_excluded_terms(topic: str) -> tuple[str, ...]:
    if topic == "co2_tax":
        return (
            "elbilspremie",
            "elbilspremien",
            "price cap",
            "prisgrans",
            "prisgräns",
            "subsidy",
            "purchase incentive",
        )
    return ()


def _policy_source_candidate_haystack(entry: dict[str, Any]) -> str:
    source_url = str(entry.get("sourceUrl") or "")
    return " ".join(
        str(value or "")
        for value in (
            entry.get("brand"),
            entry.get("model"),
            entry.get("sourceSearchQuery"),
            entry.get("relativePath"),
            entry.get("candidateDomain"),
            entry.get("candidateSourceType"),
            entry.get("sourceCode"),
            source_url,
            _source_repair_google_query(entry),
        )
    )


def _topic_policy_source_required_candidates(*, country: str, topic: str) -> dict[str, Any]:
    queries = _topic_policy_source_queries(country=country, topic=topic)
    if not queries:
        return {}
    return {
        "dataStatus": "external_policy_source_required",
        "queries": queries,
        "candidateCount": len(queries),
        "materializedCandidateCount": 0,
    }


def _topic_policy_source_queries(*, country: str, topic: str) -> list[str]:
    country_label = str(country or "").strip() or "target market"
    country_key = country_label.casefold()
    if ("sweden" in country_key or "sverige" in country_key or "瑞典" in country_key) and topic == "co2_tax":
        return [
            "site:skatteverket.se bilförmån laddhybrid CO2 2026",
            "site:skatteverket.se fordonsskatt koldioxid laddhybrid 2026",
            "site:transportstyrelsen.se bonus malus koldioxid laddhybrid 2026",
        ]
    if ("sweden" in country_key or "sverige" in country_key or "瑞典" in country_key) and topic == "bev_subsidy":
        return [
            "site:regeringen.se elbilspremie 2026 elbil prisgräns",
            "site:transportstyrelsen.se elbil bonus malus 2026 prisgräns",
            "site:skatteverket.se elbilspremie 2026",
        ]
    if topic == "co2_tax":
        return [
            f"{country_label} company car benefit CO2 vehicle tax 2026 official",
            f"{country_label} PHEV tax CO2 company car benefit 2026 government",
        ]
    if topic == "bev_subsidy":
        return [
            f"{country_label} electric vehicle subsidy price cap 2026 official government",
            f"{country_label} EV purchase incentive eligibility price threshold 2026 official",
        ]
    return []


def _is_external_source_repair_candidates(candidates: dict[str, Any]) -> bool:
    data_status = str(candidates.get("dataStatus") or "").strip()
    return data_status in {
        "external_research_query_candidates",
        "external_research_required",
        "external_policy_source_candidates",
        "external_policy_source_required",
        "leasing_tco_source_candidates",
    }


def _needs_leasing_tco_source_repair(*, intent: str, missing_names: set[str], question: str) -> bool:
    if intent not in {"pricing_analysis", "news_policy_search"}:
        return False
    if "leasing_tco_or_company_car_evidence" in missing_names and _question_mentions_leasing_tco(question):
        return True
    if "minimum_external_sources" in missing_names and _question_mentions_leasing_tco(question):
        return True
    return False


def _question_mentions_leasing_tco(question: str) -> bool:
    text = str(question or "").lower()
    return any(
        token in text
        for token in (
            "leasing",
            "lease",
            "tco",
            "company car",
            "company-car",
            "fleet",
            "residual",
            "rv",
            "月供",
            "残值",
            "大客户",
            "公司车",
            "benefit",
        )
    )


def _leasing_tco_source_repair_candidates(
    evidence_package: dict[str, Any],
    *,
    queries: list[str] | None = None,
    question: str = "",
) -> dict[str, Any]:
    query_values = _dedupe_strings([
        *_default_leasing_tco_repair_queries(evidence_package, question=question),
        *(queries or []),
    ])[:8]
    if not query_values:
        return {}
    return {
        "dataStatus": "leasing_tco_source_candidates",
        "queries": query_values,
        "candidateCount": len(query_values),
        "materializedCandidateCount": 0,
    }


def _external_source_candidate_queries(candidates: dict[str, Any]) -> list[str]:
    values: list[str] = []
    queries = candidates.get("queries") if isinstance(candidates.get("queries"), list) else []
    values.extend(str(query or "").strip() for query in queries if str(query or "").strip())
    for key in ("ownModel", "competitorCorridor"):
        rows = candidates.get(key) if isinstance(candidates.get(key), list) else []
        for entry in rows:
            if not isinstance(entry, dict):
                continue
            query = (
                str(entry.get("sourceSearchQuery") or "").strip()
                or _source_repair_google_query(entry)
                or str(entry.get("relativePath") or "").strip()
            )
            if query:
                values.append(query)
    return _dedupe_strings(values)


def _default_leasing_tco_repair_queries(evidence_package: dict[str, Any], *, question: str = "") -> list[str]:
    country = (
        str(evidence_package.get("country") or "").strip()
        or _visual_user_material_country_hint(question)
        or "target market"
    )
    return [
        f"{country} PHEV company car benefit tax 2026 official",
        f"{country} plug-in hybrid vehicle tax fleet benefit 2026 government",
        f"{country} PHEV leasing monthly payment residual value fleet TCO",
        f"{country} XC60 PHEV Kia Sportage PHEV leasing monthly payment residual value",
    ]


def _default_external_repair_queries(
    evidence_package: dict[str, Any],
    *,
    question: str,
    intent: str,
) -> list[str]:
    country = str(evidence_package.get("country") or "").strip() or "target market"
    subject = _external_repair_subject(question)
    if intent == "news_policy_search":
        return _dedupe_strings([
            f"{country} {subject} official policy source publish date eligibility",
            f"{country} government {subject} EV subsidy tax policy affected models",
        ])[:2]
    if intent == "configuration_analysis":
        return _dedupe_strings([
            f"{country} {subject} OEM spec trim equipment source publish date",
            f"{country} {subject} competitor media test owner relevance",
        ])[:2]
    return _dedupe_strings([
        f"{country} {subject} owner review forum complaint",
        f"{country} {subject} media review user complaints product issue",
    ])[:2]


def _external_repair_subject(question: str) -> str:
    text = str(question or "").strip()
    lower = text.lower()
    if any(token in lower for token in ("v2h", "v2g")):
        return "V2H EV purchase driver"
    if any(token in lower for token in ("冬季包", "winter package", "heat pump", "热泵", "battery preconditioning", "电池预热")):
        return "Nordic winter package heat pump battery preconditioning EV SUV"
    if any(token in lower for token in ("80kwh", "95kwh", "battery", "电池", "续航", "range", "charging", "800v")):
        return "BEV battery size range charging winter driving"
    if any(token in lower for token in ("拖车", "tow", "roof", "冬季胎", "winter tyre", "winter tire", "däck")):
        return "tow hook roof load winter tires SUV"
    if any(token in lower for token in ("elbilspremien", "补贴", "subsidy")):
        return "Elbilspremien 2026 BEV subsidy"
    if any(token in lower for token in ("company car", "benefit", "公司车")):
        return "company car benefit BEV PHEV"
    if any(token in lower for token in ("omoda", "jaecoo", "o5", "o9", "j7", "j8")):
        tokens = re.findall(r"\b(?:OMODA|JAECOO|O5|O9|J7|J8|BEV|HEV|PHEV)\b", text, flags=re.IGNORECASE)
        if tokens:
            return " ".join(_dedupe_strings(token.upper() for token in tokens))
    return "automotive customer voice"


def _external_source_repair_rows(candidates: dict[str, Any], *, intent: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    data_status = str(candidates.get("dataStatus") or "").strip()
    candidate_type = (
        "TCO / company-car source"
        if data_status == "leasing_tco_source_candidates"
        else "Pricing official source"
        if intent == "pricing_analysis"
        else "Policy official source"
        if data_status in {"external_policy_source_candidates", "external_policy_source_required"} or intent == "news_policy_search"
        else "Configuration external source"
        if intent == "configuration_analysis"
        else "VOC / media / forum query"
    )
    row_intent = "pricing_analysis" if data_status == "leasing_tco_source_candidates" else intent
    queries = candidates.get("queries") if isinstance(candidates.get("queries"), list) else []
    for index, query in enumerate(queries[:8], start=1):
        query_text = str(query or "").strip()
        if not query_text:
            continue
        rows.append({
            "sourceNeed": _external_source_need(candidate_type, query_text, intent=row_intent),
            "queryOrSource": query_text,
            "validationStage": "search query candidate",
            "evidenceUse": _external_source_evidence_use(query_text, intent=row_intent),
            "requiredFields": _external_source_required_fields(
                intent=row_intent,
                source_text=query_text,
                data_status=data_status,
            ),
            "nextStep": _external_source_next_step(data_status, intent=row_intent),
            "canUseInAnswer": "No - validate first",
            "sourceHint": f"external-query-{index}",
        })
    competitor_corridor = (
        candidates.get("competitorCorridor")
        if isinstance(candidates.get("competitorCorridor"), list)
        else []
    )
    for entry in competitor_corridor:
        if isinstance(entry, dict):
            rows.append(_external_source_repair_row(entry, candidate_type, data_status, intent=row_intent))
    return _dedupe_external_source_rows(rows)[:8]


def _external_source_repair_row(entry: dict[str, Any], candidate_type: str, data_status: str, *, intent: str) -> dict[str, Any]:
    label = _external_source_query_or_source(entry)
    return {
        "sourceNeed": _external_source_need(candidate_type, label, intent=intent),
        "queryOrSource": label,
        "validationStage": _external_source_status(entry),
        "evidenceUse": _external_source_evidence_use(label, intent=intent),
        "requiredFields": _external_source_required_fields(
            intent=intent,
            source_text=label,
            data_status=data_status,
        ),
        "nextStep": _external_source_next_step(data_status, intent=intent),
        "canUseInAnswer": "No - validate first",
        "sourceHint": _source_repair_hint(entry),
    }


def _external_source_query_or_source(entry: dict[str, Any]) -> str:
    query = str(entry.get("sourceSearchQuery") or "").strip() or _source_repair_google_query(entry)
    if query:
        return query
    source_url = str(entry.get("sourceUrl") or "").strip()
    if source_url and "google." not in urlparse(source_url).netloc.lower():
        return source_url
    label = _source_repair_model_label(entry)
    if label != "Model source candidate":
        return label
    source_url = str(entry.get("sourceUrl") or "").strip()
    if source_url:
        return source_url
    return _source_repair_hint(entry)


def _external_source_need(candidate_type: str, query_or_source: str, *, intent: str) -> str:
    text = f"{candidate_type} {query_or_source}".lower()
    if "tco" in text or "leasing" in text or "company-car" in text or "company car" in text or "residual" in text or "rv" in text:
        return "Leasing/TCO/company-car source"
    if intent == "pricing_analysis" and any(token in text for token in ("official price", "msrp", "pris", "price list", "pricing")):
        return "Official price/MSRP source"
    if intent == "news_policy_search" or any(token in text for token in ("policy", "subsidy", "tax", "government", "official")):
        return "Official policy/news source"
    if intent == "configuration_analysis":
        return "Configuration/media source"
    if any(token in text for token in ("forum", "owner", "review", "complaint", "ägare", "klagomål", "omdöme")):
        return "VOC owner/media source"
    if any(token in text for token in ("test", "media", "teknikens", "magazine")):
        return "Media review source"
    return "External research source"


def _external_source_evidence_use(query_or_source: str, *, intent: str) -> str:
    text = query_or_source.lower()
    if intent == "pricing_analysis" and any(token in text for token in ("official price", "msrp", "pris", "price list")):
        return "Validate official MSRP/current price, trim scope, currency and publish date before using pricing conclusions."
    if intent == "news_policy_search":
        return "Verify policy fact, date, scope and affected models."
    if intent == "configuration_analysis":
        return "Validate feature need, test condition and competitor/user relevance before turning it into must-have equipment."
    if any(token in text for token in ("tco", "leasing", "company-car", "company car", "residual", "rv", "benefit", "bilförmån", "fleet")):
        return "Validate monthly payment, residual value, tax/benefit formula and fleet assumptions before using TCO claims."
    if any(token in text for token in ("v2h", "v2g", "home", "energy")):
        return "Validate whether V2H is a real purchase driver or only a feature hypothesis."
    if any(token in text for token in ("tow", "roof", "winter", "tyre", "däck", "dragkrok")):
        return "Validate Nordic utility demand frequency before turning it into must-have equipment."
    if any(token in text for token in ("complaint", "problem", "klagomål", "reliability")):
        return "Identify recurring pain points and map them to product, dealer or warranty actions."
    if any(token in text for token in ("owner", "review", "ägare", "omdöme")):
        return "Capture owner/media claims and separate isolated anecdotes from repeated themes."
    return "Validate source-backed claim text before using it in the answer."


def _external_source_required_fields(*, intent: str, source_text: str = "", data_status: str = "") -> str:
    text = f"{source_text} {data_status}".lower()
    if any(token in text for token in ("tco", "leasing", "company-car", "company car", "residual", "rv", "benefit", "bilförmån", "fleet")):
        return "URL, title, publish date, monthly payment/RV/tax formula, eligible model/scope"
    if intent == "news_policy_search":
        return "URL, title, publish date, official scope, policy effect"
    if intent == "configuration_analysis":
        return "URL, title, publish date, test condition, feature claim, model relevance"
    if intent == "pricing_analysis":
        return "URL, title, publish date, model/trim, currency, MSRP/current price"
    return "URL, title, publish date, claim text, market relevance"


def _external_source_status(entry: dict[str, Any]) -> str:
    draft_status = str(entry.get("draftStatus") or "").strip()
    if draft_status == "candidate_search_query":
        return "search query candidate"
    if str(entry.get("sourceUrl") or "").strip():
        return "source URL candidate"
    if draft_status:
        return draft_status.replace("_", " ")
    return "source candidate"


def _external_source_next_step(data_status: str, *, intent: str) -> str:
    if data_status == "external_policy_source_candidates" or intent == "news_policy_search":
        return "Open official source, capture URL/title/date/scope, then rerun validation."
    if data_status == "leasing_tco_source_candidates":
        return "Open source, capture URL/title/date/TCO assumptions, then rerun validation."
    if intent == "pricing_analysis":
        return "Open official price source, capture URL/title/date/model/trim/MSRP, then create current price evidence."
    if intent == "configuration_analysis":
        return "Open source, capture URL/title/date/test condition/feature claim, then rerun validation."
    return "Open result, capture URL/title/date/claim text, then create citation-ready VOC evidence."


def _external_source_repair_summary(candidates: dict[str, Any]) -> str:
    candidate_count = _source_repair_candidate_count(candidates)
    if candidate_count <= 0:
        queries = candidates.get("queries") if isinstance(candidates.get("queries"), list) else []
        candidate_count = len([query for query in queries if str(query or "").strip()])
    data_status = str(candidates.get("dataStatus") or "").replace("_", " ").strip()
    if data_status:
        return f"{candidate_count} candidates; {data_status}"
    return f"{candidate_count} candidates"


def _dedupe_external_source_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row.get("sourceNeed") or ""), str(row.get("queryOrSource") or "").lower())
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _dedupe_strings(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _supplemental_policy_market_artifacts(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[VisualArtifact]:
    if str(evidence_package.get("intent") or "") != "news_policy_search":
        return []
    if not _policy_question_needs_market_context(question):
        return []
    market_refs = _policy_market_context_refs(refs)
    if not market_refs:
        return []
    rows = _market_overview_decision_rows(evidence_package, market_refs, answer)
    rows = [row for row in rows if not _is_zero_other_pct_market_row(row)]
    if not rows:
        return []
    raw_columns = _business_table_columns("market_overview")
    columns = _display_table_columns("market_overview", raw_columns)
    market_package = dict(evidence_package)
    market_package["intent"] = "market_overview"
    return [
        {
            "id": "artifact_policy_market_context_table",
            "type": "table",
            "title": "Market context table",
            "subtitle": "Supplemental JATO context used to translate policy impact into powertrain, channel, and segment actions.",
            "data": {
                "rows": _display_table_rows(rows[:10], columns),
                "intentAnalysis": _intent_analysis_block(market_package, answer, market_refs),
            },
            "spec": {
                "columns": columns,
                "rawColumns": raw_columns,
                "maxRows": 10,
                "sortBy": _table_sort_by("market_overview"),
                "businessExplanation": _table_explanation("market_overview"),
                "columnPolicy": "Supplemental policy-market table; official policy sources remain in the primary policy table.",
            },
            "sourceEvidenceRefs": _source_evidence_refs_from_rows(rows),
        }
    ]


def _is_zero_other_pct_market_row(row: dict[str, Any]) -> bool:
    signal = str(row.get("signal") or "").strip().lower()
    evidence = str(row.get("evidence") or "").strip()
    return "other" in signal and bool(re.match(r"^0(?:\.0+)?\s*%$", evidence))


def _policy_question_needs_market_context(question: str) -> bool:
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


def _policy_market_context_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for ref in refs:
        label = str(ref.get("label") or "").lower()
        source = str(ref.get("source") or ref.get("table") or "").lower()
        if not any(token in source for token in ("jato_country_chart_deck", "jato_country_snapshot")):
            continue
        if label in {"row_count", "metadata.result_count", "chart_count"}:
            continue
        if any(token in label for token in ("registrationby", "business", "private", "powertrainmix", "drivebyfuel", "segmentbyfuel")):
            result.append(ref)
    return result


def _tco_validation_artifact(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> VisualArtifact | None:
    intent = str(evidence_package.get("intent") or "")
    if intent not in {"news_policy_search", "pricing_analysis"}:
        return None
    if intent == "pricing_analysis":
        if not _pricing_question_needs_tco_validation(question, evidence_package):
            return None
    elif not _question_needs_tco_validation(question, evidence_package, answer):
        return None
    rows = _tco_validation_rows(question, refs, answer)
    columns = [
        "scenario",
        "evidenceNeeded",
        "sourceOrTool",
        "acceptanceCriteria",
        "currentStatus",
        "businessUse",
        "priority",
    ]
    return {
        "id": "artifact_tco_validation_table",
        "type": "table",
        "title": "TCO / company-car validation table",
        "subtitle": "Validation matrix for leasing, residual value, company-car tax, charging behavior, mileage and winter-risk evidence.",
        "data": {
            "rows": _display_table_rows(rows, columns),
            "intentAnalysis": {
                "template": "tco_company_car_validation",
                "recommendation": "Do not treat market size or generic policy context as TCO proof; validate monthly payment, RV, tax/benefit formula and usage assumptions before making a fleet recommendation.",
                "coverage": _tco_validation_coverage(rows),
            },
        },
        "spec": {
            "columns": columns,
            "rawColumns": columns,
            "maxRows": 8,
            "sortBy": "priority",
            "businessExplanation": "TCO validation table separates market/channel background from the missing monthly, residual-value, tax and usage evidence needed for company-car or leasing conclusions.",
            "columnPolicy": "Candidate validation requirements are not numeric evidence until source/tool rows are materialized in EvidencePackage.",
            "evidenceMode": "validation_matrix_not_final_tco_evidence",
        },
        "sourceEvidenceRefs": _source_evidence_refs_from_rows(rows),
    }


def _bom_entity_validation_artifact(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> VisualArtifact | None:
    if str(evidence_package.get("intent") or "") != "inventory_analysis":
        return None
    if not _question_needs_bom_entity_validation(question, evidence_package, answer):
        return None
    rows = _bom_entity_validation_rows(evidence_package, refs, answer)
    columns = [
        "entityLayer",
        "mappingNeeded",
        "sourceOrTool",
        "acceptanceCriteria",
        "currentStatus",
        "businessUse",
        "priority",
    ]
    return {
        "id": "artifact_bom_entity_validation_table",
        "type": "table",
        "title": "BOM / entity mapping validation table",
        "subtitle": "Validation matrix for PI, market overlay, variant, material code, color, lifecycle and editable quantity relationships.",
        "data": {
            "rows": _display_table_rows(rows, columns),
            "intentAnalysis": {
                "template": "bom_entity_mapping_validation",
                "recommendation": "Do not use market volume, MSRP or generic model counts as proof for BOM relationships; validate entity keys and lifecycle before turning material codes into editable order quantities.",
                "coverage": _bom_entity_validation_coverage(rows),
            },
        },
        "spec": {
            "columns": columns,
            "rawColumns": columns,
            "maxRows": 8,
            "sortBy": "priority",
            "businessExplanation": "BOM/entity validation table separates inventory/BOM mapping requirements from ordinary market metrics so material-code answers are traceable and actionable.",
            "columnPolicy": "Rows are validation requirements until the matching source/tool produces evidenceRefs for the entity relationship.",
            "evidenceMode": "validation_matrix_not_final_bom_evidence",
        },
        "sourceEvidenceRefs": _source_evidence_refs_from_rows(rows),
    }


def _question_needs_bom_entity_validation(
    question: str,
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
) -> bool:
    text = " ".join(
        [
            str(question or ""),
            str(answer.get("direct") or ""),
            str(answer.get("summary") or ""),
            " ".join(_string_list(answer.get("reportReadyBullets"))),
            " ".join(_missing_evidence_names(evidence_package)),
        ]
    ).casefold()
    return any(
        token in text
        for token in (
            "bom",
            "inventory",
            "material",
            "materialcode",
            "material_code",
            "sku",
            "variant",
            "lifecycle",
            "order",
            "pi",
            "物料",
            "版本",
            "内外饰",
            "颜色",
            "生命周期",
            "选品",
            "可编辑数量",
            "bom_entity_mapping_evidence",
        )
    )


def _bom_entity_validation_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    action = _first_action(answer) or "建立 PI / 市场 / 版本 / 颜色 / 物料号 / 生命周期关系后，再生成可编辑数量。"
    row_specs = [
        {
            "entityLayer": "PI / shared header",
            "mappingNeeded": "PI header、产品系列、年款和共享配置定义",
            "sourceOrTool": "BOM master / product definition table",
            "acceptanceCriteria": "能说明 SE/FI 是否共享同一 PI header，以及哪些字段允许共用",
            "businessUse": "决定跨市场能合并的产品定义边界",
            "priority": "P0",
            "tokens": ("pi", "header", "productdefinition", "series", "modelyear"),
        },
        {
            "entityLayer": "Market overlay",
            "mappingNeeded": "country/market、销售市场、当地法规/包配置 overlay",
            "sourceOrTool": "market overlay table / query_with_filters",
            "acceptanceCriteria": "同一 PI 下能按市场拆出可售版本、价格口径和订单状态",
            "businessUse": "避免 SE/FI 合并 PI 后误合并车辆生成逻辑",
            "priority": "P0",
            "tokens": ("market", "country", "overlay", "sweden", "finland", "se", "fi", "市场"),
        },
        {
            "entityLayer": "Business variant",
            "mappingNeeded": "业务版本、trim、variant、powertrain 和 drive",
            "sourceOrTool": "vehicle variant table / compare_vehicle_variants",
            "acceptanceCriteria": "每个客户可见版本都能映射到唯一业务版本键",
            "businessUse": "把物料号解释成用户能理解的版本结构",
            "priority": "P0",
            "tokens": ("version", "trim", "variant", "powertrain", "drive", "车型版本", "版本"),
        },
        {
            "entityLayer": "Material code",
            "mappingNeeded": "materialCode / SKU / part number 与业务版本的关系",
            "sourceOrTool": "material master / BOM lookup",
            "acceptanceCriteria": "同一业务版本下多物料号必须能按市场、颜色、PI 或生命周期解释",
            "businessUse": "判断一个版型多个物料号是正常拆分还是数据冲突",
            "priority": "P0",
            "tokens": ("materialcode", "material_code", "material", "sku", "partnumber", "物料"),
        },
        {
            "entityLayer": "Color / interior",
            "mappingNeeded": "exterior、interior、color package 与物料号组合",
            "sourceOrTool": "color mapping table / BOM attributes",
            "acceptanceCriteria": "外饰+内饰组合能解释物料号差异，且不丢失可售颜色",
            "businessUse": "把客户选择项转成可下单物料组合",
            "priority": "P1",
            "tokens": ("exterior", "interior", "color", "colour", "colorspec", "内饰", "外饰", "颜色"),
        },
        {
            "entityLayer": "Lifecycle / orderability",
            "mappingNeeded": "active、phase-out、replacement、historical material lifecycle",
            "sourceOrTool": "material lifecycle / order status table",
            "acceptanceCriteria": "历史物料号、替代物料和当前可下单状态可追溯",
            "businessUse": "避免历史或停用物料进入新订单",
            "priority": "P0",
            "tokens": ("lifecycle", "status", "active", "phase", "replacement", "orderable", "生命周期", "可下单"),
        },
        {
            "entityLayer": "Editable quantity",
            "mappingNeeded": "stock、allocated units、pipeline、customer editable quantity",
            "sourceOrTool": "inventory allocation / order planning table",
            "acceptanceCriteria": "可编辑数量必须由库存、已分配、订单冻结和生命周期共同计算",
            "businessUse": action,
            "priority": "P1",
            "tokens": ("availableunits", "available_units", "stock", "quantity", "qty", "allocation", "editable", "可编辑数量", "库存"),
        },
    ]
    rows: list[dict[str, Any]] = []
    for spec in row_specs:
        ref = _first_ref_matching_tokens(refs, spec["tokens"])
        rows.append({
            "entityLayer": spec["entityLayer"],
            "mappingNeeded": spec["mappingNeeded"],
            "sourceOrTool": spec["sourceOrTool"],
            "acceptanceCriteria": spec["acceptanceCriteria"],
            "currentStatus": _bom_entity_current_status(ref),
            "businessUse": spec["businessUse"],
            "priority": spec["priority"],
            "evidenceRef": str(ref.get("refId") or "") if ref else "",
            "source": str(ref.get("source") or ref.get("table") or "") if ref else "framework",
        })
    return rows


def _bom_entity_current_status(ref: dict[str, Any] | None) -> str:
    if not ref:
        return "待补实体关系 evidenceRef"
    ref_id = str(ref.get("refId") or "").strip()
    label = str(ref.get("label") or "").strip()
    value = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
    evidence = ref_id or label
    if evidence and value:
        return f"已有 {evidence}: {value}"
    if evidence:
        return f"已有 evidenceRef: {evidence}"
    return "已有部分证据，需补完整实体键"


def _bom_entity_validation_coverage(rows: list[dict[str, Any]]) -> str:
    ready = len([row for row in rows if str(row.get("evidenceRef") or "").strip()])
    return f"{ready}/{len(rows)} entity layers have evidence refs; missing rows remain validation requirements, not BOM facts."


def _question_needs_tco_validation(
    question: str,
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
) -> bool:
    text = " ".join(
        [
            str(question or ""),
            str(answer.get("direct") or ""),
            str(answer.get("summary") or ""),
            " ".join(_string_list(answer.get("reportReadyBullets"))),
            " ".join(_missing_evidence_names(evidence_package)),
        ]
    ).casefold()
    return any(
        token in text
        for token in (
            "leasing",
            "lease",
            "tco",
            "rv",
            "residual",
            "company car",
            "company-car",
            "fleet",
            "benefit",
            "月供",
            "残值",
            "公司车",
            "大客户",
            "税费",
            "用车成本",
            "leasing_tco_or_company_car_evidence",
        )
    )


def _pricing_question_needs_tco_validation(question: str, evidence_package: dict[str, Any]) -> bool:
    text = str(question or "").casefold()
    if any(
        token in text
        for token in (
            "leasing",
            "lease",
            "tco",
            "company car",
            "company-car",
            "fleet",
            "benefit",
            "月供",
            "残值",
            "公司车",
            "大客户",
            "税费",
            "用车成本",
        )
    ):
        return True
    return "leasing_tco_or_company_car_evidence" in _missing_evidence_names(evidence_package)


def _missing_evidence_names(evidence_package: dict[str, Any]) -> list[str]:
    missing = evidence_package.get("missingEvidence")
    if not isinstance(missing, list):
        return []
    return [
        str(item.get("name") or "").strip()
        for item in missing
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]


def _tco_validation_rows(question: str, refs: list[dict[str, Any]], answer: dict[str, Any]) -> list[dict[str, Any]]:
    action = _first_action(answer) or "建立 BEV/PHEV company-car / leasing TCO 对比表"
    preferred_powertrain = _tco_preferred_powertrain(question, answer)
    row_specs = [
        {
            "scenario": "Channel / fleet exposure",
            "evidenceNeeded": "BEV/PHEV Business 与 Private 渠道占比、销量池和车型级 channel mix",
            "sourceOrTool": "build_market_chart / query_country_snapshot",
            "acceptanceCriteria": "能说明公司车渠道是否足够大，以及 PHEV 是否比 BEV 更依赖 Business 盘",
            "businessUse": "决定该问题应走 fleet/company-car 逻辑还是私人零售逻辑",
            "priority": "P0",
            "tokens": ("business_pct", "private_pct", "registrationbyfuel", "channel", "business", "private"),
        },
        {
            "scenario": "Monthly payment / lease quote",
            "evidenceNeeded": "24/36/48 个月月供、年里程、首付、版本和是否含服务包",
            "sourceOrTool": "leasing_calculator / dealer leasing source / current_price",
            "acceptanceCriteria": "同一假设下能比较 BEV、PHEV、HEV 或核心竞品的真实月付压力",
            "businessUse": "判断大客户是否会因为现金流或预算选择 PHEV",
            "priority": "P0",
            "tokens": ("monthlypayment", "monthly payment", "lease quote", "lease payment", "leasing monthly", "monthly", "payment", "月供"),
        },
        {
            "scenario": "Residual value / RV risk",
            "evidenceNeeded": "24/36/48 个月 RV、回购/二手风险、品牌和动力路线折损假设",
            "sourceOrTool": "rv_lookup / leasing source / finance assumptions",
            "acceptanceCriteria": "能解释 PHEV 在总成本中是降低风险还是增加残值不确定性",
            "businessUse": "决定 PHEV 是否能作为低风险 fleet 方案",
            "priority": "P0",
            "tokens": ("rv", "residual", "residualvalue", "resale", "残值"),
        },
        {
            "scenario": "Tax / company-car benefit formula",
            "evidenceNeeded": "官方 benefit tax / CO2 / 车辆税公式、发布日期、适用车型和价格口径",
            "sourceOrTool": "external_research / official tax source",
            "acceptanceCriteria": "能把认证 CO2、价格、benefit formula 转成 BEV/PHEV 可比税费差异",
            "businessUse": "避免把低排放标签误写成确定税费优势",
            "priority": "P0",
            "tokens": ("company car", "benefit", "tax", "co2", "co₂", "emission", "skatteverket", "税"),
        },
        {
            "scenario": "Charging / mileage behavior",
            "evidenceNeeded": "家庭/公司充电可得性、长途里程、用电/用油比例、冬季能耗",
            "sourceOrTool": "VOC/external source + fleet usage assumptions",
            "acceptanceCriteria": "能证明 PHEV 的可油可电在真实使用中降低成本或风险",
            "businessUse": action,
            "priority": "P1",
            "tokens": ("charging", "mileage", "electricity", "winter", "充电", "里程", "冬季"),
        },
    ]
    rows: list[dict[str, Any]] = []
    for spec in row_specs:
        ref = _first_ref_matching_tokens(
            refs,
            spec["tokens"],
            preferred_powertrain=preferred_powertrain if spec["scenario"] == "Channel / fleet exposure" else "",
        )
        rows.append({
            "scenario": spec["scenario"],
            "evidenceNeeded": spec["evidenceNeeded"],
            "sourceOrTool": spec["sourceOrTool"],
            "acceptanceCriteria": spec["acceptanceCriteria"],
            "currentStatus": _tco_current_status(ref),
            "businessUse": spec["businessUse"],
            "priority": spec["priority"],
            "evidenceRef": str(ref.get("refId") or "") if ref else "",
            "source": str(ref.get("source") or ref.get("table") or "") if ref else "framework",
        })
    return rows


def _tco_preferred_powertrain(question: str, answer: dict[str, Any]) -> str:
    text = " ".join([
        str(question or ""),
        str(answer.get("title") or ""),
        str(answer.get("direct") or ""),
    ]).casefold()
    present = [
        fuel
        for fuel in ("bev", "phev", "hev")
        if re.search(rf"(?<![a-z0-9]){re.escape(fuel)}(?![a-z0-9])", text)
    ]
    return present[0] if len(present) == 1 else ""


def _first_ref_matching_tokens(
    refs: list[dict[str, Any]],
    tokens: tuple[str, ...],
    *,
    preferred_powertrain: str = "",
) -> dict[str, Any] | None:
    lowered_tokens = tuple(str(token or "").casefold() for token in tokens)
    preferred_powertrain = str(preferred_powertrain or "").casefold()
    best: tuple[int, int, int, dict[str, Any]] | None = None
    for index, ref in enumerate(refs):
        label = str(ref.get("label") or "").casefold()
        ref_id = str(ref.get("refId") or "").casefold()
        value = str(ref.get("value") or "").casefold()
        source = " ".join([str(ref.get("source") or ""), str(ref.get("table") or "")]).casefold()
        score = 0
        for token in lowered_tokens:
            if not token:
                continue
            if _tco_token_matches(label, token):
                score += 4
            if _tco_token_matches(ref_id, token):
                score += 3
            if _tco_token_matches(value, token):
                score += 2
            if _tco_token_matches(source, token):
                score += 1
        if score <= 0:
            continue
        if "business_pct" in label or "business_pct" in ref_id:
            score += 3
        if "private_pct" in label or "private_pct" in ref_id:
            score += 1
        if "claim" in label or "claim" in ref_id:
            score += 2
        if preferred_powertrain and any(
            token in f" {label} {ref_id} {value} "
            for token in (
                f".{preferred_powertrain}.",
                f".{preferred_powertrain}_",
                f" {preferred_powertrain} ",
            )
        ):
            score += 8
        candidate = (score, -len(label), -index, ref)
        if best is None or candidate > best:
            best = candidate
    return best[3] if best else None


def _tco_token_matches(text: str, token: str) -> bool:
    if not token:
        return False
    if len(token) <= 2 and token.isascii():
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", text))
    return token in text


def _tco_current_status(ref: dict[str, Any] | None) -> str:
    if not ref:
        return "待补可引用证据"
    ref_id = str(ref.get("refId") or "").strip()
    label = str(ref.get("label") or "").strip()
    display_label = _metric_card_display_label(label) if label else ""
    if not display_label or display_label == label:
        display_label = ref_id if ref_id and not ref_id.casefold().startswith("ev_") else label
    value = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
    if display_label and value:
        return f"已有 {display_label}: {value}"
    if display_label:
        return f"已有 {display_label}"
    if ref_id:
        return f"已有 evidenceRef: {ref_id}"
    return "已有部分证据，需补完整口径"


def _tco_validation_coverage(rows: list[dict[str, Any]]) -> str:
    ready = len([row for row in rows if str(row.get("evidenceRef") or "").strip()])
    return f"{ready}/{len(rows)} validation rows have evidence refs; missing rows remain validation requirements, not facts."


def _policy_pricing_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        lower = label.lower()
        source = str(ref.get("source") or ref.get("table") or "").lower()
        unit = str(ref.get("unit") or "").lower()
        if label in {"row_count", "metadata.result_count", "chart_count"}:
            continue
        from_price_tool = "jato_msrp_postgres" in source or "pricing" in source
        price_metric = (
            lower.startswith("pricing.records.")
            or lower.startswith("pricestats.")
            or lower.endswith((".msrp", ".price", ".avgprice", ".medianprice", ".minprice", ".maxprice"))
            or any(token in lower for token in ("main trim msrp", "own-model msrp", "current msrp", "target price"))
        )
        currency_metric = unit in {"currency", "eur", "sek"} or "currency" in unit or "eur" in unit or "sek" in unit
        if from_price_tool or (price_metric and currency_metric):
            result.append(ref)
    return result


def _business_table_rows(
    intent: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
    *,
    question: str = "",
) -> list[dict[str, Any]]:
    if intent == "market_overview":
        market_rows = _market_overview_decision_rows(evidence_package, refs, answer)
        if market_rows:
            return market_rows[:10]
    if intent == "pricing_analysis":
        pricing_rows = _pricing_decision_rows(evidence_package, refs, answer, question=question)
        if pricing_rows:
            return pricing_rows[:10]
        return []
    if intent == "competitor_compare":
        competitor_rows = _competitor_decision_rows(evidence_package, refs, answer)
        if competitor_rows:
            return competitor_rows[:10]
        return []
    if intent == "configuration_analysis":
        evidence_rows = _configuration_evidence_rows(evidence_package, refs, answer)
        if evidence_rows:
            return evidence_rows[:10]
        configuration_rows = _configuration_decision_rows(evidence_package, refs, answer)
        if configuration_rows:
            return configuration_rows[:10]
    if intent == "inventory_analysis":
        inventory_rows = _inventory_bom_decision_rows(evidence_package, refs, answer)
        if inventory_rows:
            return inventory_rows[:10]
    if intent == "news_policy_search":
        policy_rows = _policy_news_decision_rows(evidence_package, refs, answer, question=question)
        if policy_rows:
            return policy_rows[:10]
        return []
    if intent == "voc_analysis":
        voc_rows = _voc_decision_rows(evidence_package, refs, answer)
        if voc_rows:
            return voc_rows[:10]
        return []
    if intent == "report_generation":
        report_rows = _report_generation_evidence_rows(evidence_package, refs, answer, question=question)
        if report_rows:
            return report_rows[:10]
        return []
    return [_business_table_row(intent, ref) for ref in refs[:10]]


def _market_overview_decision_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    action = _first_action(answer) or "拆分到机会 segment、动力路线、竞品池和产品进入顺序。"
    confidence = str(evidence_package.get("confidence") or "medium")
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        if not label:
            continue
        dimension = _market_dimension_from_ref(label, ref)
        signal = _market_signal_from_ref(label, ref)
        key = (dimension.lower(), signal.lower())
        if key in seen:
            continue
        seen.add(key)
        evidence = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
        rows.append({
            "dimension": dimension,
            "signal": signal,
            "evidence": evidence,
            "businessImplication": _market_business_implication(dimension, signal, evidence),
            "recommendedAction": action,
            "confidence": confidence,
            "evidenceRef": str(ref.get("refId") or ""),
            "source": str(ref.get("source") or ref.get("table") or ""),
        })
    return _sort_market_overview_rows(rows)


def _market_dimension_from_ref(label: str, ref: dict[str, Any]) -> str:
    lower = f"{label} {ref.get('source') or ''} {ref.get('table') or ''}".lower()
    if any(token in lower for token in ("business", "company", "fleet", "private", "retail", "channel")):
        return "Channel mix"
    if any(token in lower for token in ("segment", "suv", "a0", "a-suv", "suv a", "suv b", "body")):
        return "Segment structure"
    if any(token in lower for token in ("bev", "phev", "hev", "ice", "powertrain", "fuel", "drive")):
        return "Powertrain mix"
    if any(token in lower for token in ("model", "ranking", "rank", "top")):
        return "Top models / competitor pull"
    if any(token in lower for token in ("month", "year", "trend", "penetration", "share", "mix")):
        return "Trend / penetration"
    if any(token in lower for token in ("sales", "volume", "registration", "market", "total")):
        return "Market size"
    return "Market evidence"


def _market_signal_from_ref(label: str, ref: dict[str, Any]) -> str:
    text = str(label or "").strip()
    value = str(ref.get("value") or "").strip()
    cross_country_signal = _cross_country_market_signal(text)
    if cross_country_signal:
        return cross_country_signal
    if text.upper() in {"BEV", "PHEV", "HEV", "ICE"}:
        return text.upper()
    parts = [part for part in re.split(r"[.>/|]", text) if part.strip()]
    if parts:
        candidate = parts[-1].replace("_", " ").strip()
        if candidate.lower() in {"value", "sales", "share", "volume"} and len(parts) >= 2:
            candidate = parts[-2].replace("_", " ").strip()
        if candidate:
            return _market_display_signal(candidate, label=text)
    return value[:64] or "market signal"


def _cross_country_market_signal(label: str) -> str:
    text = str(label or "").strip()
    match = re.match(r"crossCountry\.([^.]+)\.kpis\.cumulativeSales$", text, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 累计销量"
    match = re.match(r"crossCountry\.([^.]+)\.powertrainMix\.([^.]+)\.(?:sales|value)$", text, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} 动力销量"
    match = re.match(r"crossCountry\.([^.]+)\.powertrainMix\.([^.]+)\.([^.]+)_pct$", text, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} {match.group(3).upper()} 占比"
    return ""


def _market_display_signal(candidate: str, *, label: str = "") -> str:
    text = str(candidate or "").strip()
    compact = text.replace(" ", "").replace("_", "").casefold()
    label_text = str(label or "").casefold()
    if compact == "cumulativesales":
        return "累计销量"
    if compact in {"totalvolume", "totalsales"}:
        return "总销量"
    if compact.endswith("pct"):
        base = re.sub(r"[_\s-]*pct$", "", text, flags=re.IGNORECASE).strip()
        if "segmentbyfuel" in label_text:
            return f"{base} 渗透率".strip()
        if "registrationby" in label_text:
            return f"{base} 注册占比".strip()
        return f"{base} 占比".strip()
    if compact.endswith("share"):
        base = re.sub(r"[_\s-]*share$", "", text, flags=re.IGNORECASE).strip()
        return f"{base} 份额".strip()
    return text


def _market_business_implication(dimension: str, signal: str, evidence: str = "") -> str:
    lower = f"{dimension} {signal}".lower()
    evidence_text = str(evidence or "").strip()
    evidence_clause = f"{signal} 已有 {evidence_text}，" if evidence_text else ""
    if "bev" in lower:
        return f"{evidence_clause}验证 BEV 产品定义、价格门槛、续航/充电和公司车场景。"
    if "phev" in lower:
        return f"{evidence_clause}验证 PHEV 是否仍有 TCO、长途和无稳定充电条件下的业务理由。"
    if "hev" in lower:
        return f"{evidence_clause}验证 HEV 是否适合作为低风险、低使用门槛的主流进入路线。"
    if "suv" in lower or "segment" in lower:
        return f"{evidence_clause}把市场机会拆到 SUV A0/A/B 层级，再决定车型和版本优先级。"
    if "model" in lower or "competitor" in lower or "top" in lower:
        return f"{evidence_clause}用于锁定竞品池，后续补价格、配置和销量差异矩阵。"
    if "trend" in lower or "penetration" in lower or "share" in lower:
        return f"{evidence_clause}判断需求是在扩大、替代还是分化，避免只看单点销量。"
    if "channel" in lower or "business" in lower or "fleet" in lower:
        return f"{evidence_clause}拆分私人零售和公司车逻辑，避免一个价格方案覆盖所有场景。"
    if "market size" in lower or "累计销量" in lower or "总销量" in lower or "sales" in lower:
        return f"{evidence_clause}先判断市场体量是否值得继续拆到车型、价格带、渠道和配置动作。"
    return f"{evidence_clause}把证据转成机会判断、产品动作和下一步验证任务。"


def _sort_market_overview_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    priority = {
        "market size": 0,
        "powertrain mix": 1,
        "segment structure": 2,
        "trend / penetration": 3,
        "top models / competitor pull": 4,
        "channel mix": 5,
        "market evidence": 6,
    }
    return sorted(
        rows,
        key=lambda row: (
            priority.get(str(row.get("dimension") or "").lower(), 99),
            _market_signal_sort_priority(str(row.get("signal") or "")),
            str(row.get("signal") or ""),
        ),
    )


def _market_signal_sort_priority(signal: str) -> int:
    text = str(signal or "").strip().upper()
    if text in {"SUV A0", "A0"}:
        return 0
    if text in {"SUV A", "A"}:
        return 1
    if text in {"SUV B", "B"}:
        return 2
    powertrain_priority = {
        "BEV": 0,
        "PHEV": 1,
        "HEV": 2,
        "MHEV": 3,
        "ICE": 4,
        "REEV": 5,
    }
    if text in powertrain_priority:
        return powertrain_priority[text]
    if "SALES" in text or "VOLUME" in text or "TOTAL" in text:
        return 3
    if "PCT" in text or "%" in text or "SHARE" in text:
        return 4
    return 3


def _inventory_bom_decision_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        key = _inventory_record_key_from_ref(label, ref.get("value"))
        if not key:
            continue
        group = groups.setdefault(key, {
            "market": "",
            "model": "",
            "version": "",
            "exterior": "",
            "interior": "",
            "colorSpec": "",
            "materialCode": "",
            "availableUnits": "",
            "lifecycle": "",
            "risk": "",
            "evidenceRef": str(ref.get("refId") or ""),
            "source": str(ref.get("source") or ref.get("table") or ""),
        })
        if not group.get("evidenceRef"):
            group["evidenceRef"] = str(ref.get("refId") or "")
        if not group.get("source"):
            group["source"] = str(ref.get("source") or ref.get("table") or "")
        _merge_inventory_metric(group, ref)

    rows: list[dict[str, Any]] = []
    default_market = str(evidence_package.get("country") or "当前市场")
    default_model = _entity_label(evidence_package, fallback="目标车型")
    for group in groups.values():
        rows.append({
            "market": group["market"] or default_market,
            "model": group["model"] or default_model,
            "version": group["version"] or "待映射版本",
            "exterior": group["exterior"],
            "interior": group["interior"],
            "colorSpec": _inventory_color_spec(group),
            "materialCode": group["materialCode"] or "待补物料号",
            "availableUnits": group["availableUnits"] or "待补客户可编辑数量",
            "risk": _inventory_risk_text(group),
            "evidenceRef": group["evidenceRef"],
            "source": group["source"],
        })
    if rows:
        return _dedupe_inventory_rows(rows)
    return _inventory_framework_rows(evidence_package, refs, answer)


def _inventory_record_key_from_ref(label: str, value: Any) -> str:
    text = str(label or "").strip()
    lower = text.lower()
    parts = text.split(".")
    prefixes = (
        "inventory.records.",
        "material.records.",
        "bom.records.",
        "stock.records.",
        "order.records.",
    )
    for prefix in prefixes:
        if lower.startswith(prefix) and len(parts) >= 4:
            return ".".join(parts[2:-1]).strip()
    metric_keys = {
        "market",
        "country",
        "model",
        "version",
        "trim",
        "variant",
        "exterior",
        "interior",
        "color",
        "colour",
        "colorspec",
        "materialcode",
        "material_code",
        "sku",
        "partnumber",
        "availableunits",
        "available_units",
        "units",
        "stock",
        "quantity",
        "qty",
        "lifecycle",
        "status",
        "risk",
    }
    if len(parts) >= 2 and parts[-1].lower() in metric_keys:
        return ".".join(parts[:-1]).strip()
    if "materialcode" in lower.replace("_", "") and str(value or "").strip():
        return str(value or "").strip()
    return ""


def _merge_inventory_metric(group: dict[str, Any], ref: dict[str, Any]) -> None:
    label = str(ref.get("label") or "")
    metric = label.lower().split(".")[-1].replace("_", "")
    formatted = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
    if metric in {"market", "country"}:
        group["market"] = formatted
    elif metric == "model":
        group["model"] = formatted
    elif metric in {"version", "trim", "variant"}:
        group["version"] = formatted
    elif metric == "exterior":
        group["exterior"] = formatted
    elif metric == "interior":
        group["interior"] = formatted
    elif metric in {"color", "colour", "colorspec"}:
        group["colorSpec"] = formatted
    elif metric in {"materialcode", "sku", "partnumber"}:
        group["materialCode"] = formatted
    elif metric in {"availableunits", "units", "stock", "quantity", "qty"}:
        group["availableUnits"] = formatted
    elif metric in {"lifecycle", "status"}:
        group["lifecycle"] = formatted
    elif metric == "risk":
        group["risk"] = formatted
    elif not group.get("risk") and _looks_like_inventory_risk(label, formatted):
        group["risk"] = formatted


def _inventory_color_spec(group: dict[str, Any]) -> str:
    explicit = str(group.get("colorSpec") or "").strip()
    if explicit:
        return explicit
    exterior = str(group.get("exterior") or "").strip()
    interior = str(group.get("interior") or "").strip()
    if exterior or interior:
        return " / ".join(item for item in [exterior or "外饰待映射", interior or "内饰待映射"] if item)
    return "外饰/内饰待映射"


def _inventory_risk_text(group: dict[str, Any]) -> str:
    risk = str(group.get("risk") or "").strip()
    lifecycle = str(group.get("lifecycle") or "").strip()
    material = str(group.get("materialCode") or "").strip()
    version = str(group.get("version") or "").strip()
    if risk:
        return _inventory_risk_public_text(risk)
    if lifecycle:
        return f"生命周期状态：{lifecycle}；需确认是否可下单、可编辑数量和替代物料。"
    if material and version:
        return "需确认同一版本下物料号与颜色/市场/PI 的唯一关系。"
    if material:
        return "物料号已命中，但版本、颜色和生命周期还需补齐。"
    return "需确认版本、颜色、市场、PI、库存和生命周期关系。"


def _inventory_risk_public_text(value: str) -> str:
    text = str(value or "").strip()
    lower = text.lower()
    if "duplicate" in lower or "多个" in text or "重复" in text:
        return "同一业务版本存在多个物料号；必须按颜色、市场、PI 或生命周期拆分，不能直接合并。"
    if "missing" in lower or "缺" in text:
        return "缺少版本/颜色/库存映射；不能直接生成客户可编辑数量。"
    if "lifecycle" in lower or "生命周期" in text:
        return "生命周期状态需确认；避免历史物料号进入新订单。"
    return text


def _looks_like_inventory_risk(label: str, value: str) -> bool:
    lower = f"{label} {value}".lower()
    return any(token in lower for token in ("risk", "duplicate", "conflict", "lifecycle", "missing", "bom", "material"))


def _inventory_framework_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    if not refs and not _has_business_structure(answer, evidence_package):
        return []
    market = str(evidence_package.get("country") or "当前市场")
    model = _entity_label(evidence_package, fallback="目标车型")
    action = _first_action(answer)
    ref = _first_inventory_ref(refs)
    ref_id = str(ref.get("refId") or "") if ref else ""
    source = str(ref.get("source") or ref.get("table") or "framework") if ref else "framework"
    risk_value = _inventory_risk_public_text(str(ref.get("value") or "")) if ref else "需确认版本/物料生命周期"
    return [
        {
            "market": market,
            "model": model,
            "version": "业务版本",
            "exterior": "外饰",
            "interior": "内饰",
            "colorSpec": "外饰 + 内饰",
            "materialCode": "多个物料号需拆分",
            "availableUnits": "客户可编辑数量待计算",
            "risk": risk_value or "同一版本多个物料号需按颜色、市场、PI 或生命周期拆分。",
            "evidenceRef": ref_id,
            "source": source,
        },
        {
            "market": market,
            "model": "实体关系",
            "version": "车型版本",
            "exterior": "外饰",
            "interior": "内饰",
            "colorSpec": "外饰 + 内饰",
            "materialCode": "物料号",
            "availableUnits": "库存/订单/客户可编辑数量",
            "risk": action or "建立版本-颜色-物料号-生命周期映射后，再生成客户可编辑数量。",
            "evidenceRef": ref_id,
            "source": source,
        },
    ]


def _first_inventory_ref(refs: list[dict[str, Any]]) -> dict[str, Any] | None:
    for ref in refs:
        label = str(ref.get("label") or "")
        value = str(ref.get("value") or "")
        if _looks_like_inventory_risk(label, value):
            return ref
    return refs[0] if refs else None


def _dedupe_inventory_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        key = (
            str(row.get("market") or "").lower(),
            str(row.get("version") or "").lower(),
            str(row.get("materialCode") or "").lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _policy_news_decision_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
    *,
    question: str = "",
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    action = _first_action(answer) or "补官方来源、发布日期、资格门槛和 JATO 交叉验证"
    country = str(evidence_package.get("country") or "当前市场")
    topic_filter = _policy_source_repair_topic(question)
    for ref in refs:
        if not _is_policy_news_ref(ref):
            continue
        if not _policy_news_ref_matches_question(ref, question=question, topic_filter=topic_filter):
            continue
        label = str(ref.get("label") or "").strip()
        topic = _policy_topic_from_ref(label)
        if not topic:
            continue
        group = groups.setdefault(topic, {
            "policyTopic": _display_policy_topic(topic, country),
            "sourceDate": "",
            "source": "",
            "policyEffect": "",
            "affectedModels": "",
            "businessAction": action,
            "risk": "",
            "evidenceRef": str(ref.get("refId") or ""),
            "sourceRaw": str(ref.get("source") or ref.get("table") or ""),
        })
        if not group.get("evidenceRef"):
            group["evidenceRef"] = str(ref.get("refId") or "")
        if not group.get("sourceRaw"):
            group["sourceRaw"] = str(ref.get("source") or ref.get("table") or "")
        _merge_policy_metric(group, ref)

    rows: list[dict[str, Any]] = []
    for group in groups.values():
        rows.append({
            "policyTopic": group["policyTopic"],
            "sourceDate": group["sourceDate"] or "待补发布日期",
            "source": group["source"] or group["sourceRaw"] or "待补官方/高质量来源",
            "policyEffect": group["policyEffect"] or "待补政策影响结论",
            "affectedModels": group["affectedModels"] or "待补适用车型/价格门槛",
            "businessAction": group["businessAction"],
            "risk": group["risk"] or _policy_row_risk(group),
            "evidenceRef": group["evidenceRef"],
            "sourceRaw": group["sourceRaw"],
        })
    return _dedupe_policy_rows(rows)


def _policy_news_ref_matches_question(
    ref: dict[str, Any],
    *,
    question: str,
    topic_filter: str,
) -> bool:
    if not topic_filter:
        return True
    haystack = " ".join(
        str(ref.get(key) or "")
        for key in ("label", "value", "source", "table")
    )
    if _policy_source_text_score(haystack, topic_filter) > 0:
        return True
    question_text = str(question or "").casefold()
    if "elbilspremien" in question_text:
        folded = haystack.casefold()
        return "elbilspremien" in folded or "elbilspremie" in folded
    return False


def _is_policy_news_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    source = str(ref.get("source") or "").lower()
    table = str(ref.get("table") or "").lower()
    combined = " ".join([label, source, table])
    if (
        label in {"row_count", "metadata.result_count", "chart_count"}
        or label.endswith(".rank")
        or label.endswith(".rankseed")
        or label.endswith("rankseed")
    ):
        return False
    if any(token in combined for token in ("jato_country_snapshot", "jato_country_chart_deck", "business_method_material")):
        return False
    return any(
        token in combined
        for token in (
            "external_research",
            "web_search",
            "search_market_news",
            "pageindex",
            "minirag",
            "http://",
            "https://",
            "policy",
            "subsidy",
            "benefit",
            "tax",
            "elbil",
            "co2",
            "co₂",
            "source",
            "date",
            "claim",
            "effect",
            "impact",
            "eligibility",
            "价格上限",
            "补贴",
            "政策",
            "税",
        )
    )


def _policy_topic_from_ref(label: str) -> str:
    text = str(label or "").strip()
    lower = text.lower()
    metric_keys = {
        "source",
        "sourc",
        "url",
        "date",
        "publisheddate",
        "published",
        "retrievedat",
        "claim",
        "effect",
        "impact",
        "policyeffect",
        "policy_effect",
        "affectedmodels",
        "affected_models",
        "models",
        "eligibility",
        "pricecap",
        "price_cap",
        "title",
    }
    parts = text.split(".")
    if len(parts) >= 2 and parts[-1].lower() in metric_keys:
        return ".".join(parts[:-1]).strip()
    if any(token in lower for token in ("policy", "subsidy", "tax", "benefit", "elbil", "co2", "co₂", "补贴", "政策", "税")):
        return text
    return ""


def _display_policy_topic(topic: str, country: str) -> str:
    text = re.sub(r"^(policy|news|external|research)\.\d+\.?", "", str(topic or ""), flags=re.IGNORECASE)
    text = text.replace("_", " ").strip(". ")
    if text and not re.fullmatch(r"\d+", text):
        return text
    return f"{country} policy / news item"


def _merge_policy_metric(group: dict[str, Any], ref: dict[str, Any]) -> None:
    label = str(ref.get("label") or "")
    metric = label.lower().split(".")[-1]
    formatted = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
    if metric in {"source", "sourc", "url"}:
        group["source"] = formatted
    elif metric in {"date", "publisheddate", "published", "retrievedat"}:
        group["sourceDate"] = formatted
    elif metric == "title":
        group["policyTopic"] = formatted
    elif metric in {"effect", "impact", "policyeffect", "policy_effect", "claim"}:
        group["policyEffect"] = formatted
    elif metric in {"affectedmodels", "affected_models", "models", "eligibility", "pricecap", "price_cap"}:
        existing = str(group.get("affectedModels") or "")
        group["affectedModels"] = "；".join(item for item in [existing, formatted] if item)
    elif not group.get("policyEffect"):
        group["policyEffect"] = formatted


def _policy_row_risk(group: dict[str, Any]) -> str:
    source = str(group.get("source") or "").strip()
    date = str(group.get("sourceDate") or "").strip()
    effect = str(group.get("policyEffect") or "").strip()
    if not source or "待补" in source:
        return "缺少官方/高质量来源，不能写确定政策事实"
    if not date or "待补" in date:
        return "缺少发布日期/有效期，不能判断是否仍适用"
    if not effect or "待补" in effect:
        return "缺少车型/价格/动力影响拆解"
    return "需核对资格、价格门槛和 JATO 市场交叉验证"


def _dedupe_policy_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        topic = str(row.get("policyTopic") or "").strip().lower()
        key = topic or str(row.get("evidenceRef") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _voc_decision_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    action = _first_action(answer) or "补媒体测评、论坛评论、用户原声并按主题聚类。"
    confidence = str(evidence_package.get("confidence") or "medium")
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        if _is_voc_noise_ref(ref):
            continue
        topic = _voc_topic_from_ref(label, ref.get("value"))
        if not topic:
            continue
        group = groups.setdefault(topic, {
            "theme": _voc_theme_from_text(topic, str(ref.get("value") or ""), answer),
            "source": "",
            "evidenceSignal": "",
            "productImplication": "",
            "validationStatus": "",
            "recommendedAction": action,
            "confidence": confidence,
            "evidenceRef": str(ref.get("refId") or ""),
            "sourceRaw": str(ref.get("source") or ref.get("table") or ""),
        })
        if not group.get("evidenceRef"):
            group["evidenceRef"] = str(ref.get("refId") or "")
        if not group.get("sourceRaw"):
            group["sourceRaw"] = str(ref.get("source") or ref.get("table") or "")
        _merge_voc_metric(group, ref)

    rows: list[dict[str, Any]] = []
    for group in groups.values():
        theme = str(group.get("theme") or "VOC theme").strip()
        signal = str(group.get("evidenceSignal") or "").strip()
        rows.append({
            "theme": theme,
            "source": group["source"] or group["sourceRaw"] or "待补 VOC 来源",
            "evidenceSignal": signal or "待补可引用用户原声/媒体测评摘要",
            "productImplication": group["productImplication"] or _voc_product_implication(theme, signal),
            "validationStatus": group["validationStatus"] or _voc_validation_status(group),
            "recommendedAction": group["recommendedAction"],
            "confidence": group["confidence"],
            "evidenceRef": group["evidenceRef"],
            "sourceRaw": group["sourceRaw"],
        })
    return _dedupe_voc_rows(rows)


def _is_voc_noise_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    value = str(ref.get("value") or "").strip().lower()
    if _is_technical_count_label(label):
        return True
    if label.endswith(".rank") or label.endswith(".rankseed") or label.endswith("rankseed"):
        return True
    return value in {"0", "0.0"} and any(token in label for token in ("count", "rows", "row_count"))


def _voc_topic_from_ref(label: str, value: Any) -> str:
    text = str(label or "").strip()
    parts = text.split(".")
    metric_keys = {"source", "url", "claim", "summary", "date", "published", "title", "sentiment", "topic"}
    if len(parts) >= 2 and parts[-1].lower() in metric_keys:
        return ".".join(parts[:-1]).strip()
    value_text = str(value or "").strip()
    if value_text.startswith(("http://", "https://")):
        return text or value_text
    if text and not _is_technical_count_label(text):
        return text
    return ""


def _merge_voc_metric(group: dict[str, Any], ref: dict[str, Any]) -> None:
    label = str(ref.get("label") or "")
    metric = label.lower().split(".")[-1]
    formatted = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
    if metric in {"source", "url"} or formatted.startswith(("http://", "https://")):
        group["source"] = formatted
    elif metric in {"claim", "summary", "title", "topic"}:
        group["evidenceSignal"] = formatted
        if str(group.get("theme") or "") == "VOC source signal":
            group["theme"] = _voc_theme_from_text("", formatted, {})
        if not group.get("productImplication"):
            group["productImplication"] = _voc_product_implication(str(group.get("theme") or ""), formatted)
    elif metric in {"date", "published"}:
        group["validationStatus"] = f"来源日期：{formatted}"
    elif metric == "sentiment":
        group["validationStatus"] = f"情绪/倾向：{formatted}"
    elif not group.get("evidenceSignal"):
        group["evidenceSignal"] = formatted
        group["productImplication"] = _voc_product_implication(str(group.get("theme") or ""), formatted)


def _voc_theme_from_text(topic: str, signal: str, answer: dict[str, Any]) -> str:
    text = f"{topic} {signal}".lower()
    if not text.strip():
        text = _visual_answer_text(answer).lower()
    if any(token in text for token in ("tow", "拖车", "roof", "行李架", "winter tyre", "冬季胎", "ski", "roof box")):
        return "Nordic utility / winter use"
    if any(token in text for token in ("v2h", "home", "energy", "backup", "家庭能源", "备份")):
        return "V2H / home energy value"
    if any(token in text for token in ("service", "warranty", "dealer", "售后", "质保", "经销", "brand", "品牌")):
        return "Brand trust / service risk"
    if any(token in text for token in ("adas", "hud", "camera", "车机", "infotainment", "software", "影像")):
        return "UX / ADAS / cockpit experience"
    if any(token in text for token in ("range", "charging", "battery", "续航", "充电", "电池")):
        return "Range / charging confidence"
    return "VOC source signal"


def _voc_product_implication(theme: str, signal: str) -> str:
    text = f"{theme} {signal}".lower()
    if "utility" in text or "winter" in text or "拖车" in text or "roof" in text:
        return "把拖车钩、roof load、冬季胎/冬季包拆成 must-have、visible value 和 dealer accessory。"
    if "v2h" in text or "energy" in text:
        return "先作为高感知技术加分项验证，避免直接写成高频购买主卖点。"
    if "adas" in text or "cockpit" in text or "software" in text or "车机" in text:
        return "转成试驾检查项、OTA/车机说明和销售交付话术。"
    if "service" in text or "brand" in text or "dealer" in text or "售后" in text:
        return "用质保、经销服务覆盖和交付承诺降低新品牌信任风险。"
    if "range" in text or "charging" in text or "battery" in text:
        return "把冬季真实续航、充电速度和电池保障转成版本/话术验证项。"
    return "先保留为主题假设，继续补可追溯来源后再写成高频需求。"


def _voc_validation_status(group: dict[str, Any]) -> str:
    source = str(group.get("source") or group.get("sourceRaw") or "").strip()
    signal = str(group.get("evidenceSignal") or "").strip()
    if not source or "待补" in source:
        return "缺少可追溯来源，不能声称高频"
    if not signal or "待补" in signal:
        return "有来源但缺少可引用用户/媒体结论"
    return "可作为候选 VOC 主题，仍需频次和代表性验证"


def _dedupe_voc_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row.get("theme") or "").lower(), str(row.get("source") or "").lower())
        if not key[0] or key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _report_generation_display_refs(
    evidence_package: dict[str, Any],
    question: str,
    refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    models = _report_requested_models(evidence_package, question)
    clean_refs = [ref for ref in refs if not _is_report_generation_noise_ref(ref)]
    if len(models) < 2:
        return [ref for ref in clean_refs if not _is_report_generation_generic_market_ref(ref)]
    result: list[dict[str, Any]] = []
    for ref in clean_refs:
        if _is_report_generation_generic_market_ref(ref) or _report_generation_model_name_only_ref(ref):
            continue
        if _report_ref_mentions_any_model(ref, models) or _report_ref_matches_context(question, ref):
            result.append(ref)
    return result


def _report_generation_metric_refs(
    evidence_package: dict[str, Any],
    question: str,
    refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    models = _report_requested_models(evidence_package, question)
    if len(models) >= 2:
        return [
            ref
            for ref in _report_generation_display_refs(evidence_package, question, refs)
            if not _is_report_generation_generic_market_ref(ref)
            and not _report_generation_model_name_only_ref(ref)
        ]
    return [ref for ref in refs if not _is_report_generation_noise_ref(ref) and not _is_report_generation_generic_market_ref(ref)]


def _report_ref_mentions_any_model(ref: dict[str, Any], models: list[str]) -> bool:
    haystack = _report_model_key(
        " ".join(
            str(ref.get(key) or "")
            for key in ("label", "value", "source", "table")
        )
    )
    if not haystack:
        return False
    return any(_report_model_key(model) and _report_model_key(model) in haystack for model in models)


def _report_ref_matches_context(question: str, ref: dict[str, Any]) -> bool:
    question_text = str(question or "").casefold()
    haystack = " ".join(
        str(ref.get(key) or "")
        for key in ("label", "value", "source", "table")
    ).casefold()
    context_tokens = [
        token
        for token in ("bev", "phev", "hev", "suv", "company car", "leasing", "winter", "policy", "补贴", "冬季", "公司车")
        if token in question_text
    ]
    return bool(context_tokens) and any(token in haystack for token in context_tokens)


def _is_report_generation_generic_market_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    haystack = f"{label} {source}"
    if re.search(r"\b(?:ex30|ex40|ex60|ev3|ev9|o5|o9|j7|j8|xc40|xc60|xc90|rav4|sportage|sorento|enyaq|tayron|id\.4|id\.7)\b", label):
        return False
    generic_labels = (
        "cumulativesales",
        "avgmsrp",
        "modelcount",
        "versioncount",
        "result_count",
        "metadata.",
        "topmodels",
        "yearseries",
    )
    if any(token in label for token in generic_labels):
        return True
    return "jato_country_snapshot" in haystack and any(token in label for token in ("sales", "msrp", "count"))


def _report_generation_model_name_only_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").strip().lower()
    return label.startswith("competitor.") and label.endswith(".model")


def _report_generation_evidence_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
    *,
    question: str = "",
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    action = _report_next_action(answer) or _first_action(answer) or "把证据压成一页汇报结论和 appendix。"
    confidence = str(evidence_package.get("confidence") or "medium")
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        if _is_report_generation_noise_ref(ref):
            continue
        topic, metric = _report_generation_topic_and_metric(label)
        if not topic:
            topic = label or "report evidence"
        group = groups.setdefault(topic, {
            "section": _report_section_for_ref(evidence_package, topic, ref),
            "evidence": "",
            "source": "",
            "sourceDate": "",
            "businessUse": "",
            "nextAction": action,
            "confidence": confidence,
            "evidenceRef": str(ref.get("refId") or ""),
            "sourceRaw": str(ref.get("source") or ref.get("table") or ""),
        })
        if not group.get("evidenceRef"):
            group["evidenceRef"] = str(ref.get("refId") or "")
        if not group.get("sourceRaw"):
            group["sourceRaw"] = str(ref.get("source") or ref.get("table") or "")
        _merge_report_generation_metric(group, ref, metric)

    rows: list[dict[str, Any]] = []
    for group in groups.values():
        section = str(group.get("section") or "Supporting evidence").strip()
        evidence = str(group.get("evidence") or "").strip()
        row = {
            "section": section,
            "evidence": evidence or "待补可引用证据摘要",
            "source": _report_source_label(group),
            "businessUse": group["businessUse"] or _report_business_use(section, evidence),
            "nextAction": group["nextAction"],
            "confidence": group["confidence"],
            "evidenceRef": group["evidenceRef"],
            "sourceRaw": group["sourceRaw"],
        }
        if not _report_generation_row_matches_context(row, question):
            continue
        rows.append({
            **row,
        })
    return _dedupe_report_generation_rows(rows)


def _report_model_coverage_artifact(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> VisualArtifact | None:
    if str(evidence_package.get("intent") or "") != "report_generation":
        return None
    models = _report_requested_models(evidence_package, question)
    if len(models) < 2:
        return None
    if not _report_has_multi_model_coverage_gap(evidence_package, models, refs):
        return None
    rows = _report_model_coverage_rows(models, evidence_package, refs, answer)
    if not rows:
        return None
    raw_columns = [
        "model",
        "role",
        "coverageStatus",
        "availableEvidence",
        "missingEvidence",
        "nextAction",
        "source",
    ]
    columns = raw_columns[:7]
    return {
        "id": "artifact_report_model_coverage_table",
        "type": "table",
        "title": "Competitor report coverage matrix",
        "subtitle": "Coverage table for multi-model PPT reports; it separates available evidence from missing MSRP, configuration, battery/range and source-date inputs.",
        "data": {
            "rows": _display_table_rows(rows, columns),
            "intentAnalysis": _intent_analysis_block(evidence_package, answer, refs),
        },
        "spec": {
            "columns": columns,
            "rawColumns": raw_columns,
            "maxRows": 10,
            "sortBy": "coverageStatus",
            "businessExplanation": "Multi-model report coverage table keeps the target model and benchmark vehicles in one view, so partial evidence does not become a full competitor conclusion.",
            "columnPolicy": "Coverage rows show what is proven now and which fields must be repaired before a PPT claim becomes presentation-ready.",
        },
        "sourceEvidenceRefs": _source_evidence_refs_from_rows(rows),
    }


def _report_model_coverage_chart_artifact(
    question: str,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> VisualArtifact | None:
    if str(evidence_package.get("intent") or "") != "report_generation":
        return None
    models = _report_requested_models(evidence_package, question)
    if len(models) < 2:
        return None
    if not _report_has_multi_model_coverage_gap(evidence_package, models, refs):
        return None
    rows = _report_model_coverage_rows(models, evidence_package, refs, answer)
    chart_rows: list[dict[str, Any]] = []
    source_refs: list[str] = []
    for row in rows:
        model = str(row.get("model") or "").strip()
        if not model:
            continue
        ref_ids = [
            ref_id.strip()
            for ref_id in str(row.get("evidenceRef") or "").split(",")
            if ref_id.strip()
        ]
        if ref_ids:
            source_refs.extend(ref_id for ref_id in ref_ids if ref_id not in source_refs)
        chart_rows.append({
            "label": model,
            "value": min(len(ref_ids), 4),
            "unit": "refs",
            "series": "available evidence refs",
            "coverageStatus": str(row.get("coverageStatus") or ""),
            "missingEvidence": str(row.get("missingEvidence") or ""),
        })
    if len(chart_rows) < 2 or not any(float(row.get("value") or 0) > 0 for row in chart_rows):
        return None
    return {
        "id": "artifact_report_model_coverage_chart",
        "type": "chart",
        "title": "Competitor report evidence coverage",
        "subtitle": "Coverage chart for requested report models; zero means the PPT row is still a validation gap, not a proven competitor claim.",
        "data": chart_rows[:8],
        "spec": {
            "chartType": "bar",
            "xField": "label",
            "yField": "value",
            "seriesField": "series",
            "data": chart_rows[:8],
            "note": "Evidence coverage chart: use it with the coverage matrix before treating a competitor report as presentation-ready.",
        },
        "sourceEvidenceRefs": source_refs[:6],
    }


def _report_requested_models(evidence_package: dict[str, Any], question: str) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values: list[str] = []
    for key in ("models", "competitors"):
        raw_values = entities.get(key)
        if isinstance(raw_values, list):
            values.extend(str(item or "").strip() for item in raw_values if str(item or "").strip())
    values.extend(_report_model_mentions_from_question(question))
    return _report_unique_model_names(values)


def _report_model_mentions_from_question(question: str) -> list[str]:
    text = str(question or "")
    patterns = [
        r"\b[A-Z][A-Za-z0-9.-]{1,12}\s+(?:HEV|PHEV|BEV|EV|SUV|Recharge|E-Tech|e-tron)\b",
        r"\b(?:OMODA\s?9|OMODA9|OMODA\s?5|OMODA5|JAECOO\s?J7|JAECOO\s?J8|J8|J7|O9|O5|EX30|EX40|EX60|XC40|XC60|XC90|EV3|EV9|RAV4|Sportage|Sorento|Enyaq|ID\.4|ID\.7|Kodiaq|Tayron)\b",
    ]
    result: list[str] = []
    for pattern in patterns:
        for match in re.findall(pattern, text, flags=re.IGNORECASE):
            value = " ".join(str(match).strip().split())
            if value:
                result.append(value)
    return result


def _report_unique_model_names(values: list[str]) -> list[str]:
    result: list[tuple[str, str]] = []
    for value in values:
        name = str(value or "").strip()
        key = _report_model_key(name)
        if not key:
            continue
        if any(key == existing_key or key in existing_key for existing_key, _ in result):
            continue
        result.append((key, name))
    return [name for _, name in result[:8]]


def _report_has_multi_model_coverage_gap(
    evidence_package: dict[str, Any],
    models: list[str],
    refs: list[dict[str, Any]],
) -> bool:
    missing = _missing_names(evidence_package)
    if any(
        name in missing
        for name in (
            "competitive_or_configuration_data_unavailable",
            "configuration_delta",
            "current_msrp",
            "coverage_diagnostic:no_current_prices_for_requested_models",
        )
    ):
        return True
    return any(not _report_refs_for_model(refs, model) for model in models)


def _report_model_coverage_rows(
    models: list[str],
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    action = _first_action(answer) or _report_next_action(answer) or "补齐 MSRP、配置、续航/电池、ADAS/冬季配置和来源日期。"
    rows: list[dict[str, Any]] = []
    competitors = _report_entity_values(evidence_package, "competitors")
    for index, model in enumerate(models):
        model_refs = _report_refs_for_model(refs, model)
        available = _report_available_evidence_summary(model_refs)
        rows.append({
            "model": model,
            "role": _report_model_role(model, index=index, competitors=competitors, refs=model_refs),
            "coverageStatus": "部分覆盖" if model_refs else "待补",
            "availableEvidence": available or "待补可引用证据",
            "missingEvidence": _report_missing_evidence_summary(model_refs),
            "nextAction": _report_model_next_action(model, action, has_refs=bool(model_refs)),
            "source": _report_model_source_summary(model_refs),
            "evidenceRef": ",".join(
                str(ref.get("refId") or "")
                for ref in model_refs[:3]
                if str(ref.get("refId") or "").strip()
            ),
        })
    return rows


def _report_entity_values(evidence_package: dict[str, Any], key: str) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values = entities.get(key)
    if not isinstance(values, list):
        return []
    return [str(item or "").strip() for item in values if str(item or "").strip()]


def _report_refs_for_model(refs: list[dict[str, Any]], model: str) -> list[dict[str, Any]]:
    key = _report_model_key(model)
    if not key:
        return []
    matched: list[dict[str, Any]] = []
    for ref in refs:
        label = str(ref.get("label") or "")
        if label.lower().startswith("competitor.") and label.lower().endswith(".model"):
            continue
        value = str(ref.get("value") or "")
        source = str(ref.get("source") or ref.get("table") or "")
        haystack = _report_model_key(" ".join([label, value, source]))
        if key and key in haystack:
            if _is_report_generation_noise_ref(ref):
                continue
            matched.append(ref)
    return matched


def _report_available_evidence_summary(refs: list[dict[str, Any]]) -> str:
    if not refs:
        return ""
    labels: list[str] = []
    for ref in refs[:4]:
        label = str(ref.get("label") or "").strip()
        value = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
        labels.append(f"{label}={value}" if value else label)
    return "；".join(labels)


def _report_missing_evidence_summary(refs: list[dict[str, Any]]) -> str:
    if not refs:
        return "MSRP、版本、续航/电池、ADAS/冬季配置、来源日期"
    labels = " ".join(str(ref.get("label") or "").casefold() for ref in refs)
    missing: list[str] = []
    if not any(token in labels for token in ("msrp", "price", "pricing")):
        missing.append("MSRP/价格")
    if not any(token in labels for token in ("config", "feature", "battery", "range", "winter", "adas", "trim")):
        missing.append("配置/电池/续航")
    if not any(token in labels for token in ("date", "published", "source")):
        missing.append("来源日期")
    return "、".join(missing) if missing else "覆盖可用于汇报 appendix"


def _report_model_role(model: str, *, index: int, competitors: list[str], refs: list[dict[str, Any]]) -> str:
    model_key = _report_model_key(model)
    competitor_keys = {_report_model_key(item) for item in competitors}
    if index == 0 and model_key not in competitor_keys:
        return "目标车型"
    evidence_role = _report_model_role_from_refs(model, refs)
    if evidence_role:
        return evidence_role
    if model_key in competitor_keys:
        return "竞品"
    return "对标对象"


def _report_model_role_from_refs(model: str, refs: list[dict[str, Any]]) -> str:
    model_key = _report_model_key(model)
    if not model_key:
        return ""
    for ref in refs:
        raw_text = " ".join(
            str(ref.get(key) or "")
            for key in ("label", "value", "source", "table")
        )
        normalized = _report_model_key(raw_text)
        raw_text_lower = raw_text.casefold()
        if model_key not in normalized:
            continue
        if any(token in normalized for token in ("mainbenchmark", "primarybenchmark", "targetbenchmark", "principalbenchmark")) or any(
            token in raw_text_lower for token in ("主对标", "优先对标")
        ):
            return _report_verified_role_label("主对标", refs)
        if any(
            token in normalized
            for token in ("priceconfigurationanchor", "priceconfiganchor", "validationanchor", "priceposition")
        ) or any(token in raw_text_lower for token in ("价格/配置校验锚点", "价格配置校验锚点", "校验锚点")):
            return _report_verified_role_label("价格/配置校验锚点", refs)
    return ""


def _report_verified_role_label(role: str, refs: list[dict[str, Any]]) -> str:
    missing = _report_missing_evidence_summary(refs)
    if missing and missing != "覆盖可用于汇报 appendix":
        return f"待验证{role}"
    return role


def _report_model_next_action(model: str, action: str, *, has_refs: bool) -> str:
    if has_refs:
        return f"补齐 {model} 的 MSRP、版本、配置和来源日期后再写确定胜负。"
    return f"先补 {model} 的可引用价格、配置、续航/电池和来源日期。{action}"


def _report_model_source_summary(refs: list[dict[str, Any]]) -> str:
    sources = [
        _compact_source_label(str(ref.get("source") or ref.get("table") or ""))
        for ref in refs
        if str(ref.get("source") or ref.get("table") or "").strip()
    ]
    sources = [item for item in _dedupe_strings(sources) if item]
    return "、".join(sources[:3]) if sources else "待补来源"


def _report_model_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _is_report_generation_noise_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    value = str(ref.get("value") or "").strip().lower()
    if _is_technical_count_label(label):
        return True
    if label.endswith(".rank") or label.endswith(".rankseed") or label.endswith("rankseed"):
        return True
    return value in {"0", "0.0"} and any(token in label for token in ("count", "rows", "row_count"))


def _report_generation_row_matches_context(row: dict[str, Any], question: str) -> bool:
    if not _report_question_has_policy_context(question):
        return True
    source_text = " ".join(str(row.get(key) or "") for key in ("source", "sourceRaw")).casefold()
    if not _report_row_looks_external_source(source_text):
        return True
    haystack = " ".join(str(row.get(key) or "") for key in ("section", "evidence", "source", "sourceRaw")).casefold()
    specific_terms = _report_specific_policy_terms(question)
    if specific_terms and any(term in haystack for term in specific_terms):
        return True
    return any(term in haystack for term in _REPORT_POLICY_CONTEXT_TERMS)


_REPORT_POLICY_CONTEXT_TERMS = (
    "policy",
    "subsid",
    "incentive",
    "bonus",
    "bidrag",
    "premie",
    "tax",
    "co2",
    "co₂",
    "benefit",
    "transportstyrelsen",
    "regeringen",
    "elbil",
    "补贴",
    "政策",
    "税",
)


def _report_question_has_policy_context(question: str) -> bool:
    text = str(question or "").casefold()
    return any(term in text for term in _REPORT_POLICY_CONTEXT_TERMS)


def _report_specific_policy_terms(question: str) -> set[str]:
    text = str(question or "").casefold()
    terms = {"elbilspremien", "bonus malus", "company car benefit", "bilförmån", "bilmån", "klimatbonus"}
    return {term for term in terms if term in text}


def _report_row_looks_external_source(value: str) -> bool:
    text = str(value or "").casefold()
    return any(
        token in text
        for token in (
            "http://",
            "https://",
            ".com",
            ".se",
            ".eu",
            "reuters",
            "media",
            "web",
            "external_research",
            "search_market_news",
            "jato_web_search_service",
        )
    )


def _report_generation_topic_and_metric(label: str) -> tuple[str, str]:
    text = str(label or "").strip()
    parts = text.split(".")
    metric_keys = {
        "source",
        "url",
        "claim",
        "summary",
        "date",
        "published",
        "title",
        "effect",
        "impact",
        "value",
        "sales",
        "share",
        "volume",
        "count",
        "segment",
        "powertrain",
        "price",
        "msrp",
        "avgprice",
        "minprice",
        "maxprice",
        "battery",
        "range",
        "config",
        "feature",
        "trim",
    }
    if len(parts) >= 2 and parts[-1].lower() in metric_keys:
        return ".".join(parts[:-1]).strip(), parts[-1].lower()
    return text, ""


def _merge_report_generation_metric(group: dict[str, Any], ref: dict[str, Any], metric: str) -> None:
    label = str(ref.get("label") or "")
    display_label = _report_generation_display_label(label)
    metric_key = metric or label.lower().split(".")[-1]
    formatted = _report_generation_formatted_value(ref, metric_key)
    if metric_key in {"source", "url"} or formatted.startswith(("http://", "https://")):
        group["source"] = _compact_source_label(formatted)
    elif metric_key in {"date", "published"}:
        group["sourceDate"] = formatted[:10]
    elif metric_key in {"claim", "summary", "title", "effect", "impact", "value"}:
        _append_report_generation_evidence(group, formatted)
    elif not group.get("evidence"):
        _append_report_generation_evidence(group, f"{display_label}: {formatted}" if display_label and formatted else formatted)
    else:
        _append_report_generation_evidence(group, f"{display_label}: {formatted}" if display_label and formatted else formatted)
    if not group.get("source"):
        group["source"] = _compact_source_label(str(ref.get("source") or ref.get("table") or ""))
    if not group.get("businessUse"):
        group["businessUse"] = _report_business_use(str(group.get("section") or ""), str(group.get("evidence") or formatted))


def _append_report_generation_evidence(group: dict[str, Any], item: str) -> None:
    text = _sanitize_artifact_display_text(str(item or "").strip())
    if not text:
        return
    current = str(group.get("evidence") or "").strip()
    if not current:
        group["evidence"] = text
        return
    existing = [value.strip() for value in current.split("；") if value.strip()]
    if text not in existing:
        group["evidence"] = "；".join([*existing, text])


def _report_generation_formatted_value(ref: dict[str, Any], metric_key: str) -> str:
    unit = str(ref.get("unit") or "")
    label = str(ref.get("label") or "").lower()
    if metric_key == "count" or label.endswith(".count"):
        unit = ""
    return format_artifact_value(ref.get("value"), unit)


def _report_source_label(group: dict[str, Any]) -> str:
    source = str(group.get("source") or group.get("sourceRaw") or "").strip()
    date = str(group.get("sourceDate") or "").strip()
    compact = _sanitize_artifact_display_text(_compact_source_label(source))
    if compact and date:
        return f"{compact} · {date}"
    return compact or "待补来源"


def _report_generation_display_label(label: str) -> str:
    text = str(label or "").strip()
    if not text:
        return ""
    display_label = _metric_card_display_label(text)
    if display_label and display_label != text:
        return display_label
    return _sanitize_artifact_display_text(text)


def _report_section_for_ref(evidence_package: dict[str, Any], topic: str, ref: dict[str, Any]) -> str:
    role = _report_entity_role_for_topic(evidence_package, topic)
    if role == "target":
        return "Target model evidence"
    if role == "competitor":
        return "Competitor evidence"
    return _report_section_from_text(topic, str(ref.get("value") or ""))


def _report_entity_role_for_topic(evidence_package: dict[str, Any], topic: str) -> str:
    topic_key = _report_model_key(topic)
    if not topic_key:
        return ""
    models = [_report_model_key(item) for item in _report_entity_values(evidence_package, "models")]
    competitors = [_report_model_key(item) for item in _report_entity_values(evidence_package, "competitors")]
    if any(key and key in topic_key for key in competitors):
        return "competitor"
    if any(key and key in topic_key for key in models):
        return "target"
    return ""


def _compact_source_label(value: str) -> str:
    text = str(value or "").strip()
    match = re.search(r"https?://([^/]+)", text)
    if match:
        return match.group(1).removeprefix("www.")
    return text


def _report_section_from_text(topic: str, evidence: str) -> str:
    text = f"{topic} {evidence}".lower()
    if any(token in text for token in ("price", "msrp", "monthly", "leasing", "rv", "residual", "pva", "价", "月供")):
        return "Pricing evidence"
    if any(token in text for token in ("competitor", "benchmark", "compare", "vs", "ex30", "ev3", "rav4", "sorento", "竞品", "对标")):
        return "Competitor evidence"
    if any(token in text for token in ("policy", "tax", "subsidy", "bonus", "malus", "benefit", "政策", "补贴", "税")):
        return "Policy evidence"
    if any(token in text for token in ("config", "feature", "battery", "range", "winter", "adas", "配置", "电池", "续航", "冬季")):
        return "Product / configuration evidence"
    if any(token in text for token in ("sales", "share", "volume", "market", "segment", "bev", "hev", "phev", "销量", "份额", "市场")):
        return "Market evidence"
    if any(token in text for token in ("voc", "forum", "review", "owner", "user", "用户", "论坛", "测评")):
        return "VOC evidence"
    return "Supporting evidence"


def _report_business_use(section: str, evidence: str) -> str:
    text = f"{section} {evidence}".lower()
    if "target model" in text:
        return "标记目标车型已查证据和缺口，避免把待补数据写成确定产品结论。"
    if "pricing" in text:
        return "支撑价格走廊、版本策略、月供/RV 或价值覆盖结论。"
    if "competitor" in text:
        return "支撑主对标、价格锚点、配置校验锚点和销售替代对象判断。"
    if "policy" in text:
        return "支撑政策边界、适用对象、价格门槛和风险说明。"
    if "configuration" in text or "product" in text:
        return "支撑 must-have / visible value / nice-to-have 的产品定义。"
    if "market" in text:
        return "支撑市场空间、动力结构、细分机会和进入顺序判断。"
    if "voc" in text:
        return "支撑用户痛点、可转化卖点和后续验证任务。"
    return "作为汇报 appendix 证据，支撑 key message 和 next action。"


def _report_next_action(answer: dict[str, Any]) -> str:
    bullets = _string_list(answer.get("reportReadyBullets"))
    for item in bullets:
        if item.lower().startswith("next action"):
            return _clean_report_action_text(_split_report_bullet(item)[1])
        if item.startswith("Next action："):
            return _clean_report_action_text(_split_report_bullet(item)[1])
    return ""


def _clean_report_action_text(value: str) -> str:
    return _sanitize_artifact_display_text(str(value or "").strip()).rstrip("。.!！?？；; ")


def _dedupe_report_generation_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        key = (
            str(row.get("section") or "").lower(),
            str(row.get("evidence") or "").lower(),
            str(row.get("source") or "").lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _visual_answer_text(answer: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("title", "direct", "summary", "pmInsight", "answerPreview"):
        value = str(answer.get(key) or "").strip()
        if value:
            parts.append(value)
    for key in ("bullets", "keyTakeaways", "reportReadyBullets", "businessImplications"):
        parts.extend(_string_list(answer.get(key)))
    return " ".join(parts)


def _competitor_decision_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    pool_rows: list[dict[str, Any]] = []
    target_model = _entity_label(evidence_package, fallback="目标车型")
    for ref in refs:
        if _is_market_context_ref_for_competitor_table(ref):
            continue
        label = str(ref.get("label") or "").strip()
        lower = label.lower()
        if "competitor pool" in lower and isinstance(ref.get("value"), str):
            pool_rows.extend(_competitor_pool_rows(ref, target_model=target_model))
            continue
        model = _competitor_model_from_ref(label, ref.get("value"))
        if not model or _looks_like_non_model_competitor_group(model):
            continue
        group = groups.setdefault(model, {
            "model": model,
            "segment": "",
            "powertrain": "",
            "sales": "",
            "share": "",
            "rank": "",
            "price": "",
            "priceRange": "",
            "priceEvidenceStatus": "",
            "sourceDraftPath": "",
            "candidateDomain": "",
            "reviewPendingRows": "",
            "config": "",
            "priceValue": None,
            "minPriceValue": None,
            "maxPriceValue": None,
            "evidenceRef": str(ref.get("refId") or ""),
            "source": str(ref.get("source") or ref.get("table") or ""),
        })
        if not group.get("evidenceRef"):
            group["evidenceRef"] = str(ref.get("refId") or "")
        if not group.get("source"):
            group["source"] = str(ref.get("source") or ref.get("table") or "")
        _merge_competitor_metric(group, ref)

    target_group = next(
        (
            group
            for group in groups.values()
            if _same_business_label(str(group.get("model") or ""), target_model)
        ),
        None,
    )
    competitor_groups = [
        group
        for group in groups.values()
        if not _same_business_label(str(group.get("model") or ""), target_model)
    ]
    market_context_rows = _competitor_market_context_framework_rows(evidence_package)
    if market_context_rows and not any(_competitor_group_has_decision_signal(group) for group in groups.values()):
        return _dedupe_competitor_rows(
            [*market_context_rows, *_competitor_market_context_repair_rows(evidence_package)]
        )
    rows: list[dict[str, Any]] = []
    for group in groups.values():
        row = {
            "model": group["model"],
            "segment": group["segment"] or "待确认",
            "powertrain": group["powertrain"] or _infer_powertrain(str(group["model"])),
            "keyAdvantage": _competitor_key_advantage(group),
            "gapVsOj": _competitor_gap_text(
                group,
                target_model,
                target_group=target_group,
                competitor_groups=competitor_groups,
            ),
            "productImplication": _competitor_implication_text(
                group,
                target_model=target_model,
                target_group=target_group,
                competitor_groups=competitor_groups,
            ),
            "priceEvidence": _competitor_price_evidence_text(group),
            "evidenceRef": group["evidenceRef"],
            "source": group["source"],
        }
        rows.append(_apply_competitor_role_to_row(row, evidence_package, answer, target_model=target_model))
    rows.extend(_missing_competitor_role_rows(evidence_package, answer, rows=[*rows, *pool_rows], target_model=target_model))
    return _dedupe_competitor_rows([*rows, *pool_rows])


def _competitor_group_has_decision_signal(group: dict[str, Any]) -> bool:
    if not group:
        return False
    signal_keys = (
        "segment",
        "powertrain",
        "sales",
        "share",
        "rank",
        "price",
        "priceRange",
        "config",
        "priceValue",
        "minPriceValue",
        "maxPriceValue",
    )
    return any(group.get(key) not in (None, "") for key in signal_keys)


def _competitor_market_context_repair_rows(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    target_model = _entity_label(evidence_package, fallback="目标车型")
    competitor_label = _competitor_label(evidence_package, fallback="核心竞品")
    return [
        {
            "model": target_model,
            "segment": "待补车型级",
            "powertrain": "待补",
            "keyAdvantage": "目标车型价格、配置、销量证据待补",
            "gapVsOj": f"不能只凭市场场景判定已胜出；仍需 {target_model} 直接车型证据",
            "priceEvidence": "待补官方 MSRP / 月供 / RV",
            "productImplication": f"补 {target_model} 官方 MSRP、配置、月供/RV 和版本证据。",
            "evidenceRef": "",
            "source": "framework",
        },
        {
            "model": competitor_label,
            "segment": "待补车型级",
            "powertrain": "待补",
            "keyAdvantage": "直接竞品价格/配置/TCO 证据待补",
            "gapVsOj": "市场场景证据只说明验证方向，不能替代直接竞品矩阵",
            "priceEvidence": "待补官方 MSRP / 月供 / RV",
            "productImplication": f"把 {target_model}/{competitor_label} 放进价格、配置、TCO 和渠道场景同一张表。",
            "evidenceRef": "",
            "source": "framework",
        },
    ]


def _apply_competitor_role_to_row(
    row: dict[str, Any],
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
    *,
    target_model: str,
) -> dict[str, Any]:
    role = _competitor_role_for_model(str(row.get("model") or ""), evidence_package, answer, target_model=target_model)
    if not role:
        return row
    updated = dict(row)
    if not updated.get("segment") or updated.get("segment") == "待确认":
        updated["segment"] = role["segment"]
    if role["keyAdvantage"] not in str(updated.get("keyAdvantage") or ""):
        current_advantage = str(updated.get("keyAdvantage") or "").strip()
        updated["keyAdvantage"] = "；".join(item for item in (role["keyAdvantage"], current_advantage) if item)
    updated["gapVsOj"] = role["gapVsOj"]
    updated["productImplication"] = role["productImplication"]
    return updated


def _missing_competitor_role_rows(
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
    *,
    rows: list[dict[str, Any]],
    target_model: str,
) -> list[dict[str, Any]]:
    existing = {_visual_model_key(str(row.get("model") or "")) for row in rows}
    result: list[dict[str, Any]] = []
    for model in _competitor_role_candidate_models(evidence_package, answer, target_model=target_model):
        key = _visual_model_key(model)
        if not key or key in existing:
            continue
        role = _competitor_role_for_model(model, evidence_package, answer, target_model=target_model)
        if not role:
            continue
        model_refs = _competitor_direct_model_refs(model, evidence_package)
        result.append({
            "model": model,
            "segment": role["segment"],
            "powertrain": _infer_powertrain(f"{model} {target_model}"),
            "keyAdvantage": role["keyAdvantage"],
            "gapVsOj": role["gapVsOj"],
            "productImplication": role["productImplication"],
            "evidenceRef": ",".join(
                str(ref.get("refId") or "")
                for ref in model_refs[:3]
                if str(ref.get("refId") or "").strip()
            ),
            "source": _competitor_model_source_summary(model_refs) or "role_hypothesis_missing_direct_evidence",
        })
    return result


def _competitor_role_candidate_models(
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
    *,
    target_model: str,
) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values: list[str] = []
    competitors = entities.get("competitors")
    if isinstance(competitors, list):
        values.extend(str(item or "").strip() for item in competitors)
    values.extend(_model_mentions_from_question(_visual_answer_text(answer)))
    target_key = _visual_model_key(target_model)
    result: list[str] = []
    for value in _dedupe_strings(values):
        key = _visual_model_key(value)
        if not value or not key:
            continue
        if key == target_key or (len(key) >= 2 and bool(target_key) and (key in target_key or target_key in key)):
            continue
        result.append(value)
    return result


def _competitor_role_for_model(
    model: str,
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
    *,
    target_model: str,
) -> dict[str, str]:
    text = _visual_competitor_role_text(evidence_package, answer)
    model_key = _visual_model_key(model)
    if not text or not model_key or model_key not in _visual_model_key(text):
        return {}
    has_direct_evidence = bool(_competitor_direct_model_refs(model, evidence_package))
    model_pattern = re.escape(str(model).strip())
    primary_pattern = rf"{model_pattern}[^。；，,\n]{{0,36}}(?:主对标|优先|目标用户|品牌心智|产品定位)"
    validation_pattern = rf"{model_pattern}[^。；，,\n]{{0,42}}(?:校验|锚点|价格带|配置价值|替代理由)"
    if re.search(primary_pattern, text, flags=re.IGNORECASE):
        return {
            "segment": "主对标",
            "keyAdvantage": (
                "主对标角色：用于判断目标用户、品牌心智和产品定位。"
                if has_direct_evidence
                else "待验证主对标角色：用于判断目标用户、品牌心智和产品定位；直接车型证据待补。"
            ),
            "gapVsOj": f"与 {target_model} 的价格带、配置可赢点和短板仍需量化。",
            "productImplication": "作为主对标，先拆目标用户、价格带、配置可赢点和销售短板。",
        }
    if re.search(validation_pattern, text, flags=re.IGNORECASE):
        return {
            "segment": "价格/配置校验锚点",
            "keyAdvantage": (
                "校验锚点：用于验证价格带、配置价值和购买替代理由。"
                if has_direct_evidence
                else "待验证校验锚点：用于验证价格带、配置价值和购买替代理由；直接车型证据待补。"
            ),
            "gapVsOj": f"与 {target_model} 的官方 MSRP、配置、月供/RV 和场景替代关系待补。",
            "productImplication": "作为价格/配置校验锚点，补 MSRP、配置、月供/RV 后判断价差是否成立。",
        }
    return {}


def _competitor_direct_model_refs(model: str, evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    model_key = _visual_model_key(model)
    if not model_key:
        return []
    refs: list[dict[str, Any]] = []
    for ref in _evidence_refs(evidence_package):
        if _is_competitor_model_only_ref(ref):
            continue
        haystack = _visual_model_key(
            " ".join(
                str(ref.get(key) or "")
                for key in ("label", "value", "source", "table")
            )
        )
        if model_key and model_key in haystack:
            refs.append(ref)
    return refs


def _competitor_model_source_summary(refs: list[dict[str, Any]]) -> str:
    sources = [
        _compact_source_label(str(ref.get("source") or ref.get("table") or ""))
        for ref in refs
        if str(ref.get("source") or ref.get("table") or "").strip()
    ]
    sources = [source for source in _dedupe_strings(sources) if source]
    return "、".join(sources[:3])


def _visual_competitor_role_text(evidence_package: dict[str, Any], answer: dict[str, Any]) -> str:
    return _visual_answer_text(answer)


def _competitor_pool_rows(ref: dict[str, Any], *, target_model: str) -> list[dict[str, Any]]:
    value = str(ref.get("value") or "")
    names = [
        item.strip()
        for item in re.split(r"[,，/、;；]", value)
        if item.strip()
    ]
    rows: list[dict[str, Any]] = []
    for name in names[:6]:
        rows.append({
            "model": name,
            "segment": "用户材料竞品池",
            "powertrain": _infer_powertrain(f"{name} {target_model}"),
            "keyAdvantage": "已被用户材料列为核心对标对象",
            "gapVsOj": "价格、配置和使用场景差异待量化",
            "productImplication": "作为竞品池行，用于确认正面对抗、错位竞争或价格锚点。",
            "evidenceRef": str(ref.get("refId") or ""),
            "source": str(ref.get("source") or ref.get("table") or "business_method_material"),
        })
    return rows


def _competitor_model_from_ref(label: str, value: Any) -> str:
    text = str(label or "").strip()
    lower = text.lower()
    if lower.startswith("competitor.") and lower.endswith(".model"):
        return str(value or "").strip()
    for prefix in ("sales.rankings.", "pricing.records."):
        if lower.startswith(prefix):
            parts = text.split(".")
            return parts[2].strip() if len(parts) >= 4 else ""
    metric_keys = {
        "sales", "value", "share", "rank", "volume", "count",
        "avgprice", "minprice", "maxprice", "price", "msrp", "pricerecords",
        "priceevidencestatus", "priceevidencerole", "sourcedraftpath", "candidatedomain",
        "candidatesourcetype", "materializationstatus", "materializationreadinessscore",
        "reviewpendingrows", "currentpricerows",
        "4wd_sales", "business_sales", "private_sales",
        "segment", "powertrain", "configuration", "config",
    }
    parts = text.split(".")
    if len(parts) >= 2 and parts[-1].lower() in metric_keys:
        return ".".join(parts[:-1]).strip()
    return ""


def _merge_competitor_metric(group: dict[str, Any], ref: dict[str, Any]) -> None:
    label = str(ref.get("label") or "").strip()
    lower = label.lower()
    formatted = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
    metric = lower.split(".")[-1]
    numeric = _number_value(ref.get("value"))
    if metric == "segment":
        group["segment"] = formatted
    elif metric == "powertrain":
        group["powertrain"] = formatted
    elif metric in {"sales", "value", "volume", "count"}:
        if numeric is not None and numeric > 0:
            group["sales"] = formatted
    elif metric == "share":
        group["share"] = formatted
    elif metric == "rank":
        group["rank"] = formatted
    elif metric in {"avgprice", "price", "msrp"} and not _pricing_ref_value_is_status(ref):
        group["price"] = formatted
        group["priceValue"] = numeric
    elif metric in {"avgprice", "price", "msrp"}:
        group["priceEvidenceStatus"] = str(ref.get("value") or "").strip()
    elif metric in {"minprice", "maxprice"} and not _pricing_ref_value_is_status(ref):
        existing = str(group.get("priceRange") or "")
        group["priceRange"] = f"{existing} / {formatted}".strip(" /")
        if metric == "minprice":
            group["minPriceValue"] = numeric
        else:
            group["maxPriceValue"] = numeric
    elif metric in {"minprice", "maxprice"}:
        group["priceEvidenceStatus"] = str(ref.get("value") or "").strip()
    elif metric == "priceevidencestatus":
        group["priceEvidenceStatus"] = str(ref.get("value") or "").strip()
    elif metric == "sourcedraftpath":
        group["sourceDraftPath"] = formatted
    elif metric == "candidatedomain":
        group["candidateDomain"] = formatted
    elif metric == "reviewpendingrows":
        group["reviewPendingRows"] = formatted
    elif metric in {"configuration", "config"}:
        group["config"] = formatted
    elif metric == "pricerecords":
        group["price"] = f"{formatted} price records"


def _competitor_key_advantage(group: dict[str, Any]) -> str:
    parts: list[str] = []
    if group.get("sales"):
        parts.append(f"Sales {group['sales']}")
    if group.get("share"):
        parts.append(f"Share {group['share']}")
    if group.get("rank"):
        parts.append(f"Rank {group['rank']}")
    if group.get("priceRange"):
        parts.append(f"Price range {group['priceRange']}")
    elif group.get("price"):
        parts.append(f"Price {group['price']}")
    if group.get("config"):
        parts.append(f"Config {group['config']}")
    if parts:
        return "；".join(parts[:3])
    if group.get("priceEvidenceStatus") or group.get("sourceDraftPath"):
        return "价格来源待物化，尚不能作为已验证 MSRP / 销量 / 配置优势。"
    return "待补销量/MSRP/配置差异证据。"


def _competitor_gap_text(
    group: dict[str, Any],
    target_model: str,
    *,
    target_group: dict[str, Any] | None,
    competitor_groups: list[dict[str, Any]],
) -> str:
    price_gap = _competitor_price_gap_text(group, target_model, target_group=target_group, competitor_groups=competitor_groups)
    if price_gap:
        return price_gap
    if group.get("price") or group.get("priceRange"):
        return f"与 {target_model} 的价格带和配置价值差异待量化"
    if group.get("sales") or group.get("share"):
        return f"与 {target_model} 的销量/场景强弱可对标"
    if group.get("config"):
        return f"与 {target_model} 的配置可感知价值待拆解"
    return "待补价格、配置、销量或场景证据"


def _competitor_implication_text(
    group: dict[str, Any],
    *,
    target_model: str,
    target_group: dict[str, Any] | None,
    competitor_groups: list[dict[str, Any]],
) -> str:
    price_implication = _competitor_price_implication_text(
        group,
        target_model=target_model,
        target_group=target_group,
        competitor_groups=competitor_groups,
    )
    if price_implication:
        return price_implication
    if group.get("price") or group.get("priceRange"):
        return "用于判断正面对抗、错位定价或高配价值支撑。"
    if group.get("sales") or group.get("share"):
        return "用于判断竞品池优先级和主销场景。"
    if group.get("config"):
        return "用于拆解可感知配置差异和销售话术。"
    return "先锁定竞品池，再补价格/配置/销量矩阵。"


def _competitor_price_gap_text(
    group: dict[str, Any],
    target_model: str,
    *,
    target_group: dict[str, Any] | None,
    competitor_groups: list[dict[str, Any]],
) -> str:
    model = str(group.get("model") or "")
    group_value = _pricing_group_center_value(group)
    target_value = _pricing_group_center_value(target_group or {})
    is_target = _same_business_label(model, target_model)
    if is_target:
        low, high = _pricing_competitor_value_range(competitor_groups)
        if group_value is None or low is None or high is None:
            return ""
        low_text = format_artifact_value(low, "")
        high_text = format_artifact_value(high, "")
        if group_value < low:
            return f"低于已查竞品价格下沿 {low_text}，价格进入风险低但价值感待验证"
        if group_value > high:
            return f"高于已查竞品价格上沿 {high_text}，溢价理由待验证"
        return f"位于已查竞品价格带 {low_text}-{high_text} 内，可做正面对标验证"
    if group_value is None or target_value is None:
        return ""
    delta = group_value - target_value
    delta_text = format_artifact_value(abs(delta), "")
    if delta > 0:
        return f"高于 {target_model} {delta_text}，作为上方价格锚点"
    if delta < 0:
        return f"低于 {target_model} {delta_text}，作为低价风险锚点"
    return f"与 {target_model} 价格接近，适合做正面对标锚点"


def _competitor_price_implication_text(
    group: dict[str, Any],
    *,
    target_model: str,
    target_group: dict[str, Any] | None,
    competitor_groups: list[dict[str, Any]],
) -> str:
    model = str(group.get("model") or "")
    group_value = _pricing_group_center_value(group)
    target_value = _pricing_group_center_value(target_group or {})
    is_target = _same_business_label(model, target_model)
    if is_target:
        low, high = _pricing_competitor_value_range(competitor_groups)
        if group_value is None or low is None or high is None:
            return ""
        if group_value < low:
            return "目标车型可作为低位切入/价格锚点，但必须用配置、质保、月供/RV 和销售话术证明低价不等于低价值。"
        if group_value > high:
            return "目标车型需要明确高配、尺寸、动力、品牌或 TCO 溢价理由；否则应拆高低配或下调目标价。"
        return "目标车型已进入竞品价格带，下一步重点是配置差异、主销版本和成交支持。"
    if group_value is None or target_value is None:
        return ""
    if group_value > target_value:
        return f"用 {model} 验证 {target_model} 的低价是否仍有配置价值、TCO 和品牌信任支撑。"
    if group_value < target_value:
        return f"{model} 是低价风险锚点，需要验证 {target_model} 是否需要入门版、促销或配置补偿。"
    return f"{model} 可作为 {target_model} 的正面对标锚点，继续补配置、月供/RV 和销售话术。"


def _competitor_price_evidence_text(group: dict[str, Any]) -> str:
    price = str(group.get("price") or "").strip()
    price_range = str(group.get("priceRange") or "").strip()
    if price and price_range:
        return f"当前价格 {price}；价格区间 {price_range}"
    if price:
        return f"当前价格 {price}"
    if price_range:
        return f"价格区间 {price_range}"
    status = str(group.get("priceEvidenceStatus") or "").strip()
    if status == "source_draft_available":
        draft = str(group.get("sourceDraftPath") or "").strip()
        return f"有官方价格源草稿待物化{': ' + draft if draft else ''}"
    if status == "candidate_search_query":
        domain = str(group.get("candidateDomain") or "").strip()
        return f"需检索/确认官方价格源{': ' + domain if domain else ''}"
    if status in {"no_current_prices_for_requested_models", "current_price_missing"}:
        return "当前 MSRP 缺失，不能生成确定价格结论"
    if status:
        return status
    return "价格证据待补"


def _dedupe_competitor_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        model = str(row.get("model") or "").strip()
        key = model.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _looks_like_non_model_competitor_group(value: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "", str(value or "").lower())
    return normalized in {"competitiveanalysis", "targetmodel", "competitor", "competitors"}


def _configuration_evidence_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    target_model = _entity_label(evidence_package, fallback="目标车型")
    competitor = _competitor_entity_label(evidence_package)
    actions = _action_texts(answer)
    groups: dict[str, dict[str, Any]] = {}
    for ref in refs:
        feature = _configuration_feature_from_ref(str(ref.get("label") or ""))
        if not feature:
            continue
        group = groups.setdefault(feature, {
            "feature": feature,
            "targetValue": "",
            "competitorValue": "",
            "gap": "",
            "priority": "",
            "value": "",
            "evidenceRef": str(ref.get("refId") or ""),
            "source": str(ref.get("source") or ref.get("table") or ""),
        })
        if not group.get("evidenceRef"):
            group["evidenceRef"] = str(ref.get("refId") or "")
        if not group.get("source"):
            group["source"] = str(ref.get("source") or ref.get("table") or "")
        _merge_configuration_metric(group, ref)

    rows: list[dict[str, Any]] = []
    for group in groups.values():
        feature = group["feature"]
        rows.append({
            "feature": feature,
            "targetModel": target_model,
            "validationData": _configuration_gap_text(group),
            "sourceOrTool": _configuration_source_or_tool(group),
            "acceptanceCriteria": _configuration_acceptance_criteria(feature),
            "currentStatus": _configuration_current_status(group["evidenceRef"]),
            "priority": group["priority"] or _configuration_priority(group),
            "evidenceRef": group["evidenceRef"],
            "source": group["source"],
        })
    return _dedupe_configuration_rows(rows)


def _configuration_feature_from_ref(label: str) -> str:
    text = str(label or "").strip()
    lower = text.lower()
    for prefix in ("configuration_delta.", "common_feature."):
        if lower.startswith(prefix):
            return text.split(".", 1)[1].strip()
    visible_token = "visible feature value."
    if visible_token in lower:
        return text[text.lower().index(visible_token) + len(visible_token):].strip()
    metric_keys = {"targetvalue", "competitorvalue", "gap", "priority", "customer_value", "customervalue"}
    parts = text.split(".")
    if len(parts) >= 2 and parts[-1].lower() in metric_keys:
        return ".".join(parts[:-1]).strip()
    return ""


def _merge_configuration_metric(group: dict[str, Any], ref: dict[str, Any]) -> None:
    label = str(ref.get("label") or "").strip()
    metric = label.lower().split(".")[-1]
    formatted = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
    if metric == "targetvalue":
        group["targetValue"] = formatted
    elif metric == "competitorvalue":
        group["competitorValue"] = formatted
    elif metric == "gap":
        group["gap"] = formatted
    elif metric == "priority":
        group["priority"] = formatted
    elif metric in {"customer_value", "customervalue"}:
        group["value"] = formatted
    elif not group.get("value"):
        group["value"] = formatted


def _configuration_gap_text(group: dict[str, Any]) -> str:
    if group.get("gap"):
        return str(group["gap"])
    target = str(group.get("targetValue") or "").strip()
    competitor = str(group.get("competitorValue") or "").strip()
    if target or competitor:
        return f"目标：{target or '待补'}；竞品：{competitor or '待补'}"
    return str(group.get("value") or "待补配置差异")


def _configuration_customer_value(group: dict[str, Any], actions: list[str]) -> str:
    value = str(group.get("value") or "").strip()
    if value and value != str(group.get("gap") or "").strip():
        return value
    if actions:
        return actions[0]
    feature = str(group.get("feature") or "").lower()
    if any(token in feature for token in ("battery", "kwh", "range", "电池", "续航")):
        return "续航安心感和高配价值边界"
    if any(token in feature for token in ("winter", "heat", "tyre", "tire", "冬季", "热泵")):
        return "低温可用性和北欧日常安心感"
    if any(token in feature for token in ("hud", "camera", "adas", "seat", "天窗", "影像", "座椅")):
        return "销售可感知高配价值"
    return "用于判断主销配置和销售话术"


def _configuration_priority(group: dict[str, Any]) -> str:
    feature = str(group.get("feature") or "").lower()
    if any(token in feature for token in ("battery", "kwh", "winter", "heat", "range", "电池", "冬季", "续航")):
        return "P0"
    return "P1"


def _configuration_source_or_tool(group: dict[str, Any]) -> str:
    source = str(group.get("source") or "").strip()
    if source and source != "framework":
        return source
    feature = str(group.get("feature") or "").lower()
    if any(token in feature for token in ("price", "msrp", "pva", "value", "价格")):
        return "query_msrp_pricing + configuration/PVA source"
    if any(token in feature for token in ("battery", "kwh", "range", "charging", "800v", "电池", "续航", "充电")):
        return "compare_vehicle_variants + engineering config matrix"
    if any(token in feature for token in ("winter", "heat", "tyre", "tire", "tow", "roof", "冬季", "热泵", "拖车")):
        return "configuration matrix + Nordic VOC/source check"
    return "compare_vehicle_variants / engineering config matrix"


def _configuration_acceptance_criteria(feature: str) -> str:
    lower = str(feature or "").lower()
    if any(token in lower for token in ("80kwh", "80 kwh", "battery", "range", "电池", "续航")):
        return "证明冬季真实续航、价格/重量压力和竞品长续航版差距"
    if any(token in lower for token in ("95kwh", "95 kwh", "dual motor", "800v", "双电机")):
        return "证明续航、牵引/四驱、补能效率和高配价格带能覆盖成本"
    if any(token in lower for token in ("winter", "heat", "tyre", "tire", "冬季", "热泵")):
        return "证明低温可用性、舒适配置和北欧竞品标配/选装边界"
    if any(token in lower for token in ("tow", "roof", "v2h", "拖车")):
        return "证明北欧场景需求频次、配置可用性和销售可感知价值"
    return "证明该配置能转成用户价值、版本策略或销售话术"


def _configuration_current_status(evidence_ref: str) -> str:
    ref = str(evidence_ref or "").strip()
    if ref:
        return f"已有 evidenceRef: {ref}"
    return "待补竞品配置/价格证据"


def _competitor_entity_label(evidence_package: dict[str, Any]) -> str:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    competitors = entities.get("competitors")
    if isinstance(competitors, list):
        values = [str(item or "").strip() for item in competitors if str(item or "").strip()]
        if values:
            return ", ".join(values[:3])
    return "核心竞品"


def _configuration_decision_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    text = _answer_business_text(answer).lower()
    actions = _action_texts(answer)
    model = _entity_label(evidence_package, fallback="目标车型")
    config_ref = _first_configuration_ref(refs)
    ref_id = str(config_ref.get("refId") or "") if config_ref else ""
    source = str(config_ref.get("source") or config_ref.get("table") or "") if config_ref else "framework"
    rows: list[dict[str, Any]] = _configuration_market_context_rows(refs, model, text)

    if "80kwh" in text or "80 kwh" in text:
        feature = "80kWh long-range battery"
        rows.append({
            "feature": feature,
            "targetModel": model,
            "validationData": "冬季真实续航、竞品长续航版、重量、成本、MSRP/价格压力",
            "sourceOrTool": "compare_vehicle_variants + query_msrp_pricing",
            "acceptanceCriteria": _configuration_acceptance_criteria(feature),
            "currentStatus": _configuration_current_status(ref_id),
            "priority": "P0",
            "evidenceRef": ref_id,
            "source": source,
        })
    if "冬季包" in text or "winter package" in text or "winter" in text or "热泵" in text:
        feature = "Nordic winter package"
        rows.append({
            "feature": feature,
            "targetModel": model,
            "validationData": "热泵、电池预热、座椅/方向盘加热、冬季胎/TPMS、竞品标配/选装",
            "sourceOrTool": "configuration matrix + Nordic VOC/source check",
            "acceptanceCriteria": _configuration_acceptance_criteria(feature),
            "currentStatus": _configuration_current_status(ref_id),
            "priority": "P0",
            "evidenceRef": ref_id,
            "source": source,
        })
    if "95kwh" in text or "95 kwh" in text or "800v" in text or "双电机" in text:
        feature = "95kWh + dual motor + 800V architecture"
        rows.append({
            "feature": feature,
            "targetModel": model,
            "validationData": "续航、牵引/四驱、补能效率、成本、竞品高配价格带",
            "sourceOrTool": "compare_vehicle_variants + engineering config matrix + pricing corridor",
            "acceptanceCriteria": _configuration_acceptance_criteria(feature),
            "currentStatus": _configuration_current_status(ref_id),
            "priority": "P0",
            "evidenceRef": ref_id,
            "source": source,
        })
    if "拖车" in text or "roof" in text or "v2h" in text or "visible value" in text:
        feature = "Visible Nordic value features"
        rows.append({
            "feature": feature,
            "targetModel": model,
            "validationData": "拖车钩、roof load、V2H/快充、影像、远程预热、用户场景来源",
            "sourceOrTool": "configuration matrix + VOC/external source check",
            "acceptanceCriteria": _configuration_acceptance_criteria(feature),
            "currentStatus": _configuration_current_status(ref_id),
            "priority": "P1",
            "evidenceRef": ref_id,
            "source": source,
        })
    if rows:
        action = actions[0] if actions else "生成配置差异矩阵和主销配置建议"
        rows.append({
            "feature": "Version strategy decision",
            "targetModel": model,
            "validationData": "低配价格锚点、高配价值覆盖、PVA/配置价值、竞品版本梯度",
            "sourceOrTool": "pricing corridor + configuration/PVA matrix",
            "acceptanceCriteria": "证明高配价差能被可感知配置价值覆盖，并且低配仍保留价格锚点",
            "currentStatus": action,
            "priority": "P1",
            "evidenceRef": ref_id,
            "source": source,
        })
        return _dedupe_configuration_rows(_prioritize_configuration_decision_rows(rows))
    return []


def _configuration_market_context_rows(
    refs: list[dict[str, Any]],
    target_model: str,
    context_text: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        if not label or not _is_configuration_market_context_ref(ref):
            continue
        display_label = _metric_card_display_label(label) or label
        numeric_value = _number_value(ref.get("value"))
        value = format_artifact_value(numeric_value if numeric_value is not None else ref.get("value"), str(ref.get("unit") or ""))
        ref_id = str(ref.get("refId") or "").strip()
        key = f"{display_label.casefold()}|{value}"
        if key in seen:
            continue
        row = _configuration_market_context_row(
            display_label,
            value,
            ref_id,
            str(ref.get("source") or ref.get("table") or ""),
            target_model,
            context_text,
        )
        if not row:
            continue
        seen.add(key)
        rows.append(row)
    rows.sort(key=_configuration_market_context_priority)
    return rows[:3]


def _is_configuration_market_context_ref(ref: dict[str, Any]) -> bool:
    if not _is_market_structural_ref(ref):
        return False
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    haystack = f"{label} {source}"
    return any(
        token in haystack
        for token in (
            "bev",
            "phev",
            "suv a",
            "suva",
            "4wd",
            "powertrainmix",
            "drivebysegment",
            "segmentbyfuel",
            "crosscountry",
        )
    )


def _configuration_market_context_row(
    display_label: str,
    value: str,
    ref_id: str,
    source: str,
    target_model: str,
    context_text: str,
) -> dict[str, Any] | None:
    text = str(display_label or "")
    acceptance = _configuration_market_context_acceptance(text, context_text)
    if "SUV" in text and "4WD" in text:
        return {
            "feature": f"市场场景证据 · {text}",
            "targetModel": target_model,
            "validationData": f"{text} = {value}",
            "sourceOrTool": source or "build_market_chart",
            "acceptanceCriteria": acceptance or "支持四驱、冬季通过性和高配价值方向，但不能替代车型配置矩阵。",
            "currentStatus": _configuration_current_status(ref_id),
            "priority": "P1",
            "evidenceRef": ref_id,
            "source": source,
        }
    if "SUV" in text and ("BEV" in text or "PHEV" in text):
        return {
            "feature": f"市场场景证据 · {text}",
            "targetModel": target_model,
            "validationData": f"{text} = {value}",
            "sourceOrTool": source or "build_market_chart",
            "acceptanceCriteria": acceptance or "支持该动力路线在目标 SUV 细分有验证价值，但仍需竞品配置和价格带证明。",
            "currentStatus": _configuration_current_status(ref_id),
            "priority": "P1",
            "evidenceRef": ref_id,
            "source": source,
        }
    if "BEV" in text and ("动力销量" in text or "销量" in text):
        return {
            "feature": f"市场场景证据 · {text}",
            "targetModel": target_model,
            "validationData": f"{text} = {value}",
            "sourceOrTool": source or "build_market_chart",
            "acceptanceCriteria": acceptance or "支持高价值 BEV 架构继续验证，但不能证明 95kWh、双电机或 800V 已是必选。",
            "currentStatus": _configuration_current_status(ref_id),
            "priority": "P1",
            "evidenceRef": ref_id,
            "source": source,
        }
    if "PHEV" in text and ("动力销量" in text or "销量" in text):
        return {
            "feature": f"市场场景证据 · {text}",
            "targetModel": target_model,
            "validationData": f"{text} = {value}",
            "sourceOrTool": source or "build_market_chart",
            "acceptanceCriteria": acceptance or "提示公司车/长途/TCO 场景重要，但不替代 BEV 配置成本收益验证。",
            "currentStatus": _configuration_current_status(ref_id),
            "priority": "P2",
            "evidenceRef": ref_id,
            "source": source,
        }
    return None


def _configuration_market_context_acceptance(display_label: str, context_text: str) -> str:
    context = str(context_text or "").lower()
    label = str(display_label or "")
    if "冬季包" in context or "winter package" in context:
        return "支持北欧 BEV/冬季使用场景继续验证，但不能证明具体冬季包配置已是标配或高频需求。"
    if "95kwh" in context or "95 kwh" in context or "800v" in context or "双电机" in context:
        return "支持高价值 BEV 架构继续验证，但不能证明 95kWh、双电机或 800V 已是必选。"
    if "80kwh" in context or "80 kwh" in context:
        if "4WD" in label:
            return "支持四驱、冬季通过性和高配价值方向，但不能证明 80kWh 应全系标配。"
        return "支持 A0/A SUV BEV 需求和长续航版本继续验证，但仍需竞品电池、续航、价格和重量证据。"
    return ""


def _configuration_market_context_priority(row: dict[str, Any]) -> int:
    text = str(row.get("feature") or "")
    if "4WD" in text:
        return 0
    if "SUV" in text and "BEV" in text:
        return 1
    if "SUV" in text and "PHEV" in text:
        return 2
    if "BEV" in text:
        return 3
    if "PHEV" in text:
        return 4
    return 9


def _prioritize_configuration_decision_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=_configuration_decision_row_priority)


def _configuration_decision_row_priority(row: dict[str, Any]) -> tuple[int, int, str]:
    feature = str(row.get("feature") or "")
    priority = str(row.get("priority") or "").upper()
    is_market_context = feature.startswith("市场场景证据")
    if priority == "P0" and not is_market_context:
        group = 0
    elif is_market_context:
        group = 1
    elif priority == "P1":
        group = 2
    else:
        group = 3
    return (group, _configuration_market_context_priority(row) if is_market_context else 0, feature)


def _answer_business_text(answer: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("title", "direct", "summary", "pmInsight"):
        value = str(answer.get(key) or "").strip()
        if value:
            parts.append(value)
    for key in ("bullets", "keyTakeaways", "reportReadyBullets", "businessImplications"):
        parts.extend(_string_list(answer.get(key)))
    synthesis = answer.get("businessSynthesisPlan") if isinstance(answer.get("businessSynthesisPlan"), dict) else {}
    for key in ("executiveConclusion", "summary"):
        value = str(synthesis.get(key) or "").strip()
        if value:
            parts.append(value)
    parts.extend(_string_list(synthesis.get("reportReadyBullets")))
    parts.extend(_string_list(synthesis.get("businessImplications")))
    return " ".join(parts)


def _first_configuration_ref(refs: list[dict[str, Any]]) -> dict[str, Any] | None:
    tokens = (
        "config",
        "feature",
        "equipment",
        "variant",
        "trim",
        "battery",
        "range",
        "winter",
        "heat",
        "tire",
        "tow",
        "roof",
        "charging",
        "kwh",
        "配置",
        "电池",
        "冬季",
    )
    for ref in refs:
        label = str(ref.get("label") or "").lower()
        source = str(ref.get("source") or ref.get("table") or "").lower()
        if any(token in label or token in source for token in tokens):
            return ref
    return None


def _dedupe_configuration_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        feature = str(row.get("feature") or "").strip().lower()
        if not feature or feature in seen:
            continue
        seen.add(feature)
        result.append(row)
    return result


def _pricing_decision_rows(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    answer: dict[str, Any],
    *,
    question: str = "",
    filter_requested_models: bool = True,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    model = _entity_label(evidence_package, fallback="目标车型")
    requested_models = (
        _pricing_relevant_model_labels(evidence_package, answer, question=question)
        if filter_requested_models
        else []
    )
    target_min = _ref_by_label_tokens(refs, ("user supplied own-model target price min",))
    target_max = _ref_by_label_tokens(refs, ("user supplied own-model target price max",))
    target_mid = _ref_by_label_tokens(refs, ("user supplied own-model target price midpoint",))
    price_min = _ref_by_label_tokens(refs, ("pricestats.min",))
    price_max = _ref_by_label_tokens(refs, ("pricestats.max",))
    price_avg = _ref_by_label_tokens(refs, ("pricestats.avg",))
    price_median = _ref_by_label_tokens(refs, ("pricestats.median",))
    own_msrp = _ref_by_label_tokens(refs, ("main trim msrp", "own-model msrp", "current msrp", "premium msrp", "target price midpoint"))
    corridor = _ref_by_label_tokens(refs, ("competitor corridor", "price corridor"))
    relative_delta = _ref_by_label_tokens(refs, ("user supplied relative price delta", "relative price delta"))
    price_gap = _ref_by_label_tokens(refs, ("price gap", "trim price gap", "high-low price gap"))
    pva_coverage = _ref_by_label_tokens(refs, ("pva coverage", "pva"))
    monthly_ref = _ref_by_label_tokens(refs, ("monthlypayment", "monthly payment", "monthly", "leasing", "lease payment"))
    rv_ref = _ref_by_label_tokens(refs, ("residualvalue", "residual value", "residual", "rv"))
    monthly_value = _pricing_optional_value(monthly_ref, "待补月供/租赁方案")
    rv_value = _pricing_optional_value(rv_ref, "待补残值/RV")
    price_stats_are_reference_sample = bool(price_min or price_max or price_avg or price_median)
    if target_min and target_max:
        rows.append({
            "model": model,
            "evidenceStatus": _pricing_ref_evidence_status(target_mid or target_min, fallback="用户输入价格假设；待官方 MSRP 验证"),
            "powertrain": _infer_powertrain(model),
            "msrp": f"{format_artifact_value(target_min.get('value'), str(target_min.get('unit') or ''))}-{format_artifact_value(target_max.get('value'), str(target_max.get('unit') or ''))} user target",
            "monthlyPayment": monthly_value,
            "rv": rv_value,
            "pricePosition": _pricing_position_label(target_mid, price_median, price_avg, price_min, price_max, target_min, target_max),
            "action": "把目标价作为场景价，补官方 MSRP 后定案",
            "evidenceRef": str((target_mid or target_min).get("refId") or ""),
            "source": str((target_mid or target_min).get("source") or "user_question"),
        })
    elif own_msrp:
        rows.append({
            "model": model,
            "evidenceStatus": _pricing_ref_evidence_status(own_msrp),
            "powertrain": _infer_powertrain(model),
            "msrp": format_artifact_value(own_msrp.get("value"), str(own_msrp.get("unit") or "")),
            "monthlyPayment": monthly_value,
            "rv": rv_value,
            "pricePosition": _own_model_price_position(own_msrp, price_gap, pva_coverage),
            "action": _own_model_price_action(own_msrp),
            "evidenceRef": str(own_msrp.get("refId") or ""),
            "source": str(own_msrp.get("source") or ""),
        })
    rows.extend(_pricing_record_rows(
        refs,
        target_model=model,
        answer=answer,
        requested_models=requested_models,
    ))
    if _pricing_has_decision_context_for_missing_model_rows(
        rows=rows,
        target_min=target_min,
        target_max=target_max,
        own_msrp=own_msrp,
        corridor=corridor,
        relative_delta=relative_delta,
        price_min=price_min,
        price_max=price_max,
    ) or _pricing_should_add_source_repair_missing_rows(
        evidence_package,
        answer,
        requested_models=requested_models,
    ):
        rows.extend(_pricing_missing_requested_model_rows(
            evidence_package,
            answer=answer,
            requested_models=requested_models,
            existing_rows=rows,
            monthly_value=monthly_value,
            rv_value=rv_value,
        ))
    if relative_delta:
        rows.append({
            "model": "Relative price delta",
            "evidenceStatus": _pricing_ref_evidence_status(relative_delta, fallback="用户输入价差假设；非官方 MSRP"),
            "powertrain": _infer_powertrain(model),
            "msrp": format_artifact_value(relative_delta.get("value"), str(relative_delta.get("unit") or "")),
            "monthlyPayment": monthly_value,
            "rv": rv_value,
            "pricePosition": "用户给定价差假设；不是官方 MSRP",
            "action": "验证价差是否覆盖电池/续航、配置价值、月供/RV、company car 和品牌风险。",
            "evidenceRef": str(relative_delta.get("refId") or ""),
            "source": str(relative_delta.get("source") or "user_question"),
        })
    if price_min and price_max:
        rows.append({
            "model": "Reference sample range" if price_stats_are_reference_sample else "竞品价格走廊",
            "evidenceStatus": _pricing_ref_evidence_status(price_min, fallback="背景价格样本；需确认是否为核心竞品"),
            "powertrain": _infer_powertrain(model),
            "msrp": f"{format_artifact_value(price_min.get('value'), str(price_min.get('unit') or ''))}-{format_artifact_value(price_max.get('value'), str(price_max.get('unit') or ''))}",
            "monthlyPayment": monthly_value,
            "rv": rv_value,
            "pricePosition": (
                "非本题核心竞品走廊；仅作已物化 MSRP 背景样本"
                if price_stats_are_reference_sample
                else "样本最低-最高价格走廊"
            ),
            "action": (
                "先补请求车型和核心竞品官方 MSRP，再判断价格走廊是否成立。"
                if price_stats_are_reference_sample
                else "检查样本车型池是否就是核心竞品"
            ),
            "evidenceRef": str(price_min.get("refId") or price_max.get("refId") or ""),
            "source": str(price_min.get("source") or price_max.get("source") or ""),
        })
    if corridor and (not price_min or not price_max or price_stats_are_reference_sample):
        rows.append({
            "model": "竞品价格走廊",
            "evidenceStatus": _pricing_ref_evidence_status(corridor),
            "powertrain": _infer_powertrain(model),
            "msrp": format_artifact_value(corridor.get("value"), str(corridor.get("unit") or "")),
            "monthlyPayment": monthly_value,
            "rv": rv_value,
            "pricePosition": "核心价格走廊",
            "action": "用竞品走廊校准低配锚点和高配主推",
            "evidenceRef": str(corridor.get("refId") or ""),
            "source": str(corridor.get("source") or ""),
        })
    if price_median or price_avg:
        rows.append({
            "model": "Reference sample center" if price_stats_are_reference_sample else "价格走廊中心",
            "evidenceStatus": _pricing_ref_evidence_status(price_median or price_avg, fallback="背景价格样本；需确认是否为核心竞品"),
            "powertrain": _infer_powertrain(model),
            "msrp": " / ".join(
                item
                for item in [
                    f"Median {format_artifact_value(price_median.get('value'), str(price_median.get('unit') or ''))}" if price_median else "",
                    f"Avg {format_artifact_value(price_avg.get('value'), str(price_avg.get('unit') or ''))}" if price_avg else "",
                ]
                if item
            ),
            "monthlyPayment": monthly_value,
            "rv": rv_value,
            "pricePosition": (
                "样本中位/均值，仅作价格背景"
                if price_stats_are_reference_sample
                else "判断目标价位于低位/中段/高位"
            ),
            "action": (
                "补请求车型和核心竞品官方 MSRP、版本、月供/RV 后再判断真实价格位置。"
                if price_stats_are_reference_sample
                else "补 leasing/RV 后判断真实支付压力"
            ),
            "evidenceRef": str((price_median or price_avg or {}).get("refId") or ""),
            "source": str((price_median or price_avg or {}).get("source") or ""),
        })
    value_ref = pva_coverage or price_gap
    if value_ref:
        rows.append({
            "model": "高配价值证明",
            "evidenceStatus": _pricing_ref_evidence_status(value_ref, fallback="价值证明材料；非 MSRP"),
            "powertrain": _infer_powertrain(model),
            "msrp": _high_trim_value_amount(price_gap, pva_coverage),
            "monthlyPayment": monthly_value,
            "rv": rv_value,
            "pricePosition": _high_trim_value_position(price_gap, pva_coverage),
            "action": _high_trim_value_action(price_gap, pva_coverage),
            "evidenceRef": str(value_ref.get("refId") or ""),
            "source": str(value_ref.get("source") or ""),
        })
    return _dedupe_table_rows(rows)


def _pricing_has_decision_context_for_missing_model_rows(
    *,
    rows: list[dict[str, Any]],
    target_min: dict[str, Any] | None,
    target_max: dict[str, Any] | None,
    own_msrp: dict[str, Any] | None,
    corridor: dict[str, Any] | None,
    relative_delta: dict[str, Any] | None,
    price_min: dict[str, Any] | None,
    price_max: dict[str, Any] | None,
) -> bool:
    return bool(
        rows
        or (target_min and target_max)
        or own_msrp
        or corridor
        or relative_delta
        or (price_min and price_max)
    )


def _pricing_should_add_source_repair_missing_rows(
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
    *,
    requested_models: list[str],
) -> bool:
    if not requested_models or not _pricing_has_current_price_gap(evidence_package):
        return False
    candidates = _msrp_source_repair_candidates(evidence_package, answer)
    if not candidates or _source_repair_candidate_count(candidates) <= 0:
        return False
    display_candidates = _display_msrp_source_repair_candidates(candidates, requested_models)
    if not display_candidates or _source_repair_candidate_count(display_candidates) <= 0:
        return False
    if _pending_msrp_review_rows(display_candidates):
        return False
    return True


def _pricing_missing_requested_model_rows(
    evidence_package: dict[str, Any],
    *,
    answer: dict[str, Any],
    requested_models: list[str],
    existing_rows: list[dict[str, Any]],
    monthly_value: str,
    rv_value: str,
) -> list[dict[str, Any]]:
    if not requested_models or not _pricing_has_current_price_gap(evidence_package):
        return []
    rows: list[dict[str, Any]] = []
    existing_models = [str(row.get("model") or "") for row in existing_rows]
    target_models = _pricing_requested_target_models(evidence_package)
    competitor_models = _pricing_requested_competitor_models(evidence_package)
    ordered_models = _dedupe_strings([
        *target_models,
        *competitor_models,
        *[
            model
            for model in requested_models
            if not _pricing_model_is_relevant(model, [*target_models, *competitor_models])
        ],
    ])
    for model in ordered_models[:4]:
        if not model or any(_same_business_label(model, existing) for existing in existing_models):
            continue
        is_target = _pricing_model_is_relevant(model, target_models) or (not target_models and not rows)
        rows.append({
            "model": model,
            "evidenceStatus": _pricing_missing_model_evidence_status(evidence_package, answer, model),
            "powertrain": _infer_powertrain(model),
            "msrp": "待补官方 MSRP",
            "monthlyPayment": monthly_value,
            "rv": rv_value,
            "pricePosition": "目标车型价格缺口" if is_target else "竞品价格缺口",
            "action": _pricing_missing_model_action(model, is_target=is_target, competitors=competitor_models),
            "evidenceRef": "",
            "source": "",
        })
        existing_models.append(model)
    return rows


def _pricing_has_current_price_gap(evidence_package: dict[str, Any]) -> bool:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    names = {str(item.get("name") or "") for item in missing if isinstance(item, dict)}
    return bool(
        names
        & {
            "current_msrp",
            "own_model_price",
            "competitor_price_range",
            "competitor_corridor",
            "coverage_diagnostic:no_current_prices_for_requested_models",
        }
    )


def _pricing_requested_target_models(evidence_package: dict[str, Any]) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values = entities.get("models") if isinstance(entities.get("models"), list) else []
    models = _dedupe_strings([str(item or "").strip() for item in values if str(item or "").strip()])
    competitors = _pricing_requested_competitor_models_from_entities(entities)
    if competitors:
        competitor_exact = {item.casefold() for item in competitors}
        target = [model for model in models if model.casefold() not in competitor_exact]
        return target or models[:1]
    if len(models) <= 1:
        return models
    target, _ = _pricing_split_target_and_competitor_models(models)
    return target


def _pricing_requested_competitor_models(evidence_package: dict[str, Any]) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    competitors = _pricing_requested_competitor_models_from_entities(entities)
    if competitors:
        target_models = _pricing_requested_target_models(evidence_package)
        return _dedupe_strings([
            competitor
            for competitor in competitors
            if not _pricing_model_is_relevant(competitor, target_models)
        ])
    values = entities.get("models") if isinstance(entities.get("models"), list) else []
    models = _dedupe_strings([str(item or "").strip() for item in values if str(item or "").strip()])
    if len(models) <= 1:
        return []
    _, split_competitors = _pricing_split_target_and_competitor_models(models)
    return split_competitors


def _pricing_requested_competitor_models_from_entities(entities: dict[str, Any]) -> list[str]:
    values = entities.get("competitors") if isinstance(entities.get("competitors"), list) else []
    return _dedupe_strings([str(item or "").strip() for item in values if str(item or "").strip()])


def _pricing_split_target_and_competitor_models(models: list[str]) -> tuple[list[str], list[str]]:
    if not models:
        return [], []
    target: list[str] = [models[0]]
    competitors: list[str] = []
    target_key = _visual_model_key(models[0])
    for model in models[1:]:
        model_key = _visual_model_key(model)
        if target_key and model_key and (target_key in model_key or model_key in target_key):
            target.append(model)
        else:
            competitors.append(model)
    return _dedupe_strings(target), _dedupe_strings(competitors)


def _pricing_missing_model_action(model: str, *, is_target: bool, competitors: list[str]) -> str:
    if is_target:
        competitor_text = " / ".join(competitors[:2]) if competitors else "核心竞品"
        return f"补 {model} 官方 MSRP、版本/配置和月供/RV，再与 {competitor_text} 校验价格锚点。"
    return f"补 {model} 官方 MSRP、电池/续航/配置、月供/RV 和 company car 口径，再判断价差是否足够。"


def _pricing_optional_value(ref: dict[str, Any] | None, fallback: str) -> str:
    if not ref:
        return fallback
    return format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))


def _own_model_price_position(
    own_msrp: dict[str, Any] | None,
    price_gap: dict[str, Any] | None,
    pva_coverage: dict[str, Any] | None,
) -> str:
    pva_text = _pricing_ref_text(pva_coverage)
    gap_text = _pricing_ref_text(price_gap)
    prefix = "用户材料价格锚点；不是当前官方 MSRP" if _pricing_ref_is_user_material(own_msrp) else "本车型价格锚点"
    if pva_text and gap_text:
        return f"{prefix}；价差 {gap_text}；PVA {pva_text}"
    if pva_text:
        return f"{prefix}；PVA {pva_text}"
    if gap_text:
        return f"{prefix}；价差 {gap_text}"
    return prefix


def _own_model_price_action(own_msrp: dict[str, Any] | None) -> str:
    if _pricing_ref_is_user_material(own_msrp):
        return "先把用户材料价格作为定位假设，补当前官方 MSRP、竞品官方价格、月供/RV 和配置差异后再定案。"
    return "对齐竞品走廊、月供/RV 和可感知配置价值后确认主销版本"


def _pricing_ref_is_user_material(ref: dict[str, Any] | None) -> bool:
    return _is_user_method_material_ref(ref)


def _pricing_ref_evidence_status(ref: dict[str, Any] | None, *, fallback: str = "") -> str:
    if not isinstance(ref, dict):
        return fallback or "证据状态待确认"
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    value = str(ref.get("value") or "").casefold()
    haystack = f"{label} {source} {value}"
    if _pricing_ref_is_user_material(ref):
        return "用户材料假设；非当前官方 MSRP"
    status_label = _pricing_status_value_label(value)
    if status_label:
        return status_label
    if any(token in haystack for token in ("source_draft", "source draft", "review_pending", "待审核")):
        return "待审核价格候选；非当前价格证据"
    if any(token in source for token in ("current_price", "jato_msrp_postgres", "msrp_postgres")):
        return "正式价格记录；可引用"
    if "jato_price_positioning" in source or label.startswith("pricestats."):
        return fallback or "背景价格样本；需确认车型池"
    if any(token in source for token in ("user_question", "user_input")):
        return fallback or "用户输入假设；待官方 MSRP 验证"
    if "pva" in label or "price gap" in label or "trim price gap" in label:
        return fallback or "价值证明材料；非 MSRP"
    return fallback or "价格证据；需确认来源口径"


def _pricing_ref_value_is_status(ref: dict[str, Any]) -> bool:
    value = str(ref.get("value") or "").casefold()
    return bool(_pricing_status_value_label(value))


def _pricing_status_value_label(value: str) -> str:
    normalized = str(value or "").strip().casefold()
    if not normalized:
        return ""
    if normalized in {"source_draft_available", "source draft available"}:
        return "待审核价格候选；非当前价格证据"
    if normalized in {"review_pending_not_current_price", "review pending not current price"}:
        return "待审核价格观察；非当前价格证据"
    if normalized in {"candidate_search_query", "generic_official_price_search", "brand_official_search"}:
        return "官方价格源候选；待检索/确认"
    if normalized in {"no_current_prices_for_requested_models", "current_price_missing"}:
        return "当前官方 MSRP 缺失"
    if normalized in {"current_price_materialized", "current_price_available", "accepted_current_price"}:
        return "正式价格记录；可引用"
    return ""


def _pricing_missing_model_evidence_status(
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
    model: str,
) -> str:
    candidates = _msrp_source_repair_candidates(evidence_package, answer)
    if not candidates:
        return "当前官方 MSRP 缺失"
    for row in _msrp_source_repair_rows(candidates):
        candidate_model = str(row.get("model") or "").strip()
        if not candidate_model or not _pricing_model_is_relevant(candidate_model, [model]):
            continue
        status = str(row.get("sourceStatus") or "").strip()
        source_type = str(row.get("sourceType") or "").strip()
        review_status = str(row.get("reviewStatus") or "").strip()
        if "已有当前价格" in status:
            return "正式价格记录；可引用"
        if "待审核" in status or source_type in {"source_draft", "待审核价格观察"} or review_status:
            return "待审核价格候选；非当前价格证据"
        if "搜索" in status or "search" in source_type:
            return "官方价格源候选；待检索/确认"
    return "当前官方 MSRP 缺失"


def _pricing_record_evidence_status(group: dict[str, Any]) -> str:
    status = _pricing_status_value_label(str(group.get("evidenceStatus") or ""))
    if status:
        return status
    existing_status = str(group.get("evidenceStatus") or "").strip()
    if existing_status and existing_status not in {"价格证据；需确认来源口径", "证据状态待确认"}:
        return existing_status
    source = str(group.get("source") or "").casefold()
    if "source_draft" in source or group.get("sourceDraftPath") or group.get("reviewPendingRows"):
        return "待审核价格候选；非当前价格证据"
    if any(token in source for token in ("current_price", "jato_msrp_postgres", "msrp_postgres")):
        return "正式价格记录；可引用"
    if "jato_price_positioning" in source:
        return "背景价格样本；需确认车型池"
    return existing_status or "价格证据；需确认来源口径"


def _high_trim_value_amount(price_gap: dict[str, Any] | None, pva_coverage: dict[str, Any] | None) -> str:
    gap_text = _pricing_ref_text(price_gap)
    pva_text = _pricing_ref_text(pva_coverage)
    if gap_text:
        return f"非 MSRP：高低配价差 {gap_text}"
    if pva_text:
        return f"非 MSRP：PVA 覆盖 {pva_text}"
    return "待补 value proof"


def _high_trim_value_position(price_gap: dict[str, Any] | None, pva_coverage: dict[str, Any] | None) -> str:
    gap_text = _pricing_ref_text(price_gap)
    pva_text = _pricing_ref_text(pva_coverage)
    if pva_text and gap_text:
        return f"高配价值覆盖：PVA {pva_text} 覆盖价差 {gap_text}"
    if pva_text:
        return f"高配价值覆盖：PVA {pva_text}"
    if gap_text:
        return f"高低配价差 {gap_text}"
    return "版本价值覆盖"


def _high_trim_value_action(price_gap: dict[str, Any] | None, pva_coverage: dict[str, Any] | None) -> str:
    if pva_coverage and price_gap:
        return "用 PVA 覆盖率证明高配价差可被用户感知价值覆盖，支撑高配主推。"
    if pva_coverage:
        return "用 PVA 覆盖率验证高配价值是否足以支撑主销版本。"
    if price_gap:
        return "用可见配置解释高低配价差，避免只靠低价成交。"
    return "用可感知配置价值覆盖高低配价差，支撑高配主推。"


def _pricing_ref_text(ref: dict[str, Any] | None) -> str:
    if not ref:
        return ""
    return format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))


def _pricing_relevant_model_labels(
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
    *,
    question: str,
) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values: list[str] = []
    for key in ("models", "competitors"):
        raw_values = entities.get(key)
        if isinstance(raw_values, list):
            values.extend(str(item or "").strip() for item in raw_values)
    values.extend(_model_mentions_from_question(question))
    for key in ("title", "direct"):
        values.extend(_model_mentions_from_question(str(answer.get(key) or "")))
    return [
        value
        for value in _dedupe_strings(values)
        if len(_visual_model_key(value)) >= 2
    ]


def _pricing_record_rows(
    refs: list[dict[str, Any]],
    *,
    target_model: str,
    answer: dict[str, Any],
    requested_models: list[str],
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for ref in refs:
        model = _pricing_record_model_from_ref(str(ref.get("label") or ""))
        if not model:
            continue
        group = groups.setdefault(model, {
            "model": model,
            "evidenceStatus": "",
            "powertrain": "",
            "msrp": "",
            "avgPrice": "",
            "minPrice": "",
            "maxPrice": "",
            "monthlyPayment": "",
            "rv": "",
            "sourceDraftPath": "",
            "reviewPendingRows": "",
            "reviewStatus": "",
            "evidenceRef": str(ref.get("refId") or ""),
            "source": str(ref.get("source") or ref.get("table") or ""),
            "priceValue": None,
            "minPriceValue": None,
            "maxPriceValue": None,
        })
        if not group.get("evidenceRef"):
            group["evidenceRef"] = str(ref.get("refId") or "")
        if not group.get("source"):
            group["source"] = str(ref.get("source") or ref.get("table") or "")
        _merge_pricing_record_metric(group, ref)

    target_group = next(
        (
            group
            for group in groups.values()
            if _same_business_label(str(group.get("model") or ""), target_model)
        ),
        None,
    )
    competitor_groups = [
        group
        for group in groups.values()
        if not _same_business_label(str(group.get("model") or ""), target_model)
        and (not requested_models or _pricing_record_is_relevant(group, requested_models))
    ]
    rows: list[dict[str, Any]] = []
    for group in groups.values():
        msrp = _pricing_group_msrp(group)
        if not msrp:
            continue
        model = str(group.get("model") or "")
        if requested_models and not _pricing_record_is_relevant(group, requested_models):
            continue
        is_target = _same_business_label(str(group.get("model") or ""), target_model)
        price_position = _pricing_record_position(
            group,
            is_target=is_target,
            target_group=target_group,
            competitor_groups=competitor_groups,
        )
        rows.append({
            "model": group["model"],
            "evidenceStatus": _pricing_record_evidence_status(group),
            "powertrain": group["powertrain"] or _infer_powertrain(f"{group['model']} {target_model}"),
            "msrp": msrp,
            "monthlyPayment": group["monthlyPayment"] or "待补月供/租赁方案",
            "rv": group["rv"] or "待补残值/RV",
            "pricePosition": price_position,
            "action": _pricing_record_action(
                group,
                is_target=is_target,
                price_position=price_position,
                answer=answer,
            ),
            "evidenceRef": group["evidenceRef"],
            "source": group["source"],
        })
    return rows


def _pricing_record_is_relevant(group: dict[str, Any], requested_models: list[str]) -> bool:
    model = str(group.get("model") or "")
    if _pricing_model_is_relevant(model, requested_models):
        return True
    source = str(group.get("source") or "").lower()
    if any(token in source for token in ("jato_msrp_postgres", "jato_price_positioning", "current_price", "source_draft", "msrp_postgres")):
        return False
    return True


def _pricing_model_is_relevant(model: str, requested_models: list[str]) -> bool:
    model_key = _visual_model_key(model)
    if not model_key:
        return False
    for requested in requested_models:
        requested_key = _visual_model_key(requested)
        if not requested_key:
            continue
        if model_key == requested_key:
            return True
        if len(model_key) >= 2 and len(requested_key) >= 2 and (model_key in requested_key or requested_key in model_key):
            return True
    return False


def _pricing_record_model_from_ref(label: str) -> str:
    text = str(label or "").strip()
    lower = text.lower()
    parts = text.split(".")
    if lower.startswith("pricing.records.") and len(parts) >= 4:
        return ".".join(parts[2:-1]).strip()
    metric_keys = {
        "msrp",
        "price",
        "avgprice",
        "medianprice",
        "minprice",
        "maxprice",
        "monthly",
        "monthlypayment",
        "monthlypaymenteur",
        "effectivemonthlyeur",
        "leasing",
        "leasepayment",
        "rv",
        "residual",
        "residualvalue",
        "residualvalueeur",
        "residualvaluepercent",
        "totalcontractcosteur",
        "termmonths",
        "mileageperyear",
        "powertrain",
        "fuel",
        "priceevidencestatus",
        "evidencestatus",
        "sourcedraftpath",
        "reviewpendingrows",
        "reviewstatus",
    }
    if len(parts) >= 2 and parts[-1].lower() in metric_keys and parts[0].lower() != "pricestats":
        return ".".join(parts[:-1]).strip()
    return ""


def _merge_pricing_record_metric(group: dict[str, Any], ref: dict[str, Any]) -> None:
    label = str(ref.get("label") or "")
    metric = label.lower().split(".")[-1]
    formatted = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
    numeric = _number_value(ref.get("value"))
    if not group.get("evidenceStatus"):
        group["evidenceStatus"] = _pricing_ref_evidence_status(ref)
    if metric in {"powertrain", "fuel"}:
        group["powertrain"] = formatted
    elif metric in {"msrp", "price"} and not _pricing_ref_value_is_status(ref):
        group["msrp"] = formatted
        group["priceValue"] = numeric
    elif metric in {"avgprice", "medianprice"} and not _pricing_ref_value_is_status(ref):
        group["avgPrice"] = formatted
        group["priceValue"] = numeric
    elif metric == "minprice" and not _pricing_ref_value_is_status(ref):
        group["minPrice"] = formatted
        group["minPriceValue"] = numeric
    elif metric == "maxprice" and not _pricing_ref_value_is_status(ref):
        group["maxPrice"] = formatted
        group["maxPriceValue"] = numeric
    elif metric in {"monthly", "monthlypayment", "monthlypaymenteur", "effectivemonthlyeur", "leasing", "leasepayment"}:
        group["monthlyPayment"] = formatted
    elif metric in {"rv", "residual", "residualvalue", "residualvalueeur", "residualvaluepercent"}:
        group["rv"] = formatted
    elif metric in {"priceevidencestatus", "evidencestatus"}:
        group["evidenceStatus"] = _pricing_status_value_label(str(ref.get("value") or ""))
    elif metric == "sourcedraftpath":
        group["sourceDraftPath"] = formatted
        group["evidenceStatus"] = group.get("evidenceStatus") or "待审核价格候选；非当前价格证据"
    elif metric == "reviewpendingrows":
        group["reviewPendingRows"] = formatted
        group["evidenceStatus"] = "待审核价格观察；非当前价格证据"
    elif metric == "reviewstatus":
        group["reviewStatus"] = formatted
        group["evidenceStatus"] = _pricing_status_value_label(str(ref.get("value") or ""))


def _pricing_group_msrp(group: dict[str, Any]) -> str:
    min_price = str(group.get("minPrice") or "").strip()
    max_price = str(group.get("maxPrice") or "").strip()
    if min_price and max_price:
        return f"{min_price}-{max_price}"
    return str(group.get("msrp") or group.get("avgPrice") or min_price or max_price or "").strip()


def _pricing_record_position(
    group: dict[str, Any],
    *,
    is_target: bool,
    target_group: dict[str, Any] | None,
    competitor_groups: list[dict[str, Any]],
) -> str:
    if is_target:
        target_value = _pricing_group_center_value(group)
        competitor_low, competitor_high = _pricing_competitor_value_range(competitor_groups)
        if target_value is not None and competitor_low is not None and competitor_high is not None:
            low_text = format_artifact_value(competitor_low, "")
            high_text = format_artifact_value(competitor_high, "")
            if target_value < competitor_low:
                return f"低于已查竞品价格下沿 {low_text}，可做低位切入/价格锚点"
            if target_value > competitor_high:
                return f"高于已查竞品价格上沿 {high_text}，需要溢价证明"
            return f"位于已查竞品价格带 {low_text}-{high_text} 内，可验证主销版本"
        return "本车型价格锚点"

    target_value = _pricing_group_center_value(target_group or {})
    group_value = _pricing_group_center_value(group)
    if target_value is not None and group_value is not None:
        delta = group_value - target_value
        delta_text = format_artifact_value(abs(delta), "")
        if delta > 0:
            return f"高于本车型 {delta_text}，作为竞品价格走廊上方锚点"
        if delta < 0:
            return f"低于本车型 {delta_text}，作为低价竞品风险锚点"
        return "与本车型价格接近，作为正面对标锚点"
    return "竞品价格锚点"


def _pricing_record_action(
    group: dict[str, Any],
    *,
    is_target: bool,
    price_position: str,
    answer: dict[str, Any],
) -> str:
    if is_target:
        if "低于已查竞品价格下沿" in price_position:
            return "用配置差异、月供/RV、质保/售后和高配价值证明低位切入不是单纯低价。"
        if "高于已查竞品价格上沿" in price_position:
            return "补配置、TCO、品牌和渠道价值证据；若证据不足，应下调目标价或拆高低配版本。"
        if "位于已查竞品价格带" in price_position:
            return "继续验证主销版本、配置价值、月供/RV 和销售话术，形成定价决策表。"
        return _first_action(answer) or "校准本车型主销版本、配置价值和成交支持。"
    if "高于本车型" in price_position:
        return "作为竞品价格走廊锚点，补配置差异和月供/RV，判断本车型低价是否有价值感。"
    if "低于本车型" in price_position:
        return "作为低价竞品风险锚点，验证本车型是否需要入门版、促销或配置补偿。"
    return "作为竞品价格走廊锚点，继续补配置、月供/RV 和版本差异。"


def _pricing_competitor_value_range(groups: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    values: list[float] = []
    for group in groups:
        low, high = _pricing_group_value_bounds(group)
        if low is not None:
            values.append(low)
        if high is not None:
            values.append(high)
    if not values:
        return None, None
    return min(values), max(values)


def _pricing_group_center_value(group: dict[str, Any]) -> float | None:
    value = group.get("priceValue")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    low, high = _pricing_group_value_bounds(group)
    if low is not None and high is not None:
        return (low + high) / 2
    return low if low is not None else high


def _pricing_group_value_bounds(group: dict[str, Any]) -> tuple[float | None, float | None]:
    low = group.get("minPriceValue")
    high = group.get("maxPriceValue")
    center = group.get("priceValue")
    low_value = float(low) if isinstance(low, (int, float)) and not isinstance(low, bool) else None
    high_value = float(high) if isinstance(high, (int, float)) and not isinstance(high, bool) else None
    center_value = float(center) if isinstance(center, (int, float)) and not isinstance(center, bool) else None
    if low_value is None and high_value is None and center_value is not None:
        return center_value, center_value
    if low_value is None:
        low_value = high_value
    if high_value is None:
        high_value = low_value
    return low_value, high_value


def _same_business_label(left: str, right: str) -> bool:
    normalize = lambda value: re.sub(r"[^a-z0-9]+", "", str(value or "").lower())
    return bool(normalize(left) and normalize(left) == normalize(right))


def _ref_by_label_tokens(refs: list[dict[str, Any]], tokens: tuple[str, ...]) -> dict[str, Any] | None:
    for ref in refs:
        label = str(ref.get("label") or "").lower()
        if any(token in label for token in tokens):
            return ref
    return None


def _pricing_position_label(
    target_mid: dict[str, Any] | None,
    median_ref: dict[str, Any] | None,
    avg_ref: dict[str, Any] | None,
    min_ref: dict[str, Any] | None,
    max_ref: dict[str, Any] | None,
    target_min_ref: dict[str, Any] | None = None,
    target_max_ref: dict[str, Any] | None = None,
) -> str:
    midpoint = _number_value(target_mid.get("value")) if target_mid else None
    target_min = _number_value(target_min_ref.get("value")) if target_min_ref else None
    target_max = _number_value(target_max_ref.get("value")) if target_max_ref else None
    min_value = _number_value(min_ref.get("value")) if min_ref else None
    max_value = _number_value(max_ref.get("value")) if max_ref else None
    if target_min is not None and target_max is not None and min_value is not None and max_value is not None:
        if target_min >= min_value and target_max <= max_value:
            pass
        elif target_max < min_value:
            return "整体低于样本走廊，低位进攻价"
        elif target_min > max_value:
            return "整体高于样本走廊，需要溢价证明"
        elif target_min < min_value and target_max <= max_value:
            return "低位切入，部分进入样本走廊"
        elif target_min >= min_value and target_max > max_value:
            return "上沿溢价，部分高于样本走廊"
        elif target_min < min_value and target_max > max_value:
            return "覆盖并超出样本走廊，需拆版本场景"
    if midpoint is None:
        return "目标价场景"
    median = _number_value(median_ref.get("value")) if median_ref else None
    avg = _number_value(avg_ref.get("value")) if avg_ref else None
    if min_value is not None and max_value is not None and not (min_value <= midpoint <= max_value):
        return "高于/低于样本走廊，需重新校准"
    if median is not None and midpoint <= median:
        return "位于或低于样本中位数"
    if avg is not None and midpoint <= avg:
        return "走廊中段，低于样本均值"
    if avg is not None:
        return "高于样本均值，需要配置价值证明"
    return "位于样本走廊内"


def _first_action(answer: dict[str, Any]) -> str:
    actions = _action_texts(answer)
    return actions[0] if actions else ""


def _dedupe_table_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row.get("model") or ""), str(row.get("msrp") or ""))
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _source_evidence_refs_from_rows(rows: list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()
    for row in rows[:10]:
        ref = str(row.get("evidenceRef") or "").strip()
        if not ref or ref in seen:
            continue
        seen.add(ref)
        refs.append(ref)
    return refs


def _display_table_rows(
    rows: list[dict[str, Any]],
    columns: list[str],
    *,
    max_columns: int = 8,
) -> list[dict[str, Any]]:
    display_columns = columns[:max(1, max_columns)]
    result: list[dict[str, Any]] = []
    for row in rows[:10]:
        display_row = {
            column: _compact_table_cell(row.get(column, ""))
            for column in display_columns
        }
        if any(str(value or "").strip() for value in display_row.values()):
            result.append(display_row)
    return result


def _compact_table_cell(value: Any) -> Any:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    text = str(value or "").strip()
    if len(text) <= 96:
        return text
    return f"{text[:93]}..."


def _fallback_business_table_artifact(
    intent: str,
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
    raw_columns: list[str],
) -> VisualArtifact | None:
    if not _has_business_structure(answer, evidence_package):
        return None
    rows = _fallback_business_rows(intent, evidence_package, answer)
    if not rows:
        return None
    columns = _display_table_columns(intent, raw_columns)
    source_refs = _source_evidence_refs_from_rows(rows)
    display_rows = _display_table_rows(rows, columns)
    return {
        "id": f"artifact_{intent}_framework_table",
        "type": "table",
        "title": _framework_table_title(intent),
        "subtitle": f"{_table_explanation(intent)} 当前缺少 evidence refs，表格是验证框架，不是已确认事实。",
        "data": {
            "rows": display_rows,
            "intentAnalysis": _fallback_intent_analysis(intent, evidence_package, answer),
        },
        "spec": {
            "columns": columns,
            "rawColumns": raw_columns,
            "maxRows": len(rows),
            "sortBy": _table_sort_by(intent),
            "businessExplanation": _table_explanation(intent),
            "evidenceMode": "missing_refs_framework",
            "columnPolicy": "Main table is capped at seven business columns; raw redundant fields remain in evidencePackage / Analysis Path.",
        },
        "fallbackReason": "evidence_refs_missing",
        "sourceEvidenceRefs": source_refs,
    }


def _framework_table_title(intent: str) -> str:
    if intent == "pricing_analysis":
        return "价格验证框架表"
    if intent == "market_overview":
        return "市场验证框架表"
    if intent == "competitor_compare":
        return "竞品验证框架表"
    if intent == "configuration_analysis":
        return "配置验证框架表"
    if intent == "news_policy_search":
        return "政策验证框架表"
    if intent == "voc_analysis":
        return "VOC 验证框架表"
    if intent == "inventory_analysis":
        return "库存/BOM 验证框架表"
    if intent == "report_generation":
        return "汇报验证框架表"
    return "验证框架表"


def _report_block_artifact(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    ref_ids: list[str],
    *,
    question: str = "",
) -> VisualArtifact | None:
    report_bullets = _string_list(answer.get("reportReadyBullets"))
    actions = _action_texts(answer)
    if not report_bullets and not actions:
        return None
    structured = _structured_report_bullets(report_bullets)
    intent = str(evidence_package.get("intent") or "")
    product_implication = structured.get("productImplication") or _report_product_implication_for_intent(answer, evidence_package, ref_ids)
    lead_judgment = _evidence_backed_lead_judgment(answer)
    if lead_judgment and _should_prefer_evidence_backed_lead_text(product_implication):
        product_implication = lead_judgment
    evidence_backed_implication = _report_generation_evidence_product_implication(answer, evidence_package)
    if evidence_backed_implication and _report_generation_should_prefer_evidence_implication(product_implication):
        product_implication = evidence_backed_implication
    if intent == "inventory_analysis" and _is_generic_inventory_implication(product_implication):
        product_implication = _report_product_implication_for_intent(answer, evidence_package, ref_ids)
    data = {
        "title": structured.get("title") or str(answer.get("title") or "Report block").strip() or "Report block",
        "keyMessage": _report_key_message_for_intent(answer, evidence_package, ref_ids, report_bullets, structured),
        "evidence": _report_evidence_lines_for_intent(
            answer,
            evidence_package,
            report_bullets,
            structured,
            question=question,
        ),
        "productImplication": product_implication,
        "nextAction": _report_block_next_action(answer, evidence_package, report_bullets, structured),
        "intent": intent,
    }
    data = _sanitize_report_block_data(data)
    return {
        "id": "artifact_report_block",
        "type": "report_block",
        "title": _report_block_title(intent),
        "subtitle": _report_block_subtitle(intent),
        "data": data,
        **({"fallbackReason": "evidence_refs_missing"} if not ref_ids else {}),
        "sourceEvidenceRefs": ref_ids[:6],
    }


def _report_block_title(intent: str) -> str:
    if intent == "pricing_analysis":
        return "定价汇报块"
    if intent == "market_overview":
        return "市场机会汇报块"
    if intent == "competitor_compare":
        return "竞品对比汇报块"
    if intent == "configuration_analysis":
        return "配置价值汇报块"
    if intent == "news_policy_search":
        return "政策影响汇报块"
    if intent == "inventory_analysis":
        return "库存/BOM 汇报块"
    if intent == "voc_analysis":
        return "VOC 汇报块"
    return "PPT 汇报块"


def _report_block_subtitle(intent: str) -> str:
    if intent == "pricing_analysis":
        return "可复制到定价页的结论、证据、产品含义和下一步动作。"
    if intent == "market_overview":
        return "可复制到市场机会页的核心判断和证据摘要。"
    return "可复制到产品或市场汇报中的结构化摘要。"


def _structured_report_bullets(report_bullets: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    mapping = {
        "title": "title",
        "key message": "keyMessage",
        "keymessage": "keyMessage",
        "evidence": "evidence",
        "product implication": "productImplication",
        "productimplication": "productImplication",
        "next action": "nextAction",
        "nextaction": "nextAction",
    }
    for bullet in report_bullets:
        text = str(bullet or "").strip()
        if not text:
            continue
        key, value = _split_report_bullet(text)
        normalized = re.sub(r"[^a-z]+", " ", key.lower()).strip()
        compact = normalized.replace(" ", "")
        mapped = mapping.get(normalized) or mapping.get(compact)
        if mapped and value and mapped not in result:
            result[mapped] = value
    return result


def _split_report_bullet(text: str) -> tuple[str, str]:
    for sep in ("：", ":"):
        if sep in text:
            key, value = text.split(sep, 1)
            return key.strip(), value.strip()
    return text.strip(), ""


def _report_key_message(answer: dict[str, Any], report_bullets: list[str], structured: dict[str, str]) -> str:
    key_message = structured.get("keyMessage")
    if key_message:
        return key_message
    for bullet in report_bullets:
        text = str(bullet or "").strip()
        if not text:
            continue
        key, value = _split_report_bullet(text)
        normalized = re.sub(r"[^a-z]+", " ", key.lower()).strip()
        if normalized in {"title", "evidence", "product implication", "next action"}:
            continue
        if text.startswith(("建议动作", "下一步", "Next action", "next action")):
            continue
        return value or text
    return _clean_report_block_message(str(answer.get("summary") or answer.get("direct") or "").strip())


def _report_key_message_for_intent(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    ref_ids: list[str],
    report_bullets: list[str],
    structured: dict[str, str],
) -> str:
    key_message = _report_key_message(answer, report_bullets, structured)
    intent = str(evidence_package.get("intent") or "")
    lead_judgment = _evidence_backed_lead_judgment(answer)
    if lead_judgment and _should_prefer_evidence_backed_lead_text(key_message):
        return lead_judgment
    if intent == "competitor_compare":
        competitor_context_key_message = _competitor_report_context_key_message(evidence_package)
        if competitor_context_key_message:
            return competitor_context_key_message
    if intent == "report_generation":
        coverage_key_message = _report_generation_coverage_key_message(answer, evidence_package, key_message)
        if coverage_key_message:
            return coverage_key_message
    if intent == "inventory_analysis" and _is_generic_inventory_key_message(key_message):
        inventory_key_message = _inventory_report_key_message(evidence_package)
        if inventory_key_message:
            return inventory_key_message
    if not _is_report_method_line(key_message):
        return _sanitize_artifact_display_text(key_message)
    for candidate in (
        _clean_report_block_message(str(answer.get("direct") or answer.get("summary") or answer.get("answerPreview") or "").strip()),
        _first_concrete_product_implication(answer),
        _product_implication(answer),
    ):
        text = _compact_report_block_text(_sanitize_artifact_display_text(candidate))
        if text and not _is_report_method_line(text):
            return text
    if intent == "competitor_compare":
        return "基于当前证据输出竞品池、价格/配置差异和下一步验证动作。"
    if intent == "pricing_analysis":
        return "基于当前证据输出价格走廊、月供/TCO 缺口和下一步验证动作。"
    return _sanitize_artifact_display_text(key_message)


def _competitor_report_context_key_message(evidence_package: dict[str, Any]) -> str:
    rows = _competitor_market_context_framework_rows(evidence_package)
    metrics = [
        str(row.get("keyAdvantage") or "").replace(" = ", " ")
        for row in rows
        if row.get("keyAdvantage")
    ]
    if len(metrics) < 2:
        return ""
    target_model = _entity_label(evidence_package, fallback="目标车型")
    competitor_label = _competitor_label(evidence_package, fallback="核心竞品")
    return _compact_report_block_text(
        f"对标判断：{target_model} vs {competitor_label} 应先写成证据驱动的场景验证："
        f"{'，'.join(metrics[:4])}。"
        "这些证据支持先验证细分、动力/驱动、渠道和用户场景；"
        "但不能替代车型级官方 MSRP、配置和 TCO 交叉验证。"
    )


def _report_generation_coverage_key_message(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    current_key_message: str,
) -> str:
    current_text = _sanitize_artifact_display_text(str(current_key_message or ""))
    if not _report_generation_key_message_is_overconfident(current_text):
        return ""
    context = " ".join(
        str(answer.get(key) or "")
        for key in ("title", "direct", "summary", "answerPreview")
    )
    models = _report_requested_models(evidence_package, context)
    if len(models) < 2:
        return ""
    refs = [
        ref
        for ref in _evidence_refs(evidence_package)
        if not _is_report_generation_noise_ref(ref)
        and not _is_report_generation_generic_market_ref(ref)
    ]
    if not _report_has_multi_model_coverage_gap(evidence_package, models, refs):
        return ""
    rows = _report_model_coverage_rows(models, evidence_package, refs, answer)
    if not rows:
        return ""
    target = next((row for row in rows if str(row.get("role") or "") == "目标车型"), rows[0])
    target_label = str(target.get("model") or models[0])
    competitor_labels = [
        str(row.get("model") or "")
        for row in rows
        if str(row.get("model") or "") and not _same_business_label(str(row.get("model") or ""), target_label)
    ][:3]
    covered = [
        str(row.get("model") or "")
        for row in rows
        if str(row.get("coverageStatus") or "") != "待补"
    ][:3]
    missing = [
        str(row.get("model") or "")
        for row in rows
        if str(row.get("coverageStatus") or "") == "待补"
    ][:3]
    scope = f"{target_label} 对标 {' / '.join(competitor_labels)}" if competitor_labels else target_label
    covered_text = "、".join(item for item in covered if item) or "少量背景证据"
    missing_text = "、".join(item for item in missing if item) or "关键价格/配置/TCO字段"
    return _compact_report_block_text(
        f"{scope} 这页仍是证据覆盖验证页：已覆盖 {covered_text}，待补 {missing_text}；"
        "不能把部分证据写成确定主对标、校验锚点或胜负结论。"
    )


def _report_generation_key_message_is_overconfident(value: str) -> bool:
    text = _sanitize_artifact_display_text(str(value or "")).casefold()
    if not text:
        return False
    return any(
        marker in text
        for marker in (
            "做主对标",
            "主对标",
            "校验锚点",
            "已验证胜出",
            "确定胜出",
            "clearly beats",
            "main benchmark",
            "primary benchmark",
        )
    )


def _report_ref_metric_line(
    refs: list[dict[str, Any]],
    tokens: tuple[str, ...],
    label: str,
) -> str:
    for ref in refs:
        ref_label = str(ref.get("label") or "").casefold()
        if not all(token in ref_label for token in tokens):
            continue
        value = format_artifact_value(ref.get("value"), str(ref.get("unit") or ""))
        if value:
            return f"{label} {value}"
    return ""


def _inventory_report_key_message(evidence_package: dict[str, Any]) -> str:
    fields = _inventory_report_fields(evidence_package)
    material = fields.get("materialCode", "")
    version = fields.get("version", "")
    if not (material and version):
        return ""
    model = fields.get("model") or _entity_label(evidence_package, fallback="目标车型")
    return (
        f"{model} {version} 已命中物料号 {material}；先按颜色/内饰、市场、PI 和生命周期拆分，"
        "不能直接判定为错误或直接合并。"
    )


def _is_generic_inventory_key_message(value: str) -> bool:
    text = _sanitize_artifact_display_text(str(value or "")).casefold()
    if not text:
        return False
    return any(
        token in text
        for token in (
            "车型版本、物料号、市场、颜色",
            "车型版本、物料号",
            "客户可编辑数量",
            "entity relationship",
            "material code",
        )
    )


def _report_evidence_lines(report_bullets: list[str], structured: dict[str, str]) -> list[str]:
    evidence = structured.get("evidence")
    if evidence:
        return [evidence]
    lines: list[str] = []
    for bullet in report_bullets:
        text = str(bullet or "").strip()
        if not text:
            continue
        key, value = _split_report_bullet(text)
        normalized = re.sub(r"[^a-z]+", " ", key.lower()).strip()
        if normalized in {"title", "key message", "product implication", "next action"}:
            continue
        if text.startswith(("建议动作", "下一步", "Next action", "next action")):
            continue
        lines.append(value or text)
    return lines[:3]


def _report_evidence_lines_for_intent(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    report_bullets: list[str],
    structured: dict[str, str],
    *,
    question: str = "",
) -> list[str]:
    intent = str(evidence_package.get("intent") or "")
    raw_lines = _report_evidence_lines(report_bullets, structured)
    lead_lines = _evidence_backed_lead_evidence_lines(answer)
    if lead_lines and _should_prefer_evidence_backed_lead_evidence(raw_lines):
        return _dedupe_strings([*lead_lines, *_evidence_gap_lines(evidence_package)])[:4]
    if intent == "inventory_analysis":
        inventory_lines = _inventory_report_evidence_lines(answer, evidence_package)
        if inventory_lines:
            return inventory_lines[:4]
    if intent == "report_generation":
        evidence_lines = _report_generation_evidence_summary_lines(answer, evidence_package)
        if evidence_lines:
            raw_clean = _clean_report_evidence_candidates(raw_lines)
            lines = _dedupe_strings([
                *evidence_lines,
                *[_sanitize_artifact_display_text(line) for line in raw_clean],
            ])[:4]
            return _filter_report_generation_evidence_lines_for_context(lines, question)
    if intent == "configuration_analysis":
        configuration_lines = _configuration_report_evidence_lines(answer, evidence_package)
        if configuration_lines:
            raw_clean = _clean_report_evidence_candidates(raw_lines)
            return _dedupe_strings([
                *configuration_lines,
                *[_sanitize_artifact_display_text(line) for line in raw_clean],
            ])[:4]
    if intent == "market_overview":
        market_lines = _market_overview_report_evidence_lines(answer, evidence_package)
        if market_lines:
            raw_clean = _clean_report_evidence_candidates(raw_lines)
            return _dedupe_strings([
                *market_lines,
                *[_sanitize_artifact_display_text(line) for line in raw_clean],
            ])[:4]
    if intent not in {"competitor_compare", "pricing_analysis"}:
        clean_lines = _clean_report_evidence_candidates(raw_lines)
        if intent == "report_generation":
            return _filter_report_generation_evidence_lines_for_context(clean_lines, question)
        return clean_lines or [_sanitize_artifact_display_text(line) for line in raw_lines[:3]]
    if intent == "pricing_analysis":
        material_lines = _pricing_report_material_evidence_lines(evidence_package)
        if material_lines:
            return material_lines[:4]
    digest = _clean_report_evidence_candidates(_string_list(answer.get("evidenceDigest")))
    if digest:
        return [_sanitize_artifact_display_text(line) for line in digest[:3]]
    bullet_lines = _clean_report_evidence_candidates(raw_lines)
    if bullet_lines:
        return [_sanitize_artifact_display_text(line) for line in bullet_lines[:3]]
    missing = _missing_names(evidence_package)
    if missing:
        return [f"{name}：待补可引用证据" for name in missing[:3]]
    if intent == "pricing_analysis":
        return ["价格走廊：待补可引用证据", "月供/TCO：待补可引用证据", "竞品配置价值：待补可引用证据"]
    return ["Competitor table：待补可引用证据", "Feature delta：待补可引用证据"]


def _filter_report_generation_evidence_lines_for_context(lines: list[str], question: str) -> list[str]:
    if not _report_question_has_policy_context(question):
        return lines
    result: list[str] = []
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        if not _report_line_looks_external_source(text):
            result.append(text)
            continue
        haystack = text.casefold()
        specific_terms = _report_specific_policy_terms(question)
        if specific_terms and any(term in haystack for term in specific_terms):
            result.append(text)
            continue
        if any(term in haystack for term in _REPORT_POLICY_CONTEXT_TERMS):
            result.append(text)
    return result


def _report_line_looks_external_source(value: str) -> bool:
    text = str(value or "").casefold()
    return any(
        token in text
        for token in (
            "[r",
            "http://",
            "https://",
            ".com",
            ".se",
            ".eu",
            "reuters",
            "source:",
            "来源",
        )
    )


def _market_overview_report_evidence_lines(answer: dict[str, Any], evidence_package: dict[str, Any]) -> list[str]:
    refs = _market_overview_focus_refs("", _evidence_refs(evidence_package))
    rows = _market_overview_decision_rows(evidence_package, refs, answer)
    if not rows:
        return []
    lines: list[str] = []
    for row in sorted(rows, key=_market_report_row_priority):
        signal = str(row.get("signal") or "").strip()
        evidence = str(row.get("evidence") or "").strip()
        if not signal or not evidence:
            continue
        lines.append(f"{signal} = {evidence}")
        if len(_dedupe_strings(lines)) >= 4:
            break
    return _dedupe_strings([_sanitize_artifact_display_text(line) for line in lines if line])[:4]


def _market_report_row_priority(row: dict[str, Any]) -> tuple[int, int, int, str]:
    dimension = str(row.get("dimension") or "").strip().casefold()
    signal = str(row.get("signal") or "").strip()
    evidence = str(row.get("evidence") or "").strip().casefold()
    has_volume = "unit" in evidence or "辆" in evidence
    if dimension == "powertrain mix" and has_volume:
        group = 0
    elif dimension == "segment structure" and has_volume:
        group = 1
    elif dimension == "market size" and has_volume:
        group = 2
    elif dimension == "powertrain mix":
        group = 3
    elif dimension == "segment structure":
        group = 4
    else:
        group = {
            "trend / penetration": 5,
            "top models / competitor pull": 6,
            "channel mix": 7,
            "market evidence": 8,
        }.get(dimension, 9)
    return (group, 0 if has_volume else 1, _market_signal_sort_priority(signal), signal)


def _inventory_report_evidence_lines(answer: dict[str, Any], evidence_package: dict[str, Any]) -> list[str]:
    fields = _inventory_report_fields(evidence_package)
    rows = _inventory_bom_decision_rows(evidence_package, _evidence_refs(evidence_package), answer)
    lines: list[str] = []

    model = fields.get("model") or _entity_label(evidence_package, fallback="目标车型")
    market = fields.get("market") or str(evidence_package.get("country") or "").strip()
    version = fields.get("version", "")
    material = fields.get("materialCode", "")
    if material or version:
        parts = [model]
        if version:
            parts.append(version)
        if material:
            parts.append(material)
        scope = " / ".join(item for item in parts if item)
        suffix = f"（{market}）" if market else ""
        lines.append(f"BOM 映射证据：{scope}{suffix}。")

    available = fields.get("availableUnits", "")
    lifecycle = fields.get("lifecycle", "")
    if available or lifecycle:
        parts = []
        if available:
            parts.append(f"客户可编辑数量/库存 {available}")
        if lifecycle:
            parts.append(f"生命周期 {lifecycle}")
        lines.append(f"库存/生命周期证据：{'；'.join(parts)}。")

    risk = fields.get("risk", "")
    if risk:
        lines.append(f"风险证据：{_inventory_risk_public_text(risk)}")

    if not lines:
        for row in rows:
            material_value = str(row.get("materialCode") or "").strip()
            if not material_value or material_value.startswith("待补"):
                continue
            version_value = str(row.get("version") or "").strip()
            market_value = str(row.get("market") or "").strip()
            row_model = str(row.get("model") or model).strip()
            lines.append(
                "BOM 映射证据："
                + " / ".join(item for item in (row_model, version_value, material_value) if item)
                + (f"（{market_value}）" if market_value else "")
                + "。"
            )
            risk_value = str(row.get("risk") or "").strip()
            if risk_value:
                lines.append(f"风险证据：{risk_value}")
            break

    if (material or any(str(row.get("materialCode") or "").strip() for row in rows)) and not available:
        lines.append("数量动作边界：客户可编辑数量、可下单状态和订单生命周期待补，不能只凭物料号直接生成数量。")
    return _dedupe_strings([_sanitize_artifact_display_text(line) for line in lines if line])


def _inventory_report_fields(evidence_package: dict[str, Any]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for ref in _evidence_refs(evidence_package):
        label = str(ref.get("label") or "").strip()
        value = format_artifact_value(ref.get("value"), str(ref.get("unit") or "")).strip()
        if not value:
            continue
        record_match = re.match(
            r"^(?:inventory|material|bom|stock|order)\.records\.([^.]+)\.([^.]+)$",
            label,
            flags=re.IGNORECASE,
        )
        if record_match:
            fields.setdefault("model", _entity_label(evidence_package, fallback=record_match.group(1).replace("_", " ")))
        metric = label.casefold().split(".")[-1].replace("_", "")
        if metric in {"market", "country"}:
            fields.setdefault("market", value)
        elif metric == "model":
            fields.setdefault("model", value)
        elif metric in {"version", "trim", "variant"}:
            fields.setdefault("version", value)
        elif metric in {"materialcode", "sku", "partnumber"}:
            fields.setdefault("materialCode", value)
        elif metric in {"availableunits", "units", "stock", "quantity", "qty"}:
            fields.setdefault("availableUnits", value)
        elif metric in {"lifecycle", "status"}:
            fields.setdefault("lifecycle", value)
        elif metric == "risk" or "risk" in label.casefold() or "lifecycle risk" in label.casefold():
            fields.setdefault("risk", value)
    return fields


def _pricing_report_material_evidence_lines(evidence_package: dict[str, Any]) -> list[str]:
    refs = [
        ref
        for ref in _evidence_refs(evidence_package)
        if _is_user_method_material_ref(ref)
        and _visual_ref_matches_material_scope(ref, evidence_package)
    ]
    if not refs:
        return []
    main_trim = _ref_by_label_tokens(refs, ("main trim msrp", "main trim price"))
    corridor = _ref_by_label_tokens(refs, ("competitor corridor", "price corridor"))
    price_gap = _ref_by_label_tokens(refs, ("price gap", "trim price gap", "high-low price gap"))
    pva = _ref_by_label_tokens(refs, ("pva coverage", "pva"))
    pool = _ref_by_label_tokens(refs, ("competitor pool",))
    lines: list[str] = []
    market_line = _pricing_report_market_evidence_line(evidence_package)
    if market_line:
        lines.append(market_line)
    if _pricing_has_current_price_gap(evidence_package):
        lines.append("官方 MSRP / 核心竞品当前价格：待补可引用记录，用户材料价格不能写成最终定价。")
    if main_trim or corridor:
        parts: list[str] = []
        if main_trim:
            parts.append(f"主销高配假设 {format_artifact_value(main_trim.get('value'), str(main_trim.get('unit') or ''))}")
        if corridor:
            parts.append(f"用户材料竞品价格带 {format_artifact_value(corridor.get('value'), str(corridor.get('unit') or ''))}")
        lines.append(f"用户材料假设：{'，'.join(parts)}，用于验证价格姿态，不是官方 current MSRP。")
    value_parts: list[str] = []
    if price_gap:
        value_parts.append(f"高低配价差 {format_artifact_value(price_gap.get('value'), str(price_gap.get('unit') or ''))}")
    if pva:
        value_parts.append(f"PVA 覆盖 {format_artifact_value(pva.get('value'), str(pva.get('unit') or ''))}")
    if value_parts:
        lines.append(f"高配价值证明：{'，'.join(value_parts)}，用于验证高配主推是否能被用户感知价值覆盖。")
    if pool and len(lines) < 3:
        lines.append(f"竞品池：{format_artifact_value(pool.get('value'), str(pool.get('unit') or ''))}，需继续补官方价格、月供/RV 和配置差异。")
    return _dedupe_strings([_sanitize_artifact_display_text(line) for line in lines])


def _pricing_report_market_evidence_line(evidence_package: dict[str, Any]) -> str:
    refs = _evidence_refs(evidence_package)
    values: list[str] = []
    for fuel in ("HEV", "BEV", "PHEV"):
        ref = _ref_by_label_tokens(refs, (f"powertrainmix.{fuel.lower()}.sales", f"powertrainMix.{fuel}.sales".lower()))
        if not ref:
            ref = next(
                (
                    item
                    for item in refs
                    if re.search(rf"(?:contextSnapshot|marketSnapshot)\.powertrainMix\.{fuel}\.(?:sales|value)$", str(item.get("label") or ""), flags=re.IGNORECASE)
                ),
                None,
            )
        if ref:
            values.append(f"{fuel} {format_artifact_value(ref.get('value'), str(ref.get('unit') or ''))}")
    if not values:
        return ""
    return f"JATO 图表口径：{'，'.join(values)}；需和用户材料周期口径统一后再写最终定价页。"


def _pricing_has_current_price_gap(evidence_package: dict[str, Any]) -> bool:
    names = {name.casefold() for name in _missing_names(evidence_package)}
    return any(
        "current_msrp" in name
        or "own_model_price" in name
        or "no_current_prices_for_requested_models" in name
        or "current_official_msrp" in name
        for name in names
    )


def _report_generation_evidence_summary_lines(answer: dict[str, Any], evidence_package: dict[str, Any]) -> list[str]:
    rows = _report_generation_competitor_decision_rows(answer, evidence_package)
    if not rows:
        return []
    target_model = _entity_label(evidence_package, fallback="目标车型")
    target_row = _report_generation_target_row(rows, target_model)
    competitor_rows = [
        row
        for row in rows
        if not _same_business_label(str(row.get("model") or ""), target_model)
        and _report_row_has_current_price(row)
    ][:3]
    lines: list[str] = []
    if target_row and _report_row_has_current_price(target_row):
        parts = [
            f"{target_row.get('model')} {target_row.get('priceEvidence')}",
            str(target_row.get("gapVsOj") or ""),
        ]
        lines.append(f"价格证据：{'；'.join(item for item in parts if item)}。")
    if competitor_rows:
        comp_text = "；".join(
            f"{row.get('model')} {row.get('priceEvidence')}"
            for row in competitor_rows
            if row.get("model") and row.get("priceEvidence")
        )
        if comp_text:
            lines.append(f"竞品价格锚点：{comp_text}。")
    boundary = _report_generation_price_boundary_line(evidence_package)
    if boundary:
        lines.append(boundary)
    return _dedupe_strings([_sanitize_artifact_display_text(line) for line in lines if line])


def _report_generation_evidence_product_implication(answer: dict[str, Any], evidence_package: dict[str, Any]) -> str:
    if str(evidence_package.get("intent") or "") != "report_generation":
        return ""
    rows = _report_generation_competitor_decision_rows(answer, evidence_package)
    if not rows:
        return ""
    target_model = _entity_label(evidence_package, fallback="目标车型")
    target_row = _report_generation_target_row(rows, target_model)
    if target_row and _report_row_has_current_price(target_row):
        implication = str(target_row.get("productImplication") or "").strip()
        if implication:
            target_label = str(target_row.get("model") or target_model).strip()
            return _compact_report_block_text(_sanitize_artifact_display_text(
                implication.replace("目标车型", f"{target_label} " if target_label else "目标车型")
            ))
    competitor_rows = [
        row
        for row in rows
        if not _same_business_label(str(row.get("model") or ""), target_model)
        and _report_row_has_current_price(row)
    ][:3]
    if competitor_rows:
        labels = " / ".join(str(row.get("model") or "").strip() for row in competitor_rows if row.get("model"))
        return _compact_report_block_text(
            f"用 {labels} 建立竞品价格锚点，再把 {target_model} 的价格、配置、月供/RV 和销售话术放进同一页验证。"
        )
    return ""


def _report_generation_should_prefer_evidence_implication(value: str) -> bool:
    text = _sanitize_artifact_display_text(str(value or "")).casefold()
    if not text:
        return True
    weak_markers = (
        "先给对标角色",
        "验证路径",
        "不写确定胜负",
        "待补",
        "再生成最终",
        "补齐",
        "验证",
    )
    return _is_report_method_line(text) or _is_generic_product_implication(text) or any(marker in text for marker in weak_markers)


def _evidence_backed_lead_parts(answer: dict[str, Any]) -> tuple[list[str], str]:
    text = _sanitize_artifact_display_text(str(answer.get("evidenceBackedLead") or "").strip())
    if not text:
        return [], ""
    evidence_text = text
    judgment = ""
    if "业务判断" in text:
        evidence_text, judgment = re.split(r"业务判断[:：]", text, maxsplit=1)
    evidence_text = re.sub(r"^已查数据[:：]\s*", "", evidence_text).strip()
    evidence_text = evidence_text.rstrip("。.! ")
    evidence_lines = [
        _compact_report_block_text(_sanitize_artifact_display_text(item.strip().rstrip("。.! ")))
        for item in re.split(r"[；;]\s*", evidence_text)
        if item.strip().rstrip("。.! ")
    ]
    judgment = _compact_report_block_text(_sanitize_artifact_display_text(judgment.strip()))
    return _dedupe_strings(evidence_lines), judgment


def _evidence_backed_lead_evidence_lines(answer: dict[str, Any]) -> list[str]:
    evidence_lines, _ = _evidence_backed_lead_parts(answer)
    return evidence_lines[:4]


def _evidence_backed_lead_judgment(answer: dict[str, Any]) -> str:
    _, judgment = _evidence_backed_lead_parts(answer)
    return judgment


def _should_prefer_evidence_backed_lead_text(value: str) -> bool:
    text = _sanitize_artifact_display_text(str(value or "")).casefold()
    if not text:
        return True
    weak_markers = (
        "待补",
        "补完整",
        "先补",
        "补齐",
        "验证路径",
        "再判断打法",
        "不写确定胜负",
        "先给对标角色",
        "需要先用价格和配置证据确认打法",
    )
    return _is_report_method_line(text) or any(marker in text for marker in weak_markers)


def _should_prefer_evidence_backed_lead_evidence(raw_lines: list[str]) -> bool:
    clean_lines = _clean_report_evidence_candidates(raw_lines)
    if not clean_lines:
        return True
    text = " ".join(str(line or "") for line in raw_lines).casefold()
    weak_markers = (
        "待补",
        "补完整",
        "先补",
        "补齐",
        "验证路径",
        "矩阵",
        "framework",
    )
    return any(marker in text for marker in weak_markers)


def _evidence_gap_lines(evidence_package: dict[str, Any]) -> list[str]:
    missing = _missing_names(evidence_package)
    if not missing:
        return []
    labels = [_report_missing_evidence_label(name) for name in missing[:3]]
    return [f"{'、'.join(label for label in labels if label)}待补，不能只凭已查证据写成最终胜负。"]


def _report_missing_evidence_label(name: str) -> str:
    labels = {
        "configuration_delta": "配置差异",
        "feature_diff": "配置/功能差异",
        "monthly_payment": "月供",
        "leasing_payment": "月供",
        "rv": "RV",
        "residual_value": "RV",
        "current_msrp": "官方 MSRP",
        "own_model_price": "本车型价格",
        "competitor_price_range": "竞品价格走廊",
    }
    key = str(name or "").strip()
    return labels.get(key, key.replace("_", " "))


def _report_generation_competitor_decision_rows(answer: dict[str, Any], evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    refs = [
        ref
        for ref in _evidence_refs(evidence_package)
        if not _is_report_generation_noise_ref(ref)
        and not _is_report_generation_generic_market_ref(ref)
    ]
    if not refs:
        return []
    rows = _competitor_decision_rows(evidence_package, refs, answer)
    return [
        row
        for row in rows
        if isinstance(row, dict) and (row.get("priceEvidence") or row.get("keyAdvantage") or row.get("productImplication"))
    ]


def _report_generation_target_row(rows: list[dict[str, Any]], target_model: str) -> dict[str, Any] | None:
    for row in rows:
        if _same_business_label(str(row.get("model") or ""), target_model):
            return row
    return None


def _report_row_has_current_price(row: dict[str, Any] | None) -> bool:
    if not isinstance(row, dict):
        return False
    text = str(row.get("priceEvidence") or row.get("keyAdvantage") or "").strip()
    return "当前价格" in text or "Price " in text or "价格区间" in text


def _report_generation_price_boundary_line(evidence_package: dict[str, Any]) -> str:
    names = {str(item or "").casefold() for item in _missing_names(evidence_package)}
    parts: list[str] = []
    if any("configuration" in name or "feature" in name or "config" in name for name in names):
        parts.append("配置差异")
    if any("monthly" in name or "leasing" in name or "rv" in name or "residual" in name for name in names):
        parts.append("月供/RV")
    if any("current_msrp" in name or "target_model_price" in name or "own_model_price" in name for name in names):
        parts.append("目标车型官方 MSRP")
    if not parts:
        return ""
    return f"缺口边界：{'、'.join(_dedupe_strings(parts))}待补，不能只凭价格锚点写成最终胜负。"


def _report_product_implication_for_intent(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    ref_ids: list[str],
) -> str:
    implication = _product_implication(answer)
    intent = str(evidence_package.get("intent") or "")
    if intent == "inventory_analysis":
        inventory_implication = _inventory_report_product_implication(evidence_package)
        if inventory_implication and (
            _is_report_method_line(implication)
            or _is_generic_product_implication(implication)
            or _is_generic_inventory_implication(implication)
        ):
            return inventory_implication
    if intent == "configuration_analysis":
        configuration_implication = _configuration_report_product_implication(answer, evidence_package)
        if configuration_implication and (
            not implication
            or _is_report_method_line(implication)
            or _is_generic_product_implication(implication)
            or _looks_like_report_action_only(implication)
        ):
            return configuration_implication
    if _is_playbook_label_implication(implication):
        concrete = _first_concrete_product_implication(answer)
        if concrete:
            return _compact_report_block_text(concrete)
        product_action = _answer_preview_inline_section(answer, "产品动作")
        if product_action:
            return _compact_report_block_text(product_action)
    if _is_report_method_line(implication) or _is_generic_product_implication(implication):
        concrete = _first_concrete_product_implication(answer)
        if concrete:
            return _compact_report_block_text(_sanitize_artifact_display_text(concrete))
        product_action = _concrete_answer_section_for_report(answer)
        if product_action:
            return _compact_report_block_text(_sanitize_artifact_display_text(product_action))
        if intent == "competitor_compare":
            return "把已验证的销量、价格、级别或配置锚点转成定位差异、可赢点、短板和销售话术。"
        if intent == "pricing_analysis":
            return "将价格证据转成价格锚点、主推配置和销售话术。"
    if intent != "competitor_compare" or ref_ids or not _is_report_method_line(implication):
        return _sanitize_artifact_display_text(implication)
    product_action = _answer_preview_inline_section(answer, "产品动作")
    if product_action:
        return _compact_report_block_text(_sanitize_artifact_display_text(product_action))
    return _sanitize_artifact_display_text(implication)


def _configuration_report_evidence_lines(answer: dict[str, Any], evidence_package: dict[str, Any]) -> list[str]:
    rows = _configuration_report_rows(answer, evidence_package)
    lines: list[str] = []
    for row in rows[:3]:
        feature = str(row.get("feature") or "").strip()
        validation = str(row.get("validationData") or "").strip()
        priority = str(row.get("priority") or "").strip()
        if validation == "待补配置差异" and priority:
            validation = f"priority {priority}"
        if not feature or not validation or validation == "待补配置差异":
            continue
        suffix = f"（{priority}）" if priority else ""
        lines.append(f"{feature}{suffix}：{validation}")
    return _dedupe_strings([_sanitize_artifact_display_text(line) for line in lines if line])


def _configuration_report_product_implication(answer: dict[str, Any], evidence_package: dict[str, Any]) -> str:
    rows = _configuration_report_rows(answer, evidence_package)
    if not rows:
        return ""
    priority_rows = [
        row for row in rows
        if str(row.get("priority") or "").strip().upper() in {"P0", "P1"}
    ] or rows
    parts: list[str] = []
    for row in priority_rows[:3]:
        feature = str(row.get("feature") or "").strip()
        validation = str(row.get("validationData") or "").strip()
        priority = str(row.get("priority") or "").strip()
        if validation == "待补配置差异" and priority:
            validation = priority
        if not feature or not validation or validation == "待补配置差异":
            continue
        tag = f"{priority} " if priority else ""
        parts.append(f"{tag}{feature}={validation}")
    if not parts:
        return ""
    target = _entity_label(evidence_package, fallback="目标车型")
    return _compact_report_block_text(
        f"{target} 配置页应把 {'；'.join(parts)} 转成主销版本、价格补偿和销售话术；缺价格/竞品矩阵时不能直接写成配置胜出。"
    )


def _configuration_report_rows(answer: dict[str, Any], evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    refs = [ref for ref in _evidence_refs(evidence_package) if _is_configuration_metric_ref(ref)]
    if refs:
        evidence_rows = _configuration_evidence_rows(evidence_package, refs, answer)
        if evidence_rows:
            return [
                row for row in evidence_rows
                if str(row.get("currentStatus") or "").startswith("已有 evidenceRef")
            ] or evidence_rows
    all_refs = _evidence_refs(evidence_package)
    return _configuration_decision_rows(evidence_package, all_refs, answer)


def _looks_like_report_action_only(value: str) -> bool:
    text = _sanitize_artifact_display_text(str(value or "")).strip()
    return text.startswith(("生成", "补齐", "建立", "输出", "复制", "把已查证据做成"))


def _inventory_report_product_implication(evidence_package: dict[str, Any]) -> str:
    fields = _inventory_report_fields(evidence_package)
    material = fields.get("materialCode", "")
    version = fields.get("version", "")
    risk = fields.get("risk", "")
    if material and version:
        return (
            "同一业务版本存在多个物料号时，应先建立版本-颜色/内饰-市场-物料号-生命周期映射，"
            "再判断是正常拆分、历史物料保留还是数据冲突。"
        )
    if risk:
        return f"当前风险应先按 BOM 实体关系验证：{_inventory_risk_public_text(risk)}"
    return ""


def _is_generic_inventory_implication(value: str) -> bool:
    text = _sanitize_artifact_display_text(str(value or "")).casefold()
    if not text:
        return False
    return any(
        token in text
        for token in (
            "没有底表证据",
            "定义实体关系",
            "异常处理规则",
            "先定义实体",
            "entity relationship",
            "entity mapping",
        )
    )


def _answer_preview_inline_section(answer: dict[str, Any], heading: str) -> str:
    text = str(answer.get("answerPreview") or answer.get("direct") or answer.get("summary") or "").strip()
    if not text:
        return ""
    for marker in (f"{heading}：", f"{heading}:"):
        if marker not in text:
            continue
        section = text.split(marker, 1)[1].strip()
        for stop in ("\n##", "\n###", "\n- P0", "\n- P1", " 展示骨架", " 产品动作", " 缺 ", " 关键证据", " 产品经理判断", " 下一步动作", " ##"):
            if stop in section:
                section = section.split(stop, 1)[0].strip()
        return section
    return ""


def _concrete_answer_section_for_report(answer: dict[str, Any]) -> str:
    for heading in ("产品动作", "产品经理判断"):
        inline = _answer_preview_inline_section(answer, heading)
        if inline:
            return inline
        markdown = _answer_preview_markdown_section(answer, heading)
        if markdown:
            return markdown
    return ""


def _answer_preview_markdown_section(answer: dict[str, Any], heading: str) -> str:
    text = str(answer.get("answerPreview") or answer.get("direct") or answer.get("summary") or "").strip()
    if not text:
        return ""
    section = ""
    for marker in (f"## {heading}", f"### {heading}"):
        if marker in text:
            section = text.split(marker, 1)[1].strip()
            break
    if not section:
        return ""
    for stop in ("\n## ", "\n### "):
        if stop in section:
            section = section.split(stop, 1)[0].strip()
    for line in section.splitlines():
        candidate = line.strip().lstrip("-").strip()
        if candidate:
            return candidate
    return ""


def _first_concrete_product_implication(answer: dict[str, Any]) -> str:
    candidates: list[str] = []
    for values in (answer.get("businessImplications"), _business_synthesis_implications(answer)):
        if not isinstance(values, list):
            continue
        for item in values:
            text = str(item or "").strip()
            if (
                text
                and not _is_playbook_label_implication(text)
                and not _is_generic_product_implication(text)
            ):
                candidates.append(text)
    if not candidates:
        return ""
    return sorted(candidates, key=_product_implication_sort_key)[0]


def _product_implication_sort_key(text: str) -> tuple[int, int]:
    value = str(text or "")
    product_tokens = (
        "高配",
        "低配",
        "主推",
        "价格锚点",
        "配置",
        "价值",
        "打法",
        "销售话术",
        "产品动作",
        "用户场景",
    )
    evidence_tokens = ("竞品池", "价格判断", "核心竞争带", "市场", "证据")
    if any(token in value for token in product_tokens):
        priority = 0
    elif any(token in value for token in evidence_tokens):
        priority = 2
    else:
        priority = 1
    return (priority, len(value))


def _business_synthesis_implications(answer: dict[str, Any]) -> list[Any]:
    synthesis = answer.get("businessSynthesisPlan")
    if not isinstance(synthesis, dict):
        return []
    values = synthesis.get("businessImplications")
    return values if isinstance(values, list) else []


def _clean_report_evidence_candidates(lines: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for line in lines:
        text = str(line or "").strip()
        if not text or _is_report_method_line(text):
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _is_report_method_line(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return True
    method_markers = (
        "定价逻辑应先验证",
        "应先验证目标车型所属价格走廊",
        "验证目标车型的价格走廊",
        "价格走廊、竞品池、配置价值和用户购买场景",
        "市场窗口、竞品走廊和配置差异要一起验证",
        "若 MSRP、竞品价格、leasing/RV 或配置估值缺失",
        "竞品判断应先锁定",
        "竞品对比先定义",
        "正面对抗、错位竞争",
        "不要只列车型名称",
        "而不是只列车型名称",
        "再拆价格",
        "用竞品矩阵",
        "结论要",
        "先输出",
        "需要先",
    )
    return any(marker in value for marker in method_markers)


def _is_playbook_label_implication(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return False
    return value.startswith((
        "定价走廊方法：",
        "市场机会方法：",
        "竞品定位方法：",
        "配置价值方法：",
        "政策影响方法：",
        "库存/BOM 方法：",
        "VOC 证据方法：",
        "汇报生成方法：",
    )) or "方法样例：" in value


def _is_generic_product_implication(text: str) -> bool:
    value = str(text or "").strip()
    return value.startswith((
        "定价判断不能",
        "定价不能只看",
        "市场机会方法",
        "配置价值方法",
        "竞品定位方法",
        "结论要能转成",
        "本轮工具链已经覆盖",
    )) or any(marker in value for marker in (
        "应先验证目标车型所属价格走廊",
        "缺工程配置时先输出",
        "不能直接写成",
        "下一步应补齐缺失证据后再收敛结论",
    ))


def _first_action_like_report_bullet(report_bullets: list[str]) -> str:
    for bullet in report_bullets:
        text = str(bullet or "").strip()
        if text.startswith(("建议动作", "下一步", "Next action", "next action")):
            value = _split_report_bullet(text)[1]
            return value if value else ""
    return ""


def _report_block_next_action(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    report_bullets: list[str],
    structured: dict[str, str],
) -> str:
    candidates = [
        structured.get("nextAction") or "",
        _report_next_action(answer),
        _first_action_like_report_bullet(report_bullets),
        *_action_texts(answer),
    ]
    intent = str(evidence_package.get("intent") or "")
    for candidate in candidates:
        text = _sanitize_artifact_display_text(str(candidate or "").strip())
        if (
            text
            and not _is_source_repair_action_text(text)
            and not _is_generic_report_next_action_text(text, intent)
        ):
            if intent == "inventory_analysis":
                refined_inventory_action = _refined_inventory_next_action(text, evidence_package)
                if refined_inventory_action:
                    return refined_inventory_action
            return text
    return _report_business_next_action(evidence_package)


def _is_generic_report_next_action_text(text: str, intent: str) -> bool:
    value = _sanitize_artifact_display_text(str(text or "").strip()).casefold()
    if not value:
        return False
    if intent == "competitor_compare":
        return (
            "生成竞品矩阵" in value
            and not any(token in value for token in ("j8", "sorento", "7座", "7 座", "4wd", "四驱", "tco"))
        )
    if intent == "inventory_analysis":
        return any(
            token in value
            for token in (
                "画实体关系",
                "补底表字段验证异常规则",
                "定义实体关系",
                "entity relationship",
            )
        )
    return False


def _is_source_repair_action_text(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return False
    markers = (
        "来源修复表",
        "外部来源验证矩阵",
        "官方价格搜索候选",
        "搜索候选",
        "检索线索",
        "补证线索",
        "当前价格记录",
        "当前价格行",
        "来源草稿",
        "source repair",
        "source candidate",
        "current_price",
        "candidate_search_query",
    )
    return any(marker in value for marker in markers)


def _report_business_next_action(evidence_package: dict[str, Any]) -> str:
    intent = str(evidence_package.get("intent") or "")
    missing = set(_missing_names(evidence_package))
    if "leasing_tco_or_company_car_evidence" in missing:
        return "补齐 leasing/TCO/company-car 的月供、残值/RV、税务 benefit 和充电条件后，再判断 PHEV 大客户场景是否成立。"
    if intent == "pricing_analysis":
        return "补齐官方 MSRP、竞品价格走廊、月供/RV 和配置价值后，再确认定价姿态。"
    if intent == "competitor_compare":
        return "补齐本车型/竞品价格、配置、销量和使用场景后，确认正面对抗、错位竞争或价格锚点。"
    if intent == "news_policy_search":
        return "补齐官方来源、发布日期、适用车型/资格后，再写政策影响和产品动作。"
    if intent == "voc_analysis":
        return "补齐可追溯用户原声、媒体测评或论坛来源后，再判断需求频次和卖点优先级。"
    if intent == "configuration_analysis":
        return "补齐配置矩阵、竞品配置、价格/重量/续航和用户场景证据后，再确认配置优先级。"
    if intent == "inventory_analysis":
        inventory_action = _inventory_report_next_action(evidence_package)
        if inventory_action:
            return inventory_action
        return "补齐 PI、车型版本、颜色、物料号和生命周期映射后，再生成可运营的数量调整动作。"
    if intent == "market_overview":
        return "补齐市场规模、细分结构、动力路线和竞品进入顺序后，再形成产品机会判断。"
    return "补齐关键证据后，把结论压成一页可复用的业务建议。"


def _inventory_report_next_action(evidence_package: dict[str, Any]) -> str:
    fields = _inventory_report_fields(evidence_package)
    if fields.get("materialCode") and fields.get("version"):
        return "建立版本-颜色/内饰-市场-物料号-生命周期映射表，并补客户可编辑数量、可下单状态和订单生命周期。"
    if fields.get("risk"):
        return "把 BOM 风险拆成版本、颜色/内饰、市场、PI 和生命周期字段后，再判断是否合并或拆分物料号。"
    return ""


def _refined_inventory_next_action(candidate: str, evidence_package: dict[str, Any]) -> str:
    fields = _inventory_report_fields(evidence_package)
    if not (fields.get("materialCode") and fields.get("version")):
        return ""
    text = _sanitize_artifact_display_text(candidate)
    if "客户可编辑数量" in text and "可下单状态" in text:
        return ""
    return _inventory_report_next_action(evidence_package)


def _clean_report_block_message(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"^直接结论[:：]\s*", "", text)
    for marker in ("展示骨架", "Display", "输出视图", "产品动作", "下一步执行", "证据状态", "分析对象", "##"):
        if marker in text:
            text = text.split(marker, 1)[0].strip()
    return _sanitize_artifact_display_text(text)


def _sanitize_report_block_data(data: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, str):
            result[key] = _sanitize_artifact_display_text(value)
        elif isinstance(value, list):
            result[key] = [
                _sanitize_artifact_display_text(item) if isinstance(item, str) else item
                for item in value
            ]
        else:
            result[key] = value
    result["keyMessage"] = _refresh_stale_report_block_key_message(result)
    evidence = result.get("evidence")
    if isinstance(evidence, list):
        result["evidence"] = _filter_report_block_duplicate_evidence(result, evidence)
    return result


def _refresh_stale_report_block_key_message(data: dict[str, Any]) -> str:
    key_message = str(data.get("keyMessage") or "")
    if "当前价格样本显示" not in key_message:
        return key_message
    if "O5" not in key_message or "EV3" not in key_message:
        return key_message
    if "当前仍缺 O5/EV3 官方 MSRP" not in key_message and "O5/EV3" not in key_message:
        return key_message
    return re.sub(
        r"当前价格样本显示：([^。]+)。",
        r"非本题核心车型的已物化价格背景：\1；只能作为价格环境参考，不能当作 O5/EV3 官方 MSRP 或竞品价格走廊。",
        key_message,
        count=1,
    )


def _filter_report_block_duplicate_evidence(data: dict[str, Any], evidence: list[Any]) -> list[Any]:
    summary_keys = (
        _report_block_text_key(str(data.get("keyMessage") or "")),
        _report_block_text_key(str(data.get("productImplication") or "")),
        _report_block_text_key(str(data.get("nextAction") or "")),
    )
    result: list[Any] = []
    seen: set[str] = set()
    for item in evidence:
        if not isinstance(item, str):
            result.append(item)
            continue
        text = _sanitize_artifact_display_text(item)
        key = _report_block_text_key(text)
        if not key or key in seen:
            continue
        seen.add(key)
        if _report_block_keep_numeric_evidence(text):
            result.append(text)
            continue
        if _report_block_evidence_duplicates_summary(key, summary_keys):
            continue
        result.append(text)
    return result


def _report_block_keep_numeric_evidence(value: str) -> bool:
    text = str(value or "").strip()
    if not re.search(r"\d", text):
        return False
    return any(
        token in text
        for token in (
            "HEV",
            "BEV",
            "PHEV",
            "SUV A0",
            "SUV A",
            "SUV B",
            "累计销量",
            "销量",
            "注册量",
            "占比",
            "渗透率",
            "units",
            "%",
        )
    )


def _report_block_evidence_duplicates_summary(evidence_key: str, summary_keys: tuple[str, ...]) -> bool:
    if len(evidence_key) < 8:
        return False
    for summary_key in summary_keys:
        if len(summary_key) < 8:
            continue
        if evidence_key == summary_key:
            return True
        shorter, longer = (
            (evidence_key, summary_key)
            if len(evidence_key) <= len(summary_key)
            else (summary_key, evidence_key)
        )
        if len(shorter) >= 12 and shorter in longer:
            return True
    return False


def _report_block_text_key(value: str) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", str(value or "").casefold())


def _sanitize_artifact_display_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    replacements = {
        "Use this source to validate price corridor, monthly payment, or competitor positioning before making a firm pricing recommendation": "先验证价格走廊、月供或竞品定位，再给确定定价建议",
        "Use this source to decide what needs official-source confirmation before making policy claims": "先确认官方来源、发布日期和适用车型/资格",
        "Use this source as external context, then anchor the market conclusion to internal JATO sales/share evidence": "外部信息只作背景，市场结论仍要回到 JATO 销量/份额证据",
        "Use this source as a citation candidate and cross-check with internal evidence": "作为候选来源，并与内部证据交叉验证",
        "External source repair table": "外部来源验证矩阵",
        "external source repair table": "外部来源验证矩阵",
        "External source validation matrix": "外部来源验证矩阵",
        "MSRP source validation table": "MSRP 来源验证表",
        "MSRP source repair table": "MSRP 来源验证表",
        "MSRP 来源 repair table": "MSRP 来源验证表",
        "source repair candidates": "来源修复候选",
        "source repair candidate": "来源修复候选",
        "current_price rows": "当前价格记录",
        "current_price row": "当前价格记录",
        "current_price 行": "当前价格记录",
        "current_price": "当前价格记录",
        "source_draft": "来源草稿",
        "source draft": "来源草稿",
        "外部来源修复表": "外部来源验证矩阵",
        "补源线索": "补证线索",
        "补源入口": "补证线索",
        "补证入口": "补证线索",
        "这些搜索候选只是补源入口": "这些候选只是补证线索",
        "这些候选只是补源入口": "这些候选只是补证线索",
        "这些检索线索只是补源入口": "这些检索线索只是补证线索",
        "jato_msrp_postgres": "JATO MSRP 数据",
        "jato_price_positioning": "JATO 价格样本",
        "jato_country_chart_deck": "JATO 图表数据",
        "jato_country_snapshot": "JATO 市场快照",
        "jato_cross_country": "JATO 跨国对比",
        "jato_cross_reference": "JATO 交叉引用",
        "jato_filtered_query": "JATO 筛选查询",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    text = re.sub(
        r"contextSnapshot\.crossTabs\.registrationByFuel\.([^.=（]+)\.Business_pct\s*=",
        lambda match: f"{match.group(1).strip()} 公司车注册占比 =",
        text,
    )
    text = re.sub(
        r"contextSnapshot\.crossTabs\.registrationByFuel\.([^.=（]+)\.sales\s*=",
        lambda match: f"{match.group(1).strip()} 销量 =",
        text,
    )
    text = re.sub(
        r"contextSnapshot\.crossTabs\.driveBySegment\.([^.=（]+)\.4WD_pct\s*=",
        lambda match: f"{match.group(1).strip()} 4WD 占比 =",
        text,
    )
    text = re.sub(
        r"contextSnapshot\.crossTabs\.segmentByFuel\.([^.]+)\.([^.=（]+)_pct\s*=",
        lambda match: f"{match.group(1).strip()} {match.group(2).strip()} 渗透率 =",
        text,
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _compact_report_block_text(value: str, limit: int = 260) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3].rstrip()}..."


def _metric_card_rows(refs: list[dict[str, Any]], *, preserve_order: bool = False) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        range_rows = _metric_card_range_rows(ref, label)
        if range_rows:
            for row in range_rows:
                key = f"{str(row.get('label') or '').casefold()}|{str(row.get('unit') or '').casefold()}|{float(row.get('value') or 0):g}"
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
            continue
        value = _number_value(ref.get("value"))
        if not label or value is None:
            continue
        if not _is_metric_card_ref(label, str(ref.get("unit") or "")):
            continue
        display_label = _metric_card_display_label(label)
        key = f"{display_label.casefold()}|{_artifact_display_unit(str(ref.get('unit') or '')).casefold()}|{value:g}"
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "label": display_label,
            "value": value,
            "unit": _artifact_display_unit(str(ref.get("unit") or "")),
            "source": _sanitize_artifact_display_text(str(ref.get("source") or ref.get("table") or "")),
            "sourceEvidenceRef": str(ref.get("refId") or ""),
        })
    if not preserve_order:
        rows.sort(key=lambda row: float(row.get("value") or 0), reverse=True)
    return rows


def _metric_card_range_rows(ref: dict[str, Any], label: str) -> list[dict[str, Any]]:
    if not _metric_card_range_ref(label):
        return []
    values = _number_values(ref.get("value"))
    if len(values) < 2:
        return []
    low = min(values[:2])
    high = max(values[:2])
    base_label = _metric_card_display_label(label)
    unit = _pricing_range_display_unit(ref, label)
    source = _sanitize_artifact_display_text(str(ref.get("source") or ref.get("table") or ""))
    ref_id = str(ref.get("refId") or "")
    return [
        {
            "label": f"{base_label}下沿",
            "value": low,
            "unit": unit,
            "source": source,
            "sourceEvidenceRef": ref_id,
        },
        {
            "label": f"{base_label}上沿",
            "value": high,
            "unit": unit,
            "source": source,
            "sourceEvidenceRef": ref_id,
        },
    ]


def _metric_card_range_ref(label: str) -> bool:
    lower = str(label or "").casefold()
    return any(token in lower for token in ("competitor corridor", "price corridor", "price range", "target price range"))


def _pricing_range_display_unit(ref: dict[str, Any], label: str) -> str:
    unit = _artifact_display_unit(str(ref.get("unit") or ""))
    if unit:
        return unit
    value_text = str(ref.get("value") or "")
    upper_value = value_text.upper()
    for candidate in ("EUR", "SEK", "GBP", "USD"):
        if candidate in upper_value:
            return candidate
    lower_label = str(label or "").casefold()
    if any(token in lower_label for token in ("competitor corridor", "price corridor", "price range", "target price range", "msrp", "price")):
        return "EUR"
    return ""


def _metric_card_display_label(label: str) -> str:
    text = str(label or "").strip()
    user_material_label = _user_material_metric_card_label(text)
    if user_material_label:
        return user_material_label
    replacements = {
        "priceStats.min": "参考样本下沿",
        "priceStats.max": "参考样本上沿",
        "priceStats.avg": "价格样本均值",
        "priceStats.median": "价格样本中位数",
        "User supplied own-model target price min": "用户目标价下沿",
        "User supplied own-model target price max": "用户目标价上沿",
        "User supplied own-model target price midpoint": "用户目标价中点",
        "User supplied relative price delta": "用户给定价差",
        "cumulativeSales": "累计销量",
        "avgMsrp": "平均 MSRP",
    }
    if text in replacements:
        return replacements[text]
    context_label = _metric_card_context_label(text)
    if context_label:
        return context_label
    return text


def _user_material_metric_card_label(label: str) -> str:
    text = str(label or "").strip()
    match = re.match(r"^(.+?)\s+user material\s+(.+)$", text, flags=re.IGNORECASE)
    if match:
        subject = re.sub(r"\s+", " ", match.group(1).replace("_", " ")).strip()
        field = re.sub(r"\s+", " ", match.group(2).replace("_", " ")).strip().casefold()
        field_label = {
            "market window": "市场窗口",
            "main trim msrp": "主销高配价格",
            "main trim price": "主销高配价格",
            "competitor corridor": "竞品价格带",
            "price corridor": "竞品价格带",
            "price gap": "高低配价差",
            "pva coverage": "高配 PVA 覆盖率",
            "positioning": "定价定位",
            "competitor pool": "竞品池",
        }.get(field)
        if subject and field_label:
            return f"{subject} 用户材料{field_label}"
    direct_match = re.match(
        r"^(.+?)\s+(main trim msrp|main trim price|competitor corridor|price corridor|high-low trim price gap|trim price gap|price gap|pva coverage)$",
        text,
        flags=re.IGNORECASE,
    )
    if direct_match:
        subject = re.sub(r"\s+", " ", direct_match.group(1).replace("_", " ")).strip()
        field = re.sub(r"\s+", " ", direct_match.group(2).replace("_", " ")).strip().casefold()
        field_label = {
            "main trim msrp": "主销高配 MSRP 假设",
            "main trim price": "主销高配价格假设",
            "competitor corridor": "竞品价格带",
            "price corridor": "竞品价格带",
            "high-low trim price gap": "高低配价差",
            "trim price gap": "高低配价差",
            "price gap": "高低配价差",
            "pva coverage": "PVA 覆盖率",
        }.get(field)
        if subject and field_label:
            return f"{subject} {field_label}"
    return ""


def _metric_card_context_label(label: str) -> str:
    match = re.match(r"crossCountry\.([^.]+)\.kpis\.cumulativeSales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 累计销量"
    match = re.match(r"crossCountry\.([^.]+)\.powertrainMix\.([^.]+)\.(?:sales|value)$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2)} 动力销量"
    match = re.match(r"contextSnapshot\.powertrainMix\.([^.]+)\.(?:sales|value)$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 动力销量"
    match = re.match(r"marketSnapshot\.powertrainMix\.([^.]+)\.(?:sales|value)$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 动力销量"
    cross_tabs_prefix = r"(?:contextSnapshot\.)?crossTabs\."
    match = re.match(cross_tabs_prefix + r"driveBySegment\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 细分销量"
    match = re.match(cross_tabs_prefix + r"driveBySegment\.([^.]+)\.([^.]+)_pct$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} 占比"
    match = re.match(cross_tabs_prefix + r"segmentByFuel\.([^.]+)\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} 销量"
    match = re.match(cross_tabs_prefix + r"segmentByFuel\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 细分销量"
    match = re.match(cross_tabs_prefix + r"segmentByFuel\.([^.]+)\.([^.]+)_pct$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} 渗透率"
    match = re.match(cross_tabs_prefix + r"driveByFuel\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 动力销量"
    match = re.match(cross_tabs_prefix + r"driveByFuel\.([^.]+)\.([^.]+)_pct$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} 占比"
    match = re.match(cross_tabs_prefix + r"registrationByFuel\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1).upper()} 注册量"
    match = re.match(cross_tabs_prefix + r"registrationByFuel\.([^.]+)\.([^.]+)_pct$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1).upper()} {_registration_channel_label(match.group(2))}注册占比"
    match = re.match(cross_tabs_prefix + r"registrationBySegment\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 注册量"
    match = re.match(cross_tabs_prefix + r"registrationBySegment\.([^.]+)\.([^.]+)_pct$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {_registration_channel_label(match.group(2))}注册占比"
    match = re.match(r"(?:contextSnapshot\.)?topModels\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1).strip()} 销量"
    match = re.match(r"marketSnapshot\.kpis\.cumulativeSales$", label, flags=re.IGNORECASE)
    if match:
        return "市场累计销量"
    match = re.match(r"results\.kpis\.versionCount$", label, flags=re.IGNORECASE)
    if match:
        return "版本数量"
    match = re.match(r"([A-Za-z0-9][A-Za-z0-9 ._/-]*)\.sales$", label)
    if match:
        return f"{match.group(1).strip()} 销量"
    match = re.match(r"([A-Za-z0-9][A-Za-z0-9 ._/-]*)\.(4wd|awd|business|private)_sales$", label, flags=re.IGNORECASE)
    if match:
        suffix = match.group(2).upper()
        if suffix == "BUSINESS":
            suffix = "公司车"
        elif suffix == "PRIVATE":
            suffix = "私人"
        return f"{match.group(1).strip()} {suffix}"
    match = re.match(r"([A-Za-z0-9][A-Za-z0-9 ._/-]*)\.msrp$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1).strip()} MSRP"
    return ""


def _registration_channel_label(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())
    if normalized in {"business", "company", "fleet", "corporate"}:
        return "公司车"
    if normalized in {"private", "retail"}:
        return "私人"
    if normalized == "other":
        return "其他"
    text = str(value or "").strip()
    return f"{text} " if text else ""


def _is_metric_card_ref(label: str, unit: str) -> bool:
    lower = label.lower()
    if _is_technical_count_label(lower) or any(
        token in lower
        for token in (
            "row_count",
            "result_count",
            "chart_count",
            ".source",
            "source",
            ".date",
            ".claim",
            "rankseed",
            "published",
            "retrieved",
        )
    ):
        return False
    if unit:
        return True
    return any(
        token in lower
        for token in (
            "price",
            "msrp",
            "monthly",
            "leasing",
            "rv",
            "residual",
            "sales",
            "share",
            "volume",
            "battery",
            "range",
            "availableunits",
            "available_units",
        )
    )


def _is_weak_visual_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    source = str(ref.get("source") or ref.get("table") or "").lower()
    value = str(ref.get("value") or "").strip().lower()
    is_zero_count = value in {"0", "0.0"} and any(token in label for token in ("count", "rows", "row_count"))
    is_external_source_rank = (
        ("external_research" in source or "web" in source)
        and (label.endswith(".rank") or label.endswith(".rankseed") or label.endswith("rankseed"))
    )
    return (
        label in {"row_count", "metadata.result_count", "chart_count"}
        or _is_technical_count_label(label)
        or _is_zero_other_pct_ref(ref)
        or is_zero_count
        or is_external_source_rank
        or label.endswith(".source")
        or label.endswith(".sourc")
        or label.endswith(" source")
        or label.endswith(" sourc")
        or label.endswith(".date")
        or label.endswith(".claim")
        or "published" in label
        or "retrieved" in label
        or value.startswith(("http://", "https://"))
    )


def _is_zero_other_pct_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    if not re.search(r"\.other_pct$", label):
        return False
    value = ref.get("value")
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, (int, float)):
        return float(value) == 0.0
    return bool(re.fullmatch(r"\s*0(?:\.0+)?\s*%?\s*", str(value)))


def _is_technical_count_label(label: str) -> bool:
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
        "versioncount",
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


def _evidence_refs(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for tool in _tool_results(evidence_package):
        values = tool.get("evidenceRefs")
        if isinstance(values, list):
            refs.extend(dict(ref) for ref in values if isinstance(ref, dict))
    return refs


def _tool_results(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return []
    return [tool for tool in tool_results if isinstance(tool, dict)]


def _wants_trend_chart(question: str) -> bool:
    question_text = str(question or "").lower()
    return bool(re.search(r"trend|monthly|yearly|month|year|penetration|变化|渗透率|趋势|折线|月度|年度", question_text))


def _has_line_chart(charts: list[dict[str, Any]]) -> bool:
    return any(_normalized_chart_type(str(chart.get("chartType") or "")) == "line" for chart in charts if isinstance(chart, dict))


def _ensure_missing_evidence(evidence_package: dict[str, Any], *, name: str, reason: str, impact: str) -> None:
    missing = evidence_package.setdefault("missingEvidence", [])
    if not isinstance(missing, list):
        evidence_package["missingEvidence"] = []
        missing = evidence_package["missingEvidence"]
    if any(isinstance(item, dict) and item.get("name") == name for item in missing):
        return
    missing.append({"name": name, "reason": reason, "impact": impact})
    if evidence_package.get("confidence") == "high":
        evidence_package["confidence"] = "medium"


def _chart_rows_from_plotly(plotly_data: list[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trace in plotly_data[:4]:
        if not isinstance(trace, dict):
            continue
        name = str(trace.get("name") or trace.get("type") or "series")
        x_values = trace.get("x") if isinstance(trace.get("x"), list) else []
        y_values = trace.get("y") if isinstance(trace.get("y"), list) else []
        labels = trace.get("labels") if isinstance(trace.get("labels"), list) else []
        values = trace.get("values") if isinstance(trace.get("values"), list) else []
        if x_values and y_values:
            for x_value, y_value in zip(x_values[:24], y_values[:24], strict=False):
                rows.append({"series": name, "x": x_value, "y": y_value})
        elif labels and values:
            for label, value in zip(labels[:24], values[:24], strict=False):
                rows.append({"series": name, "x": label, "y": value})
    return rows[:80]


def _number_value(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r"-?\d+(?:,\d{3})*(?:\.\d+)?|-?\d+(?:\.\d+)?", value)
        if match:
            try:
                return float(match.group(0).replace(",", ""))
            except ValueError:
                return None
    return None


def _number_values(value: Any) -> list[float]:
    if isinstance(value, bool) or value is None:
        return []
    if isinstance(value, (int, float)):
        return [float(value)]
    if not isinstance(value, str):
        return []
    numbers: list[float] = []
    text = re.sub(r"(?<=\d)\s*[-–—]\s*(?=\d)", " ", value)
    for match in re.finditer(r"-?\d+(?:,\d{3})*(?:\.\d+)?|-?\d+(?:\.\d+)?", text):
        try:
            numbers.append(float(match.group(0).replace(",", "")))
        except ValueError:
            continue
    return numbers


def _normalized_chart_type(value: str) -> ChartType:
    text = value.strip().lower()
    if text in {"line", "scatter_line", "trend"}:
        return "line"
    if text in {"stacked_bar", "stacked"}:
        return "stacked_bar"
    if text in {"scatter", "bubble"}:
        return "scatter"
    return "bar"


def _product_implication(answer: dict[str, Any]) -> str:
    implications = answer.get("businessImplications")
    if isinstance(implications, list):
        for item in implications:
            text = str(item or "").strip()
            if text:
                return text
    synthesis = answer.get("businessSynthesisPlan")
    if isinstance(synthesis, dict):
        values = synthesis.get("businessImplications")
        if isinstance(values, list):
            for item in values:
                text = str(item or "").strip()
                if text:
                    return text
    bullets = _string_list(answer.get("bullets"))
    return bullets[0] if bullets else ""


def _table_title(intent: str) -> str:
    if intent == "market_overview":
        return "市场决策表"
    if intent == "voc_analysis":
        return "VOC 证据表"
    if intent == "pricing_analysis":
        return "价格证据表"
    if intent == "news_policy_search":
        return "政策/新闻证据表"
    if intent == "competitor_compare":
        return "竞品对比表"
    if intent == "configuration_analysis":
        return "配置验证矩阵"
    if intent == "inventory_analysis":
        return "库存/BOM 证据表"
    if intent == "report_generation":
        return "汇报证据附录"
    return "证据表"


def _business_table_columns(intent: str) -> list[str]:
    if intent == "market_overview":
        return ["dimension", "signal", "evidence", "businessImplication", "recommendedAction", "confidence"]
    if intent == "voc_analysis":
        return ["theme", "source", "evidenceSignal", "productImplication", "validationStatus", "recommendedAction", "confidence"]
    if intent == "pricing_analysis":
        return ["model", "powertrain", "evidenceStatus", "msrp", "monthlyPayment", "rv", "pricePosition", "action"]
    if intent == "news_policy_search":
        return ["policyTopic", "sourceDate", "source", "policyEffect", "affectedModels", "businessAction", "risk"]
    if intent == "competitor_compare":
        return ["model", "segment", "powertrain", "keyAdvantage", "gapVsOj", "priceEvidence", "productImplication"]
    if intent == "configuration_analysis":
        return ["feature", "targetModel", "validationData", "sourceOrTool", "acceptanceCriteria", "currentStatus", "priority"]
    if intent == "inventory_analysis":
        return ["market", "model", "version", "exterior", "interior", "materialCode", "availableUnits", "risk"]
    if intent == "report_generation":
        return ["section", "evidence", "source", "businessUse", "nextAction", "confidence"]
    return ["metric", "value", "unit", "source", "action"]


def _display_table_columns(intent: str, raw_columns: list[str]) -> list[str]:
    if intent == "inventory_analysis":
        return ["market", "model", "version", "colorSpec", "materialCode", "availableUnits", "risk"]
    return raw_columns[:8]


def _business_table_row(intent: str, ref: dict[str, Any]) -> dict[str, Any]:
    label = str(ref.get("label") or "evidence")
    value = ref.get("value", "n/a")
    unit = str(ref.get("unit") or "")
    formatted = format_artifact_value(value, unit)
    ref_id = str(ref.get("refId") or "")
    source = str(ref.get("source") or ref.get("table") or "")
    if intent == "pricing_analysis":
        return {
            "model": label,
            "evidenceStatus": _pricing_ref_evidence_status(ref),
            "powertrain": _infer_powertrain(label),
            "msrp": formatted if _looks_like_price_ref(label, unit) else "待补",
            "monthlyPayment": formatted if "monthly" in label.lower() or "leasing" in label.lower() else "待补",
            "rv": formatted if "rv" in label.lower() or "residual" in label.lower() else "待补",
            "pricePosition": "核心价格带验证项",
            "action": "补齐竞品走廊后定价",
            "evidenceRef": ref_id,
            "source": source,
        }
    if intent == "market_overview":
        dimension = _market_dimension_from_ref(label, ref)
        signal = _market_signal_from_ref(label, ref)
        return {
            "dimension": dimension,
            "signal": signal,
            "evidence": formatted,
            "businessImplication": _market_business_implication(dimension, signal),
            "recommendedAction": "拆分机会 segment、动力路线和车型进入顺序",
            "confidence": "medium",
            "evidenceRef": ref_id,
            "source": source,
        }
    if intent == "voc_analysis":
        theme = _voc_theme_from_text(label, formatted, {})
        return {
            "theme": theme,
            "source": source or ("可引用来源" if formatted.startswith(("http://", "https://")) else "待补 VOC 来源"),
            "evidenceSignal": formatted,
            "productImplication": _voc_product_implication(theme, formatted),
            "validationStatus": "候选 VOC 主题，需验证频次和代表性",
            "recommendedAction": "补媒体测评、论坛评论、用户原声并按主题聚类",
            "confidence": "medium",
            "evidenceRef": ref_id,
            "sourceRaw": source,
        }
    if intent == "competitor_compare":
        return {
            "model": label,
            "segment": "待确认",
            "powertrain": _infer_powertrain(label),
            "keyAdvantage": formatted,
            "gapVsOj": "待对比",
            "productImplication": "用于判断正面对抗或错位竞争",
            "evidenceRef": ref_id,
            "source": source,
        }
    if intent == "configuration_analysis":
        return {
            "feature": label,
            "targetModel": "目标车型",
            "validationData": formatted,
            "sourceOrTool": source or "compare_vehicle_variants / engineering config matrix",
            "acceptanceCriteria": _configuration_acceptance_criteria(label),
            "currentStatus": _configuration_current_status(ref_id),
            "priority": "P1",
            "evidenceRef": ref_id,
            "source": source,
        }
    if intent == "inventory_analysis":
        return {
            "market": "当前市场",
            "model": label,
            "version": "待映射",
            "exterior": "待映射",
            "interior": "待映射",
            "colorSpec": "外饰/内饰待映射",
            "materialCode": formatted,
            "availableUnits": formatted if _looks_like_units_ref(label, unit) else "待补",
            "risk": "需确认版本/物料生命周期",
            "evidenceRef": ref_id,
            "source": source,
        }
    if intent == "news_policy_search":
        lower = label.lower()
        return {
            "policyTopic": label,
            "sourceDate": formatted if any(token in lower for token in ("date", "published", "retrieved")) else "待补发布日期",
            "source": formatted if any(token in lower for token in ("source", "url")) else source or "待补来源",
            "policyEffect": formatted if any(token in lower for token in ("claim", "effect", "impact", "policy")) else "待补政策影响",
            "affectedModels": formatted if any(token in lower for token in ("model", "eligibility", "price")) else "待补适用车型",
            "businessAction": "补官方来源、资格门槛和 JATO 交叉验证",
            "risk": "不能把未核验来源写成确定政策事实",
            "evidenceRef": ref_id,
            "sourceRaw": source,
        }
    return {
        "metric": label,
        "value": formatted,
        "unit": unit,
        "source": source,
        "action": "补齐证据后复核",
        "evidenceRef": ref_id,
    }


def _has_business_structure(answer: dict[str, Any], evidence_package: dict[str, Any]) -> bool:
    if answer.get("businessFrame") or answer.get("businessSynthesisPlan"):
        return True
    if _string_list(answer.get("reportReadyBullets")) or _string_list(answer.get("businessImplications")):
        return True
    if isinstance(answer.get("recommendedActions"), list) and answer.get("recommendedActions"):
        return True
    return str(evidence_package.get("intent") or "") in {
        "pricing_analysis",
        "competitor_compare",
        "configuration_analysis",
        "inventory_analysis",
        "news_policy_search",
        "voc_analysis",
    }


def _fallback_business_rows(
    intent: str,
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    actions = _action_texts(answer)
    missing = _missing_names(evidence_package)
    if intent == "news_policy_search" and _is_o5_bev_price_cap_context(evidence_package, answer):
        return _policy_price_cap_scenario_rows(evidence_package, actions, missing)
    if intent == "pricing_analysis":
        return [
            {
                "model": _entity_label(evidence_package, fallback="目标车型"),
                "powertrain": _infer_powertrain(_entity_label(evidence_package, fallback="")),
                "evidenceStatus": "验证框架；无可引用价格证据",
                "msrp": "待补 evidenceRef",
                "monthlyPayment": "待补 evidenceRef",
                "rv": "待补 evidenceRef",
                "pricePosition": "先验证核心竞争带中段",
                "action": actions[0] if actions else "补齐本车型与竞品 MSRP / TP / 月供价格矩阵后再定价",
                "evidenceRef": "",
                "source": "framework",
            },
            {
                "model": "竞品价格走廊",
                "powertrain": "待确认",
                "evidenceStatus": "验证框架；无可引用价格证据",
                "msrp": "待补 competitor corridor",
                "monthlyPayment": "待补 leasing",
                "rv": "待补 residual value",
                "pricePosition": "低配锚点 + 高配主推",
                "action": actions[1] if len(actions) > 1 else "生成价格矩阵",
                "evidenceRef": "",
                "source": "framework",
            },
        ]
    if intent == "market_overview":
        return [
            {
                "dimension": "Market size",
                "signal": str(evidence_package.get("country") or "当前市场"),
                "evidence": "待补 evidenceRef",
                "businessImplication": "先判断市场体量是否足够支撑目标车型进入。",
                "recommendedAction": actions[0] if actions else "调用 query_country_snapshot 并生成市场规模/动力结构图表",
                "confidence": str(evidence_package.get("confidence") or "low"),
                "evidenceRef": "",
                "source": "framework",
            },
            {
                "dimension": "Segment structure",
                "signal": _entity_label(evidence_package, fallback="机会 segment"),
                "evidence": "待补 segment / powertrain refs",
                "businessImplication": "把结论落到 SUV A0/A/B、动力路线和主销价格带。",
                "recommendedAction": actions[1] if len(actions) > 1 else "补 segment-by-fuel、top models 和竞品价格配置矩阵",
                "confidence": str(evidence_package.get("confidence") or "low"),
                "evidenceRef": "",
                "source": "framework",
            },
        ]
    if intent == "voc_analysis":
        return [
            {
                "theme": "VOC source signal",
                "source": "待补 VOC 来源",
                "evidenceSignal": "待补用户原声、媒体测评或论坛评论",
                "productImplication": "先保留为主题假设，不能声称高频需求。",
                "validationStatus": _missing_names(evidence_package)[0] if _missing_names(evidence_package) else "缺少可追溯来源",
                "recommendedAction": actions[0] if actions else "补媒体测评、论坛评论、用户原声并按主题聚类",
                "confidence": str(evidence_package.get("confidence") or "low"),
                "evidenceRef": "",
                "sourceRaw": "framework",
            },
            {
                "theme": "Product action mapping",
                "source": "VOC + JATO 交叉验证",
                "evidenceSignal": "将主题映射到配置、价格、售后和销售话术",
                "productImplication": "输出可验证的配置/话术动作，而不是停留在泛泛用户声音。",
                "validationStatus": "待补频次和代表性",
                "recommendedAction": actions[1] if len(actions) > 1 else "生成 VOC 主题表和产品动作表",
                "confidence": str(evidence_package.get("confidence") or "low"),
                "evidenceRef": "",
                "sourceRaw": "framework",
            },
        ]
    if intent == "competitor_compare":
        market_rows = _competitor_market_context_framework_rows(evidence_package)
        if market_rows:
            target_model = _entity_label(evidence_package, fallback="目标车型")
            competitor_label = _competitor_label(evidence_package, fallback="核心竞品")
            repair_rows = [
                {
                    "model": target_model,
                    "segment": "待补车型级",
                    "powertrain": _infer_powertrain(target_model),
                    "keyAdvantage": "待补官方 MSRP / 月供 / RV",
                    "gapVsOj": "不能写确定价格胜负",
                    "productImplication": "先验证本车型与竞品当前价格，再判断价格锚点或错位竞争。",
                    "evidenceRef": "",
                    "source": "framework",
                },
                {
                    "model": "直接竞品对比表",
                    "segment": f"{target_model} vs {competitor_label}",
                    "powertrain": "待补",
                    "keyAdvantage": "待补座位/驱动/动力/配置/价格逐项差异",
                    "gapVsOj": "不能只凭市场场景判定已胜出",
                    "productImplication": "把市场场景支撑转成配置、价格、TCO 和销售话术验证矩阵。",
                    "evidenceRef": "",
                    "source": "framework",
                },
            ]
            return [*market_rows, *repair_rows]
        return [
            {
                "model": _entity_label(evidence_package, fallback="目标车型"),
                "segment": "待确认",
                "powertrain": _infer_powertrain(_entity_label(evidence_package, fallback="")),
                "keyAdvantage": "待补竞品证据",
                "gapVsOj": "待对比",
                "productImplication": "先确定竞品池，再判断正面对抗/错位竞争/价格锚点",
                "evidenceRef": "",
                "source": "framework",
            },
            {
                "model": "核心竞品池",
                "segment": "待补",
                "powertrain": "待补",
                "keyAdvantage": "价格/配置/空间/动力差异",
                "gapVsOj": "待补 evidenceRef",
                "productImplication": "补齐价格/配置/销量矩阵后确认正面对抗、错位竞争或价格锚点。",
                "evidenceRef": "",
                "source": "framework",
            },
        ]
    if intent == "configuration_analysis":
        return [
            {
                "feature": _configuration_focus(evidence_package, answer),
                "targetModel": _entity_label(evidence_package, fallback="目标车型"),
                "validationData": "目标配置、竞品配置、价格/重量/续航/充电/冬季使用证据",
                "sourceOrTool": "compare_vehicle_variants / engineering config matrix",
                "acceptanceCriteria": "连接用户场景后证明该配置是否主销必备或高配可感知价值",
                "currentStatus": "待补配置矩阵证据",
                "priority": "P0",
                "evidenceRef": "",
                "source": "framework",
            },
            {
                "feature": "北欧关键场景",
                "targetModel": "目标配置包",
                "validationData": "冬季/续航/充电/拖车/ADAS/VOC 需求频次",
                "sourceOrTool": "configuration matrix + Nordic VOC/source check",
                "acceptanceCriteria": actions[0] if actions else "输出主销配置建议并验证 must-have / visible value / optional 分层",
                "currentStatus": "可先输出框架，不能当作已验证配置结论",
                "priority": "P1",
                "evidenceRef": "",
                "source": "framework",
            },
        ]
    if intent == "inventory_analysis":
        return [
            {
                "market": str(evidence_package.get("country") or "当前市场"),
                "model": _entity_label(evidence_package, fallback="目标车型"),
                "version": "待映射",
                "exterior": "待映射",
                "interior": "待映射",
                "colorSpec": "外饰/内饰待映射",
                "materialCode": "待补物料号",
                "availableUnits": "待补库存/订单",
                "risk": missing[0] if missing else "需确认版本/物料生命周期",
                "evidenceRef": "",
                "source": "framework",
            },
            {
                "market": str(evidence_package.get("country") or "当前市场"),
                "model": "实体关系",
                "version": "车型版本",
                "exterior": "外饰",
                "interior": "内饰",
                "colorSpec": "外饰 + 内饰",
                "materialCode": "物料号",
                "availableUnits": "客户可编辑数量",
                "risk": actions[0] if actions else "画实体关系并定义生命周期",
                "evidenceRef": "",
                "source": "framework",
            },
        ]
    if intent == "news_policy_search":
        return [
            {
                "policyTopic": str(evidence_package.get("country") or "当前市场"),
                "sourceDate": "待补发布日期/有效期",
                "source": "待补官方/高质量来源",
                "policyEffect": "待补政策影响结论",
                "affectedModels": "待补适用车型、价格门槛和动力类型",
                "businessAction": actions[0] if actions else "补官方来源、发布日期、资格门槛和 JATO 交叉验证",
                "risk": missing[0] if missing else "不能把未核验来源写成确定政策事实",
                "evidenceRef": "",
                "sourceRaw": "framework",
            },
            {
                "policyTopic": "业务影响路径",
                "sourceDate": "待补",
                "source": "政策/新闻 + JATO 交叉验证",
                "policyEffect": "拆到零售/公司车、价格上限、动力路线和交付时间",
                "affectedModels": _entity_label(evidence_package, fallback="目标车型/竞品池"),
                "businessAction": actions[1] if len(actions) > 1 else "生成政策影响表和产品动作建议",
                "risk": "缺少资格细则时只能写影响路径，不能点名确定受益车型",
                "evidenceRef": "",
                "sourceRaw": "framework",
            },
        ]
    return []


def _competitor_market_context_framework_rows(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    target_model = _entity_label(evidence_package, fallback="目标车型")
    competitor_label = _competitor_label(evidence_package, fallback="核心竞品")
    for ref in _evidence_refs(evidence_package):
        label = str(ref.get("label") or "").strip()
        if not label or not _is_market_context_ref_for_competitor_table(ref):
            continue
        display_label = _metric_card_display_label(label) or label
        numeric_value = _number_value(ref.get("value"))
        value = format_artifact_value(numeric_value if numeric_value is not None else ref.get("value"), str(ref.get("unit") or ""))
        ref_id = str(ref.get("refId") or "").strip()
        key = f"{display_label.casefold()}|{value}"
        if key in seen:
            continue
        row = _competitor_market_context_row(
            display_label,
            value,
            ref_id,
            str(ref.get("source") or ref.get("table") or ""),
            target_model=target_model,
            competitor_label=competitor_label,
        )
        if not row:
            continue
        seen.add(key)
        rows.append(row)
    priority = {
        "SUV A 4WD": 0,
        "SUV A PHEV": 1,
        "PHEV 公司车": 2,
        "SUV A 细分": 3,
        "PHEV 注册": 4,
    }
    rows.sort(key=lambda row: min((rank for token, rank in priority.items() if token in str(row.get("keyAdvantage") or "")), default=9))
    return rows[:5]


def _competitor_market_context_row(
    display_label: str,
    value: str,
    ref_id: str,
    source: str,
    *,
    target_model: str,
    competitor_label: str,
) -> dict[str, Any] | None:
    text = str(display_label or "")
    pair_label = f"{target_model}/{competitor_label}"
    if "SUV A" in text and "4WD" in text:
        return {
            "model": "市场场景证据",
            "segment": "SUV A",
            "powertrain": "4WD",
            "keyAdvantage": f"{text} = {value}",
            "gapVsOj": f"支持 {target_model} 的四驱/冬季/长途场景验证，但不是车型胜负证据",
            "productImplication": "四驱卖点应连到家庭/公司车、冬季路况和长途安全感。",
            "evidenceRef": ref_id,
            "source": source,
        }
    if "SUV A" in text and "PHEV" in text:
        return {
            "model": "市场场景证据",
            "segment": "SUV A",
            "powertrain": "PHEV",
            "keyAdvantage": f"{text} = {value}",
            "gapVsOj": f"支持 PHEV 场景重要，但不替代 {pair_label} 版本配置对比",
            "productImplication": f"把动力路线、驱动形式和用户场景落到 {pair_label} 的配置/价格/TCO 验证矩阵。",
            "evidenceRef": ref_id,
            "source": source,
        }
    if "PHEV 公司车" in text:
        return {
            "model": "渠道场景证据",
            "segment": "Company car",
            "powertrain": "PHEV",
            "keyAdvantage": f"{text} = {value}",
            "gapVsOj": "支持大客户/TCO 验证，但仍缺月供、RV 和税费口径",
            "productImplication": f"销售话术应把 {pair_label} 对比放进 fleet leasing / TCO / 高配价值矩阵。",
            "evidenceRef": ref_id,
            "source": source,
        }
    if "SUV A 细分销量" in text:
        return {
            "model": "市场体量证据",
            "segment": "SUV A",
            "powertrain": "All",
            "keyAdvantage": f"{text} = {value}",
            "gapVsOj": f"证明赛道有体量，不能证明 {target_model} 单车已胜出",
            "productImplication": f"先确认 {target_model} 是否落在该细分和价格带，再做 {competitor_label} 正面对抗判断。",
            "evidenceRef": ref_id,
            "source": source,
        }
    if "PHEV 注册量" in text:
        return {
            "model": "动力路线证据",
            "segment": "PHEV",
            "powertrain": "PHEV",
            "keyAdvantage": f"{text} = {value}",
            "gapVsOj": "支持保留 PHEV 机会池，但仍需车型级价格/配置/TCO",
            "productImplication": f"PHEV 只能作为机会线索，下一步必须验证 {pair_label} 具体版本。",
            "evidenceRef": ref_id,
            "source": source,
        }
    return None


def _is_o5_bev_price_cap_context(evidence_package: dict[str, Any], answer: dict[str, Any]) -> bool:
    text = " ".join([
        _visual_answer_text(answer),
        _entity_label(evidence_package, fallback=""),
    ]).lower()
    has_price_cap = "价格上限" in text or "price cap" in text
    return has_price_cap and "o5" in text and "bev" in text


def _policy_price_cap_scenario_rows(
    evidence_package: dict[str, Any],
    actions: list[str],
    missing: list[str],
) -> list[dict[str, Any]]:
    target_model = _entity_label(evidence_package, fallback="O5 BEV")
    risk = _policy_missing_risk_label(missing[0]) if missing else "未确认官方有效性，不能写成现行政策"
    return [
        {
            "policyTopic": "Scenario A · 价格上限仍有效且 O5 适用",
            "sourceDate": "待补当前有效期",
            "source": "待补官方/高质量来源",
            "policyEffect": "低配/主销版承担补贴资格锚点，价格需留在资格门槛内。",
            "affectedModels": target_model,
            "businessAction": actions[0] if actions else "准备补贴内资格锚点价格页，锁定入门/主销配置边界",
            "risk": risk,
            "evidenceRef": "",
            "sourceRaw": "framework",
        },
        {
            "policyTopic": "Scenario B · 价格上限失效或 O5 不适用",
            "sourceDate": "待补政策状态",
            "source": "待补官方/高质量来源",
            "policyEffect": "补贴门槛只保留为历史价格锚点，定价回到竞品走廊、配置价值和月供/TCO。",
            "affectedModels": target_model,
            "businessAction": actions[1] if len(actions) > 1 else "生成竞品价格走廊和补贴外高配价值页",
            "risk": "不能用旧补贴口径证明当前价格合理性",
            "evidenceRef": "",
            "sourceRaw": "framework",
        },
        {
            "policyTopic": "Scenario C · 新计划或细则未确认",
            "sourceDate": "待补发布日期/细则",
            "source": "待补官方/高质量来源",
            "policyEffect": "先并行保留补贴内入门锚点与补贴外高配价值两套方案，等待官方边界落地。",
            "affectedModels": target_model,
            "businessAction": actions[2] if len(actions) > 2 else "准备双价格页并标记需要补的官方条文、门槛和适用人群",
            "risk": "只能写成情景判断，不能点名确定受益车型",
            "evidenceRef": "",
            "sourceRaw": "framework",
        },
    ]


def _policy_missing_risk_label(name: str) -> str:
    labels = {
        "fresh_external_signal": "缺少最新外部信号，不能确认当前政策状态",
        "official_source": "缺少官方来源，不能写成现行政策",
        "source_date": "缺少来源日期，不能判断政策有效期",
        "published_date": "缺少发布日期，不能判断政策有效期",
        "policy_effect": "缺少政策效果说明，不能判断车型影响",
        "minimum_external_sources": "外部来源数量不足，不能形成稳定结论",
        "external_research_claims_unavailable": "外部来源结论不足，不能确认政策事实",
    }
    value = str(name or "").strip()
    return labels.get(value, value.replace("_", " ") or "未确认官方有效性，不能写成现行政策")


def _fallback_intent_analysis(
    intent: str,
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
) -> dict[str, Any]:
    return {
        "template": intent,
        "evidenceMode": "missing_refs_framework",
        "confidence": str(evidence_package.get("confidence") or "low"),
        "missingEvidence": _missing_names(evidence_package)[:6],
        "productImplication": _sanitize_artifact_display_text(_product_implication(answer)) or "先输出业务框架，待 evidenceRef 回来后再写确定数字。",
        "nextActions": _action_texts(answer)[:4],
    }


def _action_texts(answer: dict[str, Any]) -> list[str]:
    actions = answer.get("recommendedActions")
    result: list[str] = []
    if isinstance(actions, list):
        for item in actions:
            if isinstance(item, dict):
                text = str(item.get("action") or item.get("rationale") or "").strip()
            else:
                text = str(item or "").strip()
            if text:
                result.append(_sanitize_artifact_display_text(text))
    if result:
        return result
    synthesis = answer.get("businessSynthesisPlan") if isinstance(answer.get("businessSynthesisPlan"), dict) else {}
    values = synthesis.get("recommendedActions") if isinstance(synthesis.get("recommendedActions"), list) else []
    for item in values:
        if isinstance(item, dict):
            text = str(item.get("action") or item.get("rationale") or "").strip()
        else:
            text = str(item or "").strip()
        if text:
            result.append(_sanitize_artifact_display_text(text))
    return result


def _missing_names(evidence_package: dict[str, Any]) -> list[str]:
    missing = evidence_package.get("missingEvidence")
    if not isinstance(missing, list):
        return []
    return [
        str(item.get("name") or item.get("reason") or "").strip()
        for item in missing
        if isinstance(item, dict) and str(item.get("name") or item.get("reason") or "").strip()
    ]


def _entity_label(evidence_package: dict[str, Any], *, fallback: str) -> str:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    for key in ("models", "brands", "competitors", "powertrains"):
        values = entities.get(key)
        if isinstance(values, list):
            for item in values:
                text = str(item or "").strip()
                if text:
                    return text
    return fallback


def _competitor_label(evidence_package: dict[str, Any], *, fallback: str) -> str:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values = entities.get("competitors")
    if isinstance(values, list):
        for item in values:
            text = str(item or "").strip()
            if text:
                return text
    return fallback


def _configuration_focus(evidence_package: dict[str, Any], answer: dict[str, Any]) -> str:
    for item in _string_list(answer.get("keyTakeaways")) + _string_list(answer.get("bullets")):
        lower = item.lower()
        if any(token in lower for token in ("battery", "kwh", "winter", "tow", "seat", "hud", "adas", "电池", "冬季", "拖车", "配置")):
            return item[:80]
    label = _entity_label(evidence_package, fallback="")
    return label or "关键配置"


def _table_sort_by(intent: str) -> str:
    if intent == "market_overview":
        return "dimension"
    if intent == "voc_analysis":
        return "theme"
    if intent == "pricing_analysis":
        return "pricePosition"
    if intent == "news_policy_search":
        return "sourceDate"
    if intent == "competitor_compare":
        return "segment"
    if intent == "configuration_analysis":
        return "priority"
    if intent == "inventory_analysis":
        return "risk"
    if intent == "report_generation":
        return "section"
    return "metric"


def _table_explanation(intent: str) -> str:
    if intent == "market_overview":
        return "把市场快照证据转成业务含义和下一步产品动作。"
    if intent == "voc_analysis":
        return "把来源、用户/媒体信号、产品含义和验证状态放在同一张 VOC 表里。"
    if intent == "pricing_analysis":
        return "用固定决策列展示 MSRP、月供、RV、价格位置和缺口。"
    if intent == "news_policy_search":
        return "把来源日期、来源、政策影响、受影响车型、业务动作和风险放在同一张表里。"
    if intent == "competitor_compare":
        return "把车型、级别、动力、优势、差距和产品含义放在同一张竞品表里。"
    if intent == "configuration_analysis":
        return "配置验证矩阵：同时展示配置项、所需数据、来源/工具、验收标准、当前证据状态和优先级。"
    if intent == "inventory_analysis":
        return "把市场、版本、物料号、数量和生命周期风险放在同一张库存/BOM 表里。"
    if intent == "report_generation":
        return "把 evidence refs 转成可放进 PPT 的章节、来源、业务用途和下一步动作。"
    return "由证据支撑的业务表。"


def _infer_powertrain(label: str) -> str:
    lower = label.lower()
    if "phev" in lower:
        return "PHEV"
    if "hev" in lower:
        return "HEV"
    if "bev" in lower or "ev" in lower:
        return "BEV"
    known_bev_models = (
        "ex30",
        "ex40",
        "ex60",
        "ex90",
        "model y",
        "modely",
        "enyaq",
        "id.4",
        "id4",
        "id.7",
        "id7",
    )
    if any(model in lower for model in known_bev_models):
        return "BEV"
    if "ice" in lower:
        return "ICE"
    return "待确认"


def _looks_like_price_ref(label: str, unit: str) -> bool:
    lower = f"{label} {unit}".lower()
    return any(token in lower for token in ("msrp", "price", "pricing", "eur", "sek", "currency", "€"))


def _looks_like_units_ref(label: str, unit: str) -> bool:
    lower = f"{label} {unit}".lower()
    return any(token in lower for token in ("unit", "stock", "inventory", "available", "sales", "volume", "count", "辆", "台"))


def format_artifact_value(value: Any, unit: str = "") -> str:
    if isinstance(value, float):
        base = f"{int(value):,}" if value.is_integer() else f"{value:,.1f}"
    elif isinstance(value, int):
        base = f"{value:,}"
    elif isinstance(value, str):
        text = value.strip()
        normalized = text.replace(",", "")
        if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
            numeric = float(normalized)
            base = f"{int(numeric):,}" if numeric.is_integer() else f"{numeric:,.1f}"
        else:
            base = text
    else:
        base = str(value)
    display_unit = _artifact_display_unit(unit)
    suffix = f" {display_unit}" if display_unit and display_unit not in base else ""
    return f"{base}{suffix}".strip()


def _artifact_display_unit(unit: str) -> str:
    text = str(unit or "").strip()
    if text.lower() == "currency":
        return "EUR"
    return text


def _metric_subtitle(intent: str) -> str:
    if intent == "market_overview":
        return "市场规模、动力结构、主销车型和机会信号，均来自本轮 evidence refs。"
    if intent == "pricing_analysis":
        return "MSRP、参考价格样本、竞品走廊、leasing/RV 和定价建议输入，均来自本轮 evidence refs。"
    if intent == "competitor_compare":
        return "竞品锚点和市场场景信号，均来自本轮 evidence refs。"
    return "当前回答中的可追溯数字证据。"


def _metric_title(intent: str) -> str:
    if intent == "pricing_analysis":
        return "定价关键指标"
    if intent == "market_overview":
        return "市场关键指标"
    if intent == "competitor_compare":
        return "竞品关键指标"
    if intent == "configuration_analysis":
        return "配置关键指标"
    if intent == "inventory_analysis":
        return "库存/BOM 关键指标"
    if intent == "news_policy_search":
        return "政策/新闻关键指标"
    return "关键指标"


def _intent_analysis_block(
    evidence_package: dict[str, Any],
    answer: dict[str, Any],
    refs: list[dict[str, Any]],
) -> dict[str, Any]:
    intent = str(evidence_package.get("intent") or "")
    labels = [str(ref.get("label") or "") for ref in refs]
    implication = _sanitize_artifact_display_text(_product_implication(answer))
    if intent == "market_overview":
        return {
            "template": "market_overview",
            "keyMetrics": _matching_ref_rows(refs, ("sales", "volume", "share", "mix", "bev", "phev", "hev", "ice")),
            "powertrainMix": _matching_ref_rows(refs, ("bev", "phev", "hev", "ice", "powertrain", "fuel")),
            "topModels": _matching_ref_rows(refs, ("model", "top", "ranking", "rank")),
            "productImplication": implication or "Use the market snapshot to choose priority segment, powertrain and competitor entry point.",
        }
    if intent == "pricing_analysis":
        return {
            "template": "pricing_analysis",
            "msrp": _matching_ref_rows(refs, ("msrp", "price", "pricing")),
            "competitorCorridor": _matching_ref_rows(refs, ("corridor", "competitor", "range")),
            "leasingRv": _matching_ref_rows(refs, ("leasing", "monthly", "rv", "residual", "company car")),
            "recommendation": implication or "Validate MSRP, competitor corridor, leasing/RV and configuration value before final price stance.",
        }
    if intent == "competitor_compare":
        competitor_labels = [
            _metric_card_display_label(str(ref.get("label") or "")) or str(ref.get("label") or "")
            for ref in refs
            if not _is_market_context_ref_for_competitor_table(ref)
            and not _is_competitor_zero_sales_ref(ref)
        ]
        market_context = [
            _metric_card_display_label(str(ref.get("label") or "")) or str(ref.get("label") or "")
            for ref in refs
            if _is_market_context_ref_for_competitor_table(ref)
        ]
        return {
            "template": "competitor_compare",
            "competitorTable": [label for label in competitor_labels if label][:8],
            "marketContext": [label for label in market_context if label][:8],
            "featureDelta": _matching_ref_rows(refs, ("feature", "config", "battery", "range", "trim", "equipment")),
            "positioningStatement": implication or "Define whether the model should fight head-on, anchor price, or compete through visible high-value configuration.",
        }
    if intent == "configuration_analysis":
        return {
            "template": "configuration_analysis",
            "trimConfigTable": _matching_ref_rows(refs, ("trim", "config", "feature", "equipment", "version")),
            "mustHaveFeatures": _matching_ref_rows(refs, ("winter", "tow", "heat", "seat", "camera", "hud", "adas", "battery")),
            "gap": _matching_ref_rows(refs, ("gap", "missing", "delta", "shortfall")),
            "recommendation": implication or "Separate must-have Nordic configuration from nice-to-have value features.",
        }
    if intent == "news_policy_search":
        return {
            "template": "news_policy_search",
            "sources": _matching_ref_rows(refs, ("source", "url", "published", "date")),
            "policyClaims": _matching_ref_rows(refs, ("policy", "claim", "effect", "impact", "subsidy", "tax", "benefit")),
            "affectedModels": _matching_ref_rows(refs, ("model", "eligibility", "pricecap", "price_cap", "bev", "phev", "hev")),
            "recommendation": implication or "Validate source date, eligibility, price cap, powertrain impact and JATO cross-check before making a policy claim.",
        }
    if intent == "inventory_analysis":
        return {
            "template": "inventory_analysis",
            "stockMaterialBomLogic": _matching_ref_rows(refs, ("stock", "inventory", "bom", "material", "variant", "version")),
            "risk": _matching_ref_rows(refs, ("risk", "lifecycle", "missing", "duplicate", "conflict")),
            "nextAction": implication or "Normalize version, material number, market, color and order lifecycle before operational use.",
        }
    if intent == "report_generation":
        return {
            "template": "report_generation",
            "reportStructure": ["Title", "Key message", "Evidence", "Product implication", "Next action"],
            "productImplication": implication,
        }
    return {"template": intent or "general_qa", "evidenceLabels": labels[:8], "productImplication": implication}


def _is_competitor_zero_sales_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").strip().casefold()
    if not label.endswith(".sales"):
        return False
    numeric = _number_value(ref.get("value"))
    return numeric is not None and abs(float(numeric)) < 0.000001


def _matching_ref_rows(refs: list[dict[str, Any]], tokens: tuple[str, ...]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ref in refs:
        if _is_weak_visual_ref(ref):
            continue
        label = str(ref.get("label") or "").lower()
        source = str(ref.get("source") or ref.get("table") or "").lower()
        if not any(token in label or token in source for token in tokens):
            continue
        display_label = _evidence_ref_display_label(ref)
        rows.append({
            "label": display_label or str(ref.get("label") or "evidence"),
            "value": ref.get("value", "n/a"),
            "unit": str(ref.get("unit") or ""),
            "evidenceRef": str(ref.get("refId") or ""),
        })
    return rows[:8]


def _evidence_ref_display_label(ref: dict[str, Any]) -> str:
    label = str(ref.get("label") or "").strip()
    if not label:
        return ""
    return _metric_card_display_label(label) or _market_signal_from_ref(label, ref)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]
