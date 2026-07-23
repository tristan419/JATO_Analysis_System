"""Canonical compare facts for AI composition and exports.

The API receives only identity and display-scope parameters. This module turns
the server-built compare response into downstream facts so browser-provided
rows, evidence, labels, and AI conclusions never become trusted input.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any


PRESENT_AVAILABILITY = {"STANDARD", "VALUE"}
AVAILABLE_AVAILABILITY = PRESENT_AVAILABILITY | {"OPTIONAL"}
REVIEW_PATTERN = re.compile(r"需核对|待核对|待确认|缺失|缺少|回看", re.IGNORECASE)
DELTA_FILTERS = {
    "ALL",
    "DIFFERENCE",
    "ADDED",
    "REMOVED",
    "VALUE_CHANGED",
    "OPTIONAL_CHANGED",
    "INFERRED",
    "MISSING_SOURCE",
    "MERGED_SOURCE",
    "UNKNOWN",
    "COMMON",
}


def classify_config_delta(base_value: dict | None, target_value: dict | None) -> str:
    if _cell_signature(base_value) == _cell_signature(target_value):
        return "SAME"
    if _unknown_cell(base_value) or _unknown_cell(target_value):
        return "UNKNOWN"
    base_available = _available_cell(base_value)
    target_available = _available_cell(target_value)
    if not base_available and target_available:
        return "ADDED"
    if base_available and not target_available:
        return "REMOVED"
    if (
        _availability(base_value) == "OPTIONAL"
        or _availability(target_value) == "OPTIONAL"
    ) and _availability(base_value) != _availability(target_value):
        return "OPTIONAL_CHANGED"
    return "VALUE_CHANGED"


def build_business_summary_facts(
    compare_facts: dict[str, Any],
    *,
    base_trim_id: str,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scope = _normalized_filters(filters)
    trims = _dict_list(compare_facts.get("trims"))
    base_index = _trim_index(trims, base_trim_id)
    target_indexes = _target_indexes(trims, base_index, scope.get("targetTrimId"))
    rows = _filtered_rows(compare_facts, base_index, target_indexes, scope)
    base_trim = trims[base_index]
    targets = [
        _business_target_facts(
            rows,
            base_trim=base_trim,
            base_index=base_index,
            target_trim=trims[target_index],
            target_index=target_index,
        )
        for target_index in target_indexes
    ]
    compared_trims = [base_trim, *(trims[index] for index in target_indexes)]
    context: dict[str, Any] = {
        "deltaFilter": scope["deltaFilter"],
        "compareScope": _compare_scope(compared_trims, targets, scope["deltaFilter"]),
        "instruction": (
            "Use only these canonical server compare facts to write concise Chinese business summaries. "
            "Do not invent features or infer a source citation from similar text."
        ),
    }
    if scope.get("forceRefresh"):
        context["cacheControl"] = {"forceRefresh": True}
    return {
        "baseTrim": _trim_fact(base_trim),
        "targets": targets,
        "context": context,
    }


def build_compare_export_facts(
    compare_facts: dict[str, Any],
    *,
    base_trim_id: str,
    filters: dict[str, Any] | None = None,
    business_summary_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scope = _normalized_filters(filters)
    trims = _dict_list(compare_facts.get("trims"))
    base_index = _trim_index(trims, base_trim_id)
    target_indexes = _target_indexes(trims, base_index, scope.get("targetTrimId"))
    visible_indexes = [base_index, *target_indexes]
    rows = _filtered_rows(compare_facts, base_index, target_indexes, scope)
    projected_rows = [
        {
            **row,
            "values": [
                _value_at(row, index)
                for index in visible_indexes
            ],
        }
        for row in rows
    ]
    visible_trims = [trims[index] for index in visible_indexes]
    base_label = _trim_label(trims[base_index])
    target_labels = [_trim_label(trims[index]) for index in target_indexes]
    range_label = _range_label(scope["deltaFilter"])
    export_scope = {
        "title": f"{base_label} vs {' vs '.join(target_labels)}",
        "baseLabel": base_label,
        "targetLabel": " / ".join(target_labels),
        "rangeLabel": range_label,
        "rowCount": len(projected_rows),
        "categoryLabel": scope.get("category") or None,
        "searchLabel": scope.get("search") or None,
        "versionScope": compare_facts.get("versionScope") or "published",
    }
    result: dict[str, Any] = {
        "scope": export_scope,
        "summary": {
            **(_dict(compare_facts.get("summary"))),
            "shownFeatures": len(projected_rows),
        },
        "trims": visible_trims,
        "rows": projected_rows,
        "evidenceSummary": _evidence_summary(projected_rows, len(visible_trims)),
    }
    if business_summary_result:
        result["businessSummary"] = _dict_list(business_summary_result.get("summaries"))
        result["businessSummaryUsage"] = _dict(business_summary_result.get("usage"))
    return result


def _business_target_facts(
    rows: list[dict[str, Any]],
    *,
    base_trim: dict[str, Any],
    base_index: int,
    target_trim: dict[str, Any],
    target_index: int,
) -> dict[str, Any]:
    deltas = [
        _delta_fact(
            row,
            base_trim=base_trim,
            base_index=base_index,
            target_trim=target_trim,
            target_index=target_index,
        )
        for row in rows
    ]
    differences = [delta for delta in deltas if delta["deltaType"] != "SAME"]
    counts = _delta_counts(differences)
    category_deltas: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for delta in differences:
        category_deltas[str(delta["row"].get("category") or "Unknown")].append(delta)
    category_facts = []
    for category, items in sorted(
        category_deltas.items(),
        key=lambda pair: (-len(pair[1]), pair[0].casefold()),
    )[:12]:
        category_counts = _delta_counts(items)
        category_facts.append({
            "category": category,
            "totalDifferenceCount": len(items),
            "addedCount": category_counts["added"],
            "removedCount": category_counts["removed"],
            "optionalChangedCount": category_counts["optionalChanged"],
            "valueChangedCount": category_counts["valueChanged"],
            "inferredCount": category_counts["inferred"],
            "unknownCount": category_counts["unknown"],
            "changeSummary": _change_summary(category_counts),
            "sampleFeatures": " / ".join(_feature_label(item["row"]) for item in items[:5]),
        })
    evidence_facts = [_evidence_fact(delta) for delta in differences[:40]]
    source_names = _unique_text(
        str(fact.get("sourceSheetName") or "") for fact in evidence_facts
    )[:8]
    with_source_count = sum(1 for fact in evidence_facts if fact["hasSourceEvidence"])
    merged_count = sum(1 for fact in evidence_facts if fact.get("mergedRange"))
    added = [delta for delta in differences if delta["deltaType"] == "ADDED"]
    removed = [delta for delta in differences if delta["deltaType"] == "REMOVED"]
    changed = [
        delta
        for delta in differences
        if delta["deltaType"] in {"VALUE_CHANGED", "OPTIONAL_CHANGED"}
    ]
    target_label = _trim_label(target_trim)
    return {
        "targetTrimId": str(target_trim.get("trimId") or ""),
        "targetLabel": target_label,
        "targetTrim": _trim_fact(target_trim),
        "differenceCounts": {
            **counts,
            "totalDifference": len(differences),
        },
        # Upgrade pairing is intentionally left to the LLM. Facts and exact
        # evidence keys remain deterministic and server-owned.
        "upgradeSignals": [],
        "evidenceFacts": evidence_facts,
        "addedFeatures": [_feature_label(item["row"]) for item in added[:16]],
        "removedFeatures": [_feature_label(item["row"]) for item in removed[:16]],
        "changedFeatures": [
            {
                "feature": _feature_label(item["row"]),
                "baseValue": _display_value(item.get("baseValue")),
                "targetValue": _display_value(item.get("targetValue")),
            }
            for item in changed[:16]
        ],
        "businessFocusGroups": [
            {
                "label": fact["category"],
                "count": fact["totalDifferenceCount"],
                "evidence": fact["changeSummary"],
                "sampleFeatures": fact["sampleFeatures"],
            }
            for fact in category_facts[:8]
        ],
        "categoryFacts": category_facts,
        "sourceEvidenceSummary": {
            "differenceCount": len(differences),
            "withSourceEvidenceCount": with_source_count,
            "missingSourceEvidenceCount": max(0, len(evidence_facts) - with_source_count),
            "inferredCount": counts["inferred"],
            "unknownCount": counts["unknown"],
            "mergedCellExpandedCount": merged_count,
            "sourceSheetNames": source_names,
            "sourceEvidencePolicy": (
                "Only exact evidenceKey references are formal citations; inferred values are not source text."
            ),
        },
        "context": _target_context(base_trim, target_trim),
        "evidence": {
            "inferredCount": counts["inferred"],
            "unknownCount": counts["unknown"],
            "warning": (
                "不配备* 是规则推断，不是 Excel 原文；引用前需要核对来源证据。"
                if counts["inferred"]
                else "当前差异可继续从单元格追溯来源。"
            ),
        },
    }


def _delta_fact(
    row: dict[str, Any],
    *,
    base_trim: dict[str, Any],
    base_index: int,
    target_trim: dict[str, Any],
    target_index: int,
) -> dict[str, Any]:
    base_value = _value_at(row, base_index)
    target_value = _value_at(row, target_index)
    return {
        "row": row,
        "baseTrim": base_trim,
        "targetTrim": target_trim,
        "baseValue": base_value,
        "targetValue": target_value,
        "deltaType": classify_config_delta(base_value, target_value),
        "inferred": bool(
            (base_value or {}).get("inferred")
            or (target_value or {}).get("inferred")
        ),
    }


def _evidence_fact(delta: dict[str, Any]) -> dict[str, Any]:
    row = delta["row"]
    target_trim = delta["targetTrim"]
    evidence_trim, evidence_cell = _evidence_target(delta)
    source = _dict((evidence_cell or {}).get("source"))
    business_note = str(row.get("businessNote") or "").strip() or None
    return {
        "evidenceKey": _evidence_key(delta),
        "targetTrimId": str(target_trim.get("trimId") or ""),
        "featureCode": str(row.get("featureCode") or ""),
        "featureName": _feature_label(row),
        "category": str(row.get("category") or "Unknown"),
        "deltaType": delta["deltaType"],
        "businessNote": business_note,
        "requiresReview": bool(business_note and REVIEW_PATTERN.search(business_note)),
        "inferred": bool(delta["inferred"]),
        "baseValue": _display_value(delta.get("baseValue")),
        "targetValue": _display_value(delta.get("targetValue")),
        "evidenceTrimId": str(evidence_trim.get("trimId") or ""),
        "hasSourceEvidence": bool(source),
        "sourceSheetName": source.get("sheetName"),
        "sourceCell": source.get("cell"),
        "sourceOriginCell": source.get("sourceCell"),
        "mergedRange": source.get("mergedRange"),
        "inferenceReason": (evidence_cell or {}).get("inferenceReason") or source.get("inferenceReason"),
        "confidence": (evidence_cell or {}).get("confidence") or source.get("confidence"),
    }


def _evidence_target(delta: dict[str, Any]) -> tuple[dict[str, Any], dict | None]:
    base_value = delta.get("baseValue")
    target_value = delta.get("targetValue")
    if isinstance(target_value, dict) and target_value.get("inferred"):
        return delta["targetTrim"], target_value
    if isinstance(base_value, dict) and base_value.get("inferred"):
        return delta["baseTrim"], base_value
    if delta["deltaType"] == "REMOVED":
        return delta["baseTrim"], base_value
    return delta["targetTrim"], target_value


def _evidence_key(delta: dict[str, Any]) -> str:
    row = delta["row"]
    row_key = str(row.get("featureCode") or "").strip()
    if not row_key:
        row_key = _stable_key(f"{row.get('category') or ''}-{row.get('featureName') or ''}")
    return f"{delta['targetTrim'].get('trimId')}:{delta['deltaType']}:{row_key}"


def _filtered_rows(
    compare_facts: dict[str, Any],
    base_index: int,
    target_indexes: list[int],
    scope: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = _dict_list(compare_facts.get("rows"))
    category = str(scope.get("category") or "").strip().casefold()
    search = str(scope.get("search") or "").strip().casefold()
    delta_filter = scope["deltaFilter"]
    result = []
    for row in rows:
        if category and str(row.get("category") or "").strip().casefold() != category:
            continue
        if search:
            haystack = " ".join(
                str(row.get(key) or "")
                for key in ("category", "featureCode", "featureName", "businessNote")
            ).casefold()
            if search not in haystack:
                continue
        deltas = [
            classify_config_delta(_value_at(row, base_index), _value_at(row, target_index))
            for target_index in target_indexes
        ]
        selected_indexes = [base_index, *target_indexes]
        selected_values = [_value_at(row, index) for index in selected_indexes]
        if not _matches_delta_filter(delta_filter, deltas, selected_values):
            continue
        result.append(row)
    return result


def _matches_delta_filter(
    delta_filter: str,
    deltas: list[str],
    values: list[dict | None],
) -> bool:
    if delta_filter == "ALL":
        return True
    if delta_filter == "COMMON":
        return bool(deltas) and all(delta == "SAME" for delta in deltas)
    if delta_filter == "DIFFERENCE":
        return any(delta != "SAME" for delta in deltas)
    if delta_filter == "INFERRED":
        return any(delta != "SAME" for delta in deltas) and any(
            bool((value or {}).get("inferred")) for value in values
        )
    if delta_filter == "MISSING_SOURCE":
        return any(value is None or not _dict(value.get("source")) for value in values)
    if delta_filter == "MERGED_SOURCE":
        return any(_merged_cell(value) for value in values)
    if delta_filter == "UNKNOWN":
        return "UNKNOWN" in deltas
    return delta_filter in deltas


def _evidence_summary(rows: list[dict[str, Any]], trim_count: int) -> dict[str, int]:
    inferred = 0
    merged = 0
    missing_source = 0
    missing_value = 0
    source_issue_values = 0
    source_issue_rows = 0
    for row in rows:
        row_issue = False
        for value in row.get("values") or []:
            if not isinstance(value, dict):
                missing_value += 1
                source_issue_values += 1
                row_issue = True
                continue
            if not _dict(value.get("source")):
                missing_source += 1
                source_issue_values += 1
                row_issue = True
            if value.get("inferred"):
                inferred += 1
            if _merged_cell(value):
                merged += 1
        if row_issue:
            source_issue_rows += 1
    return {
        "rowCount": len(rows),
        "trimCount": trim_count,
        "valueCount": len(rows) * trim_count,
        "inferredValueCount": inferred,
        "mergedCellValueCount": merged,
        "missingSourceValueCount": missing_source,
        "missingValueCount": missing_value,
        "sourceIssueRowCount": source_issue_rows,
        "sourceIssueValueCount": source_issue_values,
    }


def _compare_scope(
    trims: list[dict[str, Any]],
    targets: list[dict[str, Any]],
    delta_filter: str,
) -> dict[str, Any]:
    markets = _unique_text(_trim_market(trim) for trim in trims)
    years = _unique_text(str(trim.get("modelYear") or "") for trim in trims)
    sources = _unique_text(str(trim.get("sourceFileName") or "") for trim in trims)
    missing_market = sum(1 for trim in trims if not _trim_market(trim))
    missing_year = sum(1 for trim in trims if not str(trim.get("modelYear") or "").strip())
    missing_source = sum(1 for trim in trims if not str(trim.get("sourceFileName") or "").strip())
    own_count = sum(1 for trim in trims if _own_product(trim))
    external_count = sum(1 for trim in trims if not _own_product(trim))
    inferred_count = sum(int(_dict(target.get("differenceCounts")).get("inferred") or 0) for target in targets)
    unknown_count = sum(int(_dict(target.get("differenceCounts")).get("unknown") or 0) for target in targets)
    missing_evidence_count = sum(
        int(_dict(target.get("sourceEvidenceSummary")).get("missingSourceEvidenceCount") or 0)
        for target in targets
    )
    source_groups = _source_review_groups(trims)
    source_hints = [str(group["reviewHint"]) for group in source_groups]
    caution = [
        "跨国家/市场对比时，差异可能来自区域配置，不一定是版本升级。" if len(markets) > 1 else None,
        "跨年款对比时，差异可能来自改款换代。" if len(years) > 1 else None,
        *source_hints,
        "多来源对比时，需核对网站、sheet 或上传来源一致性。" if len(sources) > 1 else None,
        "规则推断值不是 Excel 原文，引用前需要来源证据核对。" if inferred_count else None,
        "部分差异缺来源证据，不能直接写成确定卖点。" if missing_evidence_count else None,
    ]
    return {
        "deltaFilter": delta_filter,
        "targetCount": len(targets),
        "trimCount": len(trims),
        "marketScope": _scope_status(markets, missing_market, "same_market", "cross_market", "market_missing"),
        "markets": markets,
        "missingMarketCount": missing_market,
        "modelYearScope": _scope_status(years, missing_year, "same_model_year", "cross_model_year", "model_year_missing"),
        "modelYears": years,
        "missingModelYearCount": missing_year,
        "sourceScope": _scope_status(sources, missing_source, "same_source", "multi_source", "source_missing"),
        "sources": sources[:8],
        "sourceCount": len(sources),
        "missingSourceCount": missing_source,
        "sourceGroups": source_groups,
        "sourceReviewHints": source_hints,
        "identityScope": (
            "own_vs_competitor"
            if own_count and external_count
            else "competitor_only" if external_count else "own_product_only"
        ),
        "ownProductCount": own_count,
        "externalCount": external_count,
        "unknownIdentityCount": 0,
        "inferredCount": inferred_count,
        "unknownCount": unknown_count,
        "missingSourceEvidenceCount": missing_evidence_count,
        "caution": [item for item in caution if item],
    }


def _source_review_groups(trims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for trim in trims:
        key = (
            str(trim.get("brand") or "品牌待补"),
            str(trim.get("modelName") or "车型待补"),
            _trim_market(trim) or "市场待补",
            str(trim.get("modelYear") or "年款待补"),
        )
        groups[key].append(trim)
    result = []
    for (brand, model, market, year), items in groups.items():
        sources = _unique_text(str(item.get("sourceFileName") or "") for item in items)
        owners = _unique_text(str(item.get("sourceCreatedBy") or "") for item in items)
        if len(sources) <= 1 and len(owners) <= 1:
            continue
        result.append({
            "brand": brand,
            "modelName": model,
            "market": market,
            "modelYear": year,
            "label": f"{model} · {market} · {year}",
            "trimCount": len(items),
            "sourceCount": len(sources),
            "ownerCount": len(owners),
            "sources": sources[:8],
            "owners": owners[:8],
            "trimLabels": [_trim_label(item) for item in items[:8]],
            "reviewHint": (
                f"同国家同年款多来源：{model} · {market} · {year} 存在 {len(sources)} 个来源"
                f"{' / ' + str(len(owners)) + ' 个上传人' if len(owners) > 1 else ''}，AI 摘要需保留来源前提。"
            ),
        })
    return result


def _target_context(base_trim: dict[str, Any], target_trim: dict[str, Any]) -> list[dict[str, str]]:
    result = [{
        "label": "对比身份",
        "value": "本品 → 本品" if _own_product(base_trim) and _own_product(target_trim) else "本品 / 竞品",
        "detail": "身份由物料号和来源类型确定。",
        "tone": "ready",
    }]
    base_market = _trim_market(base_trim)
    target_market = _trim_market(target_trim)
    result.append({
        "label": "市场",
        "value": "同市场" if base_market and base_market == target_market else "跨市场 / 待补",
        "detail": f"{base_market or '待补'} → {target_market or '待补'}",
        "tone": "ready" if base_market and base_market == target_market else "warning",
    })
    return result


def _delta_counts(deltas: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "added": sum(1 for item in deltas if item["deltaType"] == "ADDED"),
        "removed": sum(1 for item in deltas if item["deltaType"] == "REMOVED"),
        "valueChanged": sum(1 for item in deltas if item["deltaType"] == "VALUE_CHANGED"),
        "optionalChanged": sum(1 for item in deltas if item["deltaType"] == "OPTIONAL_CHANGED"),
        "inferred": sum(1 for item in deltas if item["inferred"]),
        "unknown": sum(1 for item in deltas if item["deltaType"] == "UNKNOWN"),
    }


def _change_summary(counts: dict[str, int]) -> str:
    parts = [
        f"新增 {counts['added']}" if counts["added"] else None,
        f"减少 {counts['removed']}" if counts["removed"] else None,
        f"选装变化 {counts['optionalChanged']}" if counts["optionalChanged"] else None,
        f"值变化 {counts['valueChanged']}" if counts["valueChanged"] else None,
        f"待确认 {counts['unknown']}" if counts["unknown"] else None,
    ]
    return "，".join(item for item in parts if item) or "差异待确认"


def _normalized_filters(filters: dict[str, Any] | None) -> dict[str, Any]:
    raw = filters if isinstance(filters, dict) else {}
    delta_filter = str(raw.get("deltaFilter") or "ALL").upper()
    if delta_filter not in DELTA_FILTERS:
        delta_filter = "ALL"
    return {
        "deltaFilter": delta_filter,
        "category": str(raw.get("category") or "").strip() or None,
        "search": str(raw.get("search") or "").strip(),
        "targetTrimId": str(raw.get("targetTrimId") or "").strip() or None,
        "includeBusinessSummary": bool(raw.get("includeBusinessSummary", True)),
        "forceRefresh": bool(raw.get("forceRefresh", False)),
    }


def _trim_index(trims: list[dict[str, Any]], trim_id: str) -> int:
    for index, trim in enumerate(trims):
        if str(trim.get("trimId") or "") == trim_id:
            return index
    raise ValueError("base trim is not part of canonical compare facts")


def _target_indexes(
    trims: list[dict[str, Any]],
    base_index: int,
    target_trim_id: str | None,
) -> list[int]:
    if target_trim_id:
        target_index = _trim_index(trims, target_trim_id)
        if target_index == base_index:
            raise ValueError("target trim must differ from base trim")
        return [target_index]
    return [index for index in range(len(trims)) if index != base_index]


def _trim_fact(trim: dict[str, Any]) -> dict[str, Any]:
    return {
        "trimId": str(trim.get("trimId") or ""),
        "label": _trim_label(trim),
        "brand": trim.get("brand"),
        "modelName": trim.get("modelName"),
        "trimName": trim.get("trimName"),
        "market": _trim_market(trim) or None,
        "modelYear": trim.get("modelYear"),
        "materialNo": trim.get("materialNo"),
        "salesVersion": trim.get("salesVersion"),
        "dataOrigin": "本品" if _own_product(trim) else "竞品 / 外部",
        "source": trim.get("sourceFileName"),
    }


def _trim_label(trim: dict[str, Any]) -> str:
    return str(
        trim.get("fullTrimName")
        or trim.get("trimName")
        or trim.get("salesVersion")
        or trim.get("trimId")
        or "Unnamed trim"
    ).strip()


def _own_product(trim: dict[str, Any]) -> bool:
    return bool(str(trim.get("materialNo") or "").strip()) or trim.get("dataOrigin") == "own_catalog"


def _trim_market(trim: dict[str, Any]) -> str:
    return str(trim.get("market") or trim.get("country") or "").strip()


def _scope_status(values: list[str], missing: int, same: str, multi: str, missing_label: str) -> str:
    if missing:
        return missing_label
    return same if len(values) <= 1 else multi


def _range_label(delta_filter: str) -> str:
    return {
        "ALL": "全部配置行",
        "DIFFERENCE": "差异行",
        "ADDED": "新增配置",
        "REMOVED": "减少配置",
        "VALUE_CHANGED": "值变化",
        "OPTIONAL_CHANGED": "选装变化",
        "INFERRED": "规则推断",
        "MISSING_SOURCE": "来源问题",
        "MERGED_SOURCE": "合并格证据",
        "UNKNOWN": "待确认",
        "COMMON": "共同配置",
    }.get(delta_filter, "全部配置行")


def _feature_label(row: dict[str, Any]) -> str:
    return str(row.get("featureName") or row.get("featureCode") or "Unnamed feature").strip()


def _display_value(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    text = value.get("displayValue") or value.get("rawValue")
    return str(text) if text is not None else None


def _value_at(row: dict[str, Any], index: int) -> dict | None:
    values = row.get("values")
    if not isinstance(values, list) or index >= len(values):
        return None
    value = values[index]
    return value if isinstance(value, dict) else None


def _availability(value: dict | None) -> str:
    return str((value or {}).get("availability") or "")


def _unknown_cell(value: dict | None) -> bool:
    return value is None or _availability(value) == "UNKNOWN"


def _available_cell(value: dict | None) -> bool:
    return _availability(value) in AVAILABLE_AVAILABILITY


def _cell_signature(value: dict | None) -> tuple[str, str, str, str]:
    if value is None:
        return ("missing", "", "", "")
    return (
        _availability(value),
        str(value.get("normalizedValue") or ""),
        str(value.get("rawValue") or ""),
        str(value.get("displayValue") or ""),
    )


def _merged_cell(value: dict | None) -> bool:
    source = _dict((value or {}).get("source"))
    return bool(
        source.get("mergedRange")
        and source.get("sourceCell")
        and source.get("sourceCell") != source.get("cell")
    )


def _stable_key(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.strip().casefold())
    return re.sub(r"[^a-z0-9\u4e00-\u9fa5]+", "-", normalized).strip("-") or "uncategorized"


def _unique_text(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _dict_list(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
