"""Generate heuristic VOC enriched signals and deck artifacts from raw captures."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import json
import math
from pathlib import Path
import re
from typing import Any

from jato_scraper.voc_taxonomy import get_voc_taxonomy_profile

REPO_ROOT = Path(__file__).resolve().parents[2]
_WHITESPACE_RE = re.compile(r"\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\s*[;\n]+\s*")
_NON_WORD_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)

_COMMON_BOILERPLATE_MARKERS = (
    "loggført",
    "snarveier",
    "digitale tjenester",
    "kontakt",
    "tips oss",
    "kundesupport",
    "startet av",
    "les mer om:",
    "ikke ansvarlig for innhold",
    "kopiering av materiale",
    "hero member",
    "innlegg:",
    "lokasjon:",
    "hoppa till sida",
    "alla tidsangivelser",
    "europe/stockholm",
    "gjest",
)
_STOPWORD_HINTS = (
    " the ",
    " and ",
    " for ",
    " with ",
    " that ",
    " but ",
    " och ",
    " det ",
    " som ",
    " med ",
    " ikke ",
    " og ",
    " men ",
    " med ",
)

_PAIN_POINT_RULES: dict[str, tuple[str, ...]] = {
    "winter_range": (
        "winter range",
        "range loss",
        "cold weather",
        "cold-weather",
        "winter",
        "vinter",
        "kulde",
        "kallt",
    ),
    "charging_queue": (
        "charging queue",
        "queue at charger",
        "charger queue",
        "waiting for charger",
        "charging line",
        "ladeko",
        "k for lading",
    ),
    "public_charging_reliability": (
        "public charging",
        "charger broken",
        "charging reliability",
        "charging network",
        "broken charger",
        "snabbladd",
        "hurtiglading",
    ),
    "software_bug": (
        "software bug",
        "software issue",
        "software update",
        "ota",
        "app issue",
        "programvare",
        "uppdater",
        "bug",
    ),
    "service_wait_time": (
        "service wait",
        "dealer wait",
        "service center",
        "verkstad",
        "aftersales",
        "after sales",
        "service appointment",
    ),
    "price_value": (
        "price",
        "pricing",
        "lease",
        "leasing",
        "tco",
        "budget",
        "subsid",
        "discount",
        "cost",
        "pris",
        "erbjud",
    ),
    "delivery_delay": (
        "delivery delay",
        "late delivery",
        "delivery time",
        "waited months",
        "order tracker",
        "leverans",
        "forsink",
    ),
}

_PRODUCT_SIGNAL_RULES: dict[str, tuple[str, ...]] = {
    "price_value": _PAIN_POINT_RULES["price_value"],
    "range_efficiency": (
        "range",
        "efficiency",
        "consumption",
        "battery",
        "winter range",
        "real-world range",
    ),
    "charging_speed": (
        "charging speed",
        "fast charging",
        "dc charging",
        "home charging",
        "charger",
        "lader",
        "ladda",
    ),
    "software_ui": (
        "software",
        "ota",
        "infotainment",
        "app",
        "carplay",
        "android auto",
        "ui",
    ),
    "reliability_quality": (
        "quality",
        "reliability",
        "recall",
        "fault",
        "issue",
        "problem",
        "build quality",
    ),
    "service_after_sales": (
        "dealer",
        "service",
        "aftersales",
        "after sales",
        "verkstad",
        "service center",
    ),
    "comfort_space": (
        "space",
        "family",
        "trunk",
        "cargo",
        "seat",
        "third row",
        "tow",
        "trailer",
        "boat",
        "caravan",
    ),
}

_OWNERSHIP_STAGE_RULES: dict[str, tuple[str, ...]] = {
    "shopping": (
        "considering",
        "cross-shopping",
        "comparing",
        "test drive",
        "looking at",
        "shopping",
        "privatleasing",
        "offer",
        "lease",
    ),
    "ordering_delivery": (
        "ordered",
        "delivery",
        "order status",
        "waiting for car",
        "order tracker",
        "leverans",
        "delivery delay",
    ),
    "daily_use": (
        "daily use",
        "commute",
        "family trip",
        "school run",
        "weekend trip",
        "road trip",
        "every day",
    ),
    "charging_energy": (
        "charging",
        "charger",
        "home charging",
        "public charging",
        "charging queue",
        "lader",
        "ladda",
    ),
    "service_after_sales": (
        "service",
        "dealer",
        "warranty",
        "repair",
        "verkstad",
        "service center",
    ),
    "quality_reliability": (
        "bug",
        "issue",
        "problem",
        "reliability",
        "quality",
        "fault",
        "recall",
    ),
}

_POWERTRAIN_RULES: dict[str, tuple[str, ...]] = {
    "BEV": ("bev", "battery electric", "full ev", "full bev", "electric vehicle", "elbil"),
    "PHEV": ("phev", "plug-in hybrid", "plug in hybrid", "ladhybrid", "ladbar hybrid"),
    "HEV": ("hev", "hybrid", "mild hybrid", "self charging hybrid"),
    "ICE": ("diesel", "petrol", "gasoline", "ice", "combustion"),
}

_POSITIVE_SENTIMENT_HINTS = (
    "good",
    "great",
    "love",
    "smooth",
    "reliable",
    "satisfied",
    "recommend",
    "impressed",
    "better",
)
_NEGATIVE_SENTIMENT_HINTS = (
    "issue",
    "problem",
    "bug",
    "fail",
    "queue",
    "expensive",
    "delay",
    "complaint",
    "broken",
    "worse",
    "painful",
)

_PAIN_POINT_LABELS = {
    "winter_range": "Winter range",
    "charging_queue": "Charging queue",
    "public_charging_reliability": "Public charging reliability",
    "software_bug": "Software / OTA bugs",
    "service_wait_time": "Service wait time",
    "price_value": "Price / TCO / lease value",
    "delivery_delay": "Delivery delay",
}
_PRODUCT_SIGNAL_LABELS = {
    "price_value": "Price / value",
    "range_efficiency": "Range / efficiency",
    "charging_speed": "Charging convenience",
    "software_ui": "Software / UI",
    "reliability_quality": "Reliability / quality",
    "service_after_sales": "Service / aftersales",
    "comfort_space": "Space / towing / family use",
}
_OWNERSHIP_STAGE_LABELS = {
    "shopping": "Shopping / cross-shop",
    "ordering_delivery": "Ordering / delivery",
    "daily_use": "Daily use",
    "charging_energy": "Charging / energy",
    "service_after_sales": "Service / aftersales",
    "quality_reliability": "Quality / reliability",
}
_DECISION_FACTOR_LABELS = {
    "price_tco": "Price / TCO / incentives",
    "range_charging": "Range / charging / winter usability",
    "software_quality": "Software / OTA / reliability",
    "service_delivery": "Service / delivery / dealer experience",
    "family_practicality": "Space / towing / family practicality",
}
_SCORE_BAND_LABELS = {
    "high_signal": "High-signal match",
    "medium_signal": "Medium-signal match",
    "thin_signal": "Thin-signal match",
}
_SYNERGY_GROUP_LABELS = {
    "themeTag": "Theme tag",
    "personaCohort": "Persona cohort",
    "decisionFactor": "Decision factor",
    "productSignal": "Product signal",
    "painPoint": "Pain point",
    "matchedProduct": "Matched product",
    "powertrain": "Powertrain",
}
_COMPARISON_HINTS = (
    " vs ",
    " versus ",
    " compare ",
    " comparing ",
    " cross-shopping ",
    " cross shopping ",
    " against ",
    " instead of ",
)
_EXPLICIT_CONTENT_UNIT_KEYS = (
    ("contentUnits", "content_unit"),
    ("comments", "comment"),
    ("replyPosts", "reply_post"),
    ("readerComments", "reader_comment"),
    ("publicComments", "public_comment"),
)


def _normalize_space(value: Any) -> str:
    return _WHITESPACE_RE.sub(" ", str(value or "")).strip()


def _resolve_repo_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return REPO_ROOT / candidate


def _normalize_country_filter(values: list[str] | None) -> set[str] | None:
    if not values:
        return None
    normalized = {value.strip().upper() for value in values if value.strip()}
    return normalized or None


def _load_json_file(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _share(value: int, total: int) -> float:
    return round((value / total), 4) if total > 0 else 0.0


def _build_share_items(
    counter: Counter[str],
    *,
    total: int,
    limit: int,
    labels: dict[str, str] | None = None,
    mention_counter: Counter[str] | None = None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for raw_label, value in counter.most_common(limit):
        item = {
            "label": (labels or {}).get(raw_label, raw_label),
            "rawLabel": raw_label,
            "value": int(value),
            "sharePct": _share(int(value), total),
        }
        if mention_counter is not None:
            item["mentionCount"] = int(mention_counter.get(raw_label, 0))
        items.append(item)
    return items


def _get_analysis_profile(profile_name: str | None) -> dict[str, Any]:
    normalized = str(profile_name or "").strip() or "nordic_core"
    try:
        return get_voc_taxonomy_profile(normalized)
    except ValueError:
        return get_voc_taxonomy_profile("nordic_core")


def _build_entry_labels(entries: list[dict[str, Any]]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for entry in entries:
        key = str(entry.get("key") or "").strip()
        label = str(entry.get("label") or key).strip()
        if key:
            labels[key] = label or key
    return labels


def _build_entry_rules(
    entries: list[dict[str, Any]],
    *,
    value_key: str = "keywords",
) -> dict[str, tuple[str, ...]]:
    rules: dict[str, tuple[str, ...]] = {}
    for entry in entries:
        key = str(entry.get("key") or "").strip()
        if not key:
            continue
        values = entry.get(value_key) or []
        normalized_values = {
            str(value).strip()
            for value in values
            if str(value).strip()
        }
        normalized_values.add(str(entry.get("label") or key).strip())
        normalized_values.add(key.replace("_", " "))
        rules[key] = tuple(sorted(normalized_values))
    return rules


def _dedupe_preserve(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _pick_primary_product(
    document: dict[str, Any],
    product_mentions: list[str],
    product_rules: dict[str, tuple[str, ...]],
) -> str | None:
    if not product_mentions:
        return None
    title_text = _normalize_space(
        " ".join(
            [
                str(document.get("title") or ""),
                str(document.get("summary") or ""),
            ]
        )
    )
    if title_text:
        title_matches, _ = _find_matches(title_text, product_rules)
        for match in title_matches:
            if match in product_mentions:
                return match
    return product_mentions[0]


def _infer_competitor_mentions(
    text: str,
    *,
    product_mentions: list[str],
    primary_product: str | None,
    product_rules: dict[str, tuple[str, ...]],
) -> list[str]:
    if len(product_mentions) <= 1:
        return []
    competitor_mentions: list[str] = []
    for sentence in _text_sentences(text):
        normalized = f" {sentence.lower()} "
        sentence_mentions, _ = _find_matches(sentence, product_rules)
        if not sentence_mentions:
            continue
        if len(sentence_mentions) >= 2 or any(hint in normalized for hint in _COMPARISON_HINTS):
            competitor_mentions.extend(
                match
                for match in sentence_mentions
                if match != primary_product
            )
    if not competitor_mentions:
        competitor_mentions = [
            match for match in product_mentions if match != primary_product
        ]
    return _dedupe_preserve(competitor_mentions)


def _score_signal_band(overall_score: int) -> str:
    if overall_score >= 70:
        return "high_signal"
    if overall_score >= 45:
        return "medium_signal"
    return "thin_signal"


def _build_auto_scores(
    document: dict[str, Any],
    *,
    alias_hit_count: int,
    primary_in_title: bool,
) -> dict[str, Any]:
    quality_score = int(document.get("qualityScore") or 0)
    observation_count = int(document.get("observationCount") or 0)
    theme_count = len(document.get("themeTags") or [])
    persona_count = len(document.get("personaTags") or [])
    product_count = len(document.get("productMentions") or [])
    competitor_count = len(document.get("competitorMentions") or [])
    signal_count = len(document.get("painPoints") or []) + len(document.get("productSignals") or [])
    relevance_score = min(
        100,
        quality_score * 5
        + observation_count * 7
        + theme_count * 6
        + signal_count * 4
        + (10 if bool(document.get("publishReady")) else 0),
    )
    persona_score = min(
        100,
        persona_count * 24
        + len(document.get("decisionFactors") or []) * 10
        + theme_count * 5
        + competitor_count * 4,
    )
    match_confidence = min(
        100,
        (35 if document.get("primaryProduct") else 0)
        + product_count * 12
        + alias_hit_count * 5
        + (10 if primary_in_title else 0)
        + min(observation_count * 4, 16),
    )
    overall_score = round((relevance_score + persona_score + match_confidence) / 3)
    return {
        "relevanceScore": relevance_score,
        "personaScore": persona_score,
        "matchConfidence": match_confidence,
        "overallScore": overall_score,
        "scoreBand": _score_signal_band(overall_score),
    }


def _build_pair_items(
    counter: Counter[tuple[str, str]],
    *,
    left_labels: dict[str, str],
    right_labels: dict[str, str],
    left_key_name: str,
    left_label_name: str,
    right_key_name: str,
    right_label_name: str,
    limit: int = 18,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for (left_key, right_key), value in counter.most_common(limit):
        items.append(
            {
                left_key_name: left_key,
                left_label_name: left_labels.get(left_key, left_key),
                right_key_name: right_key,
                right_label_name: right_labels.get(right_key, right_key),
                "count": int(value),
            }
        )
    return items


def _build_association_specs(
    *,
    theme_labels: dict[str, str],
    persona_labels: dict[str, str],
    product_labels: dict[str, str],
) -> list[dict[str, Any]]:
    return [
        {
            "group": "themeTag",
            "groupLabel": _SYNERGY_GROUP_LABELS["themeTag"],
            "field": "themeTags",
            "fieldLabel": "themeTags",
            "labels": theme_labels,
        },
        {
            "group": "personaCohort",
            "groupLabel": _SYNERGY_GROUP_LABELS["personaCohort"],
            "field": "personaTags",
            "fieldLabel": "personaTags",
            "labels": persona_labels,
        },
        {
            "group": "decisionFactor",
            "groupLabel": _SYNERGY_GROUP_LABELS["decisionFactor"],
            "field": "decisionFactors",
            "fieldLabel": "decisionFactors",
            "labels": _DECISION_FACTOR_LABELS,
        },
        {
            "group": "productSignal",
            "groupLabel": _SYNERGY_GROUP_LABELS["productSignal"],
            "field": "productSignals",
            "fieldLabel": "productSignals",
            "labels": _PRODUCT_SIGNAL_LABELS,
        },
        {
            "group": "painPoint",
            "groupLabel": _SYNERGY_GROUP_LABELS["painPoint"],
            "field": "painPoints",
            "fieldLabel": "painPoints",
            "labels": _PAIN_POINT_LABELS,
        },
        {
            "group": "matchedProduct",
            "groupLabel": _SYNERGY_GROUP_LABELS["matchedProduct"],
            "field": "productMentions",
            "fieldLabel": "productMentions",
            "labels": product_labels,
        },
        {
            "group": "powertrain",
            "groupLabel": _SYNERGY_GROUP_LABELS["powertrain"],
            "field": "powertrains",
            "fieldLabel": "powertrains",
            "labels": {},
        },
    ]


def _build_association_nodes(
    document: dict[str, Any],
    *,
    specs: list[dict[str, Any]],
) -> list[dict[str, str]]:
    nodes: list[dict[str, str]] = []
    seen: set[str] = set()

    def _append(group: str, group_label: str, field: str, key: str, label: str) -> None:
        normalized_key = str(key or "").strip()
        if not normalized_key:
            return
        node_id = f"{group}:{normalized_key}"
        if node_id in seen:
            return
        seen.add(node_id)
        nodes.append(
            {
                "nodeId": node_id,
                "group": group,
                "groupLabel": group_label,
                "field": field,
                "key": normalized_key,
                "label": str(label or normalized_key).strip() or normalized_key,
            }
        )

    for spec in specs:
        group = str(spec.get("group") or "").strip()
        group_label = str(spec.get("groupLabel") or group).strip() or group
        field = str(spec.get("field") or "").strip()
        labels = dict(spec.get("labels") or {})
        if not group or not field:
            continue
        for key in set(document.get(field) or []):
            _append(group, group_label, field, key, labels.get(key, key))
    return nodes


def _association_expected_count(
    *,
    left_support: int,
    right_support: int,
    total: int,
) -> float:
    if total <= 0:
        return 0.0
    return round((left_support / total) * (right_support / total) * total, 4)


def _association_phi(
    *,
    pair_count: int,
    left_support: int,
    right_support: int,
    total: int,
) -> float:
    a = pair_count
    b = max(left_support - pair_count, 0)
    c = max(right_support - pair_count, 0)
    d = max(total - a - b - c, 0)
    denominator = (a + b) * (a + c) * (b + d) * (c + d)
    if denominator <= 0:
        return 0.0
    return round(((a * d) - (b * c)) / math.sqrt(denominator), 4)


def _association_npmi(
    *,
    pair_count: int,
    left_support: int,
    right_support: int,
    total: int,
) -> float:
    if total <= 0 or pair_count <= 0 or left_support <= 0 or right_support <= 0:
        return 0.0
    pair_prob = pair_count / total
    left_prob = left_support / total
    right_prob = right_support / total
    if pair_prob <= 0 or left_prob <= 0 or right_prob <= 0:
        return 0.0
    denominator = -math.log2(pair_prob)
    if denominator <= 0:
        return 0.0
    pmi = math.log2(pair_prob / (left_prob * right_prob))
    return round(pmi / denominator, 4)


def _fisher_enrichment_p_value(
    *,
    pair_count: int,
    left_support: int,
    right_support: int,
    total: int,
) -> float:
    if total <= 0 or pair_count <= 0 or left_support <= 0 or right_support <= 0:
        return 1.0
    maximum_overlap = min(left_support, right_support)
    minimum_overlap = max(0, left_support + right_support - total)
    if pair_count < minimum_overlap or pair_count > maximum_overlap:
        return 1.0
    denominator = math.comb(total, left_support)
    if denominator <= 0:
        return 1.0
    probability = 0.0
    for overlap in range(pair_count, maximum_overlap + 1):
        probability += (
            math.comb(right_support, overlap)
            * math.comb(total - right_support, left_support - overlap)
        ) / denominator
    return round(min(max(probability, 0.0), 1.0), 10)


def _apply_bh_adjustment(
    items: list[dict[str, Any]],
    *,
    p_value_key: str,
    output_key: str,
) -> None:
    if not items:
        return
    ordered = sorted(
        enumerate(items),
        key=lambda pair: float(pair[1].get(p_value_key, 1.0)),
    )
    adjusted_values: list[float] = [1.0] * len(items)
    running = 1.0
    total = len(ordered)
    for rank in range(total, 0, -1):
        index, item = ordered[rank - 1]
        p_value = float(item.get(p_value_key, 1.0))
        adjusted = min(running, (p_value * total) / rank)
        running = adjusted
        adjusted_values[index] = round(min(adjusted, 1.0), 10)
    for index, adjusted in enumerate(adjusted_values):
        items[index][output_key] = adjusted


def _month_bucket(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw[:7] if re.match(r"^\d{4}-\d{2}", raw) else ""
    return f"{parsed.year:04d}-{parsed.month:02d}"


def _build_association_matrix(
    pair_counter: Counter[tuple[str, str]],
    *,
    node_counter: Counter[str],
    node_meta: dict[str, dict[str, str]],
    pair_source_codes: dict[tuple[str, str], set[str]],
    pair_site_types: dict[tuple[str, str], set[str]],
    pair_month_buckets: dict[tuple[str, str], set[str]],
    total: int,
    limit: int = 28,
    validation_alpha: float = 0.05,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for (left_id, right_id), count in pair_counter.items():
        left = node_meta.get(left_id)
        right = node_meta.get(right_id)
        if not left or not right:
            continue
        left_support = int(node_counter.get(left_id, 0))
        right_support = int(node_counter.get(right_id, 0))
        if left_support <= 0 or right_support <= 0:
            continue
        support_pct = _share(int(count), total)
        left_share = _share(left_support, total)
        right_share = _share(right_support, total)
        confidence_forward_pct = round((int(count) / left_support), 4)
        confidence_reverse_pct = round((int(count) / right_support), 4)
        base_rate = (left_support / total) * (right_support / total) if total > 0 else 0.0
        lift = round((support_pct / base_rate), 4) if base_rate > 0 else 0.0
        jaccard = round(int(count) / (left_support + right_support - int(count)), 4) if (left_support + right_support - int(count)) > 0 else 0.0
        expected_count = _association_expected_count(
            left_support=left_support,
            right_support=right_support,
            total=total,
        )
        fisher_p_value = _fisher_enrichment_p_value(
            pair_count=int(count),
            left_support=left_support,
            right_support=right_support,
            total=total,
        )
        npmi = _association_npmi(
            pair_count=int(count),
            left_support=left_support,
            right_support=right_support,
            total=total,
        )
        phi = _association_phi(
            pair_count=int(count),
            left_support=left_support,
            right_support=right_support,
            total=total,
        )
        source_codes = sorted(pair_source_codes.get((left_id, right_id), set()))
        site_types = sorted(pair_site_types.get((left_id, right_id), set()))
        month_buckets = sorted(pair_month_buckets.get((left_id, right_id), set()))
        items.append(
            {
                "leftNodeId": left_id,
                "leftGroup": left["group"],
                "leftGroupLabel": left["groupLabel"],
                "leftField": left.get("field", ""),
                "leftKey": left["key"],
                "leftLabel": left["label"],
                "rightNodeId": right_id,
                "rightGroup": right["group"],
                "rightGroupLabel": right["groupLabel"],
                "rightField": right.get("field", ""),
                "rightKey": right["key"],
                "rightLabel": right["label"],
                "count": int(count),
                "supportPct": support_pct,
                "leftSupport": left_support,
                "leftSharePct": left_share,
                "rightSupport": right_support,
                "rightSharePct": right_share,
                "confidenceForwardPct": confidence_forward_pct,
                "confidenceReversePct": confidence_reverse_pct,
                "lift": lift,
                "jaccard": jaccard,
                "npmi": npmi,
                "phiCoefficient": phi,
                "expectedCount": expected_count,
                "fisherPValue": fisher_p_value,
                "sourceCount": len(source_codes),
                "sourceCodes": source_codes,
                "siteTypeCount": len(site_types),
                "siteTypes": site_types,
                "monthBucketCount": len(month_buckets),
                "monthBuckets": month_buckets,
                "validation": {
                    "passesCountGate": int(count) >= 2,
                    "passesExpectedCountGate": expected_count >= 1.0,
                    "fisherSignificant": fisher_p_value <= validation_alpha,
                    "replicatedAcrossSources": len(source_codes) >= 2,
                    "replicatedAcrossSiteTypes": len(site_types) >= 2,
                    "replicatedAcrossMonths": len(month_buckets) >= 2,
                },
            }
        )
    _apply_bh_adjustment(items, p_value_key="fisherPValue", output_key="fdrAdjustedPValue")
    for item in items:
        validation = item.get("validation") if isinstance(item.get("validation"), dict) else {}
        validation["fdrSignificant"] = float(item.get("fdrAdjustedPValue") or 1.0) <= validation_alpha
        item["validation"] = validation
    items.sort(
        key=lambda item: (
            -int(item["count"]),
            -float(item["npmi"]),
            -float(item["lift"]),
            str(item["leftLabel"]),
            str(item["rightLabel"]),
        )
    )
    return items[:limit]


def _build_filter_suggestions(
    synergy_items: list[dict[str, Any]],
    *,
    node_counter: Counter[str],
    total: int,
    limit: int = 16,
    per_anchor: int = 5,
) -> list[dict[str, Any]]:
    anchor_map: dict[str, dict[str, Any]] = {}
    for item in synergy_items:
        directional_pairs = (
            (
                item["leftNodeId"],
                item["leftGroup"],
                item["leftGroupLabel"],
                item["leftField"],
                item["leftKey"],
                item["leftLabel"],
                item["rightNodeId"],
                item["rightGroup"],
                item["rightGroupLabel"],
                item["rightField"],
                item["rightKey"],
                item["rightLabel"],
                item["confidenceForwardPct"],
            ),
            (
                item["rightNodeId"],
                item["rightGroup"],
                item["rightGroupLabel"],
                item["rightField"],
                item["rightKey"],
                item["rightLabel"],
                item["leftNodeId"],
                item["leftGroup"],
                item["leftGroupLabel"],
                item["leftField"],
                item["leftKey"],
                item["leftLabel"],
                item["confidenceReversePct"],
            ),
        )
        for (
            anchor_id,
            anchor_group,
            anchor_group_label,
            anchor_field,
            anchor_key,
            anchor_label,
            node_id,
            node_group,
            node_group_label,
            node_field,
            node_key,
            node_label,
            anchor_confidence_pct,
        ) in directional_pairs:
            anchor_item = anchor_map.setdefault(
                str(anchor_id),
                {
                    "anchorNodeId": anchor_id,
                    "anchorGroup": anchor_group,
                    "anchorGroupLabel": anchor_group_label,
                    "anchorField": anchor_field,
                    "anchorKey": anchor_key,
                    "anchorLabel": anchor_label,
                    "anchorSupport": int(node_counter.get(str(anchor_id), 0)),
                    "anchorSharePct": _share(int(node_counter.get(str(anchor_id), 0)), total),
                    "recommendations": [],
                },
            )
            anchor_item["recommendations"].append(
                {
                    "nodeId": node_id,
                    "group": node_group,
                    "groupLabel": node_group_label,
                    "field": node_field,
                    "key": node_key,
                    "label": node_label,
                    "count": int(item["count"]),
                    "supportPct": float(item["supportPct"]),
                    "confidencePct": float(anchor_confidence_pct),
                    "lift": float(item["lift"]),
                    "jaccard": float(item["jaccard"]),
                    "npmi": float(item["npmi"]),
                    "phiCoefficient": float(item["phiCoefficient"]),
                    "expectedCount": float(item["expectedCount"]),
                    "fisherPValue": float(item["fisherPValue"]),
                    "fdrAdjustedPValue": float(item["fdrAdjustedPValue"]),
                    "validation": dict(item.get("validation") or {}),
                }
            )

    suggestions = list(anchor_map.values())
    for item in suggestions:
        item["recommendations"].sort(
            key=lambda recommendation: (
                -float(recommendation["confidencePct"]),
                float(recommendation["fdrAdjustedPValue"]),
                -float(recommendation["npmi"]),
                -int(recommendation["count"]),
                str(recommendation["label"]),
            )
        )
        item["recommendations"] = item["recommendations"][:per_anchor]
    suggestions.sort(
        key=lambda item: (
            -int(item["anchorSupport"]),
            str(item["anchorLabel"]),
        )
    )
    return suggestions[:limit]


def _build_persona_cohort_summaries(
    documents: list[dict[str, Any]],
    *,
    persona_labels: dict[str, str],
    pain_point_labels: dict[str, str],
    decision_factor_labels: dict[str, str],
    product_labels: dict[str, str],
    total: int,
) -> list[dict[str, Any]]:
    cohorts: list[dict[str, Any]] = []
    for persona_key, persona_label in persona_labels.items():
        cohort_documents = [
            document
            for document in documents
            if persona_key in (document.get("personaTags") or [])
        ]
        if not cohort_documents:
            continue
        pain_counter: Counter[str] = Counter()
        factor_counter: Counter[str] = Counter()
        product_counter: Counter[str] = Counter()
        score_total = 0
        for document in cohort_documents:
            pain_counter.update(set(document.get("painPoints") or []))
            factor_counter.update(set(document.get("decisionFactors") or []))
            product_counter.update(set(document.get("productMentions") or []))
            score_total += int((document.get("autoScores") or {}).get("overallScore") or 0)
        cohorts.append(
            {
                "key": persona_key,
                "label": persona_label,
                "documentCount": len(cohort_documents),
                "sharePct": _share(len(cohort_documents), total),
                "avgOverallScore": round(score_total / len(cohort_documents), 1),
                "topPainPoints": _build_share_items(
                    pain_counter,
                    total=len(cohort_documents),
                    limit=3,
                    labels=pain_point_labels,
                ),
                "topDecisionFactors": _build_share_items(
                    factor_counter,
                    total=len(cohort_documents),
                    limit=3,
                    labels=decision_factor_labels,
                ),
                "topProducts": _build_share_items(
                    product_counter,
                    total=len(cohort_documents),
                    limit=3,
                    labels=product_labels,
                ),
            }
        )
    cohorts.sort(key=lambda item: (-int(item["documentCount"]), item["label"]))
    return cohorts


def _document_text(document: dict[str, Any]) -> str:
    return _normalize_space(
        " ".join(
            [
                str(document.get("title") or ""),
                str(document.get("summary") or ""),
                str(document.get("excerpt") or ""),
                str(document.get("rawText") or ""),
            ]
        )
    )


def _word_count(text: str) -> int:
    return len([token for token in _NON_WORD_RE.sub(" ", text).split() if token])


def _has_stopword_hint(text: str) -> bool:
    normalized = f" {text.lower()} "
    return any(token in normalized for token in _STOPWORD_HINTS)


def _looks_like_boilerplate(segment: str) -> bool:
    normalized = _normalize_space(segment)
    if not normalized:
        return True
    lower = normalized.lower()
    if any(marker in lower for marker in _COMMON_BOILERPLATE_MARKERS):
        return True
    if lower.startswith(("re:", "sv:")) and _word_count(lower) <= 6:
        return True
    if _word_count(lower) < 5 and not _has_stopword_hint(lower):
        return True
    return False


def _clean_document_text(document: dict[str, Any]) -> str:
    raw_segments: list[str] = []
    title = _normalize_space(document.get("title"))
    summary = _normalize_space(document.get("summary"))
    raw_text = _normalize_space(document.get("rawText"))
    excerpt = _normalize_space(document.get("excerpt"))
    if title:
        raw_segments.append(title)
    if summary and summary.casefold() != title.casefold():
        raw_segments.append(summary)
    if raw_text:
        raw_segments.extend(_text_sentences(raw_text))
    elif excerpt:
        raw_segments.extend(_text_sentences(excerpt))

    cleaned_segments: list[str] = []
    seen: set[str] = set()
    for segment in raw_segments:
        normalized = _normalize_space(segment)
        key = normalized.casefold()
        if not normalized or key in seen:
            continue
        seen.add(key)
        if _looks_like_boilerplate(normalized):
            continue
        cleaned_segments.append(normalized)
    return " ".join(cleaned_segments)


def _token_present(text: str, token: str) -> bool:
    normalized_text = text.lower()
    normalized_token = token.lower().strip()
    if not normalized_token:
        return False
    pattern = re.compile(rf"(?<!\w){re.escape(normalized_token)}(?!\w)", flags=re.UNICODE)
    return bool(pattern.search(normalized_text))


def _find_matches(text: str, rules: dict[str, tuple[str, ...]]) -> tuple[list[str], dict[str, list[str]]]:
    matched: list[str] = []
    matched_tokens: dict[str, list[str]] = {}
    normalized = text.lower()
    for key, tokens in rules.items():
        hits = [token for token in tokens if _token_present(normalized, token)]
        if hits:
            matched.append(key)
            matched_tokens[key] = hits
    return matched, matched_tokens


def _infer_sentiment(text: str) -> str:
    normalized = text.lower()
    positive_hits = sum(1 for token in _POSITIVE_SENTIMENT_HINTS if token in normalized)
    negative_hits = sum(1 for token in _NEGATIVE_SENTIMENT_HINTS if token in normalized)
    if positive_hits > 0 and negative_hits > 0:
        return "mixed"
    if negative_hits > 0:
        return "negative"
    if positive_hits > 0:
        return "positive"
    return "neutral"


def _pick_best_stage(matches: list[str]) -> str | None:
    if not matches:
        return None
    priority = {
        "ordering_delivery": 6,
        "charging_energy": 5,
        "service_after_sales": 4,
        "quality_reliability": 3,
        "daily_use": 2,
        "shopping": 1,
    }
    return max(matches, key=lambda item: priority.get(item, 0))


def _text_sentences(text: str) -> list[str]:
    return [segment.strip() for segment in _SENTENCE_SPLIT_RE.split(text) if segment.strip()]


def _select_evidence_snippets(text: str, tokens: list[str], *, limit: int = 2) -> list[str]:
    if not text:
        return []
    snippets: list[str] = []
    lower_tokens = [token.lower() for token in tokens if token]
    for sentence in _text_sentences(text):
        normalized = sentence.lower()
        if any(token in normalized for token in lower_tokens):
            snippets.append(sentence[:220])
        if len(snippets) >= limit:
            break
    if snippets:
        return snippets
    fallback = _normalize_space(text)[:220]
    return [fallback] if fallback else []


def _extract_signal_observations(text: str) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for sentence in _text_sentences(text):
        trimmed = _normalize_space(sentence)
        if not trimmed:
            continue
        sentence_matches = [
            ("painPoint", _PAIN_POINT_RULES, _PAIN_POINT_LABELS),
            ("productSignal", _PRODUCT_SIGNAL_RULES, _PRODUCT_SIGNAL_LABELS),
            ("ownershipStage", _OWNERSHIP_STAGE_RULES, _OWNERSHIP_STAGE_LABELS),
            ("powertrain", _POWERTRAIN_RULES, None),
        ]
        for signal_kind, rules, labels in sentence_matches:
            matched, token_map = _find_matches(trimmed, rules)
            for signal_key in matched:
                observations.append(
                    {
                        "signalKind": signal_kind,
                        "signalKey": signal_key,
                        "label": (labels or {}).get(signal_key, signal_key),
                        "sentence": trimmed[:280],
                        "matchedTokens": token_map.get(signal_key, []),
                        "sentiment": _infer_sentiment(trimmed),
                    }
                )
    return observations


def _analyze_text_item(
    source_document: dict[str, Any],
    analysis_text: str,
    *,
    theme_rules: dict[str, tuple[str, ...]],
    persona_rules: dict[str, tuple[str, ...]],
    product_rules: dict[str, tuple[str, ...]],
) -> tuple[dict[str, Any], dict[str, int | bool]]:
    pain_points, pain_point_tokens = _find_matches(analysis_text, _PAIN_POINT_RULES)
    product_signals, product_signal_tokens = _find_matches(analysis_text, _PRODUCT_SIGNAL_RULES)
    ownership_matches, ownership_tokens = _find_matches(analysis_text, _OWNERSHIP_STAGE_RULES)
    powertrains, powertrain_tokens = _find_matches(analysis_text, _POWERTRAIN_RULES)
    theme_tags, theme_tokens = _find_matches(analysis_text, theme_rules)
    persona_tags, persona_tokens = _find_matches(analysis_text, persona_rules)
    product_mentions, product_tokens = _find_matches(analysis_text, product_rules)
    observations = _extract_signal_observations(analysis_text)
    evidence_tokens: list[str] = []
    for token_list in [
        *pain_point_tokens.values(),
        *product_signal_tokens.values(),
        *ownership_tokens.values(),
        *powertrain_tokens.values(),
        *theme_tokens.values(),
        *persona_tokens.values(),
        *product_tokens.values(),
    ]:
        evidence_tokens.extend(token_list)
    primary_product = _pick_primary_product(
        source_document,
        product_mentions,
        product_rules,
    )
    primary_in_title = bool(
        primary_product
        and primary_product
        in (
            _find_matches(
                _normalize_space(
                    " ".join(
                        [
                            str(source_document.get("title") or ""),
                            str(source_document.get("summary") or ""),
                        ]
                    )
                ),
                product_rules,
            )[0]
        )
    )
    competitor_mentions = _infer_competitor_mentions(
        analysis_text,
        product_mentions=product_mentions,
        primary_product=primary_product,
        product_rules=product_rules,
    )
    analyzed = {
        "sentiment": _infer_sentiment(analysis_text),
        "ownershipStage": _pick_best_stage(ownership_matches),
        "painPoints": pain_points,
        "productSignals": product_signals,
        "powertrains": powertrains,
        "themeTags": theme_tags,
        "personaTags": persona_tags,
        "productMentions": product_mentions,
        "primaryProduct": primary_product,
        "competitorMentions": competitor_mentions,
        "decisionFactors": [],
        "cleanedText": analysis_text,
        "observationCount": len(observations),
        "observations": observations,
        "evidenceSnippets": _select_evidence_snippets(analysis_text, evidence_tokens),
    }
    analyzed["decisionFactors"] = _map_decision_factors(analyzed)
    return analyzed, {
        "aliasHitCount": sum(len(item) for item in product_tokens.values()),
        "primaryInTitle": primary_in_title,
    }


def _build_sentence_window_units(text: str) -> list[str]:
    units: list[str] = []
    current: list[str] = []
    current_chars = 0
    for sentence in _text_sentences(text):
        normalized = _normalize_space(sentence)
        if not normalized or _looks_like_boilerplate(normalized):
            continue
        current.append(normalized)
        current_chars += len(normalized)
        joined = " ".join(current)
        if len(current) >= 2 or current_chars >= 260 or _word_count(joined) >= 32:
            units.append(joined)
            current = []
            current_chars = 0
    if current:
        units.append(" ".join(current))
    if not units:
        fallback = _normalize_space(text)
        if fallback:
            units.append(fallback)
    return units


def _extract_analysis_units(
    source_document: dict[str, Any],
    *,
    cleaned_text: str,
) -> list[dict[str, Any]]:
    units: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _append_unit(text: Any, *, unit_type: str, unit_source: str, index: int, raw_unit: dict[str, Any] | None = None) -> None:
        normalized = _normalize_space(text)
        key = normalized.casefold()
        if not normalized or key in seen:
            return
        seen.add(key)
        raw_unit = raw_unit or {}
        units.append(
            {
                "unitId": str(raw_unit.get("unitId") or f'{source_document.get("url") or "unit"}#{unit_source}-{index}'),
                "unitType": str(raw_unit.get("unitType") or unit_type),
                "unitSource": unit_source,
                "text": normalized,
                "author": str(raw_unit.get("author") or ""),
                "publishedAt": raw_unit.get("publishedAt") or source_document.get("publishedAt"),
            }
        )

    for field_name, default_type in _EXPLICIT_CONTENT_UNIT_KEYS:
        raw_units = source_document.get(field_name)
        if not isinstance(raw_units, list):
            continue
        for index, raw_item in enumerate(raw_units, start=1):
            if isinstance(raw_item, dict):
                _append_unit(
                    raw_item.get("text")
                    or raw_item.get("rawText")
                    or raw_item.get("body")
                    or raw_item.get("content")
                    or raw_item.get("excerpt"),
                    unit_type=str(raw_item.get("unitType") or raw_item.get("kind") or default_type),
                    unit_source=str(raw_item.get("unitSource") or "explicit"),
                    index=index,
                    raw_unit=raw_item,
                )
            else:
                _append_unit(raw_item, unit_type=default_type, unit_source="explicit", index=index)
    if units:
        return units

    body_text = _normalize_space(source_document.get("rawText") or source_document.get("excerpt") or cleaned_text)
    for index, unit_text in enumerate(_build_sentence_window_units(body_text), start=1):
        _append_unit(unit_text, unit_type="sentence_window", unit_source="derived_sentence_window", index=index)
    return units


def _has_association_signal(item: dict[str, Any]) -> bool:
    for field_name in (
        "themeTags",
        "personaTags",
        "decisionFactors",
        "productSignals",
        "painPoints",
        "productMentions",
        "powertrains",
    ):
        if item.get(field_name):
            return True
    return False


def _map_decision_factors(document: dict[str, Any]) -> list[str]:
    factors: set[str] = set()
    pain_points = set(document.get("painPoints") or [])
    product_signals = set(document.get("productSignals") or [])
    if pain_points & {"price_value"} or product_signals & {"price_value"}:
        factors.add("price_tco")
    if pain_points & {"winter_range", "charging_queue", "public_charging_reliability"} or product_signals & {
        "range_efficiency",
        "charging_speed",
    }:
        factors.add("range_charging")
    if pain_points & {"software_bug"} or product_signals & {"software_ui", "reliability_quality"}:
        factors.add("software_quality")
    if pain_points & {"service_wait_time", "delivery_delay"} or product_signals & {"service_after_sales"}:
        factors.add("service_delivery")
    if product_signals & {"comfort_space"}:
        factors.add("family_practicality")
    return sorted(factors)


def _build_evidence_cards(documents: list[dict[str, Any]], *, limit: int = 6) -> list[dict[str, Any]]:
    def _preview_text(value: Any, *, limit: int = 1400) -> tuple[str, bool]:
        text = _normalize_space(value)
        if not text:
            return "", False
        if len(text) <= limit:
            return text, False
        return f"{text[:limit].rstrip()}…", True

    def _serialize_observations(value: Any, *, limit: int = 4) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        items: list[dict[str, Any]] = []
        for observation in value:
            if not isinstance(observation, dict):
                continue
            label = str(observation.get("label") or observation.get("signalKey") or "").strip()
            sentence = _normalize_space(observation.get("sentence"))
            if not label and not sentence:
                continue
            items.append(
                {
                    "signalKind": str(observation.get("signalKind") or "").strip(),
                    "label": label,
                    "sentence": sentence,
                    "matchedTokens": [
                        str(token).strip()
                        for token in observation.get("matchedTokens") or []
                        if str(token).strip()
                    ],
                    "sentiment": str(observation.get("sentiment") or "neutral").strip() or "neutral",
                }
            )
            if len(items) >= limit:
                break
        return items

    def _score(document: dict[str, Any]) -> tuple[int, int, int]:
        matched_count = len(document.get("painPoints") or []) + len(document.get("productSignals") or [])
        quality_score = int(document.get("qualityScore") or 0)
        publish_ready = 1 if bool(document.get("publishReady")) else 0
        return (publish_ready, matched_count, quality_score)

    cards: list[dict[str, Any]] = []
    for document in sorted(documents, key=_score, reverse=True)[:limit]:
        signals = [
            *_build_signal_labels(document.get("painPoints") or [], _PAIN_POINT_LABELS),
            *_build_signal_labels(document.get("productSignals") or [], _PRODUCT_SIGNAL_LABELS),
        ]
        content_preview, content_truncated = _preview_text(document.get("cleanedText") or document.get("excerpt"))
        observations = _serialize_observations(document.get("observations"))
        cards.append(
            {
                "title": str(document.get("title") or "(untitled)"),
                "url": str(document.get("url") or ""),
                "siteName": str(document.get("siteName") or ""),
                "siteType": str(document.get("siteType") or ""),
                "sourceCode": str(document.get("sourceCode") or ""),
                "countryCode": str(document.get("countryCode") or ""),
                "countryLabel": str(document.get("countryLabel") or ""),
                "language": str(document.get("language") or ""),
                "publishedAt": document.get("publishedAt"),
                "collectedAt": document.get("collectedAt"),
                "publishTier": str(document.get("publishTier") or ""),
                "publishDecision": str(document.get("publishDecision") or ""),
                "sentiment": str(document.get("sentiment") or ""),
                "qualityScore": int(document.get("qualityScore") or 0),
                "observationCount": int(document.get("observationCount") or len(observations)),
                "signals": signals[:4],
                "evidenceSnippets": list(document.get("evidenceSnippets") or [])[:2],
                "excerpt": str(document.get("excerpt") or ""),
                "contentPreview": content_preview,
                "contentTruncated": content_truncated,
                "observations": observations,
            }
        )
    return cards


def _build_signal_labels(values: list[str], labels: dict[str, str]) -> list[str]:
    return [labels.get(value, value) for value in values]


def load_country_voc_raw_payloads(country_root: str | Path) -> list[dict[str, Any]]:
    root = Path(country_root)
    raw_root = root / "raw"
    if not raw_root.exists():
        return []
    payloads: list[dict[str, Any]] = []
    for path in sorted(raw_root.glob("*.json")):
        payload = _load_json_file(path)
        if payload is None:
            continue
        payload["_rawPath"] = str(path)
        payloads.append(payload)
    return payloads


def build_country_voc_enrichment(country_root: str | Path) -> dict[str, Any]:
    root = Path(country_root)
    payloads = load_country_voc_raw_payloads(root)
    country_code = root.name.upper()
    country_label = country_code
    taxonomy_profile = ""
    analysis_profile = _get_analysis_profile(None)
    theme_entries = list(analysis_profile.get("themeTags") or [])
    persona_entries = list(analysis_profile.get("personaCohorts") or [])
    product_entries = list(analysis_profile.get("productCatalog") or [])
    theme_rules = _build_entry_rules(theme_entries)
    persona_rules = _build_entry_rules(persona_entries)
    product_rules = _build_entry_rules(product_entries, value_key="aliases")
    theme_labels = _build_entry_labels(theme_entries)
    persona_labels = _build_entry_labels(persona_entries)
    product_labels = _build_entry_labels(product_entries)
    association_specs = _build_association_specs(
        theme_labels=theme_labels,
        persona_labels=persona_labels,
        product_labels=product_labels,
    )
    source_runs: list[dict[str, Any]] = []
    documents: list[dict[str, Any]] = []
    total_error_count = 0

    for payload in payloads:
        source = payload.get("source")
        source_payload = source if isinstance(source, dict) else {}
        country_code = str(source_payload.get("country_code") or country_code).strip().upper() or country_code
        country_label = str(source_payload.get("country_label") or country_label).strip() or country_label
        taxonomy_profile = str(payload.get("taxonomyProfile") or taxonomy_profile).strip()
        analysis_profile = _get_analysis_profile(taxonomy_profile)
        theme_entries = list(analysis_profile.get("themeTags") or [])
        persona_entries = list(analysis_profile.get("personaCohorts") or [])
        product_entries = list(analysis_profile.get("productCatalog") or [])
        theme_rules = _build_entry_rules(theme_entries)
        persona_rules = _build_entry_rules(persona_entries)
        product_rules = _build_entry_rules(product_entries, value_key="aliases")
        theme_labels = _build_entry_labels(theme_entries)
        persona_labels = _build_entry_labels(persona_entries)
        product_labels = _build_entry_labels(product_entries)
        auto_review = payload.get("autoReview")
        source_auto_review = auto_review if isinstance(auto_review, dict) else {}
        source_runs.append(
            {
                "sourceCode": str(source_payload.get("source_code") or ""),
                "siteName": str(source_payload.get("site_name") or ""),
                "siteType": str(source_payload.get("site_type") or ""),
                "language": str(source_payload.get("language") or ""),
                "documentCount": int(payload.get("documentCount") or 0),
                "publishReadyCount": int(source_auto_review.get("publishReadyCount") or 0),
                "publishTier": str(source_auto_review.get("publishTier") or ""),
                "publishDecision": str(source_auto_review.get("publishDecision") or ""),
                "errorCount": len(payload.get("errors") or []),
                "rawPath": str(payload.get("_rawPath") or ""),
            }
        )
        total_error_count += len(payload.get("errors") or [])

        for document in payload.get("documents") or []:
            if not isinstance(document, dict):
                continue
            cleaned_text = _clean_document_text(document)
            analysis_text = cleaned_text or _document_text(document)
            signal_payload, signal_meta = _analyze_text_item(
                document,
                analysis_text,
                theme_rules=theme_rules,
                persona_rules=persona_rules,
                product_rules=product_rules,
            )
            auto_review = document.get("autoReview")
            document_auto_review = auto_review if isinstance(auto_review, dict) else {}
            enriched_document = {
                "sourceCode": str(document.get("sourceCode") or source_payload.get("source_code") or ""),
                "countryCode": str(document.get("countryCode") or country_code),
                "countryLabel": str(document.get("countryLabel") or country_label),
                "siteName": str(document.get("siteName") or source_payload.get("site_name") or ""),
                "siteType": str(document.get("siteType") or source_payload.get("site_type") or ""),
                "language": str(document.get("language") or source_payload.get("language") or ""),
                "url": str(document.get("url") or ""),
                "title": str(document.get("title") or ""),
                "publishedAt": document.get("publishedAt"),
                "excerpt": str(document.get("excerpt") or "")[:400],
                "collectedAt": document.get("collectedAt") or payload.get("collectedAt"),
                "publishTier": str(document_auto_review.get("publishTier") or ""),
                "publishDecision": str(document_auto_review.get("publishDecision") or ""),
                "publishReady": str(document_auto_review.get("publishDecision") or "") != "hold_raw",
                "qualityScore": int(document_auto_review.get("score") or 0),
                **signal_payload,
            }
            enriched_document["autoScores"] = _build_auto_scores(
                enriched_document,
                alias_hit_count=int(signal_meta["aliasHitCount"]),
                primary_in_title=bool(signal_meta["primaryInTitle"]),
            )
            analysis_units: list[dict[str, Any]] = []
            for unit in _extract_analysis_units(document, cleaned_text=analysis_text):
                unit_text = _normalize_space(unit.get("text"))
                if not unit_text:
                    continue
                unit_payload, _ = _analyze_text_item(
                    document,
                    unit_text,
                    theme_rules=theme_rules,
                    persona_rules=persona_rules,
                    product_rules=product_rules,
                )
                if not _has_association_signal(unit_payload):
                    continue
                analysis_units.append(
                    {
                        "unitId": str(unit.get("unitId") or ""),
                        "unitType": str(unit.get("unitType") or ""),
                        "unitSource": str(unit.get("unitSource") or ""),
                        "sourceCode": enriched_document["sourceCode"],
                        "siteName": enriched_document["siteName"],
                        "siteType": enriched_document["siteType"],
                        "language": enriched_document["language"],
                        "url": enriched_document["url"],
                        "author": str(unit.get("author") or ""),
                        "publishedAt": unit.get("publishedAt") or enriched_document.get("publishedAt"),
                        "text": unit_text,
                        **unit_payload,
                    }
                )
            enriched_document["analysisUnits"] = analysis_units
            documents.append(enriched_document)

    analysis_documents = [document for document in documents if bool(document.get("publishReady"))] or documents
    analysis_count = len(analysis_documents)
    association_transactions: list[dict[str, Any]] = []
    for document in analysis_documents:
        unit_items = document.get("analysisUnits") if isinstance(document.get("analysisUnits"), list) else []
        if unit_items:
            association_transactions.extend(
                item for item in unit_items if isinstance(item, dict)
            )
            continue
        if _has_association_signal(document):
            association_transactions.append(
                {
                    "unitId": str(document.get("url") or document.get("sourceCode") or "document"),
                    "unitType": "document_fallback",
                    "unitSource": "document_fallback",
                    "sourceCode": str(document.get("sourceCode") or ""),
                    "siteName": str(document.get("siteName") or ""),
                    "siteType": str(document.get("siteType") or ""),
                    "language": str(document.get("language") or ""),
                    "url": str(document.get("url") or ""),
                    "publishedAt": document.get("publishedAt"),
                    "text": str(document.get("cleanedText") or ""),
                    "themeTags": list(document.get("themeTags") or []),
                    "personaTags": list(document.get("personaTags") or []),
                    "decisionFactors": list(document.get("decisionFactors") or []),
                    "productSignals": list(document.get("productSignals") or []),
                    "painPoints": list(document.get("painPoints") or []),
                    "productMentions": list(document.get("productMentions") or []),
                    "powertrains": list(document.get("powertrains") or []),
                }
            )
    association_transaction_count = len(association_transactions)
    source_counter = Counter(str(document.get("siteName") or "") for document in analysis_documents if str(document.get("siteName") or ""))
    site_type_counter = Counter(str(document.get("siteType") or "") for document in analysis_documents if str(document.get("siteType") or ""))
    language_counter = Counter(str(document.get("language") or "") for document in analysis_documents if str(document.get("language") or ""))
    sentiment_counter = Counter(str(document.get("sentiment") or "neutral") for document in analysis_documents)
    ownership_counter = Counter(
        str(document.get("ownershipStage") or "unclassified")
        for document in analysis_documents
    )
    pain_point_counter: Counter[str] = Counter()
    product_signal_counter: Counter[str] = Counter()
    powertrain_counter: Counter[str] = Counter()
    decision_factor_counter: Counter[str] = Counter()
    theme_counter: Counter[str] = Counter()
    persona_counter: Counter[str] = Counter()
    product_mention_counter: Counter[str] = Counter()
    competitor_counter: Counter[str] = Counter()
    score_band_counter: Counter[str] = Counter()
    pain_point_mention_counter: Counter[str] = Counter()
    product_signal_mention_counter: Counter[str] = Counter()
    powertrain_mention_counter: Counter[str] = Counter()
    ownership_mention_counter: Counter[str] = Counter()
    product_pain_counter: Counter[tuple[str, str]] = Counter()
    persona_factor_counter: Counter[tuple[str, str]] = Counter()
    theme_source_counter: Counter[tuple[str, str]] = Counter()
    synergy_node_counter: Counter[str] = Counter()
    synergy_pair_counter: Counter[tuple[str, str]] = Counter()
    synergy_node_meta: dict[str, dict[str, str]] = {}
    pair_source_codes: dict[tuple[str, str], set[str]] = {}
    pair_site_types: dict[tuple[str, str], set[str]] = {}
    pair_month_buckets: dict[tuple[str, str], set[str]] = {}
    analysis_source_codes: set[str] = set()
    analysis_site_types: set[str] = set()
    analysis_month_buckets: set[str] = set()
    unit_source_counter: Counter[str] = Counter()
    signal_observation_count = 0
    for transaction in association_transactions:
        source_code = str(transaction.get("sourceCode") or "").strip()
        site_type = str(transaction.get("siteType") or "").strip()
        month_bucket = _month_bucket(transaction.get("publishedAt"))
        unit_source = str(transaction.get("unitSource") or "unknown").strip() or "unknown"
        unit_source_counter[unit_source] += 1
        if source_code:
            analysis_source_codes.add(source_code)
        if site_type:
            analysis_site_types.add(site_type)
        if month_bucket:
            analysis_month_buckets.add(month_bucket)
        synergy_nodes = _build_association_nodes(
            transaction,
            specs=association_specs,
        )
        synergy_node_ids = sorted({str(item["nodeId"]) for item in synergy_nodes if str(item.get("nodeId") or "")})
        for item in synergy_nodes:
            node_id = str(item.get("nodeId") or "")
            if node_id:
                synergy_node_meta[node_id] = {
                    "group": str(item.get("group") or ""),
                    "groupLabel": str(item.get("groupLabel") or ""),
                    "field": str(item.get("field") or ""),
                    "key": str(item.get("key") or ""),
                    "label": str(item.get("label") or ""),
                }
        for node_id in synergy_node_ids:
            synergy_node_counter[node_id] += 1
        for index, left_id in enumerate(synergy_node_ids):
            left_meta = synergy_node_meta.get(left_id) or {}
            left_key = str(left_meta.get("key") or "")
            for right_id in synergy_node_ids[index + 1 :]:
                right_meta = synergy_node_meta.get(right_id) or {}
                right_key = str(right_meta.get("key") or "")
                if left_key and right_key and left_key == right_key:
                    continue
                pair_key = (left_id, right_id)
                synergy_pair_counter[pair_key] += 1
                if source_code:
                    pair_source_codes.setdefault(pair_key, set()).add(source_code)
                if site_type:
                    pair_site_types.setdefault(pair_key, set()).add(site_type)
                if month_bucket:
                    pair_month_buckets.setdefault(pair_key, set()).add(month_bucket)
    for document in analysis_documents:
        source_code = str(document.get("sourceCode") or "").strip()
        site_type = str(document.get("siteType") or "").strip()
        month_bucket = _month_bucket(document.get("publishedAt") or document.get("collectedAt"))
        for pain_point in set(document.get("painPoints") or []):
            pain_point_counter[pain_point] += 1
        for product_signal in set(document.get("productSignals") or []):
            product_signal_counter[product_signal] += 1
        for powertrain in set(document.get("powertrains") or []):
            powertrain_counter[powertrain] += 1
        for factor in set(document.get("decisionFactors") or []):
            decision_factor_counter[factor] += 1
        for theme_tag in set(document.get("themeTags") or []):
            theme_counter[theme_tag] += 1
        for persona_tag in set(document.get("personaTags") or []):
            persona_counter[persona_tag] += 1
        for product_key in set(document.get("productMentions") or []):
            product_mention_counter[product_key] += 1
        for product_key in set(document.get("competitorMentions") or []):
            competitor_counter[product_key] += 1
        score_band = str((document.get("autoScores") or {}).get("scoreBand") or "").strip()
        if score_band:
            score_band_counter[score_band] += 1
        for product_key in set(document.get("productMentions") or []):
            for pain_point in set(document.get("painPoints") or []):
                product_pain_counter[(product_key, pain_point)] += 1
        for persona_tag in set(document.get("personaTags") or []):
            for factor in set(document.get("decisionFactors") or []):
                persona_factor_counter[(persona_tag, factor)] += 1
        for theme_tag in set(document.get("themeTags") or []):
            if site_type:
                theme_source_counter[(theme_tag, site_type)] += 1
        for observation in document.get("observations") or []:
            if not isinstance(observation, dict):
                continue
            signal_key = str(observation.get("signalKey") or "")
            signal_kind = str(observation.get("signalKind") or "")
            if not signal_key:
                continue
            signal_observation_count += 1
            if signal_kind == "painPoint":
                pain_point_mention_counter[signal_key] += 1
            elif signal_kind == "productSignal":
                product_signal_mention_counter[signal_key] += 1
            elif signal_kind == "powertrain":
                powertrain_mention_counter[signal_key] += 1
            elif signal_kind == "ownershipStage":
                ownership_mention_counter[signal_key] += 1

    publish_ready_count = sum(1 for document in documents if bool(document.get("publishReady")))
    quality_score_avg = round(
        sum(int(document.get("qualityScore") or 0) for document in analysis_documents) / analysis_count,
        2,
    ) if analysis_count > 0 else 0.0
    overall_score_avg = round(
        sum(int((document.get("autoScores") or {}).get("overallScore") or 0) for document in analysis_documents) / analysis_count,
        2,
    ) if analysis_count > 0 else 0.0
    publish_tier_counter = Counter(str(document.get("publishTier") or "unknown") for document in documents)
    score_band_labels = dict(_SCORE_BAND_LABELS)
    synergy_matrix = _build_association_matrix(
        synergy_pair_counter,
        node_counter=synergy_node_counter,
        node_meta=synergy_node_meta,
        pair_source_codes=pair_source_codes,
        pair_site_types=pair_site_types,
        pair_month_buckets=pair_month_buckets,
        total=max(association_transaction_count, 1),
    )
    filter_suggestions = _build_filter_suggestions(
        synergy_matrix,
        node_counter=synergy_node_counter,
        total=analysis_count,
    )

    return {
        "countryCode": country_code,
        "countryLabel": country_label,
        "taxonomyProfile": taxonomy_profile or None,
        "analysisProfile": {
            "themeTags": theme_entries,
            "personaCohorts": persona_entries,
            "productCatalog": product_entries,
            "crossAnalysisAxes": list(analysis_profile.get("crossAnalysisAxes") or []),
            "scoringDimensions": list(analysis_profile.get("scoringDimensions") or []),
            "associationMethodology": {
                "transactionUnit": "content_unit",
                "fallbackUnitStrategy": "explicit_content_units_or_derived_sentence_windows",
                "nodeFields": [
                    {
                        "group": str(spec.get("group") or ""),
                        "groupLabel": str(spec.get("groupLabel") or ""),
                        "field": str(spec.get("field") or ""),
                        "fieldLabel": str(spec.get("fieldLabel") or spec.get("field") or ""),
                    }
                    for spec in association_specs
                ],
                "pairMetrics": [
                    "count",
                    "supportPct",
                    "confidenceForwardPct",
                    "confidenceReversePct",
                    "lift",
                    "jaccard",
                    "npmi",
                    "phiCoefficient",
                    "expectedCount",
                    "fisherPValue",
                    "fdrAdjustedPValue",
                ],
                "pairSorting": ["count_desc", "npmi_desc", "lift_desc"],
                "statisticalValidation": {
                    "positiveAssociationTest": "one_tailed_fisher_exact",
                    "multipleTestingCorrection": "benjamini_hochberg_fdr",
                    "alpha": 0.05,
                },
                "replicationAxes": [
                    {"key": "sourceCode", "observedCount": len(analysis_source_codes)},
                    {"key": "siteType", "observedCount": len(analysis_site_types)},
                    {"key": "monthBucket", "observedCount": len(analysis_month_buckets)},
                ],
                "transactionCount": association_transaction_count,
                "unitSourceCounts": _build_share_items(
                    unit_source_counter,
                    total=max(association_transaction_count, 1),
                    limit=8,
                ),
            },
        },
        "generatedAt": datetime.now(UTC).isoformat(),
        "sourceCount": len(source_runs),
        "documentCount": len(documents),
        "publishReadyDocumentCount": publish_ready_count,
        "errorCount": total_error_count,
        "analysisDocumentCount": analysis_count,
        "analysisUnitCount": association_transaction_count,
        "signalObservationCount": signal_observation_count,
        "qualityScoreAvg": quality_score_avg,
        "overallScoreAvg": overall_score_avg,
        "sourceRuns": source_runs,
        "documents": documents,
        "aggregates": {
            "sourceSites": _build_share_items(source_counter, total=analysis_count, limit=8),
            "siteTypes": _build_share_items(site_type_counter, total=analysis_count, limit=8),
            "languages": _build_share_items(language_counter, total=analysis_count, limit=8),
            "publishTiers": _build_share_items(publish_tier_counter, total=max(len(documents), 1), limit=4),
            "sentiment": _build_share_items(sentiment_counter, total=analysis_count, limit=4),
            "ownershipStages": _build_share_items(
                ownership_counter,
                total=analysis_count,
                limit=6,
                labels=_OWNERSHIP_STAGE_LABELS,
                mention_counter=ownership_mention_counter,
            ),
            "painPoints": _build_share_items(
                pain_point_counter,
                total=analysis_count,
                limit=8,
                labels=_PAIN_POINT_LABELS,
                mention_counter=pain_point_mention_counter,
            ),
            "productSignals": _build_share_items(
                product_signal_counter,
                total=analysis_count,
                limit=8,
                labels=_PRODUCT_SIGNAL_LABELS,
                mention_counter=product_signal_mention_counter,
            ),
            "powertrains": _build_share_items(
                powertrain_counter,
                total=analysis_count,
                limit=4,
                mention_counter=powertrain_mention_counter,
            ),
            "decisionFactors": _build_share_items(
                decision_factor_counter,
                total=analysis_count,
                limit=5,
                labels=_DECISION_FACTOR_LABELS,
            ),
            "themeTags": _build_share_items(
                theme_counter,
                total=analysis_count,
                limit=8,
                labels=theme_labels,
            ),
            "personaCohorts": _build_share_items(
                persona_counter,
                total=analysis_count,
                limit=8,
                labels=persona_labels,
            ),
            "matchedProducts": _build_share_items(
                product_mention_counter,
                total=analysis_count,
                limit=10,
                labels=product_labels,
            ),
            "competitorMentions": _build_share_items(
                competitor_counter,
                total=analysis_count,
                limit=10,
                labels=product_labels,
            ),
            "scoreBands": _build_share_items(
                score_band_counter,
                total=analysis_count,
                limit=4,
                labels=score_band_labels,
            ),
            "evidenceCards": _build_evidence_cards(analysis_documents),
            "crossAnalysis": {
                "productPainPoints": _build_pair_items(
                    product_pain_counter,
                    left_labels=product_labels,
                    right_labels=_PAIN_POINT_LABELS,
                    left_key_name="productKey",
                    left_label_name="productLabel",
                    right_key_name="painPointKey",
                    right_label_name="painPointLabel",
                ),
                "personaDecisionFactors": _build_pair_items(
                    persona_factor_counter,
                    left_labels=persona_labels,
                    right_labels=_DECISION_FACTOR_LABELS,
                    left_key_name="personaKey",
                    left_label_name="personaLabel",
                    right_key_name="decisionFactorKey",
                    right_label_name="decisionFactorLabel",
                ),
                "themeBySourceType": _build_pair_items(
                    theme_source_counter,
                    left_labels=theme_labels,
                    right_labels={key: key for key in site_type_counter},
                    left_key_name="themeKey",
                    left_label_name="themeLabel",
                    right_key_name="sourceTypeKey",
                    right_label_name="sourceTypeLabel",
                ),
            },
            "associationGraph": synergy_matrix,
            "synergyMatrix": synergy_matrix,
            "associationRecommendations": filter_suggestions,
            "filterSuggestions": filter_suggestions,
            "personaSummaries": _build_persona_cohort_summaries(
                analysis_documents,
                persona_labels=persona_labels,
                pain_point_labels=_PAIN_POINT_LABELS,
                decision_factor_labels=_DECISION_FACTOR_LABELS,
                product_labels=product_labels,
                total=analysis_count,
            ),
        },
    }


def build_country_voc_deck(country_enrichment: dict[str, Any]) -> dict[str, Any]:
    aggregates = country_enrichment.get("aggregates") or {}
    analysis_profile = country_enrichment.get("analysisProfile") if isinstance(country_enrichment.get("analysisProfile"), dict) else {}
    association_methodology = dict(analysis_profile.get("associationMethodology") or {})
    country_label = str(country_enrichment.get("countryLabel") or country_enrichment.get("countryCode") or "VOC")
    source_sites = list(aggregates.get("sourceSites") or [])
    pain_points = list(aggregates.get("painPoints") or [])
    product_signals = list(aggregates.get("productSignals") or [])
    decision_factors = list(aggregates.get("decisionFactors") or [])
    theme_tags = list(aggregates.get("themeTags") or [])
    persona_cohorts = list(aggregates.get("personaCohorts") or [])
    matched_products = list(aggregates.get("matchedProducts") or [])
    competitor_mentions = list(aggregates.get("competitorMentions") or [])
    score_bands = list(aggregates.get("scoreBands") or [])
    cross_analysis = dict(aggregates.get("crossAnalysis") or {})
    association_graph = list(aggregates.get("associationGraph") or aggregates.get("synergyMatrix") or [])
    association_recommendations = list(aggregates.get("associationRecommendations") or aggregates.get("filterSuggestions") or [])
    synergy_matrix = list(aggregates.get("synergyMatrix") or association_graph)
    filter_suggestions = list(aggregates.get("filterSuggestions") or association_recommendations)
    persona_summaries = list(aggregates.get("personaSummaries") or [])
    top_source = source_sites[0]["label"] if source_sites else "No source yet"
    top_pain_point = pain_points[0]["label"] if pain_points else "No dominant pain point yet"
    top_signal = product_signals[0]["label"] if product_signals else "No dominant product signal yet"
    top_factor = decision_factors[0]["label"] if decision_factors else "No dominant decision factor yet"
    top_product = matched_products[0]["label"] if matched_products else "No consistent product match yet"
    top_synergy = synergy_matrix[0] if synergy_matrix else None
    top_synergy_label = (
        f'{top_synergy["leftLabel"]} + {top_synergy["rightLabel"]}'
        if top_synergy
        else "No stable co-consideration yet"
    )
    publish_ready_count = int(country_enrichment.get("publishReadyDocumentCount") or 0)
    document_count = int(country_enrichment.get("documentCount") or 0)
    analysis_unit_count = int(country_enrichment.get("analysisUnitCount") or 0)
    source_count = int(country_enrichment.get("sourceCount") or 0)
    signal_observation_count = int(country_enrichment.get("signalObservationCount") or 0)
    quality_score_avg = float(country_enrichment.get("qualityScoreAvg") or 0.0)
    overall_score_avg = float(country_enrichment.get("overallScoreAvg") or 0.0)

    return {
        "sourceMode": "forum_voc",
        "countryCode": country_enrichment.get("countryCode"),
        "countryLabel": country_enrichment.get("countryLabel"),
        "generatedAt": country_enrichment.get("generatedAt"),
        "title": f"{country_label} forum VOC deck",
        "subtitle": "Observed signals from public forums, owner communities, and comment pages.",
        "methodologyNote": (
            "Observed sections come from publish-ready public VOC documents. "
            "Demographic, age, household, and commute structures remain inferred-only and are intentionally excluded "
            "from sample-fact style reporting. Association graph edges are computed from document-level co-occurrence "
            "using support, directional confidence, lift, Jaccard, NPMI, and phi coefficient."
        ),
        "associationMethodology": association_methodology,
        "metrics": [
            {
                "label": "Sources",
                "value": source_count,
                "detail": "Public source files included in this country artifact.",
            },
            {
                "label": "Documents",
                "value": document_count,
                "detail": "Raw documents captured from those public sources.",
            },
            {
                "label": "Publish-ready docs",
                "value": publish_ready_count,
                "detail": "Documents that passed the current auto-review gate.",
            },
            {
                "label": "Analysis units",
                "value": analysis_unit_count,
                "detail": "Content-unit transactions used by the association graph, preferring explicit comments/replies and falling back to derived sentence windows.",
            },
            {
                "label": "Signal observations",
                "value": signal_observation_count,
                "detail": "Sentence-level signal hits preserved as evidence-backed observations.",
            },
            {
                "label": "Avg quality score",
                "value": quality_score_avg,
                "detail": "Average document-level auto-review score on analyzed documents.",
            },
            {
                "label": "Avg overall score",
                "value": overall_score_avg,
                "detail": "Heuristic composite score across relevance, persona richness, and match confidence.",
            },
        ],
        "conclusionCards": [
            {
                "label": "Source mix",
                "value": top_source,
                "detail": "Largest source contributor among analyzed documents.",
            },
            {
                "label": "Top pain point",
                "value": top_pain_point,
                "detail": "Most frequently observed issue across analyzed documents.",
            },
            {
                "label": "Top product signal",
                "value": top_signal,
                "detail": "Most repeated product dimension in observed discussion.",
            },
            {
                "label": "Lead decision factor",
                "value": top_factor,
                "detail": "Highest-level reason cluster aggregated from pain points and product signals.",
            },
            {
                "label": "Top matched product",
                "value": top_product,
                "detail": "Most frequently matched vehicle/product entity across analyzed forum evidence.",
            },
            {
                "label": "Top co-consideration",
                "value": top_synergy_label,
                "detail": "Most repeated attribute affinity observed from the same publish-ready VOC documents.",
            },
        ],
        "observedSections": [
            "source_mix",
            "site_type_mix",
            "language_mix",
            "sentiment",
            "ownership_stages",
            "pain_points",
            "product_signals",
            "powertrains",
            "decision_factors",
            "theme_tags",
            "persona_cohorts",
            "matched_products",
            "competitor_mentions",
            "score_bands",
            "cross_analysis",
            "synergy_matrix",
            "filter_suggestions",
            "evidence_cards",
        ],
        "inferredSections": [
            "demographics",
            "age_distribution",
            "household_structure",
            "weekly_commute",
        ],
        "sourceMix": source_sites,
        "siteTypes": list(aggregates.get("siteTypes") or []),
        "languages": list(aggregates.get("languages") or []),
        "publishTiers": list(aggregates.get("publishTiers") or []),
        "sentiment": list(aggregates.get("sentiment") or []),
        "ownershipStages": list(aggregates.get("ownershipStages") or []),
        "painPoints": pain_points,
        "productSignals": product_signals,
        "powertrains": list(aggregates.get("powertrains") or []),
        "decisionFactors": decision_factors,
        "themeTags": theme_tags,
        "personaCohorts": persona_cohorts,
        "matchedProducts": matched_products,
        "competitorMentions": competitor_mentions,
        "scoreBands": score_bands,
        "personaSummaries": persona_summaries,
        "crossAnalysis": cross_analysis,
        "associationGraph": association_graph,
        "synergyMatrix": synergy_matrix,
        "associationRecommendations": association_recommendations,
        "filterSuggestions": filter_suggestions,
        "evidenceCards": list(aggregates.get("evidenceCards") or []),
        "notes": [
            "This artifact is designed as a forum-VOC deck, not a demographic survey deck.",
            "Benchmark Excel samples can still be used separately for curated profile comparisons.",
            "Multi-label themes, persona cohorts, heuristic scores, and entity matching are heuristic/config-driven and should be used as an observed evidence layer, not as deterministic truth.",
            "Synergy matrix and filter suggestions are based on tag co-occurrence in the same publish-ready VOC documents. They are useful for guided filtering, not proof of causality.",
        ],
    }


def build_voc_enriched_collection(
    *,
    output_root: str | Path = "04_Processed_data/voc",
    country_filter: set[str] | None = None,
) -> dict[str, Any]:
    root = _resolve_repo_path(output_root)
    countries_payload: list[dict[str, Any]] = []
    total_sources = 0
    total_documents = 0
    total_publish_ready_documents = 0

    if not root.exists():
        return {
            "root": str(root),
            "country_count": 0,
            "source_count": 0,
            "document_count": 0,
            "publish_ready_document_count": 0,
            "countries": [],
        }

    for country_root in sorted(path for path in root.iterdir() if path.is_dir()):
        country_code = country_root.name.upper()
        if country_filter and country_code not in country_filter:
            continue
        enrichment = build_country_voc_enrichment(country_root)
        deck = build_country_voc_deck(enrichment)
        enriched_root = country_root / "enriched"
        deck_root = country_root / "deck"
        enriched_root.mkdir(parents=True, exist_ok=True)
        deck_root.mkdir(parents=True, exist_ok=True)
        enriched_path = enriched_root / "customer_insight_signals.json"
        deck_path = deck_root / "customer_insight_deck.json"
        enriched_path.write_text(json.dumps(enrichment, ensure_ascii=False, indent=2), encoding="utf-8")
        deck_path.write_text(json.dumps(deck, ensure_ascii=False, indent=2), encoding="utf-8")
        total_sources += int(enrichment.get("sourceCount") or 0)
        total_documents += int(enrichment.get("documentCount") or 0)
        total_publish_ready_documents += int(enrichment.get("publishReadyDocumentCount") or 0)
        countries_payload.append(
            {
                "country_code": str(enrichment.get("countryCode") or country_code),
                "country_label": str(enrichment.get("countryLabel") or country_code),
                "source_count": int(enrichment.get("sourceCount") or 0),
                "document_count": int(enrichment.get("documentCount") or 0),
                "publish_ready_document_count": int(
                    enrichment.get("publishReadyDocumentCount") or 0,
                ),
                "enriched_output_path": str(enriched_path),
                "deck_output_path": str(deck_path),
            }
        )

    return {
        "root": str(root),
        "country_count": len(countries_payload),
        "source_count": total_sources,
        "document_count": total_documents,
        "publish_ready_document_count": total_publish_ready_documents,
        "countries": countries_payload,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build VOC enriched signals and country-level deck artifacts from raw captures.",
    )
    parser.add_argument(
        "--countries",
        nargs="*",
        help="Optional list of country codes to keep.",
    )
    parser.add_argument(
        "--output-root",
        default="04_Processed_data/voc",
        help="Root path containing country raw folders and receiving enriched/deck outputs.",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Optional summary JSON output path.",
    )
    args = parser.parse_args(argv)

    payload = build_voc_enriched_collection(
        output_root=args.output_root,
        country_filter=_normalize_country_filter(args.countries),
    )
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        output_path = _resolve_repo_path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
