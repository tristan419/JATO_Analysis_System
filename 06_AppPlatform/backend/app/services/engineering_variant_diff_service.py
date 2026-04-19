from __future__ import annotations

import json
import re
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import (
    ConfigBaseVariant,
    ConfigMarketFeatureOverride,
    ConfigMarketVariant,
    ConfigProject,
)
from app.db.session import get_session_factory
from app.infra import engineering_repository as repo
from app.services import country_profiles

_CORE_FEATURE_CODES = {
    "body_style",
    "drive_type",
    "battery_kwh",
    "range_km",
}


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _normalize_upper(value: object | None) -> str:
    return _normalize_text(value).upper()


def _normalize_search_text(value: object | None) -> str:
    return re.sub(r"\s+", " ", _normalize_text(value).casefold())


def _normalize_powertrain(value: str | None) -> str:
    return _normalize_upper(value)


def _normalize_compare_subjects(
    *,
    compare_subjects: list[dict[str, Any]] | None = None,
    model: str | None = None,
    models: list[str] | None = None,
    max_subjects: int = 3,
) -> list[dict[str, str | None]]:
    normalized: list[dict[str, str | None]] = []
    seen: set[tuple[str, str]] = set()
    raw_subjects = list(compare_subjects or [])
    if not raw_subjects:
        raw_subjects = [
            {"model": item, "variantQuery": None}
            for item in [model, *(models or [])]
            if _normalize_upper(item)
        ]

    for raw in raw_subjects:
        model_name = _normalize_upper(raw.get("model"))
        variant_query = _normalize_text(raw.get("variantQuery")) or None
        if not model_name:
            continue
        dedupe_key = (model_name, _normalize_search_text(variant_query))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append(
            {
                "model": model_name,
                "variantQuery": variant_query,
            }
        )
        if len(normalized) >= max(2, int(max_subjects)):
            break
    return normalized


def _empty_compare_payload(
    *,
    country: str,
    model: str | None = None,
    models: list[str] | None = None,
    compare_subjects: list[dict[str, Any]] | None = None,
    max_subjects: int = 3,
) -> dict[str, Any]:
    normalized_subjects = _normalize_compare_subjects(
        compare_subjects=compare_subjects,
        model=model,
        models=models,
        max_subjects=max_subjects,
    )
    return {
        "country": country,
        "queryModels": [str(item["model"]) for item in normalized_subjects],
        "subjects": [],
        "differentFeatures": [],
        "commonFeatures": [],
        "selectionNotes": [],
        "latestUpdatedAt": None,
    }


def _country_aliases(country: str) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    def add(value: object | None) -> None:
        text = _normalize_text(value)
        if not text:
            return
        key = text.casefold()
        if key in seen:
            return
        seen.add(key)
        ordered.append(text)

    add(country)
    profile = country_profiles.get_country_profile(country)
    market_label = _normalize_text(profile.get("market_label")) if profile else ""
    for part in re.split(r"/", market_label):
        add(part)
    return ordered


def _list_projects_for_country(session: Session, country: str) -> list[ConfigProject]:
    projects: list[ConfigProject] = []
    seen: set[str] = set()
    for alias in _country_aliases(country):
        for item in repo.list_projects(session, "active", None, alias, 200):
            project_id = str(getattr(item, "project_id", ""))
            if not project_id or project_id in seen:
                continue
            seen.add(project_id)
            projects.append(item)
    return projects


def _project_match_score(
    project: ConfigProject,
    *,
    query_model: str,
    brand: str | None = None,
) -> int:
    project_model = _normalize_upper(project.model)
    query_model = _normalize_upper(query_model)
    if not project_model or not query_model:
        return 0
    if project_model == query_model:
        score = 100
    elif query_model in project_model or project_model in query_model:
        score = 60
    else:
        return 0

    brand_query = _normalize_upper(brand)
    if brand_query:
        project_brand = _normalize_upper(project.brand)
        if project_brand == brand_query:
            score += 20
        elif brand_query not in project_brand:
            score -= 10
    return score


def _feature_value_from_override(item: ConfigMarketFeatureOverride) -> object | None:
    if item.value_type == "bool":
        return item.bool_value
    if item.value_type == "number":
        return float(item.number_value) if item.number_value is not None else None
    if item.value_type == "text":
        return item.text_value
    return item.json_value


def _json_safe_value(value: object | None) -> object | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, list):
        items = [
            safe_item
            for item in value
            if (safe_item := _json_safe_value(item)) is not None
        ]
        return items or None
    if isinstance(value, dict):
        payload = {
            str(key): safe_value
            for key, raw_value in value.items()
            if (safe_value := _json_safe_value(raw_value)) is not None
        }
        return payload or None
    return str(value)


def _canonical_value(value: object | None) -> str:
    safe_value = _json_safe_value(value)
    if isinstance(safe_value, float):
        safe_value = round(safe_value, 6)
    return json.dumps(
        safe_value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _format_feature_value(value: object | None) -> str:
    safe_value = _json_safe_value(value)
    if safe_value is None:
        return "-"
    if isinstance(safe_value, bool):
        return "Yes" if safe_value else "No"
    if isinstance(safe_value, float):
        if safe_value.is_integer():
            return f"{int(safe_value)}"
        return f"{safe_value:.1f}".rstrip("0").rstrip(".")
    if isinstance(safe_value, (int,)):
        return str(safe_value)
    if isinstance(safe_value, list):
        return ", ".join(str(item) for item in safe_value)
    if isinstance(safe_value, dict):
        return json.dumps(safe_value, ensure_ascii=False, sort_keys=True)
    return str(safe_value)


def _variant_query_score(
    *,
    base_variant: ConfigBaseVariant,
    market_variant: ConfigMarketVariant,
    variant_query: str | None,
) -> tuple[int, bool]:
    normalized_query = _normalize_search_text(variant_query)
    if not normalized_query:
        return 0, False
    candidates = [
        _normalize_search_text(base_variant.trim_name),
        _normalize_search_text(base_variant.version_name),
        _normalize_search_text(market_variant.external_row_key),
    ]
    haystack = " ".join(item for item in candidates if item)
    if not haystack:
        return -100, False
    if normalized_query == haystack or normalized_query in haystack:
        return 120, True
    query_tokens = [token for token in normalized_query.split(" ") if token]
    if not query_tokens:
        return -100, False
    matched_tokens = sum(1 for token in query_tokens if token in haystack)
    if matched_tokens == len(query_tokens):
        return 90 + matched_tokens, True
    if matched_tokens > 0:
        return 40 + matched_tokens, False
    return -100, False


def _build_effective_feature_map(
    base_variant: ConfigBaseVariant,
    overrides: list[ConfigMarketFeatureOverride],
) -> dict[str, dict[str, Any]]:
    feature_map: dict[str, dict[str, Any]] = {}
    for feature_code, raw_value in sorted((base_variant.base_features_json or {}).items()):
        feature_map[str(feature_code)] = {
            "featureCode": str(feature_code),
            "featureLabel": str(
                (base_variant.base_feature_labels_json or {}).get(
                    str(feature_code),
                    str(feature_code),
                )
            ),
            "value": _json_safe_value(raw_value),
            "availability": None,
            "packageCode": None,
            "source": "base",
        }

    for item in overrides:
        feature_code = _normalize_text(item.feature_code)
        if not feature_code:
            continue
        feature_map[feature_code] = {
            "featureCode": feature_code,
            "featureLabel": _normalize_text(item.feature_label) or feature_code,
            "value": _json_safe_value(_feature_value_from_override(item)),
            "availability": _normalize_text(item.availability) or None,
            "packageCode": _normalize_text(item.package_code) or None,
            "source": "override",
        }
    return feature_map


def _build_subject_label(
    *,
    model: str,
    trim: str,
    version: str,
) -> str:
    return " ".join(part for part in [model, trim, version] if _normalize_text(part)).strip()


def _select_subject_variant(
    session: Session,
    *,
    country: str,
    brand: str | None,
    subject: dict[str, str | None],
    powertrain: str | None,
    project_cache: dict[str, ConfigProject | None],
) -> dict[str, Any] | None:
    query_model = _normalize_upper(subject.get("model"))
    if not query_model:
        return None

    project = project_cache.get(query_model)
    if query_model not in project_cache:
        candidate_projects = _list_projects_for_country(session, country)
        ranked_projects = sorted(
            candidate_projects,
            key=lambda item: _project_match_score(
                item,
                query_model=query_model,
                brand=brand,
            ),
            reverse=True,
        )
        project = ranked_projects[0] if ranked_projects else None
        if project and _project_match_score(project, query_model=query_model, brand=brand) <= 0:
            project = None
        project_cache[query_model] = project

    if project is None:
        return None

    base_variants = repo.list_base_variants(session, project.project_id, None, 500)
    market_variants = repo.list_market_variants(
        session,
        project.project_id,
        None,
        project.market_country,
        1000,
    )
    overrides = repo.list_market_feature_overrides(
        session,
        project.project_id,
        None,
        None,
        project.market_country,
        None,
        2000,
    )

    overrides_by_market: dict[str, list[ConfigMarketFeatureOverride]] = {}
    for item in overrides:
        overrides_by_market.setdefault(str(item.market_variant_id), []).append(item)

    market_by_base: dict[str, list[ConfigMarketVariant]] = {}
    for item in market_variants:
        market_by_base.setdefault(str(item.base_variant_id), []).append(item)

    normalized_powertrain = _normalize_powertrain(powertrain)
    variant_query = _normalize_text(subject.get("variantQuery")) or None
    candidates: list[dict[str, Any]] = []
    for base_variant in base_variants:
        if _normalize_upper(base_variant.model) != query_model:
            continue
        if normalized_powertrain and _normalize_powertrain(base_variant.powertrain) != normalized_powertrain:
            continue
        related_markets = market_by_base.get(str(base_variant.base_variant_id), [])
        if not related_markets:
            continue
        for market_variant in related_markets:
            variant_score, explicit_match = _variant_query_score(
                base_variant=base_variant,
                market_variant=market_variant,
                variant_query=variant_query,
            )
            if variant_query and variant_score <= 0:
                continue
            feature_map = _build_effective_feature_map(
                base_variant,
                overrides_by_market.get(str(market_variant.market_variant_id), []),
            )
            target_msrp = (
                float(market_variant.target_msrp)
                if market_variant.target_msrp is not None
                else None
            )
            selection_score = variant_score
            if target_msrp is not None:
                selection_score += 10
            if normalized_powertrain and _normalize_powertrain(base_variant.powertrain) == normalized_powertrain:
                selection_score += 20
            candidates.append(
                {
                    "project": project,
                    "baseVariant": base_variant,
                    "marketVariant": market_variant,
                    "featureMap": feature_map,
                    "selectionScore": selection_score,
                    "explicitMatch": explicit_match,
                    "targetMsrp": target_msrp,
                }
            )

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: (
            -int(item["selectionScore"]),
            item["targetMsrp"] is None,
            float(item["targetMsrp"] or 0.0),
            _normalize_search_text(item["baseVariant"].trim_name),
            _normalize_search_text(item["baseVariant"].version_name),
        )
    )
    selected = candidates[0]
    base_variant = selected["baseVariant"]
    market_variant = selected["marketVariant"]
    selection_mode = "explicit-match"
    selection_note = None
    if not variant_query:
        selection_mode = (
            "entry-variant" if selected["targetMsrp"] is not None else "first-available"
        )
        selection_note = (
            f"{query_model} 未指定具体版本，默认取 {project.market_country} 当前目标 MSRP 最低的命中版本。"
            if selection_mode == "entry-variant"
            else f"{query_model} 未指定具体版本，默认取 {project.market_country} 当前首个可用命中版本。"
        )
    elif not selected["explicitMatch"]:
        selection_mode = "closest-match"
        selection_note = (
            f"{query_model} 未命中完全一致的版本名，已按最接近的 trim/version 文本选择当前配置。"
        )

    updated_candidates = [
        getattr(project, "updated_at_utc", None),
        getattr(base_variant, "updated_at_utc", None),
        getattr(market_variant, "updated_at_utc", None),
        *[
            getattr(item, "updated_at_utc", None)
            for item in overrides_by_market.get(str(market_variant.market_variant_id), [])
        ],
    ]
    latest_updated = max(
        (item.isoformat() for item in updated_candidates if item is not None),
        default=None,
    )
    return {
        "queryModel": query_model,
        "variantQuery": variant_query,
        "selectionMode": selection_mode,
        "selectionNote": selection_note,
        "projectId": str(project.project_id),
        "projectDisplayName": _normalize_text(project.display_name),
        "marketCountry": _normalize_text(project.market_country),
        "brand": _normalize_text(base_variant.brand),
        "model": _normalize_text(base_variant.model),
        "trim": _normalize_text(base_variant.trim_name),
        "version": _normalize_text(base_variant.version_name),
        "powertrain": _normalize_text(base_variant.powertrain),
        "targetMsrp": selected["targetMsrp"],
        "subjectLabel": _build_subject_label(
            model=_normalize_text(base_variant.model),
            trim=_normalize_text(base_variant.trim_name),
            version=_normalize_text(base_variant.version_name),
        ),
        "featureCount": len(selected["featureMap"]),
        "latestUpdatedAt": latest_updated,
        "_featureMap": selected["featureMap"],
    }


def _feature_priority(item: dict[str, Any]) -> tuple[int, str]:
    code = _normalize_text(item.get("featureCode"))
    label = _normalize_search_text(item.get("featureLabel"))
    return (0 if code in _CORE_FEATURE_CODES else 1, label)


def compare_market_variants(
    session: Session,
    *,
    country: str,
    brand: str | None = None,
    model: str | None = None,
    models: list[str] | None = None,
    powertrain: str | None = None,
    compare_subjects: list[dict[str, Any]] | None = None,
    max_subjects: int = 3,
    max_diff_features: int = 16,
    max_common_features: int = 8,
) -> dict[str, Any]:
    normalized_subjects = _normalize_compare_subjects(
        compare_subjects=compare_subjects,
        model=model,
        models=models,
        max_subjects=max_subjects,
    )
    empty_payload = _empty_compare_payload(
        country=country,
        model=model,
        models=models,
        compare_subjects=compare_subjects,
        max_subjects=max_subjects,
    )
    if len(normalized_subjects) < 2:
        return empty_payload

    project_cache: dict[str, ConfigProject | None] = {}
    resolved_subjects = [
        _select_subject_variant(
            session,
            country=country,
            brand=brand,
            subject=subject,
            powertrain=powertrain,
            project_cache=project_cache,
        )
        for subject in normalized_subjects
    ]
    selected_subjects = [item for item in resolved_subjects if item is not None]
    if len(selected_subjects) < 2:
        return {
            **empty_payload,
            "subjects": [
                {
                    key: value
                    for key, value in item.items()
                    if not key.startswith("_")
                }
                for item in selected_subjects
            ],
        }

    feature_codes = sorted(
        {
            feature_code
            for subject in selected_subjects
            for feature_code in subject["_featureMap"].keys()
        }
    )

    different_features: list[dict[str, Any]] = []
    common_features: list[dict[str, Any]] = []
    for feature_code in feature_codes:
        entries = [subject["_featureMap"].get(feature_code) for subject in selected_subjects]
        feature_label = next(
            (
                _normalize_text(entry.get("featureLabel"))
                for entry in entries
                if isinstance(entry, dict) and _normalize_text(entry.get("featureLabel"))
            ),
            feature_code,
        )
        raw_values = [
            entry.get("value") if isinstance(entry, dict) else None
            for entry in entries
        ]
        formatted_values = [_format_feature_value(value) for value in raw_values]
        canonical_values = [_canonical_value(value) for value in raw_values]
        payload = {
            "featureCode": feature_code,
            "featureLabel": feature_label,
            "values": formatted_values,
        }
        if len(set(canonical_values)) == 1:
            if formatted_values[0] != "-":
                common_features.append(
                    {
                        **payload,
                        "value": formatted_values[0],
                    }
                )
            continue
        different_features.append(payload)

    different_features.sort(key=_feature_priority)
    common_features.sort(key=_feature_priority)

    latest_updated_at = max(
        (
            str(subject.get("latestUpdatedAt") or "").strip()
            for subject in selected_subjects
            if str(subject.get("latestUpdatedAt") or "").strip()
        ),
        default=None,
    )
    selection_notes = [
        str(subject.get("selectionNote") or "").strip()
        for subject in selected_subjects
        if str(subject.get("selectionNote") or "").strip()
    ]
    return {
        "country": country,
        "queryModels": [str(item["model"]) for item in normalized_subjects],
        "subjects": [
            {
                key: value
                for key, value in subject.items()
                if not key.startswith("_")
            }
            for subject in selected_subjects
        ],
        "differentFeatures": different_features[: max(1, int(max_diff_features))],
        "commonFeatures": common_features[: max(0, int(max_common_features))],
        "selectionNotes": selection_notes,
        "latestUpdatedAt": latest_updated_at,
    }


def compare_market_variants_from_db(
    *,
    country: str,
    brand: str | None = None,
    model: str | None = None,
    models: list[str] | None = None,
    powertrain: str | None = None,
    compare_subjects: list[dict[str, Any]] | None = None,
    max_subjects: int = 3,
    max_diff_features: int = 16,
    max_common_features: int = 8,
) -> dict[str, Any]:
    empty_payload = _empty_compare_payload(
        country=country,
        model=model,
        models=models,
        compare_subjects=compare_subjects,
        max_subjects=max_subjects,
    )

    try:
        session_factory = get_session_factory()
    except Exception:  # noqa: BLE001
        return empty_payload

    try:
        with session_factory() as session:
            return compare_market_variants(
                session,
                country=country,
                brand=brand,
                model=model,
                models=models,
                powertrain=powertrain,
                compare_subjects=compare_subjects,
                max_subjects=max_subjects,
                max_diff_features=max_diff_features,
                max_common_features=max_common_features,
            )
    except Exception:  # noqa: BLE001
        return empty_payload
