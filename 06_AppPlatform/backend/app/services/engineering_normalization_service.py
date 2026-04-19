from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import (
    ConfigBaseVariant,
    ConfigMarketFeatureOverride,
    ConfigMarketVariant,
    ConfigProject,
    ConfigVariant,
)
from app.infra import engineering_repository as repo


@dataclass(frozen=True)
class _FeatureEntry:
    code: str
    label: str
    value: object


def _normalize_identity_token(value: object) -> str:
    text = str(value or "").strip().casefold()
    return re.sub(r"\s+", " ", text)


def _normalize_feature_code(label: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "_", label.strip().casefold()).strip("_")
    return token or "feature"


def _json_safe_feature_value(value: object) -> object | None:
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
            _json_safe_feature_value(item)
            for item in value
            if _json_safe_feature_value(item) is not None
        ]
        return items or None
    if isinstance(value, dict):
        payload = {
            str(key): safe_value
            for key, raw_value in value.items()
            if (safe_value := _json_safe_feature_value(raw_value)) is not None
        }
        return payload or None
    return str(value)


def _canonical_feature_value(value: object) -> str:
    safe_value = _json_safe_feature_value(value)
    if isinstance(safe_value, float):
        safe_value = round(safe_value, 6)
    return json.dumps(
        safe_value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _resolve_value_type(value: object) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "text"
    return "json"


def _infer_availability(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    token = value.strip().casefold()
    if token in {"std", "standard", "included", "series"}:
        return "standard"
    if token in {"optional", "option"}:
        return "optional"
    if token in {"package", "pack"}:
        return "package"
    if token in {"not available", "unavailable", "n/a", "na", "no"}:
        return "not_available"
    return None


def _resolve_package_code(label: str, value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if "pack" not in label.casefold():
        return None
    text = value.strip()
    return text or None


def _feature_storage_columns(
    value: object,
) -> tuple[str, bool | None, float | None, str | None, dict | list | None]:
    safe_value = _json_safe_feature_value(value)
    if isinstance(safe_value, bool):
        return "bool", safe_value, None, None, None
    if isinstance(safe_value, (int, float)):
        return "number", None, float(safe_value), None, None
    if isinstance(safe_value, str):
        return "text", None, None, safe_value, None
    return "json", None, None, None, safe_value


def _feature_payload(
    feature_code: str,
    feature_label: str,
    value: object,
) -> dict[str, object]:
    return {
        "featureCode": feature_code,
        "featureLabel": feature_label,
        "value": _json_safe_feature_value(value),
        "valueType": _resolve_value_type(_json_safe_feature_value(value)),
    }


def _serialize_base_features(
    values: dict[str, object] | None,
    labels: dict[str, str] | None,
) -> list[dict[str, object]]:
    if not values:
        return []
    resolved_labels = labels or {}
    return [
        _feature_payload(
            code,
            resolved_labels.get(code, code),
            value,
        )
        for code, value in sorted(values.items())
    ]


def _build_variant_business_key(variant: ConfigVariant) -> str:
    return "|".join(
        (
            _normalize_identity_token(variant.brand),
            _normalize_identity_token(variant.model),
            _normalize_identity_token(variant.trim_name),
            _normalize_identity_token(variant.version_name),
            _normalize_identity_token(variant.powertrain),
        )
    )


def _add_feature(
    feature_map: dict[str, _FeatureEntry],
    code: str,
    label: str,
    raw_value: object,
) -> None:
    safe_value = _json_safe_feature_value(raw_value)
    if safe_value is None:
        return
    resolved_code = _normalize_feature_code(code)
    if resolved_code in feature_map:
        existing = feature_map[resolved_code]
        if _canonical_feature_value(existing.value) == _canonical_feature_value(
            safe_value
        ):
            return
        suffix = 2
        while f"{resolved_code}_{suffix}" in feature_map:
            suffix += 1
        resolved_code = f"{resolved_code}_{suffix}"
    feature_map[resolved_code] = _FeatureEntry(
        code=resolved_code,
        label=label,
        value=safe_value,
    )


def _extract_variant_features(
    variant: ConfigVariant,
) -> dict[str, _FeatureEntry]:
    feature_map: dict[str, _FeatureEntry] = {}
    _add_feature(feature_map, "body_style", "Body Style", variant.body_style)
    _add_feature(feature_map, "drive_type", "Drive Type", variant.drive_type)
    _add_feature(feature_map, "battery_kwh", "Battery kWh", variant.battery_kwh)
    _add_feature(feature_map, "range_km", "Range Km", variant.range_km)
    for raw_label, raw_value in sorted((variant.attributes_json or {}).items()):
        label = str(raw_label).strip() or "Attribute"
        _add_feature(feature_map, label, label, raw_value)
    return feature_map


def _derive_base_features(
    feature_maps: list[dict[str, _FeatureEntry]],
) -> tuple[dict[str, object], dict[str, str]]:
    base_values: dict[str, object] = {}
    base_labels: dict[str, str] = {}
    all_feature_codes = sorted(
        {
            feature_code
            for feature_map in feature_maps
            for feature_code in feature_map.keys()
        }
    )
    for feature_code in all_feature_codes:
        entries = [feature_map.get(feature_code) for feature_map in feature_maps]
        if any(entry is None for entry in entries):
            continue
        first_entry = entries[0]
        assert first_entry is not None
        first_value = _canonical_feature_value(first_entry.value)
        if any(
            _canonical_feature_value(entry.value) != first_value
            for entry in entries[1:]
            if entry is not None
        ):
            continue
        base_values[feature_code] = first_entry.value
        base_labels[feature_code] = first_entry.label
    return base_values, base_labels


def _build_normalized_rows(
    project: ConfigProject,
    active_variants: list[ConfigVariant],
) -> tuple[
    list[ConfigBaseVariant],
    list[ConfigMarketVariant],
    list[ConfigMarketFeatureOverride],
]:
    grouped_variants: dict[str, list[ConfigVariant]] = {}
    for variant in active_variants:
        grouped_variants.setdefault(
            _build_variant_business_key(variant),
            [],
        ).append(variant)

    base_variants: list[ConfigBaseVariant] = []
    market_variants: list[ConfigMarketVariant] = []
    feature_overrides: list[ConfigMarketFeatureOverride] = []

    for business_key, variants in sorted(grouped_variants.items()):
        ordered_variants = sorted(
            variants,
            key=lambda item: (
                item.market_country,
                item.external_row_key or "",
                str(item.variant_id),
            ),
        )
        anchor = ordered_variants[0]
        feature_maps = {
            variant.variant_id: _extract_variant_features(variant)
            for variant in ordered_variants
        }
        base_features, base_labels = _derive_base_features(
            list(feature_maps.values())
        )
        base_variant_id = uuid4()
        base_variants.append(
            ConfigBaseVariant(
                base_variant_id=base_variant_id,
                project_id=project.project_id,
                business_key=business_key,
                brand=anchor.brand,
                model=anchor.model,
                trim_name=anchor.trim_name,
                version_name=anchor.version_name,
                powertrain=anchor.powertrain,
                base_features_json=base_features or None,
                base_feature_labels_json=base_labels or None,
                source_variant_count=len(ordered_variants),
                market_count=len(
                    {
                        variant.market_country.strip()
                        for variant in ordered_variants
                        if variant.market_country.strip()
                    }
                ),
            )
        )

        for variant in ordered_variants:
            market_variant_id = uuid4()
            overrides_for_market: list[ConfigMarketFeatureOverride] = []
            for feature_code, entry in sorted(feature_maps[variant.variant_id].items()):
                base_value = base_features.get(feature_code)
                if base_value is not None and _canonical_feature_value(
                    base_value
                ) == _canonical_feature_value(entry.value):
                    continue
                value_type, bool_value, number_value, text_value, json_value = (
                    _feature_storage_columns(entry.value)
                )
                overrides_for_market.append(
                    ConfigMarketFeatureOverride(
                        feature_override_id=uuid4(),
                        project_id=project.project_id,
                        market_variant_id=market_variant_id,
                        source_variant_id=variant.variant_id,
                        feature_code=entry.code,
                        feature_label=entry.label,
                        value_type=value_type,
                        bool_value=bool_value,
                        number_value=number_value,
                        text_value=text_value,
                        json_value=json_value,
                        availability=_infer_availability(entry.value),
                        package_code=_resolve_package_code(
                            entry.label,
                            entry.value,
                        ),
                    )
                )
            market_variants.append(
                ConfigMarketVariant(
                    market_variant_id=market_variant_id,
                    project_id=project.project_id,
                    base_variant_id=base_variant_id,
                    source_variant_id=variant.variant_id,
                    external_row_key=variant.external_row_key,
                    market_country=variant.market_country,
                    target_msrp=variant.target_msrp,
                    source_file_path=variant.source_file_path,
                    override_count=len(overrides_for_market),
                )
            )
            feature_overrides.extend(overrides_for_market)

    return base_variants, market_variants, feature_overrides


def _base_variant_payload(item: ConfigBaseVariant) -> dict[str, object]:
    return {
        "baseVariantId": str(item.base_variant_id),
        "projectId": str(item.project_id),
        "businessKey": item.business_key,
        "brand": item.brand,
        "model": item.model,
        "trimName": item.trim_name,
        "versionName": item.version_name,
        "powertrain": item.powertrain,
        "baseFeatures": _serialize_base_features(
            item.base_features_json,
            item.base_feature_labels_json,
        ),
        "sourceVariantCount": item.source_variant_count,
        "marketCount": item.market_count,
        "createdAtUtc": item.created_at_utc.isoformat(),
        "updatedAtUtc": item.updated_at_utc.isoformat(),
    }


def _market_variant_payload(item: ConfigMarketVariant) -> dict[str, object]:
    return {
        "marketVariantId": str(item.market_variant_id),
        "projectId": str(item.project_id),
        "baseVariantId": str(item.base_variant_id),
        "sourceVariantId": str(item.source_variant_id),
        "externalRowKey": item.external_row_key,
        "marketCountry": item.market_country,
        "targetMsrp": (
            float(item.target_msrp) if item.target_msrp is not None else None
        ),
        "sourceFilePath": item.source_file_path,
        "overrideCount": item.override_count,
        "createdAtUtc": item.created_at_utc.isoformat(),
        "updatedAtUtc": item.updated_at_utc.isoformat(),
    }


def _override_effective_value(
    item: ConfigMarketFeatureOverride,
) -> object | None:
    if item.value_type == "bool":
        return item.bool_value
    if item.value_type == "number":
        return float(item.number_value) if item.number_value is not None else None
    if item.value_type == "text":
        return item.text_value
    return item.json_value


def _market_feature_override_payload(
    item: ConfigMarketFeatureOverride,
) -> dict[str, object]:
    return {
        "featureOverrideId": str(item.feature_override_id),
        "projectId": str(item.project_id),
        "marketVariantId": str(item.market_variant_id),
        "sourceVariantId": (
            str(item.source_variant_id) if item.source_variant_id is not None else None
        ),
        "featureCode": item.feature_code,
        "featureLabel": item.feature_label,
        "valueType": item.value_type,
        "value": _override_effective_value(item),
        "availability": item.availability,
        "packageCode": item.package_code,
        "createdAtUtc": item.created_at_utc.isoformat(),
        "updatedAtUtc": item.updated_at_utc.isoformat(),
    }


def rebuild_project_normalized_config(
    session: Session,
    project: ConfigProject,
) -> dict[str, object]:
    active_variants = repo.list_active_variants_for_project(
        session,
        project.project_id,
    )
    repo.replace_project_normalized_variants(session, project.project_id)
    if not active_variants:
        return {
            "baseVariantCount": 0,
            "marketVariantCount": 0,
            "featureOverrideCount": 0,
        }
    base_variants, market_variants, feature_overrides = _build_normalized_rows(
        project,
        active_variants,
    )
    repo.add_base_variants(session, base_variants)
    repo.add_market_variants(session, market_variants)
    repo.add_market_feature_overrides(session, feature_overrides)
    return {
        "baseVariantCount": len(base_variants),
        "marketVariantCount": len(market_variants),
        "featureOverrideCount": len(feature_overrides),
    }


def normalize_config_project(
    session: Session,
    project_id: str,
) -> dict[str, object]:
    project = repo.get_project(session, UUID(project_id))
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    summary = rebuild_project_normalized_config(session, project)
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Engineering normalization contains duplicate rows",
        ) from exc
    return {
        "projectId": str(project.project_id),
        **summary,
    }


def list_config_base_variants(
    session: Session,
    project_id: UUID,
    model: str | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_base_variants(session, project_id, model, limit)
    return {
        "rows": len(items),
        "items": [_base_variant_payload(item) for item in items],
    }


def list_config_market_variants(
    session: Session,
    project_id: UUID,
    base_variant_id: UUID | None,
    market_country: str | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_market_variants(
        session,
        project_id,
        base_variant_id,
        market_country,
        limit,
    )
    return {
        "rows": len(items),
        "items": [_market_variant_payload(item) for item in items],
    }


def list_config_market_feature_overrides(
    session: Session,
    project_id: UUID,
    base_variant_id: UUID | None,
    market_variant_id: UUID | None,
    market_country: str | None,
    feature_code: str | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_market_feature_overrides(
        session,
        project_id,
        base_variant_id,
        market_variant_id,
        market_country,
        feature_code,
        limit,
    )
    return {
        "rows": len(items),
        "items": [_market_feature_override_payload(item) for item in items],
    }
