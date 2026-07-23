"""Shared normalization helpers for config-like source comparison.

These helpers keep display text intact elsewhere. They only produce stable
keys/tokens for matching, digesting, and difference checks.
"""

from __future__ import annotations

import math
import re
from typing import Any

STANDARD_COMPARE_TOKENS: frozenset[str] = frozenset(
    {"●", "s", "standard", "标配", "标准", "有", "yes", "y", "true", "✓", "✔", "√", "■"}
)
OPTIONAL_COMPARE_TOKENS: frozenset[str] = frozenset(
    {"o", "optional", "选装", "可选", "○", "◯"}
)
NOT_AVAILABLE_COMPARE_TOKENS: frozenset[str] = frozenset(
    {"-", "--", "---", "—", "/", "无", "no", "false", "不配备", "×", "✘", "✕"}
)
NOT_APPLICABLE_COMPARE_TOKENS: frozenset[str] = frozenset(
    {"n/a", "na", "not applicable", "不适用"}
)
COMPARE_UNIT_SUFFIX_PATTERN = re.compile(
    r"\s*(?:km|kw|kwh|mm|kg|l|hp|nm|s|mph|g/km|l/100km|%|千瓦|毫米|公斤|千克|公里|升)\s*$",
    re.IGNORECASE,
)
PAREN_CONTENT_PATTERN = re.compile(r"[\(（][^\(\)（）]*[\)）]")


def clean_config_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if value.__class__.__name__ in {"NAType", "NaTType"}:
        return ""
    return str(value).strip()


def normalise_config_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_header_phrase(value: str) -> str:
    return normalise_config_space(value).lower()


def normalize_compact_token(value: Any) -> str:
    text = clean_config_cell(value).lower()
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", text)


def normalize_config_feature_key(value: str) -> str:
    text = normalise_config_space(value)
    previous = None
    while previous != text:
        previous = text
        text = PAREN_CONTENT_PATTERN.sub("", text)
    return re.sub(r"\s+", "", text).lower()


def config_feature_semantic_keys(value: str) -> list[str]:
    """Return conservative business-level keys for high-confidence feature aliases.

    These keys are intentionally sparse. They are used only as a fallback after
    exact feature-code/name matching, so ambiguous source fields such as
    "Seat ventilation" or "Speaker system" are left for manual review unless a
    formal field mapping catalog provides a stronger alias.
    """
    compact = normalize_compact_token(value)
    normalized = normalize_config_feature_key(value)
    if not compact:
        return []

    keys: list[str] = []
    if "360" in compact and (
        "camera" in compact
        or "view" in compact
        or "影像" in compact
        or "全景" in compact
    ):
        keys.append("feature.camera.360")
    if (
        ("wireless" in compact and ("charge" in compact or "charging" in compact))
        or "无线充电" in compact
    ):
        keys.append("feature.wireless_charging")
    if (
        (
            "sunroof" in compact
            or "panoramicroof" in compact
            or "panoramicroof" in normalized
            or "panoramadach" in normalized
            or "panoramicskylight" in compact
            or compact == "roof"
            or "天窗" in compact
        )
        and "without" not in compact
        and not compact.startswith("no")
        and "不带" not in compact
        and "无天窗" not in compact
    ):
        keys.append("feature.sunroof")
    if (
        compact == "hud"
        or "headupdisplay" in compact
        or "headupdisplay" in normalized
        or "抬头显示" in compact
    ):
        keys.append("feature.display.head_up")
    if (
        (
            ("tailgate" in compact and ("electric" in compact or "power" in compact))
            or ("heckklappe" in normalized and "elektrisch" in normalized)
            or "电动尾门" in compact
        )
        and "manual" not in compact
        and "手动" not in compact
    ):
        keys.append("feature.tailgate.power")
    if (
        (
            ("seat" in compact and ("ventilation" in compact or "ventilated" in compact))
            or ("sitzbelüftung" in normalized and "vorne" in normalized)
            or "前排座椅通风" in compact
        )
        and ("front" in compact or "vorne" in normalized or "前排" in compact)
    ):
        keys.append("feature.seat.front_ventilation")
    if (
        (
            ("driverseat" in compact and "memory" in compact)
            or ("fahrersitz" in normalized and "memory" in normalized)
            or "座椅记忆" in compact
        )
        and "mirror" not in compact
        and "后视镜" not in compact
    ):
        keys.append("feature.seat.driver_memory")
    return keys


def normalize_config_value_for_compare(value: str | None) -> str | None:
    cleaned = normalise_config_space(value or "")
    if not cleaned:
        return None

    lowered = cleaned.lower()
    if lowered in STANDARD_COMPARE_TOKENS:
        return "__YES__"
    if lowered in OPTIONAL_COMPARE_TOKENS:
        return "__OPTIONAL__"
    if lowered in NOT_AVAILABLE_COMPARE_TOKENS:
        return "__NO__"
    if lowered in NOT_APPLICABLE_COMPARE_TOKENS:
        return "__NA__"

    stripped_unit = COMPARE_UNIT_SUFFIX_PATTERN.sub("", cleaned).strip()
    return normalise_config_space(stripped_unit).lower() or lowered
