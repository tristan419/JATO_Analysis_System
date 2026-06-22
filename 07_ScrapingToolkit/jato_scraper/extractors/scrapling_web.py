"""Scrapling-based web page extractor."""

from __future__ import annotations

import json
import logging
import re
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Literal

from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation

log = logging.getLogger(__name__)
FetcherTier = Literal["http", "stealth", "dynamic"]


@dataclass(frozen=True)
class CssMapping:
    vehicle_container: str
    model: str
    trim: str = ""
    price: str = ""
    currency: str | None = None
    availability: str | None = None
    exclude_if_selector: str | None = None
    include_if_text_contains: tuple[str, ...] = ()


@dataclass(frozen=True)
class AttrJsonMapping:
    """Extract vehicle data from JSON stored in HTML element attributes.

    Many manufacturer sites embed structured data as JSON strings
    in data-* attributes rather than in the visible DOM text.
    """
    vehicle_container: str          # CSS selector for each vehicle element
    filter_attr: str = ""           # attribute name with price/filter JSON
    tracking_attr: str = ""         # attribute name with model identity JSON
    # Keys inside filter_attr JSON
    price_key: str = "price"
    fuel_key: str = "fuelType"
    category_key: str = "category"
    series_key: str = "series"
    # Keys inside tracking_attr JSON
    name_key: str = "name"
    range_key: str = "range"


@dataclass(frozen=True)
class TextRegexEntryPattern:
    pattern: str
    official_trim: str | None = None
    official_powertrain: str | None = None
    official_edition: str | None = None
    availability_text: str | None = None
    price_label: str | None = None


@dataclass(frozen=True)
class TextRegexMapping:
    source_selector: str = "body"
    entry_patterns: tuple[TextRegexEntryPattern, ...] = ()
    include_element_html: bool = False


@dataclass(frozen=True)
class PricingContextMapping:
    fields: dict[str, str] = field(default_factory=dict)
    constants: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScraplingProfile:
    url: str
    tier: FetcherTier = "http"
    css: CssMapping | None = None
    attr_json: AttrJsonMapping | None = None
    text_regex: TextRegexMapping | None = None
    pricing_context: PricingContextMapping | None = None
    json_script_selector: str | None = None
    json_vehicles_path: str | None = None
    headless: bool = True
    network_idle: bool = True
    load_dom: bool | None = None
    timeout_ms: int | None = None
    wait_ms: int | None = None
    disable_resources: bool | None = None
    retries: int | None = None
    retry_delay_seconds: float | None = None
    impersonate: str = "chrome"
    solve_cloudflare: bool = False
    default_currency: str = "EUR"
    default_tax_included: bool = True
    default_price_label: str = "Manufacturer's Recommended Retail Price"
    fixed_model: str | None = None
    fixed_jato_model: str | None = None
    fixed_jato_powertrain: str | None = None
    copy_trim_to_jato_trim: bool = False
    match_confidence: float | None = None
    match_status: str = "review_required"
    match_reason: dict[str, Any] | None = None
    exclude_price_prefixes: tuple[str, ...] = ()
    confidence_rules: dict[str, Any] | None = None
    structured_fields: dict[str, Any] | None = None
    auto_accept_gates: dict[str, Any] | None = None
    model_rules: tuple[dict[str, Any], ...] | None = None
    skip_if_model_unmapped: bool = False


@dataclass(frozen=True)
class StructuredVariantFields:
    trim_search_text: str
    powertrain_search_text: str
    official_edition: str | None = None
    official_powertrain: str | None = None
    jato_powertrain: str | None = None
    model_mapping_source: str | None = None
    model_mapping_keywords: tuple[str, ...] = ()
    has_special_edition: bool = False
    special_edition_labels: tuple[str, ...] = ()
    edition_sources: tuple[str, ...] = ()
    powertrain_source: str | None = None
    powertrain_ambiguous: bool = False


_PRICE_RE = re.compile(r"[\d]+(?:[.,'\u2019]\d{3})*(?:[.,]\d{1,2})?")
_FINANCE_AMOUNT_FIELDS = frozenset({
    "monthly_payment",
    "down_payment",
    "down_payment_pct",
    "apr",
    "effective_apr",
    "balloon_payment",
    "total_credit_cost",
    "total_amount_payable",
    "subsidy_amount",
    "net_price_after_subsidy",
})
_FINANCE_INTEGER_FIELDS = frozenset({
    "term_months",
    "annual_mileage_limit",
})
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_GENERIC_EDITION_RE = re.compile(
    r"\b([a-z0-9][a-z0-9\-]*\s+edition)\b",
    re.IGNORECASE,
)
_SPECIAL_EDITION_LABELS = {
    "black edition",
    "first edition",
    "launch edition",
    "limited edition",
    "special edition",
}
_DEFAULT_POWERTRAIN_RULES: tuple[dict[str, Any], ...] = (
    {
        "powertrain": "BEV",
        "keywords": (
            "bev",
            "battery electric",
            "electric vehicle",
            "fully electric",
            "electric",
        ),
    },
    {
        "powertrain": "PHEV",
        "keywords": (
            "phev",
            "plug-in hybrid",
            "plugin hybrid",
            "plug in hybrid",
            "laddhybrid",
            "recharge",
        ),
    },
    {
        "powertrain": "MHEV",
        "keywords": (
            "mhev",
            "mild hybrid",
        ),
    },
    {
        "powertrain": "HEV",
        "keywords": (
            "hev",
            "full hybrid",
            "self-charging hybrid",
            "self charging hybrid",
        ),
    },
    {
        "powertrain": "HEV",
        "keywords": ("hybrid",),
        "ambiguous": True,
    },
    {
        "powertrain": "ICE",
        "keywords": (
            "ice",
            "petrol",
            "gasoline",
            "diesel",
            "bensin",
            "benzin",
        ),
    },
    {
        "powertrain": "FCV",
        "keywords": (
            "fcv",
            "fcev",
            "fuel cell",
            "hydrogen",
        ),
    },
    {
        "powertrain": "LPG",
        "keywords": (
            "lpg",
            "autogas",
        ),
    },
    {
        "powertrain": "REEV",
        "keywords": (
            "reev",
            "erev",
            "range extender",
            "range-extended",
        ),
    },
)


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _normalize_powertrain_label(value: str | None) -> str | None:
    normalized = _normalize_space(str(value or ""))
    return normalized.upper() or None


def _title_case_label(value: str) -> str:
    words = _normalize_space(value).split(" ")
    return " ".join(
        word.upper() if len(word) <= 4 else word.capitalize()
        for word in words
    )


def parse_price(raw: str) -> float | None:
    m = _PRICE_RE.search(raw.replace("\xa0", "").replace(" ", ""))
    if not m:
        return None
    s = m.group().replace("'", "").replace("\u2019", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts[-1]) <= 2:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "." in s:
        parts = s.split(".")
        if all(len(p) == 3 for p in parts[1:]):
            s = s.replace(".", "")
    return float(s)


def _resolve_payload_path(payload: dict[str, Any], path: str) -> Any | None:
    current: Any = payload
    for part in path.split("."):
        key = part.strip()
        if not key:
            return None
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    return current


def _coerce_pricing_context_value(field_name: str, value: Any) -> Any | None:
    if value in (None, "", [], {}):
        return None
    if field_name in _FINANCE_AMOUNT_FIELDS:
        if isinstance(value, (int, float)):
            return value
        parsed = parse_price(str(value))
        return parsed if parsed is not None else value
    if field_name in _FINANCE_INTEGER_FIELDS:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        parsed = parse_price(str(value))
        return int(parsed) if parsed is not None else value
    if isinstance(value, str):
        return value.strip() or None
    return value


class ScraplingExtractor(BaseExtractor):
    def __init__(
        self,
        config: ExtractorConfig,
        profile: ScraplingProfile,
    ) -> None:
        super().__init__(config)
        self.profile = profile
        self._last_fetch_error: str | None = None

    @property
    def extractor_version(self) -> str:
        return "0.6.2-scrapling"

    def _string_values(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, (list, tuple, set)):
            return [
                str(item)
                for item in value
                if item not in (None, "")
            ]
        return []

    def _collect_text_result_values(self, text_result: Any) -> list[str]:
        values: list[str] = []
        if text_result is None:
            return values
        for method_name in ("getall", "get_all"):
            method = getattr(text_result, method_name, None)
            if not callable(method):
                continue
            try:
                values.extend(self._string_values(method()))
            except Exception:
                continue
        if values:
            return values
        try:
            values.extend(self._string_values(text_result.get()))
        except Exception:
            pass
        return values

    def _build_trim_search_text(
        self,
        resolved_trim: str,
        raw_payload: dict[str, Any],
    ) -> str:
        return _normalize_space(
            " ".join(
                part
                for part in (
                    resolved_trim,
                    str(raw_payload.get("trimText", "")),
                )
                if part
            )
        ).lower()

    def _nested_payload_text(
        self,
        raw_payload: dict[str, Any],
        *container_names: str,
    ) -> str:
        parts: list[str] = []
        for container_name in container_names:
            container = raw_payload.get(container_name)
            if not isinstance(container, dict):
                continue
            parts.extend(
                str(value)
                for value in container.values()
                if value not in (None, "", [], {})
            )
        return _normalize_space(" ".join(parts))

    def _build_powertrain_search_text(
        self,
        resolved_model: str,
        resolved_trim: str,
        raw_payload: dict[str, Any],
    ) -> str:
        return _normalize_space(
            " ".join(
                part
                for part in (
                    self.profile.url,
                    self.config.source_url,
                    resolved_model,
                    resolved_trim,
                    str(raw_payload.get("trimText", "")),
                    str(raw_payload.get("powertrain", "")),
                    str(raw_payload.get("powertrainText", "")),
                    str(raw_payload.get("fuel_type", "")),
                    str(raw_payload.get("fuelType", "")),
                    str(raw_payload.get("category", "")),
                    self._nested_payload_text(raw_payload, "filter", "tracking"),
                )
                if part
            )
        ).lower()

    def _build_model_search_text(
        self,
        resolved_model: str,
        resolved_trim: str,
        raw_payload: dict[str, Any],
    ) -> str:
        return _normalize_space(
            " ".join(
                part
                for part in (
                    resolved_model,
                    resolved_trim,
                    str(raw_payload.get("trimText", "")),
                    str(raw_payload.get("name", "")),
                    str(raw_payload.get("model", "")),
                    str(raw_payload.get("description", "")),
                    str(raw_payload.get("series", "")),
                    str(raw_payload.get("range", "")),
                    str(raw_payload.get("fuel_type", "")),
                    str(raw_payload.get("fuelType", "")),
                    str(raw_payload.get("category", "")),
                    self._nested_payload_text(raw_payload, "filter", "tracking"),
                )
                if part
            )
        ).lower()

    def _resolve_model_mapping(
        self,
        resolved_model: str,
        resolved_trim: str,
        raw_payload: dict[str, Any],
    ) -> tuple[str, str | None, tuple[str, ...], str | None]:
        fixed_jato_model = _normalize_space(
            str(self.profile.fixed_jato_model or "")
        )
        if fixed_jato_model:
            return fixed_jato_model, None, (), None

        model_rules = self.profile.model_rules
        if not isinstance(model_rules, tuple) or not model_rules:
            return "", None, (), None

        model_search_text = self._build_model_search_text(
            resolved_model,
            resolved_trim,
            raw_payload,
        )
        for index, rule in enumerate(model_rules):
            if not isinstance(rule, dict):
                continue
            keywords = [
                _normalize_space(str(keyword)).lower()
                for keyword in list(rule.get("keywords") or [])
                if _normalize_space(str(keyword))
            ]
            if not keywords:
                keyword = _normalize_space(str(rule.get("keyword", "")))
                if keyword:
                    keywords = [keyword.lower()]
            if not keywords:
                fallback_keyword = _normalize_space(
                    str(
                        rule.get("jato_model")
                        or rule.get("official_model")
                        or rule.get("label")
                        or ""
                    )
                )
                if fallback_keyword:
                    keywords = [fallback_keyword.lower()]
            if not keywords:
                continue

            matched_keywords = tuple(
                keyword
                for keyword in keywords
                if keyword in model_search_text
            )
            if not matched_keywords:
                continue

            jato_model = _normalize_space(
                str(
                    rule.get("jato_model")
                    or rule.get("model")
                    or rule.get("label")
                    or ""
                )
            )
            if not jato_model:
                continue

            official_model_override = _normalize_space(
                str(
                    rule.get("official_model")
                    or rule.get("official_label")
                    or ""
                )
            )
            key = str(rule.get("key") or f"model_rule_{index}")
            return (
                jato_model,
                f"rule:{key}",
                matched_keywords,
                official_model_override or None,
            )

        return "", None, (), None

    def _extract_edition_matches(
        self,
        trim_text: str,
    ) -> tuple[list[str], list[str], list[str]]:
        config = self.profile.structured_fields or {}
        trim_search_text = trim_text.lower()
        labels: list[str] = []
        sources: list[str] = []
        special_labels: set[str] = set()
        seen: set[str] = set()
        rules = config.get("edition_rules")

        if isinstance(rules, list):
            for index, rule in enumerate(rules):
                if not isinstance(rule, dict):
                    continue
                keywords = [
                    _normalize_space(str(keyword)).lower()
                    for keyword in list(rule.get("keywords") or [])
                    if _normalize_space(str(keyword))
                ]
                if not keywords:
                    keyword = _normalize_space(str(rule.get("keyword", "")))
                    if keyword:
                        keywords = [keyword.lower()]
                if not keywords:
                    continue
                if not any(
                    keyword in trim_search_text
                    for keyword in keywords
                ):
                    continue
                label = _normalize_space(
                    str(rule.get("label") or keywords[0])
                )
                if not label:
                    continue
                dedupe_key = label.lower()
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                labels.append(label)
                key = str(rule.get("key") or f"edition_{index}")
                sources.append(f"rule:{key}")
                if bool(rule.get("special")):
                    special_labels.add(label)

        for match in _GENERIC_EDITION_RE.finditer(trim_text):
            label = _title_case_label(match.group(1))
            dedupe_key = label.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            labels.append(label)
            sources.append("generic:edition")
            if dedupe_key in _SPECIAL_EDITION_LABELS:
                special_labels.add(label)

        return labels, list(special_labels), sources

    def _match_powertrain_rule(
        self,
        search_text: str,
        rules: list[dict[str, Any]] | tuple[dict[str, Any], ...],
        source_prefix: str,
    ) -> tuple[str | None, str | None, bool]:
        for index, rule in enumerate(rules):
            if not isinstance(rule, dict):
                continue
            keywords = [
                _normalize_space(str(keyword)).lower()
                for keyword in list(rule.get("keywords") or [])
                if _normalize_space(str(keyword))
            ]
            if not keywords:
                keyword = _normalize_space(str(rule.get("keyword", "")))
                if keyword:
                    keywords = [keyword.lower()]
            if not keywords:
                continue
            if not any(keyword in search_text for keyword in keywords):
                continue
            label = _normalize_powertrain_label(
                str(rule.get("powertrain") or rule.get("label") or "")
            )
            if not label:
                continue
            key = str(rule.get("key") or f"{source_prefix}_{index}")
            return label, f"{source_prefix}:{key}", bool(rule.get("ambiguous"))
        return None, None, False

    def _resolve_powertrain(
        self,
        trim_search_text: str,
        powertrain_search_text: str,
    ) -> tuple[str | None, str | None, bool]:
        config = self.profile.structured_fields or {}
        configured_rules = config.get("powertrain_rules")
        if isinstance(configured_rules, list):
            # Pass 1: trim text only (higher precision)
            label, source, amb = self._match_powertrain_rule(
                trim_search_text,
                configured_rules,
                "configured_powertrain_trim",
            )
            if label:
                return label, source, bool(amb)
            # Pass 2: full text including URL
            label, source, amb = self._match_powertrain_rule(
                powertrain_search_text,
                configured_rules,
                "configured_powertrain",
            )
            if label:
                return label, source, bool(amb)

        label, source, ambiguous = (
            self._match_powertrain_rule(
                powertrain_search_text,
                _DEFAULT_POWERTRAIN_RULES,
                "generic_powertrain",
            )
        )
        fixed_pt = _normalize_powertrain_label(
            self.profile.fixed_jato_powertrain
        )
        if ambiguous and fixed_pt:
            return (
                fixed_pt,
                "fallback:fixed_jato_powertrain",
                True,
            )
        return label, source, bool(ambiguous)

    def _extract_structured_variant_fields(
        self,
        resolved_model: str,
        resolved_trim: str,
        raw_payload: dict[str, Any],
        model_mapping_source: str | None = None,
        model_mapping_keywords: tuple[str, ...] = (),
    ) -> StructuredVariantFields:
        trim_text = _normalize_space(
            " ".join(
                part
                for part in (
                    resolved_trim,
                    str(raw_payload.get("trimText", "")),
                )
                if part
            )
        )
        trim_search_text = self._build_trim_search_text(
            resolved_trim,
            raw_payload,
        )
        powertrain_search_text = self._build_powertrain_search_text(
            resolved_model,
            resolved_trim,
            raw_payload,
        )
        edition_labels, special_edition_labels, edition_sources = (
            self._extract_edition_matches(trim_text)
        )
        official_powertrain, powertrain_source, pt_ambiguous = (
            self._resolve_powertrain(
                trim_search_text,
                powertrain_search_text,
            )
        )
        fixed_pt = _normalize_powertrain_label(
            self.profile.fixed_jato_powertrain
        )
        if official_powertrain and not pt_ambiguous:
            jato_powertrain = official_powertrain
        else:
            jato_powertrain = fixed_pt or official_powertrain
        return StructuredVariantFields(
            trim_search_text=trim_search_text,
            powertrain_search_text=powertrain_search_text,
            official_edition=(" | ".join(edition_labels) or None),
            official_powertrain=official_powertrain,
            jato_powertrain=jato_powertrain,
            model_mapping_source=model_mapping_source,
            model_mapping_keywords=tuple(model_mapping_keywords),
            has_special_edition=bool(special_edition_labels),
            special_edition_labels=tuple(special_edition_labels),
            edition_sources=tuple(edition_sources),
            powertrain_source=powertrain_source,
            powertrain_ambiguous=pt_ambiguous,
        )

    def _append_structured_fields_to_reason(
        self,
        match_reason: dict[str, Any] | None,
        variant_fields: StructuredVariantFields,
    ) -> dict[str, Any]:
        reason = deepcopy(match_reason) if match_reason else {}
        reason["structuredFields"] = {
            "officialEdition": variant_fields.official_edition,
            "officialPowertrain": variant_fields.official_powertrain,
            "jatoPowertrain": variant_fields.jato_powertrain,
            "modelMappingSource": variant_fields.model_mapping_source,
            "modelMappingKeywords": list(
                variant_fields.model_mapping_keywords
            ),
            "hasSpecialEdition": variant_fields.has_special_edition,
            "specialEditionLabels": list(
                variant_fields.special_edition_labels
            ),
            "editionSources": list(variant_fields.edition_sources),
            "powertrainSource": variant_fields.powertrain_source,
            "powertrainAmbiguous": (
                variant_fields.powertrain_ambiguous
            ),
        }
        return reason

    def _resolve_match_status(
        self,
        match_confidence: float,
        variant_fields: StructuredVariantFields,
        match_reason: dict[str, Any] | None,
    ) -> tuple[str, dict[str, Any]]:
        base_status = self.profile.match_status
        gates = self.profile.auto_accept_gates or {}
        should_evaluate = (
            base_status == "auto_accepted"
            or bool(gates)
        )
        reason = self._append_structured_fields_to_reason(
            match_reason,
            variant_fields,
        )
        if not should_evaluate:
            return base_status, reason

        review_thr = float(
            gates.get(
                "review_threshold",
                gates.get("threshold", 0.95),
            )
        )
        semi_thr = float(
            gates.get("semi_auto_threshold", 1.01)
        )
        require_pt = bool(
            gates.get("require_powertrain_match")
        )
        force_pt_missing = bool(
            gates.get(
                "force_review_if_powertrain_missing"
            )
        )
        force_special = bool(
            gates.get(
                "force_review_for_special_edition"
            )
        )
        force_pt_ambig = bool(
            gates.get(
                "force_review_if_powertrain_ambiguous"
            )
        )

        checks: list[dict[str, Any]] = []
        score_ok = match_confidence >= review_thr
        checks.append(
            {
                "key": "score_threshold",
                "passed": score_ok,
                "expected": round(review_thr, 4),
                "actual": round(
                    match_confidence, 4
                ),
            }
        )
        if not score_ok:
            reason["autoAcceptGate"] = {
                "evaluated": True,
                "tier": "below_threshold",
                "reviewThreshold": round(
                    review_thr, 4
                ),
                "semiAutoThreshold": round(
                    semi_thr, 4
                ),
                "finalStatus": "review_required",
                "checks": checks,
            }
            return "review_required", reason

        all_ok = True

        if require_pt:
            expected_pt = (
                _normalize_powertrain_label(
                    self.profile
                    .fixed_jato_powertrain
                )
                or _normalize_powertrain_label(
                    gates.get(
                        "expected_jato_powertrain"
                    )
                )
            )
            actual_pt = (
                variant_fields.official_powertrain
            )
            pt_ok = True
            if expected_pt and actual_pt:
                pt_ok = actual_pt == expected_pt
            elif expected_pt and force_pt_missing:
                pt_ok = False
            checks.append(
                {
                    "key": "powertrain_match",
                    "passed": pt_ok,
                    "expected": expected_pt,
                    "actual": actual_pt,
                }
            )
            all_ok = all_ok and pt_ok

        if force_special:
            sp_ok = (
                not variant_fields
                .has_special_edition
            )
            sp_labels = list(
                variant_fields
                .special_edition_labels
            )
            checks.append(
                {
                    "key": "special_edition_block",
                    "passed": sp_ok,
                    "actual": sp_labels,
                }
            )
            all_ok = all_ok and sp_ok

        if force_pt_ambig:
            amb_ok = (
                not variant_fields
                .powertrain_ambiguous
            )
            checks.append(
                {
                    "key": "powertrain_ambiguity",
                    "passed": amb_ok,
                    "actual": (
                        variant_fields
                        .powertrain_ambiguous
                    ),
                }
            )
            all_ok = all_ok and amb_ok

        if all_ok:
            if match_confidence >= semi_thr:
                tier = "full_auto"
            else:
                tier = "semi_auto"
        else:
            tier = "constraint_failed"

        final_status = (
            "auto_accepted" if all_ok
            else "review_required"
        )
        reason["autoAcceptGate"] = {
            "evaluated": True,
            "tier": tier,
            "reviewThreshold": round(
                review_thr, 4
            ),
            "semiAutoThreshold": round(
                semi_thr, 4
            ),
            "finalStatus": final_status,
            "checks": checks,
        }
        return final_status, reason

    def _compute_rule_based_match(
        self,
        resolved_model: str,
        resolved_trim: str,
        resolved_jato_model: str,
        resolved_jato_trim: str,
        source_price_value: float,
        currency: str,
        raw_payload: dict[str, Any],
        variant_fields: StructuredVariantFields,
    ) -> tuple[float, dict[str, Any]]:
        rules = self.profile.confidence_rules or {}
        base = float(rules.get("base", 0.0))
        clamp_min = float(rules.get("clamp_min", 0.0))
        clamp_max = float(rules.get("clamp_max", 1.0))
        score = base
        components: list[dict[str, Any]] = []
        trim_search_text = variant_fields.trim_search_text
        powertrain_search_text = variant_fields.powertrain_search_text

        def build_rule_key(
            prefix: str,
            raw_key: Any,
            fallback_index: int,
        ) -> str:
            if raw_key not in (None, ""):
                return str(raw_key)
            slug = re.sub(r"[^a-z0-9]+", "_", prefix.lower()).strip("_")
            return f"{slug}_{fallback_index}"

        def format_price_value(value: float, value_currency: str) -> str:
            rounded = round(value, 2)
            if float(rounded).is_integer():
                return f"{int(rounded)} {value_currency}"
            return f"{rounded:.2f} {value_currency}"

        def apply_component(
            key: str,
            label: str,
            applied: bool,
            delta: float,
            evidence: Any | None = None,
        ) -> None:
            nonlocal score
            if applied:
                score += delta
            component: dict[str, Any] = {
                "key": key,
                "label": label,
                "applied": applied,
                "delta": round(delta, 4),
            }
            if evidence not in (None, "", [], {}):
                component["evidence"] = evidence
            components.append(component)

        apply_component(
            "fixed_model",
            "Fixed official model configured",
            bool(self.profile.fixed_model),
            float(rules.get("fixed_model_bonus", 0.0)),
            self.profile.fixed_model,
        )
        apply_component(
            "fixed_jato_model",
            "Fixed JATO model configured",
            bool(self.profile.fixed_jato_model),
            float(rules.get("fixed_jato_model_bonus", 0.0)),
            self.profile.fixed_jato_model,
        )
        apply_component(
            "model_rule_match",
            "JATO model resolved from configured model rules",
            bool(
                variant_fields.model_mapping_source
                and resolved_jato_model
            ),
            float(rules.get("model_rule_bonus", 0.0)),
            (
                {
                    "source": variant_fields.model_mapping_source,
                    "keywords": list(
                        variant_fields.model_mapping_keywords
                    ),
                    "jatoModel": resolved_jato_model,
                }
                if variant_fields.model_mapping_source
                else None
            ),
        )
        apply_component(
            "trim_present",
            "Trim text extracted",
            bool(resolved_trim),
            float(rules.get("trim_present_bonus", 0.0)),
            resolved_trim,
        )
        apply_component(
            "copy_trim_to_jato_trim",
            "Trim copied to JATO trim",
            bool(self.profile.copy_trim_to_jato_trim and resolved_jato_trim),
            float(rules.get("copy_trim_to_jato_trim_bonus", 0.0)),
            resolved_jato_trim,
        )
        apply_component(
            "exclude_price_prefixes",
            "Family-card price prefixes excluded",
            bool(self.profile.exclude_price_prefixes),
            float(rules.get("exclude_price_prefixes_bonus", 0.0)),
            list(self.profile.exclude_price_prefixes),
        )
        apply_component(
            "exclude_if_selector",
            "Powertrain cards excluded by selector",
            bool(self.profile.css and self.profile.css.exclude_if_selector),
            float(rules.get("exclude_if_selector_bonus", 0.0)),
            self.profile.css.exclude_if_selector if self.profile.css else None,
        )
        apply_component(
            "parsed_price_text",
            "Price text parsed successfully",
            bool(raw_payload.get("priceText")),
            float(rules.get("parsed_price_text_bonus", 0.0)),
            raw_payload.get("priceText"),
        )
        apply_component(
            "currency",
            "Currency resolved",
            bool(currency),
            float(rules.get("currency_bonus", 0.0)),
            currency,
        )
        apply_component(
            "price_label",
            "Price label configured",
            bool(self.profile.default_price_label),
            float(rules.get("price_label_bonus", 0.0)),
            self.profile.default_price_label,
        )

        trim_keyword_rules = rules.get("trim_keyword_bonuses")
        if isinstance(trim_keyword_rules, list):
            for index, rule in enumerate(trim_keyword_rules):
                if not isinstance(rule, dict):
                    continue
                keyword = str(rule.get("keyword", "")).strip()
                if not keyword:
                    continue
                apply_component(
                    build_rule_key(
                        "trim_keyword",
                        rule.get("key"),
                        index,
                    ),
                    str(
                        rule.get(
                            "label",
                            f"Trim keyword matched: {keyword}",
                        )
                    ),
                    keyword.lower() in trim_search_text,
                    float(rule.get("delta", 0.0)),
                    keyword,
                )

        price_band_rules = rules.get("price_band_bonuses")
        price_band_matched = False
        if isinstance(price_band_rules, list):
            for index, rule in enumerate(price_band_rules):
                if not isinstance(rule, dict):
                    continue
                minimum_raw = rule.get("min")
                maximum_raw = rule.get("max")
                minimum = (
                    float(minimum_raw)
                    if minimum_raw not in (None, "")
                    else None
                )
                maximum = (
                    float(maximum_raw)
                    if maximum_raw not in (None, "")
                    else None
                )
                in_band = True
                if minimum is not None:
                    in_band = in_band and source_price_value >= minimum
                if maximum is not None:
                    in_band = in_band and source_price_value <= maximum
                applied = bool(in_band and not price_band_matched)
                if applied:
                    price_band_matched = True
                apply_component(
                    build_rule_key(
                        "price_band",
                        rule.get("key"),
                        index,
                    ),
                    str(
                        rule.get(
                            "label",
                            "Price band matched",
                        )
                    ),
                    applied,
                    float(rule.get("delta", 0.0)),
                    format_price_value(source_price_value, currency),
                )

        powertrain_keyword_rules = rules.get("powertrain_keyword_bonuses")
        if isinstance(powertrain_keyword_rules, list):
            for index, rule in enumerate(powertrain_keyword_rules):
                if not isinstance(rule, dict):
                    continue
                keyword = str(rule.get("keyword", "")).strip()
                if not keyword:
                    continue
                apply_component(
                    build_rule_key(
                        "powertrain_keyword",
                        rule.get("key"),
                        index,
                    ),
                    str(
                        rule.get(
                            "label",
                            f"Powertrain keyword matched: {keyword}",
                        )
                    ),
                    keyword.lower() in powertrain_search_text,
                    float(rule.get("delta", 0.0)),
                    keyword,
                )

        powertrain_rules = rules.get("powertrain_bonuses")
        if isinstance(powertrain_rules, list):
            for index, rule in enumerate(powertrain_rules):
                if not isinstance(rule, dict):
                    continue
                expected_powertrain = _normalize_powertrain_label(
                    str(rule.get("powertrain") or rule.get("label") or "")
                )
                if not expected_powertrain:
                    continue
                apply_component(
                    build_rule_key(
                        "powertrain",
                        rule.get("key"),
                        index,
                    ),
                    str(
                        rule.get(
                            "label",
                            f"Powertrain matched: {expected_powertrain}",
                        )
                    ),
                    variant_fields.official_powertrain == expected_powertrain,
                    float(rule.get("delta", 0.0)),
                    variant_fields.official_powertrain,
                )

        total = round(max(clamp_min, min(score, clamp_max)), 4)
        advanced_rule_mode = any(
            isinstance(rules.get(key), list) and rules.get(key)
            for key in (
                "trim_keyword_bonuses",
                "price_band_bonuses",
                "powertrain_keyword_bonuses",
                "powertrain_bonuses",
            )
        )
        reason: dict[str, Any] = (
            deepcopy(self.profile.match_reason)
            if self.profile.match_reason
            else {}
        )
        reason["confidenceRule"] = {
            "mode": (
                "weighted_profile_v2"
                if advanced_rule_mode
                else "weighted_profile_v1"
            ),
            "base": round(base, 4),
            "total": total,
            "clampMin": round(clamp_min, 4),
            "clampMax": round(clamp_max, 4),
            "components": components,
        }
        reason["evidence"] = {
            "officialModel": resolved_model,
            "officialTrim": resolved_trim,
            "officialEdition": variant_fields.official_edition,
            "officialPowertrain": variant_fields.official_powertrain,
            "jatoModel": resolved_jato_model,
            "jatoTrim": resolved_jato_trim,
            "jatoPowertrain": variant_fields.jato_powertrain,
            "modelMappingSource": variant_fields.model_mapping_source,
            "modelMappingKeywords": list(
                variant_fields.model_mapping_keywords
            ),
            "sourcePriceValue": source_price_value,
            "currency": currency,
            "priceText": raw_payload.get("priceText"),
            "sourceUrl": self.profile.url,
        }
        reason = self._append_structured_fields_to_reason(
            reason,
            variant_fields,
        )
        return total, reason

    def _resolve_match_metadata(
        self,
        resolved_model: str,
        resolved_trim: str,
        resolved_jato_model: str,
        resolved_jato_trim: str,
        source_price_value: float,
        currency: str,
        raw_payload: dict[str, Any],
        variant_fields: StructuredVariantFields,
    ) -> tuple[float, dict[str, Any] | None]:
        if self.profile.confidence_rules:
            return self._compute_rule_based_match(
                resolved_model,
                resolved_trim,
                resolved_jato_model,
                resolved_jato_trim,
                source_price_value,
                currency,
                raw_payload,
                variant_fields,
            )
        match_reason = (
            deepcopy(self.profile.match_reason)
            if self.profile.match_reason
            else None
        )
        reason = self._append_structured_fields_to_reason(
            match_reason,
            variant_fields,
        )
        return float(self.profile.match_confidence or 0.0), reason

    def _build_pricing_context(
        self,
        raw_payload: dict[str, Any],
    ) -> dict[str, Any]:
        mapping = self.profile.pricing_context
        if mapping is None:
            return {}

        context: dict[str, Any] = {}
        for field_name, value in mapping.constants.items():
            normalized_field_name = str(field_name).strip()
            if not normalized_field_name:
                continue
            coerced = _coerce_pricing_context_value(
                normalized_field_name,
                value,
            )
            if coerced is not None:
                context[normalized_field_name] = coerced

        for field_name, source_path in mapping.fields.items():
            normalized_field_name = str(field_name).strip()
            normalized_source_path = str(source_path).strip()
            if not (normalized_field_name and normalized_source_path):
                continue
            value = _resolve_payload_path(raw_payload, normalized_source_path)
            coerced = _coerce_pricing_context_value(
                normalized_field_name,
                value,
            )
            if coerced is not None:
                context[normalized_field_name] = coerced

        return context

    def _raw_payload_with_pricing_context(
        self,
        raw_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        payload = dict(raw_payload or {})
        pricing_context = self._build_pricing_context(payload)
        if not pricing_context:
            return payload

        existing = payload.get("pricingContext")
        if isinstance(existing, dict):
            payload["pricingContext"] = {
                **pricing_context,
                **existing,
            }
        else:
            payload["pricingContext"] = pricing_context
        return payload

    def _build_observation(
        self,
        official_model: str,
        official_trim: str,
        msrp_value: float,
        currency: str,
        raw_payload: dict[str, Any] | None = None,
        availability_text: str | None = None,
        source_url: str | None = None,
    ) -> RawObservation | None:
        profile = self.profile
        payload = self._raw_payload_with_pricing_context(raw_payload)
        resolved_model = (profile.fixed_model or official_model).strip()
        resolved_trim = official_trim.strip()
        (
            resolved_jato_model,
            model_mapping_source,
            model_mapping_keywords,
            official_model_override,
        ) = self._resolve_model_mapping(
            resolved_model,
            resolved_trim,
            payload,
        )
        if official_model_override:
            resolved_model = official_model_override
        if (
            profile.skip_if_model_unmapped
            and not resolved_jato_model
            and not profile.fixed_jato_model
            and profile.model_rules
        ):
            log.info(
                (
                    "Skipping unmapped brand-family observation "
                    "for %s/%s: %s | %s"
                ),
                self.config.country,
                self.config.brand,
                official_model,
                official_trim,
            )
            return None
        resolved_jato_trim = (
            resolved_trim if profile.copy_trim_to_jato_trim else ""
        )
        variant_fields = self._extract_structured_variant_fields(
            resolved_model,
            resolved_trim,
            payload,
            model_mapping_source=model_mapping_source,
            model_mapping_keywords=model_mapping_keywords,
        )
        match_confidence, match_reason = self._resolve_match_metadata(
            resolved_model,
            resolved_trim,
            resolved_jato_model,
            resolved_jato_trim,
            msrp_value,
            currency,
            payload,
            variant_fields,
        )
        match_status, match_reason = self._resolve_match_status(
            match_confidence,
            variant_fields,
            match_reason,
        )
        return RawObservation(
            official_model=resolved_model,
            official_trim=resolved_trim,
            msrp_value=msrp_value,
            currency=currency,
            tax_included=profile.default_tax_included,
            price_label=profile.default_price_label,
            source_url=source_url or profile.url,
            availability_text=availability_text,
            raw_payload=payload,
            jato_model=resolved_jato_model,
            jato_trim=resolved_jato_trim,
            jato_powertrain=variant_fields.jato_powertrain,
            official_edition=variant_fields.official_edition,
            official_powertrain=variant_fields.official_powertrain,
            match_confidence=match_confidence,
            match_status=match_status,
            match_reason=match_reason,
        )

    def _fetch_metadata(self, page: Any) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        raw_status = getattr(page, "status", None)
        if raw_status is None:
            raw_status = getattr(page, "status_code", None)
        try:
            metadata["httpStatus"] = int(raw_status)
        except (TypeError, ValueError):
            pass

        final_url = getattr(page, "url", None)
        if final_url:
            metadata["finalUrl"] = str(final_url)

        headers = getattr(page, "headers", None)
        if isinstance(headers, dict):
            content_type = headers.get("content-type") or headers.get("Content-Type")
            if content_type:
                metadata["contentType"] = str(content_type)

        return metadata

    def _page_text_sample(self, page: Any, *, limit: int = 2_000) -> str:
        parts: list[str] = []
        try:
            parts.extend(
                self._collect_text_result_values(page.css("body ::text"))
            )
        except Exception:
            pass
        if not parts:
            try:
                elements = page.css("body")
            except Exception:
                elements = []
            for element in elements[:1]:
                try:
                    parts.extend(
                        self._collect_text_result_values(element.css("::text"))
                    )
                except Exception:
                    pass
                if not parts:
                    try:
                        parts.extend(self._string_values(element.get()))
                    except Exception:
                        pass
        return _normalize_space(" ".join(parts))[:limit]

    def _access_denied_error(self, page: Any) -> str | None:
        sample = self._page_text_sample(page)
        lower = sample.lower()
        if not sample or "access denied" not in lower:
            return None
        if (
            "permission to access" not in lower
            and "errors.edgesuite.net" not in lower
            and "akamai" not in lower
        ):
            return None
        return f"anti_bot_access_denied: {sample[:240]}"

    def extract(self) -> list[RawObservation]:
        page = self._fetch()
        if page is None:
            error = self._last_fetch_error or "fetch_failed"
            self.record_audit_event(
                url=self.profile.url,
                attempted_strategies=[],
                winning_strategy=None,
                observations=[],
                tier=self.profile.tier or "http",
                error=error,
            )
            return []

        fetch_metadata = self._fetch_metadata(page)
        attempted: list[dict[str, Any]] = []
        p = self.profile
        results: list[RawObservation] = []
        access_error = self._access_denied_error(page)
        if access_error:
            self.record_audit_event(
                url=p.url,
                attempted_strategies=attempted,
                winning_strategy=None,
                observations=[],
                tier=p.tier or "http",
                error=access_error,
                extra=fetch_metadata or None,
            )
            return []

        # Strategy 1: attribute-embedded JSON (most reliable for React SPAs)
        if p.attr_json:
            results = self._extract_from_attr_json(page)
            attempted.append({
                "strategy": "attr_json",
                "status": "success" if results else "no_match",
                "observations_count": len(results),
            })
            if results:
                self.record_audit_event(
                    url=p.url,
                    attempted_strategies=attempted,
                    winning_strategy="attr_json",
                    observations=results,
                    tier=p.tier or "http",
                    extra=fetch_metadata or None,
                )
                return results
            log.warning(
                "Attribute-JSON extraction yielded nothing, "
                "trying next strategy"
            )

        # Strategy 2: ld+json script tags
        if p.json_script_selector:
            results = self._extract_from_json(page)
            attempted.append({
                "strategy": "json_script_selector",
                "status": "success" if results else "no_match",
                "observations_count": len(results),
            })
            if results:
                self.record_audit_event(
                    url=p.url,
                    attempted_strategies=attempted,
                    winning_strategy="json_script_selector",
                    observations=results,
                    tier=p.tier or "http",
                    extra=fetch_metadata or None,
                )
                return results
            log.warning(
                "JSON extraction yielded nothing, trying next strategy"
            )

        # Strategy 3: regex over selected text/script payloads
        if p.text_regex:
            results = self._extract_from_text_regex(page)
            attempted.append({
                "strategy": "text_regex",
                "status": "success" if results else "no_match",
                "observations_count": len(results),
            })
            if results:
                self.record_audit_event(
                    url=p.url,
                    attempted_strategies=attempted,
                    winning_strategy="text_regex",
                    observations=results,
                    tier=p.tier or "http",
                    extra=fetch_metadata or None,
                )
                return results
            log.warning(
                "Text-regex extraction yielded nothing, falling back to CSS"
            )

        # Strategy 4: CSS selectors on visible text
        if p.css:
            results = self._extract_from_css(page)
            attempted.append({
                "strategy": "css_selectors",
                "status": "success" if results else "no_match",
                "observations_count": len(results),
            })
            self.record_audit_event(
                url=p.url,
                attempted_strategies=attempted,
                winning_strategy="css_selectors" if results else None,
                observations=results,
                tier=p.tier or "http",
                extra=fetch_metadata or None,
            )
            return results

        log.error(
            "No extraction strategy configured for %s",
            self.config.source_code,
        )
        self.record_audit_event(
            url=p.url,
            attempted_strategies=attempted,
            winning_strategy=None,
            observations=[],
            tier=p.tier or "http",
            error="no_strategy_configured",
            extra=fetch_metadata or None,
        )
        return []

    def _fetch(self) -> Any | None:
        p = self.profile
        self._last_fetch_error = None
        browser_kwargs: dict[str, Any] = {
            "headless": p.headless,
            "network_idle": p.network_idle,
        }
        if p.load_dom is not None:
            browser_kwargs["load_dom"] = p.load_dom
        if p.timeout_ms is not None:
            browser_kwargs["timeout"] = p.timeout_ms
        if p.wait_ms is not None:
            browser_kwargs["wait"] = p.wait_ms
        if p.disable_resources is not None:
            browser_kwargs["disable_resources"] = p.disable_resources
        if p.retries is not None:
            browser_kwargs["retries"] = p.retries
        if p.retry_delay_seconds is not None:
            browser_kwargs["retry_delay"] = p.retry_delay_seconds
        try:
            if p.tier == "stealth":
                from scrapling.fetchers import StealthyFetcher

                return StealthyFetcher.fetch(
                    p.url,
                    **browser_kwargs,
                    solve_cloudflare=p.solve_cloudflare,
                )
            if p.tier == "dynamic":
                from scrapling.fetchers import DynamicFetcher

                return DynamicFetcher.fetch(
                    p.url,
                    **browser_kwargs,
                )
            from scrapling.fetchers import Fetcher

            return Fetcher.get(
                p.url,
                stealthy_headers=True,
                impersonate=p.impersonate,
            )
        except Exception as exc:
            log.error(
                "Scrapling fetch failed [tier=%s] for %s: %s",
                p.tier,
                self.config.source_code,
                exc,
            )
            self._last_fetch_error = f"{type(exc).__name__}: {exc}".strip()
            return None

    def _text_regex_source_text(self, page: Any) -> str:
        mapping = self.profile.text_regex
        if mapping is None:
            return ""
        selector = (mapping.source_selector or "body").strip() or "body"
        elements = page.css(selector)
        if not elements:
            log.warning(
                "No elements found for text_regex selector '%s'",
                selector,
            )
            return ""

        parts: list[str] = []
        descendant_selector = f"{selector} ::text"
        try:
            parts.extend(
                self._collect_text_result_values(page.css(descendant_selector))
            )
        except Exception:
            pass
        for element in elements:
            try:
                text_result = element.css("::text")
            except Exception:
                text_result = None
            text_parts = self._collect_text_result_values(text_result)
            if not text_parts or mapping.include_element_html:
                try:
                    text_parts.extend(self._string_values(element.get()))
                except Exception:
                    pass
            parts.extend(text_parts)
        return "\n".join(parts)

    def _extract_from_text_regex(self, page: Any) -> list[RawObservation]:
        mapping = self.profile.text_regex
        if mapping is None:
            return []

        source_text = self._text_regex_source_text(page)
        if not source_text:
            return []

        p = self.profile
        results: list[RawObservation] = []
        seen: set[tuple[str, str, float]] = set()
        for entry in mapping.entry_patterns:
            try:
                matches = re.finditer(
                    entry.pattern,
                    source_text,
                    flags=re.IGNORECASE | re.DOTALL,
                )
            except re.error as exc:
                log.warning(
                    "Invalid text_regex pattern for %s: %s",
                    self.config.source_code,
                    exc,
                )
                continue

            for match in matches:
                group_values = match.groupdict()
                price_raw = str(group_values.get("price") or "").strip()
                if not price_raw:
                    log.warning(
                        "Skipping text_regex match without named price group "
                        "for %s",
                        self.config.source_code,
                    )
                    continue
                msrp = parse_price(price_raw)
                if msrp is None:
                    log.warning("Cannot parse price from '%s'", price_raw)
                    continue

                model_text = _normalize_space(
                    str(group_values.get("model") or p.fixed_model or "")
                )
                trim_text = _normalize_space(
                    str(
                        entry.official_trim
                        or group_values.get("trim")
                        or ""
                    )
                )
                powertrain_text = _normalize_space(
                    str(
                        entry.official_powertrain
                        or group_values.get("powertrain")
                        or ""
                    )
                )
                availability = _normalize_space(
                    str(
                        entry.availability_text
                        or group_values.get("availability")
                        or ""
                    )
                ) or None
                if not (model_text or p.fixed_model) or not trim_text:
                    continue

                dedupe_key = (
                    (model_text or p.fixed_model or "").lower(),
                    trim_text.lower(),
                    msrp,
                )
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                observation = self._build_observation(
                    official_model=model_text,
                    official_trim=trim_text,
                    msrp_value=msrp,
                    currency=p.default_currency,
                    availability_text=availability,
                    source_url=p.url,
                    raw_payload={
                        "priceText": price_raw,
                        "trimText": trim_text,
                        "powertrain": powertrain_text,
                        "officialEdition": entry.official_edition,
                        "sourcePriceValue": msrp,
                        "regexPattern": entry.pattern,
                        "regexSelector": mapping.source_selector,
                        "regexGroups": {
                            key: value
                            for key, value in group_values.items()
                            if value not in (None, "")
                        },
                    },
                )
                if observation is not None:
                    if entry.price_label:
                        observation.price_label = entry.price_label
                    results.append(observation)

        return results

    def _extract_from_attr_json(self, page: Any) -> list[RawObservation]:
        """Extract vehicle data from HTML attribute JSON payloads."""
        aj = self.profile.attr_json
        p = self.profile
        containers = page.css(aj.vehicle_container)
        if not containers:
            log.warning(
                "No elements found for attr_json container '%s'",
                aj.vehicle_container,
            )
            return []

        results: list[RawObservation] = []
        for el in containers:
            attrs = el.attrib if hasattr(el, 'attrib') else {}
            if not attrs:
                continue

            # Parse filter JSON (contains price)
            filter_raw = (
                attrs.get(aj.filter_attr, "{}")
                if aj.filter_attr
                else "{}"
            )
            tracking_raw = (
                attrs.get(aj.tracking_attr, "{}")
                if aj.tracking_attr
                else "{}"
            )

            try:
                filter_data = json.loads(filter_raw) if filter_raw else {}
            except (json.JSONDecodeError, TypeError):
                filter_data = {}
            try:
                tracking_data = (
                    json.loads(tracking_raw) if tracking_raw else {}
                )
            except (json.JSONDecodeError, TypeError):
                tracking_data = {}

            # If tracking_attr is on a sub-element, try to find it
            if not tracking_data and aj.tracking_attr:
                sub = el.css(f'[{aj.tracking_attr}]')
                if sub:
                    raw = (
                        sub[0].attrib.get(aj.tracking_attr, "{}")
                        if hasattr(sub[0], "attrib")
                        else "{}"
                    )
                    try:
                        tracking_data = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        pass

            # Extract fields
            name = str(tracking_data.get(aj.name_key, "")).strip()
            price_raw = filter_data.get(aj.price_key)
            if not name or price_raw is None:
                continue

            try:
                msrp = float(price_raw)
            except (ValueError, TypeError):
                msrp_parsed = parse_price(str(price_raw))
                if msrp_parsed is None:
                    continue
                msrp = msrp_parsed

            # Build model / trim from name
            # e.g. "BMW iX xDrive45" → model="iX", trim="xDrive45"
            series_text = (
                el.css(".cmp-allmodelscarddetail__series::text").get()
                or ""
            )
            series_text = series_text.strip()
            trim_text = (
                name.replace("BMW ", "").replace(series_text, "").strip()
                if series_text
                else name
            )

            fuel_type = str(filter_data.get(aj.fuel_key, ""))
            category = str(filter_data.get(aj.category_key, ""))

            observation = self._build_observation(
                official_model=series_text or name,
                official_trim=trim_text,
                msrp_value=msrp,
                currency=p.default_currency,
                source_url=p.url,
                raw_payload={
                    "filter": filter_data,
                    "tracking": tracking_data,
                    "fuel_type": fuel_type,
                    "category": category,
                    "trimText": trim_text,
                    "sourcePriceValue": msrp,
                },
            )
            if observation is not None:
                results.append(observation)
        return results

    # ld+json @type values that may carry vehicle/product pricing data
    _VEHICLE_JSON_TYPES = frozenset({
        "Product", "Car", "Vehicle", "Offer", "AggregateOffer",
        "IndividualProduct", "ProductModel",
        "schema:Product", "schema:Car", "schema:Vehicle",
        "schema:Offer", "schema:AggregateOffer",
        "schema:IndividualProduct", "schema:ProductModel",
    })

    _JSON_FALLBACK_KEYS = (
        "name",
        "schema:name",
        "model",
        "schema:model",
        "description",
        "schema:description",
        "priceCurrency",
        "schema:priceCurrency",
    )

    _JSON_OFFER_KEYS = ("offers", "schema:offers")
    _JSON_PRICE_KEYS = (
        "price",
        "lowPrice",
        "highPrice",
        "schema:price",
        "schema:lowPrice",
        "schema:highPrice",
    )
    _JSON_CURRENCY_KEYS = ("priceCurrency", "schema:priceCurrency")
    _JSON_PRICE_SPEC_KEYS = ("priceSpecification", "schema:priceSpecification")

    def _json_first_non_empty(
        self,
        payload: dict[str, Any],
        keys: tuple[str, ...],
    ) -> Any | None:
        for key in keys:
            value = payload.get(key)
            if value not in (None, "", [], {}):
                return value
        return None

    def _resolve_json_path(
        self,
        payload: Any,
        path: str,
    ) -> Any | None:
        parts = path.split(".")

        def _walk(current: Any, index: int) -> Any | None:
            if index >= len(parts):
                return current
            key = parts[index]
            if isinstance(current, dict):
                if key in current:
                    next_value = current.get(key)
                    if isinstance(next_value, list):
                        inherited_fields = {
                            fallback_key: current.get(fallback_key)
                            for fallback_key in self._JSON_FALLBACK_KEYS
                            if current.get(fallback_key) not in (None, "", [], {})
                        }
                        if inherited_fields:
                            next_value = [
                                (
                                    {**inherited_fields, **item}
                                    if isinstance(item, dict)
                                    else item
                                )
                                for item in next_value
                            ]
                    return _walk(next_value, index + 1)
                graph = current.get("@graph")
                if isinstance(graph, list):
                    return _walk(graph, index)
                return None
            if isinstance(current, list):
                if key.isdigit():
                    resolved_index = int(key)
                    if resolved_index >= len(current):
                        return None
                    return _walk(current[resolved_index], index + 1)
                collected: list[Any] = []
                for item in current:
                    resolved = _walk(item, index)
                    if resolved is None:
                        continue
                    if isinstance(resolved, list):
                        collected.extend(resolved)
                    else:
                        collected.append(resolved)
                return collected or None
            return None

        return _walk(payload, 0)

    def _json_type_set(self, payload: dict[str, Any]) -> set[str]:
        json_type = payload.get("@type", "")
        if isinstance(json_type, list):
            return {str(item) for item in json_type if item}
        if json_type:
            return {str(json_type)}
        return set()

    def _is_untyped_price_candidate(
        self,
        payload: dict[str, Any],
        inherited: dict[str, Any],
    ) -> bool:
        has_direct_price = (
            self._json_first_non_empty(payload, self._JSON_PRICE_KEYS)
            is not None
        )
        offers = self._json_first_non_empty(payload, self._JSON_OFFER_KEYS)
        has_offer_price = False
        if isinstance(offers, dict):
            price_spec = self._json_first_non_empty(
                offers,
                self._JSON_PRICE_SPEC_KEYS,
            )
            has_offer_price = (
                self._json_first_non_empty(offers, self._JSON_PRICE_KEYS)
                is not None
                or (
                    isinstance(price_spec, dict)
                    and self._json_first_non_empty(
                        price_spec,
                        self._JSON_PRICE_KEYS,
                    )
                    is not None
                )
            )
        has_identity = any(
            payload.get(key) not in (None, "", [], {})
            or inherited.get(key) not in (None, "", [], {})
            for key in self._JSON_FALLBACK_KEYS
            if key not in self._JSON_CURRENCY_KEYS
        )
        return has_identity and (has_direct_price or has_offer_price)

    def _collect_json_vehicle_candidates(
        self,
        payload: Any,
        *,
        inherited: dict[str, Any] | None = None,
        allow_untyped: bool = False,
    ) -> list[dict[str, Any]]:
        inherited_fields = dict(inherited or {})
        if isinstance(payload, list):
            results: list[dict[str, Any]] = []
            for item in payload:
                results.extend(
                    self._collect_json_vehicle_candidates(
                        item,
                        inherited=inherited_fields,
                        allow_untyped=allow_untyped,
                    )
                )
            return results
        if not isinstance(payload, dict):
            return []

        current_inherited = dict(inherited_fields)
        for key in self._JSON_FALLBACK_KEYS:
            value = payload.get(key)
            if value not in (None, "", [], {}):
                current_inherited.setdefault(key, value)

        type_set = self._json_type_set(payload)
        typed_vehicle = bool(type_set & self._VEHICLE_JSON_TYPES)
        should_emit = False
        if typed_vehicle:
            should_emit = self._is_untyped_price_candidate(
                payload,
                current_inherited,
            )
        elif allow_untyped:
            should_emit = self._is_untyped_price_candidate(
                payload,
                current_inherited,
            )

        results: list[dict[str, Any]] = []
        if should_emit:
            candidate = deepcopy(payload)
            for key, value in current_inherited.items():
                candidate.setdefault(key, value)
            results.append(candidate)

        offers = self._json_first_non_empty(payload, self._JSON_OFFER_KEYS)
        if isinstance(offers, list):
            parent_fields = {
                key: payload.get(key, current_inherited.get(key))
                for key in self._JSON_FALLBACK_KEYS
            }
            for offer in offers:
                if not isinstance(offer, dict):
                    continue
                results.extend(
                    self._collect_json_vehicle_candidates(
                        offer,
                        inherited=parent_fields,
                        allow_untyped=(allow_untyped or typed_vehicle),
                    )
                )

        for key, value in payload.items():
            if key in self._JSON_OFFER_KEYS:
                continue
            if isinstance(value, (dict, list)):
                results.extend(
                    self._collect_json_vehicle_candidates(
                        value,
                        inherited=current_inherited,
                        allow_untyped=allow_untyped,
                    )
                )
        return results

    def _extract_from_json(self, page: Any) -> list[RawObservation]:
        p = self.profile
        scripts = page.css(p.json_script_selector)
        results: list[RawObservation] = []
        seen_payloads: set[str] = set()
        for script in scripts:
            text = script.css("::text").get()
            if not text:
                continue
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                continue
            vehicles = data
            allow_untyped = False
            if p.json_vehicles_path:
                vehicles = self._resolve_json_path(data, p.json_vehicles_path)
                allow_untyped = True
            candidates = self._collect_json_vehicle_candidates(
                vehicles,
                allow_untyped=allow_untyped,
            )
            for vehicle in candidates:
                marker = json.dumps(
                    vehicle,
                    sort_keys=True,
                    ensure_ascii=False,
                    default=str,
                )
                if marker in seen_payloads:
                    continue
                seen_payloads.add(marker)
                obs = self._map_json_vehicle(vehicle)
                if obs:
                    results.append(obs)
        return results

    def _map_json_vehicle(self, vehicle: dict) -> RawObservation | None:
        p = self.profile
        try:
            model = str(
                self._json_first_non_empty(
                    vehicle,
                    ("name", "schema:name", "model", "schema:model"),
                )
                or ""
            )
            trim = str(
                self._json_first_non_empty(
                    vehicle,
                    (
                        "trim",
                        "schema:trim",
                        "description",
                        "schema:description",
                        "model",
                        "schema:model",
                    ),
                )
                or ""
            )
            # Fallback: use model name as trim when no trim info available
            if not trim.strip():
                trim = model
            offers = self._json_first_non_empty(
                vehicle,
                self._JSON_OFFER_KEYS,
            ) or {}
            if isinstance(offers, list):
                offers = next(
                    (
                        item
                        for item in offers
                        if isinstance(item, dict)
                        and self._json_first_non_empty(
                            item,
                            self._JSON_PRICE_KEYS,
                        )
                        not in (None, "")
                    ),
                    {},
                )
            if not isinstance(offers, dict):
                offers = {}
            price_spec = self._json_first_non_empty(
                offers,
                self._JSON_PRICE_SPEC_KEYS,
            )
            price_raw = self._json_first_non_empty(
                vehicle,
                self._JSON_PRICE_KEYS,
            )
            if price_raw is None:
                price_raw = self._json_first_non_empty(
                    offers,
                    self._JSON_PRICE_KEYS,
                )
            if price_raw is None and isinstance(price_spec, dict):
                price_raw = self._json_first_non_empty(
                    price_spec,
                    self._JSON_PRICE_KEYS,
                )
            currency = (
                self._json_first_non_empty(vehicle, self._JSON_CURRENCY_KEYS)
                or self._json_first_non_empty(offers, self._JSON_CURRENCY_KEYS)
            )
            if currency is None and isinstance(price_spec, dict):
                currency = self._json_first_non_empty(
                    price_spec,
                    self._JSON_CURRENCY_KEYS,
                )
            currency = str(currency or p.default_currency)
            msrp_value = float(price_raw)
            source_url = str(
                self._json_first_non_empty(
                    offers,
                    ("url", "schema:url"),
                )
                or self._json_first_non_empty(
                    vehicle,
                    ("url", "schema:url"),
                )
                or p.url
            )
            return self._build_observation(
                official_model=model,
                official_trim=trim,
                msrp_value=msrp_value,
                currency=currency,
                source_url=source_url,
                raw_payload={
                    **vehicle,
                    "trimText": trim,
                    "sourcePriceValue": msrp_value,
                },
            )
        except (TypeError, ValueError, KeyError) as exc:
            log.warning("Skipping JSON vehicle: %s", exc)
            return None

    def _extract_from_css(self, page: Any) -> list[RawObservation]:
        css = self.profile.css
        p = self.profile
        containers = page.css(css.vehicle_container)
        if not containers:
            log.warning("No elements found for '%s'", css.vehicle_container)
            return []
        results: list[RawObservation] = []
        for el in containers:
            if css.exclude_if_selector and el.css(css.exclude_if_selector):
                continue
            model_raw = el.css(css.model).get() if css.model else ""
            model_text = _HTML_TAG_RE.sub("", model_raw).strip() if model_raw else ""
            trim_raw = el.css(css.trim).get() if css.trim else ""
            trim_text = _HTML_TAG_RE.sub("", trim_raw).strip() if trim_raw else ""
            # Fallback: use model (or fixed_model) as trim when no trim info
            if not (trim_text or "").strip():
                trim_text = model_text or p.fixed_model or ""
            price_raw = el.css(css.price).get() if css.price else ""
            price_text = _HTML_TAG_RE.sub("", price_raw).strip() if price_raw else ""
            stripped_price_text = (price_text or "").strip()
            if css.include_if_text_contains:
                container_search_text = _normalize_space(
                    " ".join(
                        part
                        for part in (
                            model_text,
                            trim_text,
                            stripped_price_text,
                        )
                        if part
                    )
                ).lower()
                if not any(
                    keyword.lower() in container_search_text
                    for keyword in css.include_if_text_contains
                ):
                    continue
            if any(
                stripped_price_text.startswith(prefix)
                for prefix in p.exclude_price_prefixes
            ):
                continue
            if not (p.fixed_model or model_text) or not price_text:
                continue
            msrp = parse_price(price_text)
            if msrp is None:
                log.warning("Cannot parse price from '%s'", price_text)
                continue
            currency = p.default_currency
            if css.currency:
                cur_text = el.css(css.currency).get()
                if cur_text:
                    currency = cur_text.strip()
            availability = None
            if css.availability:
                availability = el.css(css.availability).get()
            observation = self._build_observation(
                official_model=model_text,
                official_trim=trim_text or "",
                msrp_value=msrp,
                currency=currency,
                availability_text=availability,
                source_url=p.url,
                raw_payload={
                    "priceText": stripped_price_text,
                    "trimText": trim_text or "",
                    "sourcePriceValue": msrp,
                },
            )
            if observation is not None:
                results.append(observation)
        return results
