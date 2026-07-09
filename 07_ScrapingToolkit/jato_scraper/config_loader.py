"""Load source definitions from YAML config files.

This module reads ``sources/*.yaml`` and registers extractors into the
global registry automatically.  Adding a new brand or country only
requires creating a new YAML file — no Python code changes needed.

Each YAML file describes **one source** and must contain:
    - source_code, country, brand, source_url   (identity)
    - extractor_type: "http_json" | "http_text" | "scrapling" | "playwright" | "pdf_text"
    - profile: <dict>                            (extractor-specific config)

Optional source fields:
    - profile_preset: relative/absolute YAML path merged into profile first
"""

from __future__ import annotations

from copy import deepcopy
import logging
from pathlib import Path
from typing import Any

import yaml

from jato_scraper import registry
from jato_scraper.base import ExtractorConfig
from jato_scraper.extractors.http_json import (
    FieldMapping,
    HttpJsonExtractor,
    HttpJsonProfile,
    LookupMapping,
    MinPriceGroup,
    PricingContextMapping as HttpPricingContextMapping,
    ValueFilter,
)
from jato_scraper.extractors.http_text import (
    HttpTextEntryPattern,
    HttpTextExtractor,
    HttpTextProfile,
)
from jato_scraper.extractors.scrapling_web import (
    AttrJsonMapping,
    CssMapping,
    PricingContextMapping as ScraplingPricingContextMapping,
    ScraplingExtractor,
    ScraplingProfile,
    TextRegexEntryPattern,
    TextRegexMapping,
)
from jato_scraper.extractors.playwright_card_flow import (
    PlaywrightCardFlowExtractor,
    PlaywrightCardFlowProfile,
)
from jato_scraper.extractors.pdf_text import (
    PdfTextEntryPattern,
    PdfTextExtractor,
    PdfTextProfile,
)

log = logging.getLogger(__name__)

SOURCES_DIR = Path(__file__).resolve().parent.parent / "sources"


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"{path} did not parse into a YAML mapping")
    return data


def _deep_merge_dicts(
    base: dict[str, Any],
    overrides: dict[str, Any],
) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in overrides.items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dicts(current, value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _resolve_profile_raw(
    source_path: Path,
    data: dict[str, Any],
) -> dict[str, Any]:
    profile_raw = data["profile"]
    if not isinstance(profile_raw, dict):
        raise ValueError(f"{source_path} profile must be a mapping")

    preset_ref = data.get("profile_preset")
    if not preset_ref:
        return profile_raw

    preset_path = Path(str(preset_ref))
    if not preset_path.is_absolute():
        preset_path = (source_path.parent / preset_path).resolve()
    preset_data = _load_yaml_mapping(preset_path)
    preset_profile = preset_data.get("profile", preset_data)
    if not isinstance(preset_profile, dict):
        raise ValueError(f"{preset_path} preset profile must be a mapping")
    return _deep_merge_dicts(preset_profile, profile_raw)


def _build_extractor_config(data: dict[str, Any]) -> ExtractorConfig:
    return ExtractorConfig(
        source_code=data["source_code"],
        country=data["country"],
        brand=data["brand"],
        source_url=data["source_url"],
        source_type=data.get("source_type", "manufacturer_official"),
        price_semantics=data.get("price_semantics", "base_msrp"),
        requires_location=data.get("requires_location", False),
    )


def _build_http_json_profile(profile: dict[str, Any]) -> HttpJsonProfile:
    fm_raw = profile.get("field_mapping", {})

    def _path_field(
        raw: Any,
        default: str,
        *,
        allow_list: bool = False,
    ) -> str | LookupMapping | tuple[str | LookupMapping, ...]:
        def _one(value: Any) -> str | LookupMapping | None:
            if isinstance(value, dict):
                source_path = str(value.get("source_path", "")).strip()
                collection_path = str(value.get("collection_path", "")).strip()
                if not source_path or not collection_path:
                    return None
                return LookupMapping(
                    source_path=source_path,
                    collection_path=collection_path,
                    key_path=(
                        str(value.get("key_path", "id")).strip() or "id"
                    ),
                    value_path=(
                        str(value.get("value_path", "name")).strip()
                        or "name"
                    ),
                )
            text = str(value).strip()
            return text or None

        if allow_list and isinstance(raw, list):
            return tuple(
                value
                for value in (_one(item) for item in raw)
                if value is not None
            )
        if isinstance(raw, dict):
            value = _one(raw)
            return value if value is not None else default
        if raw is None:
            return default
        text = str(raw).strip()
        return text or default

    fm = FieldMapping(
        model=_path_field(fm_raw.get("model"), "model", allow_list=True),
        trim=_path_field(fm_raw.get("trim"), "trim", allow_list=True),
        price=str(fm_raw.get("price", "price")).strip() or "price",
        currency=str(fm_raw.get("currency", "currency")).strip() or "currency",
        tax_included=(
            str(fm_raw.get("tax_included", "taxIncluded")).strip()
            or "taxIncluded"
        ),
        price_label=(
            str(fm_raw.get("price_label", "priceLabel")).strip()
            or "priceLabel"
        ),
        availability=(
            str(fm_raw["availability"]).strip()
            if fm_raw.get("availability") is not None
            else None
        ),
        vehicles_path=str(fm_raw.get("vehicles_path", "models")).strip() or "models",
        items_path=(
            str(fm_raw["items_path"]).strip()
            if fm_raw.get("items_path") is not None
            else None
        ),
    )

    filters_raw = profile.get("filters", [])
    filters: list[ValueFilter] = []
    if filters_raw:
        if not isinstance(filters_raw, list):
            raise ValueError("http_json filters must be a list")
        for item in filters_raw:
            if not isinstance(item, dict):
                raise ValueError("http_json filter entries must be mappings")
            path = str(item.get("path", "")).strip()
            if not path:
                raise ValueError("http_json filter entries require a path")
            equals_raw = item.get("equals", item.get("in", []))
            if isinstance(equals_raw, list):
                equals = tuple(
                    str(value).strip()
                    for value in equals_raw
                    if str(value).strip()
                )
            elif equals_raw is None:
                equals = ()
            else:
                equals = (str(equals_raw).strip(),)
            filters.append(ValueFilter(path=path, equals=equals))

    min_price_group = None
    min_price_group_raw = profile.get(
        "min_price_group",
        profile.get("group_min_price"),
    )
    if min_price_group_raw:
        if not isinstance(min_price_group_raw, dict):
            raise ValueError("http_json min_price_group must be a mapping")
        key_raw = min_price_group_raw.get("key")
        key = _path_field(key_raw, "", allow_list=True)
        if not key:
            raise ValueError("http_json min_price_group requires a key")
        price = (
            str(min_price_group_raw.get("price", fm.price)).strip()
            or fm.price
        )
        min_price_group = MinPriceGroup(key=key, price=price)

    return HttpJsonProfile(
        url=profile["url"],
        method=profile.get("method", "GET"),
        headers=profile.get("headers", {}),
        params=profile.get("params", {}),
        body=profile.get("body"),
        field_mapping=fm,
        default_currency=profile.get("default_currency", "EUR"),
        default_tax_included=profile.get("default_tax_included", True),
        default_price_label=profile.get(
            "default_price_label",
            "Manufacturer's Recommended Retail Price",
        ),
        fixed_model=profile.get("fixed_model"),
        fixed_official_powertrain=profile.get("fixed_official_powertrain"),
        fixed_jato_model=profile.get("fixed_jato_model"),
        fixed_jato_powertrain=profile.get("fixed_jato_powertrain"),
        copy_trim_to_jato_trim=bool(profile.get("copy_trim_to_jato_trim", False)),
        match_confidence=(
            float(profile["match_confidence"])
            if profile.get("match_confidence") is not None
            else None
        ),
        match_status=profile.get("match_status", "review_required"),
        match_reason=profile.get("match_reason"),
        filters=tuple(filters),
        min_price_group=min_price_group,
        pricing_context=_build_pricing_context_mapping(
            profile,
            mapping_cls=HttpPricingContextMapping,
            owner="http_json",
        ),
    )


def _build_pricing_context_mapping(
    profile: dict[str, Any],
    *,
    mapping_cls: type[HttpPricingContextMapping] | type[ScraplingPricingContextMapping],
    owner: str,
) -> HttpPricingContextMapping | ScraplingPricingContextMapping | None:
    pricing_context_raw = profile.get("pricing_context")
    if not pricing_context_raw:
        return None
    if not isinstance(pricing_context_raw, dict):
        raise ValueError(f"{owner} pricing_context must be a mapping")
    fields_raw = pricing_context_raw.get("fields", {})
    constants_raw = pricing_context_raw.get("constants", {})
    if not isinstance(fields_raw, dict):
        raise ValueError(f"{owner} pricing_context fields must be a mapping")
    if not isinstance(constants_raw, dict):
        raise ValueError(f"{owner} pricing_context constants must be a mapping")
    return mapping_cls(
        fields={
            str(key).strip(): str(value).strip()
            for key, value in fields_raw.items()
            if str(key).strip() and str(value).strip()
        },
        constants={
            str(key).strip(): value
            for key, value in constants_raw.items()
            if str(key).strip()
        },
    )


def _build_scrapling_profile(profile: dict[str, Any]) -> ScraplingProfile:
    css_raw = profile.get("css")
    css = None
    if css_raw:
        include_if_text_contains_raw = css_raw.get(
            "include_if_text_contains",
            [],
        )
        if isinstance(include_if_text_contains_raw, str):
            include_if_text_contains = (
                include_if_text_contains_raw.strip(),
            ) if include_if_text_contains_raw.strip() else ()
        elif isinstance(include_if_text_contains_raw, list):
            include_if_text_contains = tuple(
                str(value).strip()
                for value in include_if_text_contains_raw
                if str(value).strip()
            )
        else:
            include_if_text_contains = ()
        css = CssMapping(
            vehicle_container=css_raw["vehicle_container"],
            model=css_raw.get("model", ""),
            trim=css_raw.get("trim", ""),
            price=css_raw.get("price", ""),
            currency=css_raw.get("currency"),
            availability=css_raw.get("availability"),
            exclude_if_selector=css_raw.get("exclude_if_selector"),
            include_if_text_contains=include_if_text_contains,
        )

    aj_raw = profile.get("attr_json")
    attr_json = None
    if aj_raw:
        attr_json = AttrJsonMapping(
            vehicle_container=aj_raw["vehicle_container"],
            filter_attr=aj_raw.get("filter_attr", ""),
            tracking_attr=aj_raw.get("tracking_attr", ""),
            price_key=aj_raw.get("price_key", "price"),
            fuel_key=aj_raw.get("fuel_key", "fuelType"),
            category_key=aj_raw.get("category_key", "category"),
            series_key=aj_raw.get("series_key", "series"),
            name_key=aj_raw.get("name_key", "name"),
            range_key=aj_raw.get("range_key", "range"),
        )

    text_regex_raw = profile.get("text_regex")
    text_regex = None
    if text_regex_raw:
        patterns_raw = text_regex_raw.get("entry_patterns", [])
        if not isinstance(patterns_raw, list):
            raise ValueError("scrapling text_regex entry_patterns must be a list")
        entry_patterns = tuple(
            TextRegexEntryPattern(
                pattern=str(item.get("pattern", "")).strip(),
                official_trim=(
                    str(item["official_trim"]).strip()
                    if item.get("official_trim") is not None
                    else None
                ),
                official_powertrain=(
                    str(item["official_powertrain"]).strip()
                    if item.get("official_powertrain") is not None
                    else None
                ),
                official_edition=(
                    str(item["official_edition"]).strip()
                    if item.get("official_edition") is not None
                    else None
                ),
                availability_text=(
                    str(item["availability_text"]).strip()
                    if item.get("availability_text") is not None
                    else None
                ),
                price_label=(
                    str(item["price_label"]).strip()
                    if item.get("price_label") is not None
                    else None
                ),
            )
            for item in patterns_raw
            if isinstance(item, dict)
            and str(item.get("pattern", "")).strip()
        )
        text_regex = TextRegexMapping(
            source_selector=str(
                text_regex_raw.get("source_selector", "body")
            ).strip() or "body",
            entry_patterns=entry_patterns,
            include_element_html=bool(
                text_regex_raw.get("include_element_html", False)
            ),
        )

    model_rules_raw = profile.get("model_rules")
    model_rules = None
    if isinstance(model_rules_raw, list):
        model_rules = tuple(
            rule for rule in model_rules_raw if isinstance(rule, dict)
        )

    return ScraplingProfile(
        url=profile["url"],
        tier=profile.get("tier", "http"),
        css=css,
        attr_json=attr_json,
        text_regex=text_regex,
        pricing_context=_build_pricing_context_mapping(
            profile,
            mapping_cls=ScraplingPricingContextMapping,
            owner="scrapling",
        ),
        json_script_selector=profile.get("json_script_selector"),
        json_vehicles_path=profile.get("json_vehicles_path"),
        headless=profile.get("headless", True),
        network_idle=profile.get("network_idle", True),
        load_dom=(
            bool(profile["load_dom"])
            if profile.get("load_dom") is not None
            else None
        ),
        timeout_ms=(
            int(profile["timeout_ms"])
            if profile.get("timeout_ms") is not None
            else None
        ),
        wait_ms=(
            int(profile["wait_ms"])
            if profile.get("wait_ms") is not None
            else None
        ),
        disable_resources=(
            bool(profile["disable_resources"])
            if profile.get("disable_resources") is not None
            else None
        ),
        retries=(
            int(profile["retries"])
            if profile.get("retries") is not None
            else None
        ),
        retry_delay_seconds=(
            float(profile["retry_delay_seconds"])
            if profile.get("retry_delay_seconds") is not None
            else None
        ),
        impersonate=profile.get("impersonate", "chrome"),
        solve_cloudflare=profile.get("solve_cloudflare", False),
        default_currency=profile.get("default_currency", "EUR"),
        default_tax_included=profile.get("default_tax_included", True),
        default_price_label=profile.get(
            "default_price_label",
            "Manufacturer's Recommended Retail Price",
        ),
        fixed_model=profile.get("fixed_model"),
        fixed_jato_model=profile.get("fixed_jato_model"),
        fixed_jato_powertrain=profile.get("fixed_jato_powertrain"),
        copy_trim_to_jato_trim=profile.get("copy_trim_to_jato_trim", False),
        match_confidence=(
            float(profile["match_confidence"])
            if profile.get("match_confidence") is not None
            else None
        ),
        match_status=profile.get("match_status", "review_required"),
        match_reason=profile.get("match_reason"),
        exclude_price_prefixes=tuple(
            profile.get("exclude_price_prefixes", [])
        ),
        confidence_rules=profile.get("confidence_rules"),
        structured_fields=profile.get("structured_fields"),
        auto_accept_gates=profile.get("auto_accept_gates"),
        model_rules=model_rules,
        skip_if_model_unmapped=bool(
            profile.get("skip_if_model_unmapped", False)
        ),
    )


def _build_playwright_profile(
    profile: dict[str, Any],
) -> PlaywrightCardFlowProfile:
    startup_dismiss_raw = profile.get("startup_dismiss_selectors", [])
    if isinstance(startup_dismiss_raw, str):
        startup_dismiss_selectors = (startup_dismiss_raw,)
    elif isinstance(startup_dismiss_raw, list):
        startup_dismiss_selectors = tuple(
            str(selector).strip()
            for selector in startup_dismiss_raw
            if str(selector).strip()
        )
    else:
        startup_dismiss_selectors = ()

    return PlaywrightCardFlowProfile(
        url=profile["url"],
        browser=profile.get("browser", "chromium"),
        headless=bool(profile.get("headless", True)),
        wait_until=profile.get("wait_until", "domcontentloaded"),
        page_timeout_ms=int(profile.get("page_timeout_ms", 120000)),
        viewport_width=int(profile.get("viewport_width", 1440)),
        viewport_height=int(profile.get("viewport_height", 1200)),
        locale=profile.get("locale"),
        timezone_id=profile.get("timezone_id"),
        initial_ready_selector=profile.get("initial_ready_selector", ""),
        initial_ready_timeout_ms=int(
            profile.get("initial_ready_timeout_ms", 60000)
        ),
        startup_dismiss_selectors=startup_dismiss_selectors,
        cookie_banner_selector=profile.get("cookie_banner_selector"),
        cookie_reject_selector=profile.get("cookie_reject_selector"),
        cookie_reject_text=profile.get("cookie_reject_text"),
        trim_card_selector=profile.get("trim_card_selector", ""),
        trim_name_selector=profile.get("trim_name_selector", "h3"),
        trim_model_selector=profile.get("trim_model_selector"),
        trim_card_wait_ms=int(profile.get("trim_card_wait_ms", 1200)),
        trim_price_ready_timeout_ms=int(
            profile.get("trim_price_ready_timeout_ms", 10000)
        ),
        next_step_selector=profile.get("next_step_selector", ""),
        detail_ready_selector=profile.get("detail_ready_selector"),
        detail_card_selector=profile.get("detail_card_selector", ""),
        detail_name_selector=profile.get("detail_name_selector"),
        detail_price_selector=profile.get("detail_price_selector"),
        detail_card_wait_ms=int(profile.get("detail_card_wait_ms", 1500)),
        powertrain_line_count=int(profile.get("powertrain_line_count", 2)),
        extract_from_trim_cards=bool(
            profile.get("extract_from_trim_cards", False)
        ),
        combine_trim_and_powertrain=bool(
            profile.get("combine_trim_and_powertrain", True)
        ),
        combined_trim_separator=profile.get(
            "combined_trim_separator",
            " | ",
        ),
        default_currency=profile.get("default_currency", "EUR"),
        default_tax_included=bool(
            profile.get("default_tax_included", True)
        ),
        default_price_label=profile.get(
            "default_price_label",
            "Manufacturer's Recommended Retail Price",
        ),
        fixed_trim=profile.get("fixed_trim"),
        fixed_model=profile.get("fixed_model"),
        fixed_jato_model=profile.get("fixed_jato_model"),
        fixed_jato_powertrain=profile.get("fixed_jato_powertrain"),
        match_confidence=(
            float(profile["match_confidence"])
            if profile.get("match_confidence") is not None
            else None
        ),
        match_status=profile.get("match_status", "review_required"),
        match_reason=profile.get("match_reason"),
        structured_fields=profile.get("structured_fields"),
    )


def _build_pdf_text_profile(profile: dict[str, Any]) -> PdfTextProfile:
    patterns_raw = profile.get("entry_patterns", [])
    if not isinstance(patterns_raw, list):
        raise ValueError("pdf_text entry_patterns must be a list")
    browser_download_fallback = bool(profile.get("browser_download_fallback", False))
    curl_download_fallback = bool(profile.get("curl_download_fallback", False))
    prefer_curl_download = bool(
        profile.get(
            "prefer_curl_download",
            profile.get("curl_download_first", False),
        )
    )
    entry_patterns = tuple(
        PdfTextEntryPattern(
            pattern=str(item["pattern"]).strip(),
            official_trim=(
                str(item["official_trim"]).strip()
                if item.get("official_trim") is not None
                else None
            ),
            official_powertrain=(
                str(item["official_powertrain"]).strip()
                if item.get("official_powertrain") is not None
                else None
            ),
            official_edition=(
                str(item["official_edition"]).strip()
                if item.get("official_edition") is not None
                else None
            ),
            availability_text=(
                str(item["availability_text"]).strip()
                if item.get("availability_text") is not None
                else None
            ),
            jato_trim=(
                str(item["jato_trim"]).strip()
                if item.get("jato_trim") is not None
                else None
            ),
            jato_powertrain=(
                str(item["jato_powertrain"]).strip()
                if item.get("jato_powertrain") is not None
                else None
            ),
            price_delta=float(item.get("price_delta", 0.0) or 0.0),
            price_label=(
                str(item["price_label"]).strip()
                if item.get("price_label") is not None
                else None
            ),
        )
        for item in patterns_raw
        if isinstance(item, dict) and str(item.get("pattern", "")).strip()
    )
    return PdfTextProfile(
        url=profile["url"],
        entry_patterns=entry_patterns,
        timeout_seconds=int(profile.get("timeout_seconds", 60)),
        retry_attempts=int(profile.get("retry_attempts", 0)),
        retry_delay_seconds=float(profile.get("retry_delay_seconds", 0.0) or 0.0),
        prefer_curl_download=prefer_curl_download,
        curl_download_fallback=curl_download_fallback,
        browser_download_fallback=browser_download_fallback,
        default_currency=profile.get("default_currency", "EUR"),
        default_tax_included=bool(profile.get("default_tax_included", True)),
        default_price_label=profile.get(
            "default_price_label",
            "Manufacturer's Recommended Retail Price",
        ),
        fixed_model=profile.get("fixed_model"),
        fixed_jato_model=profile.get("fixed_jato_model"),
        fixed_jato_powertrain=profile.get("fixed_jato_powertrain"),
        copy_trim_to_jato_trim=bool(profile.get("copy_trim_to_jato_trim", False)),
        match_confidence=(
            float(profile["match_confidence"])
            if profile.get("match_confidence") is not None
            else None
        ),
        match_status=profile.get("match_status", "review_required"),
        match_reason=profile.get("match_reason"),
    )


def _build_http_text_profile(profile: dict[str, Any]) -> HttpTextProfile:
    patterns_raw = profile.get("entry_patterns", [])
    if not isinstance(patterns_raw, list):
        raise ValueError("http_text entry_patterns must be a list")
    entry_patterns = tuple(
        HttpTextEntryPattern(
            pattern=str(item["pattern"]).strip(),
            official_trim=(
                str(item["official_trim"]).strip()
                if item.get("official_trim") is not None
                else None
            ),
            official_powertrain=(
                str(item["official_powertrain"]).strip()
                if item.get("official_powertrain") is not None
                else None
            ),
            official_edition=(
                str(item["official_edition"]).strip()
                if item.get("official_edition") is not None
                else None
            ),
            availability_text=(
                str(item["availability_text"]).strip()
                if item.get("availability_text") is not None
                else None
            ),
            jato_trim=(
                str(item["jato_trim"]).strip()
                if item.get("jato_trim") is not None
                else None
            ),
            jato_powertrain=(
                str(item["jato_powertrain"]).strip()
                if item.get("jato_powertrain") is not None
                else None
            ),
            price_delta=float(item.get("price_delta", 0.0) or 0.0),
            price_label=(
                str(item["price_label"]).strip()
                if item.get("price_label") is not None
                else None
            ),
        )
        for item in patterns_raw
        if isinstance(item, dict) and str(item.get("pattern", "")).strip()
    )
    return HttpTextProfile(
        url=profile["url"],
        entry_patterns=entry_patterns,
        timeout_seconds=int(profile.get("timeout_seconds", 30)),
        headers=profile.get("headers", {}),
        default_currency=profile.get("default_currency", "EUR"),
        default_tax_included=bool(profile.get("default_tax_included", True)),
        default_price_label=profile.get(
            "default_price_label",
            "Manufacturer's Recommended Retail Price",
        ),
        fixed_model=profile.get("fixed_model"),
        fixed_jato_model=profile.get("fixed_jato_model"),
        fixed_jato_powertrain=profile.get("fixed_jato_powertrain"),
        copy_trim_to_jato_trim=bool(profile.get("copy_trim_to_jato_trim", False)),
        match_confidence=(
            float(profile["match_confidence"])
            if profile.get("match_confidence") is not None
            else None
        ),
        match_status=profile.get("match_status", "review_required"),
        match_reason=profile.get("match_reason"),
    )


def _make_extractor_class(
    extractor_type: str,
    profile: (
        HttpJsonProfile
        | HttpTextProfile
        | ScraplingProfile
        | PlaywrightCardFlowProfile
        | PdfTextProfile
    ),
) -> type:
    """Create an extractor class that binds a fixed profile at init."""
    if extractor_type == "http_json":
        class _ConfiguredHttpJson(HttpJsonExtractor):
            _profile = profile

            def __init__(self, config: ExtractorConfig) -> None:
                super().__init__(config, self._profile)
        return _ConfiguredHttpJson

    if extractor_type == "http_text":
        class _ConfiguredHttpText(HttpTextExtractor):
            _profile = profile

            def __init__(self, config: ExtractorConfig) -> None:
                super().__init__(config, self._profile)
        return _ConfiguredHttpText

    if extractor_type == "scrapling":
        class _ConfiguredScrapling(ScraplingExtractor):
            _profile = profile

            def __init__(self, config: ExtractorConfig) -> None:
                super().__init__(config, self._profile)
        return _ConfiguredScrapling

    if extractor_type == "playwright":
        class _ConfiguredPlaywright(PlaywrightCardFlowExtractor):
            _profile = profile

            def __init__(self, config: ExtractorConfig) -> None:
                super().__init__(config, self._profile)
        return _ConfiguredPlaywright

    if extractor_type == "pdf_text":
        class _ConfiguredPdfText(PdfTextExtractor):
            _profile = profile

            def __init__(self, config: ExtractorConfig) -> None:
                super().__init__(config, self._profile)
        return _ConfiguredPdfText

    raise ValueError(f"Unknown extractor_type: {extractor_type!r}")


def load_source_file(path: Path) -> str | None:
    """Load a single YAML source file and register it."""
    try:
        data = _load_yaml_mapping(path)
    except ValueError:
        log.warning("Skipping %s — not a YAML mapping", path.name)
        return None

    required = (
        "source_code",
        "country",
        "brand",
        "source_url",
        "extractor_type",
        "profile",
    )
    missing = [k for k in required if k not in data]
    if missing:
        log.warning("Skipping %s — missing keys: %s", path.name, missing)
        return None

    source_code = data["source_code"]

    # skip if already registered (e.g. from Python code)
    if source_code in registry.list_registered():
        log.debug(
            "Source %s already registered, skipping %s",
            source_code,
            path.name,
        )
        return source_code

    ext_type = data["extractor_type"]

    try:
        config = _build_extractor_config(data)
        profile_raw = _resolve_profile_raw(path, data)
        if ext_type == "http_json":
            profile = _build_http_json_profile(profile_raw)
        elif ext_type == "http_text":
            profile = _build_http_text_profile(profile_raw)
        elif ext_type == "scrapling":
            profile = _build_scrapling_profile(profile_raw)
        elif ext_type == "playwright":
            profile = _build_playwright_profile(profile_raw)
        elif ext_type == "pdf_text":
            profile = _build_pdf_text_profile(profile_raw)
        else:
            log.warning(
                "Skipping %s — unknown extractor_type=%s",
                path.name,
                ext_type,
            )
            return None

        cls = _make_extractor_class(ext_type, profile)
        registry.register(config, cls)
        log.info("Registered source %s from %s", source_code, path.name)
        return source_code
    except Exception as exc:
        log.error("Failed to load %s: %s", path.name, exc)
        return None


def load_all_sources(sources_dir: Path | None = None) -> list[str]:
    """Scan the sources directory and register all YAML-defined extractors.

    Uses ``os.walk(followlinks=True)`` so that symlinked sub-directories
    (e.g. draft batches) are discovered on Python < 3.13 where
    ``Path.rglob`` does not follow symlinks.
    """
    import os as _os

    d = sources_dir or SOURCES_DIR
    if not d.is_dir():
        log.debug("Sources directory %s does not exist — nothing to load", d)
        return []

    yaml_paths: list[Path] = []
    for dirpath, _dirnames, filenames in _os.walk(d, followlinks=True):
        _dirnames[:] = [
            dirname for dirname in _dirnames if not dirname.startswith("_")
        ]
        for fn in filenames:
            if fn.startswith("_"):
                continue
            if fn.endswith((".yaml", ".yml")):
                yaml_paths.append(Path(dirpath) / fn)

    loaded: list[str] = []
    for path in sorted(yaml_paths):
        code = load_source_file(path)
        if code:
            loaded.append(code)

    log.info("Loaded %d source(s) from %s", len(loaded), d)
    return loaded
