"""Playwright-driven card flow extractor for dynamic configurators.

This extractor is designed for sites where a trim card selection leads to a
second step that exposes powertrain-specific pricing cards. The flow is fully
config-driven so the same extractor can be reused across similar Volkswagen
country configurators.
"""

from __future__ import annotations

import logging
import re
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:
    # pragma: no cover - exercised only when dependency missing
    PlaywrightTimeoutError = TimeoutError
    sync_playwright = None

log = logging.getLogger(__name__)

WaitUntilState = Literal["commit", "domcontentloaded", "load", "networkidle"]
BrowserName = Literal["chromium", "firefox", "webkit"]

_PRICE_RE = re.compile(r"[\d]+(?:[.,'\u2019]\d{3})*(?:[.,]\d{1,2})?")
_DEFAULT_POWERTRAIN_RULES: tuple[dict[str, Any], ...] = (
    {
        "powertrain": "PHEV",
        "keywords": (
            "ehybrid",
            "plug-in hybrid",
            "plugin hybrid",
            "plug in hybrid",
            "laddhybrid",
        ),
    },
    {
        "powertrain": "MHEV",
        "keywords": (
            "etsi",
            "mild hybrid",
            "mhev",
        ),
    },
    {
        "powertrain": "ICE",
        "keywords": (
            "tdi",
            "tsi",
            "diesel",
            "bensin",
            "petrol",
            "gasoline",
        ),
    },
)


@dataclass(frozen=True)
class PlaywrightCardFlowProfile:
    url: str
    browser: BrowserName = "chromium"
    headless: bool = True
    wait_until: WaitUntilState = "domcontentloaded"
    page_timeout_ms: int = 120000
    viewport_width: int = 1440
    viewport_height: int = 1200
    locale: str | None = None
    timezone_id: str | None = None
    initial_ready_selector: str = ""
    initial_ready_timeout_ms: int = 60000
    startup_dismiss_selectors: tuple[str, ...] = ()
    cookie_banner_selector: str | None = None
    cookie_reject_selector: str | None = None
    cookie_reject_text: str | None = None
    trim_card_selector: str = ""
    trim_name_selector: str = "h3"
    trim_model_selector: str | None = None
    trim_card_wait_ms: int = 1200
    next_step_selector: str = ""
    detail_ready_selector: str | None = None
    detail_card_selector: str = ""
    detail_name_selector: str | None = None
    detail_price_selector: str | None = None
    detail_card_wait_ms: int = 1500
    powertrain_line_count: int = 2
    extract_from_trim_cards: bool = False
    combine_trim_and_powertrain: bool = True
    combined_trim_separator: str = " | "
    default_currency: str = "EUR"
    default_tax_included: bool = True
    default_price_label: str = "Manufacturer's Recommended Retail Price"
    fixed_trim: str | None = None
    fixed_model: str | None = None
    fixed_jato_model: str | None = None
    fixed_jato_powertrain: str | None = None
    match_confidence: float | None = None
    match_status: str = "review_required"
    match_reason: dict[str, Any] | None = None
    structured_fields: dict[str, Any] | None = None


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _normalize_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _normalize_space(raw_line)
        if not line or line.isdigit():
            continue
        lines.append(line)
    return lines


def parse_price(raw: str) -> float | None:
    compact = raw.replace("\xa0", " ")
    match = _PRICE_RE.search(compact.replace(" ", ""))
    if not match:
        return None
    number = match.group().replace("'", "").replace("\u2019", "")
    if "," in number and "." in number:
        if number.rfind(",") > number.rfind("."):
            number = number.replace(".", "").replace(",", ".")
        else:
            number = number.replace(",", "")
    elif "," in number:
        parts = number.split(",")
        if len(parts[-1]) <= 2:
            number = number.replace(",", ".")
        else:
            number = number.replace(",", "")
    elif "." in number:
        parts = number.split(".")
        if all(len(part) == 3 for part in parts[1:]):
            number = number.replace(".", "")
    return float(number)


class PlaywrightCardFlowExtractor(BaseExtractor):
    def __init__(
        self,
        config: ExtractorConfig,
        profile: PlaywrightCardFlowProfile,
    ) -> None:
        super().__init__(config)
        self.profile = profile

    @property
    def extractor_version(self) -> str:
        return "0.1.0-playwright-card-flow"

    def extract(self) -> list[RawObservation]:
        if sync_playwright is None:
            raise RuntimeError(
                "Playwright is not installed. Install the toolkit dependencies"
                "and run 'playwright install chromium'."
            )

        observations: list[RawObservation] = []
        with sync_playwright() as playwright:
            browser_launcher = getattr(playwright, self.profile.browser)
            browser = browser_launcher.launch(headless=self.profile.headless)
            context_kwargs: dict[str, Any] = {
                "viewport": {
                    "width": self.profile.viewport_width,
                    "height": self.profile.viewport_height,
                }
            }
            if self.profile.locale:
                context_kwargs["locale"] = self.profile.locale
            if self.profile.timezone_id:
                context_kwargs["timezone_id"] = self.profile.timezone_id
            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            try:
                if self.profile.extract_from_trim_cards:
                    observations.extend(
                        self._extract_trim_overview_cards(page)
                    )
                elif self._has_direct_detail_flow(page):
                    observations.extend(
                        self._extract_direct_detail_cards(page)
                    )
                else:
                    trim_count = self._open_trim_stage(page)
                    for index in range(trim_count):
                        observations.extend(
                            self._extract_trim_powertrains(page, index)
                        )
            finally:
                context.close()
                browser.close()
        return observations

    def _has_direct_detail_flow(self, page) -> bool:
        self._goto(page)
        ready_selector = (
            self.profile.initial_ready_selector
            or self.profile.detail_ready_selector
            or self.profile.detail_card_selector
        )
        if ready_selector:
            page.locator(ready_selector).first.wait_for(
                timeout=self.profile.initial_ready_timeout_ms
            )
        self._dismiss_cookie_banner(page)
        page.wait_for_timeout(self.profile.trim_card_wait_ms)

        trim_count = 0
        if self.profile.trim_card_selector:
            trim_count = page.locator(self.profile.trim_card_selector).count()
        detail_count = 0
        if self.profile.detail_card_selector:
            detail_count = page.locator(
                self.profile.detail_card_selector
            ).count()
        return trim_count <= 0 and detail_count > 0

    def _goto(self, page) -> None:
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                page.goto(
                    self.profile.url,
                    wait_until=self.profile.wait_until,
                    timeout=self.profile.page_timeout_ms,
                )
                return
            except PlaywrightTimeoutError as exc:
                last_error = exc
                log.warning(
                    "Playwright goto timed out for %s (attempt %d/2)",
                    self.profile.url,
                    attempt + 1,
                )
        raise RuntimeError(
            f"Failed to load {self.profile.url!r} in Playwright"
        ) from last_error

    def _dismiss_cookie_banner(self, page) -> None:
        try:
            if self.profile.cookie_reject_selector:
                reject_button = page.locator(
                    self.profile.cookie_reject_selector
                ).first
            elif self.profile.cookie_reject_text:
                reject_button = page.get_by_text(
                    self.profile.cookie_reject_text,
                    exact=False,
                ).first
            else:
                return
            try:
                reject_button.wait_for(state="visible", timeout=3000)
            except PlaywrightTimeoutError:
                return
            reject_button.click(timeout=20000, force=True)
            if self.profile.cookie_banner_selector:
                banner = page.locator(
                    self.profile.cookie_banner_selector
                ).first
                try:
                    banner.wait_for(state="hidden", timeout=5000)
                except PlaywrightTimeoutError:
                    log.debug(
                        "Cookie banner remained attached after dismiss for %s",
                        self.config.source_code,
                    )
            page.wait_for_timeout(1000)
        except PlaywrightTimeoutError:
            log.warning(
                "Cookie banner dismiss timed out for %s",
                self.config.source_code,
            )

    def _dismiss_startup_overlays(self, page) -> None:
        for selector in self.profile.startup_dismiss_selectors:
            if not selector:
                continue
            try:
                overlay = page.locator(selector).first
                if overlay.count() <= 0:
                    continue
                if not overlay.is_visible():
                    continue
                overlay.click(timeout=5000, force=True)
                page.wait_for_timeout(600)
            except Exception:
                log.debug(
                    "Startup overlay dismiss timed out for %s on %s",
                    self.config.source_code,
                    selector,
                )

    def _open_trim_stage(self, page) -> int:
        self._goto(page)
        page.locator(self.profile.initial_ready_selector).first.wait_for(
            timeout=self.profile.initial_ready_timeout_ms
        )
        self._dismiss_startup_overlays(page)
        self._dismiss_cookie_banner(page)
        self._dismiss_startup_overlays(page)
        page.wait_for_timeout(self.profile.trim_card_wait_ms)
        trim_cards = page.locator(self.profile.trim_card_selector)
        trim_count = trim_cards.count()
        if trim_count <= 0:
            raise RuntimeError(
                f"No trim cards found for {self.config.source_code!r}"
            )
        return trim_count

    def _extract_trim_powertrains(
        self,
        page,
        index: int,
    ) -> list[RawObservation]:
        self._goto(page)
        page.locator(self.profile.initial_ready_selector).first.wait_for(
            timeout=self.profile.initial_ready_timeout_ms
        )
        self._dismiss_startup_overlays(page)
        self._dismiss_cookie_banner(page)
        self._dismiss_startup_overlays(page)
        page.wait_for_timeout(self.profile.trim_card_wait_ms)

        trim_cards = page.locator(self.profile.trim_card_selector)
        if trim_cards.count() <= index:
            raise RuntimeError(
                f"Trim index {index} missing for {self.config.source_code!r}"
            )

        trim_card = trim_cards.nth(index)
        trim_card.scroll_into_view_if_needed()
        trim_name = self._extract_trim_name(trim_card)
        model_name = self._extract_model_name(trim_card)
        trim_card.click(timeout=20000)
        page.wait_for_timeout(max(500, self.profile.trim_card_wait_ms // 2))
        self._dismiss_startup_overlays(page)
        page.locator(self.profile.next_step_selector).first.click(
            timeout=20000
        )
        ready_selector = (
            self.profile.detail_ready_selector
            or self.profile.detail_card_selector
        )
        page.locator(ready_selector).first.wait_for(
            timeout=self.profile.initial_ready_timeout_ms
        )
        page.wait_for_timeout(self.profile.detail_card_wait_ms)

        detail_cards = page.locator(self.profile.detail_card_selector)
        detail_count = detail_cards.count()
        if detail_count <= 0:
            raise RuntimeError(
                f"No detail cards found after stepping into {trim_name!r}"
            )

        observations: list[RawObservation] = []
        for detail_index in range(detail_count):
            detail_card = detail_cards.nth(detail_index)
            detail_text = detail_card.inner_text().strip()
            powertrain_label = self._extract_powertrain_label(
                detail_card,
                detail_text,
            )
            price_text = self._extract_price_text(detail_card, detail_text)
            price_value = parse_price(price_text)
            if price_value is None:
                log.warning(
                    "Skipping %s / %s — unable to parse price from %r",
                    self.config.source_code,
                    trim_name,
                    price_text,
                )
                continue
            combined_trim = self._build_combined_trim(
                trim_name,
                powertrain_label,
            )
            jato_powertrain = self._resolve_jato_powertrain(
                powertrain_label,
                detail_text,
            )
            match_reason = self._build_match_reason(
                trim_name,
                combined_trim,
                powertrain_label,
                jato_powertrain,
                page.url,
            )
            observations.append(
                RawObservation(
                    official_model=model_name,
                    official_trim=combined_trim,
                    official_powertrain=powertrain_label or None,
                    msrp_value=price_value,
                    currency=self.profile.default_currency,
                    tax_included=self.profile.default_tax_included,
                    price_label=self.profile.default_price_label,
                    source_url=page.url,
                    raw_payload={
                        "trimText": trim_name,
                        "powertrainText": powertrain_label,
                        "detailCardText": detail_text,
                        "detailIndex": detail_index,
                    },
                    jato_model=(self.profile.fixed_jato_model or model_name),
                    jato_trim=combined_trim,
                    jato_powertrain=jato_powertrain,
                    match_confidence=(
                        float(self.profile.match_confidence)
                        if self.profile.match_confidence is not None
                        else 0.84
                    ),
                    match_status=self.profile.match_status,
                    match_reason=match_reason,
                )
            )
        return observations

    def _extract_direct_detail_cards(self, page) -> list[RawObservation]:
        self._goto(page)
        ready_selector = (
            self.profile.initial_ready_selector
            or self.profile.detail_ready_selector
            or self.profile.detail_card_selector
        )
        if ready_selector:
            page.locator(ready_selector).first.wait_for(
                timeout=self.profile.initial_ready_timeout_ms
            )
        self._dismiss_startup_overlays(page)
        self._dismiss_cookie_banner(page)
        self._dismiss_startup_overlays(page)
        page.wait_for_timeout(self.profile.detail_card_wait_ms)

        detail_cards = page.locator(self.profile.detail_card_selector)
        detail_count = detail_cards.count()
        if detail_count <= 0:
            raise RuntimeError(
                f"No detail cards found for {self.config.source_code!r}"
            )

        model_name = _normalize_space(
            self.profile.fixed_model
            or self.profile.fixed_jato_model
            or self._resolve_direct_trim_name()
        )
        trim_name = self._resolve_direct_trim_name()

        observations: list[RawObservation] = []
        for detail_index in range(detail_count):
            detail_card = detail_cards.nth(detail_index)
            detail_text = detail_card.inner_text().strip()
            powertrain_label = self._extract_powertrain_label(
                detail_card,
                detail_text,
            )
            price_text = self._extract_price_text(detail_card, detail_text)
            price_value = parse_price(price_text)
            if price_value is None:
                log.warning(
                    "Skipping %s / %s — unable to parse price from %r",
                    self.config.source_code,
                    trim_name,
                    price_text,
                )
                continue
            combined_trim = self._build_combined_trim(
                trim_name,
                powertrain_label,
            )
            jato_powertrain = self._resolve_jato_powertrain(
                powertrain_label,
                detail_text,
            )
            match_reason = self._build_match_reason(
                trim_name,
                combined_trim,
                powertrain_label,
                jato_powertrain,
                page.url,
            )
            match_reason["configuratorStep"] = "engine_direct"
            observations.append(
                RawObservation(
                    official_model=model_name,
                    official_trim=combined_trim,
                    official_powertrain=powertrain_label or None,
                    msrp_value=price_value,
                    currency=self.profile.default_currency,
                    tax_included=self.profile.default_tax_included,
                    price_label=self.profile.default_price_label,
                    source_url=page.url,
                    raw_payload={
                        "trimText": trim_name,
                        "powertrainText": powertrain_label,
                        "detailCardText": detail_text,
                        "detailIndex": detail_index,
                    },
                    jato_model=(self.profile.fixed_jato_model or model_name),
                    jato_trim=combined_trim,
                    jato_powertrain=jato_powertrain,
                    match_confidence=(
                        float(self.profile.match_confidence)
                        if self.profile.match_confidence is not None
                        else 0.84
                    ),
                    match_status=self.profile.match_status,
                    match_reason=match_reason,
                )
            )
        return observations

    def _extract_trim_overview_cards(self, page) -> list[RawObservation]:
        self._goto(page)
        ready_selector = (
            self.profile.initial_ready_selector
            or self.profile.trim_card_selector
        )
        if ready_selector:
            page.locator(ready_selector).first.wait_for(
                timeout=self.profile.initial_ready_timeout_ms
            )
        self._dismiss_startup_overlays(page)
        self._dismiss_cookie_banner(page)
        self._dismiss_startup_overlays(page)
        page.wait_for_timeout(self.profile.trim_card_wait_ms)

        trim_cards = page.locator(self.profile.trim_card_selector)
        trim_count = trim_cards.count()
        if trim_count <= 0:
            raise RuntimeError(
                f"No trim cards found for {self.config.source_code!r}"
            )

        observations: list[RawObservation] = []
        for index in range(trim_count):
            trim_card = trim_cards.nth(index)
            trim_name = self._extract_trim_name(trim_card)
            model_name = self._extract_model_name(trim_card)
            detail_text = trim_card.inner_text().strip()
            powertrain_label = self._extract_trim_overview_powertrain_label(
                detail_text
            )
            price_text = self._extract_price_text(trim_card, detail_text)
            price_value = parse_price(price_text)
            if price_value is None:
                log.warning(
                    "Skipping %s / %s — unable to parse price from %r",
                    self.config.source_code,
                    trim_name,
                    price_text,
                )
                continue
            combined_trim = self._build_combined_trim(
                trim_name,
                powertrain_label,
            )
            jato_powertrain = self._resolve_jato_powertrain(
                powertrain_label,
                detail_text,
            )
            match_reason = self._build_match_reason(
                trim_name,
                combined_trim,
                powertrain_label,
                jato_powertrain,
                page.url,
            )
            match_reason["strategy"] = "playwright_trim_overview"
            match_reason["configuratorStep"] = "trim_overview"
            observations.append(
                RawObservation(
                    official_model=model_name,
                    official_trim=combined_trim,
                    official_powertrain=powertrain_label or None,
                    msrp_value=price_value,
                    currency=self.profile.default_currency,
                    tax_included=self.profile.default_tax_included,
                    price_label=self.profile.default_price_label,
                    source_url=page.url,
                    raw_payload={
                        "trimText": trim_name,
                        "powertrainText": powertrain_label,
                        "detailCardText": detail_text,
                        "trimIndex": index,
                    },
                    jato_model=(self.profile.fixed_jato_model or model_name),
                    jato_trim=combined_trim,
                    jato_powertrain=jato_powertrain,
                    match_confidence=(
                        float(self.profile.match_confidence)
                        if self.profile.match_confidence is not None
                        else 0.84
                    ),
                    match_status=self.profile.match_status,
                    match_reason=match_reason,
                )
            )
        return observations

    def _extract_trim_overview_powertrain_label(self, detail_text: str) -> str:
        lines = _normalize_lines(detail_text)
        marker_index = -1
        for index, line in enumerate(lines):
            normalized = line.lower()
            if normalized.startswith("motoren") or normalized.startswith(
                "batterien"
            ):
                marker_index = index
                break
        if marker_index < 0:
            return ""

        stop_keywords = (
            "leistung",
            "kapazität",
            "elektrische reichweite",
            "hubraum",
            "verbrauch",
            "emission",
            "co₂-klasse",
            "co2-klasse",
            "werte kombiniert",
        )
        collected: list[str] = []
        for line in lines[marker_index + 1:]:
            normalized = line.lower()
            if any(keyword in normalized for keyword in stop_keywords):
                break
            collected.append(line)
        return _normalize_space(" ".join(collected))

    def _resolve_direct_trim_name(self) -> str:
        trim_name = _normalize_space(
            self.profile.fixed_trim
            or self.profile.fixed_model
            or self.profile.fixed_jato_model
            or ""
        )
        if trim_name:
            return trim_name
        raise RuntimeError(
            "No trim name resolved for "
            f"{self.config.source_code!r}; set fixed_trim or fixed_model"
        )

    def _extract_trim_name(self, trim_card) -> str:
        trim_name = trim_card.locator(
            self.profile.trim_name_selector
        ).first.inner_text(timeout=20000)
        return _normalize_space(trim_name)

    def _extract_model_name(self, trim_card) -> str:
        if self.profile.fixed_model:
            return _normalize_space(self.profile.fixed_model)
        if self.profile.trim_model_selector:
            text = trim_card.locator(
                self.profile.trim_model_selector
            ).first.inner_text(timeout=20000)
            return _normalize_space(text)
        if self.profile.fixed_jato_model:
            return _normalize_space(self.profile.fixed_jato_model)
        raise RuntimeError(
            "No model name resolved for "
            f"{self.config.source_code!r}; set fixed_model or "
            "trim_model_selector"
        )

    def _extract_powertrain_label(self, detail_card, detail_text: str) -> str:
        if self.profile.detail_name_selector:
            labels = detail_card.locator(self.profile.detail_name_selector)
            parts: list[str] = []
            max_lines = min(
                labels.count(),
                self.profile.powertrain_line_count,
            )
            for index in range(max_lines):
                part = _normalize_space(
                    labels.nth(index).inner_text(timeout=20000)
                )
                if part:
                    parts.append(part)
            if parts:
                return _normalize_space(" ".join(parts))

        lines = _normalize_lines(detail_text)
        parts: list[str] = []
        for line in lines:
            if line.lower() == "bilens specifikation":
                break
            parts.append(line)
            if len(parts) >= max(1, self.profile.powertrain_line_count):
                break
        return _normalize_space(" ".join(parts))

    def _extract_price_text(self, detail_card, detail_text: str) -> str:
        if self.profile.detail_price_selector:
            price_locator = detail_card.locator(
                self.profile.detail_price_selector
            )
            if price_locator.count() > 0:
                return _normalize_space(
                    price_locator.first.inner_text(timeout=20000)
                )
        return _normalize_space(detail_text)

    def _build_combined_trim(
        self,
        trim_name: str,
        powertrain_label: str,
    ) -> str:
        if (
            not self.profile.combine_trim_and_powertrain
            or not powertrain_label
        ):
            return trim_name
        combined_label = (
            f"{trim_name}{self.profile.combined_trim_separator}"
            f"{powertrain_label}"
        )
        return _normalize_space(
            combined_label
        )

    def _resolve_jato_powertrain(
        self,
        powertrain_label: str,
        detail_text: str,
    ) -> str | None:
        if self.profile.fixed_jato_powertrain:
            return _normalize_space(self.profile.fixed_jato_powertrain).upper()

        search_text = _normalize_space(
            f"{powertrain_label} {detail_text}"
        ).lower()
        structured_fields = self.profile.structured_fields or {}
        powertrain_rules = structured_fields.get("powertrain_rules")
        if isinstance(powertrain_rules, list) and powertrain_rules:
            rules = [
                rule
                for rule in powertrain_rules
                if isinstance(rule, dict)
            ]
        else:
            rules = list(_DEFAULT_POWERTRAIN_RULES)

        matches: list[str] = []
        for rule in rules:
            powertrain = _normalize_space(str(rule.get("powertrain") or ""))
            keywords = [
                _normalize_space(str(keyword)).lower()
                for keyword in list(rule.get("keywords") or [])
                if _normalize_space(str(keyword))
            ]
            if (
                powertrain
                and keywords
                and any(keyword in search_text for keyword in keywords)
            ):
                matches.append(powertrain.upper())
        unique_matches = list(dict.fromkeys(matches))
        if len(unique_matches) == 1:
            return unique_matches[0]
        return None

    def _build_match_reason(
        self,
        trim_name: str,
        combined_trim: str,
        powertrain_label: str,
        jato_powertrain: str | None,
        source_url: str,
    ) -> dict[str, Any]:
        base_reason = deepcopy(self.profile.match_reason or {})
        if not isinstance(base_reason, dict):
            base_reason = {"previous": base_reason}
        base_reason.update(
            {
                "strategy": "playwright_card_flow",
                "configuratorStep": "engine",
                "trimText": trim_name,
                "combinedTrim": combined_trim,
                "powertrainText": powertrain_label,
                "jatoPowertrain": jato_powertrain,
                "sourceUrl": source_url,
            }
        )
        return base_reason
