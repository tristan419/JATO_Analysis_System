"""Regex-driven HTTP text extractor for HTML pages with embedded price data."""

from __future__ import annotations

from dataclasses import dataclass, field
import html
import logging
import re

import requests

from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation
from jato_scraper.extractors.pdf_text import parse_price

log = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 30


def _clean_text(value: object) -> str:
    text = html.unescape(str(value or "")).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


@dataclass(frozen=True)
class HttpTextEntryPattern:
    pattern: str
    official_trim: str | None = None
    official_powertrain: str | None = None
    official_edition: str | None = None
    availability_text: str | None = None
    jato_trim: str | None = None
    jato_powertrain: str | None = None
    price_delta: float = 0.0
    price_label: str | None = None


@dataclass(frozen=True)
class HttpTextProfile:
    url: str
    entry_patterns: tuple[HttpTextEntryPattern, ...] = field(default_factory=tuple)
    timeout_seconds: int = DEFAULT_TIMEOUT
    headers: dict[str, str] = field(default_factory=dict)
    default_currency: str = "EUR"
    default_tax_included: bool = True
    default_price_label: str = "Manufacturer's Recommended Retail Price"
    fixed_model: str | None = None
    fixed_jato_model: str | None = None
    fixed_jato_powertrain: str | None = None
    copy_trim_to_jato_trim: bool = False
    match_confidence: float | None = None
    match_status: str = "review_required"
    match_reason: dict[str, object] | None = None


class HttpTextExtractor(BaseExtractor):
    def __init__(self, config: ExtractorConfig, profile: HttpTextProfile) -> None:
        super().__init__(config)
        self.profile = profile
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "JATO-MSRP-Scraper/0.1",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                **profile.headers,
            }
        )

    @property
    def extractor_version(self) -> str:
        return "0.1.0-http-text"

    def extract(self) -> list[RawObservation]:
        text = self._fetch_text()
        if not text:
            self.record_strategy_audit(
                url=self.profile.url,
                strategy="http_text",
                observations=[],
                winning_strategy=None,
                error="fetch_failed",
            )
            return []

        results: list[RawObservation] = []
        seen: set[tuple[str, str, float]] = set()
        for entry in self.profile.entry_patterns:
            for match in re.finditer(entry.pattern, text, re.IGNORECASE | re.DOTALL):
                observation = self._build_observation(entry, match)
                if observation is None:
                    continue
                dedupe_key = (
                    observation.official_trim,
                    observation.official_powertrain or "",
                    observation.msrp_value,
                )
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                results.append(observation)

        self.record_strategy_audit(
            url=self.profile.url,
            strategy="http_text",
            observations=results,
            winning_strategy="http_text" if results else None,
        )
        return results

    def _fetch_text(self) -> str:
        timeout = max(1, int(self.profile.timeout_seconds or DEFAULT_TIMEOUT))
        try:
            response = self._session.get(self.profile.url, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            log.error(
                "HTTP text request failed for %s: %s",
                self.config.source_code,
                exc,
            )
            return ""
        return response.text

    def _build_observation(
        self,
        entry: HttpTextEntryPattern,
        match: re.Match[str],
    ) -> RawObservation | None:
        groups = {
            key: _clean_text(value)
            for key, value in match.groupdict().items()
            if value is not None
        }
        price_raw = groups.get("price")
        if not price_raw:
            log.warning(
                "Skipping HTTP text match without named 'price' group for %s",
                self.config.source_code,
            )
            return None
        parsed_price = parse_price(price_raw)
        if parsed_price is None:
            log.warning(
                "Skipping HTTP text match with unparseable price %r for %s",
                price_raw,
                self.config.source_code,
            )
            return None

        official_model = _clean_text(self.profile.fixed_model or groups.get("model") or "")
        official_trim = _clean_text(entry.official_trim or groups.get("trim") or "")
        official_powertrain = _clean_text(
            entry.official_powertrain or groups.get("powertrain") or ""
        ) or None
        official_edition = _clean_text(
            entry.official_edition or groups.get("edition") or ""
        ) or None
        availability_text = _clean_text(
            entry.availability_text or groups.get("availability") or ""
        ) or None

        jato_trim = _clean_text(entry.jato_trim or groups.get("jato_trim") or "")
        if not jato_trim and self.profile.copy_trim_to_jato_trim:
            jato_trim = official_trim

        jato_powertrain = _clean_text(
            entry.jato_powertrain
            or self.profile.fixed_jato_powertrain
            or groups.get("jato_powertrain")
            or ""
        ) or None

        return RawObservation(
            official_model=official_model,
            official_trim=official_trim,
            official_powertrain=official_powertrain,
            official_edition=official_edition,
            msrp_value=parsed_price + float(entry.price_delta),
            currency=self.profile.default_currency,
            tax_included=self.profile.default_tax_included,
            price_label=str(entry.price_label or self.profile.default_price_label),
            source_url=self.config.source_url,
            availability_text=availability_text,
            raw_payload={
                "text_url": self.profile.url,
                "pattern": entry.pattern,
                "match_groups": groups,
                "price_delta": entry.price_delta,
            },
            jato_model=_clean_text(
                self.profile.fixed_jato_model or groups.get("jato_model") or ""
            ),
            jato_trim=jato_trim,
            jato_powertrain=jato_powertrain,
            match_confidence=self.profile.match_confidence or 0.0,
            match_status=self.profile.match_status,
            match_reason=self.profile.match_reason,
        )
