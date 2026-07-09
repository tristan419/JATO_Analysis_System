"""Regex-driven PDF text extractor for official price-list documents."""

from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
import logging
import re
import subprocess
import tempfile
import time

import requests
from pypdf import PdfReader

from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation

log = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 60
DEFAULT_CURL_FALLBACK_TIMEOUT = 30
_PRICE_RE = re.compile(r"\d[\d\s.,'\u2019]*\d|\d")


@dataclass(frozen=True)
class PdfTextEntryPattern:
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
class PdfTextProfile:
    url: str
    urls: tuple[str, ...] = field(default_factory=tuple)
    entry_patterns: tuple[PdfTextEntryPattern, ...] = field(default_factory=tuple)
    headers: dict[str, str] = field(default_factory=dict)
    timeout_seconds: int = DEFAULT_TIMEOUT
    retry_attempts: int = 0
    retry_delay_seconds: float = 0.0
    prefer_curl_download: bool = False
    browser_download_fallback: bool = False
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


def parse_price(raw: str) -> float | None:
    normalized_raw = re.sub(r"\s+", "", raw.replace("\xa0", " "))
    match = _PRICE_RE.search(normalized_raw)
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


class PdfTextExtractor(BaseExtractor):
    def __init__(self, config: ExtractorConfig, profile: PdfTextProfile) -> None:
        super().__init__(config)
        self.profile = profile
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "JATO-MSRP-Scraper/0.1",
                "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
                **profile.headers,
            }
        )

    @property
    def extractor_version(self) -> str:
        return "0.1.0-pdf-text"

    def extract(self) -> list[RawObservation]:
        text = self._extract_text()
        if not text:
            self.record_strategy_audit(
                url=self.profile.url,
                strategy="pdf_text",
                observations=[],
                winning_strategy=None,
                error="pdf_text_extraction_failed",
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
            strategy="pdf_text",
            observations=results,
            winning_strategy="pdf_text" if results else None,
        )
        return results

    def _profile_urls(self) -> tuple[str, ...]:
        urls: list[str] = []
        for url in (self.profile.url, *self.profile.urls):
            clean_url = str(url or "").strip()
            if clean_url and clean_url not in urls:
                urls.append(clean_url)
        return tuple(urls)

    def _fetch_pdf_bytes(self) -> bytes | None:
        return self._fetch_pdf_bytes_url(self.profile.url)

    def _fetch_pdf_bytes_url(self, url: str) -> bytes | None:
        last_error: Exception | None = None
        attempts = max(1, int(self.profile.retry_attempts or 0) + 1)
        timeout = max(1, int(self.profile.timeout_seconds or DEFAULT_TIMEOUT))
        if self.profile.prefer_curl_download:
            blob = self._fetch_pdf_bytes_with_curl(
                max(timeout, DEFAULT_CURL_FALLBACK_TIMEOUT),
                url,
            )
            if blob:
                return blob
            log.warning(
                "Preferred PDF curl download failed for %s at %s; falling back to requests",
                self.config.source_code,
                url,
            )

        for attempt in range(1, attempts + 1):
            try:
                response = self._session.get(url, timeout=timeout)
                response.raise_for_status()
                return response.content
            except requests.RequestException as exc:
                last_error = exc
                log.warning(
                    "PDF request attempt %s/%s failed for %s at %s: %s",
                    attempt,
                    attempts,
                    self.config.source_code,
                    url,
                    exc,
                )
                if attempt < attempts and self.profile.retry_delay_seconds > 0:
                    time.sleep(float(self.profile.retry_delay_seconds))

        if self.profile.browser_download_fallback:
            blob = self._fetch_pdf_bytes_with_curl(
                max(timeout, DEFAULT_CURL_FALLBACK_TIMEOUT),
                url,
            )
            if blob:
                return blob

        if last_error:
            log.error(
                "PDF request failed for %s at %s: %s",
                self.config.source_code,
                url,
                last_error,
            )
        return None

    def _fetch_pdf_bytes_with_curl(
        self,
        timeout: int,
        url: str | None = None,
    ) -> bytes | None:
        fetch_url = str(url or self.profile.url)
        header_args = [
            arg
            for key, value in self.profile.headers.items()
            if str(key).strip()
            for arg in ("-H", f"{str(key).strip()}: {str(value)}")
        ]
        try:
            with tempfile.NamedTemporaryFile(
                prefix="jato_pdf_",
                suffix=".pdf",
            ) as tmp:
                result = subprocess.run(
                    [
                        "curl",
                        "-L",
                        "--http1.1",
                        "-sS",
                        "--max-time",
                        str(timeout),
                        "-o",
                        tmp.name,
                        *header_args,
                        fetch_url,
                    ],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=timeout + 5,
                )
                if result.returncode != 0:
                    log.error(
                        "PDF curl fallback failed for %s: %s",
                        self.config.source_code,
                        result.stderr.decode(errors="replace")[:300],
                    )
                    return None
                tmp.seek(0)
                blob = tmp.read()
        except (OSError, subprocess.SubprocessError) as exc:
            log.error("PDF curl fallback failed for %s: %s", self.config.source_code, exc)
            return None
        if not blob.startswith(b"%PDF"):
            log.error(
                "PDF curl fallback returned non-PDF content for %s",
                self.config.source_code,
            )
            return None
        return blob

    def _extract_text(self) -> str:
        chunks: list[str] = []
        for url in self._profile_urls():
            blob = self._fetch_pdf_bytes_url(url)
            if blob is None:
                continue
            reader = PdfReader(BytesIO(blob))
            pages = [
                (page.extract_text() or "").replace("\xa0", " ")
                for page in reader.pages
            ]
            chunks.append(f"\n\n--- JATO_PDF_TEXT_URL: {url} ---\n" + "\n".join(pages))
        return "\n".join(chunks).strip()

    def _build_observation(
        self,
        entry: PdfTextEntryPattern,
        match: re.Match[str],
    ) -> RawObservation | None:
        groups = {key: value for key, value in match.groupdict().items() if value is not None}
        price_raw = groups.get("price")
        if not price_raw:
            log.warning(
                "Skipping PDF match without named 'price' group for %s",
                self.config.source_code,
            )
            return None
        parsed_price = parse_price(price_raw)
        if parsed_price is None:
            log.warning(
                "Skipping PDF match with unparseable price %r for %s",
                price_raw,
                self.config.source_code,
            )
            return None

        official_model = str(self.profile.fixed_model or groups.get("model") or "").strip()
        official_trim = str(entry.official_trim or groups.get("trim") or "").strip()
        official_powertrain = str(
            entry.official_powertrain or groups.get("powertrain") or ""
        ).strip() or None
        official_edition = str(
            entry.official_edition or groups.get("edition") or ""
        ).strip() or None
        availability_text = str(
            entry.availability_text or groups.get("availability") or ""
        ).strip() or None

        jato_trim = str(entry.jato_trim or groups.get("jato_trim") or "").strip()
        if not jato_trim and self.profile.copy_trim_to_jato_trim:
            jato_trim = official_trim

        jato_powertrain = str(
            entry.jato_powertrain
            or self.profile.fixed_jato_powertrain
            or groups.get("jato_powertrain")
            or ""
        ).strip() or None

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
                "pdf_url": self.profile.url,
                "pdf_urls": list(self._profile_urls()),
                "pattern": entry.pattern,
                "match_groups": groups,
                "price_delta": entry.price_delta,
            },
            jato_model=str(self.profile.fixed_jato_model or groups.get("jato_model") or ""),
            jato_trim=jato_trim,
            jato_powertrain=jato_powertrain,
            match_confidence=self.profile.match_confidence or 0.0,
            match_status=self.profile.match_status,
            match_reason=self.profile.match_reason,
        )
