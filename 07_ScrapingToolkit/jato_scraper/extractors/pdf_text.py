"""Regex-driven PDF text extractor for official price-list documents."""

from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
import logging
from pathlib import Path
import re
import subprocess
import tempfile
import time

import requests
from pypdf import PdfReader

from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover - exercised only when dependency missing
    PlaywrightError = Exception
    PlaywrightTimeoutError = TimeoutError
    sync_playwright = None

log = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 60
DEFAULT_CURL_FALLBACK_TIMEOUT = 30
DEFAULT_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
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
    entry_patterns: tuple[PdfTextEntryPattern, ...] = field(default_factory=tuple)
    timeout_seconds: int = DEFAULT_TIMEOUT
    retry_attempts: int = 0
    retry_delay_seconds: float = 0.0
    prefer_curl_download: bool = False
    curl_download_fallback: bool = False
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

    def _fetch_pdf_bytes(self) -> bytes | None:
        last_error: Exception | None = None
        attempts = max(1, int(self.profile.retry_attempts or 0) + 1)
        timeout = max(1, int(self.profile.timeout_seconds or DEFAULT_TIMEOUT))
        if self.profile.prefer_curl_download:
            blob = self._fetch_pdf_bytes_with_curl(
                max(timeout, DEFAULT_CURL_FALLBACK_TIMEOUT),
            )
            if blob:
                return blob
            log.warning(
                "Preferred PDF curl download failed for %s; falling back to requests",
                self.config.source_code,
            )

        for attempt in range(1, attempts + 1):
            try:
                response = self._session.get(self.profile.url, timeout=timeout)
                response.raise_for_status()
                return response.content
            except requests.RequestException as exc:
                last_error = exc
                log.warning(
                    "PDF request attempt %s/%s failed for %s: %s",
                    attempt,
                    attempts,
                    self.config.source_code,
                    exc,
                )
                if attempt < attempts and self.profile.retry_delay_seconds > 0:
                    time.sleep(float(self.profile.retry_delay_seconds))

        if self.profile.browser_download_fallback:
            blob = self._fetch_pdf_bytes_with_browser(
                max(timeout, DEFAULT_CURL_FALLBACK_TIMEOUT),
                url,
            )
            if blob:
                return blob

        if self.profile.curl_download_fallback or self.profile.browser_download_fallback:
            blob = self._fetch_pdf_bytes_with_curl(
                max(timeout, DEFAULT_CURL_FALLBACK_TIMEOUT),
            )
            if blob:
                return blob

        if last_error:
            log.error("PDF request failed for %s: %s", self.config.source_code, last_error)
        return None

    def _fetch_pdf_bytes_with_curl(self, timeout: int) -> bytes | None:
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
                        self.profile.url,
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

    def _browser_user_agent(self) -> str:
        for key, value in self.profile.headers.items():
            if str(key).strip().lower() == "user-agent" and str(value).strip():
                return str(value).strip()
        return DEFAULT_BROWSER_USER_AGENT

    def _browser_extra_headers(self) -> dict[str, str]:
        return {
            str(key).strip(): str(value)
            for key, value in self.profile.headers.items()
            if str(key).strip() and str(key).strip().lower() != "user-agent"
        }

    def _fetch_pdf_bytes_with_browser(
        self,
        timeout: int,
        url: str | None = None,
    ) -> bytes | None:
        if sync_playwright is None:
            log.error(
                "PDF browser fallback unavailable for %s: playwright is not installed",
                self.config.source_code,
            )
            return None

        fetch_url = str(url or self.profile.url)
        timeout_ms = max(1, int(timeout)) * 1000
        browser = None
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                context = browser.new_context(
                    accept_downloads=True,
                    user_agent=self._browser_user_agent(),
                    extra_http_headers=self._browser_extra_headers(),
                )
                page = context.new_page()
                page.set_default_timeout(timeout_ms)
                try:
                    with page.expect_download(timeout=timeout_ms) as download_info:
                        try:
                            page.goto(
                                fetch_url,
                                wait_until="commit",
                                timeout=timeout_ms,
                            )
                        except PlaywrightError as exc:
                            if "Download is starting" not in str(exc):
                                raise
                    download = download_info.value
                    path = download.path()
                    blob = Path(path).read_bytes()
                finally:
                    context.close()
                browser.close()
                browser = None
        except (OSError, PlaywrightError, PlaywrightTimeoutError) as exc:
            log.error(
                "PDF browser fallback failed for %s at %s: %s",
                self.config.source_code,
                fetch_url,
                exc,
            )
            return None
        finally:
            if browser is not None:
                browser.close()

        if not blob.startswith(b"%PDF"):
            log.error(
                "PDF browser fallback returned non-PDF content for %s",
                self.config.source_code,
            )
            return None
        return blob

    def _extract_text(self) -> str:
        blob = self._fetch_pdf_bytes()
        if blob is None:
            return ""
        reader = PdfReader(BytesIO(blob))
        pages = [
            (page.extract_text() or "").replace("\xa0", " ")
            for page in reader.pages
        ]
        return "\n".join(pages)

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
