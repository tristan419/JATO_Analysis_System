"""Regex-driven PDF text extractor for official price-list documents."""

from __future__ import annotations

from dataclasses import dataclass, field
import html
from io import BytesIO
import logging
import os
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
    entry_patterns: tuple[PdfTextEntryPattern, ...] = field(default_factory=tuple)
    timeout_seconds: int = DEFAULT_TIMEOUT
    retry_attempts: int = 0
    retry_delay_seconds: float = 0.0
    prefer_curl_download: bool = False
    browser_download_fallback: bool = False
    ignore_environment_proxy: bool = False
    direct_download_fallback: bool = False
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
    normalized_raw = re.sub(r"\s+", "", html.unescape(raw).replace("\xa0", " "))
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
        self._last_fetch_error: str | None = None
        if self.profile.ignore_environment_proxy:
            self._session.trust_env = False
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
            error = self._last_fetch_error or "pdf_text_extraction_failed"
            self.record_strategy_audit(
                url=self.profile.url,
                strategy="pdf_text",
                observations=[],
                winning_strategy=None,
                error=error,
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
        self._last_fetch_error = None
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
                self._remember_fetch_error(f"pdf_download_failed: {exc}")
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
            blob = self._fetch_pdf_bytes_with_curl(
                max(timeout, DEFAULT_CURL_FALLBACK_TIMEOUT),
            )
            if blob:
                return blob

        if self.profile.direct_download_fallback and not self.profile.ignore_environment_proxy:
            blob = self._fetch_pdf_bytes_without_environment_proxy(timeout)
            if blob:
                return blob

        if last_error:
            log.error("PDF request failed for %s: %s", self.config.source_code, last_error)
        return None

    def _fetch_pdf_bytes_without_environment_proxy(self, timeout: int) -> bytes | None:
        direct_timeout = max(timeout, DEFAULT_CURL_FALLBACK_TIMEOUT)
        blob = self._fetch_pdf_bytes_with_curl(
            direct_timeout,
            ignore_environment_proxy=True,
        )
        if blob:
            return blob

        session = requests.Session()
        session.trust_env = False
        session.headers.update(self._session.headers)
        try:
            response = session.get(self.profile.url, timeout=max(1, int(timeout)))
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            self._remember_fetch_error(f"pdf_direct_download_failed: {exc}")
            log.error(
                "PDF direct download fallback failed for %s: %s",
                self.config.source_code,
                exc,
            )
            return None

    def _fetch_pdf_bytes_with_curl(
        self,
        timeout: int,
        *,
        ignore_environment_proxy: bool | None = None,
    ) -> bytes | None:
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
                    env=self._subprocess_env(ignore_environment_proxy),
                )
                if result.returncode != 0:
                    stderr_text = result.stderr.decode(errors="replace")[:300]
                    self._remember_fetch_error(
                        f"pdf_curl_download_failed: {stderr_text}"
                    )
                    log.error(
                        "PDF curl fallback failed for %s: %s",
                        self.config.source_code,
                        stderr_text,
                    )
                    return None
                tmp.seek(0)
                blob = tmp.read()
        except (OSError, subprocess.SubprocessError) as exc:
            self._remember_fetch_error(f"pdf_curl_download_failed: {exc}")
            log.error("PDF curl fallback failed for %s: %s", self.config.source_code, exc)
            return None
        if not blob.startswith(b"%PDF"):
            self._remember_fetch_error("pdf_curl_download_failed: non-PDF content")
            log.error(
                "PDF curl fallback returned non-PDF content for %s",
                self.config.source_code,
            )
            return None
        return blob

    def _remember_fetch_error(self, error: str) -> None:
        self._last_fetch_error = error[:1000]

    def _subprocess_env(
        self,
        ignore_environment_proxy: bool | None = None,
    ) -> dict[str, str] | None:
        should_ignore = (
            self.profile.ignore_environment_proxy
            if ignore_environment_proxy is None
            else ignore_environment_proxy
        )
        if not should_ignore:
            return None
        env = os.environ.copy()
        for key in (
            "ALL_PROXY",
            "HTTPS_PROXY",
            "HTTP_PROXY",
            "all_proxy",
            "https_proxy",
            "http_proxy",
        ):
            env.pop(key, None)
        return env

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
