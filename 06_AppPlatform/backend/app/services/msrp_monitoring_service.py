from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from statistics import median
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from app.core.config import PROJECT_ROOT
from app.db.models import CurrentPrice, MsrpObservation, MsrpSource, PriceHistory
from app.infra import msrp_repository as msrp_repo
from app.services.country_service import to_display_country


POWERTRAIN_COLORS = {
    "BEV": "#16a34a",
    "HEV": "#f2b705",
    "PHEV": "#2563eb",
    "ICE": "#64748b",
    "MHEV": "#f97316",
}
POWERTRAIN_FALLBACK_COLOR = "#94a3b8"
OFFICIAL_SOURCE_TYPES = {
    "manufacturer_official",
    "official_api",
    "official_configurator",
    "official_price_list",
    "official_price_list_pdf",
    "official_website",
    "manufacturer_site",
}
SOURCE_RISK_MATCH_STATUSES = {"review_required", "rejected", "failed"}
DEFAULT_MONITORING_LIMIT = 500
MIN_MONITORING_PRICE_HISTORY_LIMIT = 20
MAX_MONITORING_PRICE_HISTORY_LIMIT = 500
DEFAULT_BACKFILL_SNAPSHOT_PREVIEW_CHARS = 20_000
MAX_BACKFILL_SNAPSHOT_PREVIEW_CHARS = 100_000
MONITORING_MODE_LIVE = "live"
MONITORING_MODE_SWEDEN_DEMO = "sweden_demo"
MONITORING_DIRECTIONS = {"drops", "increases", "all"}
DEFAULT_MONITORING_DIRECTION = "drops"
BACKFILL_SNAPSHOT_TEXT_EXTENSIONS = {".html", ".htm", ".md", ".txt", ".json", ".yaml", ".yml"}
BACKFILL_SNAPSHOT_CONTENT_TYPES = {
    ".html": "text/html",
    ".htm": "text/html",
    ".md": "text/markdown",
    ".txt": "text/plain",
    ".json": "application/json",
    ".yaml": "text/yaml",
    ".yml": "text/yaml",
    ".pdf": "application/pdf",
}
BACKFILL_CONTEXT_KEYS = (
    "historicalPriceBackfill",
    "historical_price_backfill",
    "priceBackfill",
    "price_backfill",
    "backfill",
)
BACKFILL_FLAG_KEYS = (
    "backfilled",
    "isBackfilled",
    "historicalBackfilled",
    "historical_backfilled",
    "historicalPriceBackfilled",
    "historical_price_backfilled",
)
OFFICIAL_PROMOTION_BACKFILL_KINDS = {
    "official_campaign_vs_regular_price",
    "official_campaign_savings_vs_current_price",
    "official_promotion_vs_ordinary_price",
}
AUDIT_PRIORITY_ORDER = {
    "auto_pass": 0,
    "sample": 1,
    "priority_audit": 2,
    "block": 3,
}


class _HtmlTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._skip_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
        if tag in {"br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1
        if tag in {"p", "div", "li", "tr", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = " ".join(data.split())
        if text:
            self.parts.append(text)

    def text(self) -> str:
        lines = [line.strip() for line in "".join(self.parts).splitlines()]
        return "\n".join(line for line in lines if line)
AUDIT_ACTION_LABELS = {
    "auto_pass": "Auto pass after next scrape confirms unchanged MSRP.",
    "sample": "Sample one source snapshot in the routine spot-check batch.",
    "priority_audit": "Prioritize manual audit before accepting the movement.",
    "block": "Block automatic acceptance until lifecycle/source evidence is verified.",
}
BATCH_A_COUNTRIES: tuple[dict[str, str], ...] = (
    {"code": "SE", "countryLabel": "Sweden"},
    {"code": "FI", "countryLabel": "Finland"},
    {"code": "NO", "countryLabel": "Norway"},
    {"code": "DK", "countryLabel": "Denmark"},
    {"code": "HU", "countryLabel": "Hungary"},
    {"code": "HR", "countryLabel": "Croatia"},
    {"code": "AT", "countryLabel": "Austria"},
    {"code": "CZ", "countryLabel": "Czechia"},
    {"code": "DE", "countryLabel": "Germany"},
    {"code": "FR", "countryLabel": "France"},
    {"code": "IT", "countryLabel": "Italy"},
    {"code": "PL", "countryLabel": "Poland"},
)
OFFICIAL_SWEDEN_2026_OFFER_SIGNALS: tuple[dict[str, object], ...] = (
    {
        "signalId": "se-2026-toyota-c-hr-plus-official-offer",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "TOYOTA",
        "jatoModel": "C-HR+",
        "jatoTrim": "Official offer signal",
        "jatoPowertrain": "BEV",
        "primaryType": "cash_discount",
        "offerTypes": ["cash_discount", "finance_offer"],
        "headline": "Toyota C-HR+ official campaign signal",
        "valueLabel": "Up to 59,000 SEK saving; 0% Easy Billån finance signal",
        "cashDiscountSek": 59000,
        "interestRatePct": 0,
        "monthlyPaymentSek": None,
        "benefitLabels": [],
        "sourceUrl": "https://www.toyota.se/",
        "sourceLabel": "Toyota Sweden official campaign content",
        "sourceObservedDate": "2026-06-24",
        "notes": "Official offer evidence exists, but this is not a price-history movement until current MSRP scraping has a matching row.",
        "auditPriority": "priority_audit",
        "samplingBucket": "official_offer_signal",
        "matchStatus": "pending_current_price_match",
        "modelAliases": ["c-hr+", "c hr plus", "chr plus", "toyota c-hr+"],
    },
    {
        "signalId": "se-2026-toyota-corolla-touring-sports-official-offer",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "TOYOTA",
        "jatoModel": "Corolla Touring Sports",
        "jatoTrim": "Official offer signal",
        "jatoPowertrain": "HEV",
        "primaryType": "cash_discount",
        "offerTypes": ["cash_discount", "purchase_benefit"],
        "headline": "Toyota Corolla Touring Sports official customer-discount signal",
        "valueLabel": "Up to 25,000 SEK customer discount; leasing benefit signal",
        "cashDiscountSek": 25000,
        "interestRatePct": None,
        "monthlyPaymentSek": None,
        "benefitLabels": ["service", "roadside assistance"],
        "sourceUrl": "https://www.toyota.se/",
        "sourceLabel": "Toyota Sweden official campaign content",
        "sourceObservedDate": "2026-06-24",
        "notes": "Official customer-discount evidence should be sampled against current MSRP rows when Toyota Sweden scraping is loaded.",
        "auditPriority": "priority_audit",
        "samplingBucket": "official_offer_signal",
        "matchStatus": "pending_current_price_match",
        "modelAliases": ["corolla touring sports", "corolla ts"],
    },
    {
        "signalId": "se-2026-toyota-bz4x-official-finance",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "TOYOTA",
        "jatoModel": "bZ4X",
        "jatoTrim": "Official offer signal",
        "jatoPowertrain": "BEV",
        "primaryType": "finance_offer",
        "offerTypes": ["finance_offer"],
        "headline": "Toyota bZ4X official 0% finance signal",
        "valueLabel": "0% Easy Billån finance signal",
        "cashDiscountSek": None,
        "interestRatePct": 0,
        "monthlyPaymentSek": None,
        "benefitLabels": [],
        "sourceUrl": "https://www.toyota.se/",
        "sourceLabel": "Toyota Sweden official campaign content",
        "sourceObservedDate": "2026-06-24",
        "notes": "Finance-only official signal; keep separate from permanent MSRP movements until scraper evidence is matched.",
        "auditPriority": "sample",
        "samplingBucket": "official_offer_signal",
        "matchStatus": "pending_current_price_match",
        "modelAliases": ["bz4x", "toyota bz4x"],
    },
    {
        "signalId": "se-2026-hyundai-kona-electric-official-campaign",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "HYUNDAI",
        "jatoModel": "KONA",
        "jatoTrim": "Electric official campaign",
        "jatoPowertrain": "BEV",
        "primaryType": "cash_discount",
        "offerTypes": ["cash_discount", "lease_offer"],
        "headline": "Hyundai KONA Electric campaign price signal",
        "valueLabel": "Campaign price 387,920 SEK vs ordinary 484,900 SEK; monthly cost from 2,868 SEK",
        "cashDiscountSek": 96980,
        "interestRatePct": None,
        "monthlyPaymentSek": 2868,
        "benefitLabels": [],
        "sourceUrl": "https://www.hyundai.com/se/sv.html",
        "sourceLabel": "Hyundai Sweden official offer content",
        "sourceObservedDate": "2026-06-24",
        "notes": "Official campaign-vs-ordinary boundary; audit before treating as a durable MSRP drop.",
        "auditPriority": "priority_audit",
        "samplingBucket": "official_offer_signal",
        "matchStatus": "pending_current_price_match",
        "modelAliases": ["kona", "kona electric"],
    },
    {
        "signalId": "se-2026-hyundai-tucson-phev-official-campaign",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "HYUNDAI",
        "jatoModel": "TUCSON",
        "jatoTrim": "Plug-In Hybrid official campaign",
        "jatoPowertrain": "PHEV",
        "primaryType": "cash_discount",
        "offerTypes": ["cash_discount", "lease_offer"],
        "headline": "Hyundai TUCSON Plug-In Hybrid customer-discount signal",
        "valueLabel": "75,000 SEK customer discount; leasing from 7,495 SEK/month",
        "cashDiscountSek": 75000,
        "interestRatePct": None,
        "monthlyPaymentSek": 7495,
        "benefitLabels": [],
        "sourceUrl": "https://www.hyundai.com/se/sv.html",
        "sourceLabel": "Hyundai Sweden official offer content",
        "sourceObservedDate": "2026-06-24",
        "notes": "Official discount evidence; keep in spot-check queue until current MSRP and ordinary price semantics are matched.",
        "auditPriority": "priority_audit",
        "samplingBucket": "official_offer_signal",
        "matchStatus": "pending_current_price_match",
        "modelAliases": ["tucson", "tucson plug-in hybrid", "tucson phev"],
    },
    {
        "signalId": "se-2026-hyundai-inster-official-private-lease",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "HYUNDAI",
        "jatoModel": "INSTER",
        "jatoTrim": "Private lease official campaign",
        "jatoPowertrain": "BEV",
        "primaryType": "lease_offer",
        "offerTypes": ["lease_offer"],
        "headline": "Hyundai INSTER private-leasing signal",
        "valueLabel": "Private leasing from 3,395 SEK/month",
        "cashDiscountSek": None,
        "interestRatePct": None,
        "monthlyPaymentSek": 3395,
        "benefitLabels": [],
        "sourceUrl": "https://www.hyundai.com/se/sv.html",
        "sourceLabel": "Hyundai Sweden official offer content",
        "sourceObservedDate": "2026-06-24",
        "notes": "Lease-only official signal; useful for incentive monitoring, not a direct MSRP movement.",
        "auditPriority": "sample",
        "samplingBucket": "official_offer_signal",
        "matchStatus": "pending_current_price_match",
        "modelAliases": ["inster"],
    },
    {
        "signalId": "se-2026-kia-ev4-official-private-lease",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "KIA",
        "jatoModel": "EV4",
        "jatoTrim": "Private lease official campaign",
        "jatoPowertrain": "BEV",
        "primaryType": "lease_offer",
        "offerTypes": ["lease_offer", "finance_offer", "purchase_benefit"],
        "headline": "Kia EV4 official private-leasing offer signal",
        "valueLabel": "Private leasing from 3,995 SEK/month; finance/business offer compatibility signal",
        "cashDiscountSek": None,
        "interestRatePct": None,
        "monthlyPaymentSek": 3995,
        "benefitLabels": ["service agreement", "winter wheels", "insurance"],
        "sourceUrl": "https://www.kia.com/se/kopa/erbjudanden/",
        "sourceLabel": "Kia Sweden official offers page",
        "sourceObservedDate": "2026-06-24",
        "notes": "Official offer page also advertises included benefits for leasing; sample as incentive evidence, not MSRP history.",
        "auditPriority": "sample",
        "samplingBucket": "official_offer_signal",
        "matchStatus": "pending_current_price_match",
        "modelAliases": ["ev4", "kia ev4"],
    },
    {
        "signalId": "se-2026-byd-sealion-7-official-finance",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "BYD",
        "jatoModel": "SEALION 7",
        "jatoTrim": "Official finance campaign",
        "jatoPowertrain": "BEV",
        "primaryType": "finance_offer",
        "offerTypes": ["finance_offer"],
        "headline": "BYD SEALION 7 official 0% campaign-interest signal",
        "valueLabel": "0% campaign interest signal",
        "cashDiscountSek": None,
        "interestRatePct": 0,
        "monthlyPaymentSek": None,
        "benefitLabels": [],
        "sourceUrl": "https://www.byd.com/se",
        "sourceLabel": "BYD Sweden official offer content",
        "sourceObservedDate": "2026-06-24",
        "notes": "Finance-only signal for incentive monitoring; do not classify as MSRP drop without price-history evidence.",
        "auditPriority": "sample",
        "samplingBucket": "official_offer_signal",
        "matchStatus": "pending_current_price_match",
        "modelAliases": ["sealion 7", "byd sealion 7"],
    },
    {
        "signalId": "se-2026-byd-seal-u-dmi-official-private-lease",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "BYD",
        "jatoModel": "SEAL U DM-i",
        "jatoTrim": "Private lease official campaign",
        "jatoPowertrain": "PHEV",
        "primaryType": "lease_offer",
        "offerTypes": ["lease_offer"],
        "headline": "BYD SEAL U DM-i private-leasing signal",
        "valueLabel": "Private leasing from 3,995 SEK/month",
        "cashDiscountSek": None,
        "interestRatePct": None,
        "monthlyPaymentSek": 3995,
        "benefitLabels": [],
        "sourceUrl": "https://www.byd.com/se",
        "sourceLabel": "BYD Sweden official offer content",
        "sourceObservedDate": "2026-06-24",
        "notes": "Lease-only official signal; useful for incentive monitoring and later model matching.",
        "auditPriority": "sample",
        "samplingBucket": "official_offer_signal",
        "matchStatus": "pending_current_price_match",
        "modelAliases": ["seal u dm-i", "seal u dmi", "byd seal u"],
    },
    {
        "signalId": "se-2026-geely-official-retail-coverage-gap",
        "country": "SE",
        "countryLabel": "Sweden",
        "brand": "GEELY",
        "jatoModel": "GEELY AUTO",
        "jatoTrim": "Coverage gap",
        "jatoPowertrain": "UNKNOWN",
        "primaryType": "coverage_gap",
        "offerTypes": ["coverage_gap"],
        "headline": "Geely Sweden consumer offer coverage gap",
        "valueLabel": "No official Sweden consumer-retail offer page found in current source set",
        "cashDiscountSek": None,
        "interestRatePct": None,
        "monthlyPaymentSek": None,
        "benefitLabels": [],
        "sourceUrl": "https://zgh.com/investment/geely-sweden-holding/?lang=en",
        "sourceLabel": "Geely Sweden Holding public company page",
        "sourceObservedDate": "2026-06-24",
        "notes": "Holding-company page is not a consumer MSRP/offer source; keep as a source-coverage gap unless Geely group brands are mapped separately.",
        "auditPriority": "block",
        "samplingBucket": "official_offer_signal_gap",
        "matchStatus": "source_coverage_gap",
        "modelAliases": ["geely", "geely auto"],
    },
)
SWEDEN_DEMO_SCENARIOS: dict[tuple[str, str, str], dict[str, object]] = {
    ("VOLVO", "EX90", "BEV"): {
        "changePct": -6.8,
        "daysAgo": 6,
        "lengthMm": 5037,
        "scenario": "2026 spring MSRP repositioning",
    },
    ("VOLVO", "XC90", "PHEV"): {
        "changePct": 2.9,
        "daysAgo": 12,
        "lengthMm": 4953,
        "scenario": "2026 plug-in hybrid package price update",
    },
    ("SKODA", "ENYAQ", "BEV"): {
        "changePct": -4.6,
        "daysAgo": 18,
        "lengthMm": 4653,
        "scenario": "2026 BEV campaign price cut",
    },
    ("VOLKSWAGEN", "TAYRON", "UNKNOWN"): {
        "changePct": -3.2,
        "daysAgo": 24,
        "lengthMm": 4770,
        "scenario": "Configurator availability signal",
        "sourceStatus": "review_required",
        "riskReasons": ["demo_unavailable_signal", "demo_backfilled_price"],
        "lifecycleStatus": "removed_from_configurator",
    },
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_powertrain(value: object | None) -> str:
    text = str(value or "").strip().upper()
    if text in POWERTRAIN_COLORS:
        return text
    if "PHEV" in text or "PLUGIN" in text or "PLUG-IN" in text:
        return "PHEV"
    if "MHEV" in text or "MILD" in text:
        return "MHEV"
    if "BEV" in text or "ELECTRIC" in text or text == "EV":
        return "BEV"
    if "HEV" in text or "HYBRID" in text:
        return "HEV"
    if text in {"PETROL", "GASOLINE", "DIESEL", "ICE"}:
        return "ICE"
    return text or "UNKNOWN"


def _float_or_none(value: object | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def _iso(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _text_or_none(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalized_filter_text(value: object | None) -> str:
    return str(value or "").strip().casefold()


def _offer_signal_country_matches(signal: dict[str, object], country: str | None) -> bool:
    if not country:
        return True
    query = _normalized_filter_text(country)
    query_label = _normalized_filter_text(to_display_country(country))
    signal_country = _normalized_filter_text(signal.get("country"))
    signal_label = _normalized_filter_text(signal.get("countryLabel"))
    return query in {signal_country, signal_label} or query_label == signal_label


def _offer_signal_brand_matches(signal: dict[str, object], brand: str | None) -> bool:
    if not brand:
        return True
    return _normalized_filter_text(signal.get("brand")) == _normalized_filter_text(brand)


def _offer_signal_model_matches(signal: dict[str, object], jato_model: str | None) -> bool:
    if not jato_model:
        return True
    query = _normalized_filter_text(jato_model)
    candidates = {
        _normalized_filter_text(signal.get("jatoModel")),
        _normalized_filter_text(signal.get("headline")),
    }
    candidates.update(
        _normalized_filter_text(alias)
        for alias in list(signal.get("modelAliases") or [])
    )
    return any(query == candidate or query in candidate for candidate in candidates if candidate)


def _build_offer_signals(
    generated_at: datetime,
    *,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
) -> list[dict[str, object]]:
    signals: list[dict[str, object]] = []
    for signal in OFFICIAL_SWEDEN_2026_OFFER_SIGNALS:
        if not _offer_signal_country_matches(signal, country):
            continue
        if not _offer_signal_brand_matches(signal, brand):
            continue
        if not _offer_signal_model_matches(signal, jato_model):
            continue
        payload = {
            key: value
            for key, value in signal.items()
            if key != "modelAliases"
        }
        payload.setdefault("offerValidUntil", None)
        payload["capturedAtUtc"] = generated_at.isoformat()
        signals.append(payload)
    return signals


def _truthy_flag(value: object | None) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "backfilled"}


def _backfill_snapshot_root() -> Path:
    return (PROJECT_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_backfill").resolve()


def _backfill_snapshot_path(path_ref: str) -> Path | None:
    text = path_ref.strip()
    if not text:
        return None
    candidate = Path(text)
    resolved = candidate.resolve() if candidate.is_absolute() else (PROJECT_ROOT / candidate).resolve()
    try:
        resolved.relative_to(_backfill_snapshot_root())
    except ValueError:
        return None
    return resolved


def _plain_snapshot_preview(path: Path, raw_text: str) -> str:
    if path.suffix.lower() not in {".html", ".htm"}:
        return raw_text
    extractor = _HtmlTextExtractor()
    extractor.feed(raw_text)
    return extractor.text()


def _pdf_snapshot_preview(path: Path, max_chars: int) -> tuple[str | None, str | None]:
    try:
        from pypdf import PdfReader
    except ImportError:
        return None, "pypdf is not installed; PDF evidence is stored but cannot be text-previewed."

    try:
        reader = PdfReader(str(path))
        pages: list[str] = []
        for index, page in enumerate(reader.pages):
            page_text = str(page.extract_text() or "").strip()
            if page_text:
                pages.append(f"--- Page {index + 1} ---\n{page_text}")
            if sum(len(item) for item in pages) > max_chars:
                break
    except Exception as exc:
        return None, f"PDF text extraction failed: {type(exc).__name__}."

    content = "\n\n".join(pages).strip()
    if not content:
        return None, "PDF evidence is stored, but text extraction returned no readable text."
    return content, None


def build_msrp_backfill_snapshot_preview(
    path_ref: str,
    *,
    max_chars: int = DEFAULT_BACKFILL_SNAPSHOT_PREVIEW_CHARS,
) -> dict[str, object]:
    safe_max_chars = max(1_000, min(MAX_BACKFILL_SNAPSHOT_PREVIEW_CHARS, int(max_chars)))
    path = _backfill_snapshot_path(path_ref)
    if path is None:
        return {
            "exists": False,
            "previewable": False,
            "status": "blocked",
            "path": path_ref,
            "fileName": Path(path_ref).name,
            "contentType": None,
            "sizeBytes": None,
            "content": None,
            "truncated": False,
            "message": "Snapshot preview is limited to MSRP backfill artifacts.",
        }
    relative_path = str(path.relative_to(PROJECT_ROOT))
    suffix = path.suffix.lower()
    content_type = BACKFILL_SNAPSHOT_CONTENT_TYPES.get(suffix, "application/octet-stream")
    if not path.exists() or not path.is_file():
        return {
            "exists": False,
            "previewable": False,
            "status": "missing",
            "path": relative_path,
            "fileName": path.name,
            "contentType": content_type,
            "sizeBytes": None,
            "content": None,
            "truncated": False,
            "message": "Snapshot artifact was not found on disk.",
        }
    size_bytes = path.stat().st_size
    if suffix == ".pdf":
        content, extraction_message = _pdf_snapshot_preview(path, safe_max_chars)
        if content is None:
            return {
                "exists": True,
                "previewable": False,
                "status": "unsupported",
                "path": relative_path,
                "fileName": path.name,
                "contentType": content_type,
                "sizeBytes": size_bytes,
                "content": None,
                "truncated": False,
                "message": extraction_message,
            }
        truncated = len(content) > safe_max_chars
        return {
            "exists": True,
            "previewable": True,
            "status": "ok",
            "path": relative_path,
            "fileName": path.name,
            "contentType": content_type,
            "sizeBytes": size_bytes,
            "content": content[:safe_max_chars],
            "truncated": truncated,
            "message": "Preview is truncated." if truncated else "Preview loaded.",
        }
    if suffix not in BACKFILL_SNAPSHOT_TEXT_EXTENSIONS:
        return {
            "exists": True,
            "previewable": False,
            "status": "unsupported",
            "path": relative_path,
            "fileName": path.name,
            "contentType": content_type,
            "sizeBytes": size_bytes,
            "content": None,
            "truncated": False,
            "message": f"{suffix or 'This file type'} is stored as evidence but is not text-previewable in the monitor.",
        }
    raw_text = path.read_text(encoding="utf-8", errors="replace")
    content = _plain_snapshot_preview(path, raw_text)
    truncated = len(content) > safe_max_chars
    return {
        "exists": True,
        "previewable": True,
        "status": "ok",
        "path": relative_path,
        "fileName": path.name,
        "contentType": content_type,
        "sizeBytes": size_bytes,
        "content": content[:safe_max_chars],
        "truncated": truncated,
        "message": "Preview is truncated." if truncated else "Preview loaded.",
    }


def _event_key(item: CurrentPrice) -> tuple[str, str, str]:
    return (
        str(item.brand or "").strip(),
        str(item.jato_model or "").strip(),
        _normalize_powertrain(item.jato_powertrain),
    )


def _is_monitoring_fixture_current_price(item: CurrentPrice) -> bool:
    brand = str(item.brand or "").strip().upper()
    model = str(item.jato_model or "").strip().upper()
    trim = str(item.jato_trim or "").strip().upper()
    source_url = str(item.source_url or "").strip().lower()
    source_snapshot = str(item.source_snapshot_path or "").strip().lower()
    joined = " ".join([brand, model, trim])
    if brand == "CODEX":
        return True
    if "SMOKE MODEL" in joined or "SMOKE_" in joined:
        return True
    return "codex_msrp_smoke" in source_url or "codex_msrp_smoke" in source_snapshot


def _filter_monitoring_current_prices(
    current_prices: list[CurrentPrice],
) -> list[CurrentPrice]:
    return [
        item
        for item in current_prices
        if not _is_monitoring_fixture_current_price(item)
    ]


def _change_pct(current_value: float | None, previous_value: float | None) -> float | None:
    if current_value is None or previous_value in {None, 0}:
        return None
    return round((current_value - previous_value) / previous_value * 100.0, 2)


def _changed_in_window(period: PriceHistory, since: datetime | None) -> bool:
    if since is None:
        return True
    changed_at = period.valid_from_utc
    if changed_at.tzinfo is None:
        changed_at = changed_at.replace(tzinfo=timezone.utc)
    return changed_at >= since


def _normalize_monitoring_direction(value: str | None) -> str:
    direction = str(value or DEFAULT_MONITORING_DIRECTION).strip().lower()
    return direction if direction in MONITORING_DIRECTIONS else DEFAULT_MONITORING_DIRECTION


def _change_matches_direction(change_pct: float | None, direction: str) -> bool:
    if change_pct is None:
        return False
    if direction == "all":
        return True
    return change_pct < 0 if direction == "drops" else change_pct > 0


def _monitoring_since(
    generated_at: datetime,
    safe_window_days: int,
    from_date: date | None,
) -> datetime:
    if from_date is not None:
        return datetime(
            from_date.year,
            from_date.month,
            from_date.day,
            tzinfo=timezone.utc,
        )
    return generated_at - timedelta(days=safe_window_days)


def _monitoring_price_history_limit(
    generated_at: datetime,
    since: datetime,
) -> int:
    normalized_since = since
    if normalized_since.tzinfo is None:
        normalized_since = normalized_since.replace(tzinfo=timezone.utc)
    span_days = max(1, (generated_at - normalized_since).days + 1)
    return min(
        MAX_MONITORING_PRICE_HISTORY_LIMIT,
        max(MIN_MONITORING_PRICE_HISTORY_LIMIT, span_days + 2),
    )


def _monitoring_filters_payload(
    *,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    safe_window_days: int,
    from_date: date | None,
    safe_threshold_pct: float,
    safe_limit: int,
    direction: str,
) -> dict[str, object]:
    return {
        "country": country,
        "brand": brand,
        "jatoModel": jato_model,
        "windowDays": safe_window_days,
        "fromDate": from_date.isoformat() if from_date is not None else None,
        "thresholdPct": safe_threshold_pct,
        "limit": safe_limit,
        "direction": direction,
    }


def _extract_numeric_length(value: object | None) -> int | None:
    parsed = _float_or_none(value)
    if parsed is None:
        return None
    if 2500 <= parsed <= 7000:
        return int(round(parsed))
    return None


def _extract_length_from_context(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, dict):
        direct_keys = (
            "lengthMm",
            "length_mm",
            "vehicleLengthMm",
            "vehicle_length_mm",
            "length",
            "Length",
            "length (mm)",
        )
        for key in direct_keys:
            if key in value:
                length = _extract_numeric_length(value.get(key))
                if length is not None:
                    return length
        for key, nested in value.items():
            if "length" in str(key).lower():
                length = _extract_numeric_length(nested)
                if length is not None:
                    return length
            length = _extract_length_from_context(nested)
            if length is not None:
                return length
    if isinstance(value, list):
        for item in value:
            length = _extract_length_from_context(item)
            if length is not None:
                return length
    return None


def _extract_dryrun_run_id(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        for key in ("dryrunRunId", "dryrun_run_id", "runId", "run_id"):
            text = str(value.get(key) or "").strip()
            if text.startswith("msrp-dryrun-"):
                return text
        for nested in value.values():
            run_id = _extract_dryrun_run_id(nested)
            if run_id:
                return run_id
    if isinstance(value, list):
        for item in value:
            run_id = _extract_dryrun_run_id(item)
            if run_id:
                return run_id
    text = str(value).strip()
    return text if text.startswith("msrp-dryrun-") else None


def _first_text(payload: dict[str, object], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        if key in payload:
            text = _text_or_none(payload.get(key))
            if text:
                return text
    return None


def _find_backfill_payload(value: object | None) -> dict[str, object] | None:
    if isinstance(value, dict):
        for key in BACKFILL_CONTEXT_KEYS:
            nested = value.get(key)
            if isinstance(nested, dict):
                if nested.get("enabled") is False:
                    continue
                return nested
            if _truthy_flag(nested):
                return {"enabled": True}
        if any(_truthy_flag(value.get(key)) for key in BACKFILL_FLAG_KEYS):
            return value
        for nested in value.values():
            payload = _find_backfill_payload(nested)
            if payload is not None:
                return payload
    if isinstance(value, list):
        for item in value:
            payload = _find_backfill_payload(item)
            if payload is not None:
                return payload
    return None


def _extract_backfill_evidence(value: object | None) -> dict[str, object]:
    payload = _find_backfill_payload(value)
    if payload is None or payload.get("enabled") is False:
        return {}
    return {
        "backfilled": True,
        "backfillKind": _first_text(payload, ("kind", "type", "sourceType", "source_type")) or "historical_price_backfill",
        "backfillSourceLabel": _first_text(payload, ("sourceLabel", "source_label", "label", "title", "source")),
        "backfillEffectiveDate": _first_text(payload, ("effectiveDate", "effective_date", "validFrom", "valid_from", "priceDate", "price_date")),
        "backfillEvidenceUrl": _first_text(payload, ("evidenceUrl", "evidence_url", "sourceUrl", "source_url", "url")),
        "backfillSnapshotPath": _first_text(payload, ("snapshotPath", "snapshot_path", "cachePath", "cache_path")),
        "backfillCapturedAtUtc": _first_text(payload, ("capturedAtUtc", "captured_at_utc", "observedAtUtc", "observed_at_utc")),
        "backfillNotes": _first_text(payload, ("notes", "note", "description")),
    }


def _extract_related_official_evidence(value: object | None) -> list[dict[str, object]]:
    if not isinstance(value, dict):
        return []
    raw_evidence_candidates = (
        value.get("relatedOfficialEvidence"),
        value.get("related_official_evidence"),
        value.get("secondaryOfficialEvidence"),
        value.get("secondary_official_evidence"),
        value.get("officialEvidence"),
        value.get("official_evidence"),
    )
    items: list[object] = []
    for raw_evidence in raw_evidence_candidates:
        if raw_evidence is None:
            continue
        if isinstance(raw_evidence, list):
            items.extend(raw_evidence)
        else:
            items.append(raw_evidence)
    related: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        url = _first_text(item, ("url", "sourceUrl", "source_url", "evidenceUrl", "evidence_url", "pdfUrl", "pdf_url"))
        label = _first_text(item, ("label", "title", "sourceLabel", "source_label", "source"))
        snapshot_path = _first_text(item, ("snapshotPath", "snapshot_path", "cachePath", "cache_path", "pdfSnapshotPath", "pdf_snapshot_path"))
        payload_hash = _first_text(item, ("payloadHash", "payload_hash", "sourcePayloadHash", "source_payload_hash", "pdfPayloadHash", "pdf_payload_hash"))
        if label is None and (
            "pdfUrl" in item
            or "pdf_url" in item
            or str(url or "").lower().endswith(".pdf")
            or str(snapshot_path or "").lower().endswith(".pdf")
        ):
            label = "Official PDF price list"
        payload = {
            "url": url,
            "label": label,
            "snapshotPath": snapshot_path,
            "payloadHash": payload_hash,
        }
        cleaned = {key: value for key, value in payload.items() if value is not None}
        if cleaned:
            related.append(cleaned)
    return related


def _related_official_evidence_from_observation(observation: MsrpObservation) -> list[dict[str, object]]:
    seen: set[tuple[str, str, str, str]] = set()
    related: list[dict[str, object]] = []
    for context in (observation.source_context_json, observation.match_reason_json):
        for item in _extract_related_official_evidence(context):
            key = (
                str(item.get("url") or ""),
                str(item.get("label") or ""),
                str(item.get("snapshotPath") or ""),
                str(item.get("payloadHash") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            related.append(item)
    return related


def _backfill_evidence_from_observation(
    observation: MsrpObservation | None,
    role: str,
) -> dict[str, object]:
    if observation is None:
        return {}
    evidence = {
        **_extract_backfill_evidence(observation.source_context_json),
        **_extract_backfill_evidence(observation.match_reason_json),
    }
    if not evidence.get("backfilled"):
        return {}
    evidence["backfillObservationId"] = str(observation.observation_id)
    evidence["backfillEvidenceRole"] = role
    evidence["backfillEvidenceUrl"] = evidence.get("backfillEvidenceUrl") or observation.source_url
    evidence["backfillSnapshotPath"] = evidence.get("backfillSnapshotPath") or observation.source_snapshot_path
    evidence["backfillPayloadHash"] = observation.source_payload_hash
    evidence["backfillCapturedAtUtc"] = evidence.get("backfillCapturedAtUtc") or _iso(observation.observed_at_utc)
    related_official_evidence = _related_official_evidence_from_observation(observation)
    if related_official_evidence:
        evidence["relatedOfficialEvidence"] = related_official_evidence
    return {key: value for key, value in evidence.items() if value is not None}


def _median_or_none(values: list[float]) -> float | None:
    return round(float(median(values)), 2) if values else None


def _range_payload(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "max": None}
    return {"min": round(min(values), 2), "max": round(max(values), 2)}


def _summary_payload(events: list[dict[str, object]]) -> dict[str, object]:
    audit_priority_counts = {
        priority: sum(1 for item in events if item.get("auditPriority") == priority)
        for priority in AUDIT_PRIORITY_ORDER
    }
    country_events = [
        country_event
        for event in events
        for country_event in list(event.get("countries") or [])
    ]
    campaign_boundary_count = sum(
        1
        for item in country_events
        if item.get("samplingBucket") == "campaign_promotion_boundary"
        or "campaign_promotion_boundary:not_permanent_msrp_cut" in list(item.get("auditReasons") or [])
    )
    return {
        "eventCount": len(events),
        "timelineEventCount": sum(int(item.get("timelineEventCount") or 0) for item in events),
        "affectedCountryCount": len({country_event.get("country") for country_event in country_events}),
        "sourceRiskCount": sum(int(item.get("sourceRiskCount") or 0) for item in events),
        "reviewRequiredCount": sum(int(item.get("reviewRequiredCount") or 0) for item in events),
        "outlierCount": sum(int(item.get("outlierCount") or 0) for item in events),
        "lengthMissingCount": sum(1 for item in events if item.get("lengthMissing")),
        "campaignBoundaryCount": campaign_boundary_count,
        "auditPriorityCounts": audit_priority_counts,
        "autoPassCount": audit_priority_counts["auto_pass"],
        "sampleCount": audit_priority_counts["sample"],
        "priorityAuditCount": audit_priority_counts["priority_audit"],
        "blockCount": audit_priority_counts["block"],
    }


def _period_has_backfill(
    observation_by_id: dict[str, MsrpObservation],
    period: PriceHistory,
) -> bool:
    observation = observation_by_id.get(str(period.started_by_observation_id))
    if observation is None:
        return False
    return bool(_backfill_evidence_from_observation(observation, "coverage"))


def _load_histories_for_current_prices(
    session: Session,
    current_prices: list[CurrentPrice],
    *,
    per_key_limit: int = 20,
) -> dict[str, list[PriceHistory]]:
    histories: dict[str, list[PriceHistory]] = {}
    for item in current_prices:
        histories[str(item.current_price_id)] = msrp_repo.list_price_history(
            session,
            item.country,
            item.brand,
            item.jato_model,
            item.jato_trim,
            item.jato_powertrain,
            per_key_limit,
        )
    return histories


def _load_observations_for_histories(
    session: Session,
    histories: dict[str, list[PriceHistory]],
) -> dict[str, MsrpObservation]:
    observation_ids = {
        period.started_by_observation_id
        for history in histories.values()
        for period in history
        if period.started_by_observation_id is not None
    }
    observations = msrp_repo.list_observations_by_ids(
        session,
        list(observation_ids),
    )
    return {str(item.observation_id): item for item in observations}


def _launch_candidate_from_history(
    history: list[PriceHistory],
) -> PriceHistory | None:
    if len(history) != 1:
        return None
    period = history[0]
    if period.valid_to_utc is not None:
        return None
    return period


def _build_batch_a_coverage(session: Session) -> dict[str, object]:
    countries: list[dict[str, object]] = []
    for country in BATCH_A_COUNTRIES:
        code = country["code"]
        current_prices = msrp_repo.list_current_prices(
            session,
            code,
            None,
            None,
            DEFAULT_MONITORING_LIMIT,
            0,
        )
        current_prices = _filter_monitoring_current_prices(current_prices)
        histories = _load_histories_for_current_prices(session, current_prices)
        observation_by_id = _load_observations_for_histories(session, histories)
        price_history_rows = 0
        closed_period_count = 0
        backfill_period_count = 0
        launch_candidate_count = 0
        first_seen_values: list[datetime] = []
        current_started_values: list[datetime] = []

        for item in current_prices:
            history = histories.get(str(item.current_price_id), [])
            price_history_rows += len(history)
            if _launch_candidate_from_history(history) is not None:
                launch_candidate_count += 1
            for period in history:
                if period.valid_from_utc is not None:
                    first_seen_values.append(period.valid_from_utc)
                if period.valid_to_utc is None and period.valid_from_utc is not None:
                    current_started_values.append(period.valid_from_utc)
                if period.valid_to_utc is not None:
                    closed_period_count += 1
                if _period_has_backfill(observation_by_id, period):
                    backfill_period_count += 1

        has_current = len(current_prices) > 0
        has_history = closed_period_count > 0
        has_backfill = backfill_period_count > 0
        if has_backfill:
            status = "backfilled"
        elif has_history:
            status = "history_without_backfill"
        elif has_current:
            status = "current_only"
        else:
            status = "not_loaded"

        countries.append(
            {
                "code": code,
                "countryLabel": country["countryLabel"],
                "currentRows": len(current_prices),
                "priceHistoryRows": price_history_rows,
                "closedPeriodCount": closed_period_count,
                "backfillPeriodCount": backfill_period_count,
                "launchCandidateCount": launch_candidate_count,
                "hasCurrent": has_current,
                "hasHistoricalMonitoring": has_history,
                "hasHistoricalBackfill": has_backfill,
                "status": status,
                "firstSeenAtUtc": _iso(min(first_seen_values) if first_seen_values else None),
                "latestCurrentStartedAtUtc": _iso(max(current_started_values) if current_started_values else None),
            }
        )

    return {
        "batchCode": "country_msrp_batch_a",
        "countryCount": len(countries),
        "loadedCountryCount": sum(1 for item in countries if item["hasCurrent"]),
        "historicalMonitoringCountryCount": sum(1 for item in countries if item["hasHistoricalMonitoring"]),
        "historicalBackfillCountryCount": sum(1 for item in countries if item["hasHistoricalBackfill"]),
        "launchCandidateCountryCount": sum(1 for item in countries if int(item["launchCandidateCount"]) > 0),
        "currentRows": sum(int(item["currentRows"]) for item in countries),
        "backfillPeriodCount": sum(int(item["backfillPeriodCount"]) for item in countries),
        "launchCandidateCount": sum(int(item["launchCandidateCount"]) for item in countries),
        "countries": countries,
    }


def _source_status(
    current_price: CurrentPrice,
    source: MsrpSource | None,
    source_currency_changed: bool,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    match_status = str(current_price.match_status or "").strip().lower()
    match_confidence = _float_or_none(current_price.match_confidence) or 0.0
    source_type = str(source.source_type if source is not None else "").strip().lower()
    if match_status in SOURCE_RISK_MATCH_STATUSES:
        reasons.append(f"match_status:{match_status}")
    if match_confidence < 0.8:
        reasons.append("low_match_confidence")
    if source is None:
        reasons.append("missing_source_registry")
    elif source_type not in OFFICIAL_SOURCE_TYPES:
        reasons.append(f"non_official_source:{source_type or 'unknown'}")
    if source_currency_changed:
        reasons.append("source_currency_changed")
    if reasons:
        return "review_required" if match_status == "review_required" else "source_risk", reasons
    return "confirmed", []


def _launch_alert_payload(
    item: CurrentPrice,
    period: PriceHistory,
    source: MsrpSource | None,
    observation: MsrpObservation | None,
) -> dict[str, object]:
    status, risk_reasons = _source_status(item, source, False)
    priority = "priority_audit" if status != "confirmed" else "sample"
    reasons = ["new_product_launch_price_baseline", "no_previous_price_period"]
    if status != "confirmed":
        reasons.append(f"source_status:{status}")
    current_eur = _float_or_none(period.msrp_value)
    current_source = _float_or_none(period.source_msrp_value)
    return {
        "alertId": "|".join(
            [
                "launch",
                str(item.country or ""),
                str(item.brand or ""),
                str(item.jato_model or ""),
                str(item.jato_trim or ""),
                _normalize_powertrain(item.jato_powertrain),
            ]
        ),
        "country": item.country,
        "countryLabel": to_display_country(item.country),
        "brand": item.brand,
        "jatoModel": item.jato_model,
        "jatoTrim": item.jato_trim,
        "jatoPowertrain": _normalize_powertrain(item.jato_powertrain),
        "eventType": "new_product_launch_price_baseline",
        "launchedAtUtc": _iso(period.valid_from_utc),
        "currentMsrpEur": current_eur,
        "currentSourceMsrp": current_source,
        "sourceCurrency": period.source_currency,
        "sourceStatus": status,
        "reviewFlag": status != "confirmed",
        "riskReasons": risk_reasons,
        "auditPriority": priority,
        "suggestedAction": "sample_launch_price_baseline",
        "auditActionLabel": (
            "Spot-check the launch price baseline, then monitor the next scrape for the first movement."
            if priority == "sample"
            else "Review source reliability before using this launch price baseline."
        ),
        "auditReasons": sorted(set(reasons)),
        "samplingBucket": "new_launch_price_baseline",
        "currentPriceId": str(item.current_price_id),
        "priceHistoryId": str(period.price_history_id),
        "currentObservationId": str(period.started_by_observation_id),
        "lastConfirmedObservationId": str(period.last_confirmed_by_observation_id),
        "effectiveObservationId": str(item.effective_observation_id),
        "source": {
            "sourceCode": source.source_code if source is not None else None,
            "sourceType": source.source_type if source is not None else None,
            "extractorName": source.extractor_name if source is not None else None,
            "extractorVersion": source.extractor_version if source is not None else None,
            "sourceRegistryUrl": source.source_url if source is not None else None,
        },
        "evidence": {
            "sourceUrl": item.source_url,
            "sourceSnapshotPath": item.source_snapshot_path,
            "matchConfidence": _float_or_none(item.match_confidence),
            "matchStatus": item.match_status,
            "observationSourceUrl": observation.source_url if observation is not None else None,
            "sourcePayloadHash": observation.source_payload_hash if observation is not None else None,
            "observedAtUtc": _iso(observation.observed_at_utc if observation is not None else None),
        },
    }


def _build_launch_alerts(
    session: Session,
    *,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    since: datetime,
    limit: int,
) -> list[dict[str, object]]:
    current_prices = msrp_repo.list_current_prices(
        session,
        country,
        brand,
        jato_model,
        limit,
        0,
    )
    current_prices = _filter_monitoring_current_prices(current_prices)
    histories = _load_histories_for_current_prices(
        session,
        current_prices,
        per_key_limit=2,
    )
    launch_pairs: list[tuple[CurrentPrice, PriceHistory]] = []
    for item in current_prices:
        period = _launch_candidate_from_history(histories.get(str(item.current_price_id), []))
        if period is None:
            continue
        changed_at = period.valid_from_utc
        if changed_at.tzinfo is None:
            changed_at = changed_at.replace(tzinfo=timezone.utc)
        if changed_at < since:
            continue
        launch_pairs.append((item, period))

    observations = msrp_repo.list_observations_by_ids(
        session,
        [item.effective_observation_id for item, _ in launch_pairs],
    )
    observation_by_id = {str(item.observation_id): item for item in observations}
    sources = msrp_repo.list_sources_by_ids(
        session,
        [item.source_id for item in observations],
    )
    source_by_id = {str(item.source_id): item for item in sources}
    alerts: list[dict[str, object]] = []
    for item, period in launch_pairs:
        observation = observation_by_id.get(str(item.effective_observation_id))
        source = (
            source_by_id.get(str(observation.source_id))
            if observation is not None
            else None
        )
        alerts.append(
            _launch_alert_payload(
                item,
                period,
                source,
                observation,
            )
        )

    alerts.sort(
        key=lambda item: (
            str(item.get("launchedAtUtc") or ""),
            str(item.get("countryLabel") or ""),
            str(item.get("brand") or ""),
            str(item.get("jatoModel") or ""),
        ),
        reverse=True,
    )
    return alerts


def _timeline_payload(
    *,
    item: CurrentPrice,
    current_period: PriceHistory,
    previous_period: PriceHistory,
    source: MsrpSource | None,
    observation: MsrpObservation | None,
    threshold_pct: float,
    direction: str,
) -> dict[str, object] | None:
    current_eur = _float_or_none(current_period.msrp_value)
    previous_eur = _float_or_none(previous_period.msrp_value)
    source_currency_changed = current_period.source_currency != previous_period.source_currency
    current_source = _float_or_none(current_period.source_msrp_value)
    previous_source = _float_or_none(previous_period.source_msrp_value)
    delta_eur = (
        round(current_eur - previous_eur, 2)
        if current_eur is not None and previous_eur is not None
        else None
    )
    change_pct_basis = "eur_normalized"
    change_pct = _change_pct(current_eur, previous_eur)
    if (
        not source_currency_changed
        and current_source is not None
        and previous_source is not None
    ):
        source_change_pct = _change_pct(current_source, previous_source)
        if source_change_pct is not None:
            change_pct = source_change_pct
            change_pct_basis = "source_msrp"
    if (
        change_pct is None
        or abs(change_pct) < threshold_pct
        or not _change_matches_direction(change_pct, direction)
    ):
        return None

    delta_source = (
        round(current_source - previous_source, 2)
        if (
            current_source is not None
            and previous_source is not None
            and not source_currency_changed
        )
        else None
    )
    status, risk_reasons = _source_status(item, source, source_currency_changed)
    review_flag = status != "confirmed"
    return {
        "country": item.country,
        "countryLabel": to_display_country(item.country),
        "brand": item.brand,
        "jatoModel": item.jato_model,
        "jatoTrim": item.jato_trim,
        "jatoPowertrain": _normalize_powertrain(item.jato_powertrain),
        "changedAtUtc": _iso(current_period.valid_from_utc),
        "oldMsrpEur": previous_eur,
        "currentMsrpEur": current_eur,
        "changeAmountEur": delta_eur,
        "changePct": change_pct,
        "changePctBasis": change_pct_basis,
        "oldSourceMsrp": previous_source,
        "currentSourceMsrp": current_source,
        "changeAmountSource": delta_source,
        "sourceCurrency": current_period.source_currency,
        "previousSourceCurrency": previous_period.source_currency,
        "sourceCurrencyChanged": source_currency_changed,
        "sourceStatus": status,
        "reviewFlag": review_flag,
        "riskReasons": risk_reasons,
        "currentPriceId": str(item.current_price_id),
        "priceHistoryId": str(current_period.price_history_id),
        "currentObservationId": str(current_period.started_by_observation_id),
        "previousObservationId": str(previous_period.started_by_observation_id),
        "lastConfirmedObservationId": str(
            current_period.last_confirmed_by_observation_id
        ),
        "effectiveObservationId": str(item.effective_observation_id),
        "source": {
            "sourceCode": source.source_code if source is not None else None,
            "sourceType": source.source_type if source is not None else None,
            "extractorName": source.extractor_name if source is not None else None,
            "extractorVersion": source.extractor_version if source is not None else None,
            "sourceRegistryUrl": source.source_url if source is not None else None,
        },
        "evidence": {
            "sourceUrl": item.source_url,
            "sourceSnapshotPath": item.source_snapshot_path,
            "matchConfidence": _float_or_none(item.match_confidence),
            "matchStatus": item.match_status,
            "observationSourceUrl": observation.source_url if observation is not None else None,
            "sourcePayloadHash": observation.source_payload_hash if observation is not None else None,
            "observedAtUtc": _iso(observation.observed_at_utc if observation is not None else None),
        },
    }


def _hydrate_timeline_evidence(
    session: Session,
    timeline: list[dict[str, object]],
) -> None:
    observation_ids = {
        item.get("currentObservationId")
        for item in timeline
        if item.get("currentObservationId")
    }
    observation_ids.update(
        item.get("previousObservationId")
        for item in timeline
        if item.get("previousObservationId")
    )
    observation_ids.update(
        item.get("effectiveObservationId")
        for item in timeline
        if item.get("effectiveObservationId")
    )
    observations = msrp_repo.list_observations_by_ids(
        session,
        [
            UUID(str(item))
            for item in observation_ids
            if item is not None
        ],
    )
    observation_by_id = {str(item.observation_id): item for item in observations}
    batch_by_id: dict[str, object] = {}

    for event in timeline:
        current_observation = observation_by_id.get(str(event.get("currentObservationId") or ""))
        previous_observation = observation_by_id.get(str(event.get("previousObservationId") or ""))
        effective_observation = observation_by_id.get(str(event.get("effectiveObservationId") or ""))
        primary_observation = current_observation or effective_observation
        if primary_observation is None:
            continue
        batch_id = str(primary_observation.scrape_batch_id)
        if batch_id not in batch_by_id:
            batch_by_id[batch_id] = msrp_repo.get_scrape_batch(
                session,
                primary_observation.scrape_batch_id,
            )
        batch = batch_by_id.get(batch_id)
        evidence = dict(event.get("evidence") or {})
        evidence["observationSourceUrl"] = primary_observation.source_url
        evidence["sourceSnapshotPath"] = primary_observation.source_snapshot_path
        evidence["sourcePayloadHash"] = primary_observation.source_payload_hash
        evidence["observedAtUtc"] = _iso(primary_observation.observed_at_utc)
        evidence["matchStatus"] = primary_observation.match_status
        evidence["matchConfidence"] = _float_or_none(primary_observation.match_confidence)
        evidence["scrapeBatchId"] = batch_id
        evidence["scrapeBatchCode"] = getattr(batch, "batch_code", None)
        evidence["dryrunRunId"] = (
            _extract_dryrun_run_id(primary_observation.source_context_json)
            or _extract_dryrun_run_id(primary_observation.match_reason_json)
            or (
                getattr(batch, "batch_code", None)
                if str(getattr(batch, "batch_code", "")).startswith("msrp-dryrun-")
                else None
            )
        )
        seen_backfill_observation_ids: set[str] = set()
        backfill_candidates: list[dict[str, object]] = []
        for role, candidate_observation in (
            ("current", current_observation),
            ("previous", previous_observation),
            ("effective", effective_observation),
        ):
            if candidate_observation is None:
                continue
            candidate_id = str(candidate_observation.observation_id)
            if candidate_id in seen_backfill_observation_ids:
                continue
            seen_backfill_observation_ids.add(candidate_id)
            backfill_evidence = _backfill_evidence_from_observation(
                candidate_observation,
                role,
            )
            if backfill_evidence:
                backfill_candidates.append(backfill_evidence)
        if backfill_candidates:
            evidence.update(backfill_candidates[0])
            if len(backfill_candidates) > 1:
                roles = sorted(
                    {
                        str(item.get("backfillEvidenceRole"))
                        for item in backfill_candidates
                        if item.get("backfillEvidenceRole")
                    }
                )
                evidence["backfillEvidenceRole"] = "_and_".join(roles)
                evidence["backfillCount"] = len(backfill_candidates)
        event["evidence"] = evidence


def _mark_outliers(events: list[dict[str, object]]) -> None:
    values = [
        float(item["changePct"])
        for item in events
        if item.get("changePct") is not None
    ]
    if len(values) < 3:
        for item in events:
            item["outlier"] = False
            item["suspectedFalsePositive"] = bool(item.get("reviewFlag"))
        return
    center = float(median(values))
    deviations = [abs(value - center) for value in values]
    deviation_center = float(median(deviations))
    threshold = max(5.0, deviation_center * 3.0)
    for item in events:
        change_pct = _float_or_none(item.get("changePct"))
        outlier = change_pct is not None and abs(change_pct - center) >= threshold
        item["outlier"] = outlier
        item["suspectedFalsePositive"] = bool(item.get("reviewFlag")) or outlier


def _audit_decision_payload(
    priority: str,
    reasons: list[str],
    bucket: str,
) -> dict[str, object]:
    safe_priority = priority if priority in AUDIT_PRIORITY_ORDER else "priority_audit"
    return {
        "auditPriority": safe_priority,
        "suggestedAction": safe_priority,
        "auditActionLabel": AUDIT_ACTION_LABELS[safe_priority],
        "auditReasons": reasons,
        "samplingBucket": bucket,
    }


def _raise_audit_priority(current: str, candidate: str) -> str:
    if AUDIT_PRIORITY_ORDER[candidate] > AUDIT_PRIORITY_ORDER[current]:
        return candidate
    return current


def _audit_decision_for_country_event(item: dict[str, object]) -> dict[str, object]:
    reasons: list[str] = []
    priority = "auto_pass"
    bucket = "clean_confirmed"
    lifecycle_status = str(item.get("lifecycleStatus") or "active")
    source_status = str(item.get("sourceStatus") or "confirmed")
    change_pct = _float_or_none(item.get("changePct"))
    absolute_change_pct = abs(change_pct) if change_pct is not None else 0.0
    risk_reasons = [str(reason) for reason in list(item.get("riskReasons") or [])]
    evidence = dict(item.get("evidence") or {})
    demo_backfilled = bool(evidence.get("demoBackfilled"))
    historical_backfilled = bool(evidence.get("backfilled")) and not demo_backfilled
    backfill_kind = str(evidence.get("backfillKind") or "")

    if lifecycle_status not in {"", "active"}:
        priority = "block"
        bucket = "lifecycle_block"
        reasons.append(f"lifecycle_signal:{lifecycle_status}")
    if any(reason in {"match_status:rejected", "match_status:failed"} for reason in risk_reasons):
        priority = "block"
        bucket = "source_rejected"
        reasons.extend(reason for reason in risk_reasons if reason.startswith("match_status:"))
    if source_status != "confirmed":
        priority = _raise_audit_priority(priority, "priority_audit")
        if bucket == "clean_confirmed":
            bucket = "source_risk"
        reasons.append(f"source_status:{source_status}")
    if bool(item.get("outlier")):
        priority = _raise_audit_priority(priority, "priority_audit")
        if bucket == "clean_confirmed":
            bucket = "outlier"
        reasons.append("outlier_vs_model_country_cluster")
    if bool(item.get("sourceCurrencyChanged")):
        priority = _raise_audit_priority(priority, "priority_audit")
        if bucket == "clean_confirmed":
            bucket = "currency_change"
        reasons.append("source_currency_changed")
    if absolute_change_pct >= 5.0:
        priority = _raise_audit_priority(priority, "priority_audit")
        if bucket == "clean_confirmed":
            bucket = "large_price_move"
        reasons.append("large_price_move:>=5pct")
    elif absolute_change_pct >= 1.0:
        priority = _raise_audit_priority(priority, "sample")
        if bucket == "clean_confirmed":
            bucket = "routine_price_move"
        reasons.append("price_move:>=1pct")
    if demo_backfilled:
        priority = _raise_audit_priority(priority, "sample")
        if bucket == "clean_confirmed":
            bucket = "demo_backfill"
        reasons.append("demo_backfilled_scenario")
    if historical_backfilled:
        priority = _raise_audit_priority(priority, "sample")
        if bucket == "clean_confirmed":
            bucket = "historical_backfill"
        reasons.append("historical_price_backfill")
    if backfill_kind in OFFICIAL_PROMOTION_BACKFILL_KINDS:
        priority = _raise_audit_priority(priority, "priority_audit")
        if bucket in {"clean_confirmed", "historical_backfill", "large_price_move", "routine_price_move"}:
            bucket = "campaign_promotion_boundary"
        reasons.append("campaign_promotion_boundary:not_permanent_msrp_cut")

    if not reasons:
        reasons.append("confirmed_low_variance")
    return _audit_decision_payload(priority, sorted(set(reasons)), bucket)


def _audit_decision_for_model_event(
    country_events: list[dict[str, object]],
    *,
    multi_country_sync: bool,
    demo_event: bool,
) -> dict[str, object]:
    priority = "auto_pass"
    reasons: set[str] = set()
    bucket = "clean_confirmed"
    for item in country_events:
        item_priority = str(item.get("auditPriority") or "auto_pass")
        priority = _raise_audit_priority(priority, item_priority)
        reasons.update(str(reason) for reason in list(item.get("auditReasons") or []))
        if bucket == "clean_confirmed" and item_priority != "auto_pass":
            bucket = str(item.get("samplingBucket") or item_priority)

    change_values = [
        abs(float(item["changePct"]))
        for item in country_events
        if item.get("changePct") is not None
    ]
    max_change_pct = max(change_values) if change_values else 0.0
    trim_count = len({item.get("jatoTrim") for item in country_events})
    country_change_count = len(country_events)

    if priority != "block" and country_change_count == 1 and trim_count == 1 and max_change_pct >= 3.0:
        priority = _raise_audit_priority(priority, "priority_audit")
        reasons.add("single_trim_market_move:>=3pct")
        if bucket == "clean_confirmed":
            bucket = "single_trim_market_move"
    if priority == "auto_pass" and max_change_pct >= 1.0:
        priority = "sample"
        bucket = "routine_price_move"
        reasons.add("price_move:>=1pct")
    if multi_country_sync and priority in {"auto_pass", "sample"}:
        reasons.add("multi_country_sync_support")
    if demo_event:
        reasons.add("demo_backfilled_scenario")
        if priority == "auto_pass":
            priority = "sample"
            bucket = "demo_backfill"

    if not reasons:
        reasons.add("confirmed_low_variance")
    return _audit_decision_payload(priority, sorted(reasons), bucket)


def _country_latest_events(events: list[dict[str, object]]) -> list[dict[str, object]]:
    latest_by_country_trim: dict[tuple[str, str], dict[str, object]] = {}
    for event in sorted(
        events,
        key=lambda item: str(item.get("changedAtUtc") or ""),
        reverse=True,
    ):
        country = str(event.get("country") or "")
        trim = str(event.get("jatoTrim") or "")
        latest_by_country_trim.setdefault((country, trim), event)
    return sorted(
        latest_by_country_trim.values(),
        key=lambda item: (
            -abs(float(item.get("changePct") or 0.0)),
            str(item.get("countryLabel") or ""),
            str(item.get("jatoTrim") or ""),
        ),
    )


def _load_market_scan_length_lookup(
    current_prices: list[CurrentPrice],
) -> tuple[dict[tuple[str, str, str, str], int], dict[tuple[str, str, str], int], str | None]:
    """Best-effort model length lookup from the market scan parquet dataset."""
    if not current_prices:
        return {}, {}, None
    try:
        import pandas as pd
        from app.infra import parquet_repository as parquet_repo
        from app.services import market_scan_service
    except Exception as exc:  # pragma: no cover - optional runtime dependency
        return {}, {}, f"market_scan_length_lookup_unavailable:{type(exc).__name__}"

    try:
        columns = market_scan_service._get_columns()
        if not columns.length:
            return {}, {}, "market_scan_length_column_missing"
        selected_columns = [
            columns.country_value,
            columns.make,
            columns.model,
            columns.powertrain,
            columns.length,
        ]
        table = parquet_repo._open_dataset().to_table(columns=selected_columns)
        frame = table.to_pandas()
        if frame.empty:
            return {}, {}, "market_scan_length_dataset_empty"
        wanted_models = {
            (
                str(item.brand or "").strip().lower(),
                str(item.jato_model or "").strip().lower(),
                _normalize_powertrain(item.jato_powertrain),
            )
            for item in current_prices
        }
        frame["__country"] = frame[columns.country_value].astype(str).str.strip()
        frame["__brand"] = frame[columns.make].astype(str).str.strip()
        frame["__model"] = frame[columns.model].astype(str).str.strip()
        frame["__brand_key"] = frame["__brand"].str.lower()
        frame["__model_key"] = frame["__model"].str.lower()
        frame["__powertrain"] = frame[columns.powertrain].map(_normalize_powertrain)
        frame["__length"] = pd.to_numeric(frame[columns.length], errors="coerce")
        frame = frame[
            frame.apply(
                lambda row: (
                    str(row["__brand_key"]),
                    str(row["__model_key"]),
                    str(row["__powertrain"]),
                )
                in wanted_models,
                axis=1,
            )
            & frame["__length"].between(2500, 7000)
        ].copy()
        if frame.empty:
            return {}, {}, "market_scan_length_no_matches"

        country_lookup: dict[tuple[str, str, str, str], int] = {}
        model_lookup: dict[tuple[str, str, str], int] = {}
        for key, group in frame.groupby(["__country", "__brand_key", "__model_key", "__powertrain"]):
            values = [float(item) for item in group["__length"].dropna().tolist()]
            if values:
                country_lookup[
                    (
                        str(key[0]),
                        str(key[1]),
                        str(key[2]),
                        str(key[3]),
                    )
                ] = int(round(median(values)))
        for key, group in frame.groupby(["__brand_key", "__model_key", "__powertrain"]):
            values = [float(item) for item in group["__length"].dropna().tolist()]
            if values:
                model_lookup[
                    (
                        str(key[0]),
                        str(key[1]),
                        str(key[2]),
                    )
                ] = int(round(median(values)))
        return country_lookup, model_lookup, None
    except Exception as exc:  # pragma: no cover - defensive best effort
        return {}, {}, f"market_scan_length_lookup_failed:{type(exc).__name__}"


def _market_scan_length_for_item(
    item: CurrentPrice,
    country_lookup: dict[tuple[str, str, str, str], int],
    model_lookup: dict[tuple[str, str, str], int],
) -> int | None:
    powertrain = _normalize_powertrain(item.jato_powertrain)
    country_keys = [
        (
            str(item.country or "").strip(),
            str(item.brand or "").strip().lower(),
            str(item.jato_model or "").strip().lower(),
            powertrain,
        ),
        (
            to_display_country(item.country),
            str(item.brand or "").strip().lower(),
            str(item.jato_model or "").strip().lower(),
            powertrain,
        ),
    ]
    for key in country_keys:
        if key in country_lookup:
            return country_lookup[key]
    return model_lookup.get(
        (
            str(item.brand or "").strip().lower(),
            str(item.jato_model or "").strip().lower(),
            powertrain,
        )
    )


def _month_from_iso(value: object | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return f"{parsed.year:04d}-{parsed.month:02d}"
    except ValueError:
        if len(text) >= 7 and text[4] == "-":
            return text[:7]
    return None


def _effect_markers_for_event(event: dict[str, object]) -> list[dict[str, object]]:
    markers: list[dict[str, object]] = []
    for item in list(event.get("timeline") or []):
        if not isinstance(item, dict):
            continue
        period = _month_from_iso(item.get("changedAtUtc"))
        if period is None:
            continue
        change_pct = _float_or_none(item.get("changePct"))
        markers.append(
            {
                "period": period,
                "changedAtUtc": item.get("changedAtUtc"),
                "country": item.get("country"),
                "countryLabel": item.get("countryLabel"),
                "jatoTrim": item.get("jatoTrim"),
                "changePct": change_pct,
                "changeAmountEur": _float_or_none(item.get("changeAmountEur")),
                "eventType": (
                    "price_drop"
                    if change_pct is not None and change_pct < 0
                    else "price_increase"
                    if change_pct is not None and change_pct > 0
                    else "price_flat"
                ),
            }
        )
    return sorted(markers, key=lambda item: str(item.get("changedAtUtc") or ""))


def _match_market_scan_country(
    *,
    country: object | None,
    country_label: object | None,
    country_options: list[dict[str, str]],
) -> dict[str, str] | None:
    candidates = [
        str(country or "").strip(),
        str(country_label or "").strip(),
        to_display_country(country),
    ]
    normalized_candidates = {
        candidate.lower()
        for candidate in candidates
        if candidate
    }
    for option in country_options:
        value = str(option.get("value") or "").strip()
        label = str(option.get("label") or "").strip()
        if value.lower() in normalized_candidates or label.lower() in normalized_candidates:
            return option
    return None


def _attach_market_scan_sales(events: list[dict[str, object]]) -> str | None:
    """Best-effort model-level sales enrichment for MSRP monitoring bubbles."""
    if not events:
        return None
    for event in events:
        event["sales"] = None

    try:
        import pandas as pd
        from app.infra import parquet_repository as parquet_repo
        from app.services import market_scan_service
    except Exception as exc:  # pragma: no cover - optional runtime dependency
        return f"market_scan_sales_unavailable:{type(exc).__name__}"

    try:
        columns = market_scan_service._get_columns()
        if not columns.month_columns:
            return "market_scan_sales_month_columns_missing"
        month_columns = list(columns.month_columns)[-24:]
        selected_columns = [
            columns.country_value,
            columns.make,
            columns.model,
            *month_columns,
        ]
        country_options = market_scan_service._country_options(
            parquet_repo.current_dataset_token(),
        )
        country_values: set[str] = set()
        country_values_by_event_id: dict[str, set[str]] = {}
        country_labels_by_event_id: dict[str, set[str]] = {}
        country_label_by_value = {
            str(option.get("value") or "").strip(): str(option.get("label") or "").strip()
            for option in country_options
        }

        for event in events:
            event_id = str(event.get("eventId") or "")
            country_values_by_event_id[event_id] = set()
            country_labels_by_event_id[event_id] = set()
            for country_event in list(event.get("countries") or []):
                if not isinstance(country_event, dict):
                    continue
                matched = _match_market_scan_country(
                    country=country_event.get("country"),
                    country_label=country_event.get("countryLabel"),
                    country_options=country_options,
                )
                if matched is None:
                    continue
                value = str(matched.get("value") or "").strip()
                label = str(matched.get("label") or "").strip()
                if value:
                    country_values.add(value)
                    country_values_by_event_id[event_id].add(value)
                    country_labels_by_event_id[event_id].add(label or value)

        if not country_values:
            for event in events:
                event["sales"] = {
                    "source": "market_scan",
                    "countryLabels": [],
                    "totalSales": 0.0,
                    "currentMonthSales": 0.0,
                    "latestSalesPeriod": None,
                    "latestSalesLabel": None,
                    "effectCoverageStatus": "no_sales_match",
                    "coveredEffectMarkerCount": 0,
                    "pendingEffectMarkerCount": len(_effect_markers_for_event(event)),
                    "monthlySeries": [],
                    "effectMarkers": _effect_markers_for_event(event),
                    "matchedRowCount": 0,
                    "matchedCountryCount": 0,
                    "warnings": ["market_scan_country_match_missing"],
                }
            return "market_scan_sales_country_match_missing"

        dataset = parquet_repo._open_dataset()
        filter_expression = parquet_repo._build_filter_expression(
            {columns.country_value: sorted(country_values)}
        )
        table = dataset.to_table(columns=selected_columns, filter=filter_expression)
        frame = table.to_pandas()
        if frame.empty:
            return "market_scan_sales_dataset_empty"

        frame = market_scan_service._ensure_numeric_columns(frame, month_columns)
        frame["__country"] = frame[columns.country_value].astype(str).str.strip()
        frame["__brand_key"] = frame[columns.make].astype(str).str.strip().str.lower()
        frame["__model_key"] = frame[columns.model].astype(str).str.strip().str.lower()

        for event in events:
            event_id = str(event.get("eventId") or "")
            wanted_countries = country_values_by_event_id.get(event_id, set())
            brand_key = str(event.get("brand") or "").strip().lower()
            model_key = str(event.get("jatoModel") or "").strip().lower()
            warnings: list[str] = []
            if not wanted_countries:
                filtered = frame.iloc[0:0]
                warnings.append("market_scan_country_match_missing")
            else:
                filtered = frame[
                    frame["__country"].isin(wanted_countries)
                    & (frame["__brand_key"] == brand_key)
                    & (frame["__model_key"] == model_key)
                ]
            if filtered.empty:
                warnings.append("market_scan_sales_model_match_missing")

            monthly_series: list[dict[str, object]] = []
            for column in month_columns:
                period = market_scan_service._month_column_to_period(column)
                sales = 0.0
                if not filtered.empty and column in filtered.columns:
                    sales = float(pd.to_numeric(filtered[column], errors="coerce").fillna(0.0).sum())
                monthly_series.append(
                    {
                        "period": period,
                        "label": market_scan_service._short_period_label(period),
                        "sales": round(sales, 2),
                    }
                )
            latest_sales_point = monthly_series[-1] if monthly_series else None
            latest_sales_period = (
                str(latest_sales_point.get("period"))
                if latest_sales_point is not None
                else None
            )
            latest_sales_label = (
                str(latest_sales_point.get("label"))
                if latest_sales_point is not None
                else None
            )
            current_sales = (
                float(latest_sales_point.get("sales") or 0.0)
                if latest_sales_point is not None
                else 0.0
            )
            effect_markers = _effect_markers_for_event(event)
            pending_effect_count = (
                sum(
                    1
                    for marker in effect_markers
                    if latest_sales_period is None
                    or str(marker.get("period") or "") > latest_sales_period
                )
                if effect_markers
                else 0
            )
            covered_effect_count = max(0, len(effect_markers) - pending_effect_count)
            if filtered.empty:
                effect_coverage_status = "no_sales_match"
            elif not effect_markers:
                effect_coverage_status = "no_effect_markers"
            elif pending_effect_count:
                effect_coverage_status = "post_sales_pending"
            else:
                effect_coverage_status = "covered"
            country_labels = sorted(
                country_labels_by_event_id.get(event_id, set())
                or {
                    country_label_by_value.get(value, value)
                    for value in wanted_countries
                }
            )
            matched_countries = (
                filtered["__country"].nunique()
                if not filtered.empty and "__country" in filtered.columns
                else 0
            )
            event["sales"] = {
                "source": "market_scan",
                "countryLabels": country_labels,
                "totalSales": round(
                    sum(float(item.get("sales") or 0.0) for item in monthly_series),
                    2,
                ),
                "currentMonthSales": round(current_sales, 2),
                "latestSalesPeriod": latest_sales_period,
                "latestSalesLabel": latest_sales_label,
                "effectCoverageStatus": effect_coverage_status,
                "coveredEffectMarkerCount": covered_effect_count,
                "pendingEffectMarkerCount": pending_effect_count,
                "monthlySeries": monthly_series,
                "effectMarkers": effect_markers,
                "matchedRowCount": int(len(filtered)),
                "matchedCountryCount": int(matched_countries),
                "warnings": warnings,
            }
        return None
    except Exception as exc:  # pragma: no cover - defensive best effort
        return f"market_scan_sales_lookup_failed:{type(exc).__name__}"


def _build_model_event(
    key: tuple[str, str, str],
    timeline: list[dict[str, object]],
    length_by_country: dict[str, tuple[int | None, str | None]],
) -> dict[str, object]:
    _mark_outliers(timeline)
    country_events = _country_latest_events(timeline)
    for item in country_events:
        item.update(_audit_decision_for_country_event(item))
    change_values = [
        float(item["changePct"])
        for item in country_events
        if item.get("changePct") is not None
    ]
    change_pct_bases = sorted(
        {
            str(item.get("changePctBasis") or "eur_normalized")
            for item in country_events
            if item.get("changePct") is not None
        }
    )
    current_values = [
        float(item["currentMsrpEur"])
        for item in country_events
        if item.get("currentMsrpEur") is not None
    ]
    old_values = [
        float(item["oldMsrpEur"])
        for item in country_events
        if item.get("oldMsrpEur") is not None
    ]
    source_risk_count = sum(
        1 for item in country_events if item.get("sourceStatus") != "confirmed"
    )
    review_required_count = sum(1 for item in country_events if item.get("reviewFlag"))
    outlier_count = sum(1 for item in country_events if item.get("outlier"))
    suspected_false_positive_count = sum(
        1 for item in country_events if item.get("suspectedFalsePositive")
    )
    lengths = [value[0] for value in length_by_country.values() if value[0] is not None]
    length_mm = int(round(median(lengths))) if lengths else None
    length_sources = sorted(
        {
            str(source)
            for _, source in length_by_country.values()
            if source
        }
    )
    risk_reasons: dict[str, int] = {}
    for item in country_events:
        for reason in list(item.get("riskReasons") or []):
            key_text = str(reason)
            risk_reasons[key_text] = risk_reasons.get(key_text, 0) + 1
    lifecycle_statuses = sorted(
        {
            str(item.get("lifecycleStatus") or "active")
            for item in country_events
            if item.get("lifecycleStatus") or item.get("sourceStatus") == "confirmed"
        }
    )
    demo_event = any(
        bool(dict(item.get("evidence") or {}).get("demoBackfilled"))
        for item in timeline
    )
    backfilled_event_count = sum(
        1
        for item in timeline
        if bool(dict(item.get("evidence") or {}).get("backfilled"))
        and not bool(dict(item.get("evidence") or {}).get("demoBackfilled"))
    )
    multi_country_sync = len({item.get("country") for item in country_events}) >= 2
    audit_decision = _audit_decision_for_model_event(
        country_events,
        multi_country_sync=multi_country_sync,
        demo_event=demo_event,
    )

    brand, model, powertrain = key
    return {
        "eventId": "|".join(key),
        "brand": brand,
        "jatoModel": model,
        "jatoPowertrain": powertrain,
        "powertrainColor": POWERTRAIN_COLORS.get(powertrain, POWERTRAIN_FALLBACK_COLOR),
        "lengthMm": length_mm,
        "lengthMissing": length_mm is None,
        "lengthSource": "mixed" if len(length_sources) > 1 else (length_sources[0] if length_sources else None),
        "affectedCountryCount": len({item.get("country") for item in country_events}),
        "countryChangeCount": len(country_events),
        "timelineEventCount": len(timeline),
        "trimChangeCount": len({item.get("jatoTrim") for item in country_events}),
        "medianChangePct": _median_or_none(change_values),
        "minChangePct": round(min(change_values), 2) if change_values else None,
        "maxChangePct": round(max(change_values), 2) if change_values else None,
        "changePctBasis": (
            "mixed"
            if len(change_pct_bases) > 1
            else change_pct_bases[0]
            if change_pct_bases
            else "eur_normalized"
        ),
        "medianOldMsrpEur": _median_or_none(old_values),
        "medianCurrentMsrpEur": _median_or_none(current_values),
        "oldMsrpEurRange": _range_payload(old_values),
        "currentMsrpEurRange": _range_payload(current_values),
        "sourceRiskCount": source_risk_count,
        "reviewRequiredCount": review_required_count,
        "outlierCount": outlier_count,
        "suspectedFalsePositiveCount": suspected_false_positive_count,
        "multiCountrySync": multi_country_sync,
        "lifecycleStatus": (
            "mixed"
            if len(lifecycle_statuses) > 1
            else lifecycle_statuses[0]
            if lifecycle_statuses
            else "active"
        ),
        "demo": demo_event,
        "backfilled": backfilled_event_count > 0,
        "backfillEventCount": backfilled_event_count,
        **audit_decision,
        "confidence": (
            "low"
            if suspected_false_positive_count
            else "medium"
            if source_risk_count
            else "high"
        ),
        "riskReasons": risk_reasons,
        "sales": None,
        "countries": country_events,
        "timeline": sorted(
            timeline,
            key=lambda item: str(item.get("changedAtUtc") or ""),
        ),
    }


def _sweden_demo_scenario(item: CurrentPrice) -> dict[str, object] | None:
    key = (
        str(item.brand or "").strip().upper(),
        str(item.jato_model or "").strip().upper(),
        _normalize_powertrain(item.jato_powertrain),
    )
    return SWEDEN_DEMO_SCENARIOS.get(key)


def _demo_previous_value(current_value: float, change_pct: float) -> float:
    divisor = 1.0 + change_pct / 100.0
    if divisor <= 0:
        return current_value
    return round(current_value / divisor, 2)


def _demo_timeline_payload(
    *,
    item: CurrentPrice,
    observation: MsrpObservation | None,
    source: MsrpSource | None,
    generated_at: datetime,
    scenario: dict[str, object],
    threshold_pct: float,
    direction: str,
) -> dict[str, object] | None:
    current_eur = _float_or_none(item.current_msrp_value)
    if current_eur is None:
        return None
    change_pct = _float_or_none(scenario.get("changePct"))
    if (
        change_pct is None
        or abs(change_pct) < threshold_pct
        or not _change_matches_direction(change_pct, direction)
    ):
        return None
    previous_eur = _demo_previous_value(current_eur, change_pct)
    current_source = _float_or_none(item.source_msrp_value) or current_eur
    previous_source = _demo_previous_value(current_source, change_pct)
    days_ago = int(_float_or_none(scenario.get("daysAgo")) or 7)
    changed_at = generated_at - timedelta(days=max(1, days_ago))
    source_status = str(scenario.get("sourceStatus") or "confirmed")
    risk_reasons = [str(item) for item in list(scenario.get("riskReasons") or [])]
    lifecycle_status = str(scenario.get("lifecycleStatus") or "active")
    observation_id = str(getattr(observation, "observation_id", item.effective_observation_id))
    return {
        "country": item.country,
        "countryLabel": to_display_country(item.country),
        "brand": item.brand,
        "jatoModel": item.jato_model,
        "jatoTrim": item.jato_trim,
        "jatoPowertrain": _normalize_powertrain(item.jato_powertrain),
        "changedAtUtc": changed_at.isoformat(),
        "oldMsrpEur": previous_eur,
        "currentMsrpEur": current_eur,
        "changeAmountEur": round(current_eur - previous_eur, 2),
        "changePct": round(change_pct, 2),
        "changePctBasis": "source_msrp",
        "oldSourceMsrp": previous_source,
        "currentSourceMsrp": current_source,
        "changeAmountSource": round(current_source - previous_source, 2),
        "sourceCurrency": item.source_currency,
        "previousSourceCurrency": item.source_currency,
        "sourceCurrencyChanged": False,
        "sourceStatus": source_status,
        "reviewFlag": source_status != "confirmed",
        "riskReasons": risk_reasons,
        "lifecycleStatus": lifecycle_status,
        "currentPriceId": str(item.current_price_id),
        "priceHistoryId": f"demo-sweden-history:{item.current_price_id}:{changed_at.date().isoformat()}",
        "currentObservationId": observation_id,
        "previousObservationId": f"demo-sweden-backfill:{item.current_price_id}",
        "lastConfirmedObservationId": observation_id,
        "effectiveObservationId": str(item.effective_observation_id),
        "source": {
            "sourceCode": source.source_code if source is not None else None,
            "sourceType": source.source_type if source is not None else None,
            "extractorName": source.extractor_name if source is not None else None,
            "extractorVersion": source.extractor_version if source is not None else None,
            "sourceRegistryUrl": source.source_url if source is not None else None,
        },
        "evidence": {
            "sourceUrl": item.source_url,
            "sourceSnapshotPath": item.source_snapshot_path,
            "matchConfidence": _float_or_none(item.match_confidence),
            "matchStatus": item.match_status,
            "observationSourceUrl": observation.source_url if observation is not None else None,
            "sourcePayloadHash": observation.source_payload_hash if observation is not None else None,
            "observedAtUtc": _iso(observation.observed_at_utc if observation is not None else None),
            "demoBackfilled": True,
            "demoScenario": str(scenario.get("scenario") or "Sweden MSRP monitoring demo"),
            "dryrunRunId": "msrp-demo-sweden-2026",
            "scrapeBatchCode": "msrp-demo-sweden-backfill",
        },
    }


def _build_sweden_demo_events(
    session: Session,
    *,
    brand: str | None,
    jato_model: str | None,
    generated_at: datetime,
    safe_window_days: int,
    from_date: date | None,
    safe_threshold_pct: float,
    safe_limit: int,
    direction: str,
) -> dict[str, object]:
    current_prices = msrp_repo.list_current_prices(
        session,
        "瑞典",
        brand,
        jato_model,
        safe_limit,
        0,
    )
    current_prices = _filter_monitoring_current_prices(current_prices)
    current_prices = [
        item
        for item in current_prices
        if _sweden_demo_scenario(item) is not None
    ]
    current_observations = msrp_repo.list_observations_by_ids(
        session,
        [item.effective_observation_id for item in current_prices],
    )
    current_observation_by_id = {
        str(item.observation_id): item for item in current_observations
    }
    sources = msrp_repo.list_sources_by_ids(
        session,
        [item.source_id for item in current_observations],
    )
    source_by_id = {str(item.source_id): item for item in sources}
    since = _monitoring_since(generated_at, safe_window_days, from_date)
    grouped_timeline: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    grouped_lengths: dict[tuple[str, str, str], dict[str, tuple[int | None, str | None]]] = {}

    for item in current_prices:
        scenario = _sweden_demo_scenario(item)
        if scenario is None:
            continue
        changed_at = generated_at - timedelta(days=int(_float_or_none(scenario.get("daysAgo")) or 7))
        if changed_at < since:
            continue
        observation = current_observation_by_id.get(str(item.effective_observation_id))
        source = (
            source_by_id.get(str(observation.source_id))
            if observation is not None
            else None
        )
        timeline_item = _demo_timeline_payload(
            item=item,
            observation=observation,
            source=source,
            generated_at=generated_at,
            scenario=scenario,
            threshold_pct=safe_threshold_pct,
            direction=direction,
        )
        if timeline_item is None:
            continue
        key = _event_key(item)
        grouped_timeline.setdefault(key, []).append(timeline_item)
        grouped_lengths.setdefault(key, {})[item.country] = (
            int(scenario.get("lengthMm") or 0) or None,
            "sweden_demo_backfill",
        )

    events = [
        _build_model_event(
            key,
            timeline,
            grouped_lengths.get(key, {}),
        )
        for key, timeline in grouped_timeline.items()
        if timeline
    ]
    sales_lookup_warning = _attach_market_scan_sales(events)
    events.sort(
        key=lambda item: (
            -abs(float(item.get("medianChangePct") or 0.0)),
            str(item.get("brand") or ""),
            str(item.get("jatoModel") or ""),
        )
    )
    offer_signals = [] if direction == "increases" else _build_offer_signals(
        generated_at,
        country="瑞典",
        brand=brand,
        jato_model=jato_model,
    )
    summary = _summary_payload(events)
    summary["launchAlertCount"] = 0
    summary["offerSignalCount"] = len(offer_signals)
    coverage = {"batchA": _build_batch_a_coverage(session)}
    batch_a_coverage = coverage["batchA"]
    if isinstance(batch_a_coverage, dict):
        summary["batchALoadedCountryCount"] = batch_a_coverage.get("loadedCountryCount", 0)
        summary["batchAHistoricalBackfillCountryCount"] = batch_a_coverage.get("historicalBackfillCountryCount", 0)
    return {
        "schemaVersion": "msrp_monitoring_events_v1",
        "mode": MONITORING_MODE_SWEDEN_DEMO,
        "generatedAtUtc": generated_at.isoformat(),
        "filters": _monitoring_filters_payload(
            country="瑞典",
            brand=brand,
            jato_model=jato_model,
            safe_window_days=safe_window_days,
            from_date=from_date,
            safe_threshold_pct=safe_threshold_pct,
            safe_limit=safe_limit,
            direction=direction,
        ),
        "summary": summary,
        "powertrainColors": POWERTRAIN_COLORS,
        "events": events,
        "launchAlerts": [],
        "offerSignals": offer_signals,
        "coverage": coverage,
        "warnings": [
            item
            for item in [
                "sweden_demo_backfilled_not_written_to_price_history",
                sales_lookup_warning,
            ]
            if item
        ],
        "demo": {
            "enabled": True,
            "country": "Sweden",
            "backfilled": True,
            "description": "Synthetic Sweden MSRP movements built from live current prices for product review.",
        },
    }


def build_msrp_monitoring_events(
    session: Session,
    *,
    country: str | None = None,
    brand: str | None = None,
    jato_model: str | None = None,
    window_days: int = 30,
    from_date: date | None = None,
    threshold_pct: float = 0.0,
    limit: int = DEFAULT_MONITORING_LIMIT,
    mode: str = MONITORING_MODE_LIVE,
    direction: str = DEFAULT_MONITORING_DIRECTION,
) -> dict[str, object]:
    generated_at = _utc_now()
    safe_window_days = max(1, min(int(window_days), 365))
    safe_threshold_pct = max(0.0, float(threshold_pct))
    safe_limit = max(1, min(int(limit), DEFAULT_MONITORING_LIMIT))
    safe_direction = _normalize_monitoring_direction(direction)
    since = _monitoring_since(generated_at, safe_window_days, from_date)
    price_history_limit = _monitoring_price_history_limit(generated_at, since)
    safe_mode = (
        MONITORING_MODE_SWEDEN_DEMO
        if mode == MONITORING_MODE_SWEDEN_DEMO
        else MONITORING_MODE_LIVE
    )

    if safe_mode == MONITORING_MODE_SWEDEN_DEMO:
        return _build_sweden_demo_events(
            session,
            brand=brand,
            jato_model=jato_model,
            generated_at=generated_at,
            safe_window_days=safe_window_days,
            from_date=from_date,
            safe_threshold_pct=safe_threshold_pct,
            safe_limit=safe_limit,
            direction=safe_direction,
        )

    offer_signals = [] if safe_direction == "increases" else _build_offer_signals(
        generated_at,
        country=country,
        brand=brand,
        jato_model=jato_model,
    )

    if not msrp_repo.has_price_history_table(session):
        summary = _summary_payload([])
        summary["offerSignalCount"] = len(offer_signals)
        return {
            "schemaVersion": "msrp_monitoring_events_v1",
            "mode": MONITORING_MODE_LIVE,
            "generatedAtUtc": generated_at.isoformat(),
            "filters": _monitoring_filters_payload(
                country=country,
                brand=brand,
                jato_model=jato_model,
                safe_window_days=safe_window_days,
                from_date=from_date,
                safe_threshold_pct=safe_threshold_pct,
                safe_limit=safe_limit,
                direction=safe_direction,
            ),
            "summary": summary,
            "powertrainColors": POWERTRAIN_COLORS,
            "events": [],
            "launchAlerts": [],
            "offerSignals": offer_signals,
            "coverage": {"batchA": None},
            "warnings": ["price_history_unavailable"],
            "demo": None,
        }

    current_prices = msrp_repo.list_current_price_alerts(
        session,
        country,
        brand,
        jato_model,
        safe_limit,
        0,
        direction=safe_direction,
        changed_since=since,
        threshold_pct=safe_threshold_pct,
    )
    current_prices = _filter_monitoring_current_prices(current_prices)
    (
        market_scan_country_lengths,
        market_scan_model_lengths,
        length_lookup_warning,
    ) = _load_market_scan_length_lookup(current_prices)
    current_observations = msrp_repo.list_observations_by_ids(
        session,
        [item.effective_observation_id for item in current_prices],
    )
    current_observation_by_id = {
        str(item.observation_id): item for item in current_observations
    }
    sources = msrp_repo.list_sources_by_ids(
        session,
        [item.source_id for item in current_observations],
    )
    source_by_id = {str(item.source_id): item for item in sources}

    grouped_timeline: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    grouped_lengths: dict[tuple[str, str, str], dict[str, tuple[int | None, str | None]]] = {}

    for item in current_prices:
        observation = current_observation_by_id.get(str(item.effective_observation_id))
        source = (
            source_by_id.get(str(observation.source_id))
            if observation is not None
            else None
        )
        group_key = _event_key(item)
        if observation is not None:
            length = (
                _extract_length_from_context(observation.source_context_json)
                or _extract_length_from_context(observation.match_reason_json)
            )
            length_source = "observation_context" if length is not None else None
        else:
            length = None
            length_source = None
        if length is None:
            length = _market_scan_length_for_item(
                item,
                market_scan_country_lengths,
                market_scan_model_lengths,
            )
            length_source = "market_scan" if length is not None else None
        grouped_lengths.setdefault(group_key, {})[item.country] = (
            length,
            length_source,
        )
        history = msrp_repo.list_price_history(
            session,
            item.country,
            item.brand,
            item.jato_model,
            item.jato_trim,
            item.jato_powertrain,
            price_history_limit,
        )
        for index, current_period in enumerate(history[:-1]):
            previous_period = history[index + 1]
            if not _changed_in_window(current_period, since):
                continue
            timeline_item = _timeline_payload(
                item=item,
                current_period=current_period,
                previous_period=previous_period,
                source=source,
                observation=observation,
                threshold_pct=safe_threshold_pct,
                direction=safe_direction,
            )
            if timeline_item is None:
                continue
            grouped_timeline.setdefault(group_key, []).append(timeline_item)

    all_timeline = [
        item
        for group in grouped_timeline.values()
        for item in group
    ]
    _hydrate_timeline_evidence(session, all_timeline)

    events = [
        _build_model_event(
            key,
            timeline,
            grouped_lengths.get(key, {}),
        )
        for key, timeline in grouped_timeline.items()
        if timeline
    ]
    sales_lookup_warning = _attach_market_scan_sales(events)
    events.sort(
        key=lambda item: (
            -int(item.get("affectedCountryCount") or 0),
            -abs(float(item.get("medianChangePct") or 0.0)),
            str(item.get("brand") or ""),
            str(item.get("jatoModel") or ""),
        )
    )

    launch_alerts = (
        _build_launch_alerts(
            session,
            country=country,
            brand=brand,
            jato_model=jato_model,
            since=since,
            limit=safe_limit,
        )
        if safe_direction == "all"
        else []
    )
    coverage = {"batchA": _build_batch_a_coverage(session)}
    summary = _summary_payload(events)
    summary["launchAlertCount"] = len(launch_alerts)
    summary["offerSignalCount"] = len(offer_signals)
    batch_a_coverage = coverage["batchA"]
    if isinstance(batch_a_coverage, dict):
        summary["batchALoadedCountryCount"] = batch_a_coverage.get("loadedCountryCount", 0)
        summary["batchAHistoricalBackfillCountryCount"] = batch_a_coverage.get("historicalBackfillCountryCount", 0)

    return {
        "schemaVersion": "msrp_monitoring_events_v1",
        "mode": MONITORING_MODE_LIVE,
        "generatedAtUtc": generated_at.isoformat(),
        "filters": _monitoring_filters_payload(
            country=country,
            brand=brand,
            jato_model=jato_model,
            safe_window_days=safe_window_days,
            from_date=from_date,
            safe_threshold_pct=safe_threshold_pct,
            safe_limit=safe_limit,
            direction=safe_direction,
        ),
        "summary": summary,
        "powertrainColors": POWERTRAIN_COLORS,
        "events": events,
        "launchAlerts": launch_alerts,
        "offerSignals": offer_signals,
        "coverage": coverage,
        "warnings": [
            item
            for item in [length_lookup_warning, sales_lookup_warning]
            if item
        ],
        "demo": None,
    }
