"""Base types for the MSRP scraper framework.

Defines the extractor protocol, raw observation data class, and
extractor configuration that every concrete extractor must follow.
"""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class ExtractorConfig:
    """Declarative configuration that identifies a scraping source."""

    source_code: str
    country: str
    brand: str
    source_url: str
    source_type: str = "manufacturer_official"
    price_semantics: str = "base_msrp"
    requires_location: bool = False


@dataclass
class RawObservation:
    """One price observation produced by an extractor."""

    official_model: str
    official_trim: str
    msrp_value: float
    currency: str
    tax_included: bool
    price_label: str
    source_url: str
    availability_text: str | None = None
    observed_at_utc: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    raw_payload: dict[str, Any] = field(default_factory=dict)
    jato_model: str = ""
    jato_trim: str = ""
    jato_powertrain: str | None = None
    official_edition: str | None = None
    official_powertrain: str | None = None
    match_confidence: float = 0.0
    match_status: str = "review_required"
    match_reason: dict[str, Any] | None = None
    candidate_matches: list[dict[str, Any]] | None = None
    msrp_value_eur: float | None = None
    fx_rate_to_eur: float | None = None

    @property
    def payload_hash(self) -> str:
        blob = json.dumps(
            {
                "model": self.official_model,
                "trim": self.official_trim,
                "edition": self.official_edition,
                "powertrain": self.official_powertrain,
                "msrp": str(self.msrp_value),
                "currency": self.currency,
            },
            sort_keys=True,
        )
        return hashlib.sha256(blob.encode()).hexdigest()[:16]


class BaseExtractor(ABC):
    """Abstract base for all MSRP extractors."""

    def __init__(self, config: ExtractorConfig) -> None:
        self.config = config

    @property
    def extractor_name(self) -> str:
        return type(self).__name__

    @property
    def extractor_version(self) -> str:
        return "0.1.0"

    @abstractmethod
    def extract(self) -> list[RawObservation]:
        """Fetch and parse price data from the configured source."""

    def to_source_payload(self) -> dict[str, Any]:
        cfg = self.config
        return {
            "source_code": cfg.source_code,
            "country": cfg.country,
            "brand": cfg.brand,
            "source_url": cfg.source_url,
            "source_type": cfg.source_type,
            "extractor_name": self.extractor_name,
            "extractor_version": self.extractor_version,
            "price_semantics": cfg.price_semantics,
            "requires_location": cfg.requires_location,
            "enabled": True,
        }
