"""Generic HTTP JSON API extractor."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import requests

from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation

log = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 30


@dataclass(frozen=True)
class FieldMapping:
    model: str = "model"
    trim: str = "trim"
    price: str = "price"
    currency: str = "currency"
    tax_included: str = "taxIncluded"
    price_label: str = "priceLabel"
    availability: str | None = "availability"
    vehicles_path: str = "models"


@dataclass(frozen=True)
class HttpJsonProfile:
    url: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, str] = field(default_factory=dict)
    body: dict[str, Any] | None = None
    field_mapping: FieldMapping = field(default_factory=FieldMapping)
    default_currency: str = "EUR"
    default_tax_included: bool = True
    default_price_label: str = "Manufacturer's Recommended Retail Price"


class HttpJsonExtractor(BaseExtractor):
    def __init__(self, config: ExtractorConfig, profile: HttpJsonProfile) -> None:
        super().__init__(config)
        self.profile = profile
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "JATO-MSRP-Scraper/0.1",
                "Accept": "application/json",
                **profile.headers,
            }
        )

    @property
    def extractor_version(self) -> str:
        return "0.1.0"

    def extract(self) -> list[RawObservation]:
        raw_json = self._fetch()
        if raw_json is None:
            return []
        vehicles = self._navigate(raw_json)
        if vehicles is None:
            return []
        return self._map(vehicles)

    def _fetch(self) -> dict | list | None:
        p = self.profile
        try:
            if p.method.upper() == "POST":
                resp = self._session.post(
                    p.url, json=p.body, params=p.params, timeout=DEFAULT_TIMEOUT
                )
            else:
                resp = self._session.get(
                    p.url, params=p.params, timeout=DEFAULT_TIMEOUT
                )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.error("HTTP request failed for %s: %s", self.config.source_code, exc)
            return None

    def _navigate(self, data: Any) -> list[dict] | None:
        path = self.profile.field_mapping.vehicles_path
        node = data
        for key in path.split("."):
            if isinstance(node, dict):
                node = node.get(key)
            else:
                log.error(
                    "Cannot navigate path '%s' at key '%s' — node is %s",
                    path,
                    key,
                    type(node).__name__,
                )
                return None
            if node is None:
                log.error("Path '%s' not found at key '%s'", path, key)
                return None
        if not isinstance(node, list):
            log.error("Expected list at path '%s', got %s", path, type(node).__name__)
            return None
        return node

    def _map(self, vehicles: list[dict]) -> list[RawObservation]:
        fm = self.profile.field_mapping
        p = self.profile
        results: list[RawObservation] = []
        for v in vehicles:
            try:
                obs = RawObservation(
                    official_model=str(v.get(fm.model, "")),
                    official_trim=str(v.get(fm.trim, "")),
                    msrp_value=float(v.get(fm.price, 0)),
                    currency=str(v.get(fm.currency, p.default_currency)),
                    tax_included=bool(v.get(fm.tax_included, p.default_tax_included)),
                    price_label=str(v.get(fm.price_label, p.default_price_label)),
                    source_url=p.url,
                    availability_text=(
                        str(v[fm.availability]) if fm.availability and fm.availability in v else None
                    ),
                    raw_payload=v,
                )
                results.append(obs)
            except (TypeError, ValueError) as exc:
                log.warning("Skipping vehicle entry: %s — %s", v, exc)
        return results

