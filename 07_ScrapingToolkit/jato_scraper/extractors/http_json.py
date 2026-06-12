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
class LookupMapping:
    source_path: str
    collection_path: str
    key_path: str = "id"
    value_path: str = "name"


FieldMappingPath = str | LookupMapping


@dataclass(frozen=True)
class FieldMapping:
    model: FieldMappingPath | tuple[FieldMappingPath, ...] = "model"
    trim: FieldMappingPath | tuple[FieldMappingPath, ...] = "trim"
    price: str = "price"
    currency: str = "currency"
    tax_included: str = "taxIncluded"
    price_label: str = "priceLabel"
    availability: str | None = "availability"
    vehicles_path: str = "models"
    items_path: str | None = None


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
    fixed_model: str | None = None


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
        return "0.2.0"

    def extract(self) -> list[RawObservation]:
        raw_json = self._fetch()
        if raw_json is None:
            self.record_strategy_audit(
                url=self.profile.url,
                strategy="http_json",
                observations=[],
                winning_strategy=None,
                error="fetch_failed",
            )
            return []
        vehicles = self._navigate(raw_json)
        if vehicles is None:
            self.record_strategy_audit(
                url=self.profile.url,
                strategy="http_json",
                observations=[],
                winning_strategy=None,
                error="navigation_failed",
            )
            return []
        vehicles = self._expand_items(vehicles)
        results = self._map(vehicles, root=raw_json)
        self.record_strategy_audit(
            url=self.profile.url,
            strategy="http_json",
            observations=results,
            winning_strategy="http_json" if results else None,
        )
        return results

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

    def _resolve_path(
        self,
        data: Any,
        path: str,
        *,
        log_errors: bool,
    ) -> Any:
        node = data
        for key in path.split("."):
            if isinstance(node, dict):
                node = node.get(key)
            elif isinstance(node, list):
                try:
                    index = int(key)
                except ValueError:
                    if log_errors:
                        log.error(
                            "Cannot navigate path '%s' at key '%s' — list index required",
                            path,
                            key,
                        )
                    return None
                if index < 0 or index >= len(node):
                    if log_errors:
                        log.error(
                            "Path '%s' index '%s' out of range for list of size %s",
                            path,
                            key,
                            len(node),
                        )
                    return None
                node = node[index]
            else:
                if log_errors:
                    log.error(
                        "Cannot navigate path '%s' at key '%s' — node is %s",
                        path,
                        key,
                        type(node).__name__,
                    )
                return None
            if node is None:
                if log_errors:
                    log.error("Path '%s' not found at key '%s'", path, key)
                return None
        return node

    def _navigate(self, data: Any) -> list[dict] | None:
        path = self.profile.field_mapping.vehicles_path
        node = self._resolve_path(data, path, log_errors=True)
        if node is None:
            return None
        if not isinstance(node, list):
            log.error("Expected list at path '%s', got %s", path, type(node).__name__)
            return None
        return node

    def _expand_items(self, vehicles: list[dict]) -> list[dict]:
        items_path = self.profile.field_mapping.items_path
        if not items_path:
            return vehicles
        flattened: list[dict] = []
        for vehicle in vehicles:
            if not isinstance(vehicle, dict):
                log.warning("Skipping non-dict vehicle container: %s", type(vehicle).__name__)
                continue
            items = self._resolve_path(vehicle, items_path, log_errors=False)
            if items is None:
                continue
            if not isinstance(items, list):
                log.warning(
                    "Expected list at nested items path '%s', got %s",
                    items_path,
                    type(items).__name__,
                )
                continue
            for item in items:
                if not isinstance(item, dict):
                    log.warning("Skipping non-dict nested item: %s", type(item).__name__)
                    continue
                flattened.append({**vehicle, **item})
        return flattened

    def _resolve_field_value(
        self,
        vehicle: dict[str, Any],
        mapping: FieldMappingPath | tuple[FieldMappingPath, ...] | None,
        *,
        root: Any | None = None,
    ) -> Any:
        if mapping is None:
            return None
        if isinstance(mapping, tuple):
            parts: list[str] = []
            for field in mapping:
                value = self._resolve_field_value(
                    vehicle,
                    field,
                    root=root,
                )
                if value is None:
                    continue
                text = str(value).strip()
                if text:
                    parts.append(text)
            return " / ".join(parts)
        if isinstance(mapping, LookupMapping):
            lookup_key = self._resolve_path(
                vehicle,
                mapping.source_path,
                log_errors=False,
            )
            if lookup_key in (None, "") or root is None:
                return None
            collection = self._resolve_path(
                root,
                mapping.collection_path,
                log_errors=False,
            )
            if not isinstance(collection, list):
                return None
            lookup_key_text = str(lookup_key)
            for item in collection:
                if not isinstance(item, dict):
                    continue
                candidate_key = self._resolve_path(
                    item,
                    mapping.key_path,
                    log_errors=False,
                )
                if str(candidate_key) != lookup_key_text:
                    continue
                return self._resolve_path(
                    item,
                    mapping.value_path,
                    log_errors=False,
                )
            return None
        if not mapping:
            return None
        return self._resolve_path(vehicle, mapping, log_errors=False)

    def _map(
        self,
        vehicles: list[dict],
        *,
        root: Any | None = None,
    ) -> list[RawObservation]:
        fm = self.profile.field_mapping
        p = self.profile
        results: list[RawObservation] = []
        for v in vehicles:
            try:
                official_model = str(
                    p.fixed_model
                    or self._resolve_field_value(v, fm.model, root=root)
                    or ""
                )
                official_trim = str(
                    self._resolve_field_value(v, fm.trim, root=root) or ""
                )
                msrp_value = float(
                    self._resolve_field_value(v, fm.price, root=root) or 0
                )
                currency = str(
                    self._resolve_field_value(v, fm.currency, root=root)
                    or p.default_currency
                )
                tax_included = self._resolve_field_value(
                    v,
                    fm.tax_included,
                    root=root,
                )
                price_label = self._resolve_field_value(
                    v,
                    fm.price_label,
                    root=root,
                )
                availability = (
                    self._resolve_field_value(v, fm.availability, root=root)
                    if fm.availability
                    else None
                )
                obs = RawObservation(
                    official_model=official_model,
                    official_trim=official_trim,
                    msrp_value=msrp_value,
                    currency=currency,
                    tax_included=(
                        bool(tax_included)
                        if tax_included is not None
                        else p.default_tax_included
                    ),
                    price_label=str(price_label or p.default_price_label),
                    source_url=self.config.source_url,
                    availability_text=(
                        str(availability).strip() if availability is not None else None
                    ),
                    raw_payload=v,
                )
                results.append(obs)
            except (TypeError, ValueError) as exc:
                log.warning("Skipping vehicle entry: %s — %s", v, exc)
        return results
