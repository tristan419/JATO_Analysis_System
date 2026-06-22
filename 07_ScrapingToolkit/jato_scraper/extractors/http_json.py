"""Generic HTTP JSON API extractor."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import requests

from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation

log = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 30
_PRICE_RE = re.compile(r"[\d]+(?:[.,'\u2019]\d{3})*(?:[.,]\d{1,2})?")
_FINANCE_AMOUNT_FIELDS = frozenset({
    "monthly_payment",
    "down_payment",
    "down_payment_pct",
    "apr",
    "effective_apr",
    "balloon_payment",
    "total_credit_cost",
    "total_amount_payable",
    "subsidy_amount",
    "net_price_after_subsidy",
})
_FINANCE_INTEGER_FIELDS = frozenset({
    "term_months",
    "annual_mileage_limit",
})


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
class PricingContextMapping:
    fields: dict[str, str] = field(default_factory=dict)
    constants: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValueFilter:
    path: str
    equals: tuple[str, ...] = ()


@dataclass(frozen=True)
class MinPriceGroup:
    key: FieldMappingPath | tuple[FieldMappingPath, ...]
    price: str


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
    fixed_official_powertrain: str | None = None
    fixed_jato_model: str | None = None
    fixed_jato_powertrain: str | None = None
    copy_trim_to_jato_trim: bool = False
    match_confidence: float | None = None
    match_status: str = "review_required"
    match_reason: dict[str, Any] | None = None
    filters: tuple[ValueFilter, ...] = ()
    min_price_group: MinPriceGroup | None = None
    pricing_context: PricingContextMapping | None = None


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
        audit_url = getattr(self, "_last_request_url", self.profile.url)
        if raw_json is None:
            self.record_strategy_audit(
                url=audit_url,
                strategy="http_json",
                observations=[],
                winning_strategy=None,
                error="fetch_failed",
            )
            return []
        vehicles = self._navigate(raw_json)
        if vehicles is None:
            self.record_strategy_audit(
                url=audit_url,
                strategy="http_json",
                observations=[],
                winning_strategy=None,
                error="navigation_failed",
            )
            return []
        vehicles = self._expand_items(vehicles)
        vehicles = self._apply_filters(vehicles, root=raw_json)
        vehicles = self._apply_min_price_grouping(vehicles, root=raw_json)
        results = self._map(vehicles, root=raw_json)
        self.record_strategy_audit(
            url=audit_url,
            strategy="http_json",
            observations=results,
            winning_strategy="http_json" if results else None,
        )
        return results

    def _template_context(self) -> dict[str, str]:
        today = date.today().isoformat()
        return {"today": today, "current_date": today}

    def _render_request_value(self, value: Any) -> Any:
        if isinstance(value, str):
            rendered = value
            for key, replacement in self._template_context().items():
                rendered = rendered.replace("{" + key + "}", replacement)
            return rendered
        if isinstance(value, dict):
            return {
                self._render_request_value(key): self._render_request_value(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._render_request_value(item) for item in value]
        return value

    def _fetch(self) -> dict | list | None:
        p = self.profile
        url = self._render_request_value(p.url)
        params = self._render_request_value(p.params)
        body = self._render_request_value(p.body)
        self._last_request_url = url
        try:
            if p.method.upper() == "POST":
                resp = self._session.post(
                    url, json=body, params=params, timeout=DEFAULT_TIMEOUT
                )
            else:
                resp = self._session.get(
                    url, params=params, timeout=DEFAULT_TIMEOUT
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
            lookup_key_text = str(lookup_key)
            if isinstance(collection, dict):
                item = collection.get(lookup_key_text)
                if not isinstance(item, dict):
                    return None
                return self._resolve_path(
                    item,
                    mapping.value_path,
                    log_errors=False,
                )
            if not isinstance(collection, list):
                return None
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

    def _apply_filters(
        self,
        vehicles: list[dict],
        *,
        root: Any | None = None,
    ) -> list[dict]:
        if not self.profile.filters:
            return vehicles
        filtered: list[dict] = []
        for vehicle in vehicles:
            include = True
            for filter_ in self.profile.filters:
                actual = self._resolve_field_value(
                    vehicle,
                    filter_.path,
                    root=root,
                )
                actual_text = "" if actual is None else str(actual).strip()
                if filter_.equals and actual_text not in filter_.equals:
                    include = False
                    break
            if include:
                filtered.append(vehicle)
        return filtered

    def _apply_min_price_grouping(
        self,
        vehicles: list[dict],
        *,
        root: Any | None = None,
    ) -> list[dict]:
        group = self.profile.min_price_group
        if group is None:
            return vehicles

        selected: dict[str, tuple[float, dict]] = {}
        passthrough: list[dict] = []
        for vehicle in vehicles:
            group_key = self._resolve_field_value(vehicle, group.key, root=root)
            if group_key in (None, ""):
                passthrough.append(vehicle)
                continue
            try:
                price = self._coerce_price_value(
                    self._resolve_field_value(vehicle, group.price, root=root)
                )
            except (TypeError, ValueError):
                passthrough.append(vehicle)
                continue

            key_text = str(group_key).strip()
            current = selected.get(key_text)
            if current is None or price < current[0]:
                selected[key_text] = (price, vehicle)

        return [item for _, item in selected.values()] + passthrough

    def _coerce_pricing_context_value(
        self,
        field_name: str,
        value: Any,
    ) -> Any | None:
        if value in (None, "", [], {}):
            return None
        if field_name in _FINANCE_AMOUNT_FIELDS:
            if isinstance(value, (int, float)):
                return value
            parsed = self._parse_amount(str(value))
            return parsed if parsed is not None else value
        if field_name in _FINANCE_INTEGER_FIELDS:
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            parsed = self._parse_amount(str(value))
            return int(parsed) if parsed is not None else value
        if isinstance(value, str):
            return value.strip() or None
        return value

    def _parse_amount(self, raw: str) -> float | None:
        match = _PRICE_RE.search(raw.replace("\xa0", "").replace(" ", ""))
        if not match:
            return None
        value = match.group().replace("'", "").replace("\u2019", "")
        if "," in value and "." in value:
            if value.rfind(",") > value.rfind("."):
                value = value.replace(".", "").replace(",", ".")
            else:
                value = value.replace(",", "")
        elif "," in value:
            parts = value.split(",")
            if len(parts[-1]) <= 2:
                value = value.replace(",", ".")
            else:
                value = value.replace(",", "")
        elif "." in value:
            parts = value.split(".")
            if all(len(part) == 3 for part in parts[1:]):
                value = value.replace(".", "")
        return float(value)

    def _coerce_price_value(self, value: Any) -> float:
        if value in (None, ""):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        parsed = self._parse_amount(str(value or ""))
        if parsed is None:
            raise ValueError(f"price field is not numeric: {value!r}")
        return parsed

    def _build_pricing_context(
        self,
        vehicle: dict[str, Any],
        *,
        root: Any | None = None,
    ) -> dict[str, Any]:
        mapping = self.profile.pricing_context
        if mapping is None:
            return {}
        context: dict[str, Any] = {}
        for field_name, path in mapping.fields.items():
            raw_value = self._resolve_field_value(
                vehicle,
                path,
                root=root,
            )
            coerced = self._coerce_pricing_context_value(
                field_name,
                raw_value,
            )
            if coerced is not None:
                context[field_name] = coerced
        for field_name, value in mapping.constants.items():
            coerced = self._coerce_pricing_context_value(
                field_name,
                value,
            )
            if coerced is not None:
                context[field_name] = coerced
        return context

    def _raw_payload_with_pricing_context(
        self,
        vehicle: dict[str, Any],
        *,
        root: Any | None = None,
    ) -> dict[str, Any]:
        pricing_context = self._build_pricing_context(vehicle, root=root)
        if not pricing_context:
            return vehicle
        return {
            **vehicle,
            "pricingContext": pricing_context,
        }

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
                ).strip()
                official_trim = str(
                    self._resolve_field_value(v, fm.trim, root=root) or ""
                ).strip()
                msrp_value = self._coerce_price_value(
                    self._resolve_field_value(v, fm.price, root=root)
                )
                currency = str(
                    self._resolve_field_value(v, fm.currency, root=root)
                    or p.default_currency
                ).strip()
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
                raw_payload = self._raw_payload_with_pricing_context(
                    v,
                    root=root,
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
                    raw_payload=raw_payload,
                    jato_model=str(p.fixed_jato_model or ""),
                    jato_trim=official_trim if p.copy_trim_to_jato_trim else "",
                    jato_powertrain=p.fixed_jato_powertrain,
                    official_powertrain=p.fixed_official_powertrain,
                    match_confidence=float(p.match_confidence or 0.0),
                    match_status=p.match_status,
                    match_reason=p.match_reason,
                )
                results.append(obs)
            except (TypeError, ValueError) as exc:
                log.warning("Skipping vehicle entry: %s — %s", v, exc)
        return results
