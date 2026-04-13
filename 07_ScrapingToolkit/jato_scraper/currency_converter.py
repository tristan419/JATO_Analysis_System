"""Currency conversion helper — fetches live FX rates and converts to EUR.

Uses the free ECB-backed exchangerate.host API (no key required).
Rates are cached per session so only one HTTP call per run.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from jato_scraper.base import RawObservation

log = logging.getLogger(__name__)

# Free public APIs ordered by preference
_RATE_URLS: list[str] = [
    "https://open.er-api.com/v6/latest/EUR",
    "https://api.exchangerate-api.com/v4/latest/EUR",
]

_TIMEOUT = 15

# Module-level cache: populated on first call, reused for the session
_cached_rates: dict[str, float] | None = None
_cached_at: datetime | None = None


def _fetch_rates() -> dict[str, float]:
    """Fetch 1 EUR = X local-currency rates from a free public API."""
    last_err: Exception | None = None
    for url in _RATE_URLS:
        try:
            resp = requests.get(url, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            rates: dict[str, Any] = data.get("rates", {})
            if rates:
                log.info(
                    "Fetched %d FX rates from %s (base=EUR)",
                    len(rates),
                    url,
                )
                return {k: float(v) for k, v in rates.items()}
        except Exception as exc:  # noqa: BLE001
            log.warning("FX rate fetch failed for %s: %s", url, exc)
            last_err = exc
    raise RuntimeError(
        f"Could not fetch FX rates from any source: {last_err}"
    )


def get_rates(*, force_refresh: bool = False) -> dict[str, float]:
    """Return cached EUR-based rate map, fetching if needed."""
    global _cached_rates, _cached_at  # noqa: PLW0603
    if _cached_rates is not None and not force_refresh:
        return _cached_rates
    _cached_rates = _fetch_rates()
    _cached_at = datetime.now(timezone.utc)
    return _cached_rates


def convert_to_eur(
    value: float,
    currency: str,
) -> tuple[float, float]:
    """Convert *value* in *currency* to EUR.

    Returns ``(eur_value, fx_rate)`` where ``fx_rate`` is
    ``1 EUR = fx_rate <currency>``.

    If the currency is already EUR the rate is 1.0.
    """
    if currency == "EUR":
        return value, 1.0
    rates = get_rates()
    rate = rates.get(currency)
    if rate is None:
        raise KeyError(f"No FX rate found for currency {currency!r}")
    if rate == 0:
        raise ValueError(f"FX rate for {currency} is zero")
    eur_value = round(value / rate, 2)
    return eur_value, rate


def enrich_observations_with_eur(
    observations: list[RawObservation],
) -> None:
    """Attach ``msrp_value_eur`` and ``fx_rate_to_eur`` to each observation.

    Modifies observations in-place by setting the two EUR fields.
    Skips (with a warning) any observation whose currency has no rate.
    """
    # Prefetch rates once
    try:
        get_rates()
    except RuntimeError:
        log.error("Cannot enrich observations — FX rate fetch failed")
        return

    for obs in observations:
        try:
            eur_val, rate = convert_to_eur(obs.msrp_value, obs.currency)
            obs.msrp_value_eur = eur_val
            obs.fx_rate_to_eur = rate
        except (KeyError, ValueError) as exc:
            log.warning(
                "FX conversion skipped for %s/%s (%s %s): %s",
                obs.official_model,
                obs.official_trim,
                obs.msrp_value,
                obs.currency,
                exc,
            )
