from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
import json
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen


FRANKFURTER_API_BASE = "https://api.frankfurter.app"
STATIC_RATES_PER_EUR: dict[str, float] = {
    "EUR": 1.0,
    "SEK": 11.5,
    "NOK": 11.3,
    "DKK": 7.46,
    "GBP": 0.86,
    "USD": 1.09,
    "CHF": 0.96,
    "PLN": 4.28,
    "CZK": 24.9,
    "HUF": 392.0,
}


@dataclass(frozen=True)
class FxQuote:
    source_currency: str
    rate_to_eur: float
    as_of_date: date
    source: str


def _normalize_currency(currency: str) -> str:
    normalized = str(currency or "").strip().upper()
    return normalized or "EUR"


@lru_cache(maxsize=512)
def _fetch_frankfurter_rate(
    currency: str,
    quote_date_iso: str,
) -> FxQuote:
    if currency == "EUR":
        as_of = date.fromisoformat(quote_date_iso)
        return FxQuote(
            source_currency="EUR",
            rate_to_eur=1.0,
            as_of_date=as_of,
            source="identity",
        )

    query = urlencode({"from": currency, "to": "EUR"})
    url = f"{FRANKFURTER_API_BASE}/{quote_date_iso}?{query}"
    with urlopen(url, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    rate = float(payload["rates"]["EUR"])
    as_of = date.fromisoformat(str(payload["date"]))
    return FxQuote(
        source_currency=currency,
        rate_to_eur=rate,
        as_of_date=as_of,
        source="frankfurter.app",
    )


def quote_to_eur(
    currency: str,
    observed_at_utc: datetime,
) -> FxQuote:
    normalized_currency = _normalize_currency(currency)
    quote_date = observed_at_utc.date()
    try:
        return _fetch_frankfurter_rate(
            normalized_currency,
            quote_date.isoformat(),
        )
    except (KeyError, ValueError, URLError, TimeoutError, OSError):
        per_eur = STATIC_RATES_PER_EUR.get(normalized_currency)
        if per_eur is None or per_eur <= 0:
            raise ValueError(
                f"No EUR FX rate available for currency={normalized_currency}"
            )
        return FxQuote(
            source_currency=normalized_currency,
            rate_to_eur=(1.0 / per_eur),
            as_of_date=quote_date,
            source="static-fallback",
        )


def convert_amount_to_eur(
    amount: float,
    currency: str,
    observed_at_utc: datetime,
) -> tuple[float, FxQuote]:
    quote = quote_to_eur(currency, observed_at_utc)
    normalized_amount = round(float(amount) * quote.rate_to_eur, 2)
    return normalized_amount, quote