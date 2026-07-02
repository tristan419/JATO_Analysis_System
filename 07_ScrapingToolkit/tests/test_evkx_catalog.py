from __future__ import annotations

from jato_scraper.evkx_catalog import (
    EvkxFetchOptions,
    fetch_search_catalog,
    parse_pricing_section,
    parse_specifications_page,
    select_local_pricing_items,
)


_DETAIL_HTML = """
<section>
  <h3>Pricing</h3>
  <ul>
    <li>$68,500 (USA)</li>
    <li>CA$84,990 (Canada)</li>
  </ul>
</section>
""".strip()

_SPEC_HTML = """
<!-- Performance Section -->
<section>
  <h2>Performance</h2>
  <table>
    <thead><tr><th>Spec</th><th>Value</th></tr></thead>
    <tbody>
      <tr><td>Peak power</td><td>365 kW</td></tr>
      <tr><td>Top speed</td><td>210 kph</td></tr>
    </tbody>
  </table>
</section>
<!-- Battery Section -->
<section>
  <h2>Battery</h2>
  <table>
    <thead><tr><th>Spec</th><th>Value</th></tr></thead>
    <tbody>
      <tr><td>Battery net</td><td>107.5 kWh</td></tr>
      <tr><td>Max DC charging</td><td>190 kW</td></tr>
    </tbody>
  </table>
</section>
""".strip()


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def post(self, url, json, timeout):
        self.calls.append((url, json, timeout))
        return _FakeResponse(self._responses.pop(0))


def test_parse_pricing_section_extracts_amounts_and_markets() -> None:
    pricing = parse_pricing_section(_DETAIL_HTML)

    assert pricing == [
        {
            "marketLabel": "USA",
            "priceText": "$68,500",
            "amount": 68500.0,
            "currency": "USD",
        },
        {
            "marketLabel": "Canada",
            "priceText": "CA$84,990",
            "amount": 84990.0,
            "currency": "CAD",
        },
    ]


def test_parse_specifications_page_maps_sections_to_key_values() -> None:
    sections = parse_specifications_page(_SPEC_HTML)

    assert sections["Performance"]["Peak power"] == "365 kW"
    assert sections["Performance"]["Top speed"] == "210 kph"
    assert sections["Battery"]["Battery net"] == "107.5 kWh"
    assert sections["Battery"]["Max DC charging"] == "190 kW"


def test_fetch_search_catalog_paginates_until_last_page() -> None:
    session = _FakeSession(
        [
            {
                "evs": [{"evId": "1"}, {"evId": "2"}],
                "hasNextPage": True,
            },
            {
                "evs": [{"evId": "3"}],
                "hasNextPage": False,
            },
        ]
    )

    items = fetch_search_catalog(
        session,
        EvkxFetchOptions(
            pricing_country="UnitedStates",
            availability_filter="current",
            page_size=2,
            include_details=False,
        ),
    )

    assert [item["evId"] for item in items] == ["1", "2", "3"]
    assert session.calls[0][1]["page"] == 1
    assert session.calls[1][1]["page"] == 2


def test_select_local_pricing_items_filters_converted_cross_market_prices() -> None:
    items = [
        {
            "name": "Tesla Model Y Standard",
            "startPrice": 499990,
            "currency": "SEK",
            "pricingCountry": "Sweden",
            "isConverted": False,
        },
        {
            "name": "Tesla Model Y Long Range RWD",
            "startPrice": 502630,
            "currency": "SEK",
            "pricingCountry": "Australia",
            "isConverted": True,
        },
        {
            "name": "Tesla Model Y RWD",
            "startPrice": 434152,
            "currency": "SEK",
            "pricingCountry": "Sweden",
            "isConverted": True,
        },
        {
            "name": "Volvo EX30",
            "startPrice": 429000,
            "currency": "SEK",
            "pricingCountry": "Sweden",
            "isConverted": False,
        },
        {
            "name": "Tesla Model Y Missing Price",
            "currency": "SEK",
            "pricingCountry": "Sweden",
            "isConverted": False,
        },
    ]

    selected = select_local_pricing_items(
        items,
        pricing_country="Sweden",
        name_contains="Tesla Model Y",
    )

    assert selected == [items[0]]
