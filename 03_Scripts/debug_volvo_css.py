#!/usr/bin/env python3
"""Debug Volvo CSS extraction."""

import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"))

from scrapling import StealthyFetcher

url = sys.argv[1] if len(sys.argv) > 1 else "https://www.volvocars.com/se/build/ex40-electric/"
print(f"Fetching: {url}")
f = StealthyFetcher()
page = f.fetch(url)

cards = page.css('[data-testid="selection-card"]')
print(f"Total selection-card elements: {len(cards)}")

for i, card in enumerate(cards[:2]):
    unavail = card.css('[data-testid="selection-card-unavailable"]')
    title_els = card.css('[data-testid="selection-card-title"]')
    price_els = card.css('[data-testid="selection-card-price"]')

    is_unavail = len(unavail) > 0
    title_text = title_els[0].text if title_els else "NONE"
    price_text = price_els[0].text if price_els else "NONE"

    print(f"  [{i}] unavail={is_unavail}  title={title_text}  price={price_text}")

    # Show all nested data-testid elements
    nested = card.css("[data-testid]")
    for n in nested:
        tid = n.attrib.get("data-testid", "")
        txt = (n.text or "")[:60].strip()
        print(f"      data-testid={tid}  text={txt}")

    # Show card text content
    card_text = card.text or ""
    print(f"    card.text = {card_text[:200]}")
    print()
