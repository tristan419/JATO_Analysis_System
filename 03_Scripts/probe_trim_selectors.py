#!/usr/bin/env python3
"""Probe manufacturer websites to discover trim card CSS selectors.

Opens each URL with Playwright and reports which CSS selectors match elements,
so we can create proper presets without manual browser inspection.
"""

import json, sys, time
from pathlib import Path
from typing import Any

# Common selector patterns used by modern configurators
COMMON_PATTERNS = {
    "trim_cards": [
        '[data-testid="trimcard"]',
        '[data-testid="trim-card"]',
        '[data-cy="trim-card"]',
        '[class*="trim"][class*="card"]',
        '[class*="TrimCard"]',
        '[class*="modelCard"]',
        '[class*="ModelCard"]',
        '.cmp-modelcard',
        '.trim-card',
        '.trim-overview__card',
        '[data-component="trim-overview"]',
        '[data-automation="trim-card"]',
        'article[class*="trim"]',
        'div[class*="version"]',
        'li[class*="trim"]',
        '.configurator-trim',
        '.version-card',
        '.motorisation-card',
    ],
    "prices": [
        '[data-testid="prices"]',
        '[data-testid="price"]',
        '[data-cy="price"]',
        '[class*="price"]',
        '.cmp-modelcard__price',
        '.trim-card__price',
        '[data-component="price"]',
        '.price-block',
        '.msrp',
        '.starting-price',
        'span[class*="Price"]',
        'div[class*="Price"]',
    ],
    "trim_names": [
        'h2', 'h3', 'h4',
        '[data-testid="trim-name"]',
        '[class*="trim-name"]',
        '[class*="TrimName"]',
        '.cmp-modelcard__title',
        '.trim-card__title',
        'strong[class*="version"]',
    ]
}

def probe_url(url: str, brand: str, timeout_ms: int = 30000) -> dict[str, Any]:
    """Open a URL and test common selectors."""
    from playwright.sync_api import sync_playwright

    result = {"url": url, "brand": brand, "found": {}, "title": "", "error": None}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1200})
            page.set_default_timeout(timeout_ms)

            try:
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(3000)  # Wait for JS to render
            except Exception as e:
                result["error"] = f"goto: {e!s:.80s}"
                browser.close()
                return result

            result["title"] = page.title()

            for category, selectors in COMMON_PATTERNS.items():
                for sel in selectors:
                    try:
                        count = len(page.query_selector_all(sel))
                        if count > 0:
                            result["found"].setdefault(category, []).append(
                                {"selector": sel, "count": count}
                            )
                    except Exception:
                        pass

            browser.close()
    except Exception as e:
        result["error"] = str(e)[:120]

    return result


def main():
    # Key manufacturer URLs (one per brand, Sweden market for consistency)
    probes = [
        ("SKODA", "https://www.skoda.se/modeller/enyaq/enyaq"),
        ("TOYOTA", "https://www.toyota.se/new-cars/rav4"),
        ("VOLVO", "https://www.volvocars.com/se/build/xc40/"),
        ("KIA", "https://www.kia.com/se/nya-bilar/sportage/"),
        ("BMW", "https://www.bmw.se/sv/alla-modeller/x1.html"),
        ("MERCEDES", "https://www.mercedes-benz.se/passengercars/models/suv/eqa/overview.html"),
        ("AUDI", "https://www.audi.se/se/web/sv/models/q4-e-tron.html"),
        ("PEUGEOT", "https://www.peugeot.se/modeller/3008.html"),
        ("HYUNDAI", "https://www.hyundai.se/modeller/kona.html"),
        ("FORD", "https://www.ford.se/bilar/explorer"),
        ("NISSAN", "https://www.nissan.se/bilar/nye-bilar/qashqai.html"),
        ("CUPRA", "https://www.cupraofficial.se/modeller/formentor"),
        ("RENAULT", "https://www.renault.se/hybrid-bilar/captur.html"),
        ("DACIA", "https://www.dacia.se/modeller/duster.html"),
    ]

    results = []
    for brand, url in probes:
        print(f"Probing {brand:15s} {url[:70]}...", end=" ", flush=True)
        r = probe_url(url, brand)
        found_count = sum(len(v) for v in r["found"].values())
        if r["error"]:
            print(f"ERROR: {r['error'][:60]}")
        else:
            print(f"OK - {found_count} selectors matched, title={r['title'][:50]}")
        results.append(r)
        time.sleep(2)  # Be nice to servers

    # Print summary
    print(f"\n{'='*70}")
    print("SUMMARY: Selector Discovery")
    print(f"{'='*70}")
    for r in results:
        if r["error"]:
            continue
        brand = r["brand"]
        for cat in ["trim_cards", "prices", "trim_names"]:
            found = r["found"].get(cat, [])
            if found:
                best = found[0]
                print(f"  {brand:15s} {cat:15s} → {best['selector']:45s} (×{best['count']})")

    Path("/tmp/probe_results.json").write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\nFull results: /tmp/probe_results.json")


if __name__ == "__main__":
    main()
