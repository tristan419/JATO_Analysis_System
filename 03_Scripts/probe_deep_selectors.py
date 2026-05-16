#!/usr/bin/env python3
"""Deep probe: extract actual CSS selectors from manufacturer configurator pages.

Opens each URL, finds elements containing price-like text, and reports
the specific CSS class names and data attributes that can be used as selectors.
"""

import json, re, sys, time
from typing import Any

PRICE_RE = re.compile(r'[\d]{1,3}(?:[.,\s]\d{3})*(?:[.,]\d+)?\s*(?:€|\$|£|kr|SEK|NOK|DKK|PLN|CZK|HUF|CHF)', re.IGNORECASE)

PROBES = [
    ("SKODA", "https://www.skoda.se/modeller/enyaq/enyaq"),
    ("TOYOTA", "https://www.toyota.se/new-cars/rav4"),
    ("KIA", "https://www.kia.com/se/nya-bilar/sportage/"),
    ("HYUNDAI", "https://www.hyundai.se/modeller/kona.html"),
    ("PEUGEOT", "https://www.peugeot.se/modeller/3008.html"),
    ("BMW", "https://www.bmw.se/sv/alla-modeller/x1.html"),
    ("MERCEDES", "https://www.mercedes-benz.se/passengercars/models/suv/eqa/overview.html"),
    ("VOLVO", "https://www.volvocars.com/se/cars/xc60/"),
    ("NISSAN", "https://www.nissan.se/bilar/nye-bilar/qashqai.html"),
    ("RENAULT", "https://www.renault.se/hybrid-bilar/captur.html"),
    ("DACIA", "https://www.dacia.se/modeller/duster.html"),
    ("FORD", "https://www.ford.se/bilar/explorer"),
    ("CUPRA", "https://www.cupraofficial.se/modeller/formentor"),
    ("AUDI", "https://www.audi.se/se/web/sv/models/q4-e-tron.html"),
]


def probe_deep(url: str, brand: str) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    result = {
        "brand": brand, "url": url, "title": "", "error": None,
        "price_elements": [],
        "data_attributes": [],
        "key_classes": [],
    }

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1200})
            page.set_default_timeout(30000)

            try:
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(4000)
            except Exception as e:
                result["error"] = f"goto: {e!s:.80s}"
                browser.close()
                return result

            result["title"] = page.title()

            # Strategy 1: Find elements with data-testid, data-cy, data-automation
            data_attrs = page.evaluate("""() => {
                const attrs = new Set();
                document.querySelectorAll('[data-testid], [data-cy], [data-automation], [data-component]').forEach(el => {
                    for (const attr of ['data-testid', 'data-cy', 'data-automation', 'data-component']) {
                        const v = el.getAttribute(attr);
                        if (v) attrs.add(attr + '="' + v + '"');
                    }
                });
                return [...attrs].slice(0, 30);
            }""")
            result["data_attributes"] = data_attrs

            # Strategy 2: Find elements with price-like text
            price_info = page.evaluate("""(priceRegex) => {
                const results = [];
                const walker = document.createTreeWalker(
                    document.body, NodeFilter.SHOW_TEXT, null, false
                );
                const seen = new Set();
                while (walker.nextNode()) {
                    const text = walker.currentNode.textContent.trim();
                    if (!text || text.length > 200) continue;
                    if (!/[0-9]/.test(text)) continue;
                    const el = walker.currentNode.parentElement;
                    if (!el || seen.has(el)) continue;
                    seen.add(el);

                    // Check if this element or its parent looks like a price
                    const tag = el.tagName.toLowerCase();
                    const cls = el.className && typeof el.className === 'string' ? el.className : '';
                    const parentCls = el.parentElement && typeof el.parentElement.className === 'string' ? el.parentElement.className : '';

                    // Check for price patterns in text
                    if (/[\\d]{1,3}(?:[.,\\s]\\d{3})*(?:[.,]\\d+)?\\s*(?:€|\\$|£|kr|SEK|NOK|DKK)/i.test(text)) {
                        results.push({
                            text: text.substring(0, 80),
                            tag: tag,
                            cls: cls.split(' ').filter(c => c.length > 1 && c.length < 40).slice(0, 5),
                            parentCls: parentCls.split(' ').filter(c => c.length > 1 && c.length < 40).slice(0, 3),
                            dataAttrs: [...el.attributes].filter(a => a.name.startsWith('data-')).map(a => a.name + '="' + a.value + '"'),
                        });
                    }
                }
                return results.slice(0, 20);
            }""")
            result["price_elements"] = price_info

            # Strategy 3: Find key CSS classes (trim, model, version, price)
            key_classes = page.evaluate("""() => {
                const classes = new Set();
                document.querySelectorAll('[class]').forEach(el => {
                    const cls = (el.className || '').toString();
                    cls.split(/\\s+/).forEach(c => {
                        const cl = c.toLowerCase();
                        if (cl.includes('trim') || cl.includes('model') || cl.includes('version') ||
                            cl.includes('variant') || cl.includes('price') || cl.includes('card') ||
                            cl.includes('motor') || cl.includes('engine') || cl.includes('powertrain')) {
                            if (c.length > 2 && c.length < 50) classes.add(c);
                        }
                    });
                });
                return [...classes].sort().slice(0, 50);
            }""")
            result["key_classes"] = key_classes

            browser.close()
    except Exception as e:
        result["error"] = str(e)[:150]

    return result


def main():
    results = []
    for brand, url in PROBES:
        print(f"Deep probing {brand:15s}...", end=" ", flush=True)
        r = probe_deep(url, brand)
        price_count = len(r["price_elements"])
        data_count = len(r["data_attributes"])
        cls_count = len(r["key_classes"])
        status = "ERROR" if r["error"] else f"{price_count} prices, {data_count} data-attrs, {cls_count} classes"
        print(status)
        results.append(r)
        time.sleep(2)

    # Summary: best selectors per brand
    print(f"\n{'='*70}")
    print("RECOMMENDED SELECTORS PER BRAND")
    print(f"{'='*70}")
    for r in results:
        if r["error"]:
            print(f"  {r['brand']:15s} ERROR: {r['error'][:60]}")
            continue
        print(f"\n  {r['brand']} — {r['title'][:60]}")
        if r["data_attributes"]:
            print(f"    data-attrs: {', '.join(r['data_attributes'][:5])}")
        if r["price_elements"]:
            pe = r["price_elements"][0]
            print(f"    best price: text='{pe['text'][:60]}' tag=<{pe['tag']}> cls={' '.join(pe['cls'][:3])}")
        if r["key_classes"]:
            # Find classes that look like card containers
            card_like = [c for c in r["key_classes"] if 'card' in c or 'item' in c or 'tile' in c]
            trim_like = [c for c in r["key_classes"] if 'trim' in c or 'version' in c or 'variant' in c]
            price_like = [c for c in r["key_classes"] if 'price' in c]
            if card_like: print(f"    card classes: {', '.join(card_like[:5])}")
            if trim_like: print(f"    trim classes: {', '.join(trim_like[:5])}")
            if price_like: print(f"    price classes: {', '.join(price_like[:5])}")

    Path("/tmp/deep_probe_results.json").write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\nFull results: /tmp/deep_probe_results.json")


if __name__ == "__main__":
    main()
