#!/usr/bin/env python3
"""Probe 1 representative URL per brand for extraction strategy.

Checks for:
  1. schema.org ld+json with price (json_script strategy)
  2. data-* attributes with price JSON (attr_json strategy)
  3. Visible price text in DOM (css strategy)
"""
import json
import re
import sys
import time
import yaml
import os
from collections import defaultdict

# ── Brand probe list (1 URL per brand) ────────────────────────────────
BRAND_PROBES = {
    "AUDI":        "https://www.audi.se/se/web/sv/models/q4-e-tron.html",
    "BMW":         "https://www.bmw.se/sv/alla-modeller/ix1.html",
    "BYD":         "https://www.byd.com/no/car/seal-ion-7",
    "CUPRA":       "https://www.cupraofficial.se/modeller/terramar",
    "DACIA":       "https://www.dacia.hr/vozila/duster.html",
    "FORD":        "https://www.ford.hr/automobili/kuga",
    "GEELY":       "https://www.geely-auto.hr/coolray",
    "HYUNDAI":     "https://www.hyundai.hr/automobili/tucson/",
    "JAECOO":      "https://www.jaecoo.com/hu/models/j7",
    "KGM":         "https://www.kgmmotors.hu/korando",
    "KIA":         "https://www.kia.com/se/nya-bilar/ev3/premiere",
    "MAZDA":       "https://www.mazda.hr/modeli/mazda-cx-30.html",
    "MERCEDES":    "https://www.mercedes-benz.se/passengercars/models/suv/eqa/overview.html",
    "MG":          "https://www.mgmotor.hr/model/mg-zs",
    "NISSAN":      "https://www.nissan.hr/vozila/nova-vozila/qashqai.html",
    "OMODA":       "https://www.omoda.com/hu/models/omoda-5",
    "OPEL":        "https://www.opel.hr/modeli/mokka.html",
    "PEUGEOT":     "https://www.peugeot.se/modeller/suv-3008.html",
    "POLESTAR":    "https://www.polestar.com/se/polestar-4/",
    "PORSCHE":     "https://www.porsche.com/norway/models/macan/",
    "RENAULT":     "https://www.renault.hr/vozila/captur.html",
    "SEAT":        "https://www.seat.at/modelle/arona",
    "SKODA":       "https://www.skoda.se/modeller/kodiaq",
    "SUZUKI":      "https://auto.suzuki.hr/cars/vitara-hybrid",
    "TESLA":       "https://www.tesla.com/sv_se/modely",
    "TOYOTA":      "https://www.toyota.se/new-cars/rav4",
    "VOLKSWAGEN":  "https://www.volkswagen.se/sv/modeller/tiguan.html",
    "VOLVO":       "https://www.volvocars.com/se/build/ex40-electric/",
    "XPENG":       "https://www.xpeng.com/no/g6",
}


def probe_one(brand: str, url: str) -> dict:
    """Probe a single brand URL and return strategy findings."""
    from scrapling.fetchers import StealthyFetcher

    result = {
        "brand": brand,
        "url": url,
        "http_status": None,
        "strategies": [],
        "ld_json_has_price": False,
        "ld_json_price": None,
        "ld_json_currency": None,
        "ld_json_model": None,
        "data_attr_price_found": False,
        "visible_price_found": False,
        "price_in_raw_html": False,
        "raw_price_values": [],
        "error": None,
    }

    try:
        page = StealthyFetcher.fetch(
            url, headless=True, network_idle=True
        )
        result["http_status"] = page.status
        if page.status >= 400:
            result["error"] = f"HTTP {page.status}"
            return result

        body_str = (
            page.body.decode("utf-8", errors="ignore")
            if isinstance(page.body, bytes)
            else str(page.body)
        )

        # ── Strategy 1: ld+json ─────────────────────────────────────
        scripts = page.css('script[type="application/ld+json"]')
        for s in scripts:
            txt = s.text if hasattr(s, "text") else ""
            if not txt:
                continue
            try:
                data = json.loads(txt)
            except json.JSONDecodeError:
                continue
            if not isinstance(data, dict):
                continue
            # Check for direct price or offers.price
            price = data.get("price") or (
                data.get("offers", {}).get("price")
                if isinstance(data.get("offers"), dict)
                else None
            )
            if price and float(price) > 0:
                result["ld_json_has_price"] = True
                result["ld_json_price"] = float(price)
                result["ld_json_currency"] = data.get(
                    "priceCurrency",
                    data.get("offers", {}).get("priceCurrency", "?"),
                )
                result["ld_json_model"] = data.get("name", data.get("model", "?"))
                if "json_script" not in result["strategies"]:
                    result["strategies"].append("json_script")

        # ── Strategy 2: data-* attributes with JSON ─────────────────
        # Check for elements with data attributes containing price JSON
        for sel in [
            "[data-card-filter-info]",
            "[data-price]",
            "[data-vehicle]",
            "[data-model]",
            "[data-tracking]",
            "[data-product]",
            "[data-car]",
            "[data-testid]",
        ]:
            elems = page.css(sel)
            if elems:
                for el in elems[:3]:
                    for attr_name, attr_val in (el.attrib or {}).items():
                        if not attr_name.startswith("data-"):
                            continue
                        if len(attr_val) < 10:
                            continue
                        try:
                            j = json.loads(attr_val)
                            if isinstance(j, dict):
                                # Check if any value looks like a price
                                for k, v in j.items():
                                    if (
                                        "price" in k.lower()
                                        and isinstance(v, (int, float))
                                        and v > 1000
                                    ):
                                        result["data_attr_price_found"] = True
                                        if "attr_json" not in result["strategies"]:
                                            result["strategies"].append("attr_json")
                                        break
                        except (json.JSONDecodeError, TypeError):
                            pass

        # ── Strategy 3: raw HTML price search ────────────────────────
        # Find price patterns in raw HTML body
        # European number formats: 459 900, 459.900, 459,900
        price_patterns = re.findall(
            r'"price"\s*:\s*(\d+(?:\.\d+)?)', body_str
        )
        if price_patterns:
            result["price_in_raw_html"] = True
            result["raw_price_values"] = [
                float(p) for p in price_patterns[:5]
                if float(p) > 1000
            ]

        # ── Strategy 4: visible price elements ──────────────────────
        price_selectors = [
            "[class*=price]",
            "[class*=Price]",
            "[data-testid*=price]",
        ]
        for sel in price_selectors:
            elems = page.css(sel)
            for el in elems[:10]:
                txt = el.text if hasattr(el, "text") else ""
                if txt and re.search(r'\d[\d\s.,]+\d', txt):
                    result["visible_price_found"] = True
                    if "css" not in result["strategies"]:
                        result["strategies"].append("css")
                    break

        if not result["strategies"]:
            result["strategies"].append("unknown")

    except Exception as e:
        result["error"] = str(e)[:200]

    return result


def main():
    brands_to_probe = list(BRAND_PROBES.items())

    # Allow filtering by brand name
    if len(sys.argv) > 1:
        filter_brands = [b.upper() for b in sys.argv[1:]]
        brands_to_probe = [
            (b, u) for b, u in brands_to_probe if b in filter_brands
        ]

    results = []
    for i, (brand, url) in enumerate(brands_to_probe, 1):
        print(f"\n[{i}/{len(brands_to_probe)}] Probing {brand}: {url}")
        t0 = time.time()
        r = probe_one(brand, url)
        elapsed = time.time() - t0
        r["elapsed_s"] = round(elapsed, 1)

        status = "✅" if r["strategies"] != ["unknown"] else "❌"
        strat_str = ", ".join(r["strategies"])
        price_str = ""
        if r["ld_json_has_price"]:
            price_str = f' | ld+json price={r["ld_json_price"]} {r["ld_json_currency"]}'
        elif r["price_in_raw_html"]:
            price_str = f' | raw prices={r["raw_price_values"][:3]}'
        elif r["data_attr_price_found"]:
            price_str = " | data-attr JSON price found"

        print(f"  {status} Strategy: {strat_str}{price_str} ({elapsed:.1f}s)")
        if r["error"]:
            print(f"  ⚠ Error: {r['error']}")
        results.append(r)

    # Save results
    out_dir = os.path.join(
        os.path.dirname(__file__), "probe_results"
    )
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "brand_strategy_report.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Summary table
    print("\n" + "=" * 70)
    print(f"{'Brand':15s} {'Strategy':15s} {'Price':>12s} {'Currency':>8s} {'Time':>6s}")
    print("-" * 70)
    for r in results:
        strat = ",".join(r["strategies"])
        price = ""
        if r.get("ld_json_price"):
            price = str(r["ld_json_price"])
        elif r.get("raw_price_values"):
            price = str(r["raw_price_values"][0])
        curr = r.get("ld_json_currency") or ""
        print(f'{r["brand"]:15s} {strat:15s} {price:>12s} {curr:>8s} {r.get("elapsed_s",0):>5.1f}s')


if __name__ == "__main__":
    main()
