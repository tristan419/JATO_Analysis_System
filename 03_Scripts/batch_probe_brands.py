#!/usr/bin/env python3
"""Batch-probe brand URLs to discover CSS selectors — one page per brand.

Usage:
    python batch_probe_brands.py [--brands BRAND1,BRAND2,...] [--timeout 45]

Saves results to 03_Scripts/probe_results/{brand}.json
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import time
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "07_ScrapingToolkit"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

DRAFT_BASE = os.path.join(
    os.path.dirname(__file__),
    "..",
    "07_ScrapingToolkit",
    "source_drafts",
    "suv_only_country_model_top30",
)
RESULT_DIR = os.path.join(os.path.dirname(__file__), "probe_results")
BATCH_1_2 = ["se", "hr", "hu", "no", "at", "cz", "ch"]

PRICE_RE = re.compile(
    r"(?:€|EUR|SEK|NOK|DKK|CHF|CZK|HUF|PLN|RON|GBP|kr|Kč|Ft|zł|lei|£)?"
    r"\s*[\d]{2,}(?:[\s.,]\d{3})*(?:[.,]\d{1,2})?"
    r"\s*(?:€|EUR|SEK|NOK|DKK|CHF|CZK|HUF|PLN|RON|GBP|kr|Kč|Ft|zł|lei|£)?",
    re.IGNORECASE,
)


def collect_brand_urls() -> dict[str, dict]:
    """Collect one representative URL per brand from Batch 1+2 drafts."""
    brands: dict[str, dict] = {}
    for cc in BATCH_1_2:
        d = os.path.join(DRAFT_BASE, cc)
        if not os.path.isdir(d):
            continue
        for fn in sorted(os.listdir(d)):
            if not fn.endswith(".yaml"):
                continue
            with open(os.path.join(d, fn)) as f:
                cfg = yaml.safe_load(f)
            brand = cfg.get("brand", "")
            url = cfg.get("source_url", "")
            if not brand or not url or "todo.invalid" in url:
                continue
            css = cfg.get("profile", {}).get("css", {})
            has_todo = "TODO_SELECTOR" in str(css)
            if brand not in brands and has_todo:
                brands[brand] = {
                    "url": url,
                    "country": cc,
                    "file": fn,
                    "model": cfg.get("profile", {}).get("fixed_model", ""),
                    "currency": cfg.get("profile", {}).get("default_currency", "EUR"),
                }
    return brands


def _css_path_simple(el) -> str:
    """Build a CSS selector path for an element."""
    parts = []
    node = el
    for _ in range(6):
        if node is None:
            break
        tag = getattr(node, "tag", None) or ""
        if not tag or tag in ("html", "body", "[document]"):
            break
        attrs = getattr(node, "attrib", {}) or {}
        tid = attrs.get("data-testid", "")
        eid = attrs.get("id", "")
        cls = attrs.get("class", "")
        if tid:
            parts.append(f'[data-testid="{tid}"]')
        elif eid:
            parts.append(f"#{eid}")
        elif cls:
            first_cls = cls.strip().split()[0]
            parts.append(f".{first_cls}")
        else:
            parts.append(tag)
        node = getattr(node, "parent", None)
    parts.reverse()
    return " > ".join(parts)


def _get_text(el) -> str:
    try:
        t = el.text() if hasattr(el, "text") else ""
        return (t or "").strip()
    except Exception:
        return ""


def probe_page(page, brand: str, currency: str) -> dict:
    """Analyse a fetched page for price elements."""
    result = {
        "brand": brand,
        "price_elements": [],
        "data_attrs": [],
        "containers": {},
        "total_elements": 0,
    }

    all_els = page.css("*")
    result["total_elements"] = len(all_els)

    # Check for data-attribute JSON with price
    for el in all_els:
        attrs = getattr(el, "attrib", {}) or {}
        for k, v in attrs.items():
            if not k.startswith("data-") or len(v) < 10:
                continue
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    price_keys = [
                        pk
                        for pk in parsed
                        if "price" in pk.lower()
                        or "preis" in pk.lower()
                        or "pris" in pk.lower()
                        or "prix" in pk.lower()
                        or "cena" in pk.lower()
                    ]
                    if price_keys:
                        result["data_attrs"].append(
                            {
                                "attr": k,
                                "keys": list(parsed.keys())[:8],
                                "price_keys": price_keys,
                                "price_vals": {pk: str(parsed[pk])[:50] for pk in price_keys},
                                "class": (attrs.get("class", "")).split()[0] if attrs.get("class") else "",
                                "tag": getattr(el, "tag", ""),
                            }
                        )
            except (json.JSONDecodeError, TypeError):
                pass

    # Find price text elements
    for el in all_els:
        tag = getattr(el, "tag", "?")
        if tag in ("script", "style", "meta", "link", "head", "noscript"):
            continue
        text = _get_text(el)
        if not text or len(text) > 200 or not re.search(r"\d{3,}", text):
            continue
        m = PRICE_RE.search(text)
        if not m:
            continue
        attrs = getattr(el, "attrib", {}) or {}
        result["price_elements"].append(
            {
                "text": text[:120],
                "tag": tag,
                "class": (attrs.get("class", "")).split()[0] if attrs.get("class") else "",
                "data_testid": attrs.get("data-testid", ""),
                "path": _css_path_simple(el),
                "price_match": m.group().strip(),
            }
        )

    # Probe known container patterns
    patterns = [
        '[data-testid*="card"]',
        '[data-testid*="price"]',
        '[data-testid*="model"]',
        '[data-testid*="variant"]',
        '[data-testid*="trim"]',
        '[data-testid*="vehicle"]',
        '[data-testid*="product"]',
        '[data-testid*="offer"]',
        "article",
        '[role="article"]',
        '[class*="card"]',
        '[class*="Card"]',
        '[class*="vehicle"]',
        '[class*="Vehicle"]',
        '[class*="model"]',
        '[class*="Model"]',
        '[class*="variant"]',
        '[class*="Variant"]',
        '[class*="trim"]',
        '[class*="Trim"]',
        '[class*="product"]',
        '[class*="Product"]',
        '[class*="price"]',
        '[class*="Price"]',
        '[class*="offer"]',
        '[class*="Offer"]',
        '[class*="version"]',
        '[class*="Version"]',
        '[class*="motorization"]',
        '[class*="Motorization"]',
        '[class*="engine"]',
        '[class*="Engine"]',
        '[class*="grade"]',
        '[class*="Grade"]',
    ]
    for sel in patterns:
        try:
            els = page.css(sel)
            if els and 1 <= len(els) <= 50:
                price_count = 0
                samples = []
                for cel in els[:5]:
                    try:
                        full = (cel.text(deep=True) or "")[:300]
                    except Exception:
                        full = ""
                    if PRICE_RE.search(full):
                        price_count += 1
                        samples.append(full[:150])
                if price_count > 0:
                    result["containers"][sel] = {
                        "total": len(els),
                        "with_price": price_count,
                        "samples": samples[:2],
                    }
        except Exception:
            pass

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--brands", default="", help="Comma-separated brand filter")
    parser.add_argument("--timeout", type=int, default=45, help="Timeout per page in seconds")
    args = parser.parse_args()

    os.makedirs(RESULT_DIR, exist_ok=True)
    brand_filter = set(b.upper() for b in args.brands.split(",")) if args.brands else None

    brand_urls = collect_brand_urls()
    if brand_filter:
        brand_urls = {b: v for b, v in brand_urls.items() if b in brand_filter}

    # Skip VOLVO (already have selectors)
    brand_urls.pop("VOLVO", None)

    from scrapling.fetchers import StealthyFetcher

    results_summary = []
    for brand, info in sorted(brand_urls.items()):
        url = info["url"]
        out_path = os.path.join(RESULT_DIR, f"{brand.lower()}.json")

        log.info("═" * 60)
        log.info("Probing %s (%s) — %s", brand, info["country"].upper(), url)
        log.info("═" * 60)

        t0 = time.time()
        try:
            page = StealthyFetcher.fetch(
                url,
                headless=True,
                network_idle=True,
            )
            elapsed = time.time() - t0
            log.info("Fetched in %.1fs, analysing DOM...", elapsed)

            if page is None:
                result = {"brand": brand, "error": "fetch returned None", "url": url}
            else:
                result = probe_page(page, brand, info["currency"])
                result["url"] = url
                result["country"] = info["country"]
                result["model"] = info["model"]
                result["fetch_time"] = round(elapsed, 1)
        except Exception as e:
            elapsed = time.time() - t0
            result = {"brand": brand, "error": str(e), "url": url, "fetch_time": round(elapsed, 1)}
            log.error("Failed for %s: %s", brand, e)

        with open(out_path, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False, default=str)

        # Summary
        n_prices = len(result.get("price_elements", []))
        n_data_attrs = len(result.get("data_attrs", []))
        n_containers = len(result.get("containers", {}))
        status = "✅" if n_prices > 0 or n_data_attrs > 0 else "❌"
        summary = f"{status} {brand:15s} prices={n_prices} data_attrs={n_data_attrs} containers={n_containers}"
        log.info(summary)
        results_summary.append(summary)

    log.info("\n" + "═" * 60)
    log.info("SUMMARY")
    log.info("═" * 60)
    for s in results_summary:
        log.info(s)


if __name__ == "__main__":
    main()
