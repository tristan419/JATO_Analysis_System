#!/usr/bin/env python3
"""Probe brand website DOM to discover CSS selectors for MSRP extraction.

Usage:
    python probe_brand_selectors.py <url> [--brand BRAND] [--country CC]

Fetches the page with Patchright stealth and analyses the DOM
to find price-containing elements and their likely CSS paths.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import os

# Allow importing from ScrapingToolkit
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "07_ScrapingToolkit"),
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Price patterns for various currencies
PRICE_RE = re.compile(
    r"(?:€|EUR|SEK|NOK|DKK|CHF|CZK|HUF|PLN|RON|GBP|kr|Kč|Ft|zł|lei|£)?"
    r"\s*[\d]+(?:[\s.,]\d{3})*(?:[.,]\d{1,2})?"
    r"\s*(?:€|EUR|SEK|NOK|DKK|CHF|CZK|HUF|PLN|RON|GBP|kr|Kč|Ft|zł|lei|£)?",
    re.IGNORECASE,
)

# Typical non-MSRP price keywords to flag
LEASE_KEYWORDS = re.compile(
    r"mois|month|monat|månad|måned|lease|loa|lld|loyer|miete|hyra|leie|"
    r"från|from|ab |da |depuis|od |tól",
    re.IGNORECASE,
)


def _css_path(el: object) -> str:
    """Build a human-readable CSS path for an element."""
    parts = []
    node = el
    for _ in range(8):  # max depth
        if node is None:
            break
        tag = getattr(node, "tag", None) or ""
        if not tag or tag in ("html", "body", "[document]"):
            break
        attrs = getattr(node, "attrib", {}) or {}
        # Prefer data-testid, id, class for identification
        tid = attrs.get("data-testid", "")
        eid = attrs.get("id", "")
        cls = attrs.get("class", "")
        if tid:
            parts.append(f'{tag}[data-testid="{tid}"]')
        elif eid:
            parts.append(f"{tag}#{eid}")
        elif cls:
            first_cls = cls.strip().split()[0]
            parts.append(f"{tag}.{first_cls}")
        else:
            parts.append(tag)
        node = getattr(node, "parent", None)
    parts.reverse()
    return " > ".join(parts)


def _get_text(el: object) -> str:
    """Get text content of an element."""
    try:
        return (el.text() or "").strip()
    except Exception:
        try:
            return (el.get_text() or "").strip()
        except Exception:
            return ""


def _get_all_text(el: object) -> str:
    """Get all text including children."""
    try:
        return (el.text(deep=True) or "").strip()
    except Exception:
        return _get_text(el)


def _find_data_attrs(el: object) -> dict:
    """Find data- attributes that might contain JSON."""
    attrs = getattr(el, "attrib", {}) or {}
    data_attrs = {}
    for k, v in attrs.items():
        if k.startswith("data-") and len(v) > 10:
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    data_attrs[k] = parsed
            except (json.JSONDecodeError, TypeError):
                pass
    return data_attrs


def analyse_page(page: object, brand: str = "", country: str = "") -> dict:
    """Analyse the DOM to discover price elements and their containers."""
    results = {
        "brand": brand,
        "country": country,
        "price_elements": [],
        "data_attr_elements": [],
        "suggested_selectors": {},
        "page_title": "",
    }

    # Get page title
    try:
        title_els = page.css("title")
        if title_els:
            results["page_title"] = _get_text(title_els[0])
    except Exception:
        pass

    # ── Strategy 1: Find elements with data attributes containing JSON with price ──
    log.info("Checking for data-attribute JSON strategy...")
    all_els = page.css("*")
    data_attr_hits = []
    for el in all_els:
        da = _find_data_attrs(el)
        for attr_name, attr_val in da.items():
            # Check if JSON contains price-like keys
            price_keys = [
                k for k in attr_val
                if any(w in k.lower() for w in ("price", "preis", "prix", "pris", "cena"))
            ]
            if price_keys:
                tag = getattr(el, "tag", "?")
                attrs = getattr(el, "attrib", {}) or {}
                cls = attrs.get("class", "")
                data_attr_hits.append({
                    "tag": tag,
                    "class": cls.strip().split()[0] if cls else "",
                    "data_attr": attr_name,
                    "json_keys": list(attr_val.keys())[:10],
                    "price_keys": price_keys,
                    "price_values": {k: attr_val[k] for k in price_keys},
                    "css_path": _css_path(el),
                })
    results["data_attr_elements"] = data_attr_hits[:15]
    if data_attr_hits:
        log.info("Found %d elements with price data in attributes", len(data_attr_hits))

    # ── Strategy 2: Find elements with visible price text ──
    log.info("Scanning for visible price text elements...")

    # Common automotive card/container selectors to check
    container_candidates = [
        '[data-testid*="card"]',
        '[data-testid*="model"]',
        '[data-testid*="vehicle"]',
        '[data-testid*="variant"]',
        '[data-testid*="trim"]',
        '[data-testid*="price"]',
        '[data-testid*="selection"]',
        '[data-testid*="product"]',
        '[data-testid*="offer"]',
        'article',
        '[role="article"]',
        '.model-card',
        '.vehicle-card',
        '.trim-card',
        '.product-card',
        '.offer-card',
        '.card',
        '.tile',
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
        '[class*="offer"]',
        '[class*="Offer"]',
        'li[class]',
    ]

    # Find ALL elements containing price-like text
    price_hits = []
    for el in all_els:
        text = _get_text(el)
        if not text:
            continue
        # Quick check: contains digits with potential price format
        if not re.search(r"\d{2,}", text):
            continue
        # Check if element text (not too long) looks like a price
        if len(text) > 200:
            continue
        m = PRICE_RE.search(text)
        if not m:
            continue
        # Extract more info
        tag = getattr(el, "tag", "?")
        if tag in ("script", "style", "meta", "link", "head"):
            continue
        attrs = getattr(el, "attrib", {}) or {}
        is_lease = bool(LEASE_KEYWORDS.search(text))
        price_hits.append({
            "text": text[:150],
            "tag": tag,
            "class": (attrs.get("class", "")).strip().split()[0] if attrs.get("class") else "",
            "data_testid": attrs.get("data-testid", ""),
            "css_path": _css_path(el),
            "is_lease_like": is_lease,
            "price_match": m.group().strip(),
        })

    results["price_elements"] = price_hits[:40]
    log.info("Found %d elements with price-like text", len(price_hits))

    # ── Strategy 3: Check common container selectors ──
    log.info("Probing common container selectors...")
    container_hits = {}
    for sel in container_candidates:
        try:
            els = page.css(sel)
            if els and 1 <= len(els) <= 50:
                # Count how many contain price text
                price_count = 0
                sample_texts = []
                for cel in els[:5]:
                    full_text = _get_all_text(cel)
                    if PRICE_RE.search(full_text):
                        price_count += 1
                        sample_texts.append(full_text[:200])
                if price_count > 0:
                    container_hits[sel] = {
                        "total": len(els),
                        "with_price": price_count,
                        "samples": sample_texts[:3],
                    }
        except Exception:
            pass
    results["container_probe"] = container_hits

    # ── Suggest best selectors ──
    if data_attr_hits:
        first = data_attr_hits[0]
        cls = first["class"]
        container_sel = f'.{cls}' if cls else f'{first["tag"]}[{first["data_attr"]}]'
        results["suggested_selectors"] = {
            "strategy": "attr_json",
            "vehicle_container": container_sel,
            "data_attr": first["data_attr"],
            "json_keys": first["json_keys"],
        }
    elif container_hits:
        # Pick the selector with the most price-containing elements
        best = max(container_hits.items(), key=lambda x: x[1]["with_price"])
        results["suggested_selectors"] = {
            "strategy": "css",
            "vehicle_container": best[0],
            "count": best[1]["total"],
            "with_price": best[1]["with_price"],
        }

    return results


def main():
    parser = argparse.ArgumentParser(description="Probe brand website for CSS selectors")
    parser.add_argument("url", help="URL to probe")
    parser.add_argument("--brand", default="", help="Brand name")
    parser.add_argument("--country", default="", help="Country code")
    parser.add_argument("--output", default="", help="Output JSON path")
    args = parser.parse_args()

    log.info("Fetching %s ...", args.url)
    from scrapling.fetchers import StealthyFetcher

    page = StealthyFetcher.fetch(
        args.url,
        headless=True,
        network_idle=True,
    )
    if page is None:
        log.error("Failed to fetch page")
        sys.exit(1)

    log.info("Analysing DOM...")
    result = analyse_page(page, brand=args.brand, country=args.country)

    output = json.dumps(result, indent=2, ensure_ascii=False, default=str)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        log.info("Results written to %s", args.output)
    else:
        print(output)


if __name__ == "__main__":
    main()
