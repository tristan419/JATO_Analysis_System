#!/usr/bin/env python3
"""Inspect ld+json data for a given URL."""
import json
import re
import sys

sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"))

from scrapling import StealthyFetcher

url = sys.argv[1] if len(sys.argv) > 1 else "https://www.skoda.se/modeller/kodiaq"
print(f"Fetching: {url}")
f = StealthyFetcher()
page = f.fetch(url)
body = page.body.decode("utf-8", errors="replace")

pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
matches = re.findall(pattern, body, re.DOTALL)
print(f"Found {len(matches)} ld+json blocks\n")

for i, m in enumerate(matches):
    try:
        data = json.loads(m)
        t = data.get("@type", "?")
        print(f"[{i}] @type={t}")
        if "offers" in data:
            print(f"    offers: {json.dumps(data['offers'], ensure_ascii=False)[:300]}")
        if "name" in data:
            print(f"    name: {data['name']}")
        if "model" in data:
            print(f"    model: {data['model']}")
        if "description" in data:
            print(f"    description: {str(data['description'])[:150]}")
        if not any(k in data for k in ("offers", "name", "model")):
            print(f"    keys: {list(data.keys())[:10]}")
        print()
    except Exception as e:
        print(f"[{i}] parse error: {e}\n")
