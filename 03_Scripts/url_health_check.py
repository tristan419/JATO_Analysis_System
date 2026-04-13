#!/usr/bin/env python3
"""Quick URL health check for all draft YAML sources.

Checks HTTP status codes without full extraction. Much faster than
batch_dryrun.py since it only does HEAD/GET requests.

Usage:
    python3 url_health_check.py [country|batch]
    python3 url_health_check.py hr
    python3 url_health_check.py all
"""
import sys
import time
import yaml
import glob
from pathlib import Path
from collections import Counter
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

TOOLKIT = Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
DRAFTS_ROOT = TOOLKIT / "source_drafts" / "suv_only_country_model_top30"

BATCH_COUNTRIES = {
    "1": ["se", "hr"],
    "2": ["hu", "no", "at", "cz", "ch"],
    "all": ["se", "hr", "hu", "no", "at", "cz", "ch"],
}


def check_url(url: str, timeout: int = 15) -> tuple[int, str]:
    """Return (status_code, reason)."""
    try:
        req = Request(url, method="HEAD", headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"
        })
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, "OK"
    except HTTPError as e:
        return e.code, str(e.reason)
    except URLError as e:
        return 0, str(e.reason)[:50]
    except Exception as e:
        return -1, str(e)[:50]


def main():
    batch = sys.argv[1] if len(sys.argv) > 1 else "all"
    countries = BATCH_COUNTRIES.get(batch, batch.split(","))

    results = []
    for cc in countries:
        cc_dir = DRAFTS_ROOT / cc
        if not cc_dir.is_dir():
            print(f"Warning: {cc_dir} not found")
            continue
        for f in sorted(cc_dir.glob("*.yaml")):
            with open(f) as fh:
                doc = yaml.safe_load(fh)
            url = doc.get("profile", {}).get("url", "")
            brand = doc.get("brand", "?")
            code = doc.get("source_code", f.stem)
            if not url:
                results.append((cc, brand, code, -1, "no_url"))
                continue
            t0 = time.time()
            status, reason = check_url(url)
            elapsed = time.time() - t0
            results.append((cc, brand, code, status, reason))
            icon = "✓" if 200 <= status < 400 else "✗"
            print(f"  {icon} [{status:3d}] {cc} {brand:12s} {url} ({elapsed:.1f}s)")

    # Summary
    print(f"\n{'='*60}")
    by_country: dict[str, Counter] = {}
    for cc, brand, code, status, reason in results:
        by_country.setdefault(cc, Counter())
        if 200 <= status < 400:
            by_country[cc]["ok"] += 1
        elif status == 404:
            by_country[cc]["404"] += 1
        elif status == 403:
            by_country[cc]["403"] += 1
        else:
            by_country[cc]["other"] += 1

    print(f"{'Country':>8s}  {'OK':>4s}  {'404':>4s}  {'403':>4s}  {'Other':>5s}  {'Total':>5s}")
    for cc in countries:
        c = by_country.get(cc, Counter())
        total = sum(c.values())
        print(f"{cc:>8s}  {c['ok']:4d}  {c['404']:4d}  {c['403']:4d}  {c['other']:5d}  {total:5d}")


if __name__ == "__main__":
    main()
