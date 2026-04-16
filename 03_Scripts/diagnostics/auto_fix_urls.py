#!/usr/bin/env python3
"""Automatically fix 404 draft source URLs by trying common URL patterns.

For each source with a 404 URL, tries alternative domain and path patterns.
Reports fixable URLs and optionally applies fixes.

Usage:
    python3 auto_fix_urls.py [--apply]
"""
import sys
import yaml
import glob
import time
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

TOOLKIT = Path(__file__).resolve().parents[2] / "07_ScrapingToolkit"
DRAFTS_ROOT = TOOLKIT / "source_drafts" / "suv_only_country_model_top30"

# Known domain corrections
DOMAIN_FIXES = {
    "www.skoda.no": "www.skoda-auto.no",
    "www.skoda.hr": "www.skoda-auto.hr",
    # skoda-auto.cz already correct
}

# Path patterns to try for each brand
# {cc} = country code, {model} = model slug, {lang} = local language
BRAND_URL_PATTERNS = {
    "SKODA": [
        "https://www.skoda-auto.{cc}/modeller/{model}",
        "https://www.skoda-auto.{cc}/modelle/{model}",
        "https://www.skoda-auto.{cc}/modellek/{model}",
        "https://www.skoda-auto.{cc}/modeli/{model}",
        "https://www.skoda-auto.{cc}/modely/{model}",
        "https://www.skoda-auto.{cc}/models/{model}",
    ],
    "HYUNDAI": [
        "https://www.hyundai.com/{cc}/{lang}/modelle/{model}.html",
        "https://www.hyundai.com/{cc}/{lang}/models/{model}.html",
        "https://www.hyundai.com/{cc}/{lang}/modellek/{model}.html",
        "https://www.hyundai.{cc}/modely/{model}",
        "https://www.hyundai.{cc}/modellek/{model}",
    ],
    "DACIA": [
        "https://www.dacia.{cc}/modelle/{model}.html",
        "https://www.dacia.{cc}/modeles/{model}.html",
        "https://www.dacia.{cc}/modely/{model}.html",
        "https://www.dacia.{cc}/modellek/{model}.html",
        "https://www.dacia.{cc}/vozidla/{model}.html",
        "https://de.dacia.{cc}/modelle/{model}.html",
        "https://fr.dacia.{cc}/modeles/{model}.html",
    ],
    "RENAULT": [
        "https://www.renault.{cc}/vozidla/{model}.html",
        "https://www.renault.{cc}/modelle/{model}.html",
        "https://www.renault.{cc}/modeles/{model}.html",
        "https://www.renault.{cc}/modely/{model}.html",
        "https://www.renault.{cc}/modellek/{model}.html",
    ],
    "OPEL": [
        "https://www.opel.{cc}/modellek/{model}.html",
        "https://www.opel.{cc}/modelle/{model}.html",
        "https://www.opel.{cc}/modely/{model}.html",
        "https://www.opel.{cc}/modeli/{model}.html",
    ],
    "PEUGEOT": [
        "https://www.peugeot.{cc}/modelle/{model}.html",
        "https://www.peugeot.{cc}/modely/{model}.html",
        "https://www.peugeot.{cc}/modelljeink/{model}.html",
        "https://www.peugeot.{cc}/modeller/{model}.html",
    ],
    "CUPRA": [
        "https://www.cupraofficial.{cc}/modelle/{model}",
        "https://www.cupraofficial.{cc}/modely/{model}",
        "https://www.cupraofficial.{cc}/modeller/{model}",
        "https://www.cupraofficial.{cc}/modeli/{model}",
        "https://www.cupraofficial.{cc}/de/modelle/{model}",
    ],
    "NISSAN": [
        "https://www.nissan.{cc}/vozidla/nove-vozy/{model}.html",
        "https://www.nissan.{cc}/vozidla/nova-vozila/{model}.html",
        "https://www.nissan.{cc}/biler/nye-biler/{model}.html",
        "https://www.nissan.{cc}/jarmuvek/uj-jarmuvek/{model}.html",
    ],
    "VOLKSWAGEN": [
        "https://www.volkswagen.{cc}/modelle/{model}",
        "https://www.volkswagen.{cc}/modely/{model}",
        "https://www.volkswagen.{cc}/modellek/{model}",
        "https://www.volkswagen.{cc}/modeller/{model}",
        "https://www.volkswagen.{cc}/{lang}/modelle/{model}.html",
        "https://www.volkswagen.{cc}/{lang}/modely/{model}.html",
        "https://www.volkswagen.{cc}/{lang}/modellek/{model}.html",
        "https://www.volkswagen.{cc}/{lang}/modeller/{model}.html",
    ],
    "AUDI": [
        "https://www.audi.{cc}/de/modelle/{model}.html",
        "https://www.audi.{cc}/{lang}/web/{lang2}/models/{model}.html",
        "https://www.audi.{cc}/{cc}/web/{lang}/models/{model}.html",
    ],
    "TOYOTA": [
        "https://www.toyota.{cc}/new-cars/{model}",
        "https://www.toyota.{cc}/nova-auta/{model}",
        "https://www.toyota.{cc}/nye-biler/{model}",
        "https://www.toyota.{cc}/nybil/{model}",
        "https://www.toyota.{cc}/novi-automobili/{model}",
        "https://www.toyota.{cc}/neue-modelle/{model}",
    ],
}

CC_LANG = {
    "se": "sv", "no": "no", "hr": "hr", "hu": "hu",
    "at": "de", "cz": "cs", "ch": "de",
}


def check_url(url: str, timeout: int = 10) -> tuple[int, str]:
    try:
        req = Request(url, method="HEAD", headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36"
        })
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.url
    except HTTPError as e:
        return e.code, ""
    except Exception:
        return -1, ""


def extract_model_slug(url: str) -> str:
    """Extract model name from URL path."""
    parts = url.rstrip("/").split("/")
    slug = parts[-1]
    # Remove .html extension
    slug = slug.replace(".html", "")
    # Remove common prefixes
    for prefix in ("suv-", "new-", "novy-", "novi-"):
        if slug.startswith(prefix):
            slug = slug[len(prefix):]
    return slug


def main():
    apply_mode = "--apply" in sys.argv
    countries = ["se", "hr", "hu", "no", "at", "cz", "ch"]

    fixes_found = []
    for cc in countries:
        cc_dir = DRAFTS_ROOT / cc
        if not cc_dir.is_dir():
            continue
        for f in sorted(cc_dir.glob("*.yaml")):
            with open(f) as fh:
                doc = yaml.safe_load(fh)
            url = doc.get("profile", {}).get("url", "")
            brand = doc.get("brand", "?")
            code = doc.get("source_code", f.stem)

            if not url:
                continue

            # Check current URL
            status, _ = check_url(url)
            if 200 <= status < 400:
                continue  # URL is fine

            # Try domain fix first
            from urllib.parse import urlparse
            parsed = urlparse(url)
            if parsed.hostname in DOMAIN_FIXES:
                new_domain = DOMAIN_FIXES[parsed.hostname]
                fixed_url = url.replace(parsed.hostname, new_domain)
                s2, _ = check_url(fixed_url)
                if 200 <= s2 < 400:
                    fixes_found.append((cc, brand, code, url, fixed_url, "domain_fix"))
                    print(f"  ✓ DOMAIN FIX {code}: {url} → {fixed_url}")
                    if apply_mode:
                        _apply_url_fix(f, url, fixed_url)
                    continue

            # Try brand-specific patterns
            model_slug = extract_model_slug(url)
            patterns = BRAND_URL_PATTERNS.get(brand, [])
            lang = CC_LANG.get(cc, cc)
            found = False
            for pat in patterns:
                try:
                    candidate = pat.format(
                        cc=cc, model=model_slug, lang=lang, lang2=lang
                    )
                except KeyError:
                    continue
                if candidate == url:
                    continue
                s3, final_url = check_url(candidate)
                if 200 <= s3 < 400:
                    # Also check that we don't redirect to homepage
                    if final_url and final_url.rstrip("/").endswith(cc):
                        continue  # Redirected to homepage
                    fixes_found.append((cc, brand, code, url, candidate, "pattern_fix"))
                    print(f"  ✓ PATTERN FIX {code}: {url} → {candidate}")
                    if apply_mode:
                        _apply_url_fix(f, url, candidate)
                    found = True
                    break
                time.sleep(0.2)

            if not found:
                print(f"  ✗ NO FIX    {code} [{status}]")

    print(f"\n{'='*60}")
    print(f"Total fixes found: {len(fixes_found)}")
    if fixes_found and not apply_mode:
        print("Run with --apply to apply fixes")


def _apply_url_fix(yaml_path: Path, old_url: str, new_url: str):
    """Replace URL in YAML file."""
    content = yaml_path.read_text()
    content = content.replace(old_url, new_url)
    yaml_path.write_text(content)


if __name__ == "__main__":
    main()
