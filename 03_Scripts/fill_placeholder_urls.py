#!/usr/bin/env python3
"""Fill todo.invalid placeholder URLs using brand patterns from completed countries."""

import yaml
import os
import re
from collections import defaultdict
from pathlib import Path

BASE = Path('07_ScrapingToolkit/source_drafts/suv_only_country_model_top30')

# Countries that have real URLs → use as templates
TEMPLATE_COUNTRIES = ['se', 'no', 'hu', 'hr', 'at', 'cz']
# Countries to fix
TARGET_COUNTRIES = ['de', 'fr', 'it', 'pl', 'fi', 'dk']

# TLD + path language mappings
TLD_MAP = {
    'se': 'se', 'no': 'no', 'dk': 'dk', 'fi': 'fi',
    'de': 'de', 'at': 'at', 'ch': 'ch',
    'fr': 'fr', 'it': 'it', 'pl': 'pl',
    'hu': 'hu', 'hr': 'hr', 'cz': 'cz',
}

# Known URL adaptations per brand (country → URL prefix/pattern)
# Many brands use .com with /{country_code} path
BRAND_OVERRIDES = {
    'KIA': lambda cc, url: re.sub(r'www\.kia\.com/\w+/', f'www.kia.com/{cc}/', url),
    'HYUNDAI': lambda cc, url: re.sub(r'hyundai\.\w+/', f'hyundai.{TLD_MAP.get(cc,cc)}/', url).replace('hyundai.hu/', f'hyundai.{TLD_MAP.get(cc,cc)}/'),
    'TOYOTA': lambda cc, url: re.sub(r'toyota\.\w+/', f'toyota.{TLD_MAP.get(cc,cc)}/', url),
    'BMW': lambda cc, url: re.sub(r'bmw\.\w+/', f'bmw.{TLD_MAP.get(cc,cc)}/', url),
    'AUDI': lambda cc, url: re.sub(r'audi\.\w+/', f'audi.{TLD_MAP.get(cc,cc)}/', url),
    'MERCEDES': lambda cc, url: re.sub(r'mercedes-benz\.\w+/', f'mercedes-benz.{TLD_MAP.get(cc,cc)}/', url),
    'VOLVO': lambda cc, url: re.sub(r'volvocars\.com/\w+/', f'volvocars.com/{cc}/', url),
    'VOLKSWAGEN': lambda cc, url: re.sub(r'volkswagen\.\w+/', f'volkswagen.{TLD_MAP.get(cc,cc)}/', url),
    'SKODA': lambda cc, url: re.sub(r'skoda(-auto)?\.\w+/', f'skoda.{TLD_MAP.get(cc,cc)}/', url).replace('skoda-auto.', 'skoda.'),
    'CUPRA': lambda cc, url: re.sub(r'cupraofficial\.\w+/', f'cupraofficial.{TLD_MAP.get(cc,cc)}/', url),
    'PEUGEOT': lambda cc, url: re.sub(r'peugeot\.\w+/', f'peugeot.{TLD_MAP.get(cc,cc)}/', url),
    'FORD': lambda cc, url: re.sub(r'ford\.\w+/', f'ford.{TLD_MAP.get(cc,cc)}/', url),
    'NISSAN': lambda cc, url: re.sub(r'nissan\.\w+/', f'nissan.{TLD_MAP.get(cc,cc)}/', url),
    'DACIA': lambda cc, url: re.sub(r'dacia\.\w+/', f'dacia.{TLD_MAP.get(cc,cc)}/', url),
    'RENAULT': lambda cc, url: re.sub(r'renault\.\w+/', f'renault.{TLD_MAP.get(cc,cc)}/', url),
    'CITROEN': lambda cc, url: re.sub(r'citroen\.\w+/', f'citroen.{TLD_MAP.get(cc,cc)}/', url),
    'OPEL': lambda cc, url: re.sub(r'opel\.\w+/', f'opel.{TLD_MAP.get(cc,cc)}/', url),
    'SEAT': lambda cc, url: re.sub(r'seat\.\w+/', f'seat.{TLD_MAP.get(cc,cc)}/', url),
    'SUZUKI': lambda cc, url: re.sub(r'suzuki\.\w+/', f'suzuki.{TLD_MAP.get(cc,cc)}/', url),
    'MG': lambda cc, url: re.sub(r'mgmotor\.\w+/', f'mgmotor.{TLD_MAP.get(cc,cc)}/', url),
    'MAZDA': lambda cc, url: re.sub(r'mazda\.\w+/', f'mazda.{TLD_MAP.get(cc,cc)}/', url),
    'POLESTAR': lambda cc, url: re.sub(r'polestar\.com/\w+/', f'polestar.com/{cc}/', url),
    'TESLA': lambda cc, url: re.sub(r'tesla\.com/\w+_\w+/', f'tesla.com/{cc.lower()}_{TLD_MAP.get(cc,cc).upper()}/', url),
    'BYD': lambda cc, url: re.sub(r'byd\.com/\w+/', f'byd.com/{cc}/', url),
    'XPENG': lambda cc, url: re.sub(r'xpeng\.com/\w+/', f'xpeng.com/{cc}/', url),
}


def adapt_url(url: str, target_cc: str) -> str:
    """Adapt a known URL to a target country."""
    for brand, func in BRAND_OVERRIDES.items():
        brand_lower = brand.lower()
        if brand_lower in url.lower():
            try:
                return func(target_cc, url)
            except Exception:
                pass
    # Generic: replace TLD
    for src_cc in TEMPLATE_COUNTRIES:
        if f'.{src_cc}/' in url:
            return url.replace(f'.{src_cc}/', f'.{TLD_MAP.get(target_cc, target_cc)}/')
    return url


def main():
    # Build brand→model→url map from template countries
    template_map: dict[str, dict[str, str]] = defaultdict(dict)
    for cc in TEMPLATE_COUNTRIES:
        d = BASE / cc
        if not d.is_dir():
            continue
        for f in sorted(d.iterdir()):
            if not f.suffix == '.yaml':
                continue
            cfg = yaml.safe_load(f.read_text()) or {}
            url = cfg.get('source_url', '') or cfg.get('profile', {}).get('url', '')
            if not url or 'todo.invalid' in url:
                continue
            brand = str(cfg.get('brand', '')).strip().upper()
            model = str(cfg.get('profile', {}).get('fixed_model', '')).strip()
            if not model:
                model = f.stem.replace(f'_{cc}_draft_scrapling', '').replace('_', ' ')
            if brand and model and url:
                template_map[brand][model.lower()] = url

    updated = 0
    skipped = 0

    for cc in TARGET_COUNTRIES:
        d = BASE / cc
        if not d.is_dir():
            continue
        for f in sorted(d.iterdir()):
            if not f.suffix == '.yaml':
                continue
            cfg = yaml.safe_load(f.read_text()) or {}
            current_url = cfg.get('source_url', '') or cfg.get('profile', {}).get('url', '')
            if 'todo.invalid' not in current_url and current_url:
                skipped += 1
                continue

            brand = str(cfg.get('brand', '')).strip().upper()
            model = str(cfg.get('profile', {}).get('fixed_model', '')).strip()
            if not model:
                model = f.stem.replace(f'_{cc}_draft_scrapling', '').replace('_', ' ')

            # Find template URL from same brand+model
            new_url = None
            brand_models = template_map.get(brand, {})
            model_lower = model.lower()

            # Exact match
            if model_lower in brand_models:
                new_url = adapt_url(brand_models[model_lower], cc)
            else:
                # Fuzzy match: check if any key contains model
                for tmpl_model, tmpl_url in brand_models.items():
                    if model_lower in tmpl_model or tmpl_model in model_lower:
                        new_url = adapt_url(tmpl_url, cc)
                        break
                # Fallback: use any URL from same brand
                if not new_url and brand_models:
                    any_url = next(iter(brand_models.values()))
                    new_url = adapt_url(any_url, cc)

            if new_url:
                # Update the YAML
                if 'source_url' in cfg:
                    cfg['source_url'] = new_url
                elif 'profile' in cfg and 'url' in cfg['profile']:
                    cfg['profile']['url'] = new_url
                else:
                    cfg['source_url'] = new_url

                # Add notes about URL source
                cfg['notes'] = (cfg.get('notes', '') + f' [URL filled from {brand} template]').strip()

                f.write_text(yaml.dump(cfg, default_flow_style=False, allow_unicode=True, sort_keys=False))
                updated += 1
                print(f"✅ {cc.upper()} {brand:20s} {model:25s} → {new_url[:80]}")
            else:
                print(f"⚠️  {cc.upper()} {brand:20s} {model:25s} — no template found")
                skipped += 1

    print(f"\nDone: {updated} updated, {skipped} skipped")


if __name__ == '__main__':
    main()
