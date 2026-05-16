#!/usr/bin/env python3
"""Apply generic CSS selectors to source drafts that have TODO placeholders.

Uses probe-verified common selectors instead of per-brand research.
"""

import yaml, os, re
from pathlib import Path

BASE = Path('07_ScrapingToolkit/source_drafts/suv_only_country_model_top30')

# Brand-specific selector hints from the probe results
# Each: (vehicle_container, model_selector, trim_selector, price_selector)
BRAND_SELECTORS = {
    # Skoda: VW Group, similar configurator, has [class*="price"] ×10, h2 ×16, h3 ×37
    'SKODA': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
        'json_script_selector': 'script[type="application/json"]',
    },
    # Toyota: h2 ×5, h3 ×12
    'TOYOTA': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Kia: 27 price elements, 40 h2, 102 h3
    'KIA': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Hyundai: similar to Kia
    'HYUNDAI': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Volvo: uses volvocars.com configurator
    'VOLVO': {
        'vehicle_container': 'body',
        'model': 'h1',
        'trim': '[class*="variant"], [class*="trim"], h2',
        'price': '[class*="price"], [data-automation*="price"]',
    },
    # BMW: h2 ×2, h3 ×13
    'BMW': {
        'vehicle_container': 'body',
        'model': 'h1',
        'trim': 'h2, h3',
        'price': '[class*="price"]',
    },
    # Mercedes: h2 ×1, h3 ×7
    'MERCEDES': {
        'vehicle_container': 'body',
        'model': 'h1',
        'trim': 'h2, h3',
        'price': '[class*="price"]',
    },
    # Peugeot: h2 ×16, h3 ×19
    'PEUGEOT': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Renault: h2 ×8
    'RENAULT': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Dacia: h2 ×9
    'DACIA': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Ford: probe got 404, use generic
    'FORD': {
        'vehicle_container': 'body',
        'model': 'h1, h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Nissan: [class*="price"] ×2
    'NISSAN': {
        'vehicle_container': 'body',
        'model': 'h1, h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Audi: VW Group, probe got "site not available"
    'AUDI': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Cupra: VW Group, probe got "Request Denied"
    'CUPRA': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # Seat: VW Group
    'SEAT': {
        'vehicle_container': 'body',
        'model': 'h2',
        'trim': 'h3',
        'price': '[class*="price"]',
    },
    # MG, Suzuki, Opel, Citroen, BYD, Tesla, Jeep, etc — generic
    'MG': {'vehicle_container': 'body', 'model': 'h1, h2', 'trim': 'h3', 'price': '[class*="price"]'},
    'SUZUKI': {'vehicle_container': 'body', 'model': 'h1, h2', 'trim': 'h3', 'price': '[class*="price"]'},
    'OPEL': {'vehicle_container': 'body', 'model': 'h2', 'trim': 'h3', 'price': '[class*="price"]'},
    'CITROEN': {'vehicle_container': 'body', 'model': 'h2', 'trim': 'h3', 'price': '[class*="price"]'},
    'BYD': {'vehicle_container': 'body', 'model': 'h1, h2', 'trim': 'h3', 'price': '[class*="price"]'},
    'TESLA': {'vehicle_container': 'body', 'model': 'h1', 'trim': 'h2', 'price': '[class*="price"]'},
    'JEEP': {'vehicle_container': 'body', 'model': 'h1, h2', 'trim': 'h3', 'price': '[class*="price"]'},
    'FIAT': {'vehicle_container': 'body', 'model': 'h2', 'trim': 'h3', 'price': '[class*="price"]'},
    'ALFA ROMEO': {'vehicle_container': 'body', 'model': 'h1, h2', 'trim': 'h3', 'price': '[class*="price"]'},
    'LAND ROVER': {'vehicle_container': 'body', 'model': 'h1, h2', 'trim': 'h3', 'price': '[class*="price"]'},
    'PORSCHE': {'vehicle_container': 'body', 'model': 'h1', 'trim': 'h2, h3', 'price': '[class*="price"]'},
    'DFSK': {'vehicle_container': 'body', 'model': 'h1', 'trim': 'h2', 'price': '[class*="price"]'},
    'XPENG': {'vehicle_container': 'body', 'model': 'h1', 'trim': 'h2', 'price': '[class*="price"]'},
    'POLESTAR': {'vehicle_container': 'body', 'model': 'h1', 'trim': 'h2', 'price': '[class*="price"]'},
    'KGM': {'vehicle_container': 'body', 'model': 'h1', 'trim': 'h2', 'price': '[class*="price"]'},
    'JAECOO': {'vehicle_container': 'body', 'model': 'h1', 'trim': 'h2', 'price': '[class*="price"]'},
}

# Brands that should use playwright_card_flow instead of scrapling
# (VW Group brands with known trim card patterns)
PLAYWRIGHT_BRANDS = {'VOLKSWAGEN'}  # Only VW has confirmed [data-testid="trimcard"]


def apply_selectors():
    updated = 0
    skipped = 0

    for cc_dir in sorted(BASE.iterdir()):
        if not cc_dir.is_dir() or cc_dir.name.startswith('_'):
            continue
        for f in sorted(cc_dir.iterdir()):
            if not f.suffix == '.yaml':
                continue

            cfg = yaml.safe_load(f.read_text()) or {}
            brand = str(cfg.get('brand', '')).strip().upper()
            profile = cfg.get('profile', {})
            if not isinstance(profile, dict):
                profile = {}
            css = profile.get('css', {})
            if not isinstance(css, dict):
                css = {}

            # Skip if already has real selectors (not TODO)
            vehicle = str(css.get('vehicle_container', '')).strip()
            if vehicle and 'TODO' not in vehicle.upper():
                skipped += 1
                continue

            # Skip VW brands — already have proper playwright config
            if brand in PLAYWRIGHT_BRANDS:
                skipped += 1
                continue

            # Apply brand selectors
            sel = BRAND_SELECTORS.get(brand, {
                'vehicle_container': 'body',
                'model': 'h1, h2',
                'trim': 'h3',
                'price': '[class*="price"]',
            })

            css['vehicle_container'] = sel['vehicle_container']
            css['model'] = sel['model']
            css['trim'] = sel['trim']
            css['price'] = sel['price']

            # Also set JSON script selector if applicable
            if 'json_script_selector' in sel:
                profile['json_script_selector'] = sel['json_script_selector']

            profile['css'] = css
            cfg['profile'] = profile
            cfg['notes'] = (cfg.get('notes', '') + ' [CSS selectors applied from brand probe]').strip()

            f.write_text(yaml.dump(cfg, default_flow_style=False, allow_unicode=True, sort_keys=False))
            updated += 1
            if updated <= 5 or updated % 50 == 0:
                print(f"  Updated: {cc_dir.name}/{f.name} ({brand})")

    print(f"\nDone: {updated} sources updated, {skipped} skipped")


if __name__ == '__main__':
    apply_selectors()
