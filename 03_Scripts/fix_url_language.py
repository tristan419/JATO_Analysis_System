#!/usr/bin/env python3
"""Fix URL path language for batch A countries by brand-specific patterns."""

import yaml, os, re
from pathlib import Path
from collections import defaultdict

BASE = Path('07_ScrapingToolkit/source_drafts/suv_only_country_model_top30')

# Language → path corrections for known patterns
# Each entry: (wrong_pattern_regex, correct_replacement)
LANG_FIXES = {
    # Czech → German
    'de': [
        (r'/modely/', '/modelle/'),
        (r'/osobni-vozy/', '/pkws/'),
        (r'/vozidla/', '/fahrzeuge/'),
        (r'/nove-vozy/', '/neue-fahrzeuge/'),
        (r'/nova-auta/', '/neue-autos/'),
        (r'/hybridni-elektricke-vozy/', '/hybrid-elektrofahrzeuge/'),
        (r'-elektricke-vozy/', '-elektrofahrzeuge/'),
        (r'/automobily/', '/autos/'),
        (r'/modeller/', '/modelle/'),
        (r'/biler/', '/autos/'),
        (r'/nybil/', '/neuwagen/'),
        (r'/nya-bilar/', '/neue-autos/'),
        (r'/upptack/', '/entdecken/'),
        (r'/pregled\.html', '/uebersicht.html'),
        (r'/modeli/', '/modelle/'),
        (r'/unsere-modelle/', '/modelle/'),
    ],
    # Czech → French
    'fr': [
        (r'/modely/', '/modeles/'),
        (r'/osobni-vozy/', '/vehicules-particuliers/'),
        (r'/vozidla/', '/vehicules/'),
        (r'/nove-vozy/', '/nouveaux-vehicules/'),
        (r'/nova-auta/', '/nouveaux-vehicules/'),
        (r'/hybridni-elektricke-vozy/', '/hybride-electrique/'),
        (r'/hybridni-vozy/', '/hybride/'),
        (r'-elektricke-vozy/', '-electrique/'),
        (r'/automobily/', '/voitures/'),
        (r'/modeller/', '/modeles/'),
        (r'/biler/', '/voitures/'),
        (r'/nybil/', '/nouveaute/'),
        (r'/nya-bilar/', '/nouveaux-vehicules/'),
        (r'/upptack/', '/decouvrir/'),
        (r'/pregled\.html', '/apercu.html'),
        (r'/modeli/', '/modeles/'),
        (r'/unsere-modelle/', '/nos-modeles/'),
        (r'/modell/', '/modele/'),
    ],
    # Czech → Italian
    'it': [
        (r'/modely/', '/modelli/'),
        (r'/osobni-vozy/', '/auto/'),
        (r'/vozidla/', '/veicoli/'),
        (r'/nove-vozy/', '/nuovi-veicoli/'),
        (r'/nova-auta/', '/nuovi-modelli/'),
        (r'/hybridni-elektricke-vozy/', '/ibrido-elettrico/'),
        (r'/hybridni-vozy/', '/ibrido/'),
        (r'-elektricke-vozy/', '-elettrico/'),
        (r'/automobily/', '/auto/'),
        (r'/modeller/', '/modelli/'),
        (r'/biler/', '/auto/'),
        (r'/nybil/', '/nuova/'),
        (r'/nya-bilar/', '/nuovi-modelli/'),
        (r'/upptack/', '/scopri/'),
        (r'/pregled\.html', '/panoramica.html'),
        (r'/modeli/', '/modelli/'),
        (r'/unsere-modelle/', '/i-nostri-modelli/'),
        (r'/modell/', '/modello/'),
    ],
    # Czech → Polish
    'pl': [
        (r'/modely/', '/modele/'),
        (r'/osobni-vozy/', '/samochody/'),
        (r'/vozidla/', '/pojazdy/'),
        (r'/nove-vozy/', '/nowe-pojazdy/'),
        (r'/nova-auta/', '/nowe-samochody/'),
        (r'/hybridni-elektricke-vozy/', '/hybrydowo-elektryczne/'),
        (r'/hybridni-vozy/', '/hybrydowe/'),
        (r'-elektricke-vozy/', '-elektryczne/'),
        (r'/automobily/', '/samochody/'),
        (r'/modeller/', '/modele/'),
        (r'/biler/', '/samochody/'),
        (r'/nybil/', '/nowy/'),
        (r'/nya-bilar/', '/nowe-modele/'),
        (r'/upptack/', '/odkryj/'),
        (r'/pregled\.html', '/przeglad.html'),
        (r'/modeli/', '/modele/'),
        (r'/unsere-modelle/', '/nasze-modele/'),
        (r'/modell/', '/model/'),
    ],
    # Czech → Nordic (fi, dk, no, se)
    'fi': [
        (r'/modely/', '/mallit/'),
        (r'/osobni-vozy/', '/henkiloautot/'),
        (r'/vozidla/', '/ajoneuvot/'),
        (r'/nove-vozy/', '/uudet-autot/'),
        (r'/nova-auta/', '/uudet-autot/'),
        (r'/hybridni-elektricke-vozy/', '/hybridi-sahko/'),
        (r'/hybridni-vozy/', '/hybridi/'),
        (r'-elektricke-vozy/', '-sahko/'),
        (r'/automobily/', '/autot/'),
        (r'/modeller/', '/mallit/'),
        (r'/biler/', '/autot/'),
        (r'/nybil/', '/uusi/'),
        (r'/nya-bilar/', '/uudet-autot/'),
        (r'/upptack/', '/tutustu/'),
        (r'/pregled\.html', '/yleiskatsaus.html'),
        (r'/modeli/', '/mallit/'),
        (r'/unsere-modelle/', '/mallimme/'),
        (r'/modell/', '/malli/'),
    ],
    'dk': [
        (r'/modely/', '/modeller/'),
        (r'/osobni-vozy/', '/personbiler/'),
        (r'/vozidla/', '/koretojer/'),
        (r'/nove-vozy/', '/nye-biler/'),
        (r'/nova-auta/', '/nye-biler/'),
        (r'/hybridni-elektricke-vozy/', '/hybrid-elbiler/'),
        (r'/hybridni-vozy/', '/hybrid/'),
        (r'-elektricke-vozy/', '-elbiler/'),
        (r'/automobily/', '/biler/'),
        (r'/modeller/', '/modeller/'),
        (r'/biler/', '/biler/'),
        (r'/nybil/', '/ny-bil/'),
        (r'/nya-bilar/', '/nye-biler/'),
        (r'/upptack/', '/opdag/'),
        (r'/pregled\.html', '/oversigt.html'),
        (r'/modeli/', '/modeller/'),
        (r'/unsere-modelle/', '/vores-modeller/'),
        (r'/modell/', '/model/'),
    ],
}

# Brand-specific URL format overrides (complete URL reconstruction)
BRAND_URL_TEMPLATES = {
    # BMW: bmw.{tld}/{lang_path}/modelle/{series}/...
    'BMW': {
        'de': lambda m: f"https://www.bmw.de/de/modelluebersicht.html#{m.lower()}",
        'fr': lambda m: f"https://www.bmw.fr/fr/configurateur.html",
        'it': lambda m: f"https://www.bmw.it/it/configuratore.html",
        'pl': lambda m: f"https://www.bmw.pl/pl/serwis/configurator.html",
        'fi': lambda m: f"https://www.bmw.fi/fi/mallisto.html",
        'dk': lambda m: f"https://www.bmw.dk/da/alle-modeller.html",
    },
    # Cupra: cupraofficial.{tld}/{lang}/modelle/...
    'CUPRA': {
        'de': lambda m: f"https://www.cupraofficial.de/modelle/{m.lower().replace(' ', '-')}",
        'fr': lambda m: f"https://www.cupraofficial.fr/modeles/{m.lower().replace(' ', '-')}",
        'it': lambda m: f"https://www.cupraofficial.it/modelli/{m.lower().replace(' ', '-')}",
        'pl': lambda m: f"https://www.cupraofficial.pl/modele/{m.lower().replace(' ', '-')}",
        'fi': lambda m: f"https://www.cupraofficial.fi/mallisto/{m.lower().replace(' ', '-')}",
    },
    # Dacia: dacia.{tld}/...
    'DACIA': {
        'de': lambda m: f"https://www.dacia.de/modelle/{m.lower().replace(' ', '-')}.html",
        'fr': lambda m: f"https://www.dacia.fr/modeles/{m.lower().replace(' ', '-')}.html",
        'it': lambda m: f"https://www.dacia.it/modelli/{m.lower().replace(' ', '-')}.html",
        'pl': lambda m: f"https://www.dacia.pl/modele/{m.lower().replace(' ', '-')}.html",
    },
    # Ford: ford.{tld}/...
    'FORD': {
        'de': lambda m: f"https://www.ford.de/pkw-modelle/{m.lower().replace(' ', '-')}",
        'fr': lambda m: f"https://www.ford.fr/vehicules/{m.lower().replace(' ', '-')}",
        'it': lambda m: f"https://www.ford.it/auto/{m.lower().replace(' ', '-')}",
        'pl': lambda m: f"https://www.ford.pl/samochody/{m.lower().replace(' ', '-')}",
    },
    # Hyundai: hyundai.{tld}/...
    'HYUNDAI': {
        'de': lambda m: f"https://www.hyundai.de/modelle/{m.lower().replace(' ', '-')}.html",
        'fr': lambda m: f"https://www.hyundai.fr/modeles/{m.lower().replace(' ', '-')}.html",
        'it': lambda m: f"https://www.hyundai.it/modelli/{m.lower().replace(' ', '-')}.html",
        'pl': lambda m: f"https://www.hyundai.pl/modele/{m.lower().replace(' ', '-')}.html",
    },
    # Nissan: nissan.{tld}/...
    'NISSAN': {
        'de': lambda m: f"https://www.nissan.de/fahrzeuge/{m.lower().replace(' ', '-')}.html",
        'fr': lambda m: f"https://www.nissan.fr/vehicules/{m.lower().replace(' ', '-')}.html",
        'it': lambda m: f"https://www.nissan.it/veicoli/{m.lower().replace(' ', '-')}.html",
        'pl': lambda m: f"https://www.nissan.pl/pojazdy/{m.lower().replace(' ', '-')}.html",
    },
    # Opel: opel.{tld}/...
    'OPEL': {
        'de': lambda m: f"https://www.opel.de/modelle/{m.lower().replace(' ', '-')}.html",
        'fr': lambda m: f"https://www.opel.fr/modeles/{m.lower().replace(' ', '-')}.html",
        'it': lambda m: f"https://www.opel.it/modelli/{m.lower().replace(' ', '-')}.html",
        'pl': lambda m: f"https://www.opel.pl/modele/{m.lower().replace(' ', '-')}.html",
    },
    # Peugeot: peugeot.{tld}/...
    'PEUGEOT': {
        'de': lambda m: f"https://www.peugeot.de/modelle/{m.lower().replace(' ', '-')}.html",
        'fr': lambda m: f"https://www.peugeot.fr/modeles/{m.lower().replace(' ', '-')}.html",
        'it': lambda m: f"https://www.peugeot.it/modelli/{m.lower().replace(' ', '-')}.html",
        'pl': lambda m: f"https://www.peugeot.pl/modele/{m.lower().replace(' ', '-')}.html",
        'fi': lambda m: f"https://www.peugeot.fi/mallisto/{m.lower().replace(' ', '-')}.html",
    },
    # Renault: renault.{tld}/...
    'RENAULT': {
        'de': lambda m: f"https://www.renault.de/modelle/{m.lower().replace(' ', '-')}.html",
        'fr': lambda m: f"https://www.renault.fr/modeles/{m.lower().replace(' ', '-')}.html",
        'it': lambda m: f"https://www.renault.it/modelli/{m.lower().replace(' ', '-')}.html",
        'pl': lambda m: f"https://www.renault.pl/modele/{m.lower().replace(' ', '-')}.html",
    },
    # Toyota: toyota.{tld}/...
    'TOYOTA': {
        'de': lambda m: f"https://www.toyota.de/autos/{m.lower().replace(' ', '-')}",
        'fr': lambda m: f"https://www.toyota.fr/vehicules/{m.lower().replace(' ', '-')}",
        'it': lambda m: f"https://www.toyota.it/auto/{m.lower().replace(' ', '-')}",
        'pl': lambda m: f"https://www.toyota.pl/samochody/{m.lower().replace(' ', '-')}",
        'fi': lambda m: f"https://www.toyota.fi/autot/{m.lower().replace(' ', '-')}",
        'dk': lambda m: f"https://www.toyota.dk/biler/{m.lower().replace(' ', '-')}",
    },
    # Skoda: skoda.{tld}/...
    'SKODA': {
        'de': lambda m: f"https://www.skoda.de/modelle/{m.lower().replace(' ', '-')}",
        'fr': lambda m: f"https://www.skoda.fr/modeles/{m.lower().replace(' ', '-')}",
        'it': lambda m: f"https://www.skoda.it/modelli/{m.lower().replace(' ', '-')}",
        'pl': lambda m: f"https://www.skoda.pl/modele/{m.lower().replace(' ', '-')}",
        'fi': lambda m: f"https://www.skoda.fi/mallisto/{m.lower().replace(' ', '-')}",
        'dk': lambda m: f"https://www.skoda.dk/modeller/{m.lower().replace(' ', '-')}",
    },
    # Volvo: volvocars.{tld}/...
    'VOLVO': {
        'de': lambda m: f"https://www.volvocars.com/de/cars/{m.lower().replace(' ', '-')}",
        'fr': lambda m: f"https://www.volvocars.com/fr/voitures/{m.lower().replace(' ', '-')}",
        'it': lambda m: f"https://www.volvocars.com/it/auto/{m.lower().replace(' ', '-')}",
        'pl': lambda m: f"https://www.volvocars.com/pl/samochody/{m.lower().replace(' ', '-')}",
    },
    # Seat: seat.{tld}/...
    'SEAT': {
        'de': lambda m: f"https://www.seat.de/modelle/{m.lower().replace(' ', '-')}",
        'fr': lambda m: f"https://www.seat.fr/modeles/{m.lower().replace(' ', '-')}",
        'it': lambda m: f"https://www.seat.it/modelli/{m.lower().replace(' ', '-')}",
        'pl': lambda m: f"https://www.seat.pl/modele/{m.lower().replace(' ', '-')}",
    },
    # Suzuki: suzuki.{tld}/...
    'SUZUKI': {
        'de': lambda m: f"https://auto.suzuki.de/modelle/{m.lower().replace(' ', '-')}",
        'fr': lambda m: f"https://auto.suzuki.fr/modeles/{m.lower().replace(' ', '-')}",
        'it': lambda m: f"https://auto.suzuki.it/modelli/{m.lower().replace(' ', '-')}",
        'pl': lambda m: f"https://auto.suzuki.pl/modele/{m.lower().replace(' ', '-')}",
    },
    # BYD: byd.com/{cc}/...
    'BYD': {
        'de': lambda m: f"https://www.byd.com/de/model/{m.lower().replace(' ', '-')}",
        'fr': lambda m: f"https://www.byd.com/fr/model/{m.lower().replace(' ', '-')}",
        'it': lambda m: f"https://www.byd.com/it/model/{m.lower().replace(' ', '-')}",
        'pl': lambda m: f"https://www.byd.com/pl/model/{m.lower().replace(' ', '-')}",
    },
    # MG: mgmotor.{tld}/...
    'MG': {
        'de': lambda m: f"https://www.mgmotor.de/modelle/{m.lower().replace(' ', '-')}",
        'fr': lambda m: f"https://www.mgmotor.fr/modeles/{m.lower().replace(' ', '-')}",
        'it': lambda m: f"https://www.mgmotor.it/modelli/{m.lower().replace(' ', '-')}",
        'pl': lambda m: f"https://www.mgmotor.pl/modele/{m.lower().replace(' ', '-')}",
    },
}


def main():
    fixed = 0
    for cc in ['de', 'fr', 'it', 'pl', 'fi', 'dk']:
        country_dir = BASE / cc
        if not country_dir.is_dir():
            continue

        for f in sorted(country_dir.iterdir()):
            if not f.suffix == '.yaml':
                continue

            cfg = yaml.safe_load(f.read_text()) or {}
            profile = cfg.get('profile', {})
            url = (cfg.get('source_url', '') or profile.get('url', '')).strip()
            if not url or 'todo.invalid' in url:
                continue

            brand = str(cfg.get('brand', '')).strip().upper()
            model = str(profile.get('fixed_model', '')).strip()
            if not model:
                model = f.stem.replace(f'_{cc}_draft_scrapling', '').replace('_', ' ').split()[-1]

            new_url = url

            # Try brand-specific template first
            if brand in BRAND_URL_TEMPLATES and cc in BRAND_URL_TEMPLATES[brand]:
                new_url = BRAND_URL_TEMPLATES[brand][cc](model)
            # Then apply language path fixes
            elif cc in LANG_FIXES:
                for pattern, replacement in LANG_FIXES[cc]:
                    new_url = re.sub(pattern, replacement, new_url)

            if new_url != url:
                cfg['source_url'] = new_url
                if isinstance(profile, dict):
                    profile['url'] = new_url
                    cfg['profile'] = profile
                cfg['notes'] = (cfg.get('notes', '') + ' [URL language corrected]').strip()
                f.write_text(yaml.dump(cfg, default_flow_style=False, allow_unicode=True, sort_keys=False))
                fixed += 1
                print(f"✅ {cc.upper()} {brand:20s} {model:25s} → {new_url[:90]}")

    print(f"\nFixed: {fixed} URLs")


if __name__ == '__main__':
    main()
