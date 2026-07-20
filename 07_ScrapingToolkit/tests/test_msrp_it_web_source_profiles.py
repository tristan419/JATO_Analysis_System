from pathlib import Path
import re
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.config_loader import _build_scrapling_profile


IT_SOURCE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "source_drafts"
    / "suv_only_country_model_top30"
    / "it"
)


@pytest.mark.parametrize(
    (
        "filename",
        "expected_host",
        "expected_label",
        "sample_text",
        "expected",
        "excluded",
    ),
    (
        (
            "21_alfa_romeo_junior_it.yaml",
            "www.alfaromeo.it",
            "Prezzo di listino, IVA e MIS incluse",
            """
            CON 5.000€ DI VANTAGGI PER TUTTI
            Anticipo 6.012€ Rata finale residua 20.963€
            Prezzo promo da 27.950€ Da 159€/mese
            IBRIDA Prezzo di listino da 31.050 €
            IBRIDA SPRINT Prezzo di listino da 32.950€
            IBRIDA TI Prezzo di listino da 34.450€
            IBRIDA SPORT SPECIALE Prezzo di listino da 36.150€
            """,
            [
                ("Ibrida", "31.050", "MHEV"),
                ("Ibrida Sprint", "32.950", "MHEV"),
                ("Ibrida TI", "34.450", "MHEV"),
                ("Ibrida Sport Speciale", "36.150", "MHEV"),
            ],
            {"5.000", "6.012", "20.963", "27.950", "159"},
        ),
        (
            "27_alfa_romeo_tonale_it.yaml",
            "www.alfaromeo.it",
            "Prezzo di listino, IVA e MIS incluse",
            """
            7.000€ DI VANTAGGI
            Prezzo promo da 33.700€ Da 399€/48 rate mensili
            TONALE Prezzo di listino da 40.700€
            BUSINESS Prezzo di listino da 43.300€
            SPRINT Prezzo di listino da 43.550€
            TI Prezzo di listino da 46.300€
            SPORT SPECIALE Prezzo di listino da 46.950€
            VELOCE Prezzo di listino da 49.050€
            """,
            [
                ("Tonale", "40.700", "ICE"),
                ("Business", "43.300", "ICE"),
                ("Sprint", "43.550", "ICE"),
                ("TI", "46.300", "ICE"),
                ("Sport Speciale", "46.950", "ICE"),
                ("Veloce", "49.050", "ICE"),
            ],
            {"7.000", "33.700", "399", "48"},
        ),
        (
            "26_suzuki_vitara_it.yaml",
            "auto.suzuki.it",
            "Prezzo di listino, IVA inclusa",
            """
            VITARA HYBRID 1.4 COOL+
            Prezzo di listino € 27.450,00 IVA inclusa
            Prezzo promo € 22.950,00 IVA inclusa
            Sconto € 4.500,00 Anticipo € 6.025,00
            36 rate da € 198,85 Maxirata finale € 13.600,00
            """,
            [("1.4 COOL+", "27.450,00", "MHEV")],
            {"22.950,00", "4.500,00", "6.025,00", "198,85", "13.600,00"},
        ),
    ),
)
def test_official_it_web_profiles_extract_trim_list_prices_only(
    filename: str,
    expected_host: str,
    expected_label: str,
    sample_text: str,
    expected: list[tuple[str, str, str]],
    excluded: set[str],
) -> None:
    source = yaml.safe_load((IT_SOURCE_ROOT / filename).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    assert urlparse(str(source["source_url"])).hostname == expected_host
    assert source["source_type"] == "manufacturer_official"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "scrapling"
    assert profile_data["tier"] == "http"
    assert "css" not in profile_data

    profile = _build_scrapling_profile(profile_data)
    assert profile.text_regex is not None
    matches = []
    for entry in profile.text_regex.entry_patterns:
        entry_matches = list(
            re.finditer(entry.pattern, sample_text, flags=re.IGNORECASE | re.DOTALL)
        )
        assert len(entry_matches) == 1
        matches.append(
            (
                entry.official_trim or "",
                entry_matches[0].group("price"),
                profile.fixed_jato_powertrain or "",
            )
        )

    assert matches == expected
    assert all(price not in excluded for _, price, _ in matches)
    assert all(
        entry.price_label == expected_label
        for entry in profile.text_regex.entry_patterns
    )
