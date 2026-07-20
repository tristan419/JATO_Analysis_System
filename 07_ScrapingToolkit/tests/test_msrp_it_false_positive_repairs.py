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
    ("filename", "host", "sample_text", "expected", "excluded"),
    (
        (
            "03_dacia_duster_it.yaml",
            "www.dacia.it",
            """
            Versione essential prezzo di listino prezzo a partire da 19.900 €
            DUSTER essential MY26 Eco-G 120
            Versione expression prezzo di listino prezzo a partire da 21.850 €
            DUSTER expression MY26 Eco-G 120
            Versione journey prezzo di listino prezzo a partire da 23.400 €
            DUSTER journey MY26 Eco-G 120
            Versione extreme prezzo di listino prezzo a partire da 23.400 €
            DUSTER extreme MY26 Eco-G 120
            rata da 89 euro anticipo 7.700 euro
            """,
            [
                ("essential", "19.900"),
                ("expression", "21.850"),
                ("journey", "23.400"),
                ("extreme", "23.400"),
            ],
            {"89", "7.700"},
        ),
        (
            "08_renault_captur_it.yaml",
            "www.renault.it",
            """
            Captur Evolution full hybrid E-Tech series 160 cv
            A partire da 24.900 euro con bonus Renault E-Tech
            Versione evolution mostra il prezzo in contanti
            prezzo a partire da 23.576 €
            CAPTUR evolution ECO-G 120 MY26
            Versione techno mostra il prezzo in contanti
            prezzo a partire da 26.124 €
            CAPTUR techno TCe 115 MY26
            """,
            [("evolution", "23.576"), ("techno", "26.124")],
            {"24.900"},
        ),
        (
            "10_bmw_x1_it.yaml",
            "www.bmw.it",
            """
            SUV Nuovo iX Modelli 100% elettrica Da 86.000 €
            SUV X1 Modelli Benzina Da 44.400 €
            SUV X1 Modelli M Benzina Da 63.800 €
            """,
            [("X1 Benzina", "44.400")],
            {"86.000", "63.800"},
        ),
        (
            "30_bmw_x3_it.yaml",
            "www.bmw.it",
            """
            SUV Nuovo X5 Modelli Benzina Da 98.200 €
            SUV Nuovo X3 Modelli Benzina Da 66.200 €
            SUV X3 Modelli M Benzina Da 92.300 €
            """,
            [("X3 Benzina", "66.200")],
            {"98.200", "92.300"},
        ),
    ),
)
def test_it_false_positive_repairs_are_model_and_trim_scoped(
    filename: str,
    host: str,
    sample_text: str,
    expected: list[tuple[str, str]],
    excluded: set[str],
) -> None:
    source = yaml.safe_load((IT_SOURCE_ROOT / filename).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    assert urlparse(str(source["source_url"])).hostname == host
    assert source["source_type"] == "manufacturer_official"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "scrapling"
    assert "css" not in profile_data
    assert "json_script_selector" not in profile_data

    profile = _build_scrapling_profile(profile_data)
    assert profile.text_regex is not None
    actual = []
    for entry in profile.text_regex.entry_patterns:
        matches = list(
            re.finditer(entry.pattern, sample_text, flags=re.IGNORECASE | re.DOTALL)
        )
        assert len(matches) == 1
        actual.append((entry.official_trim or "", matches[0].group("price")))

    assert actual == expected
    assert all(price not in excluded for _, price in actual)


def test_captur_powertrain_rules_separate_lpg_and_petrol() -> None:
    source = yaml.safe_load(
        (IT_SOURCE_ROOT / "08_renault_captur_it.yaml").read_text(encoding="utf-8")
    )
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)
    rules = profile_data["structured_fields"]["powertrain_rules"]

    assert rules == [
        {"key": "powertrain_lpg", "powertrain": "LPG", "keywords": ["eco-g", "gpl"]},
        {"key": "powertrain_ice", "powertrain": "ICE", "keywords": ["tce", "benzina"]},
    ]
