from pathlib import Path
import re
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.config_loader import _build_scrapling_profile


SOURCE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "source_drafts"
    / "suv_only_country_model_top30"
)

REPAIRED_NISSAN_PROFILES = (
    (
        "cz/26_nissan_qashqai_cz.yaml",
        "CZK",
        "Již od 609.990 Kč JIŽ OD 7.299 KČ BEZ DPH / MĚSÍC",
        [("Qashqai entry price", "609.990")],
    ),
    (
        "it/16_nissan_qashqai_it.yaml",
        "EUR",
        (
            "Gamma Hybrid Da 26.900 EUR oppure 249 al mese "
            "Qashqai Acenta Listino a partire Da 33.100 € "
            "Prezzo Promo 26.900 € Qashqai N-Connecta "
            "Listino a partire Da 35.600 €"
        ),
        [("Acenta", "33.100"), ("N-Connecta", "35.600")],
    ),
    (
        "it/24_nissan_juke_it.yaml",
        "EUR",
        (
            "Gamma da 20.900 EUR oppure 219 al mese "
            "Pulse Prezzo promo 25.400 € Listino a partire da: 30.500 € "
            "Acenta Prezzo promo 20.900 € Listino a partire da: 26.000 €"
        ),
        [("Pulse", "30.500"), ("Acenta", "26.000")],
    ),
)


@pytest.mark.parametrize(
    ("relative_path", "currency", "sample_text", "expected"),
    REPAIRED_NISSAN_PROFILES,
)
def test_repaired_nissan_profiles_extract_official_list_prices_only(
    relative_path: str,
    currency: str,
    sample_text: str,
    expected: list[tuple[str, str]],
) -> None:
    source = yaml.safe_load((SOURCE_ROOT / relative_path).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    assert (urlparse(str(source["source_url"])).hostname or "").startswith("www.nissan.")
    assert source["source_type"] == "manufacturer_official"
    assert source["price_semantics"] == "base_msrp"
    assert profile_data["default_currency"] == currency
    assert profile_data["default_tax_included"] is True
    assert "css" not in profile_data
    assert "json_script_selector" not in profile_data

    profile = _build_scrapling_profile(profile_data)
    assert profile.text_regex is not None
    entry = profile.text_regex.entry_patterns[0]
    assert entry.price_label
    matches = [
        (
            entry.official_trim or match.groupdict().get("trim") or "",
            match.group("price"),
        )
        for match in re.finditer(entry.pattern, sample_text, flags=re.IGNORECASE | re.DOTALL)
    ]
    assert matches == expected
    assert all("26.900" != price for _, price in matches)
