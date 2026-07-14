from pathlib import Path
import re
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.config_loader import _build_scrapling_profile


PL_SOURCE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "source_drafts"
    / "suv_only_country_model_top30"
    / "pl"
)

REPAIRED_AUDI_PROFILES = (
    ("05_audi_q3_pl.yaml", "Q3", "188 900"),
    ("23_audi_q7_pl.yaml", "Q7", "393 400"),
)


def _load_source(filename: str) -> dict[str, object]:
    source = yaml.safe_load((PL_SOURCE_ROOT / filename).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    return source


@pytest.mark.parametrize(("filename", "model", "expected_price"), REPAIRED_AUDI_PROFILES)
def test_repaired_poland_audi_profiles_extract_only_official_base_msrp(
    filename: str,
    model: str,
    expected_price: str,
) -> None:
    source = _load_source(filename)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    assert urlparse(str(source["source_url"])).hostname == "www.audi.pl"
    assert urlparse(str(profile_data["url"])).hostname == "www.audi.pl"
    assert f"/pl/modele/{model.lower()}/" in str(profile_data["url"])
    assert source["source_type"] == "manufacturer_official"
    assert source["price_semantics"] == "base_msrp"
    assert profile_data["default_currency"] == "PLN"
    assert profile_data["default_tax_included"] is True
    assert "css" not in profile_data

    profile = _build_scrapling_profile(profile_data)
    assert profile.text_regex is not None
    assert len(profile.text_regex.entry_patterns) == 1
    entry = profile.text_regex.entry_patterns[0]
    assert entry.price_label == "Recommended retail price incl. VAT"
    assert "(?P<price>" in entry.pattern
    assert "zawiera\\s+VAT" in entry.pattern
    assert "m-c" not in entry.pattern

    sample_text = (
        f"Skonfiguruj swoje Audi {model} Najniższa rata 1 990 PLN m-c "
        f"{model} od {expected_price} PLN zawiera VAT"
    )
    matches = list(re.finditer(entry.pattern, sample_text, flags=re.IGNORECASE | re.DOTALL))
    assert [match.group("price") for match in matches] == [expected_price]
    assert all(match.group("price") != "1 990" for match in matches)
