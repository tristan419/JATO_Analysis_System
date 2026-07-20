from pathlib import Path
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_pdf_text_profile
from jato_scraper.extractors.pdf_text import PdfTextExtractor


SOURCE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "source_drafts"
    / "suv_only_country_model_top30"
)

VOLVO_PL_PDF_CASES = (
    (
        "pl/22_volvo_xc40_pl.yaml",
        "2026-02-02",
        "1adcce16ac777564ab8d37b46d6e3df65a0e4931e75b25fa773ca8eb5a301456",
        """
        SILNIKI BENZYNOWE MILD HYBRID Cena z VAT
        B3 FWD 2,0/ 163+14 automatyczna Geartronic, 7 biegów
        6,5-7,3* 147-164* 176 900 191 900 205 900 225 900 - -
        B4 FWD 2,0/ 197+14 automatyczna Geartronic, 7 biegów
        6,5-7,3* 148-164* - 201 900 215 900 235 900 223 900 241 900
        """,
        [
            (176_900.0, "ESSENTIAL", "MHEV"),
            (201_900.0, "CORE", "MHEV"),
        ],
    ),
    (
        "pl/24_volvo_xc60_pl.yaml",
        "2026-03-09",
        "f6604bba17c2ea7707b369d7442281df7387cfe7a11ad901271654864f72a2a0",
        """
        SILNIKI BENZYNOWE PLUG-IN HYBRID Cena z VAT
        T6 eAWD 2,0/ 336 automatyczna Geartronic 8 biegów
        2,7-3,6 61-81 269 900 274 900 296 900 299 900 317 900 320 900
        T8 eAWD 2,0/ 406 automatyczna Geartronic 8 biegów
        2,7-3,6 61-81 279 900 284 900 321 900 324 900 347 900 350 900
        SILNIKI BENZYNOWE MILD HYBRID Cena z VAT
        B5 AWD 2,0/ 250+14 automatyczna Geartronic 8 biegów
        7,9-8,9** 179-202** 221 900 226 900 261 900 264 900 289 900 292 900
        """,
        [
            (269_900.0, "ESSENTIAL", "PHEV"),
            (279_900.0, "ESSENTIAL", "PHEV"),
            (221_900.0, "ESSENTIAL", "MHEV"),
        ],
    ),
)


@pytest.mark.parametrize(
    ("relative_path", "price_date", "sha256", "sample_text", "expected"),
    VOLVO_PL_PDF_CASES,
)
def test_repaired_volvo_pl_profiles_extract_vat_inclusive_vehicle_prices(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: str,
    price_date: str,
    sha256: str,
    sample_text: str,
    expected: list[tuple[float, str, str]],
) -> None:
    source = yaml.safe_load((SOURCE_ROOT / relative_path).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    source_url = str(source["source_url"])
    assert urlparse(source_url).hostname == "azure-eu-assets.contentstack.com"
    assert source_url.endswith(".pdf")
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["prefer_curl_download"] is True
    assert profile_data["default_currency"] == "PLN"
    assert profile_data["default_tax_included"] is True
    assert profile_data["default_price_label"] == "Cena z VAT (PLN)"
    assert profile_data["match_reason"]["document_price_date"] == price_date
    assert profile_data["match_reason"]["document_audited_on"] == "2026-07-15"
    assert profile_data["match_reason"]["document_sha256_at_audit"] == sha256
    assert "css" not in profile_data

    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code=str(source["source_code"]),
            country=str(source["country"]),
            brand=str(source["brand"]),
            source_url=source_url,
            source_type=str(source["source_type"]),
            price_semantics=str(source["price_semantics"]),
        ),
        _build_pdf_text_profile(profile_data),
    )
    monkeypatch.setattr(extractor, "_extract_text", lambda: sample_text)

    observations = extractor.extract()
    actual = [
        (observation.msrp_value, observation.official_trim, observation.jato_powertrain)
        for observation in observations
    ]
    assert actual == expected
    assert all(observation.tax_included for observation in observations)
    assert all(observation.match_confidence == 0.9 for observation in observations)
    assert all(observation.match_status == "review_required" for observation in observations)
