"""Tests for the Scrapling-based web extractor.

All Scrapling fetchers are mocked — tests verify the parsing and
mapping logic without hitting real websites.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.scraper.base import ExtractorConfig
from app.scraper.extractors.scrapling_web import (
    AttrJsonMapping,
    ScraplingExtractor,
    ScraplingProfile,
    CssMapping,
    parse_price,
)


# ── parse_price tests ────────────────────────────────────────────────

class TestParsePrice:
    def test_german_format(self):
        assert parse_price("42.900 €") == 42_900.0

    def test_german_format_with_cents(self):
        assert parse_price("42.900,50 €") == 42_900.50

    def test_english_format(self):
        assert parse_price("$42,900.00") == 42_900.00

    def test_plain_integer(self):
        assert parse_price("42900") == 42_900.0

    def test_with_spaces(self):
        assert parse_price("42 900 €") == 42_900.0

    def test_no_price(self):
        assert parse_price("N/A") is None

    def test_with_nbsp(self):
        assert parse_price("42\xa0900\xa0€") == 42_900.0


# ── CSS extraction tests ─────────────────────────────────────────────

def _make_css_extractor() -> ScraplingExtractor:
    cfg = ExtractorConfig(
        source_code="test_css",
        country="德国",
        brand="TestBrand",
        source_url="https://test.example.com",
    )
    profile = ScraplingProfile(
        url="https://test.example.com/models",
        tier="http",
        css=CssMapping(
            vehicle_container=".model-card",
            model=".model-name::text",
            trim=".trim-name::text",
            price=".price::text",
        ),
    )
    return ScraplingExtractor(cfg, profile)


def _make_weighted_rule_extractor(
    fixed_jato_powertrain: str = "PHEV",
    base_score: float = 0.19,
    review_threshold: float = 0.95,
    semi_auto_threshold: float = 0.98,
) -> ScraplingExtractor:
    cfg = ExtractorConfig(
        source_code="volvo_rule_test",
        country="瑞典",
        brand="Volvo",
        source_url=(
            "https://www.volvocars.com"
            "/se/build/xc60-hybrid/"
        ),
    )
    profile = ScraplingProfile(
        url=(
            "https://www.volvocars.com"
            "/se/build/xc60-hybrid/"
        ),
        tier="http",
        css=CssMapping(
            vehicle_container=".selection-card",
            model="",
            trim=".title::text",
            price=".price::text",
            exclude_if_selector=(
                "small[data-sources*="
                "'valueDescription']"
            ),
        ),
        default_currency="SEK",
        default_price_label="Totalpris inkl. moms",
        fixed_model="XC60",
        fixed_jato_model="XC60",
        fixed_jato_powertrain=fixed_jato_powertrain,
        copy_trim_to_jato_trim=True,
        exclude_price_prefixes=("Från",),
        match_reason={
            "strategy": "volvo_build_selection_card",
        },
        confidence_rules={
            "base": base_score,
            "fixed_model_bonus": 0.18,
            "fixed_jato_model_bonus": 0.12,
            "trim_present_bonus": 0.10,
            "copy_trim_to_jato_trim_bonus": 0.09,
            "exclude_price_prefixes_bonus": 0.04,
            "exclude_if_selector_bonus": 0.04,
            "parsed_price_text_bonus": 0.03,
            "currency_bonus": 0.01,
            "price_label_bonus": 0.02,
            "trim_keyword_bonuses": [
                {
                    "key": "trim_keyword_core",
                    "keyword": "core",
                    "delta": 0.01,
                },
                {
                    "key": "trim_keyword_plus",
                    "keyword": "plus",
                    "delta": 0.02,
                },
                {
                    "key": "trim_keyword_ultra",
                    "keyword": "ultra",
                    "delta": 0.03,
                },
            ],
            "price_band_bonuses": [
                {
                    "key": "price_band_entry",
                    "max": 579999,
                    "delta": 0.00,
                },
                {
                    "key": "price_band_mid",
                    "min": 580000,
                    "max": 699999,
                    "delta": 0.01,
                },
                {
                    "key": "price_band_high",
                    "min": 700000,
                    "delta": 0.03,
                },
            ],
            "powertrain_bonuses": [
                {
                    "key": "powertrain_phev",
                    "powertrain": "PHEV",
                    "delta": 0.03,
                },
                {
                    "key": "powertrain_mhev",
                    "powertrain": "MHEV",
                    "delta": 0.03,
                },
            ],
        },
        structured_fields={
            "edition_rules": [
                {
                    "key": "edition_black",
                    "label": "Black Edition",
                    "keyword": "black edition",
                    "special": True,
                },
                {
                    "key": "edition_nordic",
                    "label": "Nordic Edition",
                    "keyword": "nordic edition",
                },
            ],
            "powertrain_rules": [
                {
                    "key": "powertrain_xc60_phev",
                    "powertrain": "PHEV",
                    "keywords": [
                        "xc60-hybrid",
                        "plug-in hybrid",
                        "recharge",
                        "t6",
                        "t8",
                    ],
                },
                {
                    "key": "powertrain_xc60_mhev",
                    "powertrain": "MHEV",
                    "keywords": [
                        "mild hybrid",
                        "mhev",
                        "b5",
                        "b6",
                    ],
                },
            ],
        },
        auto_accept_gates={
            "review_threshold": review_threshold,
            "semi_auto_threshold": (
                semi_auto_threshold
            ),
            "require_powertrain_match": True,
            "force_review_if_powertrain_missing": (
                True
            ),
            "force_review_if_powertrain_ambiguous": (
                True
            ),
            "force_review_for_special_edition": True,
        },
    )
    return ScraplingExtractor(cfg, profile)


def _make_model_rule_extractor(
    *,
    skip_if_model_unmapped: bool = False,
) -> ScraplingExtractor:
    cfg = ExtractorConfig(
        source_code="renault_family_test",
        country="法国",
        brand="Renault",
        source_url="https://www.renault.example.com/gamme",
    )
    profile = ScraplingProfile(
        url="https://www.renault.example.com/gamme",
        tier="http",
        css=CssMapping(
            vehicle_container=".vehicle-card",
            model=".vehicle-model::text",
            trim=".vehicle-trim::text",
            price=".vehicle-price::text",
        ),
        copy_trim_to_jato_trim=True,
        confidence_rules={
            "base": 0.40,
            "model_rule_bonus": 0.12,
            "trim_present_bonus": 0.10,
            "copy_trim_to_jato_trim_bonus": 0.09,
            "parsed_price_text_bonus": 0.03,
            "currency_bonus": 0.01,
            "price_label_bonus": 0.02,
        },
        model_rules=(
            {
                "key": "model_clio",
                "jato_model": "Clio",
                "official_model": "Clio",
                "keywords": ["clio", "nouvelle clio"],
            },
            {
                "key": "model_captur",
                "jato_model": "Captur",
                "official_model": "Captur",
                "keywords": ["captur"],
            },
        ),
        skip_if_model_unmapped=skip_if_model_unmapped,
    )
    return ScraplingExtractor(cfg, profile)


def _mock_page_with_css(cards: list[dict]) -> MagicMock:
    """Build a mock page that responds to .css() calls."""
    page = MagicMock()
    containers = []
    for card in cards:
        el = MagicMock()

        def _css_side_effect(selector, _card=card):
            mock_result = MagicMock()
            if "model-name" in selector:
                mock_result.get.return_value = _card.get("model")
            elif "trim-name" in selector:
                mock_result.get.return_value = _card.get("trim")
            elif "price" in selector:
                mock_result.get.return_value = _card.get("price")
            else:
                mock_result.get.return_value = None
            return mock_result
        el.css = _css_side_effect
        containers.append(el)

    page.css.return_value = containers
    return page


class TestCssExtraction:
    @patch.object(ScraplingExtractor, "_fetch")
    def test_extracts_vehicles(self, mock_fetch):
        mock_fetch.return_value = _mock_page_with_css([
            {"model": "3 Series", "trim": "320i", "price": "42.900 €"},
            {"model": "X5", "trim": "xDrive40i", "price": "72.900 €"},
        ])

        ext = _make_css_extractor()
        results = ext.extract()
        assert len(results) == 2
        assert results[0].official_model == "3 Series"
        assert results[0].msrp_value == 42_900.0
        assert results[1].official_model == "X5"
        assert results[1].msrp_value == 72_900.0

    @patch.object(ScraplingExtractor, "_fetch")
    def test_skips_card_without_price(self, mock_fetch):
        mock_fetch.return_value = _mock_page_with_css([
            {"model": "3 Series", "trim": "320i", "price": "42.900 €"},
            {"model": "Concept", "trim": "", "price": None},
        ])

        ext = _make_css_extractor()
        results = ext.extract()
        assert len(results) == 1

    @patch.object(ScraplingExtractor, "_fetch")
    def test_returns_empty_on_fetch_failure(self, mock_fetch):
        mock_fetch.return_value = None
        ext = _make_css_extractor()
        assert ext.extract() == []


class TestWeightedRuleConfidence:
    def test_confidence_varies_by_trim_price_and_powertrain_evidence(self):
        ext = _make_weighted_rule_extractor()

        core = ext._build_observation(
            official_model="",
            official_trim="Core Nordic Edition",
            msrp_value=569_900.0,
            currency="SEK",
            raw_payload={
                "priceText": "569 900 kr",
                "trimText": "Core Nordic Edition",
            },
        )
        plus = ext._build_observation(
            official_model="",
            official_trim="Plus Nordic Edition",
            msrp_value=599_900.0,
            currency="SEK",
            raw_payload={
                "priceText": "599 900 kr",
                "trimText": "Plus Nordic Edition",
            },
        )
        ultra = ext._build_observation(
            official_model="",
            official_trim="Ultra",
            msrp_value=773_000.0,
            currency="SEK",
            raw_payload={
                "priceText": "773 000 kr",
                "trimText": "Ultra",
            },
        )

        assert core.match_confidence == 0.86
        assert plus.match_confidence == 0.88
        assert ultra.match_confidence == 0.91

        core_components = {
            component["key"]
            for component in core.match_reason["confidenceRule"]["components"]
            if component["applied"]
        }
        plus_components = {
            component["key"]
            for component in plus.match_reason["confidenceRule"]["components"]
            if component["applied"]
        }
        ultra_components = {
            component["key"]
            for component in ultra.match_reason["confidenceRule"]["components"]
            if component["applied"]
        }

        assert (
            core.match_reason["confidenceRule"]["mode"]
            == "weighted_profile_v2"
        )
        assert core.official_edition == "Nordic Edition"
        assert core.official_powertrain == "PHEV"
        assert core.jato_powertrain == "PHEV"
        assert "trim_keyword_core" in core_components
        assert "price_band_entry" in core_components
        assert "powertrain_phev" in core_components
        assert "trim_keyword_plus" in plus_components
        assert "price_band_mid" in plus_components
        assert "trim_keyword_ultra" in ultra_components
        assert "price_band_high" in ultra_components

    def test_auto_accept_gate_blocks_special_editions(self):
        ext = _make_weighted_rule_extractor(
            base_score=0.24,
        )

        black_edition = ext._build_observation(
            official_model="",
            official_trim="Ultra Black Edition",
            msrp_value=773_000.0,
            currency="SEK",
            raw_payload={
                "priceText": "773 000 kr",
                "trimText": "Ultra Black Edition",
            },
        )

        assert black_edition.match_confidence == 0.96
        assert black_edition.official_edition == (
            "Black Edition"
        )
        assert black_edition.official_powertrain == "PHEV"
        assert black_edition.jato_powertrain == "PHEV"
        assert (
            black_edition.match_status
            == "review_required"
        )
        gate = black_edition.match_reason[
            "autoAcceptGate"
        ]
        assert gate["finalStatus"] == "review_required"
        assert gate["tier"] == "constraint_failed"
        assert (
            black_edition.match_reason[
                "structuredFields"
            ]["hasSpecialEdition"]
            is True
        )
        assert (
            black_edition.match_reason[
                "structuredFields"
            ]["specialEditionLabels"]
            == ["Black Edition"]
        )

    def test_auto_accept_gate_requires_powertrain_match(
        self,
    ):
        ext = _make_weighted_rule_extractor(
            fixed_jato_powertrain="BEV",
            base_score=0.24,
        )

        ultra = ext._build_observation(
            official_model="",
            official_trim="Ultra",
            msrp_value=773_000.0,
            currency="SEK",
            raw_payload={
                "priceText": "773 000 kr",
                "trimText": "Ultra",
            },
        )

        assert ultra.match_confidence == 0.96
        assert ultra.official_powertrain == "PHEV"
        assert ultra.jato_powertrain == "PHEV"
        assert ultra.match_status == "review_required"
        gate = ultra.match_reason["autoAcceptGate"]
        assert gate["finalStatus"] == "review_required"
        assert gate["tier"] == "constraint_failed"
        pt_check = next(
            c
            for c in gate["checks"]
            if c["key"] == "powertrain_match"
        )
        assert pt_check == {
            "key": "powertrain_match",
            "passed": False,
            "expected": "BEV",
            "actual": "PHEV",
        }

    def test_below_threshold_always_review(self):
        ext = _make_weighted_rule_extractor()

        ultra = ext._build_observation(
            official_model="",
            official_trim="Ultra",
            msrp_value=773_000.0,
            currency="SEK",
            raw_payload={
                "priceText": "773 000 kr",
                "trimText": "Ultra",
            },
        )

        assert ultra.match_confidence == 0.91
        assert ultra.official_powertrain == "PHEV"
        assert ultra.jato_powertrain == "PHEV"
        assert ultra.match_status == "review_required"
        gate = ultra.match_reason["autoAcceptGate"]
        assert gate["tier"] == "below_threshold"
        assert gate["reviewThreshold"] == 0.95

    def test_semi_auto_accepts_clean_variant(self):
        ext = _make_weighted_rule_extractor(
            base_score=0.24,
        )

        ultra = ext._build_observation(
            official_model="",
            official_trim="Ultra",
            msrp_value=773_000.0,
            currency="SEK",
            raw_payload={
                "priceText": "773 000 kr",
                "trimText": "Ultra",
            },
        )

        assert ultra.match_confidence == 0.96
        assert ultra.official_edition is None
        assert ultra.official_powertrain == "PHEV"
        assert ultra.jato_powertrain == "PHEV"
        assert ultra.match_status == "auto_accepted"
        gate = ultra.match_reason["autoAcceptGate"]
        assert gate["tier"] == "semi_auto"
        assert gate["finalStatus"] == "auto_accepted"

    def test_mhev_detected_from_trim_keywords(self):
        ext = _make_weighted_rule_extractor()

        b5 = ext._build_observation(
            official_model="",
            official_trim="B5 Momentum",
            msrp_value=500_000.0,
            currency="SEK",
            raw_payload={
                "priceText": "500 000 kr",
                "trimText": "B5 Momentum",
            },
        )

        assert b5.official_powertrain == "MHEV"
        assert b5.jato_powertrain == "MHEV"
        assert b5.match_status == "review_required"
        sf = b5.match_reason["structuredFields"]
        assert sf["officialPowertrain"] == "MHEV"
        assert sf["jatoPowertrain"] == "MHEV"
        assert (
            sf["powertrainSource"]
            == "configured_powertrain_trim"
            ":powertrain_xc60_mhev"
        )


class TestModelRuleMapping:
    def test_resolves_jato_model_from_brand_family_rules(self):
        ext = _make_model_rule_extractor()

        observation = ext._build_observation(
            official_model="Nouvelle Renault Clio",
            official_trim="esprit Alpine E-Tech full hybrid",
            msrp_value=28_900.0,
            currency="EUR",
            raw_payload={
                "priceText": "28 900 €",
                "trimText": "esprit Alpine E-Tech full hybrid",
                "name": "Nouvelle Renault Clio",
            },
        )

        assert observation is not None
        assert observation.official_model == "Clio"
        assert observation.jato_model == "Clio"
        assert observation.jato_trim == "esprit Alpine E-Tech full hybrid"
        assert observation.match_confidence == 0.77
        assert (
            observation.match_reason["structuredFields"][
                "modelMappingSource"
            ]
            == "rule:model_clio"
        )
        assert observation.match_reason["structuredFields"][
            "modelMappingKeywords"
        ] == ["clio"]

        applied_components = {
            component["key"]
            for component in observation.match_reason[
                "confidenceRule"
            ]["components"]
            if component["applied"]
        }
        assert "model_rule_match" in applied_components

    def test_skips_unmapped_brand_family_observation_when_required(self):
        ext = _make_model_rule_extractor(
            skip_if_model_unmapped=True,
        )

        observation = ext._build_observation(
            official_model="Renault Rafale",
            official_trim="techno full hybrid",
            msrp_value=41_000.0,
            currency="EUR",
            raw_payload={
                "priceText": "41 000 €",
                "trimText": "techno full hybrid",
                "name": "Renault Rafale",
            },
        )

        assert observation is None


# ── JSON extraction tests ────────────────────────────────────────────

def _make_json_extractor() -> ScraplingExtractor:
    cfg = ExtractorConfig(
        source_code="test_json",
        country="德国",
        brand="TestBrand",
        source_url="https://test.example.com",
    )
    profile = ScraplingProfile(
        url="https://test.example.com/models",
        tier="http",
        json_script_selector="script[type='application/ld+json']",
        json_vehicles_path="offers",
        css=CssMapping(
            vehicle_container=".model-card",
            model=".model-name::text",
        ),
    )
    return ScraplingExtractor(cfg, profile)


def _mock_page_with_json(ld_json: dict) -> MagicMock:
    """Build a mock page with ld+json script tag."""
    page = MagicMock()

    script_el = MagicMock()
    text_result = MagicMock()
    text_result.get.return_value = __import__("json").dumps(ld_json)
    script_el.css.return_value = text_result

    page.css.return_value = [script_el]
    return page


class TestJsonExtraction:
    @patch.object(ScraplingExtractor, "_fetch")
    def test_extracts_from_ld_json(self, mock_fetch):
        ld_json = {
            "offers": [
                {
                    "name": "3 Series",
                    "trim": "320i",
                    "price": 42900,
                    "priceCurrency": "EUR",
                },
                {
                    "name": "X5",
                    "trim": "xDrive40i",
                    "price": 72900,
                    "priceCurrency": "EUR",
                },
            ]
        }
        mock_fetch.return_value = _mock_page_with_json(ld_json)

        ext = _make_json_extractor()
        results = ext.extract()
        assert len(results) == 2
        assert results[0].official_model == "3 Series"
        assert results[0].msrp_value == 42_900.0
        assert results[1].official_model == "X5"

    @patch.object(ScraplingExtractor, "_fetch")
    def test_handles_single_offer_dict(self, mock_fetch):
        ld_json = {
            "offers": {"name": "iX", "price": 85000, "priceCurrency": "EUR"}
        }
        mock_fetch.return_value = _mock_page_with_json(ld_json)

        ext = _make_json_extractor()
        results = ext.extract()
        assert len(results) == 1
        assert results[0].official_model == "iX"

    @patch.object(ScraplingExtractor, "_fetch")
    def test_extracts_vehicle_offers_from_graph_payload(self, mock_fetch):
        ld_json = {
            "@context": "https://schema.org",
            "@graph": [
                {
                    "@type": "WebPage",
                    "name": "Mercedes-Benz EQA",
                },
                {
                    "@type": "Car",
                    "name": "EQA",
                    "offers": [
                        {
                            "@type": "Offer",
                            "description": "EQA 250+",
                            "price": 489000,
                            "priceCurrency": "NOK",
                        },
                        {
                            "@type": "Offer",
                            "description": "EQA 350 4MATIC",
                            "price": 569000,
                            "priceCurrency": "NOK",
                        },
                    ],
                },
            ],
        }
        mock_fetch.return_value = _mock_page_with_json(ld_json)

        ext = _make_json_extractor()
        results = ext.extract()

        assert len(results) == 2
        assert results[0].official_model == "EQA"
        assert results[0].official_trim == "EQA 250+"
        assert results[0].msrp_value == 489000.0
        assert results[1].official_trim == "EQA 350 4MATIC"


# ── Profile / tier tests ─────────────────────────────────────────────

class TestExtractorProperties:
    def test_extractor_version(self):
        ext = _make_css_extractor()
        assert "scrapling" in ext.extractor_version

    def test_profile_tier_default(self):
        profile = ScraplingProfile(url="https://example.com")
        assert profile.tier == "http"

    def test_source_payload(self):
        ext = _make_css_extractor()
        payload = ext.to_source_payload()
        assert payload["source_code"] == "test_css"
        assert payload["brand"] == "TestBrand"
        assert payload["extractor_name"] == "ScraplingExtractor"


# ── Attribute-JSON extraction tests ──────────────────────────────────

_DEFAULT_ATTR_JSON = AttrJsonMapping(
    vehicle_container=".cmp-allmodelscard",
    filter_attr="data-card-filter-info",
    tracking_attr="data-tracking-attributes",
    price_key="price",
    fuel_key="fuelType",
    category_key="category",
    series_key="series",
    name_key="name",
    range_key="range",
)


def _make_attr_json_extractor() -> ScraplingExtractor:
    cfg = ExtractorConfig(
        source_code="test_attr_json",
        country="德国",
        brand="TestBrand",
        source_url="https://test.example.com",
    )
    profile = ScraplingProfile(
        url="https://test.example.com/models",
        tier="http",
        attr_json=_DEFAULT_ATTR_JSON,
        css=CssMapping(
            vehicle_container=".cmp-allmodelscard",
            model=".cmp-allmodelscarddetail__series::text",
        ),
    )
    return ScraplingExtractor(cfg, profile)


def _mock_card(
    filter_json: dict,
    tracking_json: dict,
    series_text: str = "",
    tracking_on_sub: bool = False,
) -> MagicMock:
    """Build a mock card element with data-* attribute JSON."""
    el = MagicMock()
    import json as _json

    base_attrs = {
        "data-card-filter-info": _json.dumps(filter_json),
    }
    if not tracking_on_sub:
        base_attrs["data-tracking-attributes"] = _json.dumps(tracking_json)

    el.attrib = base_attrs

    def _css(selector, _tracking=tracking_json, _series=series_text,
             _tracking_on_sub=tracking_on_sub):
        if "data-tracking-attributes" in selector and _tracking_on_sub:
            sub = MagicMock()
            sub.attrib = {"data-tracking-attributes": _json.dumps(_tracking)}
            return [sub]
        if "data-tracking-attributes" in selector:
            return []
        # Series text selector
        result = MagicMock()
        result.get.return_value = _series
        return result

    el.css = _css
    return el


def _mock_page_with_attr_json(cards: list[MagicMock]) -> MagicMock:
    page = MagicMock()
    page.css.return_value = cards
    return page


class TestAttrJsonExtraction:
    @patch.object(ScraplingExtractor, "_fetch")
    def test_extracts_from_attr_json(self, mock_fetch):
        cards = [
            _mock_card(
                {
                    "price": "83500.0",
                    "fuelType": "e",
                    "series": "x",
                    "category": "suv",
                },
                {"name": "BMW iX xDrive45", "range": "I20"},
                series_text="iX",
            ),
            _mock_card(
                {
                    "price": "126500.0",
                    "fuelType": "e",
                    "series": "x",
                    "category": "suv",
                },
                {"name": "BMW iX M70 xDrive", "range": "I20"},
                series_text="iX",
            ),
        ]
        mock_fetch.return_value = _mock_page_with_attr_json(cards)

        ext = _make_attr_json_extractor()
        results = ext.extract()
        assert len(results) == 2
        assert results[0].official_model == "iX"
        assert results[0].official_trim == "xDrive45"
        assert results[0].msrp_value == 83_500.0
        assert results[1].official_trim == "M70 xDrive"
        assert results[1].msrp_value == 126_500.0

    @patch.object(ScraplingExtractor, "_fetch")
    def test_skips_card_without_price(self, mock_fetch):
        cards = [
            _mock_card(
                {"fuelType": "e", "series": "x"},  # no price
                {"name": "BMW Concept", "range": "C00"},
                series_text="Concept",
            ),
        ]
        mock_fetch.return_value = _mock_page_with_attr_json(cards)

        ext = _make_attr_json_extractor()
        results = ext.extract()
        assert len(results) == 0

    @patch.object(ScraplingExtractor, "_fetch")
    def test_tracking_on_sub_element(self, mock_fetch):
        cards = [
            _mock_card(
                {
                    "price": "70900.0",
                    "fuelType": "e",
                    "series": "x",
                    "category": "suv",
                },
                {"name": "BMW iX3 50 xDrive", "range": "G08"},
                series_text="iX3",
                tracking_on_sub=True,
            ),
        ]
        mock_fetch.return_value = _mock_page_with_attr_json(cards)

        ext = _make_attr_json_extractor()
        results = ext.extract()
        assert len(results) == 1
        assert results[0].official_model == "iX3"
        assert results[0].msrp_value == 70_900.0

    @patch.object(ScraplingExtractor, "_fetch")
    def test_raw_payload_includes_metadata(self, mock_fetch):
        cards = [
            _mock_card(
                {
                    "price": "51000.0",
                    "fuelType": "e",
                    "series": "x",
                    "category": "suv",
                },
                {"name": "BMW iX2 xDrive30", "range": "U10"},
                series_text="iX2",
            ),
        ]
        mock_fetch.return_value = _mock_page_with_attr_json(cards)

        ext = _make_attr_json_extractor()
        results = ext.extract()
        assert len(results) == 1
        assert results[0].raw_payload["fuel_type"] == "e"
        assert results[0].raw_payload["category"] == "suv"
