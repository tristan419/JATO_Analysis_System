#!/usr/bin/env python3
"""Build official evidence leads for Sweden/Switzerland top30 SUV MSRP demo.

The output is a review artifact for the MSRP monitor. It keeps official
price-list baselines and campaign/offer boundaries separate from permanent
MSRP movement conclusions.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = (
    PROJECT_ROOT
    / "03_Scripts"
    / "diagnostics"
    / "artifacts"
    / "msrp_backfill"
    / "sweden_swiss_top30_suv"
)
DEFAULT_TOP30_PATH = DEFAULT_OUTPUT_DIR / "top30_suv_price_movement_candidates.json"
DEFAULT_EVIDENCE_DIR = DEFAULT_OUTPUT_DIR / "evidence"


@dataclass(frozen=True)
class OfficialSource:
    code: str
    country_code: str
    country_label: str
    brand: str
    model: str
    source_url: str
    source_label: str
    local_path: str
    source_type: str
    document_date: str | None
    valid_from: str | None
    valid_until: str | None
    evidence_class: str
    expected_terms: tuple[str, ...]
    entries: tuple[dict[str, Any], ...]


OFFICIAL_SOURCES: tuple[OfficialSource, ...] = (
    OfficialSource(
        code="se_skoda_enyaq_solid_edition",
        country_code="SE",
        country_label="Sweden",
        brand="SKODA",
        model="ENYAQ",
        source_url="https://www.skoda.se/erbjudande/kampanj/erbjudande-enyaq",
        source_label="Skoda Sweden official campaign page + PDF",
        local_path="sweden_2026/skoda_enyaq_solid_edition_prislista_2026-06-17.pdf",
        source_type="official_campaign_pdf",
        document_date="2026-06-17",
        valid_from="2026-06-17",
        valid_until="2026-09-30",
        evidence_class="already_backfilled_live",
        expected_terms=("Enyaq", "Solid Edition", "599 500", "Prislista 17 juni"),
        entries=(
            {
                "trim": "Solid Edition",
                "powertrain": "BEV",
                "oldSourceMsrp": 619800,
                "currentSourceMsrp": 599500,
                "currency": "SEK",
                "evidenceRole": "campaign_price_vs_regular_price",
                "readiness": "already_backfilled_live",
            },
        ),
    ),
    OfficialSource(
        code="se_volvo_ex90_ultra_pro",
        country_code="SE",
        country_label="Sweden",
        brand="VOLVO",
        model="EX90",
        source_url="https://www.volvocars.com/se/promotions/",
        source_label="Volvo Sweden official promotions evidence note",
        local_path="sweden_2026/volvo_ex90_ultra_pro_edition_offer_2026-06-23.md",
        source_type="official_promotion_excerpt",
        document_date="2026-06-23",
        valid_from="2026-06-23",
        valid_until=None,
        evidence_class="already_backfilled_live",
        expected_terms=("EX90", "Ultra Pro Edition", "1,099,900", "1,148,800"),
        entries=(
            {
                "trim": "Ultra Pro Edition",
                "powertrain": "BEV",
                "oldSourceMsrp": 1148800,
                "currentSourceMsrp": 1099900,
                "currency": "SEK",
                "evidenceRole": "promotion_price_vs_ordinary_price",
                "readiness": "already_backfilled_live",
            },
        ),
    ),
    OfficialSource(
        code="se_vw_tayron_rline_swe",
        country_code="SE",
        country_label="Sweden",
        brand="VOLKSWAGEN",
        model="TAYRON",
        source_url="https://www.volkswagen.se/sv/kop-en-vw/erbjudanden.html",
        source_label="Volkswagen Sweden official offers + configurator snapshots",
        local_path="sweden_2026/volkswagen_offers_2026-06-24.txt",
        source_type="official_campaign_savings_snapshot",
        document_date="2026-06-24",
        valid_from="2026-06-24",
        valid_until=None,
        evidence_class="already_backfilled_live",
        expected_terms=("Tayron", "R-Line SWE Edition", "100 100"),
        entries=(
            {
                "trim": "R-Line SWE Edition",
                "powertrain": "UNKNOWN",
                "oldSourceMsrp": 711500,
                "currentSourceMsrp": 611400,
                "currency": "SEK",
                "evidenceRole": "campaign_savings_vs_current_price",
                "readiness": "already_backfilled_live",
            },
        ),
    ),
    OfficialSource(
        code="se_kia_ev3_current",
        country_code="SE",
        country_label="Sweden",
        brand="KIA",
        model="EV3",
        source_url="https://www.kia.com/se/nya-bilar/ev3/upptack/",
        source_label="Kia Sweden EV3 official model page",
        local_path="kia_se_ev3_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Kia EV3", "Rek ca pris från", "434 900 kr"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 434900,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_kia_sportage_current",
        country_code="SE",
        country_label="Sweden",
        brand="KIA",
        model="SPORTAGE",
        source_url="https://www.kia.com/se/nya-bilar/sportage/upptack/",
        source_label="Kia Sweden Sportage official model page",
        local_path="kia_se_sportage_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Sportage Plug-In Hybrid", "Rek. ca pris från", "508 900 kr"),
        entries=(
            {
                "trim": "Plug-In Hybrid from price",
                "powertrain": "PHEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 508900,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_kia_ev9_current",
        country_code="SE",
        country_label="Sweden",
        brand="KIA",
        model="EV9",
        source_url="https://www.kia.com/se/nya-bilar/ev9/upptack/",
        source_label="Kia Sweden EV9 official model page",
        local_path="kia_se_ev9_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("EV9", "Rek. ca pris från", "701 900 kr"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 701900,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_kia_ev6_current",
        country_code="SE",
        country_label="Sweden",
        brand="KIA",
        model="EV6",
        source_url="https://www.kia.com/se/nya-bilar/ev6/upptack/",
        source_label="Kia Sweden EV6 official model page",
        local_path="kia_se_ev6_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("EV6", "Rek. ca pris från", "596 400 kr"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 596400,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_toyota_rav4_current",
        country_code="SE",
        country_label="Sweden",
        brand="TOYOTA",
        model="RAV4",
        source_url="https://www.toyota.se/bilar/rav4",
        source_label="Toyota Sweden RAV4 official model page",
        local_path="toyota_se_rav4_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Nya RAV4", "Från 459 900 kr", "HYBRID & LADDHYBRID"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "HEV/PHEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 459900,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_toyota_yaris_cross_current",
        country_code="SE",
        country_label="Sweden",
        brand="TOYOTA",
        model="YARIS CROSS",
        source_url="https://www.toyota.se/bilar/yaris-cross",
        source_label="Toyota Sweden Yaris Cross official model page",
        local_path="toyota_se_yaris_cross_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Yaris Cross", "Hybrid", "Från 295 900 kr"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "HEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 295900,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_toyota_corolla_cross_current",
        country_code="SE",
        country_label="Sweden",
        brand="TOYOTA",
        model="COROLLA CROSS",
        source_url="https://www.toyota.se/bilar/corolla-cross",
        source_label="Toyota Sweden Corolla Cross official model page",
        local_path="toyota_se_corolla_cross_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Corolla Cross", "Hybrid", "Från 360 900 kr"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "HEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 360900,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_toyota_chr_current",
        country_code="SE",
        country_label="Sweden",
        brand="TOYOTA",
        model="C-HR",
        source_url="https://www.toyota.se/bilar/c-hr",
        source_label="Toyota Sweden C-HR official model page",
        local_path="toyota_se_chr_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Toyota C-HR", "Hybrid & Laddhybrid", "Från 350 900 kr"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "HEV/PHEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 350900,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_skoda_kodiaq_explore_offer",
        country_code="SE",
        country_label="Sweden",
        brand="SKODA",
        model="KODIAQ",
        source_url="https://www.skoda.se/erbjudande/kampanj/erbjudande-kodiaq",
        source_label="Skoda Sweden Kodiaq Explore official campaign page",
        local_path="skoda_se_kodiaq_explore_offer_2026-07-02.html",
        source_type="official_campaign_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_offer_boundary",
        expected_terms=("Kodiaq Explore", "399 900", "447 800", "Privatleasing från 4 430"),
        entries=(
            {
                "trim": "Explore from price",
                "powertrain": "UNKNOWN",
                "oldSourceMsrp": 447800,
                "currentSourceMsrp": 399900,
                "currency": "SEK",
                "evidenceRole": "campaign_price_vs_ordinary_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="se_vw_tiguan_rline_offer_signal",
        country_code="SE",
        country_label="Sweden",
        brand="VOLKSWAGEN",
        model="TIGUAN",
        source_url="https://www.volkswagen.se/sv/modeller/tiguan.html",
        source_label="Volkswagen Sweden Tiguan official model offer page",
        local_path="volkswagen_se_tiguan_model_page_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until="2026-09-30",
        evidence_class="official_offer_signal",
        expected_terms=("R-Line SWE Edition", "409 900", "115 300", "2026-09-30"),
        entries=(
            {
                "trim": "R-Line SWE Edition",
                "powertrain": "UNKNOWN",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 409900,
                "currency": "SEK",
                "evidenceRole": "model_offer_price_with_max_savings",
                "readiness": "offer_signal_only",
                "maximumSavingsLocal": 115300,
            },
        ),
    ),
    OfficialSource(
        code="se_vw_troc_current",
        country_code="SE",
        country_label="Sweden",
        brand="VOLKSWAGEN",
        model="T-ROC",
        source_url="https://www.volkswagen.se/sv/modeller/t-roc.html",
        source_label="Volkswagen Sweden T-Roc official model page",
        local_path="volkswagen_se_troc_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("T-Roc", "Pris från", "389 900"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "UNKNOWN",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 389900,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_volvo_ex30_current",
        country_code="SE",
        country_label="Sweden",
        brand="VOLVO",
        model="EX30",
        source_url="https://www.volvocars.com/se/cars/ex30-electric/",
        source_label="Volvo Sweden EX30 official model page extract",
        local_path="volvo_se_ex30_model_page_web_extract_2026-07-02.md",
        source_type="official_model_page_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("EX30", "Rek. pris från 429 000 kr", "Leasing från 3 895 kr/mån"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 429000,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_tesla_model_y_current",
        country_code="SE",
        country_label="Sweden",
        brand="TESLA",
        model="MODEL Y",
        source_url="https://www.tesla.com/sv_SE/modely/design",
        source_label="Tesla Sweden Model Y official design page extract",
        local_path="sweden_missing_top30_official_web_extracts_2026-07-02.md",
        source_type="official_configurator_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Model Y Bakhjulsdrift", "490 810 kr", "Pearl White"),
        entries=(
            {
                "trim": "Bakhjulsdrift",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 490810,
                "currency": "SEK",
                "evidenceRole": "current_configurator_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_polestar_4_current",
        country_code="SE",
        country_label="Sweden",
        brand="POLESTAR",
        model="4",
        source_url="https://www.polestar.com/se/polestar-4-models/polestar-4-coupe/specifications/",
        source_label="Polestar Sweden Polestar 4 official specifications page",
        local_path="polestar_se_4_specifications_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Polestar 4", "Pris från", "619 000 kr"),
        entries=(
            {
                "trim": "Rear motor from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 619000,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_vw_id4_current",
        country_code="SE",
        country_label="Sweden",
        brand="VOLKSWAGEN",
        model="ID.4",
        source_url="https://www.volkswagen.se/sv/modeller/id4.html",
        source_label="Volkswagen Sweden ID.4 official model page",
        local_path="volkswagen_se_id4_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Volkswagen ID.4", "Pris från", "538 700 kr"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 538700,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_cupra_terramar_current_offer",
        country_code="SE",
        country_label="Sweden",
        brand="CUPRA",
        model="TERRAMAR",
        source_url="https://www.cupraofficial.se/kopa/erbjudande",
        source_label="Cupra Sweden official offers page",
        local_path="cupra_se_offers_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_offer_signal",
        expected_terms=("CUPRA Terramar", "Pris från", "442.900 kr"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "MHEV/PHEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 442900,
                "currency": "SEK",
                "evidenceRole": "current_offer_page_from_price",
                "readiness": "offer_signal_only",
            },
        ),
    ),
    OfficialSource(
        code="se_volvo_xc40_current",
        country_code="SE",
        country_label="Sweden",
        brand="VOLVO",
        model="XC40",
        source_url="https://www.volvocars.com/se/cars/xc40/",
        source_label="Volvo Sweden XC40 official model page extract",
        local_path="sweden_missing_top30_official_web_extracts_2026-07-02.md",
        source_type="official_model_page_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("XC40", "Rek. pris från 430 000 kr", "Mildhybrid / Motor"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "MHEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 430000,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_volvo_xc90_current",
        country_code="SE",
        country_label="Sweden",
        brand="VOLVO",
        model="XC90",
        source_url="https://www.volvocars.com/se/cars/xc90-hybrid/",
        source_label="Volvo Sweden XC90 official model page extract",
        local_path="sweden_missing_top30_official_web_extracts_2026-07-02.md",
        source_type="official_model_page_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("XC90", "Rek. pris från 994 000 kr", "Leasing från 12 995 kr/mån"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "PHEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 994000,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_bmw_ix1_inventory_offer",
        country_code="SE",
        country_label="Sweden",
        brand="BMW",
        model="IX1",
        source_url="https://hitta.bmw.se/r/U11E-iX1",
        source_label="BMW Sweden iX1 official available-cars offer page",
        local_path="bmw_se_ix1_inventory_offer_2026-07-02.html",
        source_type="official_inventory_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_offer_boundary",
        expected_terms=("iX1 eDrive20", "Rek. ord pris", "613 700 kr", "Kontantpris 499 900 kr"),
        entries=(
            {
                "trim": "eDrive20 inventory offer",
                "powertrain": "BEV",
                "oldSourceMsrp": 613700,
                "currentSourceMsrp": 499900,
                "currency": "SEK",
                "evidenceRole": "recommended_price_vs_inventory_cash_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="se_mercedes_eqa_current",
        country_code="SE",
        country_label="Sweden",
        brand="MERCEDES",
        model="EQA",
        source_url="https://www.mercedes-benz.se/passengercars/models/suv/eqa/overview.html",
        source_label="Mercedes-Benz Sweden EQA official model-page navigation extract",
        local_path="sweden_missing_top30_official_web_extracts_2026-07-02.md",
        source_type="official_model_page_navigation_excerpt",
        document_date="2026-07-03",
        valid_from="2026-07-03",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("EQA", "Från 529 000 kr", "Pris inkl. moms"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 529000,
                "currency": "SEK",
                "evidenceRole": "current_model_page_navigation_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_audi_q4_etron_current",
        country_code="SE",
        country_label="Sweden",
        brand="AUDI",
        model="Q4 E-TRON",
        source_url="https://www.audi.se/sv/models/q4-e-tron/q4-e-tron/",
        source_label="Audi Sweden Q4 e-tron official model page extract",
        local_path="sweden_missing_top30_official_web_extracts_2026-07-02.md",
        source_type="official_model_page_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Q4 e-tron", "från 570 000 kr", "inkl. moms"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 570000,
                "currency": "SEK",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="se_peugeot_3008_edition_offer",
        country_code="SE",
        country_label="Sweden",
        brand="PEUGEOT",
        model="3008",
        source_url="https://www.peugeot.se/kop/erbjudanden/edition.html",
        source_label="Peugeot Sweden Edition official offer page",
        local_path="peugeot_se_edition_offer_web_extract_2026-07-02.md",
        source_type="official_offer_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until="2026-08-31",
        evidence_class="official_offer_signal",
        expected_terms=("3008 Hybrid Edition", "Pris: 399 900 kr", "Spara minst 40 000", "31/8–2026"),
        entries=(
            {
                "trim": "Hybrid Edition",
                "powertrain": "HEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 399900,
                "currency": "SEK",
                "evidenceRole": "edition_offer_price_with_min_savings",
                "readiness": "offer_signal_only",
                "minimumSavingsLocal": 40000,
            },
            {
                "trim": "Plug-In Hybrid Edition",
                "powertrain": "PHEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 479900,
                "currency": "SEK",
                "evidenceRole": "edition_offer_price_with_min_savings",
                "readiness": "offer_signal_only",
                "minimumSavingsLocal": 40000,
            },
            {
                "trim": "E-3008 Edition",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 499900,
                "currency": "SEK",
                "evidenceRole": "edition_offer_price_with_min_savings",
                "readiness": "offer_signal_only",
                "minimumSavingsLocal": 40000,
            },
        ),
    ),
    OfficialSource(
        code="se_peugeot_2008_edition_offer",
        country_code="SE",
        country_label="Sweden",
        brand="PEUGEOT",
        model="2008",
        source_url="https://www.peugeot.se/kop/erbjudanden/edition.html",
        source_label="Peugeot Sweden Edition official offer page",
        local_path="peugeot_se_edition_offer_web_extract_2026-07-02.md",
        source_type="official_offer_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until="2026-08-31",
        evidence_class="official_offer_signal",
        expected_terms=("2008 Edition", "Pris: 244.900 kr", "E-2008 Edition", "389 900 kr"),
        entries=(
            {
                "trim": "Edition",
                "powertrain": "ICE",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 244900,
                "currency": "SEK",
                "evidenceRole": "edition_offer_price_with_min_savings",
                "readiness": "offer_signal_only",
                "minimumSavingsLocal": 40000,
            },
            {
                "trim": "E-2008 Edition",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 389900,
                "currency": "SEK",
                "evidenceRole": "edition_offer_price_with_min_savings",
                "readiness": "offer_signal_only",
                "minimumSavingsLocal": 40000,
            },
        ),
    ),
    OfficialSource(
        code="se_peugeot_5008_edition_offer",
        country_code="SE",
        country_label="Sweden",
        brand="PEUGEOT",
        model="5008",
        source_url="https://www.peugeot.se/kop/erbjudanden/edition.html",
        source_label="Peugeot Sweden Edition official offer page",
        local_path="peugeot_se_edition_offer_web_extract_2026-07-02.md",
        source_type="official_offer_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until="2026-08-31",
        evidence_class="official_offer_signal",
        expected_terms=("5008 Hybrid Edition", "Pris: 439 900 kr", "E-5008 Edition", "539 900 kr"),
        entries=(
            {
                "trim": "Hybrid Edition",
                "powertrain": "HEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 439900,
                "currency": "SEK",
                "evidenceRole": "edition_offer_price_with_min_savings",
                "readiness": "offer_signal_only",
                "minimumSavingsLocal": 40000,
            },
            {
                "trim": "Plug-In Hybrid Edition",
                "powertrain": "PHEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 519900,
                "currency": "SEK",
                "evidenceRole": "edition_offer_price_with_min_savings",
                "readiness": "offer_signal_only",
                "minimumSavingsLocal": 40000,
            },
            {
                "trim": "E-5008 Edition",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 539900,
                "currency": "SEK",
                "evidenceRole": "edition_offer_price_with_min_savings",
                "readiness": "offer_signal_only",
                "minimumSavingsLocal": 40000,
            },
        ),
    ),
    OfficialSource(
        code="se_volvo_xc60_nordic_edition",
        country_code="SE",
        country_label="Sweden",
        brand="VOLVO",
        model="XC60",
        source_url="https://www.volvocars.com/se/promotions/",
        source_label="Volvo Sweden official promotions top30 extract",
        local_path="volvo_se_promotions_top30_web_extract_2026-07-02.md",
        source_type="official_promotion_excerpt",
        document_date="2026-07-02",
        valid_from=None,
        valid_until=None,
        evidence_class="official_offer_boundary",
        expected_terms=("XC60 Plus Black Nordic Edition", "619 900", "751 900"),
        entries=(
            {
                "trim": "Plus Black Nordic Edition",
                "powertrain": "MHEV/PHEV",
                "oldSourceMsrp": 751900,
                "currentSourceMsrp": 619900,
                "currency": "SEK",
                "evidenceRole": "promotion_price_vs_ordinary_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="se_volvo_ex40_special_editions",
        country_code="SE",
        country_label="Sweden",
        brand="VOLVO",
        model="EX40",
        source_url="https://www.volvocars.com/se/promotions/",
        source_label="Volvo Sweden official promotions top30 extract",
        local_path="volvo_se_promotions_top30_web_extract_2026-07-02.md",
        source_type="official_promotion_excerpt",
        document_date="2026-07-02",
        valid_from=None,
        valid_until=None,
        evidence_class="official_offer_boundary",
        expected_terms=(
            "EX40 Ultra Black Special Edition",
            "609 900",
            "665 900",
            "EX40 Plus Black Special Edition",
            "589 900",
            "628 900",
        ),
        entries=(
            {
                "trim": "Ultra Black Special Edition",
                "powertrain": "BEV",
                "oldSourceMsrp": 665900,
                "currentSourceMsrp": 609900,
                "currency": "SEK",
                "evidenceRole": "promotion_price_vs_ordinary_price",
                "readiness": "demo_offer_boundary",
            },
            {
                "trim": "Plus Black Special Edition",
                "powertrain": "BEV",
                "oldSourceMsrp": 628900,
                "currentSourceMsrp": 589900,
                "currency": "SEK",
                "evidenceRole": "promotion_price_vs_ordinary_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="se_volvo_ec40_special_editions",
        country_code="SE",
        country_label="Sweden",
        brand="VOLVO",
        model="EC40",
        source_url="https://www.volvocars.com/se/promotions/",
        source_label="Volvo Sweden official promotions top30 extract",
        local_path="volvo_se_promotions_top30_web_extract_2026-07-02.md",
        source_type="official_promotion_excerpt",
        document_date="2026-07-02",
        valid_from=None,
        valid_until=None,
        evidence_class="official_offer_boundary",
        expected_terms=(
            "EC40 Ultra Black Special Edition",
            "677 900",
            "609 900",
            "EC40 Plus Black Special Edition",
            "589 900",
            "646 900",
        ),
        entries=(
            {
                "trim": "Ultra Black Special Edition",
                "powertrain": "BEV",
                "oldSourceMsrp": 677900,
                "currentSourceMsrp": 609900,
                "currency": "SEK",
                "evidenceRole": "promotion_price_vs_ordinary_price",
                "readiness": "demo_offer_boundary",
            },
            {
                "trim": "Plus Black Special Edition",
                "powertrain": "BEV",
                "oldSourceMsrp": 646900,
                "currentSourceMsrp": 589900,
                "currency": "SEK",
                "evidenceRole": "promotion_price_vs_ordinary_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_vw_tiguan_united",
        country_code="CH",
        country_label="Switzerland",
        brand="VOLKSWAGEN",
        model="TIGUAN",
        source_url="https://www.volkswagen.ch/de/modelle/die-sondermodelle-united.html",
        source_label="Volkswagen Switzerland UNITED official offer page",
        local_path="volkswagen_ch_united_offer_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-01",
        valid_until="2026-08-31",
        evidence_class="official_offer_boundary",
        expected_terms=("Tiguan UNITED Life", "58’180", "53’100", "01.07.2026", "31.08.2026"),
        entries=(
            {
                "trim": "UNITED Life 2.0 TSI",
                "powertrain": "ICE",
                "oldSourceMsrp": 58180,
                "currentSourceMsrp": 53100,
                "currency": "CHF",
                "evidenceRole": "regular_price_vs_special_model_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_volvo_xc60_aurora_bonus",
        country_code="CH",
        country_label="Switzerland",
        brand="VOLVO",
        model="XC60",
        source_url="https://www.volvocars.com/de-ch/cars/xc60-hybrid/",
        source_label="Volvo Switzerland XC60 official offer extract",
        local_path="volvo_ch_xc60_web_extract_2026-07-02.md",
        source_type="official_offer_excerpt",
        document_date="2026-07-02",
        valid_from=None,
        valid_until="2026-06-30",
        evidence_class="official_offer_boundary_expired",
        expected_terms=("Aurora Bonus", "CHF 7'000", "CHF 59'700", "CHF 66'700"),
        entries=(
            {
                "trim": "B5 AWD Mild Hybrid Essential",
                "powertrain": "MHEV",
                "oldSourceMsrp": 66700,
                "currentSourceMsrp": 59700,
                "currency": "CHF",
                "evidenceRole": "catalog_price_vs_aurora_bonus_price",
                "readiness": "expired_demo_offer_boundary",
            },
            {
                "trim": "Plug-in Hybrid from price",
                "powertrain": "PHEV",
                "oldSourceMsrp": 79900,
                "currentSourceMsrp": 72900,
                "currency": "CHF",
                "evidenceRole": "catalog_price_vs_aurora_bonus_price",
                "readiness": "expired_demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_kia_sportage_black_edition",
        country_code="CH",
        country_label="Switzerland",
        brand="KIA",
        model="SPORTAGE",
        source_url="https://www.kia.ch/de/modelle/neuer-sportage",
        source_label="Kia Switzerland Sportage official offer page",
        local_path="kia_ch_sportage_offer_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from=None,
        valid_until="2026-06-30",
        evidence_class="official_offer_boundary_expired",
        expected_terms=("Sportage Black Edition", "46'950", "5890", "30.6.26"),
        entries=(
            {
                "trim": "Black Edition 1.6 T-GDi HEV",
                "powertrain": "HEV",
                "oldSourceMsrp": 52840,
                "currentSourceMsrp": 46950,
                "currency": "CHF",
                "evidenceRole": "price_advantage_vs_offer_price",
                "readiness": "expired_demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_vw_tayron_united",
        country_code="CH",
        country_label="Switzerland",
        brand="VOLKSWAGEN",
        model="TAYRON",
        source_url="https://www.volkswagen.ch/de/modelle/die-sondermodelle-united.html",
        source_label="Volkswagen Switzerland UNITED official offer page",
        local_path="volkswagen_ch_united_offer_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-01",
        valid_until="2026-08-31",
        evidence_class="official_offer_boundary",
        expected_terms=("Tayron UNITED R-Line", "64’560", "62’800", "01.07.2026", "31.08.2026"),
        entries=(
            {
                "trim": "UNITED R-Line 2.0 TSI",
                "powertrain": "ICE",
                "oldSourceMsrp": 64560,
                "currentSourceMsrp": 62800,
                "currency": "CHF",
                "evidenceRole": "regular_price_vs_special_model_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_dacia_duster_current",
        country_code="CH",
        country_label="Switzerland",
        brand="DACIA",
        model="DUSTER",
        source_url="https://de.dacia.ch/hybrid-and-electric-range/duster-suv.html",
        source_label="Dacia Switzerland Duster official model page",
        local_path="dacia_ch_duster_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("DUSTER", "Preis ab CHF", "24’990", "DUSTER extreme mild hybrid 140"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "MHEV/HEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 24990,
                "currency": "CHF",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_dacia_bigster_current",
        country_code="CH",
        country_label="Switzerland",
        brand="DACIA",
        model="BIGSTER",
        source_url="https://de.dacia.ch/hybrid-and-electric-range/bigster.html",
        source_label="Dacia Switzerland Bigster official model page",
        local_path="dacia_ch_bigster_model_page_2026-07-02.html",
        source_type="official_model_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("BIGSTER", "Preis ab CHF", "27’990", "BIGSTER journey mild hybrid 140"),
        entries=(
            {
                "trim": "from price",
                "powertrain": "MHEV/HEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 27990,
                "currency": "CHF",
                "evidenceRole": "current_model_page_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_cupra_terramar_prime_offer",
        country_code="CH",
        country_label="Switzerland",
        brand="CUPRA",
        model="TERRAMAR",
        source_url="https://www.cupraofficial.ch/de/angebote/sonderangebote",
        source_label="Cupra Switzerland special offers official page",
        local_path="cupra_ch_special_offers_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_offer_boundary",
        expected_terms=("CUPRA Terramar VZ PRIME EDITION", "Regulärer Preis", "70'150", "58'850"),
        entries=(
            {
                "trim": "VZ PRIME EDITION",
                "powertrain": "ICE",
                "oldSourceMsrp": 70150,
                "currentSourceMsrp": 58850,
                "currency": "CHF",
                "evidenceRole": "regular_price_vs_special_offer_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_cupra_formentor_prime_offer",
        country_code="CH",
        country_label="Switzerland",
        brand="CUPRA",
        model="FORMENTOR",
        source_url="https://www.cupraofficial.ch/de/angebote/sonderangebote",
        source_label="Cupra Switzerland special offers official page",
        local_path="cupra_ch_special_offers_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_offer_boundary",
        expected_terms=("CUPRA Formentor PRIME EDITION", "Regulärer Preis", "53'150", "40'350"),
        entries=(
            {
                "trim": "PRIME EDITION",
                "powertrain": "ICE",
                "oldSourceMsrp": 53150,
                "currentSourceMsrp": 40350,
                "currency": "CHF",
                "evidenceRole": "regular_price_vs_special_offer_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_volvo_ex30_fjord_bonus",
        country_code="CH",
        country_label="Switzerland",
        brand="VOLVO",
        model="EX30",
        source_url="https://www.volvocars.com/de-ch/cars/ex30-electric/",
        source_label="Volvo Switzerland EX30 official offer extract",
        local_path="volvo_ch_ex30_fjord_bonus_web_extract_2026-07-02.md",
        source_type="official_promotion_excerpt",
        document_date="2026-07-02",
        valid_from=None,
        valid_until="2026-06-30",
        evidence_class="official_offer_boundary_expired",
        expected_terms=("Fjord Bonus CHF 5'000", "Katalogpreis CHF 38'250", "CHF 33'250", "30. Juni"),
        entries=(
            {
                "trim": "P5 Core",
                "powertrain": "BEV",
                "oldSourceMsrp": 38250,
                "currentSourceMsrp": 33250,
                "currency": "CHF",
                "evidenceRole": "catalog_price_vs_fjord_bonus_price",
                "readiness": "expired_demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_vw_troc_offer_signal",
        country_code="CH",
        country_label="Switzerland",
        brand="VOLKSWAGEN",
        model="T-ROC",
        source_url="https://www.volkswagen.ch/de/beratung-und-kauf/praemien.html",
        source_label="Volkswagen Switzerland premiums official page",
        local_path="volkswagen_ch_premiums_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-01",
        valid_until="2026-08-31",
        evidence_class="official_offer_signal",
        expected_terms=("New T-Roc", "Volkswagen Prämie", "2000.-", "Advantage-Prämie", "1500.-"),
        entries=(
            {
                "trim": "New T-Roc premium",
                "powertrain": "UNKNOWN",
                "oldSourceMsrp": None,
                "currentSourceMsrp": None,
                "currency": "CHF",
                "evidenceRole": "premium_and_advantage_offer_signal",
                "readiness": "offer_signal_only",
                "minimumSavingsLocal": 3500,
            },
        ),
    ),
    OfficialSource(
        code="ch_tesla_model_y_current",
        country_code="CH",
        country_label="Switzerland",
        brand="TESLA",
        model="MODEL Y",
        source_url="https://www.tesla.com/de_CH/modely/design",
        source_label="Tesla Switzerland Model Y official design page extract",
        local_path="tesla_ch_model_y_web_extract_2026-07-02.md",
        source_type="official_configurator_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Model Y Hinterradantrieb", "CHF 40'990", "Pearl White-Lackierung"),
        entries=(
            {
                "trim": "Hinterradantrieb",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 40990,
                "currency": "CHF",
                "evidenceRole": "current_configurator_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_bmw_x1_current",
        country_code="CH",
        country_label="Switzerland",
        brand="BMW",
        model="X1",
        source_url="https://www.bmw.ch/de/bmw-neuwagen.html",
        source_label="BMW Switzerland official model overview extract",
        local_path="bmw_ch_models_web_extract_2026-07-02.md",
        source_type="official_model_overview_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("SUV X1 Modelle Benzin", "Ab CHF 49'900", "BMW SWISS BONUS"),
        entries=(
            {
                "trim": "Benzin from price",
                "powertrain": "ICE",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 49900,
                "currency": "CHF",
                "evidenceRole": "current_model_overview_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_bmw_x3_current",
        country_code="CH",
        country_label="Switzerland",
        brand="BMW",
        model="X3",
        source_url="https://www.bmw.ch/de/bmw-neuwagen.html",
        source_label="BMW Switzerland official model overview extract",
        local_path="bmw_ch_models_web_extract_2026-07-02.md",
        source_type="official_model_overview_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("SUV X3 Modelle Benzin", "Ab CHF 69'600", "BMW SWISS BONUS"),
        entries=(
            {
                "trim": "Benzin from price",
                "powertrain": "ICE",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 69600,
                "currency": "CHF",
                "evidenceRole": "current_model_overview_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_bmw_x5_current",
        country_code="CH",
        country_label="Switzerland",
        brand="BMW",
        model="X5",
        source_url="https://www.bmw.ch/de/bmw-neuwagen.html",
        source_label="BMW Switzerland official model overview extract",
        local_path="bmw_ch_models_web_extract_2026-07-02.md",
        source_type="official_model_overview_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("SUV X5 Modelle Diesel", "Ab CHF 106'800", "BMW SWISS BONUS"),
        entries=(
            {
                "trim": "Diesel from price",
                "powertrain": "ICE",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 106800,
                "currency": "CHF",
                "evidenceRole": "current_model_overview_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_bmw_ix1_current",
        country_code="CH",
        country_label="Switzerland",
        brand="BMW",
        model="IX1",
        source_url="https://www.bmw.ch/de/bmw-neuwagen.html",
        source_label="BMW Switzerland official model overview extract",
        local_path="bmw_ch_models_web_extract_2026-07-02.md",
        source_type="official_model_overview_excerpt",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("BMW iX1 xDrive30", "Ab CHF 52'200", "BMW SWISS BONUS"),
        entries=(
            {
                "trim": "xDrive30 from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 52200,
                "currency": "CHF",
                "evidenceRole": "current_model_overview_from_price",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_mercedes_glc_current",
        country_code="CH",
        country_label="Switzerland",
        brand="MERCEDES",
        model="GLC",
        source_url="https://www.mercedes-benz.ch/de/passengercars/models/suv/glc/overview.html",
        source_label="Mercedes-Benz Switzerland GLC official model page",
        local_path="mercedes_ch_suv_official_page_extracts_2026-07-03.md",
        source_type="official_model_page_excerpt",
        document_date="2026-07-03",
        valid_from="2026-07-03",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("GLC", "UVP CHF 65'900.-", "GLC 200 d 4MATIC"),
        entries=(
            {
                "trim": "GLC 200 d 4MATIC from price",
                "powertrain": "MHEV diesel",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 65900,
                "currency": "CHF",
                "evidenceRole": "current_model_page_uvp",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_mercedes_gla_current",
        country_code="CH",
        country_label="Switzerland",
        brand="MERCEDES",
        model="GLA",
        source_url="https://www.mercedes-benz.ch/de/passengercars/models/suv/gla/overview.html",
        source_label="Mercedes-Benz Switzerland GLA official model page",
        local_path="mercedes_ch_suv_official_page_extracts_2026-07-03.md",
        source_type="official_model_page_excerpt",
        document_date="2026-07-03",
        valid_from="2026-07-03",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("GLA", "UVP CHF 50'900.-", "GLA 250 e with EQ Hybrid Technology"),
        entries=(
            {
                "trim": "GLA 250 e EQ Star from price",
                "powertrain": "PHEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 50900,
                "currency": "CHF",
                "evidenceRole": "current_model_page_uvp",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_mercedes_gle_current",
        country_code="CH",
        country_label="Switzerland",
        brand="MERCEDES",
        model="GLE",
        source_url="https://www.mercedes-benz.ch/de/passengercars/models/suv/gle/overview.html",
        source_label="Mercedes-Benz Switzerland GLE official model page",
        local_path="mercedes_ch_suv_official_page_extracts_2026-07-03.md",
        source_type="official_model_page_excerpt",
        document_date="2026-07-03",
        valid_from="2026-07-03",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("GLE", "UVP CHF 108'200.-", "GLE 350 d 4MATIC"),
        entries=(
            {
                "trim": "GLE 350 d 4MATIC from price",
                "powertrain": "MHEV diesel",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 108200,
                "currency": "CHF",
                "evidenceRole": "current_model_page_uvp",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_hyundai_tucson_promotion",
        country_code="CH",
        country_label="Switzerland",
        brand="HYUNDAI",
        model="TUCSON",
        source_url="https://hyundai.ch/de/promotionen/",
        source_label="Hyundai Switzerland official promotions page",
        local_path="hyundai_ch_promotions_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_offer_signal",
        expected_terms=("TUCSON Black Line", "CHF 39'750", "Preislisten"),
        entries=(
            {
                "trim": "Black Line",
                "powertrain": "UNKNOWN",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 39750,
                "currency": "CHF",
                "evidenceRole": "current_offer_page_from_price",
                "readiness": "offer_signal_only",
            },
        ),
    ),
    OfficialSource(
        code="ch_hyundai_kona_promotion",
        country_code="CH",
        country_label="Switzerland",
        brand="HYUNDAI",
        model="KONA",
        source_url="https://hyundai.ch/de/promotionen/",
        source_label="Hyundai Switzerland official promotions page",
        local_path="hyundai_ch_promotions_2026-07-02.html",
        source_type="official_offer_page",
        document_date="2026-07-02",
        valid_from="2026-07-02",
        valid_until=None,
        evidence_class="official_offer_signal",
        expected_terms=("KONA Ab CHF 42'850", "Lagerprämie", "KONA Electric Ab CHF 46'800"),
        entries=(
            {
                "trim": "KONA offer from price",
                "powertrain": "UNKNOWN",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 42850,
                "currency": "CHF",
                "evidenceRole": "offer_price_with_inventory_bonus",
                "readiness": "offer_signal_only",
                "maximumSavingsLocal": 5500,
            },
            {
                "trim": "KONA Electric offer from price",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 46800,
                "currency": "CHF",
                "evidenceRole": "offer_price_with_inventory_bonus",
                "readiness": "offer_signal_only",
                "maximumSavingsLocal": 5000,
            },
        ),
    ),
    OfficialSource(
        code="ch_audi_q3_current",
        country_code="CH",
        country_label="Switzerland",
        brand="AUDI",
        model="Q3",
        source_url="https://emea-dam.audi.com/adobe/assets/urn%3Aaaid%3Aaem%3A973adfb6-d610-470b-a6f5-400363dbe581/original/as/Audi_Q3_Preisliste_FR.pdf",
        source_label="Audi Switzerland Q3 official price list PDF",
        local_path="audi_ch_q3_preisliste_fr_2026.pdf",
        source_type="official_price_list_pdf",
        document_date="2026-03-01",
        valid_from="2026-03-01",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Audi Q3 SUV", "Gültig ab 01.03.2026", "49 200"),
        entries=(
            {
                "trim": "TFSI 110 kW",
                "powertrain": "ICE",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 49200,
                "currency": "CHF",
                "evidenceRole": "current_price_list_baseline",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_audi_q3_sportback_current",
        country_code="CH",
        country_label="Switzerland",
        brand="AUDI",
        model="Q3 SPORTBACK",
        source_url="https://emea-dam.audi.com/adobe/assets/urn%3Aaaid%3Aaem%3A973adfb6-d610-470b-a6f5-400363dbe581/original/as/Audi_Q3_Preisliste_FR.pdf",
        source_label="Audi Switzerland Q3 Sportback official price list PDF",
        local_path="audi_ch_q3_preisliste_fr_2026.pdf",
        source_type="official_price_list_pdf",
        document_date="2026-03-01",
        valid_from="2026-03-01",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Audi Q3 Sportback", "Gültig ab 01.03.2026", "50 650"),
        entries=(
            {
                "trim": "TFSI 110 kW",
                "powertrain": "ICE",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 50650,
                "currency": "CHF",
                "evidenceRole": "current_price_list_baseline",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_audi_q5_current",
        country_code="CH",
        country_label="Switzerland",
        brand="AUDI",
        model="Q5",
        source_url="https://emea-dam.audi.com/adobe/assets/urn%3Aaaid%3Aaem%3A215d39eb-b541-4734-bdef-5c5e23b1bd11/original/as/Audi_Q5_SQ5_Preisliste_MJ26_ab_01.01.26_DE.pdf",
        source_label="Audi Switzerland Q5 official price list PDF",
        local_path="audi_ch_q5_sq5_preisliste_mj26_2026-01-01.pdf",
        source_type="official_price_list_pdf",
        document_date="2026-01-01",
        valid_from="2026-01-01",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Audi Q5 SUV", "Gültig ab 01.01.2026", "68 800"),
        entries=(
            {
                "trim": "TFSI quattro 150 kW",
                "powertrain": "ICE",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 68800,
                "currency": "CHF",
                "evidenceRole": "current_price_list_baseline",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_audi_q4_etron_current",
        country_code="CH",
        country_label="Switzerland",
        brand="AUDI",
        model="Q4 E-TRON",
        source_url="https://media.audi.com/is/content/audi/country/ch/assets/preislisten/mj26/Audi_Q4_e-tron_inkl_Sportback_Preisliste_MJ26_12.05.25_DE.pdf",
        source_label="Audi Switzerland Q4 e-tron official price list PDF",
        local_path="audi_ch_q4_etron_preisliste_mj26_2025-05-12.pdf",
        source_type="official_price_list_pdf",
        document_date="2025-05-12",
        valid_from="2025-05-12",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Q4 e-tron", "Gültig ab 12.05.2025", "56 900"),
        entries=(
            {
                "trim": "45 Heck",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 56900,
                "currency": "CHF",
                "evidenceRole": "current_price_list_baseline",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_toyota_yaris_cross_current",
        country_code="CH",
        country_label="Switzerland",
        brand="TOYOTA",
        model="YARIS CROSS",
        source_url="https://de.toyota.ch/content/dam/toyota/nmsc/switzerland/pdf_preislisten/PL_Yaris_Cross_de_01_01_26.pdf",
        source_label="Toyota Switzerland Yaris Cross official price list PDF",
        local_path="toyota_ch_yaris_cross_preisliste_2026-01-01.pdf",
        source_type="official_price_list_pdf",
        document_date="2026-01-01",
        valid_from="2026-01-01",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Yaris Cross", "Gültig ab 01.01.2026", "30'900"),
        entries=(
            {
                "trim": "1.5l Comfort e-Multidrive",
                "powertrain": "HEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 30900,
                "currency": "CHF",
                "evidenceRole": "current_price_list_baseline",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_skoda_kodiaq_dynamic",
        country_code="CH",
        country_label="Switzerland",
        brand="SKODA",
        model="KODIAQ",
        source_url="https://pageflip.ch/skoda/de/preislisten/d_pl_newkodiaq/d_pl_newkodiaq.pdf",
        source_label="Skoda Switzerland Kodiaq price list PDF",
        local_path="skoda_ch_kodiaq_preisliste_2026-07.pdf",
        source_type="official_price_list_pdf",
        document_date="2026-07-01",
        valid_from="2026-07-01",
        valid_until="2026-09-30",
        evidence_class="official_offer_boundary",
        expected_terms=("Kodiaq", "gültig ab 01.07.2026", "Sparvorteil", "Endpreis 52’570"),
        entries=(
            {
                "trim": "Dynamic 2.0 TSI 4x4",
                "powertrain": "ICE",
                "oldSourceMsrp": 60770,
                "currentSourceMsrp": 52570,
                "currency": "CHF",
                "evidenceRole": "list_price_vs_dynamic_end_price",
                "readiness": "demo_offer_boundary",
            },
            {
                "trim": "Dynamic 1.5 TSI PHEV",
                "powertrain": "PHEV",
                "oldSourceMsrp": 61350,
                "currentSourceMsrp": 48650,
                "currency": "CHF",
                "evidenceRole": "list_price_vs_dynamic_end_price",
                "readiness": "demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_skoda_karoq_dynamic",
        country_code="CH",
        country_label="Switzerland",
        brand="SKODA",
        model="KAROQ",
        source_url="https://pageflip.ch/skoda/de/preislisten/d_pl_karoq/d_pl_karoq.pdf",
        source_label="Skoda Switzerland Karoq price list PDF",
        local_path="skoda_ch_karoq_preisliste_2026-05.pdf",
        source_type="official_price_list_pdf",
        document_date="2026-05-08",
        valid_from="2026-04-01",
        valid_until="2026-06-30",
        evidence_class="official_offer_boundary_expired",
        expected_terms=("Karoq", "gültig ab 01.04.2026", "Sparvorteil", "Endpreis 39’380"),
        entries=(
            {
                "trim": "Dynamic 1.5 TSI",
                "powertrain": "ICE",
                "oldSourceMsrp": 48930,
                "currentSourceMsrp": 39380,
                "currency": "CHF",
                "evidenceRole": "list_price_vs_dynamic_end_price",
                "readiness": "expired_demo_offer_boundary",
            },
            {
                "trim": "Dynamic 2.0 TDI 4x4",
                "powertrain": "ICE",
                "oldSourceMsrp": 54610,
                "currentSourceMsrp": 45060,
                "currency": "CHF",
                "evidenceRole": "list_price_vs_dynamic_end_price",
                "readiness": "expired_demo_offer_boundary",
            },
        ),
    ),
    OfficialSource(
        code="ch_skoda_elroq_current",
        country_code="CH",
        country_label="Switzerland",
        brand="SKODA",
        model="ELROQ",
        source_url="https://pageflip.ch/skoda/de/preislisten/d_pl_elroq/d_pl_elroq.pdf",
        source_label="Skoda Switzerland Elroq price list PDF",
        local_path="skoda_ch_elroq_preisliste_2026-05.pdf",
        source_type="official_price_list_pdf",
        document_date="2026-05-04",
        valid_from="2026-05-01",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Elroq", "gültig ab 01.05.2026", "CHF 37’300", "CHF 52’200"),
        entries=(
            {
                "trim": "Essence 60",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 37300,
                "currency": "CHF",
                "evidenceRole": "current_price_list_baseline",
                "readiness": "baseline_only_no_movement",
            },
            {
                "trim": "RS",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 52200,
                "currency": "CHF",
                "evidenceRole": "current_price_list_baseline",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_skoda_enyaq_current",
        country_code="CH",
        country_label="Switzerland",
        brand="SKODA",
        model="ENYAQ",
        source_url="https://pageflip.ch/skoda/de/preislisten/d_pl_newenyaq_fl/d_pl_newenyaq_fl.pdf",
        source_label="Skoda Switzerland Enyaq price list PDF",
        local_path="skoda_ch_enyaq_preisliste_2026-05.pdf",
        source_type="official_price_list_pdf",
        document_date="2026-05-04",
        valid_from="2026-05-01",
        valid_until=None,
        evidence_class="official_current_baseline",
        expected_terms=("Enyaq", "CHF 52’250", "CHF 61’450", "WM-Lagerprämie"),
        entries=(
            {
                "trim": "85 Selection",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 52250,
                "currency": "CHF",
                "evidenceRole": "current_price_list_baseline",
                "readiness": "baseline_only_no_movement",
            },
            {
                "trim": "RS",
                "powertrain": "BEV",
                "oldSourceMsrp": None,
                "currentSourceMsrp": 61450,
                "currency": "CHF",
                "evidenceRole": "current_price_list_baseline",
                "readiness": "baseline_only_no_movement",
            },
        ),
    ),
    OfficialSource(
        code="ch_toyota_rav4_price_list_transition",
        country_code="CH",
        country_label="Switzerland",
        brand="TOYOTA",
        model="RAV4",
        source_url="https://de.toyota.ch/content/dam/toyota/nmsc/switzerland/pdf_preislisten/PL_RAV4_HEV_PHEV_de_03.pdf",
        source_label="Toyota Switzerland RAV4 official 2026 price-list transition",
        local_path="toyota_ch_rav4_hev_phev_preisliste_2026-03.pdf",
        source_type="official_price_list_pdf",
        document_date="2026-03-02",
        valid_from="2026-03-02",
        valid_until=None,
        evidence_class="official_generation_transition_baseline",
        expected_terms=("RAV4", "Gültig ab 02.03.2026", "47'900", "55'600"),
        entries=(
            {
                "trim": "Trend AWD Hybrid",
                "powertrain": "HEV",
                "oldSourceMsrp": 53500,
                "currentSourceMsrp": 47900,
                "currency": "CHF",
                "evidenceRole": "previous_price_list_vs_new_generation_price_list",
                "readiness": "generation_transition_baseline",
                "relatedEvidence": "toyota_ch_rav4_preisliste_2026-01.pdf",
            },
            {
                "trim": "GR SPORT AWD Hybrid",
                "powertrain": "HEV",
                "oldSourceMsrp": 59900,
                "currentSourceMsrp": 55600,
                "currency": "CHF",
                "evidenceRole": "previous_price_list_vs_new_generation_price_list",
                "readiness": "generation_transition_baseline",
                "relatedEvidence": "toyota_ch_rav4_preisliste_2026-01.pdf",
            },
        ),
    ),
)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"Could not read JSON artifact {path}: {exc}") from exc
    return data if isinstance(data, dict) else {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def clean_text(value: str) -> str:
    value = html.unescape(value)
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def download_if_missing(url: str, path: Path) -> str | None:
    if path.exists():
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 MSRP monitoring evidence audit",
            "Accept": "*/*",
        },
    )
    try:
        with urlopen(request, timeout=60) as response:
            path.write_bytes(response.read())
    except (OSError, URLError) as exc:
        return str(exc)
    return None


def extract_source_text(path: Path) -> tuple[str, str | None]:
    if not path.exists():
        return "", "missing_local_snapshot"
    if path.suffix.lower() == ".pdf":
        try:
            import pdfplumber  # type: ignore[import-untyped]
        except ModuleNotFoundError:
            return "", "pdfplumber_missing"
        try:
            with pdfplumber.open(path) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages[:4])
        except Exception as exc:  # pragma: no cover - PDF parser safety
            return "", f"pdf_extract_failed:{exc}"
        return clean_text(text), None
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except OSError as exc:
        return "", f"text_read_failed:{exc}"
    return clean_text(raw), None


def pct_change(old_value: object, current_value: object) -> float | None:
    try:
        old_price = float(old_value) if old_value is not None else math.nan
        current_price = float(current_value) if current_value is not None else math.nan
    except (TypeError, ValueError):
        return None
    if not old_price or math.isnan(old_price) or math.isnan(current_price):
        return None
    return round((current_price - old_price) / old_price * 100, 2)


def top30_rank_lookup(top30_payload: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for country in top30_payload.get("countries") or []:
        if not isinstance(country, dict):
            continue
        code = str(country.get("countryCode") or "").upper()
        for model in country.get("models") or []:
            if not isinstance(model, dict):
                continue
            key = (
                code,
                str(model.get("brand") or "").upper(),
                str(model.get("model") or "").upper(),
            )
            lookup[key] = {
                "rank": model.get("rank"),
                "sales12m": model.get("sales12m"),
                "sourceDraftStatus": (model.get("sourceDraft") or {}).get("status"),
                "currentPriceStatus": (model.get("currentPriceCoverage") or {}).get("status"),
                "snapshotMovementStatus": (model.get("snapshotMovement") or {}).get("status"),
            }
    return lookup


def top30_model_records(top30_payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for country in top30_payload.get("countries") or []:
        if not isinstance(country, dict):
            continue
        code = str(country.get("countryCode") or "").upper()
        label = str(country.get("countryLabel") or code)
        for model in country.get("models") or []:
            if not isinstance(model, dict):
                continue
            records.append(
                {
                    "countryCode": code,
                    "countryLabel": label,
                    "rank": model.get("rank"),
                    "brand": str(model.get("brand") or "").upper(),
                    "model": str(model.get("model") or "").upper(),
                    "sales12m": model.get("sales12m"),
                    "sourceDraftStatus": (model.get("sourceDraft") or {}).get("status"),
                    "currentPriceStatus": (model.get("currentPriceCoverage") or {}).get("status"),
                    "snapshotMovementStatus": (model.get("snapshotMovement") or {}).get("status"),
                }
            )
    return records


def top30_coverage_payload(
    top30_payload: dict[str, Any],
    sources: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    source_keys = {
        (
            str(source.get("countryCode") or "").upper(),
            str(source.get("brand") or "").upper(),
            str(source.get("model") or "").upper(),
        )
        for source in sources
    }
    by_country: dict[str, dict[str, Any]] = {}
    for record in top30_model_records(top30_payload):
        country_code = str(record["countryCode"])
        bucket = by_country.setdefault(
            country_code,
            {
                "countryCode": country_code,
                "countryLabel": record["countryLabel"],
                "top30ModelCount": 0,
                "coveredModelCount": 0,
                "missingModelCount": 0,
                "coveredModels": [],
                "missingModels": [],
            },
        )
        bucket["top30ModelCount"] += 1
        model_key = (country_code, str(record["brand"]), str(record["model"]))
        model_payload = {
            "rank": record.get("rank"),
            "brand": record.get("brand"),
            "model": record.get("model"),
            "sales12m": record.get("sales12m"),
            "snapshotMovementStatus": record.get("snapshotMovementStatus"),
            "currentPriceStatus": record.get("currentPriceStatus"),
        }
        if model_key in source_keys:
            bucket["coveredModelCount"] += 1
            bucket["coveredModels"].append(model_payload)
        else:
            bucket["missingModelCount"] += 1
            bucket["missingModels"].append(model_payload)
    return sorted(by_country.values(), key=lambda item: item["countryLabel"])


def top30_ranking_scope(top30_payload: dict[str, Any]) -> dict[str, Any]:
    scope = top30_payload.get("scope") if isinstance(top30_payload.get("scope"), dict) else {}
    window = scope.get("window") if isinstance(scope.get("window"), dict) else {}
    country_windows: list[dict[str, Any]] = []
    for country in top30_payload.get("countries") or []:
        if not isinstance(country, dict):
            continue
        ranking_window = country.get("rankingWindow") if isinstance(country.get("rankingWindow"), dict) else {}
        country_windows.append(
            {
                "countryCode": country.get("countryCode"),
                "countryLabel": country.get("countryLabel"),
                "rankingMethod": ranking_window.get("rankingMethod") or window.get("rankingMethod"),
                "latestMonth": ranking_window.get("latestMonth") or window.get("sourceLatestMonth"),
                "windowStartMonth": ranking_window.get("windowStartMonth"),
                "windowEndMonth": ranking_window.get("windowEndMonth"),
                "monthsInWindow": ranking_window.get("monthsInWindow"),
            }
        )
    return {
        "topN": scope.get("topN"),
        "segmentFilter": scope.get("segmentFilter"),
        "candidateSource": scope.get("candidateSource"),
        "rankingMethod": window.get("rankingMethod") or "rolling_12m_sales_rank",
        "sourceLatestMonth": window.get("sourceLatestMonth"),
        "sourceWindow": window.get("sourceWindow"),
        "countries": country_windows,
    }


def source_payload(
    source: OfficialSource,
    *,
    evidence_dir: Path,
    top30_lookup: dict[tuple[str, str, str], dict[str, Any]],
    run_date: date,
) -> dict[str, Any]:
    local_path = evidence_dir / source.local_path
    if not local_path.exists() and "/" in source.local_path:
        local_path = DEFAULT_OUTPUT_DIR.parent / source.local_path
    download_error = download_if_missing(source.source_url, local_path)
    text, extract_error = extract_source_text(local_path)
    expected_hits = [
        {"term": term, "matched": term.casefold() in text.casefold()}
        for term in source.expected_terms
    ]
    entry_payloads = []
    for entry in source.entries:
        change_pct = pct_change(entry.get("oldSourceMsrp"), entry.get("currentSourceMsrp"))
        valid_until = source.valid_until
        expired = bool(valid_until and date.fromisoformat(valid_until) < run_date)
        readiness = str(entry.get("readiness") or "")
        if expired and readiness == "demo_offer_boundary":
            readiness = "expired_demo_offer_boundary"
        entry_payloads.append(
            {
                **entry,
                "changePct": change_pct,
                "expired": expired,
            }
            | ({"readiness": readiness} if readiness else {})
        )
    key = (source.country_code.upper(), source.brand.upper(), source.model.upper())
    return {
        "code": source.code,
        "countryCode": source.country_code,
        "countryLabel": source.country_label,
        "brand": source.brand,
        "model": source.model,
        "top30": top30_lookup.get(key, {}),
        "sourceUrl": source.source_url,
        "sourceLabel": source.source_label,
        "sourceType": source.source_type,
        "localPath": str(local_path.relative_to(PROJECT_ROOT)) if local_path.exists() else str(local_path),
        "sha256": sha256_file(local_path),
        "documentDate": source.document_date,
        "validFrom": source.valid_from,
        "validUntil": source.valid_until,
        "evidenceClass": source.evidence_class,
        "expectedTermHits": expected_hits,
        "allExpectedTermsMatched": all(item["matched"] for item in expected_hits),
        "downloadError": download_error,
        "extractError": extract_error,
        "textSample": text[:700],
        "entries": entry_payloads,
    }


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    run_date = args.as_of_date or datetime.now(timezone.utc).date()
    top30_payload = read_json(args.top30_path)
    top30_lookup = top30_rank_lookup(top30_payload)
    sources = [
        source_payload(
            source,
            evidence_dir=args.evidence_dir,
            top30_lookup=top30_lookup,
            run_date=run_date,
        )
        for source in OFFICIAL_SOURCES
    ]
    entries = [entry for source in sources for entry in source["entries"]]
    top30_coverage = top30_coverage_payload(top30_payload, sources)
    by_country: dict[str, dict[str, Any]] = {}
    for source in sources:
        country = source["countryLabel"]
        bucket = by_country.setdefault(
            country,
            {
                "countryLabel": country,
                "sourceCount": 0,
                "entryCount": 0,
                "offerBoundaryCount": 0,
                "baselineOnlyCount": 0,
                "offerSignalOnlyCount": 0,
                "alreadyBackfilledCount": 0,
                "expiredOfferBoundaryCount": 0,
                "generationTransitionCount": 0,
            },
        )
        bucket["sourceCount"] += 1
        bucket["entryCount"] += len(source["entries"])
        for entry in source["entries"]:
            readiness = entry.get("readiness")
            if readiness == "baseline_only_no_movement":
                bucket["baselineOnlyCount"] += 1
            elif readiness == "already_backfilled_live":
                bucket["alreadyBackfilledCount"] += 1
            elif readiness == "expired_demo_offer_boundary":
                bucket["expiredOfferBoundaryCount"] += 1
            elif readiness == "generation_transition_baseline":
                bucket["generationTransitionCount"] += 1
            elif readiness == "demo_offer_boundary":
                bucket["offerBoundaryCount"] += 1
            elif readiness == "offer_signal_only":
                bucket["offerSignalOnlyCount"] += 1
    coverage_by_label = {item["countryLabel"]: item for item in top30_coverage}
    for bucket in by_country.values():
        coverage = coverage_by_label.get(bucket["countryLabel"])
        if not coverage:
            continue
        bucket["top30ModelCount"] = coverage["top30ModelCount"]
        bucket["coveredModelCount"] = coverage["coveredModelCount"]
        bucket["missingModelCount"] = coverage["missingModelCount"]
    return {
        "schemaVersion": "sweden_swiss_top30_official_evidence_v1",
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "asOfDate": run_date.isoformat(),
        "policy": {
            "status": "review_only",
            "productionWriteAllowed": False,
            "rule": (
                "Only rows with same-trim official evidence and clear regular/current prices "
                "can be used as demo offer boundaries. Baseline-only price lists and offer-only rows "
                "are not MSRP drops."
            ),
        },
        "summary": {
            "sourceCount": len(sources),
            "entryCount": len(entries),
            "countryCount": len(by_country),
            "offerBoundaryCount": sum(1 for item in entries if item.get("readiness") == "demo_offer_boundary"),
            "baselineOnlyCount": sum(1 for item in entries if item.get("readiness") == "baseline_only_no_movement"),
            "offerSignalOnlyCount": sum(1 for item in entries if item.get("readiness") == "offer_signal_only"),
            "alreadyBackfilledCount": sum(1 for item in entries if item.get("readiness") == "already_backfilled_live"),
            "expiredOfferBoundaryCount": sum(1 for item in entries if item.get("readiness") == "expired_demo_offer_boundary"),
            "generationTransitionCount": sum(
                1 for item in entries if item.get("readiness") == "generation_transition_baseline"
            ),
            "top30CoveredModelCount": sum(item["coveredModelCount"] for item in top30_coverage),
            "top30MissingModelCount": sum(item["missingModelCount"] for item in top30_coverage),
        },
        "rankingScope": top30_ranking_scope(top30_payload),
        "top30Coverage": top30_coverage,
        "countries": sorted(by_country.values(), key=lambda item: item["countryLabel"]),
        "sources": sources,
    }


def money_label(value: object, currency: str) -> str:
    if value is None:
        return "-"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{numeric:,.0f} {currency}"


def render_markdown(payload: dict[str, Any]) -> str:
    ranking_scope = payload.get("rankingScope") if isinstance(payload.get("rankingScope"), dict) else {}
    lines = [
        "# Sweden + Switzerland Rolling 12M Top30 Official MSRP Evidence Leads",
        "",
        f"Generated: {payload['generatedAtUtc']}",
        f"As of: {payload['asOfDate']}",
        (
            "Ranking scope: rolling 12M SUV top30 by sales"
            f" (latest source month: {ranking_scope.get('sourceLatestMonth') or '-'})."
        ),
        "",
        "This artifact is review-only. Offer boundaries are monitor/demo candidates; baseline-only price lists are not price drops.",
        "",
        "## Summary",
        "",
        "| Country | Top30 covered | Top30 missing | Sources | Entries | Demo offer boundaries | Offer-only | Baseline-only | Already backfilled | Expired offer boundaries | Generation transitions |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for country in payload["countries"]:
        lines.append(
            "| {countryLabel} | {coveredModelCount}/{top30ModelCount} | {missingModelCount} | "
            "{sourceCount} | {entryCount} | {offerBoundaryCount} | {offerSignalOnlyCount} | "
            "{baselineOnlyCount} | {alreadyBackfilledCount} | {expiredOfferBoundaryCount} | "
            "{generationTransitionCount} |".format(**country)
        )
    lines.extend(["", "## Top30 Coverage Gaps", ""])
    for coverage in payload.get("top30Coverage") or []:
        missing = coverage.get("missingModels") or []
        if not missing:
            lines.append(f"- {coverage['countryLabel']}: full top30 official evidence coverage.")
            continue
        missing_label = ", ".join(
            f"#{item.get('rank')} {item.get('brand')} {item.get('model')}" for item in missing[:15]
        )
        suffix = f" (+{len(missing) - 15} more)" if len(missing) > 15 else ""
        lines.append(
            f"- {coverage['countryLabel']}: {coverage['coveredModelCount']}/{coverage['top30ModelCount']} "
            f"covered; missing {coverage['missingModelCount']}: {missing_label}{suffix}."
        )
    lines.extend(
        [
            "",
            "## Evidence Queue",
            "",
            "| Country | Top30 | Brand | Model / trim | Evidence class | Old/list | Current/special | Change | Valid window | Readiness | Source |",
            "| --- | ---: | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for source in payload["sources"]:
        top30 = source.get("top30") or {}
        rank = top30.get("rank") or "-"
        for entry in source["entries"]:
            currency = str(entry.get("currency") or "")
            change = entry.get("changePct")
            change_label = f"{change:.2f}%" if isinstance(change, (int, float)) else "-"
            valid_from = source.get("validFrom") or "-"
            valid_until = source.get("validUntil") or "-"
            source_label = source.get("sourceLabel") or source.get("sourceUrl")
            lines.append(
                "| {country} | {rank} | {brand} | {model} {trim} | {klass} | {old} | {current} | "
                "{change} | {window} | {readiness} | {source_label} |".format(
                    country=source["countryLabel"],
                    rank=rank,
                    brand=source["brand"],
                    model=source["model"],
                    trim=entry.get("trim") or "",
                    klass=source["evidenceClass"],
                    old=money_label(entry.get("oldSourceMsrp"), currency),
                    current=money_label(entry.get("currentSourceMsrp"), currency),
                    change=change_label,
                    window=f"{valid_from} -> {valid_until}",
                    readiness=entry.get("readiness") or "-",
                    source_label=source_label,
                )
            )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `already_backfilled_live` rows are the existing Sweden official campaign/promotion backfills already visible in live monitoring.",
            "- `demo_offer_boundary` rows have official regular/list and special/current prices, so they can drive the Sweden + Swiss demo monitor, but they still represent campaign/offer boundaries unless later evidence proves a permanent MSRP cut.",
            "- `offer_signal_only` rows have official campaign price and validity/savings text but no old MSRP baseline. They should appear in the deck as offer signals, not as MSRP drops.",
            "- `baseline_only_no_movement` rows are official current price-list baselines. They should create launch/current baselines, not price-drop conclusions.",
            "- `expired_demo_offer_boundary` rows are still useful as 2026 historical evidence, but the dashboard should show their valid-until date so they are not treated as active offers.",
            "- `generation_transition_baseline` rows compare two official price lists across a model-year/generation transition. They are useful monitoring evidence, but should not be merged with same-trim offer drops without manual review.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE_DIR)
    parser.add_argument("--top30-path", type=Path, default=DEFAULT_TOP30_PATH)
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(args)
    json_path = args.output_dir / "official_evidence_leads.json"
    markdown_path = args.output_dir / "official_evidence_leads.md"
    write_json(json_path, payload)
    markdown_path.write_text(render_markdown(payload), encoding="utf-8")
    print(f"Wrote {json_path.relative_to(PROJECT_ROOT)}")
    print(f"Wrote {markdown_path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise
