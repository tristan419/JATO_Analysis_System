#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import logging
from pathlib import Path
import sys
import time
from typing import Any
from urllib.parse import urljoin
from uuid import uuid4

import requests
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "06_AppPlatform" / "backend"
TOOLKIT_ROOT = REPO_ROOT / "07_ScrapingToolkit"
for entry in (str(BACKEND_ROOT), str(TOOLKIT_ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from app.db.models import JatoMsrpLink, MsrpSource  # noqa: E402
from app.db.session import get_session_factory  # noqa: E402
from app.services.msrp_workflow_service import (  # noqa: E402
    create_scrape_batch_ingest,
)
from jato_scraper import registry  # noqa: E402
from jato_scraper.config_loader import load_source_file  # noqa: E402
from jato_scraper.currency_converter import (  # noqa: E402
    enrich_observations_with_eur,
)
from jato_scraper.runner import _observation_to_ingest_dict  # noqa: E402
from jato_scraper.validation import validate_observations  # noqa: E402


EVKX_SEARCH_API_URL = "https://evkx.net/api/evs/search"
EVKX_BASE_URL = "https://evkx.net/"
SEED_LABEL = "w2-initial-links-2026-04-17"
SOURCE_FILE_EXTRACT_MAX_ATTEMPTS = 3
SOURCE_FILE_EXTRACT_RETRY_DELAY_SECONDS = 2.0

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvkxVariantSeed:
    country: str
    name: str
    brand: str
    jato_model: str
    official_model: str
    official_trim: str


@dataclass(frozen=True)
class LinkSeed:
    country: str
    brand: str
    jato_model: str
    jato_trim: str
    jato_powertrain: str
    official_model: str
    official_trim: str
    official_edition: str
    official_powertrain: str
    confidence: int
    notes: str


@dataclass(frozen=True)
class SourceFileSeed:
    relative_path: str
    country: str
    brand: str
    tier: int
    notes: str


def _link_seed_business_key_from_values(
    *,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
    jato_powertrain: str,
    official_model: str,
    official_trim: str,
    official_edition: str,
    official_powertrain: str,
) -> tuple[str, ...]:
    return (
        str(country or "").strip(),
        str(brand or "").strip(),
        str(jato_model or "").strip(),
        str(jato_trim or "").strip(),
        str(jato_powertrain or "").strip(),
        str(official_model or "").strip(),
        str(official_trim or "").strip(),
        str(official_edition or "").strip(),
        str(official_powertrain or "").strip(),
    )


def _link_seed_business_key(item: LinkSeed) -> tuple[str, ...]:
    return _link_seed_business_key_from_values(
        country=item.country,
        brand=item.brand,
        jato_model=item.jato_model,
        jato_trim=item.jato_trim,
        jato_powertrain=item.jato_powertrain,
        official_model=item.official_model,
        official_trim=item.official_trim,
        official_edition=item.official_edition,
        official_powertrain=item.official_powertrain,
    )


def _existing_link_row_business_key(row: Any) -> tuple[str, ...]:
    return _link_seed_business_key_from_values(
        country=row.country,
        brand=row.brand,
        jato_model=row.jato_model,
        jato_trim=row.jato_trim,
        jato_powertrain=row.jato_powertrain,
        official_model=row.official_model,
        official_trim=row.official_trim,
        official_edition=row.official_edition,
        official_powertrain=row.official_powertrain,
    )


def _find_stale_link_seed_ids(
    rows: list[Any],
    desired_keys: set[tuple[str, ...]],
) -> list[Any]:
    stale_ids: list[Any] = []
    for row in rows:
        if _existing_link_row_business_key(row) not in desired_keys:
            stale_ids.append(row.link_id)
    return stale_ids


SWEDEN = "Sweden"
GERMANY = "Germany"
SUV_DRAFT_ROOT = (
    "07_ScrapingToolkit/source_drafts/suv_only_country_model_top30"
)


def evkx_seed(
    country: str,
    name: str,
    brand: str,
    jato_model: str,
    official_model: str,
    official_trim: str,
) -> EvkxVariantSeed:
    return EvkxVariantSeed(
        country,
        name,
        brand,
        jato_model,
        official_model,
        official_trim,
    )


def link_seed(
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
    jato_powertrain: str,
    official_model: str,
    official_trim: str,
    official_edition: str,
    official_powertrain: str,
    confidence: int,
    notes: str,
) -> LinkSeed:
    return LinkSeed(
        country,
        brand,
        jato_model,
        jato_trim,
        jato_powertrain,
        official_model,
        official_trim,
        official_edition,
        official_powertrain,
        confidence,
        notes,
    )


def source_file_seed(
    relative_path: str,
    country: str,
    brand: str,
    tier: int,
    notes: str,
) -> SourceFileSeed:
    return SourceFileSeed(relative_path, country, brand, tier, notes)


OFFICIAL_SOURCE_FILE_SEEDS: tuple[SourceFileSeed, ...] = (
    source_file_seed(
        "07_ScrapingToolkit/sources/volvo_se_xc60.yaml",
        SWEDEN,
        "VOLVO",
        1,
        "Native Volvo Sweden XC60 official source",
    ),
    source_file_seed(
        f"{SUV_DRAFT_ROOT}/se/09_volkswagen_tiguan_se.yaml",
        SWEDEN,
        "VOLKSWAGEN",
        1,
        "Sweden Tiguan official configurator draft for second-batch links",
    ),
    source_file_seed(
        f"{SUV_DRAFT_ROOT}/se/10_volkswagen_id_4_se.yaml",
        SWEDEN,
        "VOLKSWAGEN",
        1,
        "Sweden ID.4 official configurator draft for next-batch links",
    ),
    source_file_seed(
        f"{SUV_DRAFT_ROOT}/se/12_volkswagen_t_roc_se.yaml",
        SWEDEN,
        "VOLKSWAGEN",
        1,
        "Sweden T-Roc official configurator draft for third-batch links",
    ),
    source_file_seed(
        f"{SUV_DRAFT_ROOT}/se/23_volkswagen_tayron_se.yaml",
        SWEDEN,
        "VOLKSWAGEN",
        1,
        "Sweden Tayron official configurator draft for next-batch links",
    ),
    source_file_seed(
        f"{SUV_DRAFT_ROOT}/se/06_skoda_kodiaq_se.yaml",
        SWEDEN,
        "SKODA",
        1,
        "Sweden Kodiaq official model-page draft for second-batch links",
    ),
    source_file_seed(
        f"{SUV_DRAFT_ROOT}/de/02_volkswagen_tiguan_de.yaml",
        GERMANY,
        "VOLKSWAGEN",
        1,
        "Germany Tiguan official configurator draft for second-batch links",
    ),
    source_file_seed(
        f"{SUV_DRAFT_ROOT}/de/01_volkswagen_t_roc_de.yaml",
        GERMANY,
        "VOLKSWAGEN",
        1,
        "Germany T-Roc official configurator draft for second-batch links",
    ),
    source_file_seed(
        f"{SUV_DRAFT_ROOT}/de/04_skoda_kodiaq_de.yaml",
        GERMANY,
        "SKODA",
        1,
        "Germany Kodiaq official model-page draft for second-batch links",
    ),
)


EVKX_VARIANT_SEEDS: tuple[EvkxVariantSeed, ...] = (
    evkx_seed(
        SWEDEN,
        "Tesla Model Y Standard",
        "TESLA",
        "Model Y",
        "Model Y",
        "Standard",
    ),
    evkx_seed(
        SWEDEN,
        "Tesla Model Y Premium AWD",
        "TESLA",
        "Model Y",
        "Model Y",
        "Premium AWD",
    ),
    evkx_seed(
        SWEDEN,
        "Tesla Model Y Premium RWD",
        "TESLA",
        "Model Y",
        "Model Y",
        "Premium RWD",
    ),
    evkx_seed(
        SWEDEN,
        "Tesla Model Y Performance",
        "TESLA",
        "Model Y",
        "Model Y",
        "Performance",
    ),
    evkx_seed(
        SWEDEN,
        "KIA EV3 Standard Range",
        "KIA",
        "KIA EV3",
        "KIA EV3",
        "Standard Range",
    ),
    evkx_seed(
        SWEDEN,
        "Kia EV3 Long Range",
        "KIA",
        "KIA EV3",
        "KIA EV3",
        "Long Range",
    ),
    evkx_seed(
        SWEDEN,
        "Volkswagen ID.4 Pro",
        "VOLKSWAGEN",
        "ID.4",
        "ID.4",
        "Pro",
    ),
    evkx_seed(
        SWEDEN,
        "Volvo EX30 Single Motor Extended Range",
        "VOLVO",
        "Volvo EX30",
        "Volvo EX30",
        "Single Motor Extended Range",
    ),
    evkx_seed(
        SWEDEN,
        "Volvo EX30 Twin Motor Performance",
        "VOLVO",
        "Volvo EX30",
        "Volvo EX30",
        "Twin Motor Performance",
    ),
    evkx_seed(
        SWEDEN,
        "Volvo EX40 Single Motor ER",
        "VOLVO",
        "Volvo EX40",
        "Volvo EX40",
        "Single Motor ER",
    ),
    evkx_seed(
        SWEDEN,
        "Volvo EX40 Twin Motor",
        "VOLVO",
        "Volvo EX40",
        "Volvo EX40",
        "Twin Motor",
    ),
    evkx_seed(SWEDEN, "SKODA ENYAQ 85", "SKODA", "Enyaq", "Enyaq", "85"),
    evkx_seed(GERMANY, "SKODA Elroq 50", "SKODA", "ELROQ", "ELROQ", "50"),
    evkx_seed(GERMANY, "SKODA Elroq 60", "SKODA", "ELROQ", "ELROQ", "60"),
    evkx_seed(GERMANY, "SKODA Elroq 85", "SKODA", "ELROQ", "ELROQ", "85"),
    evkx_seed(GERMANY, "SKODA Elroq RS", "SKODA", "ELROQ", "ELROQ", "RS"),
    evkx_seed(GERMANY, "SKODA ENYAQ 60", "SKODA", "Enyaq", "Enyaq", "60"),
    evkx_seed(GERMANY, "SKODA ENYAQ 85", "SKODA", "Enyaq", "Enyaq", "85"),
    evkx_seed(GERMANY, "SKODA ENYAQ 85x", "SKODA", "Enyaq", "Enyaq", "85x"),
    evkx_seed(
        GERMANY,
        "Tesla Model Y Standard",
        "TESLA",
        "Model Y",
        "Model Y",
        "Standard",
    ),
    evkx_seed(
        GERMANY,
        "Tesla Model Y Standard Long Range RWD",
        "TESLA",
        "Model Y",
        "Model Y",
        "Standard Long Range RWD",
    ),
    evkx_seed(
        GERMANY,
        "Tesla Model Y Premium AWD",
        "TESLA",
        "Model Y",
        "Model Y",
        "Premium AWD",
    ),
    evkx_seed(
        GERMANY,
        "Tesla Model Y Performance",
        "TESLA",
        "Model Y",
        "Model Y",
        "Performance",
    ),
    evkx_seed(
        GERMANY,
        "Volkswagen ID.4 Pure",
        "VOLKSWAGEN",
        "ID.4",
        "ID.4",
        "Pure",
    ),
    evkx_seed(
        GERMANY,
        "Volkswagen ID.4 Pro",
        "VOLKSWAGEN",
        "ID.4",
        "ID.4",
        "Pro",
    ),
    evkx_seed(
        GERMANY,
        "Volkswagen ID.4 Pro 4MOTION",
        "VOLKSWAGEN",
        "ID.4",
        "ID.4",
        "Pro 4MOTION",
    ),
    evkx_seed(
        GERMANY,
        "Volkswagen ID.4 GTX 4MOTION",
        "VOLKSWAGEN",
        "ID.4",
        "ID.4",
        "GTX 4MOTION",
    ),
    evkx_seed(
        GERMANY,
        "Volvo EX30 Single Motor",
        "VOLVO",
        "Volvo EX30",
        "Volvo EX30",
        "Single Motor",
    ),
    evkx_seed(
        GERMANY,
        "Volvo EX30 Single Motor Extended Range",
        "VOLVO",
        "Volvo EX30",
        "Volvo EX30",
        "Single Motor Extended Range",
    ),
    evkx_seed(
        GERMANY,
        "Volvo EX30 Twin Motor Performance",
        "VOLVO",
        "Volvo EX30",
        "Volvo EX30",
        "Twin Motor Performance",
    ),
    evkx_seed(
        GERMANY,
        "Volvo EX40 Single Motor",
        "VOLVO",
        "Volvo EX40",
        "Volvo EX40",
        "Single Motor",
    ),
    evkx_seed(
        GERMANY,
        "Volvo EX40 Twin Motor",
        "VOLVO",
        "Volvo EX40",
        "Volvo EX40",
        "Twin Motor",
    ),
    evkx_seed(
        GERMANY,
        "Kia EV3 Long Range",
        "KIA",
        "KIA EV3",
        "KIA EV3",
        "Long Range",
    ),
)


LINK_SEEDS: tuple[LinkSeed, ...] = (
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo XC60",
        "CORE NORDIC EDITION",
        "PHEV",
        "XC60",
        "Core Nordic Edition",
        "",
        "PHEV",
        98,
        "Direct Volvo Sweden XC60 core mapping",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo XC60",
        "PLUS DARK NORDIC EDITION",
        "PHEV",
        "XC60",
        "Plus Nordic Edition",
        "",
        "PHEV",
        96,
        "Collapse Volvo Nordic Plus dark into official Plus card",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo XC60",
        "PLUS BLACK NORDIC EDITION",
        "PHEV",
        "XC60",
        "Plus Nordic Edition",
        "",
        "PHEV",
        96,
        "Collapse Volvo Nordic Plus black into official Plus card",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo XC60",
        "PLUS BRIGHT NORDIC EDITION",
        "PHEV",
        "XC60",
        "Plus Nordic Edition",
        "",
        "PHEV",
        95,
        "Collapse Volvo Nordic Plus bright into official Plus card",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo XC60",
        "ULTRA DARK",
        "PHEV",
        "XC60",
        "Ultra",
        "",
        "PHEV",
        94,
        "Collapse Volvo Ultra dark into official Ultra card",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo XC60",
        "ULTRA BRIGHT",
        "PHEV",
        "XC60",
        "Ultra",
        "",
        "PHEV",
        94,
        "Collapse Volvo Ultra bright into official Ultra card",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo XC60",
        "ULTRA BLACK EDITION",
        "PHEV",
        "XC60",
        "Ultra",
        "",
        "PHEV",
        94,
        "Collapse Volvo Ultra black into official Ultra card",
    ),
    link_seed(
        SWEDEN,
        "TESLA",
        "Model Y",
        "STANDARD",
        "BEV",
        "Model Y",
        "Standard",
        "",
        "BEV",
        97,
        "Sweden native EVKX Model Y Standard",
    ),
    link_seed(
        SWEDEN,
        "TESLA",
        "Model Y",
        "PREMIUM",
        "BEV",
        "Model Y",
        "Premium AWD",
        "",
        "BEV",
        97,
        "Sweden native EVKX Model Y Premium AWD",
    ),
    link_seed(
        SWEDEN,
        "TESLA",
        "Model Y",
        "LONG RANGE RWD",
        "BEV",
        "Model Y",
        "Premium RWD",
        "",
        "BEV",
        90,
        "Approximate Sweden long-range RWD to native Premium RWD",
    ),
    link_seed(
        SWEDEN,
        "TESLA",
        "Model Y",
        "PERFORMANCE",
        "BEV",
        "Model Y",
        "Performance",
        "",
        "BEV",
        97,
        "Sweden native EVKX Model Y Performance",
    ),
    link_seed(
        SWEDEN,
        "TESLA",
        "Model Y",
        "RWD",
        "BEV",
        "Model Y",
        "Standard",
        "",
        "BEV",
        88,
        "Collapse Sweden RWD to native Standard",
    ),
    link_seed(
        SWEDEN,
        "KIA",
        "KIA EV3",
        "PLUS",
        "BEV",
        "KIA EV3",
        "Long Range",
        "",
        "BEV",
        92,
        "Sweden EV3 Plus to native Long Range",
    ),
    link_seed(
        SWEDEN,
        "KIA",
        "KIA EV3",
        "-",
        "BEV",
        "KIA EV3",
        "Standard Range",
        "",
        "BEV",
        90,
        "Sweden EV3 base trim to Standard Range",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "ID.4",
        "PRO EDITION",
        "BEV",
        "ID.4",
        "Pro Edition",
        "",
        "BEV",
        97,
        "Sweden native ID.4 Pro Edition",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "ID.4",
        "PRO EDITION 4MOTION",
        "BEV",
        "ID.4",
        "Pro 4MOTION Edition",
        "",
        "BEV",
        97,
        "Sweden native ID.4 Pro 4MOTION Edition",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "ID.4",
        "GTX 4MOTION EDITION",
        "BEV",
        "ID.4",
        "GTX 4MOTION Edition",
        "",
        "BEV",
        97,
        "Sweden native ID.4 GTX 4MOTION Edition",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "ID.4",
        "PRO 4MOTION STYLE EDITION",
        "BEV",
        "ID.4",
        "Pro 4MOTION Style Edition",
        "",
        "BEV",
        96,
        "Sweden native ID.4 Pro 4MOTION Style Edition",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "ID.4",
        "PRO STYLE EDITION",
        "BEV",
        "ID.4",
        "Pro Style Edition",
        "",
        "BEV",
        96,
        "Sweden native ID.4 Pro Style Edition",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "ID.4",
        "LIFE PRO",
        "BEV",
        "ID.4",
        "Pro Life",
        "",
        "BEV",
        95,
        "Collapse Sweden Life Pro to native Pro Life",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "ID.4",
        "LIFE PRO 4MOTION",
        "BEV",
        "ID.4",
        "Pro 4MOTION Life",
        "",
        "BEV",
        95,
        "Collapse Sweden Life Pro 4MOTION to native Pro 4MOTION Life",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo EX30",
        "PLUS",
        "BEV",
        "Volvo EX30",
        "Single Motor Extended Range",
        "",
        "BEV",
        88,
        "Collapse EX30 Plus to native single-motor ER",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo EX30",
        "PLUS BLACK EDITION",
        "BEV",
        "Volvo EX30",
        "Single Motor Extended Range",
        "",
        "BEV",
        90,
        "Collapse EX30 Plus Black to native single-motor ER",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo EX30",
        "ULTRA",
        "BEV",
        "Volvo EX30",
        "Twin Motor Performance",
        "",
        "BEV",
        85,
        "Map EX30 Ultra to twin-motor performance",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo EX40",
        "PLUS SPECIAL EDITION",
        "BEV",
        "Volvo EX40",
        "Single Motor ER",
        "",
        "BEV",
        84,
        "Map EX40 Plus Special to native single-motor ER",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo EX40",
        "PLUS BLACK EDITION",
        "BEV",
        "Volvo EX40",
        "Twin Motor",
        "",
        "BEV",
        91,
        "Map EX40 Plus Black to native twin motor",
    ),
    link_seed(
        SWEDEN,
        "VOLVO",
        "Volvo EX40",
        "ULTRA BLACK EDITION",
        "BEV",
        "Volvo EX40",
        "Twin Motor",
        "",
        "BEV",
        88,
        "Map EX40 Ultra Black to native twin motor",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "ELROQ",
        "50",
        "BEV",
        "ELROQ",
        "50",
        "",
        "BEV",
        97,
        "Germany native Elroq 50",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "ELROQ",
        "60",
        "BEV",
        "ELROQ",
        "60",
        "",
        "BEV",
        97,
        "Germany native Elroq 60",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "ELROQ",
        "85",
        "BEV",
        "ELROQ",
        "85",
        "",
        "BEV",
        97,
        "Germany native Elroq 85",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "ELROQ",
        "RS",
        "BEV",
        "ELROQ",
        "RS",
        "",
        "BEV",
        98,
        "Germany native Elroq RS",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "Enyaq",
        "60",
        "BEV",
        "Enyaq",
        "60",
        "",
        "BEV",
        97,
        "Germany native Enyaq 60",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "Enyaq",
        "80",
        "BEV",
        "Enyaq",
        "85",
        "",
        "BEV",
        92,
        "Map old Enyaq 80 trim to Germany native 85",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "Enyaq",
        "85",
        "BEV",
        "Enyaq",
        "85",
        "",
        "BEV",
        97,
        "Germany native Enyaq 85",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "Enyaq",
        "80X",
        "BEV",
        "Enyaq",
        "85x",
        "",
        "BEV",
        92,
        "Map old Enyaq 80X trim to Germany native 85x",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "Enyaq",
        "85X",
        "BEV",
        "Enyaq",
        "85x",
        "",
        "BEV",
        97,
        "Germany native Enyaq 85x",
    ),
    link_seed(
        GERMANY,
        "TESLA",
        "Model Y",
        "STANDARD",
        "BEV",
        "Model Y",
        "Standard",
        "",
        "BEV",
        97,
        "Germany native Model Y Standard",
    ),
    link_seed(
        GERMANY,
        "TESLA",
        "Model Y",
        "LONG RANGE RWD",
        "BEV",
        "Model Y",
        "Standard Long Range RWD",
        "",
        "BEV",
        97,
        "Germany native Model Y Standard Long Range RWD",
    ),
    link_seed(
        GERMANY,
        "TESLA",
        "Model Y",
        "PREMIUM",
        "BEV",
        "Model Y",
        "Premium AWD",
        "",
        "BEV",
        96,
        "Germany native Model Y Premium AWD",
    ),
    link_seed(
        GERMANY,
        "TESLA",
        "Model Y",
        "PERFORMANCE",
        "BEV",
        "Model Y",
        "Performance",
        "",
        "BEV",
        97,
        "Germany native Model Y Performance",
    ),
    link_seed(
        GERMANY,
        "TESLA",
        "Model Y",
        "RWD",
        "BEV",
        "Model Y",
        "Standard",
        "",
        "BEV",
        88,
        "Collapse Germany RWD to native Standard",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "ID.4",
        "PRO EDITION",
        "BEV",
        "ID.4",
        "Pro",
        "",
        "BEV",
        96,
        "Germany native ID.4 Pro",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "ID.4",
        "PRO EDITION 4MOTION",
        "BEV",
        "ID.4",
        "Pro 4MOTION",
        "",
        "BEV",
        96,
        "Germany native ID.4 Pro 4MOTION",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "ID.4",
        "PRO PERFORMANCE 4MOTION",
        "BEV",
        "ID.4",
        "Pro 4MOTION",
        "",
        "BEV",
        90,
        "Collapse Germany Pro Performance 4MOTION to native Pro 4MOTION",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "ID.4",
        "LIFE PRO 4MOTION",
        "BEV",
        "ID.4",
        "Pro 4MOTION",
        "",
        "BEV",
        90,
        "Collapse Germany Life Pro 4MOTION to native Pro 4MOTION",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "ID.4",
        "GTX 4MOTION EDITION",
        "BEV",
        "ID.4",
        "GTX 4MOTION",
        "",
        "BEV",
        96,
        "Germany native ID.4 GTX 4MOTION",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "ID.4",
        "GTX 4MOTION",
        "BEV",
        "ID.4",
        "GTX 4MOTION",
        "",
        "BEV",
        95,
        "Germany native ID.4 GTX 4MOTION",
    ),
    link_seed(
        GERMANY,
        "VOLVO",
        "Volvo EX30",
        "CORE",
        "BEV",
        "Volvo EX30",
        "Single Motor",
        "",
        "BEV",
        83,
        "Map Germany EX30 Core to native single motor",
    ),
    link_seed(
        GERMANY,
        "VOLVO",
        "Volvo EX30",
        "PLUS",
        "BEV",
        "Volvo EX30",
        "Single Motor Extended Range",
        "",
        "BEV",
        88,
        "Map Germany EX30 Plus to native single-motor ER",
    ),
    link_seed(
        GERMANY,
        "VOLVO",
        "Volvo EX30",
        "PLUS BLACK EDITION",
        "BEV",
        "Volvo EX30",
        "Single Motor Extended Range",
        "",
        "BEV",
        90,
        "Map Germany EX30 Plus Black to native single-motor ER",
    ),
    link_seed(
        GERMANY,
        "VOLVO",
        "Volvo EX30",
        "ULTRA",
        "BEV",
        "Volvo EX30",
        "Twin Motor Performance",
        "",
        "BEV",
        86,
        "Map Germany EX30 Ultra to native twin-motor performance",
    ),
    link_seed(
        GERMANY,
        "VOLVO",
        "Volvo EX40",
        "PLUS SPECIAL EDITION",
        "BEV",
        "Volvo EX40",
        "Single Motor",
        "",
        "BEV",
        84,
        "Map Germany EX40 Plus Special to native single motor",
    ),
    link_seed(
        GERMANY,
        "VOLVO",
        "Volvo EX40",
        "PLUS BLACK EDITION",
        "BEV",
        "Volvo EX40",
        "Twin Motor",
        "",
        "BEV",
        91,
        "Map Germany EX40 Plus Black to native twin motor",
    ),
    link_seed(
        GERMANY,
        "VOLVO",
        "Volvo EX40",
        "ULTRA BLACK EDITION",
        "BEV",
        "Volvo EX40",
        "Twin Motor",
        "",
        "BEV",
        88,
        "Map Germany EX40 Ultra Black to native twin motor",
    ),
    link_seed(
        GERMANY,
        "KIA",
        "KIA EV3",
        "PLUS",
        "BEV",
        "KIA EV3",
        "Long Range",
        "",
        "BEV",
        92,
        "Map Germany EV3 Plus to native Long Range",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tiguan",
        "LIFE EDITION",
        "MHEV",
        "TIGUAN",
        "Life Edition",
        "",
        "MHEV",
        95,
        "Sweden Tiguan Life Edition MHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tiguan",
        "LIFE EDITION",
        "PHEV",
        "TIGUAN",
        "Life Edition",
        "",
        "PHEV",
        95,
        "Sweden Tiguan Life Edition PHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tiguan",
        "LIFE",
        "PHEV",
        "TIGUAN",
        "Life",
        "",
        "PHEV",
        92,
        "Sweden Tiguan Life PHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tiguan",
        "STYLE",
        "ICE",
        "TIGUAN",
        "Style",
        "",
        "ICE",
        94,
        "Sweden Tiguan Style ICE to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tiguan",
        "R-LINE EDITION",
        "ICE",
        "TIGUAN",
        "R-Line Edition",
        "",
        "ICE",
        95,
        "Sweden Tiguan R-Line Edition ICE to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tiguan",
        "R-LINE SWE EDITION",
        "PHEV",
        "TIGUAN",
        "R-Line SWE Edition",
        "",
        "PHEV",
        96,
        "Sweden Tiguan R-Line SWE Edition PHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "SKODA",
        "Kodiaq",
        "SELECTION",
        "ICE",
        "KODIAQ",
        "Kodiaq Selection Explore",
        "",
        "",
        88,
        "Sweden Kodiaq Selection ICE collapsed to official Selection Explore",
    ),
    link_seed(
        SWEDEN,
        "SKODA",
        "Kodiaq",
        "SELECTION",
        "MHEV",
        "KODIAQ",
        "Kodiaq Selection Explore",
        "",
        "",
        88,
        "Sweden Kodiaq Selection MHEV collapsed to official Selection Explore",
    ),
    link_seed(
        SWEDEN,
        "SKODA",
        "Kodiaq",
        "SELECTION",
        "PHEV",
        "KODIAQ",
        "Kodiaq Selection Explore",
        "",
        "",
        88,
        "Sweden Kodiaq Selection PHEV collapsed to official Selection Explore",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "Tiguan",
        "ENERGY",
        "ICE",
        "TIGUAN",
        "ENERGY",
        "",
        "ICE",
        95,
        "Germany Tiguan ENERGY ICE to official configurator row",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "Tiguan",
        "ENERGY",
        "MHEV",
        "TIGUAN",
        "ENERGY",
        "",
        "MHEV",
        94,
        "Germany Tiguan ENERGY MHEV to official configurator row",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "Tiguan",
        "ENERGY",
        "PHEV",
        "TIGUAN",
        "ENERGY",
        "",
        "PHEV",
        95,
        "Germany Tiguan ENERGY PHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "T-Roc",
        "LIFE",
        "MHEV",
        "T-ROC",
        "Life",
        "",
        "MHEV",
        95,
        "Sweden T-Roc Life MHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "T-Roc",
        "R-LINE",
        "MHEV",
        "T-ROC",
        "R-Line",
        "",
        "MHEV",
        95,
        "Sweden T-Roc R-Line MHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tayron",
        "LIFE EDITION",
        "MHEV",
        "TAYRON",
        "Life Edition",
        "",
        "MHEV",
        95,
        "Sweden Tayron Life Edition MHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tayron",
        "LIFE EDITION",
        "ICE",
        "TAYRON",
        "Life Edition",
        "",
        "ICE",
        94,
        "Sweden Tayron Life Edition ICE to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tayron",
        "LIFE EDITION",
        "PHEV",
        "TAYRON",
        "Life Edition",
        "",
        "PHEV",
        95,
        "Sweden Tayron Life Edition PHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tayron",
        "STYLE",
        "ICE",
        "TAYRON",
        "Style",
        "",
        "ICE",
        95,
        "Sweden Tayron Style ICE to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tayron",
        "STYLE",
        "PHEV",
        "TAYRON",
        "Style",
        "",
        "PHEV",
        92,
        "Sweden Tayron Style PHEV to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tayron",
        "R-LINE EDITION",
        "ICE",
        "TAYRON",
        "R-Line Edition",
        "",
        "ICE",
        95,
        "Sweden Tayron R-Line Edition ICE to official configurator row",
    ),
    link_seed(
        SWEDEN,
        "VOLKSWAGEN",
        "Tayron",
        "R-LINE EDITION",
        "PHEV",
        "TAYRON",
        "R-Line Edition",
        "",
        "PHEV",
        95,
        "Sweden Tayron R-Line Edition PHEV to official configurator row",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "T-Roc",
        "LIFE",
        "MHEV",
        "T-ROC",
        "Life",
        "",
        "MHEV",
        95,
        "Germany T-Roc Life MHEV to official configurator row",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "T-Roc",
        "STYLE",
        "MHEV",
        "T-ROC",
        "Style",
        "",
        "MHEV",
        95,
        "Germany T-Roc Style MHEV to official configurator row",
    ),
    link_seed(
        GERMANY,
        "VOLKSWAGEN",
        "T-Roc",
        "R-LINE",
        "MHEV",
        "T-ROC",
        "R-Line",
        "",
        "MHEV",
        95,
        "Germany T-Roc R-Line MHEV to official configurator row",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "Kodiaq",
        "SELECTION",
        "ICE",
        "KODIAQ",
        "Kodiaq",
        "",
        "",
        84,
        "Germany Kodiaq Selection ICE collapsed to official entry MSRP row",
    ),
    link_seed(
        GERMANY,
        "SKODA",
        "Kodiaq",
        "SELECTION",
        "MHEV",
        "KODIAQ",
        "Kodiaq",
        "",
        "",
        84,
        "Germany Kodiaq Selection MHEV collapsed to official entry MSRP row",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Seed MSRP current prices and explicit JATO links for "
            "Germany and Sweden."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Write sources, current prices, and explicit links to the "
            "configured database."
        ),
    )
    return parser.parse_args()


def build_batch_code(
    country: str,
    *,
    created_at: datetime,
) -> str:
    return (
        f"{SEED_LABEL.lower()}-"
        f"{country.lower()}-"
        f"{created_at:%Y%m%d%H%M%S%f}"
    )


def resolve_repo_path(relative_path: str) -> Path:
    path = (REPO_ROOT / relative_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Seed source file not found: {relative_path}")
    return path


def build_link_seed_row(
    item: LinkSeed,
    *,
    created_at: datetime,
) -> dict[str, Any]:
    return {
        "link_id": uuid4(),
        "country": item.country,
        "brand": item.brand,
        "jato_model": item.jato_model,
        "jato_trim": item.jato_trim,
        "jato_powertrain": item.jato_powertrain,
        "official_model": item.official_model,
        "official_trim": item.official_trim,
        "official_edition": item.official_edition,
        "official_powertrain": item.official_powertrain,
        "confidence": item.confidence,
        "link_source": SEED_LABEL,
        "is_active": True,
        "notes": item.notes,
        "created_at_utc": created_at,
        "updated_at_utc": created_at,
    }


def _payload_identity(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        str(payload.get("country") or ""),
        str(payload.get("brand") or ""),
        str(payload.get("jato_model") or ""),
        str(payload.get("jato_trim") or ""),
        str(payload.get("jato_powertrain") or ""),
        str(payload.get("official_model") or ""),
        str(payload.get("official_trim") or ""),
        str(payload.get("official_edition") or ""),
        str(payload.get("official_powertrain") or ""),
        float(payload.get("msrp_value") or 0.0),
        str(payload.get("currency") or ""),
        str(payload.get("source_url") or ""),
        str(payload.get("source_id") or ""),
    )


def _payload_business_key(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        str(payload.get("country") or ""),
        str(payload.get("brand") or ""),
        str(payload.get("jato_model") or ""),
        str(payload.get("jato_trim") or ""),
        str(payload.get("jato_powertrain") or ""),
    )


def _payload_source_priority(payload: dict[str, Any]) -> int:
    match_reason = payload.get("match_reason_json") or {}
    if not isinstance(match_reason, dict):
        return 0
    source_kind = str(match_reason.get("source") or "")
    if source_kind == "official_source_file":
        return 2
    if source_kind == "EVKX":
        return 1
    return 0


def _payload_preference_key(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _payload_source_priority(payload),
        float(payload.get("match_confidence") or 0.0),
        1 if str(payload.get("official_powertrain") or "").strip() else 0,
        1 if str(payload.get("official_edition") or "").strip() else 0,
        -float(payload.get("msrp_value") or 0.0),
        str(payload.get("source_url") or ""),
    )


def dedupe_observation_payloads(
    payloads: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    unique_by_identity: dict[tuple[Any, ...], dict[str, Any]] = {}
    for payload in payloads:
        identity = _payload_identity(payload)
        unique_by_identity.setdefault(identity, payload)

    unique_by_business_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for payload in unique_by_identity.values():
        business_key = _payload_business_key(payload)
        existing = unique_by_business_key.get(business_key)
        if existing is None:
            unique_by_business_key[business_key] = payload
            continue
        if _payload_preference_key(payload) > _payload_preference_key(
            existing
        ):
            unique_by_business_key[business_key] = payload

    return list(unique_by_business_key.values())


def apply_seed_metadata(
    payload: dict[str, Any],
    *,
    country: str,
    brand: str,
    observed_at: datetime,
    source_kind: str,
    source_ref: str,
    source_code: str,
) -> dict[str, Any]:
    payload["country"] = country
    payload["brand"] = brand
    payload["observed_at_utc"] = observed_at
    payload["match_status"] = "human_approved"

    match_reason = dict(payload.get("match_reason_json") or {})
    match_reason.update(
        {
            "seed": SEED_LABEL,
            "approval": "manual_seed",
            "source": source_kind,
            "sourceCode": source_code,
            "sourceRef": source_ref,
        }
    )
    payload["match_reason_json"] = match_reason

    source_context = dict(payload.get("source_context_json") or {})
    source_context.update(
        {
            "seed": SEED_LABEL,
            "source": source_kind,
            "sourceCode": source_code,
            "sourceRef": source_ref,
        }
    )
    payload["source_context_json"] = source_context
    return payload


def ensure_source(
    session: Any,
    *,
    source_code: str,
    country: str,
    brand: str,
    source_url: str,
    source_type: str,
    tier: int,
    extractor_name: str,
    extractor_version: str,
    price_semantics: str,
    notes: str,
) -> MsrpSource:
    source = session.execute(
        select(MsrpSource).where(MsrpSource.source_code == source_code)
    ).scalar_one_or_none()
    if source is None:
        source = MsrpSource(
            source_code=source_code,
            country=country,
            brand=brand,
            source_url=source_url,
            source_type=source_type,
            tier=tier,
            extractor_name=extractor_name,
            extractor_version=extractor_version,
            price_semantics=price_semantics,
            requires_location=False,
            enabled=True,
            notes=notes,
        )
        session.add(source)
        session.flush()
        return source
    source.country = country
    source.brand = brand
    source.source_url = source_url
    source.source_type = source_type
    source.tier = tier
    source.extractor_name = extractor_name
    source.extractor_version = extractor_version
    source.price_semantics = price_semantics
    source.requires_location = False
    source.enabled = True
    source.notes = notes
    session.flush()
    return source


def load_all_evkx_search_items(country: str) -> dict[str, dict[str, Any]]:
    items: dict[str, dict[str, Any]] = {}
    session = requests.Session()
    session.headers.update({"User-Agent": "JATO-EVKX-Seed/0.1"})
    page = 1
    while True:
        response = session.post(
            EVKX_SEARCH_API_URL,
            json={
                "page": page,
                "pageSize": 100,
                "sortOrder": "Name",
                "availabilityFilter": "current",
                "pricingCountry": country,
            },
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        for item in payload.get("evs") or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if name:
                items[name] = item
        if not payload.get("hasNextPage"):
            return items
        page += 1


def build_evkx_observation_payload(
    seed: EvkxVariantSeed,
    item: dict[str, Any],
    source_id: str,
    observed_at: datetime,
) -> dict[str, Any]:
    start_price = item.get("startPrice")
    currency = str(item.get("currency") or "").strip().upper()
    if start_price in (None, "") or not currency:
        raise ValueError(
            f"Missing EVKX price for {seed.country} / {seed.name}"
        )
    if bool(item.get("isConverted")):
        raise ValueError(
            "Converted EVKX market is not allowed for first-batch "
            f"native seed: {seed.country} / {seed.name}"
        )
    if str(item.get("pricingCountry") or "").strip() != seed.country:
        raise ValueError(
            "Unexpected EVKX pricing country for "
            f"{seed.country} / {seed.name}: {item.get('pricingCountry')}"
        )
    source_url = urljoin(EVKX_BASE_URL, str(item.get("infoUri") or ""))
    payload_hash = sha256(
        (
            f"{SEED_LABEL}|{seed.country}|{seed.brand}|{seed.name}|"
            f"{start_price}|{currency}"
        ).encode("utf-8")
    ).hexdigest()[:16]
    return {
        "source_id": source_id,
        "country": seed.country,
        "brand": seed.brand,
        "jato_model": seed.jato_model,
        "jato_trim": seed.official_trim,
        "jato_powertrain": "BEV",
        "official_model": seed.official_model,
        "official_trim": seed.official_trim,
        "official_edition": None,
        "official_powertrain": "BEV",
        "msrp_value": float(start_price),
        "currency": currency,
        "tax_included": False,
        "price_label": "EVKX reference MSRP",
        "availability_text": "current",
        "observed_at_utc": observed_at,
        "source_url": source_url,
        "source_snapshot_path": None,
        "source_payload_hash": payload_hash,
        "extraction_version": "evkx_search_seed_v1",
        "match_confidence": 0.95,
        "match_status": "human_approved",
        "match_reason_json": {
            "seed": SEED_LABEL,
            "approval": "manual_seed",
            "source": "EVKX",
            "vehicleName": seed.name,
            "pricingCountry": seed.country,
            "isConverted": False,
        },
        "source_context_json": {
            "source": "EVKX",
            "seed": SEED_LABEL,
            "vehicleName": seed.name,
            "pricingCountry": seed.country,
            "isConverted": False,
            "infoUrl": source_url,
        },
        "candidate_matches_json": None,
    }


def build_source_file_observations(
    session: Any,
    source_seed: SourceFileSeed,
    observed_at: datetime,
) -> list[dict[str, Any]]:
    source_path = resolve_repo_path(source_seed.relative_path)
    source_code = load_source_file(source_path)
    if not source_code:
        raise ValueError(
            f"Failed to load source file: {source_seed.relative_path}"
        )

    extractor = registry.get(source_code)
    source = ensure_source(
        session,
        source_code=source_code,
        country=source_seed.country,
        brand=source_seed.brand,
        source_url=extractor.config.source_url,
        source_type=extractor.config.source_type,
        tier=source_seed.tier,
        extractor_name=extractor.extractor_name,
        extractor_version=extractor.extractor_version,
        price_semantics=extractor.config.price_semantics,
        notes=source_seed.notes,
    )
    last_error: Exception | None = None
    report = None
    for attempt in range(1, SOURCE_FILE_EXTRACT_MAX_ATTEMPTS + 1):
        extractor = registry.get(source_code)
        try:
            observations = extractor.extract()
            report = validate_observations(
                observations,
                country=extractor.config.country,
            )
            if not report.valid:
                raise ValueError(
                    f"No valid observations for {source_seed.relative_path}"
                )
            break
        except Exception as exc:
            last_error = exc
            if attempt >= SOURCE_FILE_EXTRACT_MAX_ATTEMPTS:
                raise
            log.warning(
                (
                    "Retrying source extract for %s after "
                    "attempt %s/%s failed: %s"
                ),
                source_seed.relative_path,
                attempt,
                SOURCE_FILE_EXTRACT_MAX_ATTEMPTS,
                exc,
            )
            time.sleep(SOURCE_FILE_EXTRACT_RETRY_DELAY_SECONDS)

    if report is None or not report.valid:
        if last_error is not None:
            raise last_error
        raise ValueError(
            f"No valid observations for {source_seed.relative_path}"
        )

    enrich_observations_with_eur(report.valid)
    payloads: list[dict[str, Any]] = []
    for observation in report.valid:
        payload = _observation_to_ingest_dict(
            observation,
            str(source.source_id),
            extractor,
        )
        payloads.append(
            apply_seed_metadata(
                payload,
                country=source_seed.country,
                brand=source_seed.brand,
                observed_at=observed_at,
                source_kind="official_source_file",
                source_ref=source_seed.relative_path,
                source_code=source_code,
            )
        )
    return dedupe_observation_payloads(payloads)


def build_country_seed_payloads(
    session: Any,
) -> dict[str, list[dict[str, Any]]]:
    observed_at = datetime.now(timezone.utc)
    payloads: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for source_seed in OFFICIAL_SOURCE_FILE_SEEDS:
        payloads[source_seed.country].extend(
            build_source_file_observations(
                session,
                source_seed,
                observed_at,
            )
        )

    evkx_items_by_country = {
        SWEDEN: load_all_evkx_search_items(SWEDEN),
        GERMANY: load_all_evkx_search_items(GERMANY),
    }
    for seed in EVKX_VARIANT_SEEDS:
        item = evkx_items_by_country[seed.country].get(seed.name)
        if item is None:
            raise ValueError(
                f"Missing EVKX search item for {seed.country} / {seed.name}"
            )
        source_code = (
            f"evkx_{seed.country.lower()}_{seed.brand.lower()}_catalog"
        )
        source = ensure_source(
            session,
            source_code=source_code,
            country=seed.country,
            brand=seed.brand,
            source_url=(
                "https://evkx.net/evsearch/?page=1&pageSize=100&sortOrder=Name"
                f"&availabilityFilter=current&pricingCountry={seed.country}"
            ),
            source_type="reference_catalog",
            tier=2,
            extractor_name="evkx_search_seed",
            extractor_version="v1",
            price_semantics="reference_msrp_tax_unknown",
            notes="Native EVKX search seed for first-batch JATO MSRP links",
        )
        payloads[seed.country].append(
            build_evkx_observation_payload(
                seed,
                item,
                str(source.source_id),
                observed_at,
            )
        )

    return {
        country: dedupe_observation_payloads(observations)
        for country, observations in payloads.items()
    }


def upsert_link_seeds(session: Any) -> int:
    created_at = datetime.now(timezone.utc)
    values = [
        build_link_seed_row(item, created_at=created_at)
        for item in LINK_SEEDS
    ]
    stmt = insert(JatoMsrpLink).values(values)
    session.execute(
        stmt.on_conflict_do_update(
            constraint="uq_jato_msrp_links_business_key",
            set_={
                "confidence": stmt.excluded.confidence,
                "link_source": stmt.excluded.link_source,
                "is_active": stmt.excluded.is_active,
                "notes": stmt.excluded.notes,
                "updated_at_utc": func.now(),
            },
        )
    )
    desired_keys = {_link_seed_business_key(item) for item in LINK_SEEDS}
    existing_seed_rows = list(
        session.execute(
            select(
                JatoMsrpLink.link_id,
                JatoMsrpLink.country,
                JatoMsrpLink.brand,
                JatoMsrpLink.jato_model,
                JatoMsrpLink.jato_trim,
                JatoMsrpLink.jato_powertrain,
                JatoMsrpLink.official_model,
                JatoMsrpLink.official_trim,
                JatoMsrpLink.official_edition,
                JatoMsrpLink.official_powertrain,
            ).where(
                JatoMsrpLink.link_source == SEED_LABEL,
                JatoMsrpLink.is_active.is_(True),
            )
        )
    )
    stale_ids = _find_stale_link_seed_ids(existing_seed_rows, desired_keys)
    if stale_ids:
        session.execute(
            update(JatoMsrpLink)
            .where(JatoMsrpLink.link_id.in_(stale_ids))
            .values(
                is_active=False,
                updated_at_utc=func.now(),
            )
        )
    session.commit()
    return len(values)


def ingest_country_payloads(
    session: Any,
    payloads: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for country, observations in payloads.items():
        batch_started_at = datetime.now(timezone.utc)
        scope_brands = sorted(
            {
                str(item["brand"]).strip()
                for item in observations
                if str(item["brand"]).strip()
            }
        )
        batch = {
            "batch_code": build_batch_code(
                country,
                created_at=batch_started_at,
            ),
            "trigger_type": "manual_seed",
            "scope_country": country,
            "scope_brands": scope_brands,
            "failed_count": 0,
            "notes": f"{SEED_LABEL} current-price seed for {country}",
            "started_at_utc": batch_started_at,
            "finished_at_utc": batch_started_at,
            "observations": observations,
        }
        results.append(
            {
                "country": country,
                "brands": scope_brands,
                "observationCount": len(observations),
                "result": create_scrape_batch_ingest(session, batch),
            }
        )
    return results


def print_plan(payloads: dict[str, list[dict[str, Any]]]) -> None:
    for country, observations in payloads.items():
        print(f"COUNTRY {country}: observations={len(observations)}")
        for item in observations[:20]:
            print(
                {
                    "brand": item["brand"],
                    "jatoModel": item["jato_model"],
                    "jatoTrim": item["jato_trim"],
                    "officialModel": item["official_model"],
                    "officialTrim": item["official_trim"],
                    "powertrain": item["jato_powertrain"],
                    "price": item["msrp_value"],
                    "currency": item["currency"],
                    "sourceId": item["source_id"],
                }
            )
    print(f"LINKS {len(LINK_SEEDS)}")
    for item in LINK_SEEDS[:20]:
        print(
            {
                "country": item.country,
                "brand": item.brand,
                "jatoModel": item.jato_model,
                "jatoTrim": item.jato_trim,
                "officialModel": item.official_model,
                "officialTrim": item.official_trim,
                "confidence": item.confidence,
            }
        )


def main() -> int:
    args = parse_args()
    session = get_session_factory()()
    try:
        payloads = build_country_seed_payloads(session)
        if not args.apply:
            print_plan(payloads)
            session.rollback()
            return 0
        ingest_results = ingest_country_payloads(session, payloads)
        link_count = upsert_link_seeds(session)
        print({
            "seed": SEED_LABEL,
            "countryResults": [
                {
                    "country": item["country"],
                    "brands": item["brands"],
                    "observationCount": item["observationCount"],
                    "currentPricesTouched": item["result"].get(
                        "currentPricesTouched"
                    ),
                    "batchCode": item["result"].get(
                        "scrapeBatch",
                        {},
                    ).get("batchCode"),
                }
                for item in ingest_results
            ],
            "linkRowsUpserted": link_count,
        })
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
