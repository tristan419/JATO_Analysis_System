"""VOC taxonomy and source-collection defaults."""

from __future__ import annotations

from typing import Any

_BASE_EXTRACTION_FIELDS = (
    "sourceCode",
    "countryCode",
    "countryLabel",
    "siteName",
    "siteType",
    "language",
    "url",
    "title",
    "author",
    "publishedAt",
    "rawText",
    "translatedText",
    "sentiment",
    "ownershipStage",
    "painPoints",
    "productMentions",
    "competitorMentions",
    "evidenceSnippets",
)

_BASE_SENTIMENT_LABELS = (
    "positive",
    "neutral",
    "mixed",
    "negative",
)

_BASE_OWNERSHIP_STAGES = (
    "shopping",
    "ordering_delivery",
    "daily_use",
    "charging_energy",
    "service_after_sales",
    "quality_reliability",
)

_BASE_PRODUCT_SIGNALS = (
    "price_value",
    "range_efficiency",
    "charging_speed",
    "software_ui",
    "reliability_quality",
    "service_after_sales",
    "comfort_space",
)

_BASE_SCORING_DIMENSIONS = (
    "relevance_score",
    "persona_score",
    "match_confidence",
    "overall_score",
)

_BASE_CROSS_ANALYSIS_AXES = (
    {"key": "product_vs_pain_point", "label": "Product × pain point"},
    {"key": "persona_vs_decision_factor", "label": "Persona × decision factor"},
    {"key": "theme_vs_source_type", "label": "Theme × source type"},
    {"key": "attribute_affinity", "label": "Attribute affinity / synergy matrix"},
    {"key": "filter_recommendation", "label": "Filter recommendation graph"},
)

_PROFILE_OVERRIDES: dict[str, dict[str, Any]] = {
    "nordic_core": {
        "painPoints": (
            "winter_range",
            "charging_queue",
            "public_charging_reliability",
            "software_bug",
            "service_wait_time",
            "price_value",
            "delivery_delay",
        ),
        "focusThemes": (
            "cold_weather_usability",
            "home_vs_public_charging",
            "software_quality",
            "fleet_tax_incentives",
        ),
    },
    "cee_core": {
        "painPoints": (
            "price_affordability",
            "charging_coverage",
            "battery_confidence",
            "service_quality",
            "parts_availability",
            "resale_value",
            "delivery_delay",
        ),
        "focusThemes": (
            "ownership_cost",
            "charging_infrastructure_gaps",
            "value_for_money",
            "brand_trust",
        ),
    },
    "dach_core": {
        "painPoints": (
            "price_increase",
            "fleet_tax_policy",
            "charging_tariff",
            "software_quality",
            "service_experience",
            "quality_recall",
            "leasing_residual_value",
        ),
        "focusThemes": (
            "fleet_and_company_car",
            "premium_quality_expectation",
            "charging_cost",
            "aftersales_experience",
        ),
    },
}

_PROFILE_ANALYSIS_OVERRIDES: dict[str, dict[str, Any]] = {
    "nordic_core": {
        "themeTags": (
            {
                "key": "winter_usability",
                "label": "Winter usability",
                "keywords": (
                    "winter",
                    "cold weather",
                    "snow",
                    "ice",
                    "heat pump",
                    "awd",
                    "kaldt",
                    "vinter",
                ),
            },
            {
                "key": "charging_experience",
                "label": "Charging experience",
                "keywords": (
                    "charging",
                    "charger",
                    "fast charging",
                    "home charging",
                    "public charging",
                    "queue",
                    "lader",
                    "hurtiglading",
                    "snabbladd",
                ),
            },
            {
                "key": "software_digital",
                "label": "Software / digital",
                "keywords": (
                    "software",
                    "ota",
                    "app",
                    "infotainment",
                    "carplay",
                    "android auto",
                    "programvare",
                ),
            },
            {
                "key": "ownership_cost",
                "label": "Ownership cost",
                "keywords": (
                    "tco",
                    "lease",
                    "leasing",
                    "budget",
                    "price",
                    "discount",
                    "subsid",
                    "cost",
                    "pris",
                ),
            },
            {
                "key": "family_practicality",
                "label": "Family practicality",
                "keywords": (
                    "family",
                    "children",
                    "school run",
                    "space",
                    "cargo",
                    "trunk",
                    "third row",
                    "tow",
                    "boat",
                    "caravan",
                ),
            },
            {
                "key": "service_reliability",
                "label": "Service / reliability",
                "keywords": (
                    "service",
                    "dealer",
                    "warranty",
                    "repair",
                    "quality",
                    "reliability",
                    "recall",
                    "verkstad",
                ),
            },
        ),
        "personaCohorts": (
            {
                "key": "winter_commuter",
                "label": "Winter commuter",
                "keywords": (
                    "commute",
                    "winter",
                    "cold weather",
                    "daily use",
                    "school run",
                ),
            },
            {
                "key": "family_hauler",
                "label": "Family hauler",
                "keywords": (
                    "family",
                    "children",
                    "school run",
                    "cargo",
                    "trunk",
                    "third row",
                    "caravan",
                    "boat",
                ),
            },
            {
                "key": "cost_guarded_switcher",
                "label": "Cost-guarded switcher",
                "keywords": (
                    "tco",
                    "budget",
                    "subsid",
                    "lease",
                    "leasing",
                    "price",
                    "cost",
                ),
            },
            {
                "key": "tech_forward_driver",
                "label": "Tech-forward driver",
                "keywords": (
                    "software",
                    "ota",
                    "app",
                    "carplay",
                    "android auto",
                    "infotainment",
                ),
            },
            {
                "key": "service_sensitive_owner",
                "label": "Service-sensitive owner",
                "keywords": (
                    "service",
                    "dealer",
                    "repair",
                    "warranty",
                    "delivery",
                    "after sales",
                ),
            },
        ),
        "productCatalog": (
            {
                "key": "tesla_model_y",
                "label": "Tesla Model Y",
                "aliases": ("tesla model y", "model y", "tesla y"),
            },
            {
                "key": "volkswagen_id4",
                "label": "Volkswagen ID.4",
                "aliases": ("volkswagen id.4", "vw id.4", "id.4", "id4"),
            },
            {
                "key": "volvo_ex30",
                "label": "Volvo EX30",
                "aliases": ("volvo ex30", "ex30"),
            },
            {
                "key": "volvo_xc60",
                "label": "Volvo XC60",
                "aliases": ("volvo xc60", "xc60"),
            },
            {
                "key": "skoda_enyaq",
                "label": "Skoda Enyaq",
                "aliases": ("skoda enyaq", "enyaq"),
            },
            {
                "key": "kia_ev6",
                "label": "Kia EV6",
                "aliases": ("kia ev6", "ev6"),
            },
            {
                "key": "hyundai_ioniq5",
                "label": "Hyundai Ioniq 5",
                "aliases": ("hyundai ioniq 5", "ioniq 5"),
            },
            {
                "key": "toyota_bz4x",
                "label": "Toyota bZ4X",
                "aliases": ("toyota bz4x", "bz4x"),
            },
            {
                "key": "bmw_ix1",
                "label": "BMW iX1",
                "aliases": ("bmw ix1", "ix1"),
            },
            {
                "key": "audi_q4_etron",
                "label": "Audi Q4 e-tron",
                "aliases": ("audi q4 e-tron", "q4 e-tron", "q4 etron"),
            },
        ),
    },
    "cee_core": {
        "themeTags": (
            {
                "key": "value_for_money",
                "label": "Value for money",
                "keywords": ("value", "budget", "affordable", "price", "leasing", "cost"),
            },
            {
                "key": "charging_gap",
                "label": "Charging infrastructure gap",
                "keywords": ("charging", "charger", "coverage", "public charging", "queue"),
            },
            {
                "key": "brand_trust",
                "label": "Brand trust",
                "keywords": ("brand", "trust", "quality", "reliability", "service"),
            },
        ),
        "personaCohorts": (
            {
                "key": "budget_switcher",
                "label": "Budget switcher",
                "keywords": ("budget", "lease", "price", "affordable", "subsid"),
            },
            {
                "key": "infrastructure_cautious_driver",
                "label": "Infrastructure-cautious driver",
                "keywords": ("charging", "coverage", "range", "queue", "network"),
            },
        ),
        "productCatalog": (
            {
                "key": "tesla_model_y",
                "label": "Tesla Model Y",
                "aliases": ("tesla model y", "model y"),
            },
            {
                "key": "volkswagen_id4",
                "label": "Volkswagen ID.4",
                "aliases": ("volkswagen id.4", "vw id.4", "id.4", "id4"),
            },
            {
                "key": "skoda_enyaq",
                "label": "Skoda Enyaq",
                "aliases": ("skoda enyaq", "enyaq"),
            },
            {
                "key": "mg4",
                "label": "MG4",
                "aliases": ("mg4", "mg 4"),
            },
            {
                "key": "dacia_spring",
                "label": "Dacia Spring",
                "aliases": ("dacia spring", "spring electric"),
            },
        ),
    },
    "dach_core": {
        "themeTags": (
            {
                "key": "fleet_policy",
                "label": "Fleet / company car policy",
                "keywords": ("fleet", "company car", "leasing", "tax policy", "residual value"),
            },
            {
                "key": "premium_quality",
                "label": "Premium quality",
                "keywords": ("premium", "quality", "finish", "komfort", "comfort"),
            },
            {
                "key": "charging_cost",
                "label": "Charging cost",
                "keywords": ("charging tariff", "charging cost", "strom", "laden", "cost"),
            },
        ),
        "personaCohorts": (
            {
                "key": "fleet_manager_proxy",
                "label": "Fleet-manager proxy",
                "keywords": ("fleet", "company car", "leasing", "policy", "residual value"),
            },
            {
                "key": "premium_ev_buyer",
                "label": "Premium EV buyer",
                "keywords": ("premium", "quality", "komfort", "digital cockpit", "service"),
            },
        ),
        "productCatalog": (
            {
                "key": "audi_q4_etron",
                "label": "Audi Q4 e-tron",
                "aliases": ("audi q4 e-tron", "q4 e-tron", "q4 etron"),
            },
            {
                "key": "bmw_ix1",
                "label": "BMW iX1",
                "aliases": ("bmw ix1", "ix1"),
            },
            {
                "key": "mercedes_eqb",
                "label": "Mercedes EQB",
                "aliases": ("mercedes eqb", "eqb"),
            },
            {
                "key": "volkswagen_id4",
                "label": "Volkswagen ID.4",
                "aliases": ("volkswagen id.4", "vw id.4", "id.4", "id4"),
            },
            {
                "key": "skoda_enyaq",
                "label": "Skoda Enyaq",
                "aliases": ("skoda enyaq", "enyaq"),
            },
        ),
    },
}

_SOURCE_STRATEGIES: dict[str, dict[str, Any]] = {
    "forum": {
        "primaryUnit": "thread",
        "contentTargets": (
            "thread_title",
            "opening_post",
            "reply_posts",
        ),
        "extractionTargets": (
            "pain_points",
            "ownership_stage",
            "model_mentions",
            "competitor_mentions",
        ),
    },
    "ev_community": {
        "primaryUnit": "discussion_thread",
        "contentTargets": (
            "thread_title",
            "opening_post",
            "reply_posts",
            "charging_discussion",
        ),
        "extractionTargets": (
            "charging_experience",
            "range_feedback",
            "software_feedback",
            "ownership_stage",
        ),
    },
    "media_comments": {
        "primaryUnit": "article_comment_page",
        "contentTargets": (
            "article_title",
            "article_summary",
            "reader_comments",
        ),
        "extractionTargets": (
            "reader_reaction",
            "pain_points",
            "competitive_mentions",
            "launch_feedback",
        ),
    },
    "consumer_media": {
        "primaryUnit": "consumer_editorial_page",
        "contentTargets": (
            "article_title",
            "article_body",
            "public_comments",
        ),
        "extractionTargets": (
            "complaint_signal",
            "service_feedback",
            "ownership_cost",
            "quality_signal",
        ),
    },
    "industry_media": {
        "primaryUnit": "industry_article_page",
        "contentTargets": (
            "article_title",
            "article_body",
            "public_comments",
        ),
        "extractionTargets": (
            "dealer_feedback",
            "service_signal",
            "pricing_feedback",
            "delivery_signal",
        ),
    },
}


def get_voc_taxonomy_profile(profile_name: str) -> dict[str, Any]:
    normalized = str(profile_name or "").strip()
    if normalized not in _PROFILE_OVERRIDES:
        available = ", ".join(sorted(_PROFILE_OVERRIDES))
        raise ValueError(
            f"Unknown VOC taxonomy profile: {normalized!r}. Available: {available}",
        )
    override = _PROFILE_OVERRIDES[normalized]
    analysis = _PROFILE_ANALYSIS_OVERRIDES.get(normalized, {})
    return {
        "profile": normalized,
        "schemaVersion": "voc-core-v1",
        "sentimentLabels": list(_BASE_SENTIMENT_LABELS),
        "ownershipStages": list(_BASE_OWNERSHIP_STAGES),
        "productSignals": list(_BASE_PRODUCT_SIGNALS),
        "painPoints": list(override["painPoints"]),
        "focusThemes": list(override["focusThemes"]),
        "extractionFields": list(_BASE_EXTRACTION_FIELDS),
        "themeTags": [dict(item) for item in analysis.get("themeTags", ())],
        "personaCohorts": [dict(item) for item in analysis.get("personaCohorts", ())],
        "productCatalog": [dict(item) for item in analysis.get("productCatalog", ())],
        "crossAnalysisAxes": [dict(item) for item in _BASE_CROSS_ANALYSIS_AXES],
        "scoringDimensions": list(_BASE_SCORING_DIMENSIONS),
    }


def get_source_collection_strategy(site_type: str) -> dict[str, Any]:
    normalized = str(site_type or "").strip() or "forum"
    strategy = _SOURCE_STRATEGIES.get(normalized, _SOURCE_STRATEGIES["forum"])
    return {
        "siteType": normalized,
        "primaryUnit": strategy["primaryUnit"],
        "contentTargets": list(strategy["contentTargets"]),
        "extractionTargets": list(strategy["extractionTargets"]),
    }
