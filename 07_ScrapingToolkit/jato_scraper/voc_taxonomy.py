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
    return {
        "profile": normalized,
        "schemaVersion": "voc-core-v1",
        "sentimentLabels": list(_BASE_SENTIMENT_LABELS),
        "ownershipStages": list(_BASE_OWNERSHIP_STAGES),
        "productSignals": list(_BASE_PRODUCT_SIGNALS),
        "painPoints": list(override["painPoints"]),
        "focusThemes": list(override["focusThemes"]),
        "extractionFields": list(_BASE_EXTRACTION_FIELDS),
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
