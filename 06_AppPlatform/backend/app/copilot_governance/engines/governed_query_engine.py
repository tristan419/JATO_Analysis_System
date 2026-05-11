"""Governed Query Engine — executes source plans against existing data."""

from __future__ import annotations

from typing import Any


def execute_source_plan(
    source_plan=None,
    snapshot: dict[str, Any] | None = None,
    country: str = "",
) -> list[dict[str, Any]]:
    """Execute a source plan against available snapshot data.

    Returns a list of GovernedResult-like dicts, one per source in the plan.
    """
    snapshot = snapshot or {}
    results: list[dict[str, Any]] = []

    items = getattr(source_plan, "items", []) if source_plan else []
    for item in items:
        result = _execute_source_item(item.source_id, item.source_lane, snapshot, country)
        results.append(result)

    return results


def _execute_source_item(
    source_id: str,
    source_lane: str,
    snapshot: dict[str, Any],
    country: str,
) -> dict[str, Any]:
    base = {
        "source_id": source_id,
        "source_lane": source_lane,
        "status": "ok",
        "rows": [],
        "summary": {},
        "confidence": "medium",
        "limitations": [],
    }

    if source_id == "jato_sales_parquet":
        rows = []
        for brand in snapshot.get("topBrands", [])[:10]:
            if isinstance(brand, dict):
                rows.append({"brand": brand.get("label", ""), "volume": brand.get("value", 0)})
        return {
            **base,
            "status": "ok" if rows else "empty",
            "rows": rows,
            "summary": {
                "total_brands": snapshot.get("kpis", {}).get("brandCount", 0),
                "period": snapshot.get("periodLabel", ""),
            },
            "confidence": "high",
        }

    if source_id == "current_price_postgres":
        precise = snapshot.get("preciseLookup", {})
        if isinstance(precise, dict) and precise:
            return {
                **base,
                "status": "ok",
                "rows": precise.get("items", []) if isinstance(precise.get("items"), list) else [],
                "summary": {"models_matched": len(precise.get("matchedModels", []))},
                "confidence": "high",
            }
        return {
            **base,
            "status": "empty",
            "summary": {},
            "confidence": "medium",
            "limitations": ["No MSRP lookup data in current snapshot. Use MSRP lookup API for precise queries."],
        }

    if source_id == "voc_forum_artifacts":
        try:
            from app.services.customer_insight_service import query_nordic_customer_deck
            deck = query_nordic_customer_deck(mode="forum_live", country_codes=[country[:2].upper()] if country else None)
            observations: list[dict[str, Any]] = []
            page = deck.get("page", {})
            if isinstance(page, dict):
                for section_name, section_data in page.items():
                    if isinstance(section_data, list):
                        for item in section_data:
                            if isinstance(item, dict) and item.get("observation"):
                                observations.append({
                                    "section": section_name,
                                    "observation": item.get("observation", ""),
                                    "theme": item.get("theme", ""),
                                    "persona": item.get("persona", ""),
                                })
            return {
                **base,
                "status": "ok" if observations else "partial",
                "rows": observations[:20] if observations else [],
                "summary": {
                    "observation_count": len(observations),
                    "source": "VOC forum_live + benchmark deck",
                },
                "confidence": "low",
                "limitations": [
                    "VOC data is qualitative and cannot represent full market statistics.",
                    "Forum data may have selection bias.",
                ] if observations else ["No VOC data available for this country."],
            }
        except Exception as exc:
            return {
                **base,
                "status": "empty",
                "summary": {},
                "confidence": "low",
                "limitations": [f"VOC query failed: {exc}"],
            }

    if source_id == "country_profiles":
        from app.copilot_governance.policy_service import load_policy_rules
        from app.copilot_governance.tax_calculator import calculate_country_taxes

        rules = load_policy_rules(country)
        tax_est = calculate_country_taxes(country)
        rows = [
            {"rule_id": r.id, "name": r.name, "type": r.type, "confidence": r.confidence}
            for r in rules
        ]
        return {
            **base,
            "status": "ok" if rows else "empty",
            "rows": rows,
            "summary": {
                "rule_count": len(rules),
                "tax_estimate": tax_est.model_dump() if tax_est else None,
            },
            "confidence": "high" if rows else "low",
        }

    if source_id == "news_digest":
        news = snapshot.get("newsDigest")
        if news:
            return {
                **base,
                "status": "ok",
                "rows": [{"headline": news.get("headline", "")}],
                "summary": {"article_count": news.get("articleCount", 0)},
                "confidence": "medium",
            }
        return {
            **base,
            "status": "empty",
            "summary": {},
            "confidence": "low",
            "limitations": ["No news digest available in current snapshot."],
        }

    return {
        **base,
        "status": "error",
        "limitations": [f"Unknown source: {source_id}"],
    }
