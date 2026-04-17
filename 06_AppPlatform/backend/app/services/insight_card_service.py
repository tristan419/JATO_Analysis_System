"""Structured insight-card generator for the Country Copilot.

Each card is a self-contained analytical conclusion derived from the
Market Scan deck.  Cards are keyed by ``(country, period, category)``
and cached in memory with the same 5-minute TTL used by the deck cache.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from app.services import market_scan_service

log = logging.getLogger(__name__)

# --------------- types ---------------

InsightCard = dict[str, Any]
"""Lightweight dict so it serialises to JSON directly.

Keys: id, country, period, category, title, conclusion,
      supportingData, tone, relatedChartLink, tags
"""

# --------------- cache ---------------

_CACHE_TTL = 300  # seconds – matches deck cache
_insight_cache: dict[str, tuple[float, list[InsightCard]]] = {}
_cache_lock = threading.Lock()

# --------------- category → deep-link mapping ---------------

_CHART_LINK_TEMPLATES: dict[str, str] = {
    "overview": "/market-scan?country={country}&activePage=overview",
    "brand": "/market-scan?country={country}&activePage=overview",
    "segment": "/market-scan?country={country}&activePage=segment",
    "origin": "/market-scan?country={country}&activePage=origin",
    "nev": "/market-scan?country={country}&activePage=overview&fuelTypes=BEV,PHEV,HEV",
    "trend": "/market-scan?country={country}&activePage=overview",
    "drilldown": "/market-scan?country={country}&activePage=drilldown",
    "suv_a": "/market-scan?country={country}&activePage=suvA",
    "suv_b": "/market-scan?country={country}&activePage=suvB",
    "positioning": "/positioning-pricing?country={country}&activePage=overview",
    "price_value": "/?advGroup=price_value&advChart=length_vs_price&country={country}",
    "nev_range": "/?advGroup=nev_analysis&advChart=nev_range_distribution&country={country}",
    "segment_structure": "/?advGroup=market_structure&advChart=segment_share_by_length&country={country}",
    "powertrain_price": "/?advGroup=price_value&advChart=powertrain_vs_price&country={country}",
    "price_migration": "/?advGroup=price_value&advChart=price_migration&country={country}",
    "seasonality": "/?advGroup=market_structure&advChart=seasonality_heatmap&country={country}",
    "tco": "/?advGroup=cost_analysis&advChart=estimated_tco&country={country}",
    "nev_capacity": "/?advGroup=nev_analysis&advChart=nev_capacity_vs_msrp&country={country}",
    "price_per_meter": "/?advGroup=price_value&advChart=price_per_meter&country={country}",
    "sales_vs_price": "/?advGroup=price_value&advChart=sales_vs_price&country={country}",
    "rv_finance": "/?advGroup=cost_analysis&advChart=rv_finance_dashboard&country={country}",
    "powertrain_vs_price_cost": "/?advGroup=cost_analysis&advChart=powertrain_vs_price&country={country}",
}

# intent → card categories that are relevant
INTENT_TO_CATEGORIES: dict[str, list[str]] = {
    "brand-ranking": ["brand", "overview", "positioning", "suv_a", "suv_b"],
    "segment-analysis": ["segment", "overview", "segment_structure", "drilldown", "suv_a", "suv_b"],
    "origin-analysis": ["origin", "overview", "brand", "seasonality"],
    "nev-analysis": ["nev", "nev_range", "nev_capacity", "overview", "powertrain_price"],
    "powertrain-mix": ["nev", "overview", "powertrain_price", "powertrain_vs_price_cost", "nev_capacity"],
    "trend-summary": ["trend", "overview", "price_migration", "seasonality", "sales_vs_price"],
    "pricing-summary": ["overview", "price_value", "price_per_meter", "price_migration", "tco"],
    "competitive": ["brand", "segment", "origin", "positioning", "drilldown", "sales_vs_price"],
    "positioning-analysis": ["positioning", "price_value", "overview", "price_per_meter", "segment_structure"],
    "general-summary": ["overview", "brand", "segment", "segment_structure", "positioning", "rv_finance", "tco", "seasonality"],
}

# --------------- public API ---------------


def get_insight_cards(
    country: str,
    *,
    force: bool = False,
) -> list[InsightCard]:
    """Return cached insight cards for *country*, regenerating if stale."""
    cache_key = country
    now = time.monotonic()

    if not force:
        with _cache_lock:
            entry = _insight_cache.get(cache_key)
            if entry and (now - entry[0]) < _CACHE_TTL:
                return entry[1]

    cards = _generate_cards(country)

    with _cache_lock:
        _insight_cache[cache_key] = (now, cards)

    return cards


def cards_for_intent(
    cards: list[InsightCard],
    intent: str,
) -> list[InsightCard]:
    """Filter *cards* to only those relevant for *intent*."""
    categories = INTENT_TO_CATEGORIES.get(intent, ["overview"])
    return [c for c in cards if c.get("category") in categories]


def chart_links_for_intent(
    cards: list[InsightCard],
    intent: str,
    user_params: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    """Return ``[{label, href}]`` for chart deep-links relevant to *intent*."""
    relevant = cards_for_intent(cards, intent)
    seen: set[str] = set()
    links: list[dict[str, str]] = []
    for card in relevant:
        href = card.get("relatedChartLink", "")
        if href and href not in seen:
            seen.add(href)
            links.append({"label": card.get("title", "查看图表"), "href": href})
    # Also generate direct dashboard links from category templates
    categories = INTENT_TO_CATEGORIES.get(intent, ["overview"])
    country = ""
    if cards:
        first_id = cards[0].get("id", "")
        parts = first_id.split("-")
        if parts:
            country = parts[0]
    for cat in categories:
        template = _CHART_LINK_TEMPLATES.get(cat, "")
        if not template:
            continue
        href = template.format(country=country)
        href = _append_user_params(href, user_params)
        if href and href not in seen:
            seen.add(href)
            links.append({"label": _CATEGORY_LABELS.get(cat, "查看图表"), "href": href})
    return links[:3]  # cap at 3


def chart_links_for_intents(
    cards: list[InsightCard],
    intents: list[str],
    user_params: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    seen: set[str] = set()
    links: list[dict[str, str]] = []
    for intent in intents:
        for link in chart_links_for_intent(cards, intent, user_params):
            href = str(link.get("href", "")).strip()
            if not href or href in seen:
                continue
            seen.add(href)
            links.append(link)
            if len(links) >= 6:
                return links
    return links


# --------------- internal generators ---------------


def _generate_cards(country: str) -> list[InsightCard]:
    """Build insight cards from the Market Scan deck for *country*."""
    try:
        deck = market_scan_service.query_market_scan_deck(
            country=country,
            target_period=None,
            fuel_types=list(market_scan_service.DEFAULT_FUEL_TYPES),
            trend_window_months=24,
            origin_window_months=12,
            body_window_months=12,
            ranking_limit=15,
            drilldown_segment=None,
        )
    except Exception:
        log.warning("Cannot generate insight cards for %s – deck unavailable", country)
        return []

    metadata = deck.get("metadata", {})
    results = deck.get("results", {})
    period = metadata.get("resolvedPeriod", "")

    cards: list[InsightCard] = []
    for factory in (
        _card_from_overview,
        _card_from_brand_ranking,
        _card_from_segment,
        _card_from_origin,
        _card_from_nev,
        _card_from_trend,
    ):
        card = factory(country, period, metadata, results)
        if card:
            cards.append(card)

    return cards


# Human-readable labels for dashboard link categories
_CATEGORY_LABELS: dict[str, str] = {
    "overview": "市场概览",
    "brand": "品牌排名",
    "segment": "细分市场",
    "origin": "品牌产地",
    "nev": "新能源分析",
    "trend": "趋势分析",
    "drilldown": "细分下钻",
    "suv_a": "SUV-A 排名",
    "suv_b": "SUV-B 排名",
    "positioning": "定位气泡图",
    "price_value": "价格分布",
    "nev_range": "续航分布",
    "nev_capacity": "电池容量×定价",
    "segment_structure": "细分结构图",
    "powertrain_price": "动力×价格",
    "price_migration": "价格迁移",
    "seasonality": "季节热力图",
    "tco": "TCO分析",
    "price_per_meter": "元/米",
    "sales_vs_price": "销量×价格",
    "rv_finance": "残值金融",
    "powertrain_vs_price_cost": "动力成本×价格",
}


def _append_user_params(href: str, user_params: dict[str, Any] | None) -> str:
    """Append extracted user params (brand, powertrain, year) to a chart link URL."""
    if not user_params:
        return href
    extra: list[str] = []
    if user_params.get("brand"):
        extra.append(f"brand={user_params['brand']}")
    if user_params.get("powertrain"):
        extra.append(f"powertrain={user_params['powertrain']}")
    if user_params.get("year"):
        extra.append(f"year={user_params['year']}")
    if user_params.get("month"):
        extra.append(f"month={user_params['month']}")
    if not extra:
        return href
    sep = "&" if "?" in href else "?"
    return href + sep + "&".join(extra)


def _make_link(category: str, country: str) -> str:
    template = _CHART_LINK_TEMPLATES.get(category, "")
    return template.format(country=country) if template else ""


# -------- individual card factories --------


def _card_from_overview(
    country: str,
    period: str,
    metadata: dict,
    results: dict,
) -> InsightCard | None:
    overview = results.get("overview", {})
    summary = overview.get("summary", {})
    if not summary:
        return None

    total_vol = summary.get("totalVolume", summary.get("currentMonthVolume", 0))
    yoy = summary.get("yoy", {})
    yoy_display = yoy.get("display", "") if isinstance(yoy, dict) else ""
    yoy_tone = yoy.get("tone", "neutral") if isinstance(yoy, dict) else "neutral"

    conclusion = f"总销量 {total_vol:,} 台"
    if yoy_display:
        conclusion += f"，YoY {yoy_display}"

    return {
        "id": f"{country}-{period}-overview",
        "country": country,
        "period": period,
        "category": "overview",
        "title": "市场概况",
        "conclusion": conclusion,
        "supportingData": summary,
        "tone": yoy_tone,
        "relatedChartLink": _make_link("overview", country),
        "tags": ["销量", "YoY"],
    }


def _card_from_brand_ranking(
    country: str,
    period: str,
    metadata: dict,
    results: dict,
) -> InsightCard | None:
    overview = results.get("overview", {})
    ytd_ranking = overview.get("ytdBrandRanking", {})
    # ytdBrandRanking is {title, items: [...]}
    ytd_brands = (
        ytd_ranking.get("items", [])
        if isinstance(ytd_ranking, dict)
        else ytd_ranking if isinstance(ytd_ranking, list) else []
    )
    if not ytd_brands:
        return None

    top = ytd_brands[0]
    brand = top.get("brand", "?")
    share = top.get("share", 0)
    yoy = top.get("ytdYoy", None)

    conclusion = f"{brand} 领跑，份额 {share:.1f}%"
    tone = "neutral"
    if isinstance(yoy, (int, float)):
        direction = "+" if yoy > 0 else ""
        conclusion += f"，同比 {direction}{yoy:.1f}pp"
        tone = "positive" if yoy > 0 else "negative"

    return {
        "id": f"{country}-{period}-brand",
        "country": country,
        "period": period,
        "category": "brand",
        "title": "品牌格局",
        "conclusion": conclusion,
        "supportingData": {"top3": ytd_brands[:3]},
        "tone": tone,
        "relatedChartLink": _make_link("brand", country),
        "tags": ["品牌", "排名", "份额"],
    }


def _card_from_segment(
    country: str,
    period: str,
    metadata: dict,
    results: dict,
) -> InsightCard | None:
    segment = results.get("segment", {})
    summary_text = segment.get("summaryText", "")
    matrix = segment.get("matrix", {})
    rows = matrix.get("rows", []) if isinstance(matrix, dict) else []

    if summary_text:
        conclusion = summary_text
    elif rows:
        top_seg = rows[0]
        conclusion = (
            f"{top_seg.get('segment', '?')} 当月销量 "
            f"{top_seg.get('currentMonth', 0):,} 辆"
        )
    else:
        return None

    return {
        "id": f"{country}-{period}-segment",
        "country": country,
        "period": period,
        "category": "segment",
        "title": "细分市场",
        "conclusion": conclusion,
        "supportingData": {"rows": rows[:5]},
        "tone": "neutral",
        "relatedChartLink": _make_link("segment", country),
        "tags": ["segment", "SUV", "Sedan"],
    }


def _card_from_origin(
    country: str,
    period: str,
    metadata: dict,
    results: dict,
) -> InsightCard | None:
    origin = results.get("origin", {})
    summary_text = origin.get("summaryText", "")
    if not summary_text:
        return None

    # extract tone from the delta if possible
    matrix = origin.get("matrix", {})
    tone = "neutral"
    rows = matrix.get("rows", []) if isinstance(matrix, dict) else []
    if rows:
        top_row = rows[0]
        yoy = top_row.get("yoy", {})
        if isinstance(yoy, dict):
            tone = yoy.get("tone", "neutral")

    return {
        "id": f"{country}-{period}-origin",
        "country": country,
        "period": period,
        "category": "origin",
        "title": "车系阵营",
        "conclusion": summary_text,
        "supportingData": {},
        "tone": tone,
        "relatedChartLink": _make_link("origin", country),
        "tags": ["origin", "欧系", "日系", "中系"],
    }


def _card_from_nev(
    country: str,
    period: str,
    metadata: dict,
    results: dict,
) -> InsightCard | None:
    overview = results.get("overview", {})
    trend_data = overview.get("trend", {})
    # trend is {periods: [...], items: [...]} where each item has {period, fuels}
    trend = (
        trend_data.get("items", [])
        if isinstance(trend_data, dict)
        else trend_data if isinstance(trend_data, list) else []
    )
    if not trend:
        return None

    # The trend list contains monthly {period, fuels:{ICE:…, BEV:…, …}}
    # Sum BEV+PHEV+HEV for latest month
    latest = trend[-1] if trend else {}
    fuels = latest.get("fuelMix", latest.get("fuels", {}))
    bev = fuels.get("BEV", 0) or 0
    phev = fuels.get("PHEV", 0) or 0
    hev = fuels.get("HEV", 0) or 0
    nev_total = bev + phev + hev
    month_total = sum(v for v in fuels.values() if isinstance(v, (int, float)))
    if month_total <= 0:
        return None

    nev_share = nev_total / month_total * 100
    tone = "positive" if nev_share > 10 else "neutral"
    conclusion = (
        f"新能源（BEV+PHEV+HEV）当月占比 {nev_share:.1f}%，"
        f"合计 {nev_total:,} 辆"
    )

    return {
        "id": f"{country}-{period}-nev",
        "country": country,
        "period": period,
        "category": "nev",
        "title": "新能源渗透",
        "conclusion": conclusion,
        "supportingData": {"bev": bev, "phev": phev, "hev": hev, "share": round(nev_share, 1)},
        "tone": tone,
        "relatedChartLink": _make_link("nev", country),
        "tags": ["BEV", "PHEV", "新能源", "渗透率"],
    }


def _card_from_trend(
    country: str,
    period: str,
    metadata: dict,
    results: dict,
) -> InsightCard | None:
    overview = results.get("overview", {})
    summary = overview.get("summary", {})
    mom = summary.get("mom", {})
    yoy = summary.get("yoy", {})
    if not isinstance(mom, dict) and not isinstance(yoy, dict):
        return None

    mom_display = mom.get("display", "") if isinstance(mom, dict) else ""
    yoy_display = yoy.get("display", "") if isinstance(yoy, dict) else ""
    yoy_tone = yoy.get("tone", "neutral") if isinstance(yoy, dict) else "neutral"

    parts: list[str] = []
    if mom_display:
        parts.append(f"环比 {mom_display}")
    if yoy_display:
        parts.append(f"同比 {yoy_display}")
    if not parts:
        return None

    conclusion = "当月" + "，".join(parts)

    return {
        "id": f"{country}-{period}-trend",
        "country": country,
        "period": period,
        "category": "trend",
        "title": "增长趋势",
        "conclusion": conclusion,
        "supportingData": {"mom": mom, "yoy": yoy},
        "tone": yoy_tone,
        "relatedChartLink": _make_link("trend", country),
        "tags": ["趋势", "MoM", "YoY"],
    }
