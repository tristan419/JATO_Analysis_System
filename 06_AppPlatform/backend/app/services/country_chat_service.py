from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

import pandas as pd

from app.infra import parquet_repository as repo
from app.scraper import enable_external_scraper_package
from app.services import query_service
from app.services import market_scan_service
from app.services import insight_card_service
from app.services import country_profiles
from app.services import local_wiki_service
from app.services import news_digest_service
from app.services import news_wiki_service


enable_external_scraper_package()

from jato_scraper.llm.client import ChatMessage  # noqa: E402
from jato_scraper.llm.providers import NvidiaChatClient  # noqa: E402

log = logging.getLogger(__name__)

DEFAULT_NVIDIA_CHAT_MODEL = os.getenv(
    "APP_NVIDIA_CHAT_MODEL",
    "meta/llama-3.3-70b-instruct",
).strip()
MAX_HISTORY_TURNS = 6
TOP_BRAND_LIMIT = 15
TOP_MODEL_LIMIT = 10
TOP_POWERTRAIN_LIMIT = 6
CONTEXT_CHAR_BUDGET = 24_000
MAX_DECK_BASE_INTENTS = 3
MAX_DECK_INTENTS = 5

INTENT_PRIORITY = [
    "positioning-analysis",
    "competitive",
    "segment-analysis",
    "origin-analysis",
    "market-context",
    "nev-analysis",
    "pricing-summary",
    "brand-ranking",
    "powertrain-mix",
    "trend-summary",
    "general-summary",
]

COUNTRY_PROMPT_SUGGESTIONS = []

INTENT_SUGGESTIONS: dict[str, list[str]] = {
    "brand-ranking": [
        "排名第一的品牌在哪个 segment 最强？",
        "各品牌的动力类型分布有何差异？",
        "对比一下 YTD 和去年同期的品牌排名变化",
    ],
    "segment-analysis": [
        "SUV vs Sedan 的趋势如何变化？",
        "BEV 在各个 segment 的渗透率如何？",
        "哪个 segment 增长最快？",
    ],
    "origin-analysis": [
        "中系品牌在哪些 segment 增长最快？",
        "欧系品牌的份额同比变化如何？",
        "各车系阵营的动力结构有何差异？",
    ],
    "powertrain-mix": [
        "BEV 的市场份额同比变化多大？",
        "PHEV 在哪些 segment 占比最高？",
        "各品牌的电动化率如何排名？",
    ],
    "trend-summary": [
        "哪些品牌增长最快？",
        "SUV 占比在最近几年的变化趋势？",
        "新能源渗透率的月度变化？",
    ],
    "nev-analysis": [
        "BEV 和 PHEV 的竞争格局如何？",
        "哪些品牌的 BEV 车型最多？",
        "新能源车的平均价格与燃油车差多少？",
    ],
    "positioning-analysis": [
        "同价位同尺寸的竞品有哪些？",
        "这个定价在当地市场处于什么水平？",
        "BEV 续航分布是怎样的？",
    ],
    "market-context": [
        "当地有什么新能源补贴政策？",
        "关税政策对中国品牌有什么影响？",
        "这个市场最近有什么热点事件？",
    ],
}

_SYSTEM_PROMPT = (
    "你是核心汽车公司的资深产品经理，主导欧洲市场的竞品分析。脾气干练、逻辑严密、以目标为导向。\n\n"
    "分析原则：\n"
    "1. 结论先行：不废话，一句话抛出核心结论（如'该细分市场已被蚕食，不建议进入'或'当前是抄底好时机'）。\n"
    "2. 数据为刃：用明确的同比(YoY)/环比(MoM)百分比或销量绝对值支撑你的论点，用词要自信且果断。\n"
    "3. 品牌排位剖析：不仅报喜报忧，更要指出份额变化的内因和行业洗牌的趋势。\n"
    "4. 细分战场诊断：深挖 SUV vs Sedan 等结构变化，告诉老板这是红海还是蓝海。\n"
    "5. 阵营威胁论：分析各车系(欧/日/韩/美/中)的竞争格局，指出未来的潜在威胁者。\n"
    "6. 强烈的拟人感：用高级 PM 汇报的口吻，如'从数据上看...我们必须注意...'或者'这里的机会显而易见...'。\n"
    "7. 空白预警：如果数据缺失，立刻说'目前缺乏该维度的数据支撑，我建议我们转向分析...'，绝不含糊其辞。\n"
    "8. 竞品狙击：提到具体车型/品牌/尺寸时，利用定位地图(positioningMap)进行降维打击式分析。\n"
    "9. BEV 战局：剖析 bevShareBySegment，指出新能源突破口。\n"
    "10. 价格带切割：结合 priceDistribution 定位溢价/折价空间，给出定价策略建议。\n"
    "11. 结构化排版（极其重要）：任何时候提及【竞品对比】、【具体版型差异】、【尺寸价格对比】时，**必须且只能以 Markdown 表格** 的形式进行严谨排版。不要使用冗长的自然段落罗列。\n"
    "12. 国家热点嗅觉：当 countryProfile 可用时，结合当地政策/补贴/关税热点来解读数据变化。"
    "例如'BEV份额大幅下降' → 关联'补贴终止'。当 newsDigest 或 marketEvents 可用时，"
    "优先把最新新闻与数据变化串起来解释。融入分析，不要机械列举。\n\n"
    "数据字段说明：\n"
    "- ytdBrandRanking: YTD品牌排名，volume=销量(辆)，share=份额(%)，ytdYoy=同比增幅(%)\n"
    "- segmentMatrix: 车型级别矩阵(SUV-A00~SD-C)，含当月/MoM/YoY/YTD指标\n"
    "- originAnalysis: 按车系阵营(欧系/日系/韩系/美系/中系)的份额分析\n"
    "- suvSedanTrend: SUV vs Sedan 占比月度趋势\n"
    "- drilldown/suvA: 特定 segment 的车型排名和燃料面板\n"
    "- powertrainMix: 动力类型(BEV/PHEV/HEV/MHEV/ICE)累计销量\n"
    "- overviewSummary: 市场总量、当月MoM/YoY变化\n"
    "- positioningMap: 竞品定位(Length×MSRP散点+KMeans聚类), 含target目标位置\n"
    "- bevShareBySegment: 各segment的BEV占比排名\n"
    "- priceDistribution: 动力类型×价格带销量分布\n"
    "- pricePerMeter / salesVsPrice / nevCapacityVsMsrp: Dashboard 高级图补充视角\n"
    "- newsDigest: 最新新闻的结构化摘要；marketEvents: 最新市场事件列表\n"
    "- MSRP 单位为各国本地货币\n\n"
    "你现在有预分析的洞察卡片(insightCards)，每张包含一句结论和支撑数据。\n"
    "基于这些数据，用产品经理的自信语调强势输出你的专业判断。\n\n"
    "【严禁事项】绝对不要在回答中插入任何链接、URL、markdown图片语法(![]())、"
    "图表跳转地址或文件路径。系统会在你回答之后自动追加导航按钮，你只需要输出纯文字分析。\n"
)


# --------------- user parameter extraction ---------------

def extract_user_params(question: str) -> dict[str, Any]:
    """Extract structured parameters from user's natural language question.

    Detects: brand, model, powertrain, length(mm), msrp/price, volume/sales,
    year = specific or relative (今年/去年), month = specific or relative (上个月).
    """
    import datetime as _dt

    params: dict[str, Any] = {}
    q = question.strip()

    # ---- Year / month extraction ----
    now = _dt.date.today()

    # Relative year: 今年, 去年, 前年
    if re.search(r"今年|本年|this\s*year", q, re.IGNORECASE):
        params["year"] = now.year
    elif re.search(r"去年|上一?年|last\s*year", q, re.IGNORECASE):
        params["year"] = now.year - 1
    elif re.search(r"前年", q):
        params["year"] = now.year - 2
    else:
        # Explicit year: "2024年", "2025", "FY2024"
        year_match = re.search(r"(?:FY)?(\d{4})\s*年?", q)
        if year_match:
            y = int(year_match.group(1))
            if 2015 <= y <= now.year + 1:
                params["year"] = y

    # Relative month: 上个月, 这个月, 本月
    if re.search(r"上个?月|last\s*month", q, re.IGNORECASE):
        prev = now.replace(day=1) - _dt.timedelta(days=1)
        params["month"] = prev.month
        if "year" not in params:
            params["year"] = prev.year
    elif re.search(r"这个月|本月|this\s*month", q, re.IGNORECASE):
        params["month"] = now.month
        if "year" not in params:
            params["year"] = now.year
    else:
        # Explicit month: "3月", "12月份"
        month_match = re.search(r"(\d{1,2})\s*月(?:份)?", q)
        if month_match:
            m = int(month_match.group(1))
            if 1 <= m <= 12:
                params["month"] = m

    # Powertrain  (\b doesn't work around CJK characters)
    pt_match = re.search(
        r"(?:\b(BEV|PHEV|HEV|MHEV|ICE|REEV)\b|(纯电|插混|混动|增程))",
        q,
        re.IGNORECASE,
    )
    if pt_match:
        pt_raw = (pt_match.group(1) or pt_match.group(2)).upper()
        pt_map = {"纯电": "BEV", "插混": "PHEV", "混动": "HEV", "增程": "REEV"}
        params["powertrain"] = pt_map.get(pt_raw, pt_raw)

    # Length in mm  (e.g. "4500mm", "4500的车", "车长4500")
    len_match = re.search(
        r"(?:车长|长度|length)?\s*(\d{3,5})\s*(?:mm|毫米|的车)",
        q,
        re.IGNORECASE,
    )
    if len_match:
        val = int(len_match.group(1))
        if 2000 <= val <= 6000:
            params["length"] = val

    # MSRP / price  (e.g. "定价35000", "售价45000", "卖35000", "msrp 35000")
    price_match = re.search(
        r"(?:定价|售价|卖|价格|msrp|price)\s*(\d{3,8})",
        q,
        re.IGNORECASE,
    )
    if price_match:
        params["msrp"] = int(price_match.group(1))

    # Volume / sales  (e.g. "4500辆", "销量4500", "月销4500")
    vol_match = re.search(
        r"(?:销量|月销|年销|卖了)?\s*(\d{2,7})\s*(?:辆|台|units)",
        q,
        re.IGNORECASE,
    )
    if vol_match:
        params["volume"] = int(vol_match.group(1))

    # Brand + Model  (e.g. "JAECOO J7", "领克09", "volvo xc60")
    # Match uppercase/mixed-case brand tokens + optional alphanumeric model
    brand_model_match = re.search(
        r"\b([A-Z][A-Za-z\u4e00-\u9fff]{1,15})\s+([A-Za-z0-9][\w\-]{0,10})\b",
        q,
    )
    if brand_model_match:
        params["brand"] = brand_model_match.group(1).upper()
        params["model"] = brand_model_match.group(2).upper()
    else:
        # Try standalone brand (all-caps 2+ chars)
        brand_only = re.search(r"\b([A-Z]{2,15})\b", q)
        if brand_only and brand_only.group(1) not in {
            "BEV", "PHEV", "HEV", "MHEV", "ICE", "REEV",
            "SUV", "YTD", "MOM", "YOY", "TCO", "MSRP",
        }:
            params["brand"] = brand_only.group(1)

    return params


def get_country_chat_metadata() -> dict[str, Any]:
    country_col = _resolve_country_column()
    countries = []
    if country_col:
        countries = repo.load_distinct_options(country_col, {})

    provider_available = _nvidia_provider_available()
    provider_reason = None
    if not provider_available:
        provider_reason = (
            "当前环境未配置 NVIDIA_API_KEY / NVAPI_KEY，"
            "页面会退回本地摘要回答。"
        )

    return {
        "availableCountries": [
            {"value": country, "label": country}
            for country in countries
        ],
        "provider": "nvidia" if provider_available else "fallback",
        "providerAvailable": provider_available,
        "providerReason": provider_reason,
        "defaultModel": (
            DEFAULT_NVIDIA_CHAT_MODEL if provider_available else None
        ),
        "suggestedPrompts": COUNTRY_PROMPT_SUGGESTIONS,
    }


def answer_country_question(
    country: str,
    question: str,
    history: list[dict[str, str]] | None = None,
    news_payload_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_country = str(country).strip()
    normalized_question = str(question).strip()
    if not normalized_country:
        raise ValueError("country 不能为空")
    if not normalized_question:
        raise ValueError("question 不能为空")

    user_params = extract_user_params(normalized_question)
    snapshot = build_country_snapshot(
        normalized_country,
        user_params=user_params,
        news_payload_override=news_payload_override,
    )
    intents = infer_country_chat_intents(normalized_question)
    intent = intents[0]

    # Lazy-load Dashboard analysis data based on intent + extracted params
    _enrich_snapshot_for_intents(snapshot, intents, user_params)

    provider = "fallback"
    provider_available = _nvidia_provider_available()
    provider_reason = None
    answer = _build_fallback_answer_for_intents(
        country=normalized_country,
        question=normalized_question,
        intents=intents,
        snapshot=snapshot,
        provider_error=None,
    )

    if provider_available:
        try:
            answer = _answer_with_nvidia(
                country=normalized_country,
                question=normalized_question,
                intents=intents,
                user_params=user_params,
                snapshot=snapshot,
                history=history or [],
            )
            provider = "nvidia"
        except Exception as exc:  # noqa: BLE001
            provider_reason = str(exc)
            answer = _build_fallback_answer_for_intents(
                country=normalized_country,
                question=normalized_question,
                intents=intents,
                snapshot=snapshot,
                provider_error=provider_reason,
            )
    else:
        provider_reason = (
            "当前环境没有 NVIDIA_API_KEY / NVAPI_KEY，已使用本地摘要降级回答。"
        )

    suggestions = _suggestions_for_intents(intents, snapshot)

    all_cards = snapshot.pop("_allInsightCards", [])
    chart_links = insight_card_service.chart_links_for_intents(
        all_cards,
        intents,
        user_params,
    )

    return {
        "country": normalized_country,
        "question": normalized_question,
        "answer": answer,
        "intent": intent,
        "primaryIntent": intent,
        "intents": intents,
        "provider": provider,
        "providerAvailable": provider_available,
        "providerReason": provider_reason,
        "contextSnapshot": snapshot,
        "suggestedPrompts": suggestions,
        "chartLinks": chart_links,
        "extractedParams": user_params if user_params else None,
    }


def build_country_chart_deck(
    country: str,
    question: str = "",
    intents: list[str] | None = None,
    extracted_params: dict[str, Any] | None = None,
    selected_year: int | None = None,
    selected_model: str | None = None,
    model_top_n: int | None = None,
) -> dict[str, Any]:
    normalized_country = str(country).strip()
    normalized_question = str(question).strip()
    if not normalized_country:
        raise ValueError("country 不能为空")

    inferred_params = (
        extract_user_params(normalized_question) if normalized_question else {}
    )
    merged_params = {
        **inferred_params,
        **(extracted_params or {}),
    }
    if selected_year is not None:
        merged_params["year"] = int(selected_year)
    if selected_model is not None and str(selected_model).strip():
        merged_params["model"] = str(selected_model).strip()
    if model_top_n is not None:
        merged_params["model_top_n"] = int(model_top_n)
    inferred_intents = _normalize_intents(
        intents
        or (
            infer_country_chat_intents(normalized_question)
            if normalized_question else ["general-summary"]
        ),
    )
    base_intents = _limit_intents_for_deck(inferred_intents)
    deck_intents = _chart_deck_intents(base_intents)

    snapshot = build_country_snapshot(
        normalized_country,
        user_params=merged_params,
    )
    _enrich_snapshot_for_intents(snapshot, deck_intents, merged_params)
    controls = _inject_chart_deck_controls(
        snapshot=snapshot,
        country=normalized_country,
        merged_params=merged_params,
    )
    snapshot.pop("_allInsightCards", None)

    return {
        "country": normalized_country,
        "question": normalized_question,
        "primaryIntent": base_intents[0],
        "intents": base_intents,
        "deckIntents": deck_intents,
        "contextSnapshot": snapshot,
        "controls": controls,
        "extractedParams": merged_params if merged_params else None,
    }


def build_country_snapshot(
    country: str,
    user_params: dict[str, Any] | None = None,
    news_payload_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    country_col = _resolve_country_column()
    if not country_col:
        raise ValueError("数据集中未找到国家字段")

    filters = {country_col: [country]}
    sales_scope = _resolve_sales_scope(user_params or {})
    overview = query_service.query_overview(
        filters=filters,
        prefer_precomputed=True,
        top_n=12,
    )
    vehicle_frame = query_service._build_vehicle_frame(  # noqa: SLF001
        filters,
        sales_columns=sales_scope["salesColumns"] or None,
    )

    snapshot: dict[str, Any] = {
        "country": country,
        "route": overview.get("route", "unknown"),
        "kpis": overview.get("kpis", {}),
        "yearSeries": overview.get("yearSeries", []),
        "monthSeries": overview.get("monthSeries", []),
        "topBrands": _build_sales_rankings(
            frame=vehicle_frame,
            dimension="Brand",
            limit=TOP_BRAND_LIMIT,
        ),
        "topModels": _build_sales_rankings(
            frame=vehicle_frame,
            dimension="Model",
            limit=TOP_MODEL_LIMIT,
        ),
        "powertrainMix": _build_sales_rankings(
            frame=vehicle_frame,
            dimension="Powertrain",
            limit=TOP_POWERTRAIN_LIMIT,
        ),
        "analysisMeta": {
            "availableYears": sales_scope["availableYears"],
            "selectedYear": sales_scope["resolvedYear"],
            "selectedMonth": sales_scope["requestedMonth"],
            "yearLockedByQuestion": sales_scope["yearLockedByQuestion"],
            "defaultLatestYearApplied": sales_scope[
                "defaultLatestYearApplied"
            ],
        },
        "marketEvents": [],
        "newsDigest": None,
    }

    # ---------- Enrich with insight cards ----------
    insight_cards = insight_card_service.get_insight_cards(country)
    snapshot["insightCards"] = [
        {
            "title": c["title"],
            "conclusion": c["conclusion"],
            "tone": c["tone"],
            "relatedChartLink": c.get("relatedChartLink", ""),
        }
        for c in insight_cards[:4]
    ]
    snapshot["_allInsightCards"] = insight_cards

    _inject_news_payload(
        snapshot,
        country,
        news_payload_override=news_payload_override,
    )

    # ---------- Enrich with Market Scan deck ----------
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
        _inject_deck_panels(snapshot, deck)
    except Exception:  # noqa: BLE001
        log.warning("Market Scan deck unavailable for %s, skipping", country)

    return snapshot


def _inject_news_payload(
    snapshot: dict[str, Any],
    country: str,
    *,
    news_payload_override: dict[str, Any] | None = None,
) -> None:
    try:
        news_payload = (
            news_payload_override
            if news_payload_override is not None
            else news_digest_service.get_country_news_payload(country)
        )
        snapshot["marketEvents"] = news_payload.get("marketEvents", [])
        snapshot["newsDigest"] = news_payload.get("newsDigest")
    except Exception:  # noqa: BLE001
        log.warning("Country news unavailable for %s, skipping", country)


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _resolve_sales_scope(
    user_params: dict[str, Any],
    *,
    default_latest_year: bool = True,
) -> dict[str, Any]:
    requested_year = _coerce_optional_int(user_params.get("year"))
    requested_month = _coerce_optional_int(user_params.get("month"))
    sales_columns, available_years, resolved_year = (
        query_service._sales_columns_for_scope(  # noqa: SLF001
            repo.list_columns(),
            year=requested_year,
            month=requested_month,
            default_latest_year=(
                default_latest_year and requested_year is None
            ),
        )
    )
    return {
        "salesColumns": sales_columns,
        "availableYears": available_years,
        "resolvedYear": int(resolved_year) if resolved_year else None,
        "requestedYear": requested_year,
        "requestedMonth": requested_month,
        "yearLockedByQuestion": bool(
            requested_year is not None or requested_month is not None
        ),
        "defaultLatestYearApplied": bool(
            default_latest_year
            and requested_year is None
            and resolved_year is not None
        ),
    }


def _build_chart_scope_options(
    user_params: dict[str, Any],
    *,
    default_latest_year: bool,
) -> dict[str, Any]:
    options: dict[str, Any] = {}
    if _coerce_optional_int(user_params.get("year")) is not None:
        options["sales_year"] = int(user_params["year"])
    elif default_latest_year:
        options["default_latest_year"] = True

    month = _coerce_optional_int(user_params.get("month"))
    if month is not None:
        options["sales_month"] = month
    return options


def _filter_heatmap_items_for_year(
    items: list[dict[str, Any]],
    selected_year: int | None,
) -> list[dict[str, Any]]:
    if selected_year is None:
        return items
    year_label = str(selected_year)
    return [
        item for item in items
        if str(item.get("year", "")).strip() == year_label
    ]


def _compact_market_events_for_context(
    market_events: list[dict[str, Any]],
    limit: int = 3,
) -> list[dict[str, Any]]:
    compact_events: list[dict[str, Any]] = []
    for event in market_events[:limit]:
        compact_events.append(
            {
                "publisher": event.get("publisher"),
                "title": event.get("title"),
                "summary": event.get("summary"),
                "publishedAt": event.get("publishedAt"),
                "tags": event.get("tags", []),
            }
        )
    return compact_events


def _normalize_model_selection(
    requested_model: str | None,
    available_models: list[str],
) -> str:
    normalized_available = [
        str(model).strip() for model in available_models if str(model).strip()
    ]
    requested = str(requested_model or "").strip()
    if requested:
        for candidate in normalized_available:
            if candidate.lower() == requested.lower():
                return candidate
        return requested
    return normalized_available[0] if normalized_available else ""


def _inject_chart_deck_controls(
    *,
    snapshot: dict[str, Any],
    country: str,
    merged_params: dict[str, Any],
) -> dict[str, Any]:
    sales_scope = _resolve_sales_scope(merged_params)
    filters = _build_country_query_filters(country, merged_params)
    available_models = [
        str(item.get("label", "")).strip()
        for item in snapshot.get("topModels", [])
        if str(item.get("label", "")).strip()
    ]
    selected_model = _normalize_model_selection(
        merged_params.get("model"),
        available_models,
    )
    model_top_n = max(
        8,
        min(60, int(_coerce_optional_int(merged_params.get("model_top_n")) or 24)),
    )

    model_version_bubble: list[dict[str, Any]] = []
    if selected_model and filters:
        version_result = query_service.query_model_versions(
            filters=filters,
            model_name=selected_model,
            top_n=model_top_n,
            sales_columns=sales_scope["salesColumns"] or None,
        )
        model_version_bubble = version_result.get("items", [])
        if not model_version_bubble and available_models:
            fallback_model = available_models[0]
            if fallback_model.lower() != selected_model.lower():
                selected_model = fallback_model
                version_result = query_service.query_model_versions(
                    filters=filters,
                    model_name=selected_model,
                    top_n=model_top_n,
                    sales_columns=sales_scope["salesColumns"] or None,
                )
                model_version_bubble = version_result.get("items", [])

    if selected_model and selected_model not in available_models:
        available_models = [selected_model, *available_models]

    snapshot["modelVersionBubble"] = model_version_bubble
    snapshot["analysisMeta"] = {
        **snapshot.get("analysisMeta", {}),
        "availableYears": sales_scope["availableYears"],
        "selectedYear": sales_scope["resolvedYear"],
        "selectedMonth": sales_scope["requestedMonth"],
        "yearLockedByQuestion": sales_scope["yearLockedByQuestion"],
        "defaultLatestYearApplied": sales_scope["defaultLatestYearApplied"],
        "availableModels": available_models[:20],
        "selectedModel": selected_model or None,
        "modelTopN": model_top_n,
    }

    return dict(snapshot["analysisMeta"])


def _inject_deck_panels(
    snapshot: dict[str, Any],
    deck: dict[str, Any],
) -> None:
    """Extract key panels from the Market Scan deck into the snapshot."""
    metadata = deck.get("metadata", {})
    results = deck.get("results", {})

    snapshot["periodLabel"] = metadata.get("labels", {}).get("pageTitle", "")
    snapshot["resolvedPeriod"] = metadata.get("resolvedPeriod", "")

    overview = results.get("overview", {})
    snapshot["overviewSummary"] = overview.get("summary", {})
    snapshot["ytdBrandRanking"] = overview.get("ytdBrandRanking", [])
    snapshot["monthlyBrandRanking"] = overview.get(
        "monthlyBrandRanking", [],
    )

    origin = results.get("origin", {})
    snapshot["originAnalysis"] = {
        "summaryText": origin.get("summaryText", ""),
        "matrix": origin.get("matrix", {}),
    }

    segment = results.get("segment", {})
    snapshot["segmentMatrix"] = segment.get("matrix", {})
    snapshot["suvSedanTrend"] = segment.get("bodyShareTrend", [])

    drilldown = results.get("drilldown", {})
    snapshot["drilldown"] = {
        "segment": drilldown.get("segment", ""),
        "totalRanking": drilldown.get("totalRanking", []),
        "ytdFuelTrend": drilldown.get("ytdFuelTrend", []),
    }

    suv_a = results.get("suvA", {})
    snapshot["suvA"] = {
        "segment": suv_a.get("segment", ""),
        "totalRanking": suv_a.get("totalRanking", []),
        "ytdFuelTrend": suv_a.get("ytdFuelTrend", []),
    }


def _enrich_snapshot_for_intent(
    snapshot: dict[str, Any],
    intent: str,
    user_params: dict[str, Any],
    *,
    _filters: dict[str, list[str]] | None = None,
    _cross_section_options: dict[str, Any] | None = None,
    _trend_options: dict[str, Any] | None = None,
    _sales_scope: dict[str, Any] | None = None,
) -> None:
    """Lazy-load Dashboard analysis data based on intent + extracted params."""
    if _filters is None:
        country = snapshot.get("country", "")
        _filters = _build_country_query_filters(country, user_params)
    filters = _filters
    if not filters:
        return

    cross_section_options = _cross_section_options or _build_chart_scope_options(
        user_params,
        default_latest_year=True,
    )
    trend_options = _trend_options or _build_chart_scope_options(
        user_params,
        default_latest_year=False,
    )
    sales_scope = _sales_scope or _resolve_sales_scope(user_params)

    try:
        if intent == "positioning-analysis":
            target_length = user_params.get("length")
            target_msrp = user_params.get("msrp")
            positioning = query_service.query_positioning_map(
                filters=filters,
                target_length=float(target_length) if target_length else None,
                target_msrp=float(target_msrp) if target_msrp else None,
                length_range=800,
                manual_competitors=(
                    [user_params["brand"]]
                    if user_params.get("brand") else None
                ),
                top_n=20,
                n_clusters=4,
                sales_columns=sales_scope["salesColumns"] or None,
            )
            snapshot["positioningMap"] = positioning
            # Also fetch length_vs_price for richer context
            lp = query_service.query_advanced_chart(
                group="price_value",
                chart="length_vs_price",
                filters=filters,
                top_n=20,
                options=cross_section_options,
            )
            snapshot["priceDistribution"] = lp.get("items", [])

        elif intent == "nev-analysis":
            nev_range = query_service.query_advanced_chart(
                group="nev_analysis",
                chart="nev_range_distribution",
                filters=filters,
                top_n=15,
                options=cross_section_options,
            )
            snapshot["nevRangeDistribution"] = nev_range.get("items", [])
            # BEV share by segment
            bev_filters = {**filters, "Powertrain": ["BEV"]}
            bev_seg = query_service.query_advanced_chart(
                group="market_structure",
                chart="segment_share",
                filters=bev_filters,
                top_n=10,
                options=cross_section_options,
            )
            snapshot["bevShareBySegment"] = bev_seg.get("items", [])
            nev_capacity = query_service.query_advanced_chart(
                group="nev_analysis",
                chart="nev_capacity_vs_msrp",
                filters=filters,
                top_n=15,
                options=cross_section_options,
            )
            snapshot["nevCapacityVsMsrp"] = nev_capacity.get("items", [])

        elif intent == "pricing-summary":
            lp = query_service.query_advanced_chart(
                group="price_value",
                chart="length_vs_price",
                filters=filters,
                top_n=20,
                options=cross_section_options,
            )
            snapshot["priceDistribution"] = lp.get("items", [])
            pt_price = query_service.query_advanced_chart(
                group="price_value",
                chart="powertrain_vs_price",
                filters=filters,
                top_n=10,
                options=cross_section_options,
            )
            snapshot["powertrainVsPrice"] = pt_price.get("items", [])
            ppm = query_service.query_advanced_chart(
                group="price_value",
                chart="price_per_meter",
                filters=filters,
                top_n=15,
                options=cross_section_options,
            )
            snapshot["pricePerMeter"] = ppm.get("items", [])
            sales_vs_price = query_service.query_advanced_chart(
                group="price_value",
                chart="sales_vs_price",
                filters=filters,
                top_n=15,
                options=cross_section_options,
            )
            snapshot["salesVsPrice"] = sales_vs_price.get("items", [])

        elif intent == "competitive":
            # For competitive, also pull positioning map
            # with target if available.
            target_length = user_params.get("length")
            target_msrp = user_params.get("msrp")
            if target_length or target_msrp or user_params.get("brand"):
                positioning = query_service.query_positioning_map(
                    filters=filters,
                    target_length=(
                        float(target_length) if target_length else None
                    ),
                    target_msrp=(
                        float(target_msrp) if target_msrp else None
                    ),
                    length_range=800,
                    manual_competitors=(
                        [user_params["brand"]]
                        if user_params.get("brand") else None
                    ),
                    top_n=20,
                    n_clusters=4,
                    sales_columns=sales_scope["salesColumns"] or None,
                )
                snapshot["positioningMap"] = positioning

        elif intent == "segment-analysis":
            seg = query_service.query_advanced_chart(
                group="market_structure",
                chart="segment_share_by_length",
                filters=filters,
                top_n=15,
                options=cross_section_options,
            )
            snapshot["segmentShareByLength"] = seg.get("items", [])

        elif intent == "brand-ranking":
            bubble = query_service.query_advanced_chart(
                group="market_structure",
                chart="powertrain_bubble",
                filters=filters,
                top_n=20,
                options=cross_section_options,
            )
            snapshot["powertrainBubble"] = bubble.get("items", [])
            sales_vs_price = query_service.query_advanced_chart(
                group="price_value",
                chart="sales_vs_price",
                filters=filters,
                top_n=15,
                options=cross_section_options,
            )
            snapshot["salesVsPrice"] = sales_vs_price.get("items", [])

        elif intent == "powertrain-mix":
            pt_price = query_service.query_advanced_chart(
                group="price_value",
                chart="powertrain_vs_price",
                filters=filters,
                top_n=15,
                options=cross_section_options,
            )
            snapshot["powertrainVsPrice"] = pt_price.get("items", [])
            bubble = query_service.query_advanced_chart(
                group="market_structure",
                chart="powertrain_bubble",
                filters=filters,
                top_n=20,
                options=cross_section_options,
            )
            snapshot["powertrainBubble"] = bubble.get("items", [])

        elif intent == "trend-summary":
            migration = query_service.query_advanced_chart(
                group="price_value",
                chart="price_migration",
                filters=filters,
                top_n=15,
                options=trend_options,
            )
            snapshot["priceMigration"] = migration.get("items", [])
            # Seasonality heatmap for trend context
            try:
                hm = query_service.query_advanced_chart(
                    group="time_insight",
                    chart="seasonality_heatmap",
                    filters=filters,
                    top_n=20,
                    options=trend_options,
                )
                snapshot["seasonalityHeatmap"] = hm.get("items", [])
            except Exception:
                pass

        elif intent == "general-summary":
            seg = query_service.query_advanced_chart(
                group="market_structure",
                chart="segment_share_by_length",
                filters=filters,
                top_n=15,
                options=cross_section_options,
            )
            snapshot["segmentShareByLength"] = seg.get("items", [])
            # RV / TCO for general context
            try:
                tco = query_service.query_advanced_chart(
                    group="cost_analysis",
                    chart="estimated_tco",
                    filters=filters,
                    top_n=10,
                    options=cross_section_options,
                )
                snapshot["estimatedTco"] = tco.get("items", [])
            except Exception:
                pass
            try:
                bubble = query_service.query_advanced_chart(
                    group="market_structure",
                    chart="powertrain_bubble",
                    filters=filters,
                    top_n=20,
                    options=cross_section_options,
                )
                snapshot["powertrainBubble"] = bubble.get("items", [])
            except Exception:
                pass
            try:
                sales_vs_price = query_service.query_advanced_chart(
                    group="price_value",
                    chart="sales_vs_price",
                    filters=filters,
                    top_n=15,
                    options=cross_section_options,
                )
                snapshot["salesVsPrice"] = sales_vs_price.get("items", [])
            except Exception:
                pass
            try:
                ppm = query_service.query_advanced_chart(
                    group="price_value",
                    chart="price_per_meter",
                    filters=filters,
                    top_n=15,
                    options=cross_section_options,
                )
                snapshot["pricePerMeter"] = ppm.get("items", [])
            except Exception:
                pass
            try:
                nev_capacity = query_service.query_advanced_chart(
                    group="nev_analysis",
                    chart="nev_capacity_vs_msrp",
                    filters=filters,
                    top_n=15,
                    options=cross_section_options,
                )
                snapshot["nevCapacityVsMsrp"] = nev_capacity.get("items", [])
            except Exception:
                pass

        elif intent == "origin-analysis":
            # Seasonality heatmap adds temporal context
            try:
                hm = query_service.query_advanced_chart(
                    group="time_insight",
                    chart="seasonality_heatmap",
                    filters=filters,
                    top_n=20,
                    options=trend_options,
                )
                snapshot["seasonalityHeatmap"] = hm.get("items", [])
            except Exception:
                pass

        elif intent == "market-context":
            # Inject static country profile into snapshot
            country = snapshot.get("country", "")
            profile = country_profiles.get_country_profile(country)
            if profile:
                snapshot["countryProfile"] = profile
    except Exception:  # noqa: BLE001
        log.warning(
            "Dashboard enrichment failed for intent=%s, continuing",
            intent,
        )


def _enrich_snapshot_for_intents(
    snapshot: dict[str, Any],
    intents: list[str],
    user_params: dict[str, Any],
) -> None:
    country = snapshot.get("country", "")
    filters = _build_country_query_filters(country, user_params)
    if not filters:
        return
    cross_section_options = _build_chart_scope_options(
        user_params,
        default_latest_year=True,
    )
    trend_options = _build_chart_scope_options(
        user_params,
        default_latest_year=False,
    )
    sales_scope = _resolve_sales_scope(user_params)

    for intent in _normalize_intents(intents):
        _enrich_snapshot_for_intent(
            snapshot,
            intent,
            user_params,
            _filters=filters,
            _cross_section_options=cross_section_options,
            _trend_options=trend_options,
            _sales_scope=sales_scope,
        )


# --------------- clause splitting & weighted intent scoring ---------------

_CLAUSE_SEP = re.compile(r"[，,；;？?。.！!]+")

_INTENT_KEYWORDS: dict[str, dict[int, list[str]]] = {
    "segment-analysis": {
        3: ["细分", "车型级别", "sd-", "suv-"],
        2: ["segment", "suv", "sedan", "轿车", "越野", "车身"],
        1: ["a0", "a00", "b segment"],
    },
    "origin-analysis": {
        3: ["车系", "阵营", "国别"],
        2: ["origin", "欧系", "日系", "韩系", "美系", "中系"],
        1: ["进口", "合资", "自主"],
    },
    "nev-analysis": {
        3: ["续航", "渗透率", "电池", "充电桩"],
        2: ["新能源", "nev", "电动", "纯电", "插混", "增程"],
        1: ["充电", "range"],
    },
    "positioning-analysis": {
        3: ["定位", "定价", "positioning", "机会点", "切入点"],
        2: ["竞争力", "价格带", "有机会", "打算卖", "能卖"],
        1: ["空间在哪", "能不能进"],
    },
    "competitive": {
        3: ["竞品", "vs", "对比"],
        2: ["竞争", "比较", "对手", "差异"],
    },
    "brand-ranking": {
        3: ["品牌排名", "brand ranking", "厂家排名"],
        2: ["品牌", "brand", "车企", "主机厂", "厂家"],
    },
    "powertrain-mix": {
        3: ["动力结构", "powertrain mix"],
        2: ["动力", "powertrain", "bev", "phev", "hev", "ice", "mhev"],
    },
    "trend-summary": {
        3: ["同比", "环比", "yoy", "mom"],
        2: ["趋势", "trend", "增长", "下滑", "走势", "变化"],
        1: ["销量", "year", "month"],
    },
    "pricing-summary": {
        3: ["均价", "售价", "价格分布", "价格迁移"],
        2: ["价格", "msrp", "溢价"],
        1: ["贵", "便宜"],
    },
    "market-context": {
        3: ["政策", "补贴", "关税", "热点", "新闻"],
        2: ["incentive", "subsidy", "tariff", "市场环境", "宏观"],
        1: ["法规", "regulation"],
    },
}

_WIDE_SCOPE_TRIGGERS = [
    "概况", "概述", "总览", "全貌", "整体",
    "什么情况", "帮我看看", "分析一下",
    "大盘", "市场情况",
]

_NEGATION_PREFIXES = ["不看", "不管", "不要", "先不", "别看", "跳过"]


def _split_clauses(question: str) -> list[str]:
    """Split a question into independent clauses by Chinese/ASCII punctuation."""
    parts = [p.strip() for p in _CLAUSE_SEP.split(question.strip()) if p.strip()]
    if not parts:
        return [question.strip()] if question.strip() else [""]
    # Merge short clauses (< 4 chars) into the previous one
    merged = [parts[0]]
    for clause in parts[1:]:
        if len(clause) < 4:
            merged[-1] = merged[-1] + clause
        else:
            merged.append(clause)
    return merged


def _score_clause(clause: str) -> dict[str, int]:
    """Score a single clause against all intent keyword groups."""
    lowered = clause.lower()
    scores: dict[str, int] = {}
    for intent, weight_map in _INTENT_KEYWORDS.items():
        total = 0
        for weight, keywords in weight_map.items():
            for kw in keywords:
                if kw in lowered:
                    total += weight
        if total > 0:
            scores[intent] = total
    return scores


def _detect_negated_intents(question: str) -> set[str]:
    """Detect intents that the user explicitly wants to exclude."""
    lowered = question.lower()
    negated: set[str] = set()
    for prefix in _NEGATION_PREFIXES:
        idx = lowered.find(prefix)
        if idx < 0:
            continue
        trail = lowered[idx + len(prefix) :]
        # Cut at first clause separator so we only negate the adjacent term
        sep_match = _CLAUSE_SEP.search(trail)
        if sep_match:
            trail = trail[: sep_match.start()]
        trail = trail[:20]
        trail_scores = _score_clause(trail)
        if trail_scores:
            negated.add(max(trail_scores, key=trail_scores.get))
    # "除了X以外" pattern
    m = re.search(r"除了(.{2,10})以?外", lowered)
    if m:
        trail_scores = _score_clause(m.group(1))
        if trail_scores:
            negated.add(max(trail_scores, key=trail_scores.get))
    return negated


def infer_country_chat_intent(question: str) -> str:
    return infer_country_chat_intents(question)[0]


def infer_country_chat_intents(question: str) -> list[str]:
    clauses = _split_clauses(question)

    # Weighted scoring per clause
    global_scores: dict[str, int] = {}
    for clause in clauses:
        for intent, score in _score_clause(clause).items():
            global_scores[intent] = global_scores.get(intent, 0) + score

    # Remove negated intents
    for negated in _detect_negated_intents(question):
        global_scores.pop(negated, None)

    matched = [k for k, v in global_scores.items() if v > 0]

    if not matched:
        lowered = question.strip().lower()
        if any(t in lowered for t in _WIDE_SCOPE_TRIGGERS):
            matched = ["brand-ranking", "segment-analysis", "trend-summary"]
        else:
            matched.append("general-summary")

    return _normalize_intents(matched)


def _resolve_country_column() -> str | None:
    columns = repo.list_columns()
    return query_service._resolve_existing_column(  # noqa: SLF001
        query_service.COUNTRY_CANDIDATES,
        columns,
    )


def _nvidia_provider_available() -> bool:
    return bool(
        os.getenv("NVIDIA_API_KEY", "").strip()
        or os.getenv("NVAPI_KEY", "").strip()
    )


def _build_sales_rankings(
    *,
    frame: pd.DataFrame,
    dimension: str,
    limit: int,
) -> list[dict[str, Any]]:
    if (
        frame.empty
        or dimension not in frame.columns
        or "Sales" not in frame.columns
    ):
        return []
    ranking = frame[[dimension, "Sales"]].copy()
    ranking[dimension] = ranking[dimension].astype(str).str.strip()
    ranking.loc[ranking[dimension] == "", dimension] = pd.NA
    ranking["Sales"] = pd.to_numeric(
        ranking["Sales"],
        errors="coerce",
    ).fillna(0.0)
    ranking = ranking.dropna(subset=[dimension])
    if ranking.empty:
        return []

    grouped = (
        ranking.groupby(dimension, as_index=False, dropna=False)["Sales"]
        .sum()
        .sort_values(["Sales", dimension], ascending=[False, True])
        .head(max(1, int(limit)))
    )
    return [
        {
            "label": str(row[dimension]),
            "value": int(round(float(row["Sales"]))),
        }
        for _, row in grouped.iterrows()
    ]


def _query_local_wiki(
    query: str,
    country: str,
    brand: str = "",
    model: str = "",
) -> list[str]:
    """Retrieve fine-grained vehicle specifications from local wiki."""
    results = local_wiki_service.query_local_wiki_documents(
        query,
        country=country,
        brand=brand,
        model=model,
        limit=5,
    )
    if results:
        return results
    return [
        "Local vehicle_wiki is unavailable or returned no matching facts."
    ]


def _query_news_wiki(
    query: str,
    snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    news_digest = snapshot.get("newsDigest")
    market_events = snapshot.get("marketEvents")
    if not news_digest and not market_events:
        country = str(snapshot.get("country") or "").strip()
        if country:
            try:
                refreshed = news_digest_service.refresh_country_news(
                    country,
                    persist=False,
                    enrich_with_gemini=None,
                )
                news_digest = refreshed.get("newsDigest")
                market_events = refreshed.get("marketEvents")
                snapshot["newsDigest"] = news_digest
                snapshot["marketEvents"] = market_events or []
            except Exception as exc:  # noqa: BLE001
                log.warning("On-demand news refresh failed: %s", exc)

    results = news_wiki_service.query_news_wiki(
        query,
        news_digest=news_digest,
        market_events=market_events,
        limit=4,
    )
    return results


def _answer_with_nvidia(
    *,
    country: str,
    question: str,
    intents: list[str],
    user_params: dict[str, Any],
    snapshot: dict[str, Any],
    history: list[dict[str, str]],
) -> str:
    client = NvidiaChatClient(default_model=DEFAULT_NVIDIA_CHAT_MODEL)
    primary_intent = intents[0] if intents else "general-summary"
    messages: list[Any] = [
        ChatMessage(role="system", content=_SYSTEM_PROMPT),
    ]
    for turn in history[-MAX_HISTORY_TURNS:]:
        role = str(turn.get("role", "")).strip().lower()
        content = str(turn.get("content", "")).strip()
        if role not in {"user", "assistant"} or not content:
            continue
        messages.append({"role": role, "content": content[:2000]})

    tools = [
        {
            "type": "function",
            "function": {
                "name": "query_market_kpis",
                "description": "获取当前国家市场总体概况（品牌数量、车型数量、总销量、同比等宏观情况）。",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_top_brands",
                "description": "获取当前国家市场销量排名前列的汽车品牌列表及其市场份额数据。",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_powertrain_mix",
                "description": "获取该国市场的动力类型分布（如 BEV, PHEV, HEV 的销量、份额排名），用于计算新能源渗透率。",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_positioning_map",
                "description": "获取各类竞品的售价(MSRP)和车长(mm)定位矩阵（非常适合竞品分析、售价战分析）。",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_segment_metrics",
                "description": "获取轿车(Sedan)和SUV各级别(Segment)趋势、以及各类车型的销售矩阵。",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_news_and_events",
                "description": "获取该国的汽车市场本地政策、法规关税、近期新闻事件及宏观投资摘要。",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_news_wiki",
                "description": "检索新闻事实层，返回和当前问题最相关的政策、补贴、关税、竞争事件或 Gemini 新闻摘要片段。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "例如 '德国公司车税收支持对 BEV 有什么影响'。"
                        }
                    },
                    "required": ["query"]
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_local_wiki",
                "description": "检索本地 RAG 知识库，获取特定车辆的精确尺寸（长度/轴距）和MSRP建议零售价数据。当用户询问某款车型的长宽高或售价时调用此工具。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "具体的查询句子，例如 'Volkswagen XC60 的尺寸和售价'。"
                        },
                        "brand": {
                            "type": "string",
                            "description": "汽车品牌名称（可选），如果已知则填入以缩小搜索范围。"
                        },
                        "model": {
                            "type": "string",
                            "description": "车型系列名称（可选），如果已知则填入以缩小搜索范围。"
                        }
                    },
                    "required": ["query"]
                },
            },
        },
    ]

    messages.append(
        {
            "role": "user",
            "content": (
                f"国家: {country}\n"
                f"用户问题: {question}\n"
                f"已解析参数: {json.dumps(user_params, ensure_ascii=False)}\n\n"
                "请先调用一个或多个工具，根据回答需要的材料查询实时数据。"
                "涉及车型尺寸/价格时优先用 query_local_wiki；"
                "涉及新闻、政策、补贴、关税、竞争事件时优先用 query_news_wiki。"
                "拿到 tool 的返回值后综合思考，再用表格输出竞品分析。"
            ),
        }
    )

    max_tool_turns = 3
    turn_count = 0

    while turn_count < max_tool_turns:
        response = client.chat(
            messages,
            max_tokens=1024,
            temperature=0.2,
            timeout=60,
            tools=tools,
        )
        
        first_msg = response.first_message
        if not first_msg:
            raise RuntimeError("NVIDIA 返回了空响应")

        tool_calls = first_msg.get("tool_calls")
        if not tool_calls:
            text = (response.text or "").strip()
            if not text:
                raise RuntimeError("NVIDIA 返回了空文本内容")
            return text

        # 记录模型调用的请求
        messages.append(first_msg)

        # 逐个执行对应的方法并返回结果
        for tc in tool_calls:
            tc_id = tc.get("id")
            func = tc.get("function", {})
            fn_name = func.get("name")

            tool_result_obj: Any = {}
            if fn_name == "query_market_kpis":
                tool_result_obj = snapshot.get("kpis", {})
            elif fn_name == "query_top_brands":
                tool_result_obj = snapshot.get("ytdBrandRanking", snapshot.get("topBrands", []))
            elif fn_name == "query_powertrain_mix":
                tool_result_obj = snapshot.get("powertrainMix", [])
            elif fn_name == "query_positioning_map":
                tool_result_obj = snapshot.get("positioningMap", {})
            elif fn_name == "query_segment_metrics":
                tool_result_obj = {
                    "matrix": snapshot.get("segmentMatrix", {}),
                    "suvSedanTrend": snapshot.get("suvSedanTrend", []),
                }
            elif fn_name == "query_news_and_events":
                tool_result_obj = {
                    "profile": snapshot.get("countryProfile", country_profiles.get_compact_profile(country)),
                    "events": _compact_market_events_for_context(snapshot.get("marketEvents", []), limit=3) if snapshot.get("marketEvents") else [],
                    "digest": snapshot.get("newsDigest", {}),
                }
            elif fn_name == "query_news_wiki":
                try:
                    args = json.loads(
                        tc.get("function", {}).get("arguments", "{}")
                    )
                    q_text = args.get("query", question)
                    news_hits = _query_news_wiki(q_text, snapshot)
                    tool_result_obj = {"news_facts": news_hits}
                except Exception as e:
                    tool_result_obj = {
                        "error": f"Failed to query news wiki: {e}"
                    }
            elif fn_name == "query_local_wiki":
                # Assuming func['arguments'] comes as a JSON string from LLM in NVIDIA NIM
                try:
                    args = json.loads(tc.get("function", {}).get("arguments", "{}"))
                    q_brand = args.get("brand") or user_params.get("brand", "")
                    q_model = args.get("model") or user_params.get("model", "")
                    q_text = args.get("query", question)
                    wiki_results = _query_local_wiki(
                        query=q_text, 
                        country=country, 
                        brand=q_brand, 
                        model=q_model
                    )
                    tool_result_obj = {"wiki_facts": wiki_results}
                except Exception as e:
                    tool_result_obj = {"error": f"Failed to parse or query RAG: {e}"}
            else:
                tool_result_obj = {"error": f"Unknown tool {fn_name}"}

            # 序列化为 JSON 字符串交还给 LLM
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc_id,
                    "name": fn_name,
                    "content": json.dumps(tool_result_obj, ensure_ascii=False)[:3000],  # 截断以避免单次Token超出限制
                }
            )

        turn_count += 1
        
    return "分析中断：模型执行函数调用层级过深，未能生成最终解答。"


def _select_context_for_intent(
    snapshot: dict[str, Any],
    intent: str,
) -> dict[str, Any]:
    """Return a subset of the snapshot relevant to the detected intent.

    This keeps the JSON payload sent to the LLM within ~3-5 K tokens
    instead of dumping the entire deck (~20 K tokens).
    """
    # Always include core fields
    ctx: dict[str, Any] = {
        "country": snapshot.get("country"),
        "periodLabel": snapshot.get("periodLabel", ""),
        "kpis": snapshot.get("kpis", {}),
        "overviewSummary": snapshot.get("overviewSummary", {}),
        "powertrainMix": snapshot.get("powertrainMix", []),
    }

    # Always inject compact country profile when available
    compact_profile = country_profiles.get_compact_profile(
        str(snapshot.get("country", "")),
    )
    if compact_profile:
        ctx["countryProfile"] = compact_profile

    # Inject insight-card conclusions as compact analyst notes
    all_cards = snapshot.get("_allInsightCards", [])
    relevant_cards = insight_card_service.cards_for_intent(all_cards, intent)
    if relevant_cards:
        ctx["insightCards"] = [
            {"title": c["title"], "conclusion": c["conclusion"]}
            for c in relevant_cards
        ]

    if intent == "brand-ranking":
        ctx["ytdBrandRanking"] = snapshot.get("ytdBrandRanking", [])
        ctx["monthlyBrandRanking"] = snapshot.get(
            "monthlyBrandRanking", [],
        )
        ctx["topBrands"] = snapshot.get("topBrands", [])
        if snapshot.get("powertrainBubble"):
            ctx["powertrainBubble"] = _slice_list(
                snapshot["powertrainBubble"], 15,
            )
        if snapshot.get("salesVsPrice"):
            ctx["salesVsPrice"] = _slice_list(
                snapshot["salesVsPrice"], 10,
            )

    elif intent == "segment-analysis":
        ctx["segmentMatrix"] = snapshot.get("segmentMatrix", {})
        ctx["suvSedanTrend"] = snapshot.get("suvSedanTrend", [])
        ctx["drilldown"] = snapshot.get("drilldown", {})
        ctx["suvA"] = snapshot.get("suvA", {})
        if snapshot.get("segmentShareByLength"):
            ctx["segmentShareByLength"] = _slice_list(
                snapshot["segmentShareByLength"], 15,
            )

    elif intent == "origin-analysis":
        ctx["originAnalysis"] = snapshot.get("originAnalysis", {})
        ctx["ytdBrandRanking"] = snapshot.get("ytdBrandRanking", [])
        if snapshot.get("seasonalityHeatmap"):
            ctx["seasonalityHeatmap"] = _slice_list(
                snapshot["seasonalityHeatmap"], 20,
            )
    elif intent == "market-context":
        # Full profile already injected above; add supporting data
        if snapshot.get("countryProfile"):
            ctx["countryProfile"] = snapshot["countryProfile"]
        ctx["ytdBrandRanking"] = _slice_list(
            snapshot.get("ytdBrandRanking", []), 8,
        )
        if snapshot.get("newsDigest"):
            ctx["newsDigest"] = snapshot["newsDigest"]
        if snapshot.get("marketEvents"):
            ctx["marketEvents"] = _compact_market_events_for_context(
                snapshot["marketEvents"],
                limit=3,
            )
    elif intent in ("powertrain-mix", "nev-analysis"):
        ctx["drilldown"] = snapshot.get("drilldown", {})
        ctx["suvA"] = snapshot.get("suvA", {})
        ctx["ytdBrandRanking"] = _slice_list(
            snapshot.get("ytdBrandRanking", []), 8,
        )
        # Dashboard enrichment
        if snapshot.get("nevRangeDistribution"):
            ctx["nevRangeDistribution"] = snapshot["nevRangeDistribution"]
        if snapshot.get("bevShareBySegment"):
            ctx["bevShareBySegment"] = snapshot["bevShareBySegment"]
        if snapshot.get("powertrainVsPrice"):
            ctx["powertrainVsPrice"] = _slice_list(
                snapshot["powertrainVsPrice"], 10,
            )
        if snapshot.get("powertrainBubble"):
            ctx["powertrainBubble"] = _slice_list(
                snapshot["powertrainBubble"], 12,
            )
        if snapshot.get("nevCapacityVsMsrp"):
            ctx["nevCapacityVsMsrp"] = _slice_list(
                snapshot["nevCapacityVsMsrp"], 10,
            )

    elif intent == "positioning-analysis":
        if snapshot.get("positioningMap"):
            pm = snapshot["positioningMap"]
            ctx["positioningMap"] = {
                "rows": pm.get("rows", 0),
                "items": pm.get("items", [])[:15],
                "target": pm.get("target"),
                "cluster_top3": pm.get("cluster_top3", []),
            }
        if snapshot.get("priceDistribution"):
            ctx["priceDistribution"] = _slice_list(
                snapshot["priceDistribution"], 15,
            )
        if snapshot.get("pricePerMeter"):
            ctx["pricePerMeter"] = _slice_list(
                snapshot["pricePerMeter"], 10,
            )
        ctx["ytdBrandRanking"] = _slice_list(
            snapshot.get("ytdBrandRanking", []), 8,
        )

    elif intent == "trend-summary":
        ctx["yearSeries"] = snapshot.get("yearSeries", [])
        ctx["monthSeries"] = snapshot.get("monthSeries", [])
        ctx["ytdBrandRanking"] = _slice_list(
            snapshot.get("ytdBrandRanking", []), 8,
        )
        ctx["suvSedanTrend"] = snapshot.get("suvSedanTrend", [])
        if snapshot.get("priceMigration"):
            ctx["priceMigration"] = _slice_list(
                snapshot["priceMigration"], 15,
            )
        if snapshot.get("seasonalityHeatmap"):
            ctx["seasonalityHeatmap"] = _slice_list(
                snapshot["seasonalityHeatmap"], 20,
            )

    elif intent == "competitive":
        ctx["ytdBrandRanking"] = snapshot.get("ytdBrandRanking", [])
        ctx["segmentMatrix"] = snapshot.get("segmentMatrix", {})
        ctx["originAnalysis"] = snapshot.get("originAnalysis", {})
        if snapshot.get("positioningMap"):
            pm = snapshot["positioningMap"]
            ctx["positioningMap"] = {
                "rows": pm.get("rows", 0),
                "items": pm.get("items", [])[:15],
                "target": pm.get("target"),
                "cluster_top3": pm.get("cluster_top3", []),
            }

    else:
        # general-summary / pricing-summary
        ctx["topBrands"] = _slice_list(
            snapshot.get("topBrands", []), 8,
        )
        ctx["topModels"] = _slice_list(
            snapshot.get("topModels", []), 8,
        )
        ctx["ytdBrandRanking"] = _slice_list(
            snapshot.get("ytdBrandRanking", []), 8,
        )
        ctx["segmentMatrix"] = snapshot.get("segmentMatrix", {})
        ctx["yearSeries"] = snapshot.get("yearSeries", [])
        ctx["monthSeries"] = snapshot.get("monthSeries", [])
        if snapshot.get("priceDistribution"):
            ctx["priceDistribution"] = _slice_list(
                snapshot["priceDistribution"], 15,
            )
        if snapshot.get("powertrainVsPrice"):
            ctx["powertrainVsPrice"] = _slice_list(
                snapshot["powertrainVsPrice"], 10,
            )
        if snapshot.get("pricePerMeter"):
            ctx["pricePerMeter"] = _slice_list(
                snapshot["pricePerMeter"], 10,
            )
        if snapshot.get("salesVsPrice"):
            ctx["salesVsPrice"] = _slice_list(
                snapshot["salesVsPrice"], 10,
            )
        if snapshot.get("segmentShareByLength"):
            ctx["segmentShareByLength"] = _slice_list(
                snapshot["segmentShareByLength"], 15,
            )
        if snapshot.get("estimatedTco"):
            ctx["estimatedTco"] = _slice_list(
                snapshot["estimatedTco"], 10,
            )
        if snapshot.get("powertrainBubble"):
            ctx["powertrainBubble"] = _slice_list(
                snapshot["powertrainBubble"], 12,
            )
        if snapshot.get("nevCapacityVsMsrp"):
            ctx["nevCapacityVsMsrp"] = _slice_list(
                snapshot["nevCapacityVsMsrp"], 10,
            )

    return ctx


def _select_context_for_intents(
    snapshot: dict[str, Any],
    intents: list[str],
) -> dict[str, Any]:
    ordered_intents = _normalize_intents(intents)
    if len(ordered_intents) == 1:
        context = _select_context_for_intent(snapshot, ordered_intents[0])
        context["primaryIntent"] = ordered_intents[0]
        context["intents"] = ordered_intents
        return context

    merged: dict[str, Any] = {
        "country": snapshot.get("country"),
        "periodLabel": snapshot.get("periodLabel", ""),
        "kpis": snapshot.get("kpis", {}),
        "overviewSummary": snapshot.get("overviewSummary", {}),
        "primaryIntent": ordered_intents[0],
        "intents": ordered_intents,
    }

    for intent in ordered_intents:
        intent_context = _select_context_for_intent(snapshot, intent)
        for key, value in intent_context.items():
            candidate = dict(merged)
            if key == "insightCards":
                candidate[key] = _merge_insight_cards(
                    merged.get(key, []),
                    value,
                )
            elif key not in candidate:
                candidate[key] = value
            else:
                continue
            if (
                len(json.dumps(candidate, ensure_ascii=False))
                <= CONTEXT_CHAR_BUDGET
            ):
                merged = candidate

    return merged


def _slice_list(items: list, limit: int) -> list:
    return items[:limit] if isinstance(items, list) else items


def _suggestions_for_intent(
    intent: str,
    snapshot: dict[str, Any],
) -> list[str]:
    """Return follow-up suggestions based on the answered intent."""
    specific = INTENT_SUGGESTIONS.get(intent)
    if specific:
        return specific
    return list(COUNTRY_PROMPT_SUGGESTIONS)


def _suggestions_for_intents(
    intents: list[str],
    snapshot: dict[str, Any],
) -> list[str]:
    suggestions: list[str] = []
    for intent in _normalize_intents(intents):
        for suggestion in _suggestions_for_intent(intent, snapshot):
            if suggestion not in suggestions:
                suggestions.append(suggestion)
    return suggestions[:6] or list(COUNTRY_PROMPT_SUGGESTIONS)


def _build_fallback_answer(
    *,
    country: str,
    question: str,
    intent: str,
    snapshot: dict[str, Any],
    provider_error: str | None,
) -> str:
    del question
    kpis = snapshot.get("kpis", {})
    top_brands = snapshot.get("topBrands", [])
    top_models = snapshot.get("topModels", [])
    powertrain_mix = snapshot.get("powertrainMix", [])
    latest_year = _latest_point(snapshot.get("yearSeries", []))
    latest_month = _latest_point(snapshot.get("monthSeries", []))

    period_label = snapshot.get("periodLabel", "")
    intro = (
        f"我先基于 {country} 的当前数据快照回答你。"
        if not provider_error
        else f"NVIDIA 当前不可用，我先基于 {country} 的当前数据快照回答你。"
    )
    if period_label:
        intro += f"（数据截至 {period_label}）"

    if intent == "brand-ranking":
        ytd_brands = snapshot.get("ytdBrandRanking", [])
        if ytd_brands:
            top3 = "、".join(
                f"{b.get('brand', '?')}({b.get('volume', 0):,}辆, "
                f"MS{b.get('share', 0):.1f}%)"
                for b in ytd_brands[:3]
            )
            return (
                f"{intro}\n\n"
                f"YTD 品牌排名前三：{top3}。\n\n"
                f"按累计销量口径，前六品牌为："
                f"{_format_ranked_items(top_brands)}。"
            )
        return (
            f"{intro}\n\n"
            f"品牌数约 {int(kpis.get('brandCount', 0))} 个，"
            f"按累计销量口径看，头部品牌是："
            f"{_format_ranked_items(top_brands)}。"
        )

    if intent == "segment-analysis":
        seg_matrix = snapshot.get("segmentMatrix", {})
        rows = (
            seg_matrix.get("rows", [])
            if isinstance(seg_matrix, dict) else []
        )
        if rows:
            seg_lines = "、".join(
                f"{r.get('segment', '?')}"
                f"({r.get('currentMonth', 0):,}辆)"
                for r in rows[:5]
            )
            return (
                f"{intro}\n\n"
                f"当月各 segment 销量：{seg_lines}。\n\n"
                f"动力结构：{_format_ranked_items(powertrain_mix)}。"
            )
        return (
            f"{intro}\n\n"
            "当前快照未包含完整 segment 矩阵数据。"
            f"已有动力结构：{_format_ranked_items(powertrain_mix)}。"
        )

    if intent == "origin-analysis":
        origin = snapshot.get("originAnalysis", {})
        summary_text = (
            origin.get("summaryText", "")
            if isinstance(origin, dict) else ""
        )
        if summary_text:
            return f"{intro}\n\n{summary_text}"
        return (
            f"{intro}\n\n"
            "当前快照未包含完整车系阵营数据。"
            f"头部品牌为：{_format_ranked_items(top_brands)}。"
        )

    if intent == "nev-analysis":
        nev_items = [
            p for p in powertrain_mix
            if p.get("label", "").upper()
            in {"BEV", "PHEV", "HEV", "REEV"}
        ]
        total_sales = (
            sum(p.get("value", 0) for p in powertrain_mix) or 1
        )
        nev_sales = sum(p.get("value", 0) for p in nev_items)
        nev_share = nev_sales / total_sales * 100
        return (
            f"{intro}\n\n"
            f"新能源（BEV+PHEV+HEV）合计销量约 {nev_sales:,} 辆，"
            f"占总量的 {nev_share:.1f}%。\n"
            f"动力结构明细：{_format_ranked_items(powertrain_mix)}。"
        )

    if intent == "powertrain-mix":
        return (
            f"{intro}\n\n"
            "按累计销量口径看，动力类型分布为："
            f"{_format_ranked_items(powertrain_mix)}。"
        )

    if intent == "positioning-analysis":
        pm = snapshot.get("positioningMap", {})
        target = pm.get("target")
        cluster_top3 = pm.get("cluster_top3", [])
        items = pm.get("items", [])
        if target and items:
            nearby = "、".join(
                f"{it.get('Brand', '?')} {it.get('Model', '?')}"
                f"({int(it.get('MSRP', 0)):,})"
                for it in items[:5]
            )
            top3_text = (
                f"同聚类头部竞品：{'、'.join(cluster_top3)}。"
                if cluster_top3
                else ""
            )
            return (
                f"{intro}\n\n"
                f"目标定位：车长{target.get('Length', '?')}mm，"
                f"价格{target.get('MSRP', '?')}。\n"
                f"临近竞品：{nearby}。\n"
                f"{top3_text}"
            )
        return (
            f"{intro}\n\n"
            "当前未提取到足够的目标参数(车长/定价)来做竞品定位分析。\n"
            f"头部品牌：{_format_ranked_items(top_brands)}。"
        )

    if intent == "pricing-summary":
        avg_msrp = kpis.get("avgMsrp")
        avg_text = (
            f"平均 MSRP 约 {float(avg_msrp):,.0f}"
            if isinstance(avg_msrp, (int, float))
            else "当前快照没有稳定均价"
        )
        return (
            f"{intro}\n\n"
            f"从现有快照看，{avg_text}；"
            f"样本车型数约 {int(kpis.get('modelCount', 0))} 个，"
            f"按累计销量口径看，头部车型包括："
            f"{_format_ranked_items(top_models)}。"
        )

    if intent == "trend-summary":
        return (
            f"{intro}\n\n"
            f"最近年度锚点 {_format_point(latest_year)}，"
            f"最近月份锚点 {_format_point(latest_month)}。"
            f"累计销量约 {float(kpis.get('cumulativeSales', 0)):.0f}。"
        )

    if intent == "market-context":
        profile = snapshot.get("countryProfile")
        if not profile:
            profile = country_profiles.get_country_profile(country)
        news_digest = snapshot.get("newsDigest") or {}
        news_summary = str(news_digest.get("summary") or "").strip()
        if not news_summary:
            news_summary = "；".join(
                str(item).strip()
                for item in news_digest.get("highlights", [])[:3]
                if str(item).strip()
            )
        if profile:
            policies = "；".join(profile.get("key_policies", [])[:3])
            topics = "；".join(profile.get("hot_topics", [])[:3])
            news_block = f"\n\n【最新事件】{news_summary}" if news_summary else ""
            return (
                f"{intro}\n\n"
                f"【关键政策】{policies}\n\n"
                f"【市场热点】{topics}\n\n"
                f"{news_block}\n\n"
                f"动力结构背景：{profile.get('powertrain_context', '暂无')}。"
            )
        if news_summary:
            return (
                f"{intro}\n\n"
                f"【最新事件】{news_summary}\n\n"
                f"头部品牌：{_format_ranked_items(top_brands)}。"
            )
        return (
            f"{intro}\n\n"
            f"暂无 {country} 的政策/热点知识库覆盖。"
            f"头部品牌：{_format_ranked_items(top_brands)}。"
        )

    return (
        f"{intro}\n\n"
        f"{country} 市场概况：{int(kpis.get('brandCount', 0))} 个品牌、"
        f"{int(kpis.get('modelCount', 0))} 个车型、"
        f"{int(kpis.get('versionCount', 0))} 个版本。\n"
        f"头部品牌：{_format_ranked_items(top_brands)}。\n"
        f"动力结构：{_format_ranked_items(powertrain_mix)}。\n\n"
        "你可以继续追问品牌格局、细分市场(segment)、"
        "车系阵营(origin)、动力结构、趋势或价格。"
    )


def _build_fallback_answer_for_intents(
    *,
    country: str,
    question: str,
    intents: list[str],
    snapshot: dict[str, Any],
    provider_error: str | None,
) -> str:
    ordered_intents = _normalize_intents(intents)
    primary_answer = _build_fallback_answer(
        country=country,
        question=question,
        intent=ordered_intents[0],
        snapshot=snapshot,
        provider_error=provider_error,
    )
    if len(ordered_intents) == 1:
        return primary_answer

    extra_sections: list[str] = []
    for intent in ordered_intents[1:3]:
        detail = _build_fallback_answer(
            country=country,
            question=question,
            intent=intent,
            snapshot=snapshot,
            provider_error=provider_error,
        )
        extra_body = _strip_fallback_intro(detail)
        if extra_body:
            extra_sections.append(
                f"[{_intent_display_label(intent)}]\n{extra_body}"
            )

    if not extra_sections:
        return primary_answer
    return f"{primary_answer}\n\n补充维度：\n\n" + "\n\n".join(extra_sections)


def _build_country_query_filters(
    country: str,
    user_params: dict[str, Any],
) -> dict[str, list[str]]:
    country_col = _resolve_country_column()
    if not country_col:
        return {}

    filters: dict[str, list[str]] = {country_col: [country]}
    powertrain_col = query_service._resolve_existing_column(  # noqa: SLF001
        query_service.POWERTRAIN_CANDIDATES,
        repo.list_columns(),
    )
    if powertrain_col and user_params.get("powertrain"):
        filters[powertrain_col] = [str(user_params["powertrain"])]
    return filters


def _merge_insight_cards(
    base_cards: list[dict[str, Any]],
    next_cards: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = list(base_cards)
    seen = {
        (str(card.get("title", "")), str(card.get("conclusion", "")))
        for card in base_cards
    }
    for card in next_cards:
        signature = (
            str(card.get("title", "")),
            str(card.get("conclusion", "")),
        )
        if signature in seen:
            continue
        merged.append(card)
        seen.add(signature)
    return merged[:6]


def _normalize_intents(intents: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for intent in INTENT_PRIORITY:
        if intent in intents and intent not in seen:
            ordered.append(intent)
            seen.add(intent)
    for intent in intents:
        if intent not in seen:
            ordered.append(intent)
            seen.add(intent)
    return ordered or ["general-summary"]


def _limit_intents_for_deck(intents: list[str]) -> list[str]:
    ordered = _normalize_intents(intents)
    return ordered[:MAX_DECK_BASE_INTENTS] if ordered else ["general-summary"]


_DECK_INTENT_EXPANSIONS: dict[str, list[str]] = {
    "positioning-analysis": ["competitive", "pricing-summary"],
    "competitive": ["positioning-analysis", "brand-ranking"],
    "segment-analysis": ["trend-summary", "nev-analysis"],
    "origin-analysis": ["competitive", "trend-summary"],
    "market-context": ["trend-summary", "brand-ranking"],
    "nev-analysis": ["powertrain-mix", "segment-analysis"],
    "pricing-summary": ["positioning-analysis", "competitive"],
    "brand-ranking": ["trend-summary", "powertrain-mix"],
    "powertrain-mix": ["nev-analysis", "pricing-summary"],
    "trend-summary": ["brand-ranking", "segment-analysis"],
    "general-summary": ["brand-ranking", "segment-analysis", "trend-summary"],
}


def _chart_deck_intents(intents: list[str]) -> list[str]:
    base = _limit_intents_for_deck(intents)
    candidates = list(base)
    for intent in base:
        candidates.extend(_DECK_INTENT_EXPANSIONS.get(intent, []))

    ordered = _normalize_intents(candidates)
    if len(ordered) <= MAX_DECK_INTENTS:
        return ordered

    selected: list[str] = []
    for intent in ordered:
        if intent in base and intent not in selected:
            selected.append(intent)

    for intent in ordered:
        if intent in selected:
            continue
        selected.append(intent)
        if len(selected) >= MAX_DECK_INTENTS:
            break
    return selected


def _strip_fallback_intro(text: str) -> str:
    parts = text.split("\n\n", 1)
    return parts[1] if len(parts) == 2 else text


def _intent_display_label(intent: str) -> str:
    labels = {
        "brand-ranking": "品牌格局",
        "segment-analysis": "细分市场",
        "origin-analysis": "车系阵营",
        "powertrain-mix": "动力结构",
        "trend-summary": "趋势变化",
        "nev-analysis": "新能源分析",
        "positioning-analysis": "竞争定位",
        "competitive": "竞品比较",
        "pricing-summary": "价格结构",
        "market-context": "政策热点",
        "general-summary": "市场概况",
    }
    return labels.get(intent, intent)


def _format_ranked_items(items: list[dict[str, Any]]) -> str:
    if not items:
        return "暂无足够数据"
    return "，".join(
        f"{item['label']}({int(item['value']):,})"
        for item in items[:5]
    )


def _latest_point(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not items:
        return None
    return items[-1]


def _format_point(point: dict[str, Any] | None) -> str:
    if not point:
        return "暂无数据"
    label = str(point.get("time", "-")).strip() or "-"
    value = float(point.get("value", 0) or 0)
    return f"{label} = {value:,.0f}"
