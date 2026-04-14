from __future__ import annotations

import json
import logging
import os
from typing import Any

import pandas as pd

from app.infra import parquet_repository as repo
from app.scraper import enable_external_scraper_package
from app.services import query_service
from app.services import market_scan_service


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

COUNTRY_PROMPT_SUGGESTIONS = [
    "这个国家最近几年销量趋势怎么样？",
    "这个国家目前最强的品牌有哪些？",
    "这个国家的动力总成结构有什么特点？",
    "如果我要先看这个国家市场，你建议关注什么？",
    "分析一下这个国家的 SUV 细分市场",
    "这个国家的中系/欧系/日系品牌表现如何？",
]

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
}

_SYSTEM_PROMPT = (
    "你是 JATO Dynamics 的高级市场分析师，专注欧洲汽车市场研究。\n\n"
    "分析原则：\n"
    "1. 给出明确结论 + 关键数字支撑（销量、份额、同比/环比变化率）\n"
    "2. 主动比较同比（YoY）和环比（MoM）变化，指出增长或下降趋势\n"
    "3. 如果涉及品牌排名，指出份额变化和名次变动\n"
    "4. 如果涉及细分市场(segment)，解释市场结构并对比 SUV vs Sedan 趋势\n"
    "5. 如果涉及车系(origin)，分析各阵营市占率和增长动力\n"
    "6. 用简体中文回答，语言简洁专业，像给汽车行业客户做分析汇报\n"
    "7. 如果数据中确实没有用户问的维度，明确说'当前数据未覆盖'并建议可替代的分析方向\n\n"
    "数据字段说明：\n"
    "- ytdBrandRanking: YTD品牌排名，volume=销量(辆)，share=份额(%)，ytdYoy=同比增幅(%)\n"
    "- segmentMatrix: 车型级别矩阵(SUV-A00~SD-C)，含当月/MoM/YoY/YTD指标\n"
    "- originAnalysis: 按车系阵营(欧系/日系/韩系/美系/中系)的份额分析\n"
    "- suvSedanTrend: SUV vs Sedan 占比月度趋势\n"
    "- drilldown/suvA: 特定 segment 的车型排名和燃料面板\n"
    "- powertrainMix: 动力类型(BEV/PHEV/HEV/MHEV/ICE)累计销量\n"
    "- overviewSummary: 市场总量、当月MoM/YoY变化\n"
    "- MSRP 单位为各国本地货币\n"
)


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
) -> dict[str, Any]:
    normalized_country = str(country).strip()
    normalized_question = str(question).strip()
    if not normalized_country:
        raise ValueError("country 不能为空")
    if not normalized_question:
        raise ValueError("question 不能为空")

    snapshot = build_country_snapshot(normalized_country)
    intent = infer_country_chat_intent(normalized_question)

    provider = "fallback"
    provider_available = _nvidia_provider_available()
    provider_reason = None
    answer = _build_fallback_answer(
        country=normalized_country,
        question=normalized_question,
        intent=intent,
        snapshot=snapshot,
        provider_error=None,
    )

    if provider_available:
        try:
            answer = _answer_with_nvidia(
                country=normalized_country,
                question=normalized_question,
                intent=intent,
                snapshot=snapshot,
                history=history or [],
            )
            provider = "nvidia"
        except Exception as exc:  # noqa: BLE001
            provider_reason = str(exc)
            answer = _build_fallback_answer(
                country=normalized_country,
                question=normalized_question,
                intent=intent,
                snapshot=snapshot,
                provider_error=provider_reason,
            )
    else:
        provider_reason = (
            "当前环境没有 NVIDIA_API_KEY / NVAPI_KEY，已使用本地摘要降级回答。"
        )

    suggestions = _suggestions_for_intent(intent, snapshot)

    return {
        "country": normalized_country,
        "question": normalized_question,
        "answer": answer,
        "intent": intent,
        "provider": provider,
        "providerAvailable": provider_available,
        "providerReason": provider_reason,
        "contextSnapshot": snapshot,
        "suggestedPrompts": suggestions,
    }


def build_country_snapshot(country: str) -> dict[str, Any]:
    country_col = _resolve_country_column()
    if not country_col:
        raise ValueError("数据集中未找到国家字段")

    filters = {country_col: [country]}
    overview = query_service.query_overview(
        filters=filters,
        prefer_precomputed=True,
        top_n=12,
    )
    vehicle_frame = query_service._build_vehicle_frame(filters)  # noqa: SLF001

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
    }

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


def infer_country_chat_intent(question: str) -> str:
    lowered = question.strip().lower()
    # segment / SUV analysis — check first (before brand) because
    # "SUV 品牌" should route to segment-analysis.
    if any(
        token in lowered
        for token in [
            "segment",
            "细分",
            "suv",
            "sedan",
            "车型级别",
            "a0",
            "a00",
            "b segment",
            "sd-",
            "suv-",
            "轿车",
            "越野",
        ]
    ):
        return "segment-analysis"
    # origin / vehicle-line camp
    if any(
        token in lowered
        for token in [
            "车系",
            "origin",
            "欧系",
            "日系",
            "韩系",
            "美系",
            "中系",
            "国别",
            "阵营",
        ]
    ):
        return "origin-analysis"
    # NEV specific
    if any(
        token in lowered
        for token in [
            "新能源",
            "nev",
            "电动",
            "续航",
            "电池",
            "range",
            "渗透率",
        ]
    ):
        return "nev-analysis"
    # competitive
    if any(
        token in lowered
        for token in ["竞品", "竞争", "对比", "vs", "比较"]
    ):
        return "competitive"
    if any(token in lowered for token in ["brand", "品牌", "厂家"]):
        return "brand-ranking"
    if any(
        token in lowered
        for token in [
            "动力",
            "powertrain",
            "bev",
            "phev",
            "hev",
            "ice",
            "mhev",
        ]
    ):
        return "powertrain-mix"
    if any(
        token in lowered
        for token in ["趋势", "trend", "同比", "销量", "year", "month"]
    ):
        return "trend-summary"
    if any(token in lowered for token in ["价格", "msrp", "售价", "均价"]):
        return "pricing-summary"
    return "general-summary"


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


def _answer_with_nvidia(
    *,
    country: str,
    question: str,
    intent: str,
    snapshot: dict[str, Any],
    history: list[dict[str, str]],
) -> str:
    context = _select_context_for_intent(snapshot, intent)
    client = NvidiaChatClient(default_model=DEFAULT_NVIDIA_CHAT_MODEL)
    messages = [
        ChatMessage(role="system", content=_SYSTEM_PROMPT),
    ]
    for turn in history[-MAX_HISTORY_TURNS:]:
        role = str(turn.get("role", "")).strip().lower()
        content = str(turn.get("content", "")).strip()
        if role not in {"user", "assistant"} or not content:
            continue
        messages.append(ChatMessage(role=role, content=content[:2000]))

    messages.append(
        ChatMessage(
            role="user",
            content=(
                f"国家: {country}\n"
                f"推断意图: {intent}\n"
                f"用户问题: {question}\n"
                "国家数据快照(JSON):\n"
                f"{json.dumps(context, ensure_ascii=False)}"
            ),
        )
    )
    response = client.chat(
        messages,
        max_tokens=1024,
        temperature=0.2,
        timeout=60,
    )
    text = (response.text or "").strip()
    if not text:
        raise RuntimeError("NVIDIA 返回了空响应")
    return text


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

    if intent == "brand-ranking":
        ctx["ytdBrandRanking"] = snapshot.get("ytdBrandRanking", [])
        ctx["monthlyBrandRanking"] = snapshot.get(
            "monthlyBrandRanking", [],
        )
        ctx["topBrands"] = snapshot.get("topBrands", [])

    elif intent == "segment-analysis":
        ctx["segmentMatrix"] = snapshot.get("segmentMatrix", {})
        ctx["suvSedanTrend"] = snapshot.get("suvSedanTrend", [])
        ctx["drilldown"] = snapshot.get("drilldown", {})
        ctx["suvA"] = snapshot.get("suvA", {})

    elif intent == "origin-analysis":
        ctx["originAnalysis"] = snapshot.get("originAnalysis", {})
        ctx["ytdBrandRanking"] = snapshot.get("ytdBrandRanking", [])

    elif intent in ("powertrain-mix", "nev-analysis"):
        ctx["drilldown"] = snapshot.get("drilldown", {})
        ctx["suvA"] = snapshot.get("suvA", {})
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

    elif intent == "competitive":
        ctx["ytdBrandRanking"] = snapshot.get("ytdBrandRanking", [])
        ctx["segmentMatrix"] = snapshot.get("segmentMatrix", {})
        ctx["originAnalysis"] = snapshot.get("originAnalysis", {})

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

    return ctx


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
                f"份额{b.get('share', 0):.1f}%)"
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
