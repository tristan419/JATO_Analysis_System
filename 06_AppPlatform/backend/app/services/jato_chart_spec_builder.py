from __future__ import annotations

from typing import Any

# ── Color palette matching frontend CountryChatAnalysisDeck ──

PT_COLORS = [
    "#2563eb",  # blue
    "#0f766e",  # teal
    "#d97706",  # amber
    "#dc2626",  # red
    "#7c3aed",  # violet
    "#db2777",  # pink
    "#0891b2",  # cyan
    "#65a30d",  # lime
    "#ea580c",  # orange
    "#4f46e5",  # indigo
]

TRANSPARENT_LAYOUT: dict[str, Any] = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "margin": {"l": 50, "r": 20, "t": 40, "b": 50},
    "font": {"family": "Inter, system-ui, sans-serif", "size": 12, "color": "#374151"},
    "legend": {"orientation": "h", "y": -0.2, "font": {"size": 11}},
    "xaxis": {
        "gridcolor": "#e5e7eb",
        "zerolinecolor": "#e5e7eb",
        "tickfont": {"size": 11},
    },
    "yaxis": {
        "gridcolor": "#e5e7eb",
        "zerolinecolor": "#e5e7eb",
        "tickfont": {"size": 11},
    },
    "hovermode": "x unified",
}


def build_chart_spec_from_deck(deck: dict[str, Any]) -> dict[str, Any]:
    """Convert a chart deck response into one or more Plotly chart specs."""
    snapshot = deck.get("contextSnapshot") if isinstance(deck.get("contextSnapshot"), dict) else {}
    primary_intent = str(deck.get("primaryIntent") or deck.get("intentRoute") or "market_trend")
    charts: list[dict[str, Any]] = []

    # ── 1. Trend line chart (year/month series) ──
    year_series = _list_value(snapshot.get("yearSeries"))
    month_series = _list_value(snapshot.get("monthSeries"))
    if year_series or month_series:
        trend_chart = _build_trend_chart(year_series, month_series, primary_intent)
        if trend_chart:
            charts.append({
                "chartId": "trend_series",
                "chartType": "line",
                "title": _chart_title_for_intent(primary_intent),
                **trend_chart,
            })

    # ── 2. Bar chart (top brands / models) ──
    top_brands = _list_value(snapshot.get("topBrands"))
    top_models = _list_value(snapshot.get("topModels"))
    ranking_data = top_models if top_models else top_brands
    if ranking_data:
        bar_chart = _build_ranking_bar(ranking_data, "topModels" if top_models else "topBrands")
        if bar_chart:
            charts.append({
                "chartId": "top_ranking",
                "chartType": "bar",
                "title": "Top Models" if top_models else "Top Brands",
                **bar_chart,
            })

    # ── 3. Pie/donut chart (powertrain mix) ──
    powertrain_mix = _dict_value(snapshot.get("powertrainMix"))
    if powertrain_mix:
        pie_chart = _build_powertrain_pie(powertrain_mix)
        if pie_chart:
            charts.append({
                "chartId": "powertrain_mix",
                "chartType": "pie",
                "title": "Powertrain Mix",
                **pie_chart,
            })

    # ── 4. Scatter chart (positioning map) ──
    positioning = _list_value(snapshot.get("positioningMap"))
    if positioning:
        scatter_chart = _build_positioning_scatter(positioning)
        if scatter_chart:
            charts.append({
                "chartId": "positioning_map",
                "chartType": "scatter",
                "title": "Market Positioning Map",
                **scatter_chart,
            })

    # ── 5. Bubble chart (model version bubble / price distribution) ──
    model_bubble = _list_value(snapshot.get("modelVersionBubble"))
    price_dist = _list_value(snapshot.get("priceDistribution"))
    bubble_data = model_bubble if model_bubble else price_dist
    if bubble_data:
        bubble_chart = _build_bubble(bubble_data, "modelVersionBubble" if model_bubble else "priceDistribution")
        if bubble_chart:
            charts.append({
                "chartId": "version_bubble",
                "chartType": "bubble",
                "title": "Model Version Overview" if model_bubble else "Price Distribution",
                **bubble_chart,
            })

    return {
        "charts": charts,
        "chartCount": len(charts),
        "primaryChart": charts[0]["chartId"] if charts else None,
        "source": "jato_chart_spec_builder",
        "renderer": "plotly",
    }


# ── chart builders ──


def _build_trend_chart(
    year_series: list[dict[str, Any]],
    month_series: list[dict[str, Any]],
    primary_intent: str,
) -> dict[str, Any] | None:
    """Build a multi-line trend chart from year/month series data."""
    traces: list[dict[str, Any]] = []
    color_idx = 0

    # Prefer month_series for higher resolution
    source_data = month_series if month_series else year_series
    x_key = "month" if month_series else "year"

    if not source_data:
        return None

    # Collect all numeric keys that look like metrics
    sample = source_data[0] if source_data else {}
    metric_keys = [
        k for k in sample
        if k not in (x_key, "label", "period", "quarter")
        and isinstance(sample[k], (int, float))
    ]

    if not metric_keys:
        return None

    for key in metric_keys[:6]:  # max 6 traces
        x_values = []
        y_values = []
        for row in source_data:
            x_val = row.get(x_key)
            if x_val is not None:
                x_values.append(str(x_val))
                y_values.append(_to_number(row.get(key)))
        if any(y is not None for y in y_values):
            traces.append({
                "x": x_values,
                "y": y_values,
                "type": "scatter",
                "mode": "lines+markers",
                "name": _label_for_key(key),
                "line": {"color": PT_COLORS[color_idx % len(PT_COLORS)], "width": 2},
                "marker": {"size": 5},
            })
            color_idx += 1

    if not traces:
        return None

    title = _chart_title_for_intent(primary_intent)
    layout = {
        **TRANSPARENT_LAYOUT,
        "title": title,
        "xaxis": {**TRANSPARENT_LAYOUT["xaxis"], "title": x_key.capitalize()},
        "yaxis": {**TRANSPARENT_LAYOUT["yaxis"], "title": "Value"},
        "height": 400,
    }
    return {"data": traces, "layout": layout}


def _build_ranking_bar(
    ranking_data: list[dict[str, Any]],
    source: str,
) -> dict[str, Any] | None:
    """Build a horizontal bar chart for rankings."""
    labels: list[str] = []
    values: list[float] = []
    for item in ranking_data[:12]:
        label = str(item.get("label") or item.get("brand") or item.get("model") or "")
        value = _to_number(item.get("value") or item.get("sales") or item.get("volume"))
        if label and value is not None:
            labels.append(label)
            values.append(value)

    if not labels:
        return None

    traces = [{
        "x": values,
        "y": labels,
        "type": "bar",
        "orientation": "h",
        "marker": {"color": PT_COLORS[0]},
        "text": [f"{v:,.0f}" for v in values],
        "textposition": "outside",
    }]
    layout = {
        **TRANSPARENT_LAYOUT,
        "title": "Top Models" if source == "topModels" else "Top Brands",
        "xaxis": {**TRANSPARENT_LAYOUT["xaxis"], "title": "Sales"},
        "yaxis": {**TRANSPARENT_LAYOUT["yaxis"], "autorange": "reversed"},
        "height": max(300, len(labels) * 28),
        "margin": {**TRANSPARENT_LAYOUT["margin"], "l": 140},
    }
    return {"data": traces, "layout": layout}


def _build_powertrain_pie(powertrain_mix: dict[str, Any]) -> dict[str, Any] | None:
    """Build a donut chart for powertrain distribution."""
    labels: list[str] = []
    values: list[float] = []
    for key, val in powertrain_mix.items():
        num = _to_number(val)
        if num is not None and num > 0:
            labels.append(_label_for_key(key))
            values.append(num)

    if not labels:
        return None

    traces = [{
        "labels": labels,
        "values": values,
        "type": "pie",
        "hole": 0.45,
        "marker": {"colors": PT_COLORS[:len(labels)]},
        "textinfo": "label+percent",
        "textfont": {"size": 11},
    }]
    layout = {
        **TRANSPARENT_LAYOUT,
        "title": "Powertrain Mix",
        "height": 380,
        "showlegend": True,
    }
    return {"data": traces, "layout": layout}


def _build_positioning_scatter(
    positioning: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Build a scatter plot for market positioning (price vs volume)."""
    x_vals: list[float] = []
    y_vals: list[float] = []
    texts: list[str] = []
    for item in positioning[:30]:
        x = _to_number(item.get("price") or item.get("avgPrice") or item.get("x"))
        y = _to_number(item.get("volume") or item.get("sales") or item.get("y"))
        label = str(item.get("label") or item.get("model") or "")
        if x is not None and y is not None:
            x_vals.append(x)
            y_vals.append(y)
            texts.append(label)

    if not x_vals:
        return None

    traces = [{
        "x": x_vals,
        "y": y_vals,
        "text": texts,
        "type": "scatter",
        "mode": "markers",
        "marker": {
            "size": 10,
            "color": PT_COLORS[0],
            "opacity": 0.7,
        },
    }]
    layout = {
        **TRANSPARENT_LAYOUT,
        "title": "Positioning Map",
        "xaxis": {**TRANSPARENT_LAYOUT["xaxis"], "title": "Price / Avg Price"},
        "yaxis": {**TRANSPARENT_LAYOUT["yaxis"], "title": "Volume / Sales"},
        "height": 420,
    }
    return {"data": traces, "layout": layout}


def _build_bubble(
    bubble_data: list[dict[str, Any]],
    source: str,
) -> dict[str, Any] | None:
    """Build a bubble chart (price x volume with optional size)."""
    x_vals: list[float] = []
    y_vals: list[float] = []
    size_vals: list[float] = []
    texts: list[str] = []
    for item in bubble_data[:25]:
        x = _to_number(item.get("price") or item.get("avgPrice") or item.get("x"))
        y = _to_number(item.get("volume") or item.get("sales") or item.get("y"))
        size = _to_number(item.get("share") or item.get("marketShare") or item.get("size")) or 8
        label = str(item.get("label") or item.get("model") or item.get("version") or "")
        if x is not None and y is not None:
            x_vals.append(x)
            y_vals.append(y)
            size_vals.append(max(4, min(size * 2, 40)))
            texts.append(label)

    if not x_vals:
        return None

    traces = [{
        "x": x_vals,
        "y": y_vals,
        "text": texts,
        "type": "scatter",
        "mode": "markers",
        "marker": {
            "size": size_vals,
            "color": PT_COLORS[1],
            "opacity": 0.6,
        },
    }]
    layout = {
        **TRANSPARENT_LAYOUT,
        "title": "Model Version Overview" if source == "modelVersionBubble" else "Price Distribution",
        "xaxis": {**TRANSPARENT_LAYOUT["xaxis"], "title": "Price"},
        "yaxis": {**TRANSPARENT_LAYOUT["yaxis"], "title": "Volume"},
        "height": 420,
    }
    return {"data": traces, "layout": layout}


# ── helpers ──


def _list_value(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _to_number(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "").replace("%", "").strip())
        except (ValueError, TypeError):
            return None
    return None


def _label_for_key(key: str) -> str:
    mapping = {
        "bevShare": "BEV Share",
        "bev_share": "BEV Share",
        "phevShare": "PHEV Share",
        "phev_share": "PHEV Share",
        "hevShare": "HEV Share",
        "icevShare": "ICE Share",
        "sales": "Sales",
        "totalSales": "Total Sales",
        "volume": "Volume",
        "growthRate": "Growth Rate",
        "bev": "BEV",
        "phev": "PHEV",
        "hev": "HEV",
        "icev": "ICE",
        "mhev": "MHEV",
        "diesel": "Diesel",
        "petrol": "Petrol",
        "electric": "Electric",
        "hybrid": "Hybrid",
    }
    return mapping.get(key, key.replace("_", " ").title())


def _chart_title_for_intent(intent: str) -> str:
    mapping = {
        "market_trend": "Market Trend Over Time",
        "bev_share": "BEV Market Share Trend",
        "powertrain_share": "Powertrain Share Evolution",
        "fuel_mix": "Fuel Type Distribution",
        "market-scan": "Market Overview",
        "general-summary": "Market Summary",
        "ranking": "Top Entities Ranking",
    }
    return mapping.get(intent, "Market Data Overview")
