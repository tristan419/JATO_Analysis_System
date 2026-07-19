from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from app.services import jato_mcp_tools_service


def _mcp_port() -> int:
    try:
        return int(os.getenv("JATO_MCP_PORT", "8185").strip() or "8185")
    except ValueError:
        return 8185


mcp = FastMCP(
    "JATO MCP Server",
    host=os.getenv("JATO_MCP_HOST", "127.0.0.1").strip() or "127.0.0.1",
    port=_mcp_port(),
    stateless_http=True,
    json_response=True,
)


@mcp.tool()
def route_agent_request(
    country: str,
    question: str,
    skill_id: str = "",
    mode: str = "",
    selected_year: int | None = None,
    selected_model: str = "",
    model_top_n: int | None = None,
    brand: str = "",
    model: str = "",
    models: list[str] | None = None,
    powertrain: str = "",
    limit: int = 6,
    max_items: int = 12,
    include_sections: list[str] | None = None,
    include_secondary_paths: bool = True,
) -> dict[str, Any]:
    """Route one AstrBot/JATO agent request to the best governed JATO tool."""
    return jato_mcp_tools_service.route_agent_request(
        {
            "country": country,
            "question": question,
            "skill_id": skill_id,
            "mode": mode,
            "selected_year": selected_year,
            "selected_model": selected_model,
            "model_top_n": model_top_n,
            "brand": brand,
            "model": model,
            "models": models or [],
            "powertrain": powertrain,
            "limit": limit,
            "max_items": max_items,
            "include_sections": include_sections or [],
            "include_secondary_paths": include_secondary_paths,
        }
    )


@mcp.tool()
def query_country_snapshot(
    country: str,
    question: str = "",
    include_sections: list[str] | None = None,
    intents: list[str] | None = None,
) -> dict[str, Any]:
    """Return a governed JATO country market snapshot."""
    return jato_mcp_tools_service.query_country_snapshot(
        {
            "country": country,
            "question": question,
            "include_sections": include_sections or [],
            "intents": intents or [],
        }
    )


@mcp.tool()
def query_msrp_pricing(
    country: str,
    brand: str = "",
    model: str = "",
    models: list[str] | None = None,
    powertrain: str = "",
    max_items: int = 12,
) -> dict[str, Any]:
    """Return current MSRP pricing for one or more market models."""
    return jato_mcp_tools_service.query_msrp_pricing(
        {
            "country": country,
            "brand": brand,
            "model": model,
            "models": models or [],
            "powertrain": powertrain,
            "max_items": max_items,
        }
    )


@mcp.tool()
def query_leasing_offers(
    country: str,
    question: str = "",
    brand: str = "",
    model: str = "",
    models: list[str] | None = None,
    competitors: list[str] | None = None,
    lease_type: str = "",
    status: str = "",
    term_months: int | None = None,
    max_items: int = 16,
) -> dict[str, Any]:
    """Return governed lease offers with monthly payment, term, mileage, RV, and total cost."""
    return jato_mcp_tools_service.query_leasing_offers(
        {
            "country": country,
            "question": question,
            "brand": brand,
            "model": model,
            "models": models or [],
            "competitors": competitors or [],
            "lease_type": lease_type,
            "status": status,
            "term_months": term_months,
            "max_items": max_items,
        }
    )


@mcp.tool()
def search_market_news(
    country: str,
    question: str,
    limit: int = 6,
) -> dict[str, Any]:
    """Search market news through the existing JATO provider fallback."""
    return jato_mcp_tools_service.search_market_news(
        {
            "country": country,
            "question": question,
            "limit": limit,
        }
    )


@mcp.tool()
def read_web_page(
    url: str,
    question: str = "",
    max_chars: int = 6000,
) -> dict[str, Any]:
    """Read a public web page as static text with no browser actions or cookies."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "read_web_page",
        {
            "url": url,
            "question": question,
            "max_chars": max_chars,
        },
    )


@mcp.tool()
def browser_snapshot(
    url: str,
    question: str = "",
    max_chars: int = 6000,
    capture_screenshot: bool = False,
    timeout_ms: int = 12000,
) -> dict[str, Any]:
    """Capture a read-only browser snapshot or static fallback for a public URL."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "browser_snapshot",
        {
            "url": url,
            "question": question,
            "max_chars": max_chars,
            "capture_screenshot": capture_screenshot,
            "timeout_ms": timeout_ms,
        },
    )


@mcp.tool()
def browser_interaction_plan(
    url: str,
    action_goal: str = "",
    max_actions: int = 6,
    timeout_ms: int = 12000,
) -> dict[str, Any]:
    """Propose governed click/type actions and confirmation tokens for a public page."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "browser_interaction_plan",
        {
            "url": url,
            "action_goal": action_goal,
            "max_actions": max_actions,
            "timeout_ms": timeout_ms,
        },
    )


@mcp.tool()
def browser_click_confirmed(
    url: str,
    action_id: str,
    confirmation_token: str,
    max_chars: int = 6000,
    timeout_ms: int = 12000,
) -> dict[str, Any]:
    """Execute one approved browser click with a token minted by browser_interaction_plan."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "browser_click_confirmed",
        {
            "url": url,
            "action_id": action_id,
            "confirmation_token": confirmation_token,
            "max_chars": max_chars,
            "timeout_ms": timeout_ms,
        },
    )


@mcp.tool()
def browser_type_confirmed(
    url: str,
    action_id: str,
    confirmation_token: str,
    text: str,
    max_chars: int = 6000,
    timeout_ms: int = 12000,
) -> dict[str, Any]:
    """Fill one approved browser field with a token minted by browser_interaction_plan."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "browser_type_confirmed",
        {
            "url": url,
            "action_id": action_id,
            "confirmation_token": confirmation_token,
            "text": text,
            "max_chars": max_chars,
            "timeout_ms": timeout_ms,
        },
    )


@mcp.tool()
def compare_vehicle_variants(
    country: str,
    brand: str = "",
    model: str = "",
    models: list[str] | None = None,
    powertrain: str = "",
    compare_subjects: list[dict[str, Any]] | None = None,
    max_subjects: int = 3,
    max_diff_features: int = 16,
    max_common_features: int = 8,
) -> dict[str, Any]:
    """Compare selected market vehicle variants and feature differences."""
    return jato_mcp_tools_service.compare_vehicle_variants(
        {
            "country": country,
            "brand": brand,
            "model": model,
            "models": models or [],
            "powertrain": powertrain,
            "compare_subjects": compare_subjects or [],
            "max_subjects": max_subjects,
            "max_diff_features": max_diff_features,
            "max_common_features": max_common_features,
        }
    )


@mcp.tool()
def build_market_chart(
    country: str,
    question: str = "",
    intents: list[str] | None = None,
    selected_year: int | None = None,
    selected_model: str = "",
    model_top_n: int | None = None,
    include_sections: list[str] | None = None,
    extracted_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build chart-ready JATO market context for AstrBot answers."""
    return jato_mcp_tools_service.build_market_chart(
        {
            "country": country,
            "question": question,
            "intents": intents or [],
            "selected_year": selected_year,
            "selected_model": selected_model,
            "model_top_n": model_top_n,
            "include_sections": include_sections or [],
            "extracted_params": extracted_params or {},
        }
    )


@mcp.tool()
def pageindex_search_documents(
    country: str,
    question: str,
    limit: int = 6,
) -> dict[str, Any]:
    """Search long documents through PageIndex, falling back to governed web search."""
    return jato_mcp_tools_service.pageindex_search_documents(
        {
            "country": country,
            "question": question,
            "limit": limit,
        }
    )


@mcp.tool()
def pageindex_get_section(
    country: str,
    question: str,
    section_id: str = "",
) -> dict[str, Any]:
    """Read a PageIndex section when connected, falling back to related web citations."""
    return jato_mcp_tools_service.pageindex_get_section(
        {
            "country": country,
            "question": question,
            "section_id": section_id,
        }
    )


@mcp.tool()
def pageindex_list_documents(
    country: str,
) -> dict[str, Any]:
    """List PageIndex documents when connected, otherwise return setup guidance."""
    return jato_mcp_tools_service.pageindex_list_documents({"country": country})


@mcp.tool()
def minirag_query_graph(
    country: str,
    question: str,
) -> dict[str, Any]:
    """Query MiniRAG multi-hop graph retrieval, falling back to a governed multi-tool chain."""
    return jato_mcp_tools_service.minirag_query_graph(
        {
            "country": country,
            "question": question,
        }
    )


@mcp.tool()
def minirag_explain_entity(
    country: str,
    question: str,
    entity: str = "",
) -> dict[str, Any]:
    """Explain a MiniRAG entity relationship, falling back to market evidence search."""
    return jato_mcp_tools_service.minirag_explain_entity(
        {
            "country": country,
            "question": question,
            "entity": entity,
        }
    )


@mcp.tool()
def minirag_update_corpus(
    country: str,
) -> dict[str, Any]:
    """Prepare MiniRAG corpus update metadata for a country."""
    return jato_mcp_tools_service.minirag_update_corpus({"country": country})


@mcp.tool()
def analyze_model_performance(
    country: str,
    question: str = "",
    model: str = "",
) -> dict[str, Any]:
    """Deep-dive one model with sales, MSRP, variants, and news evidence."""
    return jato_mcp_tools_service.analyze_model_performance(
        {
            "country": country,
            "question": question,
            "model": model,
        }
    )


@mcp.tool()
def compare_competitive_set(
    country: str,
    question: str,
    model: str = "",
) -> dict[str, Any]:
    """Compare a model against segment competitors across sales, price, and features."""
    return jato_mcp_tools_service.compare_competitive_set(
        {
            "country": country,
            "question": question,
            "model": model,
        }
    )


@mcp.tool()
def analyze_market_dynamics(
    country: str,
    question: str,
) -> dict[str, Any]:
    """Cross-reference trends, news, and pricing to identify market changes."""
    return jato_mcp_tools_service.analyze_market_dynamics(
        {
            "country": country,
            "question": question,
        }
    )


@mcp.tool()
def query_with_filters(
    country: str,
    powertrain: str = "",
    fuel_type: str = "",
    segment: str = "",
    brand: str = "",
    model: str = "",
    year: int | None = None,
    metric: str = "sales",
    top_n: int = 10,
) -> dict[str, Any]:
    """Query market data with filter dimensions matching the JATO UI sidebar."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "query_with_filters",
        {
            "country": country,
            "powertrain": powertrain,
            "fuel_type": fuel_type,
            "segment": segment,
            "brand": brand,
            "model": model,
            "year": year,
            "metric": metric,
            "top_n": top_n,
        },
    )


@mcp.tool()
def query_time_series(
    country: str,
    metric: str = "sales",
    powertrain: str = "",
    fuel_type: str = "",
    segment: str = "",
    year: int | None = None,
    granularity: str = "monthly",
) -> dict[str, Any]:
    """Return monthly or yearly time-series market data."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "query_time_series",
        {
            "country": country,
            "metric": metric,
            "powertrain": powertrain,
            "fuel_type": fuel_type,
            "segment": segment,
            "year": year,
            "granularity": granularity,
        },
    )


@mcp.tool()
def query_segment_breakdown(
    country: str,
    segment: str = "",
    powertrain: str = "",
    year: int | None = None,
) -> dict[str, Any]:
    """Return segment by fuel or powertrain breakdown data."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "query_segment_breakdown",
        {
            "country": country,
            "segment": segment,
            "powertrain": powertrain,
            "year": year,
        },
    )


@mcp.tool()
def query_price_positioning(
    country: str,
    model: str = "",
    brand: str = "",
    powertrain: str = "",
    top_n: int = 10,
) -> dict[str, Any]:
    """Return MSRP distribution and competitive price positioning context."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "query_price_positioning",
        {
            "country": country,
            "model": model,
            "brand": brand,
            "powertrain": powertrain,
            "top_n": top_n,
        },
    )


@mcp.tool()
def query_competitive_landscape(
    country: str,
    model: str,
    include_pricing: bool = True,
    include_features: bool = True,
    competitor_count: int = 5,
) -> dict[str, Any]:
    """Find competitors for a model and compare sales, pricing, and features."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "query_competitive_landscape",
        {
            "country": country,
            "model": model,
            "include_pricing": include_pricing,
            "include_features": include_features,
            "competitor_count": competitor_count,
        },
    )


@mcp.tool()
def query_powertrain_trend(
    country: str,
    powertrain: str = "BEV",
) -> dict[str, Any]:
    """Return market trend data for a specific powertrain."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "query_powertrain_trend",
        {
            "country": country,
            "powertrain": powertrain,
        },
    )


@mcp.tool()
def query_brand_deep_dive(
    country: str,
    brand: str,
) -> dict[str, Any]:
    """Return sales, model, pricing, and market context for one brand."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "query_brand_deep_dive",
        {
            "country": country,
            "brand": brand,
        },
    )


@mcp.tool()
def query_cross_country(
    countries: str,
    question: str,
) -> dict[str, Any]:
    """Compare the same market question across multiple countries."""
    return jato_mcp_tools_service.call_jato_mcp_tool(
        "query_cross_country",
        {
            "countries": countries,
            "question": question,
        },
    )


def main() -> None:
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
