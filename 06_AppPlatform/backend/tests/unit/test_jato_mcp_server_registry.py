from __future__ import annotations

from app.mcp import jato_server
from app.services.jato_mcp_tools_service import JATO_MCP_TOOL_DESCRIPTORS


def test_fastmcp_server_exposes_all_descriptor_tools() -> None:
    descriptor_names = [str(tool["name"]) for tool in JATO_MCP_TOOL_DESCRIPTORS]

    missing = [
        name
        for name in descriptor_names
        if not callable(getattr(jato_server, name, None))
    ]

    assert missing == []


def test_unavailable_external_research_is_not_publicly_registered() -> None:
    descriptor_names = [str(tool["name"]) for tool in JATO_MCP_TOOL_DESCRIPTORS]

    assert "external_research" not in descriptor_names
    assert not hasattr(jato_server, "external_research")


def test_query_time_series_mcp_wrapper_delegates_to_tool_service(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_call_jato_mcp_tool(name: str, arguments: dict[str, object]):
        captured["name"] = name
        captured["arguments"] = arguments
        return {"tool": name, "data": {"ok": True}, "metadata": {}}

    monkeypatch.setattr(
        jato_server.jato_mcp_tools_service,
        "call_jato_mcp_tool",
        fake_call_jato_mcp_tool,
    )

    result = jato_server.query_time_series(
        country="Sweden",
        metric="sales",
        powertrain="BEV",
        year=2025,
        granularity="monthly",
    )

    assert result["tool"] == "query_time_series"
    assert captured["name"] == "query_time_series"
    assert captured["arguments"] == {
        "country": "Sweden",
        "metric": "sales",
        "powertrain": "BEV",
        "fuel_type": "",
        "segment": "",
        "year": 2025,
        "granularity": "monthly",
    }
