from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from app.db.session import get_session_factory
from app.services import product_evidence_service


def _port() -> int:
    try:
        return int(os.getenv("PRODUCT_CONFIG_MCP_PORT", "8285").strip() or "8285")
    except ValueError:
        return 8285


mcp = FastMCP(
    "Product Configuration MCP",
    host=os.getenv("PRODUCT_CONFIG_MCP_HOST", "127.0.0.1").strip() or "127.0.0.1",
    port=_port(),
    stateless_http=True,
    json_response=True,
)


@mcp.tool()
def search_product_evidence(
    country: str,
    query: str,
    subjects: list[str] | None = None,
    source_roles: list[str] | None = None,
    document_types: list[str] | None = None,
    features: list[str] | None = None,
    effective_date: str = "",
    limit: int = 12,
) -> dict[str, Any]:
    """Search exact-scope immutable Published configuration evidence."""
    with get_session_factory()() as session:
        return product_evidence_service.search_product_evidence(
            session,
            country=country,
            query=query,
            subjects=subjects or [],
            source_roles=source_roles or [],
            document_types=document_types or [],
            features=features or [],
            effective_date=effective_date,
            limit=limit,
        )


@mcp.tool()
def compare_published_product_configs(
    country: str,
    subject: str,
    competitors: list[str] | None = None,
    features: list[str] | None = None,
    powertrain: str = "",
    effective_date: str = "",
) -> dict[str, Any]:
    """Return a deterministic matrix from immutable Published snapshots."""
    with get_session_factory()() as session:
        return product_evidence_service.compare_published_product_configs(
            session,
            country=country,
            subject=subject,
            competitors=competitors or [],
            features=features or [],
            powertrain=powertrain,
            effective_date=effective_date,
        )


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
