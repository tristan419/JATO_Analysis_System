from __future__ import annotations

from datetime import datetime, timezone
import os
from time import perf_counter
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.core.config import ASTRBOT_MCP_SERVER_NAME
from app.core.config import ASTRBOT_MCP_TRANSPORT
from app.core.config import ASTRBOT_MCP_URL
from app.core.config import ASTRBOT_PROVIDER_API_BASE
from app.core.config import ASTRBOT_PROVIDER_ID
from app.core.config import ASTRBOT_PROVIDER_KEY_ENV
from app.core.config import ASTRBOT_PROVIDER_MODEL
from app.core.config import ASTRBOT_PROVIDER_SOURCE_ID
from app.core.config import ASTRBOT_RUNTIME_URL
from app.services.jato_agent_memory_service import get_memory_stats
from app.services.jato_agent_profiles_service import get_active_agent_profile
from app.services.jato_agent_skills_service import list_agent_skills
from app.services.jato_channel_adapter_service import read_channel_adapter_status
from app.services.jato_mcp_tools_service import list_jato_mcp_tools
from app.services import jato_minirag_client
from app.services import jato_pageindex_client


HTTP_PROBE_TIMEOUT_SECONDS = 1.5


def read_astrbot_runtime_status() -> dict[str, Any]:
    tools = list_jato_mcp_tools()["items"]
    profile = get_active_agent_profile()
    skills = list_agent_skills()
    runtime_probe = _probe_http_url(_normalize_url(ASTRBOT_RUNTIME_URL))
    mcp_probe = _probe_http_url(ASTRBOT_MCP_URL)
    provider_key_configured = bool(os.getenv(ASTRBOT_PROVIDER_KEY_ENV, "").strip())

    return {
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "runtime": {
            "name": "AstrBot Runtime",
            "url": _normalize_url(ASTRBOT_RUNTIME_URL),
            "status": _status_from_probe(runtime_probe),
            **runtime_probe,
        },
        "mcp": {
            "name": ASTRBOT_MCP_SERVER_NAME,
            "url": ASTRBOT_MCP_URL,
            "transport": ASTRBOT_MCP_TRANSPORT,
            "toolCount": len(tools),
            "tools": tools,
            "status": _status_from_probe(mcp_probe),
            **mcp_probe,
        },
        "provider": {
            "sourceId": ASTRBOT_PROVIDER_SOURCE_ID,
            "providerId": ASTRBOT_PROVIDER_ID,
            "model": ASTRBOT_PROVIDER_MODEL,
            "apiBase": ASTRBOT_PROVIDER_API_BASE,
            "keySource": f"${ASTRBOT_PROVIDER_KEY_ENV}",
            "keyConfigured": provider_key_configured,
            "status": "configured" if provider_key_configured else "missing_key",
        },
        "retrieval": _read_retrieval_dependency_status(),
        "channels": read_channel_adapter_status(),
        "profile": profile,
        "skills": skills,
        "memory": get_memory_stats(),
        "dataBoundary": {
            "mode": "mcp_only",
            "directDatabaseAccess": False,
            "directParquetAccess": False,
        },
    }


def _read_retrieval_dependency_status() -> dict[str, Any]:
    pageindex_configured = jato_pageindex_client.is_configured()
    minirag_library_installed = bool(getattr(jato_minirag_client, "MINIRAG_ENABLED", False))
    minirag_api_configured = bool(os.getenv("MINIRAG_API_URL", "").strip())
    minirag_configured = jato_minirag_client.is_configured()
    return {
        "pageIndex": {
            "name": "PageIndex",
            "status": "configured" if pageindex_configured else "fallback",
            "keySource": "$PAGEINDEX_API_KEY",
            "keyConfigured": pageindex_configured,
            "mcpUrl": jato_pageindex_client.PAGEINDEX_MCP_URL,
            "fallback": "web_search_documents",
        },
        "miniRag": {
            "name": "MiniRAG",
            "status": "configured" if minirag_configured else "fallback",
            "libraryInstalled": minirag_library_installed,
            "apiConfigured": minirag_api_configured,
            "apiUrl": jato_minirag_client.MINIRAG_API_URL,
            "workingDir": "./minirag_data",
            "corpusStatus": "not_verified",
            "fallback": "multi_tool_chain",
        },
    }


def _normalize_url(value: str) -> str:
    stripped = value.strip() or "http://localhost:6185/"
    return stripped if stripped.endswith("/") else f"{stripped}/"


def _probe_http_url(url: str) -> dict[str, Any]:
    started_at = perf_counter()
    request = Request(url, headers={"Accept": "text/html,application/json,text/event-stream,*/*"})
    try:
        with urlopen(request, timeout=HTTP_PROBE_TIMEOUT_SECONDS) as response:
            elapsed_ms = int((perf_counter() - started_at) * 1000)
            status_code = int(response.status)
            return {
                "reachable": True,
                "httpStatus": status_code,
                "latencyMs": elapsed_ms,
                "detail": "ok",
            }
    except HTTPError as exc:
        elapsed_ms = int((perf_counter() - started_at) * 1000)
        status_code = int(exc.code)
        return {
            "reachable": status_code < 500,
            "httpStatus": status_code,
            "latencyMs": elapsed_ms,
            "detail": exc.reason or "http_error",
        }
    except (OSError, URLError) as exc:
        elapsed_ms = int((perf_counter() - started_at) * 1000)
        return {
            "reachable": False,
            "httpStatus": None,
            "latencyMs": elapsed_ms,
            "detail": str(exc),
        }


def _status_from_probe(probe: dict[str, Any]) -> str:
    return "online" if bool(probe.get("reachable")) else "offline"
