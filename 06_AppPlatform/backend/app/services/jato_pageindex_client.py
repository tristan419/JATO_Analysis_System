from __future__ import annotations

import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PAGEINDEX_MCP_URL = os.getenv("PAGEINDEX_MCP_URL", "https://api.pageindex.ai/mcp").strip()
PAGEINDEX_API_KEY = os.getenv("PAGEINDEX_API_KEY", "").strip()
HTTP_TIMEOUT = 10.0


def is_configured() -> bool:
    return bool(_api_key())


def _api_key() -> str:
    return os.getenv("PAGEINDEX_API_KEY", PAGEINDEX_API_KEY).strip()


def search_documents(query: str, top_k: int = 5) -> dict[str, Any]:
    """Search PageIndex document tree for relevant sections."""
    if not is_configured():
        return {
            "status": "unconfigured",
            "sections": [],
            "summary": "PageIndex API key not configured. Set PAGEINDEX_API_KEY env var.",
        }

    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "pageindex_search_documents",
            "arguments": {"query": query, "top_k": top_k},
        },
        "id": 1,
    }

    try:
        result = _call_mcp(payload)
        return {
            "status": "ok",
            "sections": result.get("sections", []),
            "summary": f"Found {len(result.get('sections', []))} relevant sections.",
            "raw": result,
        }
    except Exception as exc:
        return {
            "status": "error",
            "sections": [],
            "summary": f"PageIndex search failed: {exc}",
            "error": str(exc),
        }


def get_section(section_id: str) -> dict[str, Any]:
    """Retrieve a specific section by ID."""
    if not is_configured():
        return {
            "status": "unconfigured",
            "text": "",
            "citations": [],
            "summary": "PageIndex API key not configured.",
        }

    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "pageindex_get_section",
            "arguments": {"section_id": section_id},
        },
        "id": 1,
    }

    try:
        result = _call_mcp(payload)
        return {
            "status": "ok",
            "text": result.get("text", ""),
            "citations": result.get("citations", []),
            "summary": "Section retrieved.",
        }
    except Exception as exc:
        return {
            "status": "error",
            "text": "",
            "citations": [],
            "summary": f"PageIndex section fetch failed: {exc}",
        }


def list_documents() -> dict[str, Any]:
    """List all indexed documents."""
    if not is_configured():
        return {
            "status": "unconfigured",
            "documents": [],
            "summary": "PageIndex API key not configured.",
        }

    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "pageindex_list_documents",
            "arguments": {},
        },
        "id": 1,
    }

    try:
        result = _call_mcp(payload)
        return {
            "status": "ok",
            "documents": result.get("documents", []),
            "summary": f"Found {len(result.get('documents', []))} indexed documents.",
        }
    except Exception as exc:
        return {
            "status": "error",
            "documents": [],
            "summary": f"PageIndex list failed: {exc}",
        }


def _call_mcp(payload: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        PAGEINDEX_MCP_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {_api_key()}",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body).get("result", {})
    except HTTPError as exc:
        body = exc.read().decode("utf-8") if exc.fp else ""
        raise RuntimeError(f"PageIndex HTTP {exc.code}: {body[:200]}") from exc
    except URLError as exc:
        raise RuntimeError(f"PageIndex unreachable: {exc.reason}") from exc
