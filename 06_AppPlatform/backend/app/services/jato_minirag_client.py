from __future__ import annotations

import os
from typing import Any

MINIRAG_ENABLED = False
try:
    import minirag  # noqa: F401

    MINIRAG_ENABLED = True
except ImportError:
    pass

MINIRAG_API_URL = os.getenv("MINIRAG_API_URL", "http://localhost:9621").strip()


def is_configured() -> bool:
    return MINIRAG_ENABLED or bool(os.getenv("MINIRAG_API_URL", "").strip())


def query_graph(question: str, top_k: int = 5) -> dict[str, Any]:
    """Query MiniRAG heterogeneous graph for multi-hop entity relationships."""
    if not MINIRAG_ENABLED:
        return {
            "status": "unconfigured",
            "paths": [],
            "entities": [],
            "supportingChunks": [],
            "summary": "MiniRAG library not installed. pip install minirag-hku",
        }

    try:
        # Use MiniRAG as a local library
        from minirag import MiniRAG  # type: ignore[import-untyped]

        rag = MiniRAG(working_dir="./minirag_data")
        result = rag.query(question, top_k=top_k)

        paths = []
        entities = []
        chunks = []

        if isinstance(result, dict):
            for item in result.get("paths", []):
                paths.append({
                    "source": item.get("source", ""),
                    "target": item.get("target", ""),
                    "relation": item.get("relation", ""),
                    "weight": item.get("weight", 0),
                })
            for item in result.get("entities", []):
                entities.append({
                    "name": item.get("name", ""),
                    "type": item.get("type", ""),
                    "properties": item.get("properties", {}),
                })
            for item in result.get("chunks", []):
                chunks.append({
                    "text": str(item.get("content") or item.get("text", ""))[:500],
                    "source": item.get("source", ""),
                    "score": item.get("score", 0),
                })

        return {
            "status": "ok",
            "paths": paths,
            "entities": entities,
            "supportingChunks": chunks,
            "summary": f"Graph query returned {len(paths)} paths, {len(entities)} entities, {len(chunks)} chunks.",
        }
    except Exception as exc:
        return {
            "status": "error",
            "paths": [],
            "entities": [],
            "supportingChunks": [],
            "summary": f"MiniRAG query failed: {exc}",
            "error": str(exc),
        }


def explain_entity(entity_name: str) -> dict[str, Any]:
    """Explain entity relationships within the MiniRAG graph."""
    if not MINIRAG_ENABLED:
        return {
            "status": "unconfigured",
            "relatedEntities": [],
            "evidence": [],
            "summary": "MiniRAG library not installed.",
        }

    try:
        from minirag import MiniRAG  # type: ignore[import-untyped]

        rag = MiniRAG(working_dir="./minirag_data")
        result = rag.query(f"Explain relationships for: {entity_name}", top_k=8)

        related = []
        if isinstance(result, dict):
            for item in result.get("entities", []):
                related.append({
                    "entity": item.get("name", ""),
                    "relationship": item.get("relation", "related_to"),
                    "source": item.get("source", "minirag"),
                    "url": item.get("url", ""),
                })

        return {
            "status": "ok",
            "relatedEntities": related,
            "evidence": result.get("chunks", []) if isinstance(result, dict) else [],
            "summary": f"Found {len(related)} related entities for '{entity_name}'.",
        }
    except Exception as exc:
        return {
            "status": "error",
            "relatedEntities": [],
            "evidence": [],
            "summary": f"MiniRAG entity explain failed: {exc}",
        }
