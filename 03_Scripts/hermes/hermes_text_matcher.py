"""Deterministic keyword extraction and registry matching for Hermes Intake.

No LLM. Rule-based only.
"""

from __future__ import annotations

import re
from typing import Any


# ── PRD text extraction ──────────────────────────────────────────────

def extract_prd_info(prd_text: str, prd_path: str = "") -> dict[str, Any]:
    """Extract structured info from a PRD markdown file."""
    lines = prd_text.split("\n")

    # Title from first # heading
    title = ""
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("# ") and not stripped.startswith("## "):
            title = stripped[2:].strip()
            break

    # All headings
    headings: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#"):
            headings.append(stripped)

    # Extract URLs/routes: look for /path patterns
    routes = list(set(re.findall(r'(/[a-zA-Z][a-zA-Z0-9_\-/]*)', prd_text)))

    # Extract API paths: /v1/something
    api_paths = list(set(re.findall(r'/v[0-9]/[a-zA-Z][a-zA-Z0-9_\-/]*', prd_text)))

    # Extract file paths
    file_paths = list(set(re.findall(
        r'[0-9]{2}_[A-Z][a-zA-Z0-9_]*/[A-Za-z0-9_\-/]*\.(?:py|tsx?|yaml|yml|sh|json|md)',
        prd_text,
    )))

    # Keywords
    text_lower = prd_text.lower()

    kw_categories = {
        "frontend": ["frontend", "react", "typescript", "vite", "component", "page", "ui", "route", "chart", "plotly"],
        "backend": ["backend", "fastapi", "api", "endpoint", "route", "service", "uvicorn"],
        "database": ["postgresql", "database", "schema", "migration", "alembic", "sqlalchemy"],
        "pipeline": ["crawler", "scrape", "airflow", "dag", "systemd", "timer", "github action", "schedule", "artifact", "etl", "refresh", "fetch"],
        "llm": ["llm", "prompt", "deepseek", "flash", "pro", "gemini", "nvidia"],
        "data": ["jato", "msrp", "voc", "news", "parquet", "partitioned", "excel", "xlsx"],
        "feature": ["country assistant", "country copilot", "copilot", "dashboard", "market scan", "version comparison", "data management", "engineering", "review", "customer insight"],
        "cost": ["pro", "deep report", "long report", "full text", "batch analysis", "multi-country"],
        "test": ["test", "contract", "integration", "unit test", "snapshot"],
    }

    detected_keywords: dict[str, list[str]] = {}
    for cat, kws in kw_categories.items():
        matched = [kw for kw in kws if kw in text_lower]
        if matched:
            detected_keywords[cat] = matched

    return {
        "prd_path": prd_path,
        "title": title,
        "headings": headings,
        "routes": routes,
        "api_paths": api_paths,
        "file_paths": file_paths,
        "detected_keywords": detected_keywords,
    }


# ── Registry matching ────────────────────────────────────────────────

def _tokenize(text: str) -> set[str]:
    """Lowercase tokenize a string into words."""
    if not text:
        return set()
    return set(re.findall(r'[a-z0-9_一-鿿]+', str(text).lower()))


def _score_text_match(query_tokens: set[str], target_text: str, weight: float) -> float:
    """Score how many query tokens appear in target text."""
    if not query_tokens or not target_text:
        return 0.0
    target_tokens = _tokenize(target_text)
    overlap = query_tokens & target_tokens
    if not overlap:
        return 0.0
    return len(overlap) / max(len(query_tokens), 1) * weight


def _field_match(query_tokens: set[str], entry: dict, field: str, weight: float) -> float:
    """Match against a single string field."""
    value = entry.get(field, "")
    if not value:
        return 0.0
    if isinstance(value, str):
        return _score_text_match(query_tokens, value, weight)
    return 0.0


def _list_field_match(query_tokens: set[str], entry: dict, field: str, weight: float) -> float:
    """Match against a list-of-strings field."""
    values = entry.get(field, [])
    if not values or not isinstance(values, list):
        return 0.0
    score = 0.0
    for v in values:
        if isinstance(v, str):
            score += _score_text_match(query_tokens, v, weight * 0.5)
    return min(score, weight)


def _dict_field_match(query_tokens: set[str], entry: dict, field: str, weight: float) -> float:
    """Match against a list-of-dicts field (e.g. knownIssues)."""
    items = entry.get(field, [])
    if not items or not isinstance(items, list):
        return 0.0
    score = 0.0
    for item in items:
        if isinstance(item, str):
            score += _score_text_match(query_tokens, item, weight * 0.3)
        elif isinstance(item, dict):
            for v in item.values():
                if isinstance(v, str):
                    score += _score_text_match(query_tokens, v, weight * 0.2)
    return min(score, weight)


def match_features(prd_info: dict, features: list[dict]) -> list[dict]:
    """Score and rank feature registry entries against PRD."""
    text = _build_search_text(prd_info)
    tokens = _tokenize(text)
    results = []

    for f in features:
        score = 0.0
        reasons: list[str] = []

        # Exact ID match (+5)
        fid = f.get("featureId", "")
        if fid and fid in text:
            score += 5
            reasons.append(f"featureId match: {fid}")

        # Route match (+4)
        s = _list_field_match(tokens, f, "routes", 4)
        if s > 0:
            score += s
            reasons.append(f"route match: {f.get('routes')}")

        # API match (+4)
        s = _list_field_match(tokens, f, "backendApis", 4)
        if s > 0:
            score += s
            reasons.append(f"API match")

        # Name match (+3)
        s = _field_match(tokens, f, "name", 3)
        if s > 0:
            score += s
            reasons.append(f"name match: {f.get('name')}")

        # Data source match (+2)
        s = _list_field_match(tokens, f, "dataSources", 2)
        if s > 0:
            score += s
            reasons.append("data source match")

        # Artifact match (+2)
        s = _list_field_match(tokens, f, "artifacts", 2)
        if s > 0:
            score += s
            reasons.append("artifact match")

        # Docs / dependency keyword (+1)
        s = _list_field_match(tokens, f, "docs", 1)
        s += _list_field_match(tokens, f, "dependencies", 1)
        s += _dict_field_match(tokens, f, "knownIssues", 1)
        if s > 0:
            score += s
            reasons.append("docs/deps/issue match")

        results.append({
            "entry": f,
            "score": round(score, 2),
            "reasons": reasons,
            "confidence": "high" if score >= 5 else "medium" if score >= 2 else "low",
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    return [r for r in results if r["score"] > 0]


def match_pipelines(prd_info: dict, pipelines: list[dict]) -> list[dict]:
    """Score and rank pipeline registry entries against PRD."""
    text = _build_search_text(prd_info)
    tokens = _tokenize(text)
    results = []

    for p in pipelines:
        score = 0.0
        reasons: list[str] = []

        s = _field_match(tokens, p, "pipelineId", 5)
        if s > 0:
            score += s
            reasons.append(f"pipelineId match")

        s = _field_match(tokens, p, "path", 4)
        if s > 0:
            score += s
            reasons.append(f"path match: {p.get('path')}")

        s = _field_match(tokens, p, "name", 3)
        if s > 0:
            score += s
            reasons.append(f"name match: {p.get('name')}")

        s = _list_field_match(tokens, p, "outputs", 3)
        if s > 0:
            score += s
            reasons.append("output match")

        s = _list_field_match(tokens, p, "consumers", 2)
        s += _field_match(tokens, p, "trigger", 2)
        s += _field_match(tokens, p, "schedule", 2)
        if s > 0:
            score += s
            reasons.append("trigger/schedule/consumer match")

        s = _dict_field_match(tokens, p, "knownIssues", 1)
        if s > 0:
            score += s
            reasons.append("known issue match")

        results.append({
            "entry": p,
            "score": round(score, 2),
            "reasons": reasons,
            "confidence": "high" if score >= 5 else "medium" if score >= 2 else "low",
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    return [r for r in results if r["score"] > 0]


def match_sources(prd_info: dict, sources: list[dict]) -> list[dict]:
    """Score and rank source registry entries against PRD."""
    text = _build_search_text(prd_info)
    tokens = _tokenize(text)
    results = []

    for s in sources:
        score = 0.0
        reasons: list[str] = []

        s_val = _field_match(tokens, s, "sourceId", 5)
        if s_val > 0:
            score += s_val
            reasons.append("sourceId match")

        s_val = _field_match(tokens, s, "name", 3)
        if s_val > 0:
            score += s_val
            reasons.append(f"name match: {s.get('name')}")

        s_val = _field_match(tokens, s, "sourceType", 2)
        s_val += _field_match(tokens, s, "country", 2)
        s_val += _field_match(tokens, s, "path", 2)
        if s_val > 0:
            score += s_val
            reasons.append("type/country/path match")

        results.append({
            "entry": s,
            "score": round(score, 2),
            "reasons": reasons,
            "confidence": "high" if score >= 5 else "medium" if score >= 2 else "low",
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    return [r for r in results if r["score"] > 0]


def match_prompts(prd_info: dict, prompts: list[dict]) -> list[dict]:
    """Score and rank prompt registry entries against PRD."""
    text = _build_search_text(prd_info)
    tokens = _tokenize(text)
    results = []

    for p in prompts:
        score = 0.0
        reasons: list[str] = []

        s = _field_match(tokens, p, "promptId", 5)
        if s > 0:
            score += s
            reasons.append("promptId match")

        s = _field_match(tokens, p, "name", 3)
        if s > 0:
            score += s
            reasons.append(f"name match: {p.get('name')}")

        s = _field_match(tokens, p, "pipeline", 2)
        s += _field_match(tokens, p, "path", 2)
        if s > 0:
            score += s
            reasons.append("pipeline/path match")

        results.append({
            "entry": p,
            "score": round(score, 2),
            "reasons": reasons,
            "confidence": "high" if score >= 5 else "medium" if score >= 2 else "low",
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    return [r for r in results if r["score"] > 0]


def match_artifacts(prd_info: dict, artifacts: list[dict]) -> list[dict]:
    """Score and rank artifact registry entries against PRD."""
    text = _build_search_text(prd_info)
    tokens = _tokenize(text)
    results = []

    for a in artifacts:
        score = 0.0
        reasons: list[str] = []

        s = _field_match(tokens, a, "artifactId", 5)
        if s > 0:
            score += s
            reasons.append("artifactId match")

        s = _field_match(tokens, a, "name", 3)
        if s > 0:
            score += s
            reasons.append(f"name match: {a.get('name')}")

        s = _field_match(tokens, a, "path", 2)
        s += _field_match(tokens, a, "pipeline", 2)
        if s > 0:
            score += s
            reasons.append("path/pipeline match")

        s = _list_field_match(tokens, a, "consumerFeatures", 1)
        s += _list_field_match(tokens, a, "producerPipelines", 1)
        if s > 0:
            score += s
            reasons.append("consumer/producer match")

        results.append({
            "entry": a,
            "score": round(score, 2),
            "reasons": reasons,
            "confidence": "high" if score >= 5 else "medium" if score >= 2 else "low",
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    return [r for r in results if r["score"] > 0]


# ── Risk detection ───────────────────────────────────────────────────

def detect_risks(prd_info: dict) -> list[dict]:
    """Detect risk areas from PRD keywords."""
    kws = prd_info.get("detected_keywords", {})
    risks: list[dict] = []

    risk_rules = [
        ("Backend", ["backend", "database"], "PRD references backend/API/database changes"),
        ("Frontend", ["frontend"], "PRD references frontend/UI/component changes"),
        ("Pipeline", ["pipeline"], "PRD references crawler/scheduler/ETL changes"),
        ("Intelligence", ["llm"], "PRD references LLM/prompt/model changes"),
        ("Cost", ["cost"], "PRD references high-cost operations (Pro, deep report, batch)"),
        ("Tests", ["test"], "PRD should include test requirements"),
        ("Docs", [], "PRD should identify docs to update"),
    ]

    for area, trigger_cats, default_reason in risk_rules:
        triggered = any(kws.get(cat) for cat in trigger_cats) if trigger_cats else True
        if triggered or not trigger_cats:
            reasons = []
            for cat in trigger_cats:
                if cat in kws:
                    reasons.append(f"keyword: {', '.join(kws[cat])}")
            risks.append({
                "area": area,
                "risk": "medium" if area in ("Backend", "Pipeline", "Intelligence") else "low",
                "reason": "; ".join(reasons) if reasons else default_reason,
            })

    return risks


# ── Helpers ──────────────────────────────────────────────────────────

def _build_search_text(prd_info: dict) -> str:
    """Build a combined searchable text from PRD info."""
    parts: list[str] = []
    parts.append(prd_info.get("title", ""))
    for h in prd_info.get("headings", []):
        parts.append(h)
    for r in prd_info.get("routes", []):
        parts.append(r)
    for a in prd_info.get("api_paths", []):
        parts.append(a)
    for fp in prd_info.get("file_paths", []):
        parts.append(fp)
    for kws in prd_info.get("detected_keywords", {}).values():
        parts.extend(kws)
    return " ".join(parts)
