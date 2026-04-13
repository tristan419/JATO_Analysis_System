"""LLM-assisted MSRP page analysis for source drafting and strategy selection.

This module is intentionally advisory. It helps operators decide whether a
page looks like true MSRP, financing content, or a dynamic configurator, and
which extractor path is most promising before hand-writing selectors.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any

import requests

from jato_scraper.llm.client import ChatMessage
from jato_scraper.llm.providers import create_chat_client


DEFAULT_FETCH_TIMEOUT = 30
DEFAULT_LLM_TIMEOUT = 90
DEFAULT_MAX_TEXT_EXCERPT = 4000
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)

DYNAMIC_LINK_HINTS = (
    "__app/",
    "configurator",
    "konfigurator",
    "bygg",
    "build",
)
POWERTRAIN_HINTS = (
    "ehybrid",
    "e-hybrid",
    "plug-in",
    "plug in",
    "phev",
    "mhev",
    "etsi",
    "tsi",
    "tdi",
    "diesel",
    "petrol",
    "benzin",
    "drivlina",
    "powertrain",
    "motor",
)

MSRP_KEYWORDS = (
    "recommended retail price",
    "manufacturer's recommended retail price",
    "msrp",
    "unverbindliche preisempfehlung",
    "doporučená cena",
    "rekommenderat cirkapris",
    "ajánlott fogyasztói ár",
    "inkl. moms",
    "inkl. mwst",
    "včetně dph",
)
FINANCE_KEYWORDS = (
    "leasing",
    "finance",
    "financing",
    "monthly",
    "per month",
    "pro monat",
    "monatlich",
    "rate",
    "raten",
    "rental",
)
CONFIGURATOR_KEYWORDS = (
    "configurator",
    "konfigurator",
    "bygg din bil",
    "build your",
    "build-your",
    "__app/",
    "engine-card",
    "trimcard",
)
JSON_HINT_KEYWORDS = (
    "application/ld+json",
    "json-ld",
    "offers",
    "aggregateoffer",
)


def fetch_page_html(
    url: str,
    timeout: int = DEFAULT_FETCH_TIMEOUT,
) -> tuple[str, dict[str, Any]]:
    response = requests.get(
        url,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=timeout,
    )
    response.raise_for_status()
    metadata = {
        "requested_url": url,
        "final_url": response.url,
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type"),
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    return response.text, metadata


def build_page_evidence(
    *,
    html: str,
    url: str | None,
    metadata: dict[str, Any] | None = None,
    max_text_excerpt: int = DEFAULT_MAX_TEXT_EXCERPT,
) -> dict[str, Any]:
    normalized_text = _normalize_space(_strip_html_tags(html))
    evidence = {
        "page": {
            "url": url,
            **(metadata or {}),
            "title": _extract_title(html),
            "meta_description": _extract_meta_description(html),
        },
        "signals": {
            "ld_json_script_count": len(
                re.findall(
                    r"<script[^>]+type=['\"]application/ld\+json['\"]",
                    html,
                    flags=re.IGNORECASE,
                )
            ),
            "data_testids": _extract_data_testids(html),
            "candidate_links": _extract_candidate_links(html),
            "headings": _extract_headings(html),
            "keyword_hits": {
                "msrp": _match_keywords(normalized_text, MSRP_KEYWORDS),
                "finance": _match_keywords(normalized_text, FINANCE_KEYWORDS),
                "configurator": _match_keywords(
                    normalized_text,
                    CONFIGURATOR_KEYWORDS,
                ),
                "json": _match_keywords(normalized_text, JSON_HINT_KEYWORDS),
            },
            "price_like_samples": _extract_price_like_samples(normalized_text),
        },
        "text_excerpt": normalized_text[:max_text_excerpt],
    }
    return evidence


def analyze_page_heuristics(evidence: dict[str, Any]) -> dict[str, Any]:
    page = evidence.get("page") or {}
    signals = evidence.get("signals") or {}
    keyword_hits = signals.get("keyword_hits") or {}

    msrp_hits = _normalize_string_list(keyword_hits.get("msrp"))
    finance_hits = _normalize_string_list(keyword_hits.get("finance"))
    configurator_hits = _normalize_string_list(
        keyword_hits.get("configurator")
    )
    json_hits = _normalize_string_list(keyword_hits.get("json"))
    data_testids = _normalize_string_list(signals.get("data_testids"))
    candidate_links = _normalize_string_list(signals.get("candidate_links"))
    headings = _normalize_string_list(signals.get("headings"))
    price_samples = _normalize_string_list(signals.get("price_like_samples"))
    title = _normalize_space(str(page.get("title") or ""))
    text_excerpt = _normalize_space(str(evidence.get("text_excerpt") or ""))
    normalized_excerpt = text_excerpt.lower()

    trimcard_testid = _find_first_match(
        data_testids,
        ("trimcard", "smart-offer-trim"),
    )
    enginecard_testid = _find_first_match(
        data_testids,
        ("engine-card", "enginecard"),
    )
    price_testid = _find_first_match(data_testids, ("price",))
    dynamic_link_detected = any(
        _contains_any(link.lower(), DYNAMIC_LINK_HINTS)
        for link in candidate_links
    )
    configurator_detected = bool(
        configurator_hits
        or trimcard_testid
        or enginecard_testid
        or dynamic_link_detected
    )
    json_detected = bool(signals.get("ld_json_script_count") or json_hits)
    powertrain_detected = _contains_any(
        normalized_excerpt,
        POWERTRAIN_HINTS,
    )

    if msrp_hits and finance_hits:
        page_semantics = "mixed"
    elif msrp_hits:
        page_semantics = "base_msrp"
    elif finance_hits:
        page_semantics = "finance_offer"
    else:
        page_semantics = "unknown"

    if configurator_detected:
        recommended_extractor = "playwright"
    elif json_detected and price_samples and not finance_hits:
        recommended_extractor = "http_json"
    elif price_samples or headings or title:
        recommended_extractor = "scrapling"
    else:
        recommended_extractor = "manual_review"

    if enginecard_testid or (configurator_detected and powertrain_detected):
        powertrain_granularity = "engine_level"
    elif trimcard_testid or price_samples:
        powertrain_granularity = "trim_level"
    else:
        powertrain_granularity = "unknown"

    confidence = 0.24
    if msrp_hits:
        confidence += 0.22
    if finance_hits:
        confidence += 0.05
    if json_detected:
        confidence += 0.08
    if configurator_detected:
        confidence += 0.15
    if price_samples:
        confidence += 0.12
    if title or headings:
        confidence += 0.05
    if page_semantics == "mixed":
        confidence -= 0.09
    if recommended_extractor == "manual_review":
        confidence -= 0.12
    if not price_samples:
        confidence -= 0.05
    confidence = round(_clamp(confidence, 0.05, 0.98), 2)

    selector_hints: dict[str, str] = {}
    if json_detected:
        selector_hints["json_script_selector"] = (
            "script[type='application/ld+json']"
        )
    if trimcard_testid:
        selector_hints["trim_card_selector"] = (
            f"[data-testid='{trimcard_testid}']"
        )
        selector_hints["next_step_selector"] = "[data-testid*='next-step']"
    if enginecard_testid:
        selector_hints["detail_card_selector"] = (
            f"[data-testid='{enginecard_testid}']"
        )
    if price_testid:
        selector_hints["price"] = f"[data-testid='{price_testid}']"
    elif price_samples and recommended_extractor == "scrapling":
        selector_hints["vehicle_container"] = (
            "Inspect the nearest repeating model card around the visible MSRP"
        )

    evidence_summary: list[str] = []
    if title:
        evidence_summary.append(f"Title: {title[:120]}")
    if headings:
        evidence_summary.append(
            "Headings: " + ", ".join(headings[:3])
        )
    if msrp_hits:
        evidence_summary.append(
            "MSRP keywords: " + ", ".join(msrp_hits[:3])
        )
    if finance_hits:
        evidence_summary.append(
            "Finance keywords: " + ", ".join(finance_hits[:3])
        )
    if json_detected:
        evidence_summary.append(
            "Structured data signals: "
            f"{signals.get('ld_json_script_count', 0)} ld+json script tags"
        )
    if configurator_detected:
        evidence_summary.append(
            "Configurator signals detected via candidate links or data-testid"
        )
    if price_samples:
        evidence_summary.append(
            "Price-like samples: " + ", ".join(price_samples[:3])
        )

    risks: list[str] = []
    if finance_hits:
        risks.append(
            "Finance wording may contaminate MSRP extraction with "
            "monthly offers"
        )
    if page_semantics == "unknown":
        risks.append(
            "The visible text excerpt does not state MSRP semantics explicitly"
        )
    if recommended_extractor == "playwright":
        risks.append(
            "Dynamic configurator flows usually need consent "
            "handling and waits"
        )
    if not price_samples:
        risks.append("No price-like values were detected in the visible text")

    should_use_llm_in_pipeline = bool(
        page_semantics in {"mixed", "unknown"}
        or recommended_extractor in {"playwright", "manual_review"}
        or not price_samples
    )
    llm_use_cases: list[str] = []
    if page_semantics in {"mixed", "unknown"}:
        llm_use_cases.append("Disambiguate MSRP vs finance semantics")
    if recommended_extractor == "playwright":
        llm_use_cases.append(
            "Suggest selectors and transition waits for the configurator"
        )
    if recommended_extractor == "manual_review" or not price_samples:
        llm_use_cases.append(
            "Inspect hidden APIs or structured payloads that hold prices"
        )
    if powertrain_granularity == "engine_level":
        llm_use_cases.append(
            "Normalize multilingual powertrain labels for review triage"
        )

    recommendation = _build_heuristic_recommendation(
        page_semantics=page_semantics,
        recommended_extractor=recommended_extractor,
        should_use_llm_in_pipeline=should_use_llm_in_pipeline,
    )

    return {
        "page_semantics": page_semantics,
        "recommended_extractor": recommended_extractor,
        "powertrain_granularity": powertrain_granularity,
        "confidence": confidence,
        "should_use_llm_in_pipeline": should_use_llm_in_pipeline,
        "llm_use_cases": llm_use_cases,
        "selector_hints": selector_hints,
        "evidence_summary": evidence_summary,
        "risks": risks,
        "recommendation": recommendation,
    }


def analyze_page_evidence(
    *,
    provider: str,
    model: str,
    evidence: dict[str, Any],
    heuristics: dict[str, Any] | None = None,
    llm_timeout: int = DEFAULT_LLM_TIMEOUT,
) -> dict[str, Any]:
    client = create_chat_client(provider, default_model=model)
    system_prompt = "\n".join(
        [
            "You analyze automotive OEM web pages for MSRP scraping.",
            "Respond with JSON only.",
            "Do not invent evidence that is not in the payload.",
            "Prefer conservative recommendations when the page looks",
            "finance-oriented or ambiguous.",
        ]
    )
    user_prompt = "\n".join(
        [
            "Analyze this page evidence and return one JSON object.",
            "You may use the deterministic heuristic summary as a hint,",
            "but override it whenever the raw evidence suggests otherwise.",
            "It must include exactly these keys:",
            "- page_semantics: one of",
            '  ["base_msrp", "finance_offer", "mixed", "unknown"]',
            "- recommended_extractor: one of",
            '  ["scrapling", "playwright", "http_json",',
            '  "manual_review"]',
            "- powertrain_granularity: one of",
            '  ["trim_level", "engine_level", "unknown"]',
            "- confidence: number from 0 to 1",
            "- should_use_llm_in_pipeline: boolean",
            "- llm_use_cases: array of short strings",
            "- selector_hints: object with optional keys",
            "  json_script_selector, vehicle_container,",
            "  price, trim_card_selector,",
            "  detail_card_selector, next_step_selector",
            "- evidence_summary: array of short strings",
            "  grounded in the payload",
            "- risks: array of short strings",
            "- recommendation: short paragraph",
            "",
            "Heuristic pre-analysis:",
            json.dumps(heuristics or {}, ensure_ascii=False, indent=2),
            "",
            "Evidence payload:",
            json.dumps(evidence, ensure_ascii=False, indent=2),
        ]
    )
    messages = [
        ChatMessage(role="system", content=system_prompt),
        ChatMessage(role="user", content=user_prompt),
    ]
    response = client.chat(
        messages,
        temperature=0.1,
        max_tokens=1400,
        response_format={"type": "json_object"},
        timeout=llm_timeout,
    )
    parsed = parse_json_object(response.text or "")
    parsed["llm"] = {
        "provider": provider,
        "model": response.model or model,
        "usage": (
            None
            if response.usage is None
            else {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
            }
        ),
    }
    return parsed


def parse_json_object(text: str) -> dict[str, Any]:
    candidate = text.strip()
    if not candidate:
        raise ValueError("LLM returned an empty response")
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", candidate, re.S)
    if fenced:
        parsed = json.loads(fenced.group(1))
        if isinstance(parsed, dict):
            return parsed

    start = candidate.find("{")
    if start < 0:
        raise ValueError("LLM response did not contain a JSON object")
    json_blob = _extract_balanced_json(candidate[start:])
    parsed = json.loads(json_blob)
    if not isinstance(parsed, dict):
        raise ValueError("LLM response JSON payload was not an object")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze an MSRP candidate page with an OpenAI-compatible "
            "LLM provider."
        ),
    )
    parser.add_argument(
        "--provider",
        help="nvidia or hf; omit for heuristics-only analysis",
    )
    parser.add_argument(
        "--model",
        help="Model name for the selected provider",
    )
    parser.add_argument("--url", help="Page URL to fetch and analyze")
    parser.add_argument(
        "--html-file",
        help="Analyze a saved HTML file instead of fetching a URL",
    )
    parser.add_argument(
        "--fetch-timeout",
        type=int,
        default=DEFAULT_FETCH_TIMEOUT,
    )
    parser.add_argument("--llm-timeout", type=int, default=DEFAULT_LLM_TIMEOUT)
    parser.add_argument(
        "--max-text-excerpt",
        type=int,
        default=DEFAULT_MAX_TEXT_EXCERPT,
    )
    parser.add_argument(
        "--heuristics-only",
        action="store_true",
        help="Skip the remote LLM call and emit deterministic heuristics only",
    )
    parser.add_argument("--output", help="Optional path for JSON output")
    args = parser.parse_args(argv)

    if not args.url and not args.html_file:
        parser.error("Provide at least one of --url or --html-file")
    if bool(args.provider) != bool(args.model):
        parser.error(
            "Provide both --provider and --model when enabling LLM analysis"
        )

    html: str
    metadata: dict[str, Any] | None = None
    if args.html_file:
        html = Path(args.html_file).read_text(encoding="utf-8")
        metadata = {
            "requested_url": args.url,
            "final_url": args.url,
            "status_code": None,
            "content_type": "text/html; source=file",
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    else:
        html, metadata = fetch_page_html(args.url, timeout=args.fetch_timeout)

    evidence = build_page_evidence(
        html=html,
        url=args.url,
        metadata=metadata,
        max_text_excerpt=args.max_text_excerpt,
    )
    heuristics = analyze_page_heuristics(evidence)
    analysis = None
    if not args.heuristics_only and args.provider and args.model:
        analysis = analyze_page_evidence(
            provider=args.provider,
            model=args.model,
            evidence=evidence,
            heuristics=heuristics,
            llm_timeout=args.llm_timeout,
        )
    payload = {
        "evidence": evidence,
        "heuristics": heuristics,
        "analysis": analysis,
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


def _extract_balanced_json(text: str) -> str:
    depth = 0
    in_string = False
    escape = False
    for index, char in enumerate(text):
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[: index + 1]
    raise ValueError("Failed to find a complete JSON object in LLM response")


def _extract_title(html: str) -> str | None:
    match = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if not match:
        return None
    return _normalize_space(_strip_html_tags(match.group(1)))


def _extract_meta_description(html: str) -> str | None:
    match = re.search(
        r"<meta[^>]+name=['\"]description['\"][^>]+content=['\"](.*?)['\"]",
        html,
        re.I | re.S,
    )
    if not match:
        return None
    return _normalize_space(unescape(match.group(1)))


def _extract_headings(html: str, limit: int = 12) -> list[str]:
    matches = re.findall(r"<h[1-3][^>]*>(.*?)</h[1-3]>", html, re.I | re.S)
    results: list[str] = []
    for raw in matches:
        text = _normalize_space(_strip_html_tags(raw))
        if text and text not in results:
            results.append(text)
        if len(results) >= limit:
            break
    return results


def _extract_data_testids(html: str, limit: int = 20) -> list[str]:
    matches = re.findall(r"data-testid=['\"]([^'\"]+)['\"]", html, re.I)
    results: list[str] = []
    for match in matches:
        value = match.strip()
        if value and value not in results:
            results.append(value)
        if len(results) >= limit:
            break
    return results


def _extract_candidate_links(html: str, limit: int = 20) -> list[str]:
    hrefs = re.findall(r"href=['\"]([^'\"]+)['\"]", html, re.I)
    interesting: list[str] = []
    keywords = ("app", "config", "build", "konfigur", "modelle", "modeller")
    for href in hrefs:
        lowered = href.lower()
        if not any(keyword in lowered for keyword in keywords):
            continue
        if href not in interesting:
            interesting.append(href)
        if len(interesting) >= limit:
            break
    return interesting


def _extract_price_like_samples(text: str, limit: int = 10) -> list[str]:
    matches = re.findall(
        r"\b\d{1,3}(?:[ .,'’]\d{3})+(?:[.,]\d{2})?\b",
        text,
    )
    results: list[str] = []
    for match in matches:
        if match not in results:
            results.append(match)
        if len(results) >= limit:
            break
    return results


def _strip_html_tags(value: str) -> str:
    return unescape(re.sub(r"<[^>]+>", " ", value))


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _match_keywords(text: str, keywords: tuple[str, ...]) -> list[str]:
    lowered = text.lower()
    return [keyword for keyword in keywords if keyword in lowered]


def _normalize_string_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    results: list[str] = []
    for value in values:
        text = _normalize_space(str(value))
        if text:
            results.append(text)
    return results


def _contains_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(needle in text for needle in needles)


def _find_first_match(
    values: list[str],
    needles: tuple[str, ...],
) -> str | None:
    for value in values:
        lowered = value.lower()
        if any(needle in lowered for needle in needles):
            return value
    return None


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _build_heuristic_recommendation(
    *,
    page_semantics: str,
    recommended_extractor: str,
    should_use_llm_in_pipeline: bool,
) -> str:
    opener = {
        "playwright": (
            "Start with Playwright because the page looks like a dynamic "
            "configurator rather than a static MSRP listing."
        ),
        "http_json": (
            "Start with structured data extraction because the page exposes "
            "JSON-like MSRP signals without clear dynamic UI requirements."
        ),
        "scrapling": (
            "Start with Scrapling because the page exposes visible MSRP-like "
            "content that should be testable with CSS selectors."
        ),
        "manual_review": (
            "Do a manual inspection first because the current evidence is too "
            "weak to pick a reliable extractor path."
        ),
    }[recommended_extractor]
    semantics_note = {
        "base_msrp": "The visible copy leans toward true MSRP semantics.",
        "finance_offer": (
            "The visible copy leans toward finance or leasing semantics."
        ),
        "mixed": (
            "The page appears to mix MSRP and finance messaging, so guard "
            "against monthly-payment contamination."
        ),
        "unknown": (
            "The visible copy does not make the pricing semantics explicit."
        ),
    }[page_semantics]
    llm_note = (
        " Add LLM only for semantic disambiguation, selector hints, or "
        "failure triage."
        if should_use_llm_in_pipeline
        else " LLM is optional here and should stay off the hot path."
    )
    return opener + " " + semantics_note + llm_note


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
