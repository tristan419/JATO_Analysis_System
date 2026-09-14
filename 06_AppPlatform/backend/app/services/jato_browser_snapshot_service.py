from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from app.services.jato_browser_readonly_service import read_web_page
from app.services.jato_browser_readonly_service import validate_public_http_url


_SNAPSHOT_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "hermes" / "browser_snapshots"
DEFAULT_TIMEOUT_MS = 12_000


def browser_snapshot(
    url: str,
    *,
    question: str = "",
    max_chars: int = 6000,
    capture_screenshot: bool = False,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
) -> dict[str, Any]:
    safe_url = validate_public_http_url(url)
    sync_playwright = _load_sync_playwright()
    if sync_playwright is None:
        static_page = read_web_page(safe_url, question=question, max_chars=max_chars)
        return {
            "status": "fallback_static",
            "url": static_page.get("url", safe_url),
            "browserEngine": "unavailable",
            "title": static_page.get("title", ""),
            "textPreview": static_page.get("textPreview", ""),
            "headings": static_page.get("headings", []),
            "links": static_page.get("links", []),
            "screenshot": None,
            "truncated": bool(static_page.get("truncated")),
            "limitations": [
                "Playwright is not installed; returned static HTTP extraction instead.",
                "No JavaScript-rendered browser snapshot was captured.",
                "No click, typing, login, cookie, or form action was performed.",
            ],
        }

    return _capture_playwright_snapshot(
        safe_url,
        question=question,
        max_chars=max_chars,
        capture_screenshot=capture_screenshot,
        timeout_ms=timeout_ms,
        sync_playwright=sync_playwright,
    )


def _capture_playwright_snapshot(
    url: str,
    *,
    question: str,
    max_chars: int,
    capture_screenshot: bool,
    timeout_ms: int,
    sync_playwright,
) -> dict[str, Any]:
    text_limit = max(1000, min(int(max_chars or 6000), 20_000))
    safe_timeout = max(1000, min(int(timeout_ms or DEFAULT_TIMEOUT_MS), 30_000))
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                viewport={"width": 1366, "height": 900},
                ignore_https_errors=False,
            )
            page = context.new_page()
            response = page.goto(url, wait_until="domcontentloaded", timeout=safe_timeout)
            page.wait_for_load_state("networkidle", timeout=min(safe_timeout, 5000))
            final_url = validate_public_http_url(page.url or url)
            title = page.title() or final_url
            body_text = _clean_text(page.locator("body").inner_text(timeout=3000))
            headings = _dedupe(page.locator("h1,h2,h3").all_inner_texts()[:20])
            links = _extract_links(page)
            screenshot = _maybe_capture_screenshot(page, capture_screenshot)
            return {
                "status": "ok",
                "url": final_url,
                "browserEngine": "playwright.chromium",
                "httpStatus": response.status if response else None,
                "title": title[:240],
                "textPreview": body_text[:text_limit],
                "headings": headings,
                "links": links[:20],
                "screenshot": screenshot,
                "truncated": len(body_text) > text_limit,
                "limitations": [
                    "Isolated headless browser context; no user cookies or login state.",
                    "No click, typing, login, cookie, or form action was performed.",
                    "Question is retrieval context only; no browser action is inferred from it.",
                ]
                if question
                else [
                    "Isolated headless browser context; no user cookies or login state.",
                    "No click, typing, login, cookie, or form action was performed.",
                ],
            }
        finally:
            browser.close()


def _extract_links(page) -> list[dict[str, str]]:
    links = page.eval_on_selector_all(
        "a[href]",
        """nodes => nodes.slice(0, 40).map(node => ({
            label: (node.innerText || node.getAttribute('aria-label') || node.href || '').trim(),
            url: node.href
        }))""",
    )
    if not isinstance(links, list):
        return []
    clean_links: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in links:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        clean_links.append({
            "label": _clean_text(str(item.get("label") or url))[:160],
            "url": url,
        })
    return clean_links


def _maybe_capture_screenshot(page, capture_screenshot: bool) -> dict[str, Any] | None:
    if not capture_screenshot:
        return None
    _SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = _SNAPSHOT_DIR / f"snapshot_{uuid.uuid4().hex[:12]}.png"
    page.screenshot(path=str(path), full_page=False)
    return {
        "path": str(path),
        "format": "png",
        "fullPage": False,
    }


def _load_sync_playwright():
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return None
    return sync_playwright


def _clean_text(value: str) -> str:
    return " ".join(str(value or "").split())


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(text[:240])
    return result
