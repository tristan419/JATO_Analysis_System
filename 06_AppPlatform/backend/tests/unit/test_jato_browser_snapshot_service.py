from __future__ import annotations

from app.services import jato_browser_snapshot_service as snapshot_service


def test_browser_snapshot_falls_back_to_static_read_when_playwright_missing(monkeypatch) -> None:
    monkeypatch.setattr(snapshot_service, "_load_sync_playwright", lambda: None)
    monkeypatch.setattr(snapshot_service, "validate_public_http_url", lambda url: url)
    monkeypatch.setattr(
        snapshot_service,
        "read_web_page",
        lambda url, question="", max_chars=6000: {
            "status": "ok",
            "url": url,
            "title": "Static Title",
            "textPreview": "Static text",
            "headings": ["Heading"],
            "links": [{"label": "Link", "url": "https://example.com/link"}],
            "truncated": False,
        },
    )

    result = snapshot_service.browser_snapshot(
        "https://example.com/report",
        question="snapshot",
        capture_screenshot=True,
    )

    assert result["status"] == "fallback_static"
    assert result["browserEngine"] == "unavailable"
    assert result["title"] == "Static Title"
    assert result["screenshot"] is None
    assert "Playwright is not installed" in result["limitations"][0]


def test_browser_snapshot_reuses_public_url_guard(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_validate(url: str) -> str:
        captured["url"] = url
        return url

    monkeypatch.setattr(snapshot_service, "_load_sync_playwright", lambda: None)
    monkeypatch.setattr(snapshot_service, "validate_public_http_url", fake_validate)
    monkeypatch.setattr(
        snapshot_service,
        "read_web_page",
        lambda url, question="", max_chars=6000: {
            "status": "ok",
            "url": url,
            "title": "Title",
            "textPreview": "",
            "headings": [],
            "links": [],
            "truncated": False,
        },
    )

    snapshot_service.browser_snapshot("https://example.com/report")

    assert captured["url"] == "https://example.com/report"
