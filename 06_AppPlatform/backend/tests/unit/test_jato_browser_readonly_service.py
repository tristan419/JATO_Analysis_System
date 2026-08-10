from __future__ import annotations

import pytest

from app.services import jato_browser_readonly_service as browser_readonly
from app.services import jato_mcp_tools_service


class FakeResponse:
    def __init__(
        self,
        body: str,
        *,
        url: str = "https://example.com/report",
        status_code: int = 200,
        content_type: str = "text/html; charset=utf-8",
    ) -> None:
        self._body = body.encode("utf-8")
        self.url = url
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}
        self.encoding = "utf-8"

    def iter_content(self, chunk_size: int):
        yield self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _mock_public_dns(monkeypatch) -> None:
    monkeypatch.setattr(
        browser_readonly.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(None, None, None, None, ("93.184.216.34", 443))],
    )


def test_read_web_page_extracts_static_html(monkeypatch) -> None:
    _mock_public_dns(monkeypatch)
    html = """
    <html>
      <head><title>EV Incentive Report</title><meta name="description" content="Policy summary"></head>
      <body>
        <h1>Sweden EV incentives</h1>
        <p>BEV incentives are changing in 2026.</p>
        <a href="/source">Source document</a>
      </body>
    </html>
    """
    monkeypatch.setattr(browser_readonly.requests, "get", lambda *args, **kwargs: FakeResponse(html))

    result = browser_readonly.read_web_page(
        "https://example.com/report",
        question="What changed?",
        max_chars=4000,
    )

    assert result["status"] == "ok"
    assert result["title"] == "EV Incentive Report"
    assert result["description"] == "Policy summary"
    assert result["headings"] == ["Sweden EV incentives"]
    assert "BEV incentives are changing" in result["textPreview"]
    assert result["links"][0]["url"] == "https://example.com/source"
    assert result["truncated"] is False


def test_read_web_page_blocks_private_and_local_urls() -> None:
    with pytest.raises(ValueError, match="blocks"):
        browser_readonly.read_web_page("http://127.0.0.1:8000/private")

    with pytest.raises(ValueError, match="localhost"):
        browser_readonly.read_web_page("http://localhost:8000/private")

    with pytest.raises(ValueError, match="http and https"):
        browser_readonly.read_web_page("file:///etc/passwd")


def test_read_web_page_blocks_credentials_in_url() -> None:
    with pytest.raises(ValueError, match="credentials"):
        browser_readonly.read_web_page("https://user:pass@example.com/private")


def test_read_web_page_truncates_large_text(monkeypatch) -> None:
    _mock_public_dns(monkeypatch)
    html = f"<html><head><title>Long</title></head><body><p>{'market data ' * 1000}</p></body></html>"
    monkeypatch.setattr(browser_readonly.requests, "get", lambda *args, **kwargs: FakeResponse(html))

    result = browser_readonly.read_web_page("https://example.com/long", max_chars=1000)

    assert result["truncated"] is True
    assert len(result["textPreview"]) <= 1000


def test_read_web_page_wraps_network_failures(monkeypatch) -> None:
    _mock_public_dns(monkeypatch)

    def fake_get(*_args, **_kwargs):
        raise browser_readonly.requests.ConnectionError("network down")

    monkeypatch.setattr(browser_readonly.requests, "get", fake_get)

    with pytest.raises(ValueError, match="Unable to read web page"):
        browser_readonly.read_web_page("https://public.example.test/report")


def test_read_web_page_mcp_tool_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        jato_mcp_tools_service,
        "browser_read_web_page",
        lambda url, question="", max_chars=6000: {
            "status": "ok",
            "url": url,
            "httpStatus": 200,
            "contentType": "text/html",
            "title": "Report",
            "description": "",
            "headings": [],
            "textPreview": "Static page text",
            "links": [],
            "truncated": False,
            "limitations": ["readonly"],
        },
    )

    result = jato_mcp_tools_service.call_jato_mcp_tool(
        "read_web_page",
        {"url": "https://example.com/report", "question": "summarize"},
    )

    assert result["tool"] == "read_web_page"
    assert result["metadata"]["source"] == "jato_browser_readonly"
    assert result["metadata"]["readonly"] is True
    assert result["data"]["textPreview"] == "Static page text"
