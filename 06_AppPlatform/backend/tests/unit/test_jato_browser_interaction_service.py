from __future__ import annotations

import pytest

from app.services import jato_browser_interaction_service as interaction_service


def test_browser_interaction_plan_static_links_mint_confirmation_tokens(monkeypatch) -> None:
    monkeypatch.setattr(interaction_service, "_load_sync_playwright", lambda: None)
    monkeypatch.setattr(interaction_service, "validate_public_http_url", lambda url: url)
    monkeypatch.setattr(
        interaction_service,
        "read_web_page",
        lambda url, question="", max_chars=4000: {
            "url": url,
            "title": "Portal",
            "links": [
                {"label": "Open dashboard", "url": "https://example.com/dashboard"},
                {"label": "Local", "url": "http://127.0.0.1/private"},
            ],
        },
    )

    result = interaction_service.browser_interaction_plan(
        "https://example.com",
        action_goal="open dashboard",
    )

    assert result["status"] == "fallback_static"
    assert result["browserEngine"] == "unavailable"
    assert len(result["actions"]) == 2
    assert result["actions"][0]["actionId"] == "act_01"
    assert result["actions"][0]["actionType"] == "click"
    assert result["actions"][0]["requiresUserApproval"] is True
    assert result["actions"][0]["confirmationToken"]
    assert "Only call a *_confirmed tool" in result["approvalInstructions"]


def test_browser_click_confirmed_static_link_uses_token(monkeypatch) -> None:
    monkeypatch.setattr(interaction_service, "_load_sync_playwright", lambda: None)
    monkeypatch.setattr(interaction_service, "validate_public_http_url", lambda url: url)

    def fake_read_web_page(url: str, question: str = "", max_chars: int = 4000):
        if url == "https://example.com":
            return {
                "url": url,
                "title": "Portal",
                "links": [{"label": "Open dashboard", "url": "https://example.com/dashboard"}],
            }
        return {
            "url": url,
            "title": "Dashboard",
            "textPreview": "Dashboard text",
            "headings": ["Dashboard"],
            "links": [],
            "truncated": False,
        }

    monkeypatch.setattr(interaction_service, "read_web_page", fake_read_web_page)

    plan = interaction_service.browser_interaction_plan("https://example.com")
    action = plan["actions"][0]
    result = interaction_service.browser_click_confirmed(
        "https://example.com",
        action_id=action["actionId"],
        confirmation_token=action["confirmationToken"],
    )

    assert result["status"] == "ok"
    assert result["action"] == "click"
    assert result["resultUrl"] == "https://example.com/dashboard"
    assert result["title"] == "Dashboard"
    assert result["textPreview"] == "Dashboard text"


def test_browser_confirmed_action_rejects_wrong_action_id(monkeypatch) -> None:
    monkeypatch.setattr(interaction_service, "_load_sync_playwright", lambda: None)
    monkeypatch.setattr(interaction_service, "validate_public_http_url", lambda url: url)
    monkeypatch.setattr(
        interaction_service,
        "read_web_page",
        lambda url, question="", max_chars=4000: {
            "url": url,
            "title": "Portal",
            "links": [{"label": "Open dashboard", "url": "https://example.com/dashboard"}],
        },
    )

    plan = interaction_service.browser_interaction_plan("https://example.com")
    token = plan["actions"][0]["confirmationToken"]

    with pytest.raises(ValueError, match="does not match"):
        interaction_service.browser_click_confirmed(
            "https://example.com",
            action_id="act_99",
            confirmation_token=token,
        )


def test_browser_interaction_plan_wraps_playwright_failures(monkeypatch) -> None:
    monkeypatch.setattr(interaction_service, "_load_sync_playwright", lambda: object())
    monkeypatch.setattr(interaction_service, "validate_public_http_url", lambda url: url)
    monkeypatch.setattr(
        interaction_service,
        "_build_playwright_plan",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("navigation timeout")),
    )

    with pytest.raises(ValueError, match="Unable to build browser interaction plan"):
        interaction_service.browser_interaction_plan("https://example.com")


def test_browser_type_confirmed_requires_playwright(monkeypatch) -> None:
    monkeypatch.setattr(interaction_service, "_load_sync_playwright", lambda: None)
    monkeypatch.setattr(interaction_service, "validate_public_http_url", lambda url: url)
    token = interaction_service._make_confirmation_token({
        "url": "https://example.com",
        "actionId": "act_01",
        "actionType": "type",
        "selectorType": "css",
        "selector": "input[name='q']",
        "label": "Search",
        "targetUrl": "",
    })["token"]

    with pytest.raises(ValueError, match="Playwright is required"):
        interaction_service.browser_type_confirmed(
            "https://example.com",
            action_id="act_01",
            confirmation_token=token,
            text="Volvo EX40",
        )


class _FakeBrowserLocator:
    def __init__(self, page: "_FakeBrowserPage", *, unsafe: bool = False) -> None:
        self.page = page
        self.unsafe = unsafe

    @property
    def first(self) -> "_FakeBrowserLocator":
        return self

    def evaluate(self, _script: str, timeout: int = 2000) -> bool:
        self.page.calls.append(("evaluate", timeout))
        return self.unsafe

    def fill(self, text: str, timeout: int = 5000) -> None:
        self.page.calls.append(("fill", text, timeout))
        self.page.typed_text = text

    def inner_text(self, timeout: int = 3000) -> str:
        self.page.calls.append(("inner_text", timeout))
        return f"Search field contains {self.page.typed_text}".strip()

    def all_inner_texts(self) -> list[str]:
        self.page.calls.append(("all_inner_texts",))
        return ["Search"]


class _FakeBrowserPage:
    def __init__(self, *, unsafe: bool = False) -> None:
        self.url = "https://example.com/search"
        self.unsafe = unsafe
        self.typed_text = ""
        self.calls: list[tuple] = []

    def goto(self, url: str, wait_until: str, timeout: int) -> object:
        self.url = url
        self.calls.append(("goto", url, wait_until, timeout))
        return object()

    def wait_for_load_state(self, state: str, timeout: int) -> None:
        self.calls.append(("wait_for_load_state", state, timeout))

    def locator(self, selector: str) -> _FakeBrowserLocator:
        self.calls.append(("locator", selector))
        return _FakeBrowserLocator(self, unsafe=self.unsafe and selector == "input[name='q']")

    def title(self) -> str:
        return "Search Portal"


class _FakeBrowserContext:
    def __init__(self, page: _FakeBrowserPage) -> None:
        self.page = page

    def new_page(self) -> _FakeBrowserPage:
        return self.page


class _FakeBrowser:
    def __init__(self, page: _FakeBrowserPage) -> None:
        self.page = page
        self.closed = False

    def new_context(self, **_kwargs) -> _FakeBrowserContext:
        return _FakeBrowserContext(self.page)

    def close(self) -> None:
        self.closed = True


class _FakeChromium:
    def __init__(self, page: _FakeBrowserPage) -> None:
        self.page = page
        self.browser: _FakeBrowser | None = None

    def launch(self, headless: bool = True) -> _FakeBrowser:
        self.browser = _FakeBrowser(self.page)
        return self.browser


class _FakePlaywright:
    def __init__(self, page: _FakeBrowserPage) -> None:
        self.chromium = _FakeChromium(page)


class _FakeSyncPlaywright:
    def __init__(self, page: _FakeBrowserPage) -> None:
        self.page = page

    def __call__(self) -> "_FakeSyncPlaywright":
        return self

    def __enter__(self) -> _FakePlaywright:
        return _FakePlaywright(self.page)

    def __exit__(self, *_args) -> None:
        return None


def test_browser_type_confirmed_fills_approved_field_without_submit(monkeypatch) -> None:
    page = _FakeBrowserPage()
    monkeypatch.setattr(interaction_service, "_load_sync_playwright", lambda: _FakeSyncPlaywright(page))
    monkeypatch.setattr(interaction_service, "validate_public_http_url", lambda url: url)
    token = interaction_service._make_confirmation_token({
        "url": "https://example.com/search",
        "actionId": "act_01",
        "actionType": "type",
        "selectorType": "css",
        "selector": "input[name='q']",
        "label": "Search",
        "targetUrl": "",
    })["token"]

    result = interaction_service.browser_type_confirmed(
        "https://example.com/search",
        action_id="act_01",
        confirmation_token=token,
        text="Volvo EX40",
    )

    assert result["status"] == "ok"
    assert result["action"] == "type"
    assert result["typedCharacters"] == len("Volvo EX40")
    assert result["resultUrl"] == "https://example.com/search"
    assert "Search field contains Volvo EX40" in result["textPreview"]
    assert any(call[0] == "fill" for call in page.calls)
    assert not any(call[0] in {"press", "submit"} for call in page.calls)
    assert any("Typing only fills the approved field" in item for item in result["limitations"])


def test_browser_type_confirmed_blocks_unsafe_field(monkeypatch) -> None:
    page = _FakeBrowserPage(unsafe=True)
    monkeypatch.setattr(interaction_service, "_load_sync_playwright", lambda: _FakeSyncPlaywright(page))
    monkeypatch.setattr(interaction_service, "validate_public_http_url", lambda url: url)
    token = interaction_service._make_confirmation_token({
        "url": "https://example.com/search",
        "actionId": "act_01",
        "actionType": "type",
        "selectorType": "css",
        "selector": "input[name='q']",
        "label": "Search",
        "targetUrl": "",
    })["token"]

    with pytest.raises(ValueError, match="unsafe or not editable"):
        interaction_service.browser_type_confirmed(
            "https://example.com/search",
            action_id="act_01",
            confirmation_token=token,
            text="Volvo EX40",
        )

    assert not any(call[0] == "fill" for call in page.calls)
