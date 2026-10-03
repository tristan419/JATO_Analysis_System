from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Any
from urllib.parse import urljoin

from app.services.jato_browser_readonly_service import read_web_page
from app.services.jato_browser_readonly_service import validate_public_http_url


DEFAULT_TIMEOUT_MS = 12_000
TOKEN_TTL_SECONDS = 10 * 60
MAX_TYPED_CHARS = 500
_TOKEN_SECRET = os.getenv("JATO_BROWSER_ACTION_SECRET", "").strip() or secrets.token_urlsafe(32)


def browser_interaction_plan(
    url: str,
    *,
    action_goal: str = "",
    max_actions: int = 6,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
) -> dict[str, Any]:
    safe_url = validate_public_http_url(url)
    action_limit = max(1, min(int(max_actions or 6), 12))
    safe_timeout = _safe_timeout(timeout_ms)
    sync_playwright = _load_sync_playwright()
    if sync_playwright is None:
        return _build_static_link_plan(safe_url, action_goal=action_goal, max_actions=action_limit)
    try:
        return _build_playwright_plan(
            safe_url,
            action_goal=action_goal,
            max_actions=action_limit,
            timeout_ms=safe_timeout,
            sync_playwright=sync_playwright,
        )
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Unable to build browser interaction plan: {exc}") from exc


def browser_click_confirmed(
    url: str,
    *,
    action_id: str,
    confirmation_token: str,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
    max_chars: int = 6000,
) -> dict[str, Any]:
    safe_url = validate_public_http_url(url)
    payload = _verify_confirmation_token(confirmation_token, expected_url=safe_url, expected_action_type="click")
    if payload["actionId"] != action_id:
        raise ValueError("Confirmation token does not match the requested browser action")

    target_url = str(payload.get("targetUrl") or "").strip()
    if payload.get("selectorType") == "url" and target_url:
        clicked_url = validate_public_http_url(target_url)
        page = read_web_page(clicked_url, question="confirmed link click", max_chars=max_chars)
        return {
            "status": "ok",
            "action": "click",
            "actionId": action_id,
            "url": safe_url,
            "resultUrl": page.get("url", clicked_url),
            "title": page.get("title", clicked_url),
            "textPreview": page.get("textPreview", ""),
            "headings": page.get("headings", []),
            "links": page.get("links", []),
            "truncated": bool(page.get("truncated")),
            "limitations": _interaction_limitations([
                "Confirmed static link navigation; JavaScript click handlers were not executed.",
            ]),
        }

    sync_playwright = _load_sync_playwright()
    if sync_playwright is None:
        raise ValueError("Playwright is required for confirmed non-link browser clicks")

    try:
        return _run_confirmed_click(
            safe_url,
            payload=payload,
            action_id=action_id,
            timeout_ms=_safe_timeout(timeout_ms),
            max_chars=max_chars,
            sync_playwright=sync_playwright,
        )
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Unable to execute confirmed browser click: {exc}") from exc


def browser_type_confirmed(
    url: str,
    *,
    action_id: str,
    confirmation_token: str,
    text: str,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
    max_chars: int = 6000,
) -> dict[str, Any]:
    safe_url = validate_public_http_url(url)
    payload = _verify_confirmation_token(confirmation_token, expected_url=safe_url, expected_action_type="type")
    if payload["actionId"] != action_id:
        raise ValueError("Confirmation token does not match the requested browser action")
    typed_text = str(text or "")
    if not typed_text:
        raise ValueError("text is required for browser_type_confirmed")
    if len(typed_text) > MAX_TYPED_CHARS:
        raise ValueError(f"browser_type_confirmed text is limited to {MAX_TYPED_CHARS} characters")
    sync_playwright = _load_sync_playwright()
    if sync_playwright is None:
        raise ValueError("Playwright is required for confirmed browser typing")
    try:
        return _run_confirmed_type(
            safe_url,
            payload=payload,
            action_id=action_id,
            text=typed_text,
            timeout_ms=_safe_timeout(timeout_ms),
            max_chars=max_chars,
            sync_playwright=sync_playwright,
        )
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Unable to execute confirmed browser typing: {exc}") from exc


def _build_static_link_plan(url: str, *, action_goal: str, max_actions: int) -> dict[str, Any]:
    page = read_web_page(url, question=action_goal, max_chars=4000)
    actions: list[dict[str, Any]] = []
    for index, link in enumerate(page.get("links", []) if isinstance(page.get("links"), list) else []):
        if not isinstance(link, dict):
            continue
        target_url = str(link.get("url") or "").strip()
        if not target_url:
            continue
        try:
            safe_target = validate_public_http_url(urljoin(url, target_url))
        except ValueError:
            continue
        action_id = f"act_{index + 1:02d}"
        label = _clean_text(str(link.get("label") or safe_target))[:160]
        token = _make_confirmation_token({
            "url": url,
            "actionId": action_id,
            "actionType": "click",
            "selectorType": "url",
            "selector": "",
            "label": label,
            "targetUrl": safe_target,
        })
        actions.append({
            "actionId": action_id,
            "actionType": "click",
            "targetType": "link",
            "label": label,
            "targetUrl": safe_target,
            "confirmationToken": token["token"],
            "expiresAt": token["expiresAt"],
            "risk": "low",
            "requiresUserApproval": True,
        })
        if len(actions) >= max_actions:
            break
    return {
        "status": "fallback_static",
        "url": page.get("url", url),
        "browserEngine": "unavailable",
        "title": page.get("title", url),
        "actionGoal": action_goal,
        "actions": actions,
        "approvalInstructions": _approval_instructions(),
        "limitations": _interaction_limitations([
            "Playwright is not installed; only public static links can be proposed.",
            "Buttons and input fields require Playwright and were not inspected.",
        ]),
    }


def _build_playwright_plan(
    url: str,
    *,
    action_goal: str,
    max_actions: int,
    timeout_ms: int,
    sync_playwright,
) -> dict[str, Any]:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            context = browser.new_context(viewport={"width": 1366, "height": 900}, ignore_https_errors=False)
            page = context.new_page()
            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            _wait_for_network_idle(page, timeout_ms)
            final_url = validate_public_http_url(page.url or url)
            raw_actions = page.eval_on_selector_all(
                "a[href],button,input,textarea,select,[role='button'],[role='link']",
                _ACTION_DISCOVERY_SCRIPT,
            )
            actions = _build_actions_from_candidates(final_url, raw_actions, max_actions=max_actions)
            return {
                "status": "ok",
                "url": final_url,
                "browserEngine": "playwright.chromium",
                "httpStatus": response.status if response else None,
                "title": (page.title() or final_url)[:240],
                "actionGoal": action_goal,
                "actions": actions,
                "approvalInstructions": _approval_instructions(),
                "limitations": _interaction_limitations(),
            }
        finally:
            browser.close()


def _build_actions_from_candidates(url: str, raw_actions: Any, *, max_actions: int) -> list[dict[str, Any]]:
    if not isinstance(raw_actions, list):
        return []
    actions: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for candidate in raw_actions:
        if not isinstance(candidate, dict):
            continue
        action_type = str(candidate.get("actionType") or "").strip()
        selector = str(candidate.get("selector") or "").strip()
        label = _clean_text(str(candidate.get("label") or candidate.get("placeholder") or selector))[:160]
        target_type = str(candidate.get("targetType") or "element").strip()
        if action_type not in {"click", "type"} or not selector or not label:
            continue
        if action_type == "type" and target_type not in {"input", "textarea", "select"}:
            continue
        if _is_blocked_candidate(candidate):
            continue
        key = (action_type, selector)
        if key in seen:
            continue
        seen.add(key)
        action_id = f"act_{len(actions) + 1:02d}"
        target_url = ""
        if action_type == "click" and candidate.get("href"):
            try:
                target_url = validate_public_http_url(urljoin(url, str(candidate["href"])))
            except ValueError:
                target_url = ""
        token = _make_confirmation_token({
            "url": url,
            "actionId": action_id,
            "actionType": action_type,
            "selectorType": "css",
            "selector": selector,
            "label": label,
            "targetUrl": target_url,
        })
        actions.append({
            "actionId": action_id,
            "actionType": action_type,
            "targetType": target_type,
            "label": label,
            "selectorHint": selector,
            "targetUrl": target_url,
            "confirmationToken": token["token"],
            "expiresAt": token["expiresAt"],
            "risk": "low" if target_url else "medium",
            "requiresUserApproval": True,
        })
        if len(actions) >= max_actions:
            break
    return actions


def _run_confirmed_click(
    url: str,
    *,
    payload: dict[str, Any],
    action_id: str,
    timeout_ms: int,
    max_chars: int,
    sync_playwright,
) -> dict[str, Any]:
    selector = str(payload.get("selector") or "").strip()
    if not selector:
        raise ValueError("Confirmation token is missing a browser selector")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            context = browser.new_context(viewport={"width": 1366, "height": 900}, ignore_https_errors=False)
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            _wait_for_network_idle(page, timeout_ms)
            locator = page.locator(selector).first
            _assert_safe_click(locator)
            locator.click(timeout=min(timeout_ms, 5000))
            _wait_for_network_idle(page, timeout_ms)
            final_url = validate_public_http_url(page.url or url)
            snapshot = _snapshot_page(page, final_url, max_chars=max_chars)
            return {
                **snapshot,
                "status": "ok",
                "action": "click",
                "actionId": action_id,
                "url": url,
                "resultUrl": final_url,
                "limitations": _interaction_limitations(),
            }
        finally:
            browser.close()


def _run_confirmed_type(
    url: str,
    *,
    payload: dict[str, Any],
    action_id: str,
    text: str,
    timeout_ms: int,
    max_chars: int,
    sync_playwright,
) -> dict[str, Any]:
    selector = str(payload.get("selector") or "").strip()
    if not selector:
        raise ValueError("Confirmation token is missing a browser selector")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            context = browser.new_context(viewport={"width": 1366, "height": 900}, ignore_https_errors=False)
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            _wait_for_network_idle(page, timeout_ms)
            locator = page.locator(selector).first
            _assert_safe_type(locator)
            locator.fill(text, timeout=min(timeout_ms, 5000))
            final_url = validate_public_http_url(page.url or url)
            snapshot = _snapshot_page(page, final_url, max_chars=max_chars)
            return {
                **snapshot,
                "status": "ok",
                "action": "type",
                "actionId": action_id,
                "url": url,
                "resultUrl": final_url,
                "typedCharacters": len(text),
                "limitations": _interaction_limitations([
                    "Typing only fills the approved field; Enter, submit, upload, and form-send actions are blocked.",
                ]),
            }
        finally:
            browser.close()


def _snapshot_page(page, url: str, *, max_chars: int) -> dict[str, Any]:
    text_limit = max(1000, min(int(max_chars or 6000), 20_000))
    body_text = _clean_text(page.locator("body").inner_text(timeout=3000))
    headings = page.locator("h1,h2,h3").all_inner_texts()[:20]
    return {
        "browserEngine": "playwright.chromium",
        "title": (page.title() or url)[:240],
        "textPreview": body_text[:text_limit],
        "headings": [_clean_text(value)[:240] for value in headings if _clean_text(value)],
        "truncated": len(body_text) > text_limit,
    }


def _assert_safe_click(locator) -> None:
    unsafe = locator.evaluate(
        """node => {
            const tag = (node.tagName || '').toLowerCase();
            const type = (node.getAttribute('type') || '').toLowerCase();
            const role = (node.getAttribute('role') || '').toLowerCase();
            const form = node.closest && node.closest('form');
            return Boolean(
                type === 'submit' ||
                type === 'file' ||
                role === 'menuitemcheckbox' ||
                (form && tag !== 'a')
            );
        }""",
        timeout=2000,
    )
    if unsafe:
        raise ValueError("Confirmed browser click blocked because the target may submit a form or modify state")


def _assert_safe_type(locator) -> None:
    unsafe = locator.evaluate(
        """node => {
            const tag = (node.tagName || '').toLowerCase();
            const type = (node.getAttribute('type') || '').toLowerCase();
            const readonly = node.hasAttribute('readonly');
            const disabled = node.hasAttribute('disabled');
            return Boolean(
                disabled ||
                readonly ||
                type === 'password' ||
                type === 'file' ||
                type === 'hidden' ||
                type === 'submit' ||
                !['input', 'textarea', 'select'].includes(tag)
            );
        }""",
        timeout=2000,
    )
    if unsafe:
        raise ValueError("Confirmed browser typing blocked because the target field is unsafe or not editable")


def _make_confirmation_token(payload: dict[str, Any]) -> dict[str, Any]:
    expires_at = int(time.time()) + TOKEN_TTL_SECONDS
    token_payload = {
        "version": 1,
        "expiresAt": expires_at,
        **payload,
    }
    body = _b64url(json.dumps(token_payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    signature = _sign(body)
    return {
        "token": f"{body}.{signature}",
        "expiresAt": expires_at,
    }


def _verify_confirmation_token(token: str, *, expected_url: str, expected_action_type: str) -> dict[str, Any]:
    try:
        body, signature = str(token or "").split(".", 1)
    except ValueError as exc:
        raise ValueError("Invalid browser confirmation token") from exc
    expected_signature = _sign(body)
    if not hmac.compare_digest(signature, expected_signature):
        raise ValueError("Invalid browser confirmation token signature")
    try:
        payload = json.loads(base64.urlsafe_b64decode(_pad_b64(body)).decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid browser confirmation token payload") from exc
    if not isinstance(payload, dict):
        raise ValueError("Invalid browser confirmation token payload")
    if int(payload.get("expiresAt") or 0) < int(time.time()):
        raise ValueError("Browser confirmation token has expired")
    if payload.get("url") != expected_url:
        raise ValueError("Confirmation token URL does not match this request")
    if payload.get("actionType") != expected_action_type:
        raise ValueError("Confirmation token action type does not match this request")
    if not str(payload.get("actionId") or "").strip():
        raise ValueError("Confirmation token is missing an action id")
    return payload


def _sign(body: str) -> str:
    digest = hmac.new(_TOKEN_SECRET.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).digest()
    return _b64url(digest)


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _pad_b64(value: str) -> bytes:
    return f"{value}{'=' * (-len(value) % 4)}".encode("ascii")


def _safe_timeout(timeout_ms: int) -> int:
    return max(1000, min(int(timeout_ms or DEFAULT_TIMEOUT_MS), 30_000))


def _wait_for_network_idle(page, timeout_ms: int) -> None:
    try:
        page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 5000))
    except Exception:
        pass


def _is_blocked_candidate(candidate: dict[str, Any]) -> bool:
    input_type = str(candidate.get("inputType") or "").lower()
    role = str(candidate.get("role") or "").lower()
    return input_type in {"password", "file", "hidden", "submit"} or role in {"menuitemcheckbox"}


def _approval_instructions() -> str:
    return (
        "Present the chosen actionId, label, and risk to the user. "
        "Only call a *_confirmed tool after the user explicitly approves that exact action."
    )


def _interaction_limitations(extra: list[str] | None = None) -> list[str]:
    limitations = [
        "Isolated browser context; no user cookies, login state, browser profile, or stored credentials.",
        "Only public HTTP/HTTPS URLs are allowed; local/private/credential-bearing URLs are blocked.",
        "No form submission, upload, purchase, account change, message sending, or Enter-key submit is supported.",
        "Confirmed tools require a short-lived confirmation token generated by browser_interaction_plan.",
    ]
    return [*(extra or []), *limitations]


def _load_sync_playwright():
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return None
    return sync_playwright


def _clean_text(value: str) -> str:
    return " ".join(str(value or "").split())


_ACTION_DISCOVERY_SCRIPT = """
nodes => {
  function clean(value) {
    return String(value || '').replace(/\\s+/g, ' ').trim();
  }
  function cssPath(node) {
    if (!node || !node.tagName) return '';
    if (node.id && !/\\s/.test(node.id)) return `#${CSS.escape(node.id)}`;
    const testId = node.getAttribute('data-testid') || node.getAttribute('data-test');
    if (testId) return `[data-testid="${CSS.escape(testId)}"]`;
    const name = node.getAttribute('name');
    if (name) return `${node.tagName.toLowerCase()}[name="${CSS.escape(name)}"]`;
    const parts = [];
    let current = node;
    while (current && current.nodeType === Node.ELEMENT_NODE && parts.length < 5) {
      let part = current.tagName.toLowerCase();
      const parent = current.parentElement;
      if (parent) {
        const sameTag = Array.from(parent.children).filter(child => child.tagName === current.tagName);
        if (sameTag.length > 1) {
          part += `:nth-of-type(${sameTag.indexOf(current) + 1})`;
        }
      }
      parts.unshift(part);
      current = parent;
    }
    return parts.join(' > ');
  }
  return nodes
    .map(node => {
      const rect = node.getBoundingClientRect();
      const style = window.getComputedStyle(node);
      const tag = (node.tagName || '').toLowerCase();
      const role = node.getAttribute('role') || '';
      const inputType = (node.getAttribute('type') || '').toLowerCase();
      const href = node.getAttribute('href') || '';
      const label = clean(
        node.innerText ||
        node.getAttribute('aria-label') ||
        node.getAttribute('title') ||
        node.getAttribute('placeholder') ||
        node.getAttribute('name') ||
        href
      );
      const targetType = tag === 'textarea' ? 'textarea' : tag === 'select' ? 'select' : tag === 'input' ? 'input' : tag === 'a' || role === 'link' ? 'link' : 'button';
      const actionType = targetType === 'input' || targetType === 'textarea' || targetType === 'select' ? 'type' : 'click';
      return {
        visible: rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none',
        actionType,
        targetType,
        selector: cssPath(node),
        label,
        placeholder: clean(node.getAttribute('placeholder') || ''),
        href,
        inputType,
        role,
      };
    })
    .filter(item => item.visible && item.selector && item.label)
    .slice(0, 40);
}
"""
