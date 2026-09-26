from __future__ import annotations

import ipaddress
import socket
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse

import requests


MAX_RESPONSE_BYTES = 1_000_000
MAX_REDIRECTS = 3
HTTP_TIMEOUT_SECONDS = 10
USER_AGENT = "JATO-AstrBot-ReadonlyBrowser/0.1"


class StaticPageParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.title = ""
        self.description = ""
        self.headings: list[str] = []
        self.links: list[dict[str, str]] = []
        self.text_parts: list[str] = []
        self._ignored_depth = 0
        self._in_title = False
        self._heading_tag = ""
        self._heading_parts: list[str] = []
        self._link_href = ""
        self._link_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.lower()
        attr_map = {key.lower(): value or "" for key, value in attrs}
        if normalized in {"script", "style", "noscript", "template", "svg", "canvas"}:
            self._ignored_depth += 1
            return
        if self._ignored_depth > 0:
            return
        if normalized == "title":
            self._in_title = True
            return
        if normalized == "meta":
            name = (attr_map.get("name") or attr_map.get("property") or "").lower()
            if name in {"description", "og:description", "twitter:description"} and not self.description:
                self.description = _clean_text(attr_map.get("content", ""))
            return
        if normalized in {"h1", "h2", "h3"}:
            self._heading_tag = normalized
            self._heading_parts = []
            return
        if normalized == "a":
            href = attr_map.get("href", "").strip()
            if href:
                self._link_href = urljoin(self.base_url, href)
                self._link_parts = []

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in {"script", "style", "noscript", "template", "svg", "canvas"} and self._ignored_depth > 0:
            self._ignored_depth -= 1
            return
        if self._ignored_depth > 0:
            return
        if normalized == "title":
            self._in_title = False
            return
        if normalized == self._heading_tag:
            heading = _clean_text(" ".join(self._heading_parts))
            if heading:
                self.headings.append(heading)
            self._heading_tag = ""
            self._heading_parts = []
            return
        if normalized == "a" and self._link_href:
            label = _clean_text(" ".join(self._link_parts)) or self._link_href
            if label and len(self.links) < 30:
                self.links.append({"label": label[:160], "url": self._link_href})
            self._link_href = ""
            self._link_parts = []

    def handle_data(self, data: str) -> None:
        if self._ignored_depth > 0:
            return
        text = _clean_text(data)
        if not text:
            return
        if self._in_title:
            self.title = _clean_text(f"{self.title} {text}") if self.title else text
        if self._heading_tag:
            self._heading_parts.append(text)
        if self._link_href:
            self._link_parts.append(text)
        self.text_parts.append(text)


def read_web_page(url: str, *, question: str = "", max_chars: int = 6000) -> dict[str, Any]:
    safe_url = validate_public_http_url(url)
    response = _fetch_static_page(safe_url)
    content_type = response["contentType"]
    body = response["body"]
    text_limit = max(1000, min(int(max_chars), 20_000))

    if "html" in content_type or "<html" in body[:500].lower():
        parser = StaticPageParser(response["url"])
        parser.feed(body)
        text = _clean_text(" ".join(parser.text_parts))
        title = parser.title or response["url"]
        description = parser.description
        headings = _dedupe(parser.headings)[:20]
        links = _dedupe_links(parser.links)[:20]
    else:
        text = _clean_text(body)
        title = response["url"]
        description = ""
        headings = []
        links = []

    truncated = len(text) > text_limit or response["truncated"]
    limitations = [
        "Static HTTP fetch only; JavaScript-rendered content is not executed.",
        "No cookies, login state, form submission, or page interaction is used.",
    ]
    if question:
        limitations.append("Question is stored as retrieval context only; no browser action is inferred from it.")

    return {
        "status": "ok",
        "url": response["url"],
        "httpStatus": response["httpStatus"],
        "contentType": content_type,
        "title": title[:240],
        "description": description[:500],
        "headings": headings,
        "textPreview": text[:text_limit],
        "links": links,
        "truncated": truncated,
        "limitations": limitations,
    }


def _fetch_static_page(url: str) -> dict[str, Any]:
    current_url = url
    for _ in range(MAX_REDIRECTS + 1):
        try:
            response = requests.get(
                current_url,
                headers={"User-Agent": USER_AGENT, "Accept": "text/html,text/plain,application/xhtml+xml,*/*;q=0.8"},
                timeout=HTTP_TIMEOUT_SECONDS,
                stream=True,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            raise ValueError(f"Unable to read web page: {exc}") from exc
        if 300 <= response.status_code < 400 and response.headers.get("Location"):
            current_url = validate_public_http_url(urljoin(current_url, response.headers["Location"]))
            continue
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ValueError(f"Unable to read web page: HTTP {response.status_code}") from exc
        final_url = validate_public_http_url(getattr(response, "url", current_url) or current_url)
        body_bytes = bytearray()
        truncated = False
        try:
            for chunk in response.iter_content(chunk_size=32768):
                if not chunk:
                    continue
                body_bytes.extend(chunk)
                if len(body_bytes) > MAX_RESPONSE_BYTES:
                    del body_bytes[MAX_RESPONSE_BYTES:]
                    truncated = True
                    break
        except requests.RequestException as exc:
            raise ValueError(f"Unable to read web page body: {exc}") from exc
        encoding = response.encoding or "utf-8"
        body = bytes(body_bytes).decode(encoding, errors="replace")
        content_type = response.headers.get("Content-Type", "text/html").split(";")[0].strip().lower()
        if not _is_supported_content_type(content_type):
            raise ValueError(f"Unsupported content type for read_web_page: {content_type}")
        return {
            "url": final_url,
            "httpStatus": response.status_code,
            "contentType": content_type,
            "body": body,
            "truncated": truncated,
        }
    raise ValueError("Too many redirects while reading web page")


def validate_public_http_url(raw_url: str) -> str:
    value = str(raw_url or "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("read_web_page only supports http and https URLs")
    if parsed.username or parsed.password:
        raise ValueError("read_web_page does not allow credentials in URLs")
    host = parsed.hostname
    if not host:
        raise ValueError("read_web_page URL must include a host")
    normalized_host = host.lower().rstrip(".")
    if normalized_host == "localhost" or normalized_host.endswith(".localhost"):
        raise ValueError("read_web_page blocks localhost URLs")
    _validate_public_host(normalized_host, parsed.port or (443 if parsed.scheme == "https" else 80))
    return value


def _validate_public_host(host: str, port: int) -> None:
    try:
        direct_ip = ipaddress.ip_address(host)
        _validate_public_ip(direct_ip)
        return
    except ValueError:
        pass
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ValueError(f"Unable to resolve host for read_web_page: {host}") from exc
    if not infos:
        raise ValueError(f"Unable to resolve host for read_web_page: {host}")
    for info in infos:
        address = info[4][0]
        _validate_public_ip(ipaddress.ip_address(address))


def _validate_public_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> None:
    if not ip.is_global:
        raise ValueError("read_web_page blocks local, private, reserved, and non-public network addresses")


def _is_supported_content_type(content_type: str) -> bool:
    return content_type in {
        "",
        "text/html",
        "text/plain",
        "application/xhtml+xml",
        "application/xml",
        "text/xml",
    }


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


def _dedupe_links(values: list[dict[str, str]]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for value in values:
        url = value.get("url", "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        result.append({"label": _clean_text(value.get("label", ""))[:160], "url": url})
    return result
