#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse


def build_headers(
    auth_token: str | None,
    user_role: str | None,
    user_name: str | None,
    *,
    json_body: bool = False,
    host_header: str | None = None,
) -> dict[str, str]:
    headers: dict[str, str] = {}
    if auth_token:
        headers["X-Auth-Token"] = auth_token
    if user_role:
        headers["X-User-Role"] = user_role
    if user_name:
        headers["X-User-Name"] = user_name
    if json_body:
        headers["Content-Type"] = "application/json"
    if host_header:
        headers["Host"] = host_header
    return headers


def resolve_api_base_url(
    app_base_url: str,
    api_base_url: str | None,
) -> str:
    if api_base_url:
        return api_base_url.rstrip("/")

    parsed = urlparse(app_base_url)
    if parsed.hostname in {"127.0.0.1", "localhost"} and parsed.port is None:
        return f"{parsed.scheme}://{parsed.hostname}:8000"
    return app_base_url.rstrip("/")


def timed_request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        method=method,
        headers=headers or {},
        data=body,
    )
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            payload = response.read()
            elapsed = time.perf_counter() - start
            return {
                "ok": True,
                "status": response.status,
                "elapsed_seconds": elapsed,
                "bytes": len(payload),
            }
    except urllib.error.HTTPError as error:
        elapsed = time.perf_counter() - start
        payload = error.read()
        return {
            "ok": False,
            "status": error.code,
            "elapsed_seconds": elapsed,
            "bytes": len(payload),
            "error": f"HTTPError: {error.code}",
        }
    except Exception as error:  # noqa: BLE001
        elapsed = time.perf_counter() - start
        return {
            "ok": False,
            "status": None,
            "elapsed_seconds": elapsed,
            "bytes": 0,
            "error": f"{type(error).__name__}: {error}",
        }


def print_result(label: str, result: dict[str, object]) -> None:
    status = result["status"] if result["status"] is not None else "ERR"
    elapsed = float(result["elapsed_seconds"])
    size_bytes = int(result["bytes"])
    suffix = (
        f"error={result['error']}"
        if not result["ok"] and result.get("error")
        else ""
    )
    message = (
        f"{label:28} status={status:<4} "
        f"time={elapsed:7.3f}s bytes={size_bytes:<8} {suffix}"
    )
    print(message.rstrip())


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Measure fullstack cold-start timing for page routes "
            "and MarketScan deck."
        )
    )
    parser.add_argument(
        "--app-base-url",
        default="http://127.0.0.1",
        help=(
            "Frontend base URL, e.g. http://127.0.0.1 "
            "or https://www.ojeur.cloud"
        ),
    )
    parser.add_argument(
        "--api-base-url",
        default=None,
        help="API base URL, defaults to app-base-url or localhost:8000",
    )
    parser.add_argument(
        "--auth-token",
        default="change-me",
        help="Viewer or editor token for protected API routes",
    )
    parser.add_argument(
        "--user-role",
        default="viewer",
        help="Role header for API requests",
    )
    parser.add_argument(
        "--user-name",
        default="coldstart-probe",
        help="User name header for API requests",
    )
    parser.add_argument(
        "--country",
        default=None,
        help="Optional market-scan country override",
    )
    parser.add_argument(
        "--target-period",
        default=None,
        help="Optional market-scan period override, e.g. 2026-03",
    )
    parser.add_argument(
        "--drilldown-segment",
        default="SUV A0",
        help="Market-scan drilldown segment",
    )
    parser.add_argument(
        "--page-host-header",
        default=None,
        help="Optional Host header for page requests via local nginx",
    )
    parser.add_argument(
        "--api-host-header",
        default=None,
        help="Optional Host header for API requests via local nginx",
    )
    args = parser.parse_args()

    app_base_url = args.app_base_url.rstrip("/")
    api_base_url = resolve_api_base_url(app_base_url, args.api_base_url)

    deck_payload = {
        "country": args.country,
        "target_period": args.target_period,
        "fuel_types": ["ICE", "MHEV", "HEV", "PHEV", "BEV", "LPG"],
        "trend_window_months": 24,
        "origin_window_months": 24,
        "body_window_months": 24,
        "ranking_limit": 6,
        "drilldown_segment": args.drilldown_segment,
    }
    deck_body = json.dumps(deck_payload).encode("utf-8")
    json_headers = build_headers(
        args.auth_token,
        args.user_role,
        args.user_name,
        json_body=True,
        host_header=args.api_host_header,
    )
    page_headers = build_headers(
        None,
        None,
        None,
        host_header=args.page_host_header,
    )

    print("== Page cold-start ==")
    print_result(
        "root-first",
        timed_request(f"{app_base_url}/", headers=page_headers),
    )
    print_result(
        "root-second",
        timed_request(f"{app_base_url}/", headers=page_headers),
    )
    print_result(
        "specification-first",
        timed_request(
            f"{app_base_url}/specification",
            headers=page_headers,
        ),
    )
    print_result(
        "market-scan-first",
        timed_request(
            f"{app_base_url}/market-scan",
            headers=page_headers,
        ),
    )

    print("\n== API cold-start ==")
    print_result(
        "healthz",
        timed_request(
            f"{api_base_url}/healthz",
            headers=build_headers(
                args.auth_token,
                args.user_role,
                args.user_name,
                host_header=args.api_host_header,
            ),
        ),
    )
    print_result(
        "market-scan-deck-first",
        timed_request(
            f"{api_base_url}/v1/market-scan/deck",
            method="POST",
            headers=json_headers,
            body=deck_body,
        ),
    )
    print_result(
        "market-scan-deck-second",
        timed_request(
            f"{api_base_url}/v1/market-scan/deck",
            method="POST",
            headers=json_headers,
            body=deck_body,
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
