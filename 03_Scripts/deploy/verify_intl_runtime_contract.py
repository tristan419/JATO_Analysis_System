#!/usr/bin/env python3
"""Fail-closed runtime contract checks for public JATO frontends."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import sys
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


MAX_RESPONSE_BYTES = 1_048_576
FRESHNESS_PATH = "/v1/analysis/data-freshness"
ALLOWED_EDGE_CACHE_STATES = frozenset({"MISS", "HIT", "STALE", "BYPASS"})
PROFILES = ("intl", "www")


class ContractValidationError(RuntimeError):
    """Raised when the deployed runtime does not satisfy the intl contract."""


@dataclass(frozen=True)
class JsonResponse:
    path: str
    status: int
    headers: Mapping[str, str]
    payload: Any


Validator = Callable[[JsonResponse], None]


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def normalize_origin(origin: str) -> str:
    normalized = origin.strip().rstrip("/")
    parsed = urlsplit(normalized)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ContractValidationError(
            "origin must be an http(s) site origin without a path, query, or fragment"
        )
    return normalized


def _header(response: JsonResponse, name: str) -> str:
    return str(response.headers.get(name.lower(), "")).strip()


def _body_preview(body: bytes) -> str:
    return body[:160].decode("utf-8", errors="replace").replace("\n", " ").strip()


def fetch_json(origin: str, path: str, timeout_seconds: float) -> JsonResponse:
    request = Request(
        f"{origin}{path}",
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "jato-intl-runtime-contract/1.0",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status = int(response.status)
            headers = {
                str(name).lower(): str(value)
                for name, value in response.headers.items()
            }
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as error:
        raise ContractValidationError(f"{path}: HTTP {error.code}") from error
    except (URLError, TimeoutError, OSError) as error:
        raise ContractValidationError(
            f"{path}: request failed ({type(error).__name__}: {error})"
        ) from error

    if status != 200:
        raise ContractValidationError(f"{path}: expected HTTP 200, got {status}")
    if len(body) > MAX_RESPONSE_BYTES:
        raise ContractValidationError(
            f"{path}: response exceeds {MAX_RESPONSE_BYTES} bytes"
        )

    content_type = headers.get("content-type", "")
    media_type = content_type.split(";", 1)[0].strip().lower()
    if media_type != "application/json":
        raise ContractValidationError(
            f"{path}: expected application/json, got {content_type or '<missing>'}; "
            f"body starts with {_body_preview(body)!r}"
        )

    try:
        payload = json.loads(body.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractValidationError(f"{path}: invalid JSON ({error})") from error

    return JsonResponse(path=path, status=status, headers=headers, payload=payload)


def _require_object(response: JsonResponse) -> Mapping[str, Any]:
    if not isinstance(response.payload, dict):
        raise ContractValidationError(
            f"{response.path}: expected a JSON object payload"
        )
    return response.payload


def validate_health(response: JsonResponse) -> None:
    payload = _require_object(response)
    if payload.get("status") != "ok":
        raise ContractValidationError(
            f"{response.path}: expected payload status='ok'"
        )
    marker = _header(response, "x-jato-edge-proxy")
    if marker != "healthz":
        raise ContractValidationError(
            f"{response.path}: expected x-jato-edge-proxy=healthz, "
            f"got {marker or '<missing>'}"
        )
    cache_control = _header(response, "cache-control").lower()
    if "no-store" not in {
        directive.split("=", 1)[0].strip()
        for directive in cache_control.split(",")
        if directive.strip()
    }:
        raise ContractValidationError(
            f"{response.path}: expected Cache-Control to include no-store, "
            f"got {cache_control or '<missing>'}"
        )


def validate_www_health(response: JsonResponse) -> None:
    payload = _require_object(response)
    if payload.get("status") != "ok":
        raise ContractValidationError(
            f"{response.path}: expected payload status='ok'"
        )


def validate_www_freshness(response: JsonResponse) -> None:
    payload = _require_object(response)
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ContractValidationError(
            f"{response.path}: expected payload items to be a non-empty list"
        )


def validate_freshness(response: JsonResponse) -> None:
    payload = _require_object(response)
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ContractValidationError(
            f"{response.path}: expected payload items to be a non-empty list"
        )

    endpoint = _header(response, "x-jato-edge-cache-endpoint")
    if endpoint != FRESHNESS_PATH:
        raise ContractValidationError(
            f"{response.path}: expected x-jato-edge-cache-endpoint={FRESHNESS_PATH}, "
            f"got {endpoint or '<missing>'}"
        )

    cache_state = _header(response, "x-jato-edge-cache").upper()
    if cache_state not in ALLOWED_EDGE_CACHE_STATES:
        allowed = "/".join(sorted(ALLOWED_EDGE_CACHE_STATES))
        raise ContractValidationError(
            f"{response.path}: expected x-jato-edge-cache in {allowed}, "
            f"got {cache_state or '<missing>'}"
        )


def validate_oauth_relay_health(response: JsonResponse) -> None:
    payload = _require_object(response)
    if payload.get("status") != "ok":
        raise ContractValidationError(
            f"{response.path}: expected payload status='ok'"
        )
    cache_control = _header(response, "cache-control")
    directives = {
        directive.split("=", 1)[0].strip().lower()
        for directive in cache_control.split(",")
        if directive.strip()
    }
    if "no-store" not in directives:
        raise ContractValidationError(
            f"{response.path}: expected Cache-Control to include no-store, "
            f"got {cache_control or '<missing>'}"
        )


CHECKS: tuple[tuple[str, Validator], ...] = (
    ("/healthz", validate_health),
    (FRESHNESS_PATH, validate_freshness),
    ("/oauth-relay/healthz", validate_oauth_relay_health),
)
WWW_CHECKS: tuple[tuple[str, Validator], ...] = (
    ("/healthz", validate_www_health),
    (FRESHNESS_PATH, validate_www_freshness),
)
CHECKS_BY_PROFILE: Mapping[str, tuple[tuple[str, Validator], ...]] = {
    "intl": CHECKS,
    "www": WWW_CHECKS,
}


def verify_once(
    origin: str,
    timeout_seconds: float,
    *,
    profile: str = "intl",
) -> tuple[JsonResponse, ...]:
    try:
        checks = CHECKS_BY_PROFILE[profile]
    except KeyError as error:
        raise ValueError(f"unsupported runtime contract profile: {profile}") from error
    responses: list[JsonResponse] = []
    errors: list[str] = []
    for path, validator in checks:
        try:
            response = fetch_json(origin, path, timeout_seconds)
            validator(response)
            responses.append(response)
        except ContractValidationError as error:
            errors.append(str(error))
    if errors:
        raise ContractValidationError("; ".join(errors))
    return tuple(responses)


def verify_runtime_contract(
    origin: str,
    *,
    attempts: int,
    delay_seconds: float,
    timeout_seconds: float,
    profile: str = "intl",
    sleeper: Callable[[float], None] = time.sleep,
) -> tuple[JsonResponse, ...]:
    normalized_origin = normalize_origin(origin)
    if attempts <= 0:
        raise ValueError("attempts must be greater than zero")
    if delay_seconds < 0:
        raise ValueError("delay_seconds must be zero or greater")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero")
    if profile not in CHECKS_BY_PROFILE:
        raise ValueError(f"unsupported runtime contract profile: {profile}")

    failures: list[str] = []
    for attempt in range(1, attempts + 1):
        try:
            return verify_once(
                normalized_origin,
                timeout_seconds,
                profile=profile,
            )
        except ContractValidationError as error:
            failures.append(f"attempt {attempt}/{attempts}: {error}")
            if attempt < attempts:
                sleeper(delay_seconds)

    raise ContractValidationError(
        f"{profile} runtime contract failed after {attempts} attempt(s): "
        + " | ".join(failures)
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--origin",
        required=True,
        help="Public site origin, for example https://intl.ojeur.cloud",
    )
    parser.add_argument(
        "--profile",
        choices=PROFILES,
        default="intl",
        help="Runtime contract profile (default: intl)",
    )
    parser.add_argument(
        "--attempts",
        type=_positive_int,
        default=12,
        help="Maximum contract attempts (default: 12)",
    )
    parser.add_argument(
        "--delay-seconds",
        type=_non_negative_float,
        default=5.0,
        help="Delay between attempts (default: 5)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=_positive_float,
        default=15.0,
        help="Timeout for each request (default: 15)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        responses = verify_runtime_contract(
            args.origin,
            attempts=args.attempts,
            delay_seconds=args.delay_seconds,
            timeout_seconds=args.timeout_seconds,
            profile=args.profile,
        )
    except (ContractValidationError, ValueError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1

    checked_paths = ", ".join(response.path for response in responses)
    print(
        f"PASS: {args.profile} runtime contract satisfied for "
        f"{args.origin}: {checked_paths}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
