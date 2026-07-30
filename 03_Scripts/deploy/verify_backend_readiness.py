#!/usr/bin/env python3
"""Verify backend readiness and immutable release identity without dependencies."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener


MAX_RESPONSE_BYTES = 64 * 1024
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class ReadinessError(RuntimeError):
    """A fail-closed backend readiness contract violation."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        observed_status: object = None,
        observed_commit: object = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.observed_status = observed_status
        self.observed_commit = observed_commit


def _read_payload(url: str, timeout_seconds: float) -> Mapping[str, Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "jato-backend-readiness-gate/1",
        },
    )
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        raise ReadinessError(
            "http_error",
            f"readyz returned HTTP {exc.code}",
        ) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ReadinessError(
            "request_failed",
            f"readyz request failed: {exc}",
        ) from exc

    if len(body) > MAX_RESPONSE_BYTES:
        raise ReadinessError(
            "response_too_large",
            f"readyz response exceeds {MAX_RESPONSE_BYTES} bytes",
        )
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReadinessError("invalid_json", "readyz response is not valid UTF-8 JSON") from exc
    if not isinstance(payload, Mapping):
        raise ReadinessError("invalid_payload", "readyz response must be a JSON object")
    return payload


def verify_backend_readiness(
    *,
    url: str,
    expected_commit: str,
    timeout_seconds: float,
) -> dict[str, object]:
    """Return the safe subset of a valid readiness payload or raise."""

    if not SHA_PATTERN.fullmatch(expected_commit):
        raise ReadinessError(
            "invalid_expected_commit",
            "expected commit must be a full lowercase git SHA",
        )
    if timeout_seconds <= 0:
        raise ReadinessError(
            "invalid_timeout",
            "timeout seconds must be positive",
        )

    payload = _read_payload(url, timeout_seconds)
    status = payload.get("status")
    release = payload.get("release")
    observed_commit = release.get("commitSha") if isinstance(release, Mapping) else None

    if status != "ready":
        raise ReadinessError(
            "status_not_ready",
            "readyz status is not ready",
            observed_status=status,
            observed_commit=observed_commit,
        )
    if not isinstance(release, Mapping):
        raise ReadinessError(
            "release_missing",
            "readyz release must be a JSON object",
            observed_status=status,
        )
    if observed_commit != expected_commit:
        raise ReadinessError(
            "commit_mismatch",
            "readyz release.commitSha does not match the target release",
            observed_status=status,
            observed_commit=observed_commit,
        )

    return {
        "status": status,
        "release": {"commitSha": observed_commit},
    }


def _log_payload(
    *,
    ok: bool,
    url: str,
    expected_commit: str,
    observed: Mapping[str, object] | None = None,
    error: ReadinessError | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "check": "backend_readyz",
        "ok": ok,
        "url": url,
        "expectedCommitSha": expected_commit,
    }
    if observed is not None:
        payload["observed"] = observed
    if error is not None:
        payload["observed"] = {
            "status": error.observed_status,
            "release": {"commitSha": error.observed_commit},
        }
        payload["error"] = {
            "code": error.code,
            "message": str(error),
        }
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail closed unless /readyz reports the target release as ready.",
    )
    parser.add_argument("--url", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser


def main() -> int:
    arguments = _build_parser().parse_args()
    try:
        observed = verify_backend_readiness(
            url=arguments.url,
            expected_commit=arguments.expected_commit,
            timeout_seconds=arguments.timeout_seconds,
        )
    except ReadinessError as exc:
        print(
            json.dumps(
                _log_payload(
                    ok=False,
                    url=arguments.url,
                    expected_commit=arguments.expected_commit,
                    error=exc,
                ),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1

    print(
        json.dumps(
            _log_payload(
                ok=True,
                url=arguments.url,
                expected_commit=arguments.expected_commit,
                observed=observed,
            ),
            sort_keys=True,
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
