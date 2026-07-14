"""Shared fail-closed validators for remote production audit evidence."""

from __future__ import annotations

import ipaddress
from typing import Any
from urllib.parse import urlparse


def is_external_https_api(api_base: str) -> bool:
    """Return true only for a non-local HTTPS API origin."""
    parsed = urlparse(api_base)
    hostname = str(parsed.hostname or "").lower().rstrip(".")
    if parsed.scheme != "https" or not hostname:
        return False
    if (
        hostname in {"local", "localhost"}
        or hostname.endswith(".local")
        or hostname.endswith(".localhost")
    ):
        return False
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return True
    return not (
        address.is_loopback
        or address.is_private
        or address.is_link_local
        or address.is_reserved
        or address.is_unspecified
    )


def validate_deploy_status(
    payload: Any,
    *,
    http_status: int | None,
) -> dict[str, Any]:
    """Validate deploy API evidence without local metadata fallbacks."""
    deploy = payload if isinstance(payload, dict) else {}
    release = deploy.get("release") if isinstance(deploy.get("release"), dict) else {}
    drift = deploy.get("drift") if isinstance(deploy.get("drift"), dict) else {}
    deployed_commit = str(
        release.get("actualCommitSha") or release.get("commitSha") or ""
    ).strip() or None
    metadata_valid = bool(
        http_status == 200
        and deploy.get("status") == "ok"
        and str(release.get("environment") or "").lower() == "production"
        and str(release.get("branch") or "").lower() == "main"
        and release.get("source") != "git_metadata_fallback"
        and drift.get("isDrift") is False
        and deployed_commit
    )
    return {
        "deployedCommit": deployed_commit,
        "metadataValid": metadata_valid,
        "status": deploy.get("status"),
        "environment": release.get("environment"),
        "branch": release.get("branch"),
        "isDrift": drift.get("isDrift"),
    }
