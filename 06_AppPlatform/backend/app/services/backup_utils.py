"""Lightweight backup utility for pre-mutation safety nets.

Fires pg_dump for specific schemas before high-risk operations.
Failures are logged but do NOT block the caller — a failed dump
should never prevent a publish.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from app.core.config import DATABASE_URL

_log = logging.getLogger(__name__)

BACKUP_ROOT = Path("/opt/backups/jato/pre_mutation")


def _normalize_pg_dump_url(db_url: str) -> str:
    """Strip async driver prefixes so pg_dump can connect."""
    parsed = urlparse(db_url)
    if "+asyncpg" in parsed.scheme or "+psycopg" in parsed.scheme or "+aiopg" in parsed.scheme:
        parsed = parsed._replace(scheme=parsed.scheme.split("+")[0])
    return urlunparse(parsed)


def _pg_dump_url_components(db_url: str) -> tuple[str, str, str, str]:
    """Return (host, port, dbname, user) extracted from a postgresql:// URL."""
    parsed = urlparse(db_url)
    host = parsed.hostname or "127.0.0.1"
    port = str(parsed.port or 5432)
    dbname = (parsed.path or "/postgres").lstrip("/")
    user = parsed.username or "postgres"
    return host, port, dbname, user


def backup_ordering_schema(trigger: str) -> str | None:
    """pg_dump the ordering schema before a risky mutation.

    Returns the backup file path on success, or None on failure.
    """
    if not DATABASE_URL:
        _log.warning("DATABASE_URL not configured; skipping pre-mutation backup")
        return None

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    safe_trigger = trigger.replace(" ", "_")
    out_path = BACKUP_ROOT / f"ordering-pre-{safe_trigger}-{ts}.dump"

    try:
        BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        _log.warning("Cannot create backup dir %s (permission denied); skipping backup", BACKUP_ROOT)
        return None
    except OSError as exc:
        _log.warning("Cannot create backup dir %s: %s; skipping backup", BACKUP_ROOT, exc)
        return None

    db_url = _normalize_pg_dump_url(DATABASE_URL)
    host, port, dbname, user = _pg_dump_url_components(db_url)
    password = urlparse(db_url).password

    cmd = [
        "pg_dump",
        "--host", host,
        "--port", port,
        "--username", user,
        "--dbname", dbname,
        "--schema=ordering",
        "--format=c",
        "--file", str(out_path),
        "--no-password",
    ]

    env = {**__import__("os").environ, "PGPASSWORD": password or ""}

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, env=env, timeout=60)
        _log.info("pre-mutation backup saved: %s", out_path)
        return str(out_path)
    except subprocess.CalledProcessError as exc:
        _log.error("pg_dump failed (trigger=%s): %s", trigger, exc.stderr.strip())
        return None
    except Exception:
        _log.exception("pg_dump unexpected error (trigger=%s)", trigger)
        return None
