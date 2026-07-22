#!/usr/bin/env python3
"""Read-only backfill / cleanup preview for XC60 review cases.

This script:
1. Reports proposed values for NULL structured fields (jato_powertrain,
   official_edition, official_powertrain) on existing XC60
   observations, current_prices, and review_cases by re-running
   the powertrain/edition evidence engine on stored trim text.
2. Reports open review_cases that have been superseded by a
   newer observation for the same (country, brand, jato_model,
   jato_trim) key.

Usage:
  cd 06_AppPlatform/backend
  python ../../03_Scripts/backfill_xc60_structured_fields.py \
      --dry-run          # preview only
  # --commit is fail-closed until an approval-gated compensation path exists.

Requires APP_DATABASE_URL env var or defaults to local dev DB.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

FACT_WRITE_DISABLED_MESSAGE = (
    "--commit is disabled: CurrentPrice changes require the approval-gated "
    "materialization/compensation path"
)


def require_read_only_mode(*, commit: bool) -> None:
    if commit:
        raise RuntimeError(FACT_WRITE_DISABLED_MESSAGE)

# ── canonical JATO powertrain types ──────────────
CANONICAL_POWERTRAINS = frozenset(
    {
        "BEV",
        "FCV",
        "HEV",
        "ICE",
        "LPG",
        "MHEV",
        "PHEV",
        "REEV",
    }
)

# ── evidence rules (Volvo XC60 specific) ─────────
_PHEV_KEYWORDS = (
    "plug-in hybrid",
    "plugin hybrid",
    "laddhybrid",
    "recharge",
    "xc60-hybrid",
    "t6",
    "t8",
)
_MHEV_KEYWORDS = (
    "mild hybrid",
    "mhev",
    "b5",
    "b6",
)
_EDITION_RE = re.compile(
    r"\b(black\s+edition|nordic\s+edition"
    r"|first\s+edition|launch\s+edition"
    r"|limited\s+edition|special\s+edition)\b",
    re.IGNORECASE,
)
_SPECIAL_EDITIONS = frozenset(
    {
        "black edition",
        "first edition",
        "launch edition",
        "limited edition",
        "special edition",
    }
)


def _detect_powertrain(
    search_text: str,
) -> str | None:
    low = search_text.lower()
    for kw in _PHEV_KEYWORDS:
        if kw in low:
            return "PHEV"
    for kw in _MHEV_KEYWORDS:
        if kw in low:
            return "MHEV"
    return None


def _detect_edition(trim: str) -> str | None:
    labels: list[str] = []
    for m in _EDITION_RE.finditer(trim):
        labels.append(m.group(1).title())
    return " | ".join(labels) or None


def _db_url() -> str:
    return os.getenv(
        "APP_DATABASE_URL",
        "postgresql+psycopg://postgres:postgres"
        "@localhost:5432/jato_app",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Preview structured-field backfill on XC60 "
            "observations / review_cases (read-only)"
        ),
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Disabled until an approval-gated compensation path is implemented.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only, do not commit",
    )
    parser.add_argument(
        "--brand",
        default="Volvo",
        help="Brand filter (default: Volvo)",
    )
    parser.add_argument(
        "--model",
        default="XC60",
        help="JATO model filter (default: XC60)",
    )
    args = parser.parse_args()

    if args.commit and args.dry_run:
        parser.error(
            "--commit and --dry-run are mutually exclusive"
        )
    try:
        require_read_only_mode(commit=args.commit)
    except RuntimeError as exc:
        parser.error(str(exc))

    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        log.error(
            "sqlalchemy not found; run inside the "
            "backend venv"
        )
        sys.exit(1)

    engine = create_engine(_db_url())

    with engine.connect() as conn:
        # ── Step 1: backfill structured fields ───
        rows = conn.execute(
            text(
                "SELECT observation_id,"
                " official_trim, source_url "
                "FROM msrp.observations "
                "WHERE brand = :brand "
                "  AND jato_model = :model "
                "  AND (jato_powertrain IS NULL "
                "   OR official_powertrain IS NULL)"
            ),
            {"brand": args.brand, "model": args.model},
        ).fetchall()

        log.info(
            "Found %d observations to backfill", len(rows)
        )

        obs_updated = 0
        for row in rows:
            obs_id = row[0]
            trim = str(row[1] or "")
            url = str(row[2] or "")
            search = f"{url} {trim}".lower()
            pt = _detect_powertrain(search)
            ed = _detect_edition(trim)

            if pt is None and ed is None:
                continue

            sets = []
            if pt:
                sets.append(
                    "jato_powertrain = :pt, "
                    "official_powertrain = :pt"
                )
            if ed:
                sets.append("official_edition = :ed")

            log.info(
                "  obs %s  trim=%s  pt=%s  ed=%s",
                obs_id,
                trim,
                pt,
                ed,
            )
            obs_updated += 1

        # ── propagate to current_prices ──────────
        cp_rows = conn.execute(
            text(
                "SELECT cp.current_price_id, "
                "  o.jato_powertrain, "
                "  o.official_powertrain, "
                "  o.official_edition "
                "FROM msrp.current_prices cp "
                "JOIN msrp.observations o "
                "  ON o.observation_id "
                "  = cp.effective_observation_id "
                "WHERE cp.brand = :brand "
                "  AND cp.jato_model = :model "
                "  AND (cp.jato_powertrain IS NULL "
                "   OR cp.official_powertrain IS NULL)"
            ),
            {"brand": args.brand, "model": args.model},
        ).fetchall()

        cp_updated = 0
        for row in cp_rows:
            cpid, jp, op, oe = row
            sets = []
            if jp:
                sets.append("jato_powertrain = :jp")
            if op:
                sets.append(
                    "official_powertrain = :op"
                )
            if oe:
                sets.append("official_edition = :oe")
            if not sets:
                continue
            cp_updated += 1

        # ── propagate to review_cases ────────────
        rc_rows = conn.execute(
            text(
                "SELECT rc.review_case_id, "
                "  o.jato_powertrain, "
                "  o.official_powertrain, "
                "  o.official_edition "
                "FROM review.review_cases rc "
                "JOIN msrp.observations o "
                "  ON o.observation_id "
                "  = rc.observation_id "
                "WHERE rc.brand = :brand "
                "  AND rc.jato_model = :model "
                "  AND (rc.jato_powertrain IS NULL "
                "   OR rc.official_powertrain IS NULL)"
            ),
            {"brand": args.brand, "model": args.model},
        ).fetchall()

        rc_updated = 0
        for row in rc_rows:
            rcid, jp, op, oe = row
            sets = []
            if jp:
                sets.append("jato_powertrain = :jp")
            if op:
                sets.append(
                    "official_powertrain = :op"
                )
            if oe:
                sets.append("official_edition = :oe")
            if not sets:
                continue
            rc_updated += 1

        # ── Step 2: close superseded review cases ─
        superseded = conn.execute(
            text(
                "WITH latest AS ( "
                "  SELECT DISTINCT ON "
                "    (country, brand, "
                "     jato_model, jato_trim) "
                "    observation_id "
                "  FROM msrp.observations "
                "  WHERE brand = :brand "
                "    AND jato_model = :model "
                "  ORDER BY country, brand, "
                "    jato_model, jato_trim, "
                "    observed_at_utc DESC "
                ") "
                "SELECT rc.review_case_id "
                "FROM review.review_cases rc "
                "WHERE rc.brand = :brand "
                "  AND rc.jato_model = :model "
                "  AND rc.review_status = 'open' "
                "  AND rc.observation_id "
                "    NOT IN (SELECT observation_id "
                "            FROM latest)"
            ),
            {"brand": args.brand, "model": args.model},
        ).fetchall()

        closed = 0
        for row in superseded:
            rcid = row[0]
            log.info(
                "  closing superseded case %s", rcid
            )
            closed += 1

        log.info("─── Summary ───")
        log.info(
            "Observations backfilled: %d", obs_updated
        )
        log.info(
            "Current prices propagated: %d", cp_updated
        )
        log.info(
            "Review cases propagated: %d", rc_updated
        )
        log.info("Superseded cases closed: %d", closed)
        log.info(
            "READ-ONLY DRY RUN — no changes committed; fact writes require "
            "the approval-gated compensation path."
        )


if __name__ == "__main__":
    main()
