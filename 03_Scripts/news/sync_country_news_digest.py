#!/usr/bin/env python3
"""Prefetch country news snapshots into PostgreSQL.

This keeps the Country Copilot request path off the public internet.
RSS/Atom remains the ingestion layer; Gemini is optional post-fetch
enrichment for digest summarization and tagging.
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import DATABASE_ENABLED, DATABASE_URL  # noqa: E402
from app.services import news_digest_service  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prefetch RSS/Atom country news and persist snapshots",
    )
    parser.add_argument(
        "--country",
        action="append",
        dest="countries",
        help="Country code or alias to refresh; repeat for multiple",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=news_digest_service.DEFAULT_NEWS_LIMIT,
        help="Max retained articles per country",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel country refresh workers",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and summarize only; do not write to the database",
    )
    parser.add_argument(
        "--no-gemini",
        action="store_true",
        help="Skip Gemini enrichment even when GEMINI_API_KEY is set",
    )
    return parser.parse_args()


def _resolve_targets(requested: list[str] | None) -> list[str]:
    if requested:
        ordered: list[str] = []
        seen: set[str] = set()
        for item in requested:
            normalized = str(item or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
        return ordered
    return [
        config.country_code
        for config in news_digest_service.list_country_news_configs()
    ]


def _refresh_one(
    country: str,
    *,
    limit: int,
    dry_run: bool,
    no_gemini: bool,
) -> tuple[str, dict[str, object]]:
    payload = news_digest_service.refresh_country_news(
        country,
        limit=limit,
        persist=not dry_run,
        enrich_with_gemini=False if no_gemini else None,
    )
    return country, payload


def main() -> int:
    args = _parse_args()
    if not args.dry_run and (not DATABASE_ENABLED or not DATABASE_URL):
        print(
            "[news-sync] database is not configured; set "
            "APP_DATABASE_ENABLED=true and APP_DATABASE_URL first",
        )
        return 1

    targets = _resolve_targets(args.countries)
    if not targets:
        print("[news-sync] no country configs found")
        return 1

    failures = 0
    max_workers = max(1, int(args.workers or 1))
    print(
        f"[news-sync] starting {len(targets)} country refresh tasks "
        f"with {max_workers} workers"
    )
    if args.dry_run:
        print("[news-sync] dry-run mode; snapshots will not be persisted")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _refresh_one,
                country,
                limit=max(1, args.limit),
                dry_run=args.dry_run,
                no_gemini=args.no_gemini,
            ): country
            for country in targets
        }
        for future in as_completed(futures):
            country = futures[future]
            try:
                _, payload = future.result()
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(
                    f"[news-sync] {country}: failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                continue

            digest = (
                payload.get("newsDigest")
                if isinstance(payload, dict)
                else None
            )
            events = (
                payload.get("marketEvents")
                if isinstance(payload, dict)
                else []
            )
            if not isinstance(digest, dict):
                print(f"[news-sync] {country}: no articles returned")
                continue
            provider = str(digest.get("summaryProvider") or "rss-fallback")
            stale = bool(digest.get("stale"))
            headline = str(digest.get("headline") or "").strip()
            article_count = len(events) if isinstance(events, list) else 0
            print(
                f"[news-sync] {country}: articles={article_count} "
                f"provider={provider} stale={str(stale).lower()} "
                f"headline={headline}"
            )

    if failures:
        print(f"[news-sync] completed with {failures} failure(s)")
        return 1

    print("[news-sync] completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
