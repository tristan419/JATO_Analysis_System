from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert

from app.core.config import PROJECT_ROOT
from app.db.models import VocRawDocument, VocSourceRun
from app.db.session import get_database_health, get_session_factory

VOC_RAW_ROOT = PROJECT_ROOT / "04_Processed_data" / "voc"


def _relative_to_project(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _load_json_file(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_voc_raw_sync_items(
    *,
    root: Path | None = None,
    country_filter: set[str] | None = None,
) -> list[dict[str, Any]]:
    target_root = root or VOC_RAW_ROOT
    if not target_root.exists():
        return []
    items: list[dict[str, Any]] = []
    for path in sorted(target_root.rglob("*.json")):
        if path.parent.name != "raw":
            continue
        payload = _load_json_file(path)
        if not isinstance(payload, dict):
            continue
        source = payload.get("source")
        source_payload = source if isinstance(source, dict) else {}
        country_code = str(
            source_payload.get("country_code") or path.parent.parent.name.upper(),
        ).strip().upper()
        if not country_code:
            continue
        if country_filter and country_code not in country_filter:
            continue
        collected_at = _parse_datetime(payload.get("collectedAt"))
        items.append(
            {
                "path": path,
                "source": source_payload,
                "countryCode": country_code,
                "countryLabel": str(
                    source_payload.get("country_label") or country_code,
                ).strip()
                or country_code,
                "collectedAt": collected_at,
                "payload": payload,
            }
        )
    return items


def sync_voc_raw_to_store(
    *,
    country_filter: set[str] | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    health = get_database_health()
    if not bool(health.get("enabled")) or not bool(health.get("connected")):
        raise RuntimeError("VOC staging sync requires a connected PostgreSQL database.")

    target_root = root or VOC_RAW_ROOT
    items = load_voc_raw_sync_items(root=target_root, country_filter=country_filter)
    session_factory = get_session_factory()
    synced_runs = 0
    synced_documents = 0
    synced_errors = 0
    countries: set[str] = set()

    with session_factory() as session:
        for item in items:
            payload = item["payload"]
            source = item["source"]
            collected_at = item.get("collectedAt") or datetime.fromtimestamp(
                item["path"].stat().st_mtime,
                UTC,
            )
            auto_review = payload.get("autoReview") or {}
            source_run_insert = insert(VocSourceRun).values(
                country_code=item["countryCode"],
                country_label=item["countryLabel"],
                source_code=str(source.get("source_code") or item["path"].stem),
                site_name=str(source.get("site_name") or item["path"].stem),
                site_type=str(source.get("site_type") or "unknown"),
                language=str(source.get("language") or "").strip() or None,
                taxonomy_profile=str(payload.get("taxonomyProfile") or "").strip() or None,
                collected_at_utc=collected_at,
                source_file_path=_relative_to_project(item["path"]),
                source_meta_json=source or None,
                landing_page_json=payload.get("landingPage"),
                collection_strategy_json=payload.get("collectionStrategy"),
                taxonomy_json=payload.get("taxonomy"),
                auto_review_json=auto_review or None,
                publish_tier=str(auto_review.get("publishTier") or "").strip() or None,
                publish_decision=str(auto_review.get("publishDecision") or "").strip() or None,
                candidate_count=int(
                    auto_review.get("candidateCount")
                    or (payload.get("landingPage") or {}).get("candidateCount")
                    or 0,
                ),
                document_count=int(payload.get("documentCount") or 0),
                publish_ready_count=int(auto_review.get("publishReadyCount") or 0),
                error_count=len(payload.get("errors") or []),
                errors_json=payload.get("errors") or [],
            )
            source_run_stmt = source_run_insert.on_conflict_do_update(
                index_elements=["source_code", "collected_at_utc"],
                set_={
                    "country_code": source_run_insert.excluded.country_code,
                    "country_label": source_run_insert.excluded.country_label,
                    "site_name": source_run_insert.excluded.site_name,
                    "site_type": source_run_insert.excluded.site_type,
                    "language": source_run_insert.excluded.language,
                    "taxonomy_profile": source_run_insert.excluded.taxonomy_profile,
                    "source_file_path": source_run_insert.excluded.source_file_path,
                    "source_meta_json": source_run_insert.excluded.source_meta_json,
                    "landing_page_json": source_run_insert.excluded.landing_page_json,
                    "collection_strategy_json": source_run_insert.excluded.collection_strategy_json,
                    "taxonomy_json": source_run_insert.excluded.taxonomy_json,
                    "auto_review_json": source_run_insert.excluded.auto_review_json,
                    "publish_tier": source_run_insert.excluded.publish_tier,
                    "publish_decision": source_run_insert.excluded.publish_decision,
                    "candidate_count": source_run_insert.excluded.candidate_count,
                    "document_count": source_run_insert.excluded.document_count,
                    "publish_ready_count": source_run_insert.excluded.publish_ready_count,
                    "error_count": source_run_insert.excluded.error_count,
                    "errors_json": source_run_insert.excluded.errors_json,
                    "updated_at_utc": collected_at,
                },
            ).returning(VocSourceRun.voc_source_run_id)
            source_run_id = session.execute(source_run_stmt).scalar_one()

            document_urls: list[str] = []
            for document in payload.get("documents") or []:
                if not isinstance(document, dict):
                    continue
                source_url = str(document.get("url") or "").strip()
                if not source_url:
                    continue
                document_urls.append(source_url)
                document_review = document.get("autoReview") or {}
                document_insert = insert(VocRawDocument).values(
                    voc_source_run_id=source_run_id,
                    country_code=str(document.get("countryCode") or item["countryCode"]),
                    country_label=str(document.get("countryLabel") or item["countryLabel"]),
                    source_code=str(document.get("sourceCode") or source.get("source_code") or ""),
                    site_name=str(document.get("siteName") or source.get("site_name") or ""),
                    site_type=str(document.get("siteType") or source.get("site_type") or ""),
                    language=str(document.get("language") or source.get("language") or "").strip() or None,
                    source_url=source_url,
                    title=str(document.get("title") or "").strip() or None,
                    page_kind=str(document.get("pageKind") or "").strip() or None,
                    link_text=str(document.get("linkText") or "").strip() or None,
                    published_at_utc=_parse_datetime(document.get("publishedAt")),
                    summary=str(document.get("summary") or "").strip() or None,
                    excerpt=str(document.get("excerpt") or "").strip() or None,
                    raw_text=str(document.get("rawText") or ""),
                    collected_at_utc=collected_at,
                    auto_review_json=document_review or None,
                    publish_tier=str(document_review.get("publishTier") or "").strip() or None,
                    publish_decision=str(document_review.get("publishDecision") or "").strip() or None,
                )
                session.execute(
                    document_insert.on_conflict_do_update(
                        index_elements=["voc_source_run_id", "source_url"],
                        set_={
                            "country_code": document_insert.excluded.country_code,
                            "country_label": document_insert.excluded.country_label,
                            "source_code": document_insert.excluded.source_code,
                            "site_name": document_insert.excluded.site_name,
                            "site_type": document_insert.excluded.site_type,
                            "language": document_insert.excluded.language,
                            "title": document_insert.excluded.title,
                            "page_kind": document_insert.excluded.page_kind,
                            "link_text": document_insert.excluded.link_text,
                            "published_at_utc": document_insert.excluded.published_at_utc,
                            "summary": document_insert.excluded.summary,
                            "excerpt": document_insert.excluded.excerpt,
                            "raw_text": document_insert.excluded.raw_text,
                            "collected_at_utc": document_insert.excluded.collected_at_utc,
                            "auto_review_json": document_insert.excluded.auto_review_json,
                            "publish_tier": document_insert.excluded.publish_tier,
                            "publish_decision": document_insert.excluded.publish_decision,
                            "updated_at_utc": collected_at,
                        },
                    )
                )

            if document_urls:
                session.execute(
                    delete(VocRawDocument).where(
                        VocRawDocument.voc_source_run_id == source_run_id,
                        VocRawDocument.source_url.notin_(document_urls),
                    )
                )
            else:
                session.execute(
                    delete(VocRawDocument).where(
                        VocRawDocument.voc_source_run_id == source_run_id,
                    )
                )

            synced_runs += 1
            synced_documents += int(payload.get("documentCount") or 0)
            synced_errors += len(payload.get("errors") or [])
            countries.add(item["countryCode"])

        session.commit()

    return {
        "root": _relative_to_project(target_root),
        "countryCount": len(countries),
        "sourceRunCount": synced_runs,
        "documentCount": synced_documents,
        "errorCount": synced_errors,
    }
