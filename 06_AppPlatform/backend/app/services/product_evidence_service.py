"""Read-only Product Configuration evidence for MCP consumers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
import re
from typing import Any, Iterable
from uuid import UUID

from sqlalchemy.orm import Session

from app.db.models import ConfigVersion, EngineeringConfigSourceContextLink, ImportBatch, VehicleTrim
from app.infra import engineering_config_repository as repo
from app.services.config_text_normalization import (
    config_feature_semantic_keys,
    normalize_config_value_for_compare,
)


MAX_PUBLISHED_TRIMS = 500
_COUNTRY_ALIASES = {
    "ch": "switzerland",
    "che": "switzerland",
    "schweiz": "switzerland",
    "suisse": "switzerland",
    "svizzera": "switzerland",
    "瑞士": "switzerland",
    "se": "sweden",
    "swe": "sweden",
    "sverige": "sweden",
    "瑞典": "sweden",
}


@dataclass(frozen=True)
class PublishedConfigRecord:
    trim: VehicleTrim
    version: ConfigVersion
    source_batch: ImportBatch | None
    source_contexts: tuple[EngineeringConfigSourceContextLink, ...]


def search_product_evidence(
    session: Session,
    *,
    country: str,
    query: str,
    subjects: list[str] | None = None,
    source_roles: list[str] | None = None,
    document_types: list[str] | None = None,
    features: list[str] | None = None,
    effective_date: str = "",
    limit: int = 12,
) -> dict[str, Any]:
    requested_country = _required_country(country)
    requested_subjects = _clean_list(subjects)
    requested_roles = {_normalise_text(item) for item in _clean_list(source_roles)}
    requested_document_types = {_normalise_text(item) for item in _clean_list(document_types)}
    requested_features = _clean_list(features)
    requested_effective_date = _parse_date(effective_date)
    retrieved_at = datetime.now(timezone.utc)

    items: list[dict[str, Any]] = []
    matched_documents: set[str] = set()
    for record in _load_published_records(session):
        if not _matches_country(record, requested_country):
            continue
        if requested_subjects and not any(_matches_subject(record, subject) for subject in requested_subjects):
            continue
        source_role = _source_role(record)
        if requested_roles and _normalise_text(source_role) not in requested_roles:
            continue
        document_type = _document_type(record)
        if requested_document_types and _normalise_text(document_type) not in requested_document_types:
            continue
        if requested_effective_date and not _is_effective(record, requested_effective_date):
            continue

        query_matches_identity = _query_matches_identity(record, query)
        for snapshot_item in _snapshot_items(record.version):
            if not _matches_snapshot_scope(
                record,
                snapshot_item,
                query=query,
                features=requested_features,
            ):
                continue
            if requested_features and not _matches_any_feature(snapshot_item, requested_features):
                continue
            if query.strip() and not query_matches_identity and not _matches_feature(snapshot_item, query):
                continue
            evidence_ref = _evidence_ref(
                record,
                snapshot_item,
                country=requested_country,
                retrieved_at=retrieved_at,
            )
            items.append(evidence_ref)
            matched_documents.add(evidence_ref["evidenceDocumentId"])
            if len(items) >= max(1, min(int(limit), 50)):
                break
        if len(items) >= max(1, min(int(limit), 50)):
            break

    if not items:
        return {
            "status": "insufficient_evidence",
            "items": [],
            "evidenceRefs": [],
            "summary": "No exact-scope published Product Configuration evidence matched the request.",
            "missingEvidence": [
                {
                    "name": "published_product_evidence",
                    "reason": f"No published evidence is available for {requested_country}.",
                    "impact": "blocking",
                }
            ],
            "coverageDiagnostics": {
                "diagnosis": "published_product_evidence_not_found",
                "country": requested_country,
                "subjects": requested_subjects,
            },
        }
    return {
        "status": "ok",
        "items": items,
        "evidenceRefs": items,
        "summary": (
            f"Found {len(items)} published evidence references from "
            f"{len(matched_documents)} governed source document(s)."
        ),
        "missingEvidence": [],
        "coverageDiagnostics": {
            "diagnosis": "published_product_evidence_ready",
            "country": requested_country,
            "documentCount": len(matched_documents),
            "evidenceRefCount": len(items),
        },
    }


def compare_published_product_configs(
    session: Session,
    *,
    country: str,
    subject: str,
    competitors: list[str] | None = None,
    features: list[str] | None = None,
    powertrain: str = "",
    effective_date: str = "",
) -> dict[str, Any]:
    requested_country = _required_country(country)
    requested_subjects = [subject.strip(), *_clean_list(competitors)]
    requested_subjects = [item for item in requested_subjects if item]
    if len(requested_subjects) < 2:
        return _insufficient_compare(
            requested_country,
            requested_subjects,
            ["A comparison requires one subject and at least one competitor."],
        )

    requested_effective_date = _parse_date(effective_date)
    records = [
        record
        for record in _load_published_records(session)
        if _matches_country(record, requested_country)
        and (not powertrain.strip() or _matches_powertrain(record, powertrain))
        and (not requested_effective_date or _is_effective(record, requested_effective_date))
    ]
    selected: list[tuple[str, PublishedConfigRecord]] = []
    missing_subjects: list[str] = []
    used_versions: set[UUID] = set()
    for requested_subject in requested_subjects:
        candidates = [
            record
            for record in records
            if record.version.version_id not in used_versions
            and _matches_subject(record, requested_subject)
        ]
        if not candidates:
            missing_subjects.append(requested_subject)
            continue
        selected_record = max(candidates, key=lambda item: _subject_match_score(item, requested_subject))
        selected.append((requested_subject, selected_record))
        used_versions.add(selected_record.version.version_id)

    if missing_subjects:
        return _insufficient_compare(
            requested_country,
            requested_subjects,
            [f"No exact-scope published configuration was found for {name}." for name in missing_subjects],
        )

    requested_features = _clean_list(features)
    feature_maps = [
        {
            _feature_identity(item): item
            for item in _snapshot_items(record.version)
            if not requested_features or _matches_any_feature(item, requested_features)
        }
        for _, record in selected
    ]
    feature_ids = sorted(set().union(*(feature_map.keys() for feature_map in feature_maps)))
    retrieved_at = datetime.now(timezone.utc)
    common_features: list[dict[str, Any]] = []
    different_features: list[dict[str, Any]] = []
    evidence_refs: list[dict[str, Any]] = []

    for feature_id in feature_ids:
        cells: list[dict[str, Any] | None] = []
        comparable_values: list[str] = []
        feature_label = feature_id
        feature_code = feature_id
        category = "Unknown"
        for (requested_subject, record), feature_map in zip(selected, feature_maps, strict=True):
            snapshot_item = feature_map.get(feature_id)
            if snapshot_item is None:
                cells.append(None)
                comparable_values.append("")
                continue
            evidence_ref = _evidence_ref(
                record,
                snapshot_item,
                country=requested_country,
                retrieved_at=retrieved_at,
            )
            evidence_refs.append(evidence_ref)
            feature_label = str(snapshot_item.get("featureName") or snapshot_item.get("featureCode") or feature_id)
            feature_code = str(snapshot_item.get("featureCode") or feature_id)
            category = str(snapshot_item.get("category") or "Unknown")
            value = _snapshot_display_value(snapshot_item)
            comparable_values.append(
                normalize_config_value_for_compare(value)
                or _normalise_text(value)
            )
            cells.append(
                {
                    "subject": requested_subject,
                    "value": value,
                    "rawValue": snapshot_item.get("rawValue"),
                    "normalizedValue": snapshot_item.get("normalizedValue"),
                    "availability": snapshot_item.get("availability"),
                    "unit": snapshot_item.get("unit"),
                    "evidenceRef": evidence_ref,
                }
            )
        populated = [value for value in comparable_values if value]
        row = {
            "featureCode": feature_code,
            "label": feature_label,
            "category": category,
            "values": cells,
        }
        if populated and len(populated) == len(comparable_values) and len(set(populated)) == 1:
            common_features.append(row)
        else:
            different_features.append(row)

    subject_payloads = [
        _subject_payload(requested_subject, record, requested_country)
        for requested_subject, record in selected
    ]
    return {
        "status": "ok",
        "country": requested_country,
        "subjects": subject_payloads,
        "differentFeatures": different_features,
        "commonFeatures": common_features,
        "evidenceRefs": evidence_refs,
        "summary": (
            f"Compared {len(subject_payloads)} published configurations across "
            f"{len(feature_ids)} feature(s); {len(different_features)} differ or are incomplete."
        ),
        "missingEvidence": [],
        "coverageDiagnostics": {
            "diagnosis": "published_product_comparison_ready",
            "publishedVersionCount": len(subject_payloads),
            "evidenceRefCount": len(evidence_refs),
            "differentFeatureCount": len(different_features),
        },
    }


def _load_published_records(session: Session) -> list[PublishedConfigRecord]:
    trims = repo.list_vehicle_trims(session, limit=MAX_PUBLISHED_TRIMS)
    if not trims:
        return []
    versions_by_trim = repo.list_config_versions_for_trims(
        session,
        [trim.trim_id for trim in trims],
    )
    selected: list[tuple[VehicleTrim, ConfigVersion]] = []
    source_ids: set[UUID] = set()
    for trim in trims:
        published_versions = [
            version
            for version in versions_by_trim.get(trim.trim_id, [])
            if version.status == "published" and _snapshot_is_complete(version)
        ]
        if not published_versions:
            continue
        version = max(published_versions, key=_version_sort_key)
        selected.append((trim, version))
        if version.source_upload_id is not None:
            source_ids.add(version.source_upload_id)
    source_batches = repo.list_import_batches_by_ids(session, source_ids)
    records: list[PublishedConfigRecord] = []
    for trim, version in selected:
        contexts = (
            tuple(
                link
                for link in repo.list_source_context_links(session, version.source_upload_id)
                if str(getattr(link, "status", "active") or "active") == "active"
            )
            if version.source_upload_id is not None
            else ()
        )
        records.append(
            PublishedConfigRecord(
                trim=trim,
                version=version,
                source_batch=source_batches.get(version.source_upload_id),
                source_contexts=contexts,
            )
        )
    return records


def _snapshot_is_complete(version: ConfigVersion) -> bool:
    values = version.snapshot_values
    return (
        isinstance(values, list)
        and bool(values)
        and int(version.snapshot_feature_count or 0) == len(values)
    )


def _snapshot_items(version: ConfigVersion) -> list[dict[str, Any]]:
    return [
        item
        for item in (version.snapshot_values or [])
        if isinstance(item, dict)
    ]


def _version_sort_key(version: ConfigVersion) -> tuple[datetime, int]:
    timestamp = version.published_at_utc or version.created_at_utc
    if timestamp is None:
        timestamp = datetime.min.replace(tzinfo=timezone.utc)
    elif timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp, int(version.version_no or 0)


def _required_country(country: str) -> str:
    cleaned = country.strip()
    if not cleaned:
        raise ValueError("country is required for Product Configuration evidence")
    return _canonical_country(cleaned)


def _canonical_country(value: str) -> str:
    normalized = _normalise_text(value)
    return _COUNTRY_ALIASES.get(normalized, value.strip())


def _normalise_text(value: object) -> str:
    return "".join(character for character in str(value or "").casefold() if character.isalnum())


def _clean_list(values: Iterable[str] | None) -> list[str]:
    return list(dict.fromkeys(item.strip() for item in (values or []) if item and item.strip()))


def _record_scope_values(record: PublishedConfigRecord) -> list[str]:
    values = [record.version.market, record.trim.market]
    for context in record.source_contexts:
        values.extend([context.country, context.market])
    return [value for value in values if isinstance(value, str) and value.strip()]


def _matches_country(record: PublishedConfigRecord, country: str) -> bool:
    expected = _normalise_text(_canonical_country(country))
    return any(
        _normalise_text(_canonical_country(scope)) == expected
        for scope in _record_scope_values(record)
    )


def _identity_terms(record: PublishedConfigRecord) -> set[str]:
    terms = {
        _normalise_text(record.trim.brand),
        _normalise_text(record.trim.model_name),
        _normalise_text(record.trim.trim_name),
        _normalise_text(record.trim.full_trim_name),
        _normalise_text(f"{record.trim.brand} {record.trim.model_name}"),
    }
    digits = "".join(re.findall(r"\d+", record.trim.model_name or ""))
    brand_initial = _normalise_text(record.trim.brand)[:1]
    if digits and brand_initial:
        terms.add(f"{brand_initial}{digits}")
    return {term for term in terms if term}


def _matches_subject(record: PublishedConfigRecord, subject: str) -> bool:
    expected = _normalise_text(subject)
    return bool(expected) and any(expected in term or term in expected for term in _identity_terms(record))


def _subject_match_score(record: PublishedConfigRecord, subject: str) -> tuple[int, tuple[datetime, int]]:
    expected = _normalise_text(subject)
    terms = _identity_terms(record)
    exact = 1 if expected in terms else 0
    contained = max((min(len(expected), len(term)) for term in terms if expected in term or term in expected), default=0)
    return exact * 10_000 + contained, _version_sort_key(record.version)


def _query_matches_identity(record: PublishedConfigRecord, query: str) -> bool:
    normalized = _normalise_text(query)
    return bool(normalized) and any(term in normalized for term in _identity_terms(record))


def _matches_feature(item: dict[str, Any], query: str) -> bool:
    expected = _normalise_text(query)
    if not expected:
        return True
    values = (
        item.get("featureCode"),
        item.get("featureName"),
        item.get("category"),
        item.get("rawValue"),
        item.get("normalizedValue"),
    )
    if any(
        normalized and (normalized in expected or expected in normalized)
        for value in values
        if (normalized := _normalise_text(value))
    ):
        return True
    expected_semantics = set(config_feature_semantic_keys(query))
    if not expected_semantics:
        return False
    return any(
        expected_semantics & set(config_feature_semantic_keys(str(value)))
        for value in values
        if value
    )


def _matches_any_feature(item: dict[str, Any], features: list[str]) -> bool:
    return any(_matches_feature(item, feature) for feature in features)


def _powertrain_hint(value: object) -> str:
    normalized = _normalise_text(value)
    if "phev" in normalized or "pluginhybrid" in normalized:
        return "PHEV"
    if "bev" in normalized or "batteryelectric" in normalized:
        return "BEV"
    if "hev" in normalized or normalized == "hybrid":
        return "HEV"
    return ""


def _drivetrain_hint(value: object) -> str:
    normalized = str(value or "").casefold().replace("×", "x")
    compact = re.sub(r"[^0-9a-z]+", "", normalized)
    if "4x4" in compact or "awd" in compact:
        return "AWD"
    if "2wd" in compact or "fwd" in compact:
        return "FWD"
    if "rwd" in compact:
        return "RWD"
    return ""


def _matches_snapshot_scope(
    record: PublishedConfigRecord,
    item: dict[str, Any],
    *,
    query: str,
    features: list[str],
) -> bool:
    if _normalise_text(item.get("category")) != "prices":
        return True
    feature_name = str(item.get("featureName") or "")
    requested_scope = " ".join([query, *features])
    expected_powertrain = (
        _powertrain_hint(requested_scope)
        or _powertrain_hint(record.trim.energy_type)
    )
    row_powertrain = _powertrain_hint(feature_name)
    if expected_powertrain and row_powertrain != expected_powertrain:
        return False
    expected_drivetrain = _drivetrain_hint(requested_scope)
    row_drivetrain = _drivetrain_hint(feature_name)
    if expected_drivetrain and row_drivetrain != expected_drivetrain:
        return False
    return True


def _matches_powertrain(record: PublishedConfigRecord, powertrain: str) -> bool:
    expected = _normalise_text(powertrain)
    values = [record.trim.energy_type]
    values.extend(context.powertrain for context in record.source_contexts)
    return any(expected == _normalise_text(value) or expected in _normalise_text(value) for value in values)


def _source_role(record: PublishedConfigRecord) -> str:
    for context in record.source_contexts:
        explicit = getattr(context, "source_role", None)
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip()
    return "own_product" if record.trim.material_no else "competitor"


def _document_type(record: PublishedConfigRecord) -> str:
    for context in record.source_contexts:
        explicit = getattr(context, "document_type", None)
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip()
    suffix = Path(record.source_batch.source_file_name).suffix.casefold() if record.source_batch else ""
    return {
        ".pdf": "official_pdf",
        ".xlsx": "configuration_workbook",
        ".xls": "configuration_workbook",
        ".csv": "configuration_table",
    }.get(suffix, "configuration_source")


def _parse_date(value: str) -> date | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return date.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError("effective_date must use YYYY-MM-DD") from exc


def _context_date(record: PublishedConfigRecord, name: str) -> date | None:
    for context in record.source_contexts:
        value = getattr(context, name, None)
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return date.fromisoformat(value.strip())
            except ValueError:
                continue
    return None


def _is_effective(record: PublishedConfigRecord, requested_date: date) -> bool:
    effective_from = _context_date(record, "effective_from")
    effective_to = _context_date(record, "effective_to")
    if effective_from is None and effective_to is None:
        return False
    if effective_from is not None and requested_date < effective_from:
        return False
    return effective_to is None or requested_date <= effective_to


def _feature_identity(item: dict[str, Any]) -> str:
    feature_code = _normalise_text(item.get("featureCode"))
    if feature_code:
        return feature_code
    return "|".join(
        (
            _normalise_text(item.get("category")),
            _normalise_text(item.get("featureName")),
        )
    )


def _snapshot_display_value(item: dict[str, Any]) -> str:
    raw_value = str(item.get("rawValue") or "").strip()
    unit = str(item.get("unit") or "").strip()
    return f"{raw_value} {unit}".strip()


def _location(item: dict[str, Any], source_batch: ImportBatch | None) -> tuple[str, str]:
    source = item.get("source") if isinstance(item.get("source"), dict) else {}
    page_number = source.get("pageNumber")
    if isinstance(page_number, int) and page_number > 0:
        return "page", f"p.{page_number}"
    sheet_name = str(source.get("sheetName") or "").strip()
    cell = str(
        source.get("mergedRange")
        or source.get("cell")
        or source.get("sourceCell")
        or ""
    ).strip()
    if sheet_name:
        return "sheet_range", f"{sheet_name}!{cell}" if cell else sheet_name
    source_column = str(item.get("sourceColumn") or "").strip()
    pdf_match = re.match(r"P(?P<page>\d+)(?:OCR)?R\d+C\d+", source_column)
    if pdf_match:
        return "page", f"p.{pdf_match.group('page')}"
    source_row = int(item.get("sourceRow") or 0)
    suffix = Path(source_batch.source_file_name).suffix.casefold() if source_batch else ""
    if suffix in {".xlsx", ".xls", ".csv"} and (source_row or source_column):
        return "sheet_range", f"row {source_row}, column {source_column or '?'}"
    return "section", source_column or (f"row {source_row}" if source_row else "source")


def _source_url(record: PublishedConfigRecord) -> str:
    for context in record.source_contexts:
        value = getattr(context, "source_url", None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _evidence_ref(
    record: PublishedConfigRecord,
    item: dict[str, Any],
    *,
    country: str,
    retrieved_at: datetime,
) -> dict[str, Any]:
    location_type, location = _location(item, record.source_batch)
    document_id = str(record.version.source_upload_id or record.version.version_id)
    feature_code = str(item.get("featureCode") or _feature_identity(item))
    ref_material = f"{record.version.version_id}|{feature_code}|{location}"
    ref_id = f"product-config-{sha256(ref_material.encode('utf-8')).hexdigest()[:16]}"
    effective_from = _context_date(record, "effective_from")
    effective_to = _context_date(record, "effective_to")
    return {
        "refId": ref_id,
        "evidenceDocumentId": document_id,
        "sourceClass": "product_evidence_library",
        "sourceRole": _source_role(record),
        "sourceType": "engineering",
        "documentType": _document_type(record),
        "country": country,
        "brand": record.trim.brand,
        "model": record.trim.model_name,
        "trim": record.trim.trim_name,
        "powertrain": record.trim.energy_type,
        "modelYear": record.version.model_year or record.trim.model_year,
        "featureCode": feature_code,
        "label": str(item.get("featureName") or feature_code),
        "category": str(item.get("category") or "Unknown"),
        "value": item.get("normalizedValue") if item.get("normalizedValue") is not None else item.get("rawValue"),
        "rawValue": item.get("rawValue"),
        "availability": item.get("availability"),
        "unit": item.get("unit"),
        "fileName": record.source_batch.source_file_name if record.source_batch else "",
        "fileHash": record.source_batch.source_file_hash if record.source_batch else None,
        "locationType": location_type,
        "location": location,
        "sourceUrl": _source_url(record),
        "effectiveFrom": effective_from.isoformat() if effective_from else None,
        "effectiveTo": effective_to.isoformat() if effective_to else None,
        "publicationStatus": "published",
        "verificationStatus": "published_verified",
        "scope": "country",
        "publishedVersionId": str(record.version.version_id),
        "publishedAt": (
            record.version.published_at_utc.isoformat()
            if record.version.published_at_utc
            else None
        ),
        "retrievedAt": retrieved_at.isoformat(),
    }


def _subject_payload(
    requested_subject: str,
    record: PublishedConfigRecord,
    country: str,
) -> dict[str, Any]:
    return {
        "requestedSubject": requested_subject,
        "country": country,
        "brand": record.trim.brand,
        "model": record.trim.model_name,
        "trim": record.trim.trim_name,
        "powertrain": record.trim.energy_type,
        "modelYear": record.version.model_year or record.trim.model_year,
        "publishedVersionId": str(record.version.version_id),
        "sourceFileName": record.source_batch.source_file_name if record.source_batch else "",
        "sourceRole": _source_role(record),
    }


def _insufficient_compare(
    country: str,
    requested_subjects: list[str],
    reasons: list[str],
) -> dict[str, Any]:
    return {
        "status": "insufficient_evidence",
        "country": country,
        "subjects": [],
        "differentFeatures": [],
        "commonFeatures": [],
        "evidenceRefs": [],
        "summary": "Published Product Configuration evidence is incomplete for this comparison.",
        "missingEvidence": [
            {
                "name": "published_product_configuration",
                "reason": reason,
                "impact": "blocking",
            }
            for reason in reasons
        ],
        "coverageDiagnostics": {
            "diagnosis": "published_product_comparison_incomplete",
            "country": country,
            "requestedSubjects": requested_subjects,
        },
    }
