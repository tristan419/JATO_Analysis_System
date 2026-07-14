from __future__ import annotations

import hmac
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from uuid import UUID

from sqlalchemy.orm import Session

from app.api.msrp_source_governance_schemas import SourceGateInput
from app.core.config import resolve_msrp_governance_evidence_root
from app.db.models import MsrpObservation
from app.db.msrp_source_governance_models import MsrpObservationEvidenceLink
from app.infra import msrp_repository
from app.infra import msrp_source_governance_repository as governance_repo
from app.services.msrp_evidence_integrity_service import (
    REPLAYABLE_EVIDENCE_TYPES,
    audit_evidence_object,
)
from app.services.msrp_official_source_policy import (
    is_enabled_official_msrp_source,
    is_official_msrp_source_type,
    source_type_token,
)


SOURCE_GATE_POLICY_VERSION = "msrp-source-evidence-v1"
MAX_UNDATED_CAPTURE_AGE = timedelta(days=30)
MAX_FUTURE_CAPTURE_SKEW = timedelta(hours=24)
DETERMINISTIC_EXTRACTOR_BLOCKLIST = frozenset(
    {"dpv4", "generative", "llm", "manual"}
)
OBJECT_ISSUE_REASONS = {
    "missing": "evidence_object_missing",
    "unreadable": "evidence_object_unreadable",
    "not_regular_file": "evidence_object_not_regular",
    "size_mismatch": "evidence_size_mismatch",
    "sha256_mismatch": "evidence_sha256_mismatch",
    "invalid_sha256_metadata": "evidence_sha256_metadata_invalid",
    "invalid_size_metadata": "evidence_size_metadata_invalid",
    "missing_storage_key": "evidence_storage_key_missing",
    "invalid_storage_key": "evidence_storage_key_invalid",
    "path_escape": "evidence_storage_path_escape",
    "content_address_mismatch": "evidence_content_address_mismatch",
}


@dataclass(frozen=True)
class EvidenceVerificationResult:
    source_version_id: UUID | None
    evidence_refs: tuple[dict[str, object], ...]
    reasons: tuple[str, ...]
    source_gate: SourceGateInput

    @property
    def passed(self) -> bool:
        return not self.reasons


def summary_passed(result: dict[str, object] | None) -> bool:
    if not result:
        return False
    if result.get("passed") is True:
        return True
    return str(result.get("status") or "").casefold() in {
        "pass",
        "passed",
        "success",
        "succeeded",
    }


def has_blocking_conflict(result: dict[str, object] | None) -> bool:
    if not result:
        return False
    if result.get("blocking") is True:
        return True
    return str(result.get("status") or "").casefold() in {
        "blocking",
        "conflict",
        "failed",
    }


def _append_reason(reasons: list[str], reason: str) -> None:
    if reason not in reasons:
        reasons.append(reason)


def _normalized_segment(value: object | None) -> str:
    return " ".join(str(value or "").split()).casefold()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _window_covers(observed_at: datetime, valid_from, valid_until) -> bool:
    observed_date = _utc(observed_at).date()
    return not (
        (valid_from is not None and observed_date < valid_from)
        or (valid_until is not None and observed_date > valid_until)
        or (
            valid_from is not None
            and valid_until is not None
            and valid_until < valid_from
        )
    )


def _capture_window_valid(asset, observed_at: datetime) -> bool:
    captured_at = _utc(asset.captured_at_utc)
    observed_at_utc = _utc(observed_at)
    if captured_at > observed_at_utc + MAX_FUTURE_CAPTURE_SKEW:
        return False
    has_declared_validity = asset.valid_from is not None or asset.valid_until is not None
    return has_declared_validity or captured_at >= observed_at_utc - MAX_UNDATED_CAPTURE_AGE


def _declared_evidence(version) -> dict[str, str]:
    declared: dict[str, str] = {}
    for item in version.evidence_refs_json or []:
        if not isinstance(item, dict):
            continue
        asset_id = str(
            item.get("evidenceAssetId") or item.get("evidence_asset_id") or ""
        )
        sha256 = str(item.get("sha256") or "").casefold()
        if asset_id:
            declared[asset_id] = sha256
    return declared


def evidence_reference_from_link(link) -> dict[str, object]:
    asset = link.evidence_asset
    return {
        "evidenceAssetId": str(link.evidence_asset_id),
        "sha256": str(link.evidence_sha256).casefold(),
        "evidenceType": asset.evidence_type,
        "evidenceRole": link.evidence_role,
        "storageKey": asset.storage_key,
        "capturedAtUtc": _utc(asset.captured_at_utc).isoformat(),
    }


def evidence_references_by_observation(
    links: Iterable[MsrpObservationEvidenceLink],
) -> dict[UUID, list[dict[str, object]]]:
    result: dict[UUID, list[dict[str, object]]] = {}
    for link in links:
        result.setdefault(link.observation_id, []).append(
            evidence_reference_from_link(link)
        )
    return result


def _verify_loaded_observation(
    session: Session,
    observation: MsrpObservation,
    *,
    target_id: UUID | None,
    links: list[MsrpObservationEvidenceLink],
    evidence_root: Path,
) -> EvidenceVerificationResult:
    reasons: list[str] = []
    verified_refs: list[dict[str, object]] = []
    source_version_id = observation.source_version_id
    version = (
        governance_repo.get_source_version(session, source_version_id)
        if source_version_id is not None
        else None
    )
    if source_version_id is None or version is None:
        _append_reason(reasons, "source_version_missing")
        return _result(source_version_id, verified_refs, reasons)

    target = governance_repo.get_target(session, version.target_id)
    source = msrp_repository.get_source(session, observation.source_id)
    if target is None:
        _append_reason(reasons, "monitoring_target_missing")
    if source is None:
        _append_reason(reasons, "source_registry_missing")
    if target_id is not None and version.target_id != target_id:
        _append_reason(reasons, "source_version_target_mismatch")
    if version.source_id != observation.source_id:
        _append_reason(reasons, "source_version_source_mismatch")
    if version.version_status != "published":
        _append_reason(reasons, "source_version_not_published")
    if target is not None and target.active_source_version_id != source_version_id:
        _append_reason(reasons, "source_version_not_active")

    if target is not None:
        identity_matches = (
            _normalized_segment(target.country) == _normalized_segment(observation.country)
            and _normalized_segment(target.brand) == _normalized_segment(observation.brand)
            and _normalized_segment(target.model)
            in {
                _normalized_segment(observation.jato_model),
                _normalized_segment(observation.official_model),
            }
        )
        if not identity_matches:
            _append_reason(reasons, "observation_target_mismatch")

    if source is not None:
        source_identity_matches = (
            _normalized_segment(source.country)
            == _normalized_segment(observation.country)
            and _normalized_segment(source.brand)
            == _normalized_segment(observation.brand)
        )
        if not source_identity_matches:
            _append_reason(reasons, "observation_source_registry_mismatch")
        if target is not None and (
            _normalized_segment(source.country)
            != _normalized_segment(target.country)
            or _normalized_segment(source.brand)
            != _normalized_segment(target.brand)
        ):
            _append_reason(reasons, "source_registry_target_mismatch")
        if getattr(source, "enabled", False) is not True:
            _append_reason(reasons, "source_disabled")
        if not is_official_msrp_source_type(source.source_type):
            _append_reason(reasons, "source_type_not_official")
        if not is_enabled_official_msrp_source(source):
            _append_reason(reasons, "source_policy_not_approved")
        if source_type_token(source.price_semantics) != "base_msrp":
            _append_reason(reasons, "source_semantic_lane_not_base_msrp")

    semantic_lane_valid = source_type_token(version.semantic_lane) == "base_msrp"
    if not semantic_lane_valid:
        _append_reason(reasons, "source_version_semantic_lane_invalid")
    currency_valid = version.currency.upper() == observation.source_currency.upper()
    if not currency_valid:
        _append_reason(reasons, "source_version_currency_mismatch")
    expected_tax_mode = "tax_included" if observation.tax_included else "tax_excluded"
    tax_mode_valid = source_type_token(version.tax_mode) == expected_tax_mode
    if not tax_mode_valid:
        _append_reason(reasons, "source_version_tax_mode_mismatch")
    extractor_type = source_type_token(version.extractor_type)
    deterministic_extraction = bool(extractor_type) and extractor_type not in (
        DETERMINISTIC_EXTRACTOR_BLOCKLIST
    )
    if source is not None:
        deterministic_extraction = deterministic_extraction and (
            version.extractor_name == source.extractor_name
            and version.extractor_version == source.extractor_version
            and observation.extraction_version == version.extractor_version
        )
    if not deterministic_extraction:
        _append_reason(reasons, "source_version_extractor_mismatch")
    validity_window_valid = _window_covers(
        observation.observed_at_utc,
        version.valid_from,
        version.valid_until,
    )
    if not validity_window_valid:
        _append_reason(reasons, "source_version_validity_window_invalid")

    schema_valid = summary_passed(version.validation_summary_json)
    targeted_dryrun_passed = summary_passed(version.dryrun_summary_json)
    blocking_conflict = has_blocking_conflict(version.conflict_summary_json)
    if not schema_valid:
        _append_reason(reasons, "source_validation_not_passed")
    if not targeted_dryrun_passed:
        _append_reason(reasons, "targeted_dryrun_not_passed")
    if blocking_conflict:
        _append_reason(reasons, "blocking_source_or_semantic_conflict")
    unresolved_correction = any(
        row.decision_status in {"draft", "submitted", "approved"}
        for row in governance_repo.list_result_corrections_for_observations(
            session,
            [observation.observation_id],
        )
    )
    if unresolved_correction:
        _append_reason(reasons, "unresolved_result_correction")

    if not links:
        _append_reason(reasons, "observation_evidence_link_missing")
    declared = _declared_evidence(version)
    non_replayable_types: set[str] = set()
    for link in links:
        if link.source_version_id != source_version_id:
            _append_reason(reasons, "evidence_link_source_version_mismatch")
            continue
        asset = link.evidence_asset or governance_repo.get_evidence_asset(
            session,
            link.evidence_asset_id,
        )
        if asset is None:
            _append_reason(reasons, "evidence_asset_missing")
            continue
        evidence_type = str(asset.evidence_type or "").strip()
        if evidence_type not in REPLAYABLE_EVIDENCE_TYPES:
            non_replayable_types.add(evidence_type)
            continue
        candidate_reasons: list[str] = []
        linked_sha = str(link.evidence_sha256 or "").casefold()
        asset_sha = str(asset.sha256 or "").casefold()
        if not hmac.compare_digest(linked_sha, asset_sha):
            _append_reason(candidate_reasons, "evidence_link_sha256_mismatch")
        if asset.target_id != version.target_id:
            _append_reason(candidate_reasons, "evidence_target_mismatch")
        if asset.source_id != observation.source_id:
            _append_reason(candidate_reasons, "evidence_source_mismatch")
        if asset.lifecycle_state != "active":
            _append_reason(candidate_reasons, "evidence_lifecycle_not_active")
        if not asset.official_domain_verified:
            _append_reason(candidate_reasons, "evidence_official_domain_not_verified")
        if not is_official_msrp_source_type(asset.source_type):
            _append_reason(candidate_reasons, "evidence_source_type_not_official")
        if source_type_token(asset.semantic_lane) != "base_msrp":
            _append_reason(candidate_reasons, "evidence_semantic_lane_invalid")
        if not _window_covers(
            observation.observed_at_utc,
            asset.valid_from,
            asset.valid_until,
        ):
            _append_reason(candidate_reasons, "evidence_validity_window_invalid")
        if not _capture_window_valid(asset, observation.observed_at_utc):
            _append_reason(candidate_reasons, "evidence_capture_window_stale")
        declared_sha = declared.get(str(asset.evidence_asset_id))
        if declared_sha is None:
            _append_reason(candidate_reasons, "evidence_not_declared_by_source_version")
        elif not hmac.compare_digest(declared_sha, asset_sha):
            _append_reason(candidate_reasons, "source_version_evidence_sha256_mismatch")
        object_result = audit_evidence_object(asset, evidence_root)
        for issue in object_result["issues"]:
            _append_reason(
                candidate_reasons,
                OBJECT_ISSUE_REASONS.get(str(issue), f"evidence_object_{issue}"),
            )
        if candidate_reasons:
            for reason in candidate_reasons:
                _append_reason(reasons, reason)
            continue
        verified_refs.append(evidence_reference_from_link(link))

    if not verified_refs:
        if non_replayable_types == {"official_url"}:
            _append_reason(reasons, "official_url_only_not_replayable")
        elif non_replayable_types == {"screenshot"}:
            _append_reason(reasons, "screenshot_only_not_replayable")
        else:
            _append_reason(reasons, "replayable_evidence_missing")

    return _result(
        source_version_id,
        verified_refs,
        reasons,
        source_version_status=version.version_status,
        deterministic_extraction=deterministic_extraction,
        semantic_lane_valid=semantic_lane_valid,
        currency_valid=currency_valid,
        tax_mode_valid=tax_mode_valid,
        validity_window_valid=validity_window_valid,
        blocking_conflict=blocking_conflict,
        unresolved_result_correction=unresolved_correction,
        schema_valid=schema_valid,
        targeted_dryrun_passed=targeted_dryrun_passed,
    )


def _result(
    source_version_id: UUID | None,
    evidence_refs: list[dict[str, object]],
    reasons: list[str],
    *,
    source_version_status: str = "missing",
    deterministic_extraction: bool = False,
    semantic_lane_valid: bool = False,
    currency_valid: bool = False,
    tax_mode_valid: bool = False,
    validity_window_valid: bool = False,
    blocking_conflict: bool = False,
    unresolved_result_correction: bool = False,
    schema_valid: bool = False,
    targeted_dryrun_passed: bool = False,
) -> EvidenceVerificationResult:
    immutable_evidence = bool(evidence_refs) and not any(
        reason.startswith("evidence_") or reason.endswith("not_replayable")
        for reason in reasons
    )
    source_gate = SourceGateInput(
        policy_version=SOURCE_GATE_POLICY_VERSION,
        source_version_status=source_version_status,
        verified_official_evidence=immutable_evidence,
        immutable_evidence=immutable_evidence,
        deterministic_extraction=deterministic_extraction,
        semantic_lane_valid=semantic_lane_valid,
        currency_valid=currency_valid,
        tax_mode_valid=tax_mode_valid,
        validity_window_valid=validity_window_valid,
        blocking_conflict=blocking_conflict,
        unresolved_result_correction=unresolved_result_correction,
        schema_valid=schema_valid,
        targeted_dryrun_passed=targeted_dryrun_passed,
        derived_reasons=reasons,
    )
    return EvidenceVerificationResult(
        source_version_id=source_version_id,
        evidence_refs=tuple(evidence_refs),
        reasons=tuple(reasons),
        source_gate=source_gate,
    )


def verify_observation_evidence(
    session: Session,
    observation: MsrpObservation,
    *,
    target_id: UUID | None = None,
    evidence_root: Path | None = None,
    links: list[MsrpObservationEvidenceLink] | None = None,
) -> EvidenceVerificationResult:
    resolved_links = links
    if resolved_links is None:
        resolved_links = governance_repo.list_observation_evidence_links(
            session,
            [observation.observation_id],
        )
    return _verify_loaded_observation(
        session,
        observation,
        target_id=target_id,
        links=resolved_links,
        evidence_root=resolve_msrp_governance_evidence_root(evidence_root),
    )
