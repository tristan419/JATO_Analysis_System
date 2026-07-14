from __future__ import annotations

import hashlib
import hmac
import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID, uuid4

import yaml
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.msrp_source_governance_schemas import (
    ApprovalRequest,
    AgentRunRequestV1,
    AgentRunResultV1,
    EvidenceReference,
    EvidenceUploadComplete,
    EvidenceUploadInitiate,
    FxNormalizationCreate,
    GateEvaluationRequest,
    HermesDiagnosisRequest,
    MonitoringTargetCreate,
    ProposalVerificationUpdate,
    PublishSourceVersionRequest,
    RepairCaseFindingCreate,
    RepairProposalCreate,
    ResolveRepairCaseRequest,
    ResultCorrectionCreate,
    MonitorAnomalyV1,
    SourceRunResultV1,
    SourceVersionCreate,
    SubmitProposalRequest,
    UrlEvidenceCreate,
    GateDecisionV1,
)
from app.db.models import CurrentPrice, MsrpObservation, MsrpSource, PriceHistory
from app.db.msrp_source_governance_models import (
    MsrpEvidenceUploadSession,
    MsrpFxNormalizationRun,
    MsrpGovernanceAuditEvent,
    MsrpGovernanceGateDecision,
    MsrpGovernanceRepairCase,
    MsrpMonitoringTarget,
    MsrpRepairProposal,
    MsrpResultCorrectionDecision,
    MsrpSourceEvidenceAsset,
    MsrpSourceVersion,
)
from app.infra import msrp_source_governance_repository as repo
from app.services.msrp_materialization_eligibility_service import (
    evaluate_materialization_eligibility,
)
from app.services.msrp_source_governance.serializers import (
    audit_event_payload,
    evidence_payload,
    evidence_upload_payload,
    fx_run_payload,
    gate_decision_payload,
    proposal_payload,
    repair_case_payload,
    result_correction_payload,
    source_version_payload,
    target_payload,
)


OPEN_CASE_STATUSES = frozenset(
    {
        "open",
        "diagnosing",
        "awaiting_evidence",
        "proposal_ready",
        "dryrun_passed",
        "awaiting_approval",
        "paused",
    }
)
ANTI_BOT_FAILURES = frozenset(
    {
        "anti_bot",
        "captcha",
        "cloudflare_challenge",
        "access_denied",
        "browser_verification_required",
    }
)
TERMINAL_AGENT_RUN_STATUSES = frozenset(
    {
        "succeeded",
        "success",
        "completed",
        "complete",
        "failed",
        "stopped",
        "cancelled",
    }
)
SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}
DEFAULT_EVIDENCE_ROOT = (
    Path(__file__).resolve().parents[3]
    / "artifacts"
    / "msrp-source-governance"
    / "evidence"
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalized_segment(value: str | None) -> str:
    return " ".join(str(value or "").split()).casefold()


def build_target_key(
    country: str,
    brand: str,
    model: str,
    trim_scope: str | None,
    powertrain_scope: str | None,
) -> str:
    return "::".join(
        (
            country.strip().upper(),
            _normalized_segment(brand),
            _normalized_segment(model),
            _normalized_segment(trim_scope),
            _normalized_segment(powertrain_scope),
        )
    )


def _case_dedupe_key(value: RepairCaseFindingCreate) -> str:
    identity = {
        "repairDomain": value.repair_domain,
        "targetId": str(value.target_id or ""),
        "sourceId": str(value.source_id or ""),
        "observationId": str(value.observation_id or ""),
        "mappingReference": value.mapping_reference or "",
        "fxRunId": str(value.fx_run_id or ""),
        "caseType": value.case_type,
        "failureClassifier": value.failure_classifier,
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _passed(result: dict[str, object] | None) -> bool:
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


def _blocking_conflict(result: dict[str, object] | None) -> bool:
    if not result:
        return False
    if result.get("blocking") is True:
        return True
    return str(result.get("status") or "").casefold() in {
        "blocking",
        "conflict",
        "failed",
    }


def _verified_official_url(url: str, official_domain: str) -> tuple[str, str]:
    parsed = urlparse(url)
    host = (parsed.hostname or "").casefold().rstrip(".")
    domain = official_domain.casefold().strip().rstrip(".")
    if parsed.scheme != "https":
        raise HTTPException(
            status_code=422,
            detail="Final official evidence URL must use HTTPS",
        )
    if not host or not (host == domain or host.endswith(f".{domain}")):
        raise HTTPException(
            status_code=422,
            detail="Evidence URL is outside the declared official domain",
        )
    return host, domain


class MsrpSourceGovernanceService:
    def __init__(self, session: Session, *, evidence_root: Path | None = None):
        self.session = session
        configured_root = os.getenv("MSRP_GOVERNANCE_EVIDENCE_ROOT")
        self.evidence_root = (
            evidence_root
            or (Path(configured_root) if configured_root else DEFAULT_EVIDENCE_ROOT)
        ).resolve()

    def _require_new_action(self, action: str, idempotency_key: str) -> None:
        key = idempotency_key.strip()
        if not key:
            raise HTTPException(status_code=400, detail="Idempotency key is required")
        existing = repo.get_audit_event_by_idempotency(
            self.session,
            action=action,
            idempotency_key=key,
        )
        if existing is not None:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Idempotency key already used for this action",
                    "auditEventId": str(existing.audit_event_id),
                },
            )

    def _audit(
        self,
        *,
        entity_type: str,
        entity_id: str,
        action: str,
        actor: str,
        actor_role: str,
        idempotency_key: str,
        before: dict[str, object] | None = None,
        after: dict[str, object] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        repo.add(
            self.session,
            MsrpGovernanceAuditEvent(
                audit_event_id=uuid4(),
                entity_type=entity_type,
                entity_id=entity_id,
                action=action,
                actor=actor,
                actor_role=actor_role,
                idempotency_key=idempotency_key.strip(),
                before_json=before,
                after_json=after,
                metadata_json=metadata,
            ),
        )

    def _commit(self, conflict_detail: str) -> None:
        try:
            self.session.commit()
        except IntegrityError as exc:
            self.session.rollback()
            raise HTTPException(status_code=409, detail=conflict_detail) from exc

    def list_targets(
        self,
        *,
        country: str | None,
        brand: str | None,
        monitoring_status: str | None,
        roster_type: str | None,
        limit: int,
        offset: int,
    ) -> dict[str, object]:
        total, rows = repo.list_targets(
            self.session,
            country=country,
            brand=brand,
            monitoring_status=monitoring_status,
            roster_type=roster_type,
            limit=limit,
            offset=offset,
        )
        latest_gates = repo.list_latest_gate_decisions_by_target(
            self.session,
            [row.target_id for row in rows],
        )
        case_summary = repo.summarize_open_cases_by_target(
            self.session,
            [row.target_id for row in rows],
        )
        items: list[dict[str, object]] = []
        for row in rows:
            item = target_payload(row)
            item["gateSummary"] = (
                gate_decision_payload(latest_gates[row.target_id])
                if row.target_id in latest_gates
                else None
            )
            item.update(
                case_summary.get(
                    row.target_id,
                    {"openCaseCount": 0, "manualEvidenceCaseCount": 0},
                )
            )
            items.append(item)
        return {
            "rows": len(rows),
            "total": total,
            "items": items,
        }

    def get_target_detail(self, target_id: UUID) -> dict[str, object]:
        target = repo.get_target(self.session, target_id)
        if target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        evidence = repo.list_evidence_for_target(self.session, target_id)
        versions = repo.list_source_versions(
            self.session,
            target_id=target_id,
            limit=200,
        )
        _, cases = repo.list_repair_cases(
            self.session,
            repair_domain=None,
            case_status=None,
            severity=None,
            target_id=target_id,
            limit=200,
            offset=0,
        )
        observation_ids = list(
            dict.fromkeys(
                row.observation_id for row in cases if row.observation_id is not None
            )
        )
        corrections = repo.list_result_corrections_for_observations(
            self.session,
            observation_ids,
        )
        fx_runs = repo.list_fx_runs_for_observations(
            self.session,
            observation_ids,
        )
        latest_gates = repo.list_latest_gate_decisions_by_target(
            self.session,
            [target_id],
        )
        latest_gate = latest_gates.get(target_id)
        return {
            "item": target_payload(target),
            "gateSnapshot": (
                gate_decision_payload(latest_gate) if latest_gate else None
            ),
            "evidence": [evidence_payload(row) for row in evidence],
            "sourceVersions": [source_version_payload(row) for row in versions],
            "repairCases": [repair_case_payload(row) for row in cases],
            "resultCorrections": [
                result_correction_payload(row) for row in corrections
            ],
            "fxRuns": [fx_run_payload(row) for row in fx_runs],
        }

    def evaluate_and_record_gate_decision(
        self,
        value: GateEvaluationRequest,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "governance_gate_decision.evaluate"
        self._require_new_action(action, idempotency_key)
        target = repo.get_target(self.session, value.target_id)
        if target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        observation = self.session.get(MsrpObservation, value.observation_id)
        if observation is None:
            raise HTTPException(status_code=404, detail="Observation not found")
        if target.country.casefold() != observation.country.casefold():
            raise HTTPException(
                status_code=422,
                detail="Gate target country differs from observation",
            )
        if _normalized_segment(target.brand) != _normalized_segment(observation.brand):
            raise HTTPException(
                status_code=422,
                detail="Gate target brand differs from observation",
            )

        decision = evaluate_materialization_eligibility(
            target_id=value.target_id,
            observation_id=value.observation_id,
            source_gate=value.source_gate,
            mapping_gate=value.mapping_gate,
            fx_gate=value.fx_gate,
        )
        row = MsrpGovernanceGateDecision(
            gate_decision_id=uuid4(),
            schema_version=decision.schema_version,
            target_id=decision.target_id,
            observation_id=decision.observation_id,
            source_gate_json=decision.source_gate.model_dump(
                mode="json",
                by_alias=True,
            ),
            mapping_gate_json=decision.mapping_gate.model_dump(
                mode="json",
                by_alias=True,
            ),
            fx_gate_json=(
                decision.fx_gate.model_dump(mode="json", by_alias=True)
                if decision.fx_gate is not None
                else None
            ),
            eligible_for_local_materialization=(
                decision.eligible_for_local_materialization
            ),
            eligible_for_normalized_materialization=(
                decision.eligible_for_normalized_materialization
            ),
            evaluation_context_json=value.evaluation_context,
            evaluated_at_utc=decision.evaluated_at,
            created_by=actor,
        )
        repo.add(self.session, row)
        self.session.flush()
        after = gate_decision_payload(row)
        self._audit(
            entity_type="governance_gate_decision",
            entity_id=str(row.gate_decision_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            after=after,
            metadata={
                "localMaterializationEligible": (
                    decision.eligible_for_local_materialization
                ),
                "normalizedMaterializationEligible": (
                    decision.eligible_for_normalized_materialization
                ),
            },
        )
        self._commit("Gate decision or idempotency key already exists")
        self.session.refresh(row)
        return gate_decision_payload(row)

    def get_latest_gate_decision(self, target_id: UUID) -> dict[str, object] | None:
        if repo.get_target(self.session, target_id) is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        row = repo.get_latest_gate_decision_for_target(self.session, target_id)
        return gate_decision_payload(row) if row is not None else None

    def _require_persisted_gate_decision(
        self,
        gate_decision_id: UUID,
        observation_id: UUID,
        *,
        normalized_lane: bool,
    ) -> MsrpGovernanceGateDecision:
        row = repo.get_gate_decision(self.session, gate_decision_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Gate Decision not found")
        if row.observation_id != observation_id:
            raise HTTPException(status_code=422, detail="Gate observation mismatch")
        if (
            row.source_gate_json.get("status") != "pass"
            or row.mapping_gate_json.get("status") != "pass"
            or not row.eligible_for_local_materialization
        ):
            raise HTTPException(
                status_code=409,
                detail="Persisted Source and Mapping Gates are not eligible",
            )
        if normalized_lane and (
            (row.fx_gate_json or {}).get("status") != "pass"
            or not row.eligible_for_normalized_materialization
        ):
            raise HTTPException(
                status_code=409,
                detail="Persisted FX Gate is not eligible",
            )
        return row

    def _validated_evidence_references(
        self,
        evidence_refs: list[EvidenceReference],
        *,
        target_id: UUID | None,
        required: bool = False,
    ) -> list[dict[str, object]]:
        if required and not evidence_refs:
            raise HTTPException(
                status_code=422,
                detail="Immutable official evidence is required",
            )
        payloads: list[dict[str, object]] = []
        for item in evidence_refs:
            evidence = repo.get_evidence_asset(self.session, item.evidence_asset_id)
            if (
                evidence is None
                or evidence.sha256 != item.sha256
                or evidence.evidence_type != item.evidence_type
                or not evidence.official_domain_verified
                or evidence.lifecycle_state != "active"
                or (target_id is not None and evidence.target_id != target_id)
            ):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Evidence reference is not active verified official evidence "
                        f"for this target: {item.evidence_asset_id}"
                    ),
                )
            payloads.append(item.model_dump(mode="json", by_alias=True))
        return payloads

    def create_target(
        self,
        value: MonitoringTargetCreate,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "monitoring_target.create"
        self._require_new_action(action, idempotency_key)
        target_key = build_target_key(
            value.country,
            value.brand,
            value.model,
            value.trim_scope,
            value.powertrain_scope,
        )
        if repo.get_target_by_key(self.session, target_key) is not None:
            raise HTTPException(status_code=409, detail="Monitoring target already exists")
        target = MsrpMonitoringTarget(
            target_id=uuid4(),
            target_key=target_key,
            country=value.country.upper(),
            brand=value.brand,
            model=value.model,
            trim_scope=value.trim_scope,
            powertrain_scope=value.powertrain_scope,
            roster_type=value.roster_type,
            roster_rank=value.roster_rank,
            monitoring_status="pending",
            schedule_json=value.schedule,
            owner=value.owner,
            notes=value.notes,
            row_version=1,
        )
        repo.add(self.session, target)
        self.session.flush()
        after = target_payload(target)
        self._audit(
            entity_type="monitoring_target",
            entity_id=str(target.target_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            after=after,
        )
        self._commit("Monitoring target or idempotency key already exists")
        self.session.refresh(target)
        return target_payload(target)

    def add_url_evidence(
        self,
        target_id: UUID,
        value: UrlEvidenceCreate,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "source_evidence.add_url"
        self._require_new_action(action, idempotency_key)
        target = repo.get_target(self.session, target_id, for_update=True)
        if target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        if value.source_id and self.session.get(MsrpSource, value.source_id) is None:
            raise HTTPException(status_code=404, detail="MSRP source not found")
        if value.repair_case_id:
            case = repo.get_repair_case(self.session, value.repair_case_id)
            if case is None:
                raise HTTPException(status_code=404, detail="Repair case not found")
            if case.target_id and case.target_id != target_id:
                raise HTTPException(
                    status_code=409,
                    detail="Repair case belongs to another monitoring target",
                )

        source_url = str(value.source_url)
        final_url = str(value.final_url or value.source_url)
        _final_host, official_domain = _verified_official_url(
            final_url,
            value.official_domain,
        )

        redirect_chain = [str(item) for item in value.redirect_chain]
        captured_at = value.captured_at_utc or _now()
        manifest = {
            "targetId": str(target_id),
            "sourceUrl": source_url,
            "finalUrl": final_url,
            "redirectChain": redirect_chain,
            "officialDomain": official_domain,
            "sourceType": value.source_type,
            "semanticLane": value.semantic_lane,
        }
        sha256 = hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()
        existing = repo.get_evidence_by_sha256(self.session, target_id, sha256)
        if existing is not None:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Equivalent immutable URL evidence already exists",
                    "evidenceAssetId": str(existing.evidence_asset_id),
                },
            )
        evidence = MsrpSourceEvidenceAsset(
            evidence_asset_id=uuid4(),
            target_id=target_id,
            source_id=value.source_id,
            repair_case_id=value.repair_case_id,
            evidence_type="official_url",
            source_url=source_url,
            final_url=final_url,
            redirect_chain_json=redirect_chain,
            official_domain_verified=True,
            sha256=sha256,
            captured_at_utc=captured_at,
            document_date=value.document_date,
            valid_from=value.valid_from,
            valid_until=value.valid_until,
            source_type=value.source_type,
            semantic_lane=value.semantic_lane,
            lifecycle_state="active",
            created_by=actor,
        )
        repo.add(self.session, evidence)
        if target.monitoring_status == "manual_evidence_required":
            target.monitoring_status = "degraded"
            target.row_version += 1
            target.updated_at_utc = _now()
        self.session.flush()
        after = evidence_payload(evidence)
        self._audit(
            entity_type="source_evidence_asset",
            entity_id=str(evidence.evidence_asset_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            after=after,
        )
        self._commit("Evidence or idempotency key already exists")
        self.session.refresh(evidence)
        return evidence_payload(evidence)

    def initiate_evidence_upload(
        self,
        value: EvidenceUploadInitiate,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "evidence_upload.initiate"
        self._require_new_action(action, idempotency_key)
        target = repo.get_target(self.session, value.target_id)
        if target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        if value.source_id and self.session.get(MsrpSource, value.source_id) is None:
            raise HTTPException(status_code=404, detail="MSRP source not found")
        if value.repair_case_id:
            case = repo.get_repair_case(self.session, value.repair_case_id)
            if case is None:
                raise HTTPException(status_code=404, detail="Repair case not found")
            if case.target_id and case.target_id != value.target_id:
                raise HTTPException(
                    status_code=409,
                    detail="Repair case belongs to another monitoring target",
                )
        _host, official_domain = _verified_official_url(
            str(value.source_url),
            value.official_domain,
        )
        expected_sha256 = value.expected_sha256.casefold()
        upload_id = uuid4()
        existing_evidence = repo.get_evidence_by_sha256(
            self.session,
            value.target_id,
            expected_sha256,
        )
        upload = MsrpEvidenceUploadSession(
            upload_session_id=upload_id,
            target_id=value.target_id,
            source_id=value.source_id,
            repair_case_id=value.repair_case_id,
            completed_evidence_asset_id=(
                existing_evidence.evidence_asset_id if existing_evidence else None
            ),
            source_url=str(value.source_url),
            official_domain=official_domain,
            original_filename=Path(value.original_filename).name,
            expected_mime_type=value.expected_mime_type,
            expected_size_bytes=value.expected_size_bytes,
            expected_sha256=expected_sha256,
            chunk_size_bytes=value.chunk_size_bytes,
            received_parts_json=[],
            staging_key=f"uploads/{upload_id}",
            source_type=value.source_type,
            semantic_lane=value.semantic_lane,
            document_date=value.document_date,
            valid_from=value.valid_from,
            valid_until=value.valid_until,
            upload_status="completed" if existing_evidence else "initiated",
            expires_at_utc=_now() + timedelta(hours=24),
            created_by=actor,
            row_version=1,
        )
        repo.add(self.session, upload)
        if existing_evidence is None:
            (self.evidence_root / upload.staging_key).mkdir(
                parents=True,
                exist_ok=False,
            )
        self.session.flush()
        after = evidence_upload_payload(upload)
        self._audit(
            entity_type="evidence_upload_session",
            entity_id=str(upload.upload_session_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            after=after,
            metadata={
                "contentAlreadyExists": existing_evidence is not None,
            },
        )
        try:
            self._commit("Evidence upload or idempotency key already exists")
        except Exception:
            if existing_evidence is None:
                shutil.rmtree(
                    self.evidence_root / upload.staging_key,
                    ignore_errors=True,
                )
            raise
        self.session.refresh(upload)
        return evidence_upload_payload(upload)

    def upload_evidence_part(
        self,
        upload_session_id: UUID,
        part_number: int,
        content: bytes,
        part_sha256: str,
        *,
        actor: str,
        actor_role: str,
    ) -> dict[str, object]:
        upload = repo.get_evidence_upload_session(
            self.session,
            upload_session_id,
            for_update=True,
        )
        if upload is None:
            raise HTTPException(status_code=404, detail="Evidence upload not found")
        if upload.upload_status == "completed":
            return evidence_upload_payload(upload)
        if upload.upload_status in {"failed", "expired"}:
            raise HTTPException(status_code=409, detail="Evidence upload is closed")
        if upload.expires_at_utc <= _now():
            upload.upload_status = "expired"
            upload.row_version += 1
            upload.updated_at_utc = _now()
            self.session.commit()
            raise HTTPException(status_code=410, detail="Evidence upload expired")
        total_parts = (
            upload.expected_size_bytes + upload.chunk_size_bytes - 1
        ) // upload.chunk_size_bytes
        if part_number < 1 or part_number > total_parts:
            raise HTTPException(status_code=422, detail="Invalid upload part number")
        expected_size = (
            upload.chunk_size_bytes
            if part_number < total_parts
            else upload.expected_size_bytes
            - upload.chunk_size_bytes * (total_parts - 1)
        )
        if len(content) != expected_size:
            raise HTTPException(
                status_code=422,
                detail=f"Upload part must contain exactly {expected_size} bytes",
            )
        actual_hash = hashlib.sha256(content).hexdigest()
        if not hmac.compare_digest(actual_hash, part_sha256.casefold()):
            raise HTTPException(status_code=422, detail="Upload part SHA-256 mismatch")
        received_by_number = {
            int(item["partNumber"]): item for item in upload.received_parts_json
        }
        existing = received_by_number.get(part_number)
        part_path = self.evidence_root / upload.staging_key / f"part-{part_number:06d}"
        if existing is not None:
            if existing.get("sha256") != actual_hash:
                raise HTTPException(
                    status_code=409,
                    detail="Upload part already exists with another hash",
                )
            if part_path.exists():
                return evidence_upload_payload(upload)
        part_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = part_path.with_suffix(".tmp")
        temporary_path.write_bytes(content)
        os.replace(temporary_path, part_path)
        received_by_number[part_number] = {
            "partNumber": part_number,
            "sizeBytes": len(content),
            "sha256": actual_hash,
            "receivedAtUtc": _now().isoformat(),
        }
        upload.received_parts_json = [
            received_by_number[number] for number in sorted(received_by_number)
        ]
        upload.upload_status = "uploading"
        upload.row_version += 1
        upload.updated_at_utc = _now()
        self.session.flush()
        payload = evidence_upload_payload(upload)
        audit_key = f"{upload_session_id}:{part_number}:{actual_hash}"
        if (
            repo.get_audit_event_by_idempotency(
                self.session,
                action="evidence_upload.part",
                idempotency_key=audit_key,
            )
            is None
        ):
            self._audit(
                entity_type="evidence_upload_session",
                entity_id=str(upload_session_id),
                action="evidence_upload.part",
                actor=actor,
                actor_role=actor_role,
                idempotency_key=audit_key,
                after=payload,
                metadata={"partNumber": part_number, "partSha256": actual_hash},
            )
        self._commit("Upload part conflicted with another writer")
        self.session.refresh(upload)
        return evidence_upload_payload(upload)

    def complete_evidence_upload(
        self,
        upload_session_id: UUID,
        value: EvidenceUploadComplete,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "evidence_upload.complete"
        self._require_new_action(action, idempotency_key)
        upload = repo.get_evidence_upload_session(
            self.session,
            upload_session_id,
            for_update=True,
        )
        if upload is None:
            raise HTTPException(status_code=404, detail="Evidence upload not found")
        if upload.row_version != value.row_version:
            raise HTTPException(status_code=409, detail="Evidence upload changed")
        if upload.upload_status == "completed":
            evidence = (
                repo.get_evidence_asset(
                    self.session,
                    upload.completed_evidence_asset_id,
                )
                if upload.completed_evidence_asset_id
                else None
            )
            if evidence is None:
                raise HTTPException(
                    status_code=409,
                    detail="Completed upload has no Evidence Asset",
                )
            return {
                "upload": evidence_upload_payload(upload),
                "evidence": evidence_payload(evidence),
            }
        if upload.upload_status not in {"initiated", "uploading"}:
            raise HTTPException(status_code=409, detail="Evidence upload is closed")
        if upload.expires_at_utc <= _now():
            raise HTTPException(status_code=410, detail="Evidence upload expired")
        total_parts = (
            upload.expected_size_bytes + upload.chunk_size_bytes - 1
        ) // upload.chunk_size_bytes
        received_numbers = {
            int(item["partNumber"]) for item in upload.received_parts_json
        }
        required_numbers = set(range(1, total_parts + 1))
        if received_numbers != required_numbers:
            missing = sorted(required_numbers - received_numbers)
            raise HTTPException(
                status_code=409,
                detail={"message": "Evidence upload is incomplete", "missingParts": missing},
            )

        staging_dir = self.evidence_root / upload.staging_key
        assembled_path = staging_dir / "assembled.pdf.tmp"
        digest = hashlib.sha256()
        actual_size = 0
        with assembled_path.open("wb") as output:
            for part_number in range(1, total_parts + 1):
                part_path = staging_dir / f"part-{part_number:06d}"
                if not part_path.is_file():
                    raise HTTPException(
                        status_code=409,
                        detail=f"Evidence upload part {part_number} is missing on storage",
                    )
                with part_path.open("rb") as source:
                    while chunk := source.read(1024 * 1024):
                        digest.update(chunk)
                        actual_size += len(chunk)
                        output.write(chunk)
        actual_sha256 = digest.hexdigest()
        if actual_size != upload.expected_size_bytes:
            assembled_path.unlink(missing_ok=True)
            raise HTTPException(status_code=422, detail="Assembled PDF size mismatch")
        if not hmac.compare_digest(actual_sha256, upload.expected_sha256):
            assembled_path.unlink(missing_ok=True)
            raise HTTPException(status_code=422, detail="Assembled PDF SHA-256 mismatch")
        with assembled_path.open("rb") as pdf:
            signature = pdf.read(5)
            pdf.seek(max(0, actual_size - 2048))
            trailer = pdf.read()
        if signature != b"%PDF-" or b"%%EOF" not in trailer:
            assembled_path.unlink(missing_ok=True)
            raise HTTPException(status_code=422, detail="File is not a valid PDF payload")

        target = repo.get_target(self.session, upload.target_id, for_update=True)
        if target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        evidence = repo.get_evidence_by_sha256(
            self.session,
            upload.target_id,
            actual_sha256,
        )
        if evidence is None:
            storage_key = f"assets/{actual_sha256[:2]}/{actual_sha256}.pdf"
            final_path = self.evidence_root / storage_key
            final_path.parent.mkdir(parents=True, exist_ok=True)
            if final_path.exists():
                assembled_path.unlink(missing_ok=True)
            else:
                os.replace(assembled_path, final_path)
            evidence = MsrpSourceEvidenceAsset(
                evidence_asset_id=uuid4(),
                target_id=upload.target_id,
                source_id=upload.source_id,
                repair_case_id=upload.repair_case_id,
                evidence_type="uploaded_pdf",
                source_url=upload.source_url,
                final_url=upload.source_url,
                redirect_chain_json=[],
                official_domain_verified=True,
                filename=upload.original_filename,
                mime_type="application/pdf",
                mime_signature="%PDF-",
                size_bytes=actual_size,
                storage_key=storage_key,
                sha256=actual_sha256,
                captured_at_utc=_now(),
                document_date=upload.document_date,
                valid_from=upload.valid_from,
                valid_until=upload.valid_until,
                content_hash=actual_sha256,
                source_type=upload.source_type,
                semantic_lane=upload.semantic_lane,
                lifecycle_state="active",
                created_by=actor,
            )
            repo.add(self.session, evidence)
        else:
            assembled_path.unlink(missing_ok=True)
        before = evidence_upload_payload(upload)
        upload.completed_evidence_asset_id = evidence.evidence_asset_id
        upload.upload_status = "completed"
        upload.row_version += 1
        upload.updated_at_utc = _now()
        if upload.repair_case_id:
            case = repo.get_repair_case(
                self.session,
                upload.repair_case_id,
                for_update=True,
            )
            if case is not None:
                reference = {
                    "evidenceAssetId": str(evidence.evidence_asset_id),
                    "sha256": evidence.sha256,
                    "evidenceType": evidence.evidence_type,
                }
                case.evidence_refs_json = [*case.evidence_refs_json, reference]
                case.manual_evidence_required = False
                if case.case_status == "awaiting_evidence":
                    case.case_status = "open"
                case.row_version += 1
                case.updated_at_utc = _now()
        if target.monitoring_status == "manual_evidence_required":
            target.monitoring_status = "degraded"
            target.row_version += 1
            target.updated_at_utc = _now()
        self.session.flush()
        after = evidence_upload_payload(upload)
        evidence_item = evidence_payload(evidence)
        self._audit(
            entity_type="source_evidence_asset",
            entity_id=str(evidence.evidence_asset_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
            metadata={"evidence": evidence_item},
        )
        self._commit("Evidence completion or idempotency key conflicted")
        shutil.rmtree(staging_dir, ignore_errors=True)
        self.session.refresh(upload)
        self.session.refresh(evidence)
        return {
            "upload": evidence_upload_payload(upload),
            "evidence": evidence_payload(evidence),
        }

    def open_or_update_case(
        self,
        value: RepairCaseFindingCreate,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "repair_case.record_finding"
        self._require_new_action(action, idempotency_key)
        target = (
            repo.get_target(self.session, value.target_id)
            if value.target_id is not None
            else None
        )
        if value.target_id is not None and target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        if value.source_id and self.session.get(MsrpSource, value.source_id) is None:
            raise HTTPException(status_code=404, detail="MSRP source not found")
        observation = (
            self.session.get(MsrpObservation, value.observation_id)
            if value.observation_id is not None
            else None
        )
        if value.observation_id is not None and observation is None:
            raise HTTPException(status_code=404, detail="Observation not found")
        if value.fx_run_id and repo.get_fx_run(self.session, value.fx_run_id) is None:
            raise HTTPException(status_code=404, detail="FX run not found")
        if target is not None and observation is not None:
            if target.country.casefold() != observation.country.casefold():
                raise HTTPException(
                    status_code=422,
                    detail="Repair Case target country differs from observation",
                )
            if _normalized_segment(target.brand) != _normalized_segment(
                observation.brand
            ):
                raise HTTPException(
                    status_code=422,
                    detail="Repair Case target brand differs from observation",
                )
        dedupe_key = _case_dedupe_key(value)
        occurred_at = value.occurred_at_utc or _now()
        case = repo.get_open_case_by_dedupe_key(
            self.session,
            dedupe_key,
            for_update=True,
        )
        before = repair_case_payload(case) if case is not None else None
        evidence_refs = self._validated_evidence_references(
            value.evidence_refs,
            target_id=value.target_id,
        )
        manual_required = bool(
            value.manual_evidence_required
            or value.failure_classifier.casefold() in ANTI_BOT_FAILURES
        )
        if case is None:
            case = MsrpGovernanceRepairCase(
                case_id=uuid4(),
                open_dedupe_key=dedupe_key,
                repair_domain=value.repair_domain,
                target_id=value.target_id,
                source_id=value.source_id,
                observation_id=value.observation_id,
                mapping_reference=value.mapping_reference,
                fx_run_id=value.fx_run_id,
                case_type=value.case_type,
                failure_classifier=value.failure_classifier,
                severity=value.severity,
                priority=value.priority,
                first_seen_at_utc=occurred_at,
                last_seen_at_utc=occurred_at,
                occurrence_count=1,
                recent_run_ids_json=[value.run_id] if value.run_id else [],
                evidence_refs_json=evidence_refs,
                manual_evidence_required=manual_required,
                agent_run_refs_json=[],
                proposal_refs_json=[],
                case_status="awaiting_evidence" if manual_required else "open",
                owner=value.owner,
                created_by=actor,
                row_version=1,
            )
            repo.add(self.session, case)
        else:
            case.last_seen_at_utc = max(case.last_seen_at_utc, occurred_at)
            case.occurrence_count += 1
            case.priority = max(case.priority, value.priority)
            if SEVERITY_ORDER[value.severity] > SEVERITY_ORDER[case.severity]:
                case.severity = value.severity
            if value.run_id:
                run_ids = [*case.recent_run_ids_json, value.run_id]
                case.recent_run_ids_json = list(dict.fromkeys(run_ids))[-20:]
            if evidence_refs:
                merged = [*case.evidence_refs_json, *evidence_refs]
                seen: set[tuple[str, str]] = set()
                deduped: list[dict[str, object]] = []
                for item in merged:
                    key = (
                        str(item.get("evidenceAssetId") or ""),
                        str(item.get("sha256") or ""),
                    )
                    if key not in seen:
                        seen.add(key)
                        deduped.append(item)
                case.evidence_refs_json = deduped
            case.manual_evidence_required = (
                case.manual_evidence_required or manual_required
            )
            if manual_required:
                case.case_status = "awaiting_evidence"
            case.row_version += 1
            case.updated_at_utc = _now()

        if value.target_id and manual_required:
            target = repo.get_target(self.session, value.target_id, for_update=True)
            if target is not None:
                target.monitoring_status = "manual_evidence_required"
                target.row_version += 1
                target.updated_at_utc = _now()
        self.session.flush()
        after = repair_case_payload(case)
        self._audit(
            entity_type="repair_case",
            entity_id=str(case.case_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
        )
        self._commit("Repair finding or idempotency key already exists")
        self.session.refresh(case)
        return repair_case_payload(case)

    def ingest_source_run_result(
        self,
        value: SourceRunResultV1,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        target = repo.get_target_by_key(self.session, value.target_key)
        if target is None:
            raise HTTPException(
                status_code=404,
                detail="Source run target is not registered in Governance",
            )
        normalized_status = value.status.casefold()
        if normalized_status in {"success", "succeeded", "passed", "complete"}:
            action = "source_run_result.accept"
            self._require_new_action(action, idempotency_key)
            target = repo.get_target(self.session, target.target_id, for_update=True)
            if target is None:
                raise HTTPException(
                    status_code=404,
                    detail="Monitoring target not found",
                )
            before = target_payload(target)
            if (
                value.published_source_version_id is not None
                and value.published_source_version_id
                == target.active_source_version_id
            ):
                target.monitoring_status = "active"
                target.row_version += 1
                target.updated_at_utc = _now()
            after = target_payload(target)
            result_payload = value.model_dump(mode="json", by_alias=True)
            self._audit(
                entity_type="monitoring_target",
                entity_id=str(target.target_id),
                action=action,
                actor=actor,
                actor_role=actor_role,
                idempotency_key=idempotency_key,
                before=before,
                after=after,
                metadata={"sourceRunResult": result_payload},
            )
            self._commit("Source run result or idempotency key already exists")
            return {"accepted": True, "case": None, "target": after}

        failure = (value.failure_class or "unknown_source_failure").casefold()
        repair_domain = "runtime"
        classifiers = {
            "parser": ("parse", "parser", "selector", "json_path", "pdf_layout"),
            "source": (
                "url",
                "redirect",
                "not_found",
                "stale",
                "anti_bot",
                "captcha",
                "access_denied",
            ),
            "semantic": ("semantic", "price_type", "currency", "tax"),
            "mapping": ("mapping", "match"),
            "fx": ("fx", "exchange_rate"),
        }
        for domain, tokens in classifiers.items():
            if any(token in failure for token in tokens):
                repair_domain = domain
                break
        case = self.open_or_update_case(
            RepairCaseFindingCreate(
                repair_domain=repair_domain,
                target_id=target.target_id,
                source_id=value.runtime_source_id,
                case_type=f"source_run:{normalized_status}",
                failure_classifier=failure,
                severity="high" if value.retryability == "non_retryable" else "medium",
                priority=80 if value.retryability == "non_retryable" else 55,
                run_id=value.run_id,
                evidence_refs=value.evidence_refs,
                manual_evidence_required=failure in ANTI_BOT_FAILURES,
                occurred_at_utc=value.completed_at,
            ),
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
        )
        return {"accepted": True, "case": case, "target": target_payload(target)}

    def ingest_monitor_anomaly(
        self,
        value: MonitorAnomalyV1,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        target_key = build_target_key(
            value.country,
            value.brand,
            value.model,
            None,
            None,
        )
        target = repo.get_target_by_key(self.session, target_key)
        if target is None:
            raise HTTPException(
                status_code=404,
                detail="Monitor anomaly target is not registered in Governance",
            )
        case = self.open_or_update_case(
            RepairCaseFindingCreate(
                repair_domain=value.suspected_repair_domain,
                target_id=target.target_id,
                observation_id=(
                    value.observation_ids[0] if value.observation_ids else None
                ),
                case_type=f"monitor_anomaly:{value.movement_type.casefold()}",
                failure_classifier=(
                    f"monitor_{value.suspected_repair_domain}_"
                    f"{value.movement_type.casefold()}"
                ),
                severity="high",
                priority=75,
                run_id=value.anomaly_id,
                evidence_refs=value.evidence_refs,
                occurred_at_utc=value.detected_at,
            ),
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
        )
        return {"accepted": True, "case": case, "target": target_payload(target)}

    def list_cases(
        self,
        *,
        repair_domain: str | None,
        case_status: str | None,
        severity: str | None,
        target_id: UUID | None,
        limit: int,
        offset: int,
    ) -> dict[str, object]:
        total, rows = repo.list_repair_cases(
            self.session,
            repair_domain=repair_domain,
            case_status=case_status,
            severity=severity,
            target_id=target_id,
            limit=limit,
            offset=offset,
        )
        return {
            "rows": len(rows),
            "total": total,
            "items": [repair_case_payload(row) for row in rows],
        }

    def list_conflicts(self, *, limit: int, offset: int) -> dict[str, object]:
        total, rows = repo.list_unresolved_conflicts(
            self.session,
            limit=limit,
            offset=offset,
        )
        return {
            "rows": len(rows),
            "total": total,
            "items": [repair_case_payload(row) for row in rows],
        }

    def get_case_detail(self, case_id: UUID) -> dict[str, object]:
        case = repo.get_repair_case(self.session, case_id)
        if case is None:
            raise HTTPException(status_code=404, detail="Repair case not found")
        proposals = repo.list_proposals_for_case(self.session, case_id)
        return {
            "item": repair_case_payload(case),
            "proposals": [proposal_payload(row) for row in proposals],
        }

    def create_proposal(
        self,
        case_id: UUID,
        value: RepairProposalCreate,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "repair_proposal.create"
        self._require_new_action(action, idempotency_key)
        case = repo.get_repair_case(self.session, case_id, for_update=True)
        if case is None:
            raise HTTPException(status_code=404, detail="Repair case not found")
        if case.case_status not in OPEN_CASE_STATUSES:
            raise HTTPException(status_code=409, detail="Repair case is closed")
        if (
            value.proposal_origin == "hermes_agent"
            and value.agent_run_id not in case.agent_run_refs_json
        ):
            raise HTTPException(
                status_code=409,
                detail="Hermes proposal Agent Run is not registered on this Case",
            )
        input_evidence_refs = self._validated_evidence_references(
            value.input_evidence_refs,
            target_id=case.target_id,
        )
        if value.source_version_id:
            source_version = repo.get_source_version(
                self.session,
                value.source_version_id,
            )
            if source_version is None:
                raise HTTPException(status_code=404, detail="Source Version not found")
            if case.target_id and source_version.target_id != case.target_id:
                raise HTTPException(
                    status_code=409,
                    detail="Source Version belongs to another Case target",
                )
            if case.source_id and source_version.source_id != case.source_id:
                raise HTTPException(
                    status_code=409,
                    detail="Source Version belongs to another Case source",
                )
        proposal = MsrpRepairProposal(
            proposal_id=uuid4(),
            case_id=case_id,
            target_id=case.target_id,
            source_id=case.source_id,
            source_version_id=value.source_version_id,
            proposal_origin=value.proposal_origin,
            proposal_type=value.proposal_type,
            agent_run_id=value.agent_run_id,
            agent_step_id=value.agent_step_id,
            dpv4_metadata_json=value.dpv4_metadata,
            input_evidence_refs_json=input_evidence_refs,
            proposed_change_json=value.proposed_change,
            field_diff_json=value.field_diff,
            assumptions_json=value.assumptions,
            unresolved_questions_json=value.unresolved_questions,
            risk_flags_json=value.risk_flags,
            proposal_status="draft",
            author=actor,
        )
        repo.add(self.session, proposal)
        case.proposal_refs_json = [
            *case.proposal_refs_json,
            str(proposal.proposal_id),
        ]
        case.case_status = "proposal_ready"
        case.row_version += 1
        case.updated_at_utc = _now()
        self.session.flush()
        after = proposal_payload(proposal)
        self._audit(
            entity_type="repair_proposal",
            entity_id=str(proposal.proposal_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            after=after,
        )
        self._commit("Proposal or idempotency key already exists")
        self.session.refresh(proposal)
        return proposal_payload(proposal)

    def get_proposal(self, proposal_id: UUID) -> dict[str, object]:
        proposal = repo.get_proposal(self.session, proposal_id)
        if proposal is None:
            raise HTTPException(status_code=404, detail="Proposal not found")
        return proposal_payload(proposal)

    def record_proposal_verification(
        self,
        proposal_id: UUID,
        value: ProposalVerificationUpdate,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "repair_proposal.record_verification"
        self._require_new_action(action, idempotency_key)
        proposal = repo.get_proposal(self.session, proposal_id, for_update=True)
        if proposal is None:
            raise HTTPException(status_code=404, detail="Proposal not found")
        if proposal.proposal_status in {"approved", "rejected", "published", "superseded"}:
            raise HTTPException(status_code=409, detail="Proposal is no longer editable")
        before = proposal_payload(proposal)
        proposal.validation_result_json = value.validation_result
        proposal.dryrun_result_json = value.dryrun_result
        proposal.replay_result_json = value.replay_result
        proposal.conflict_result_json = value.conflict_result
        proposal.gate_result_json = (
            value.gate_result.model_dump(mode="json", by_alias=True)
            if value.gate_result
            else None
        )
        validation_passed = _passed(value.validation_result)
        dryrun_passed = _passed(value.dryrun_result)
        conflict_blocked = _blocking_conflict(value.conflict_result)
        if validation_passed and dryrun_passed and not conflict_blocked:
            proposal.proposal_status = "dryrun_passed"
        elif validation_passed:
            proposal.proposal_status = "validated"
        else:
            proposal.proposal_status = "draft"
        proposal.updated_at_utc = _now()
        case = repo.get_repair_case(self.session, proposal.case_id, for_update=True)
        if case is not None:
            case.case_status = (
                "dryrun_passed"
                if proposal.proposal_status == "dryrun_passed"
                else "proposal_ready"
            )
            case.row_version += 1
            case.updated_at_utc = _now()
        self.session.flush()
        after = proposal_payload(proposal)
        self._audit(
            entity_type="repair_proposal",
            entity_id=str(proposal.proposal_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
        )
        self._commit("Verification result or idempotency key already exists")
        self.session.refresh(proposal)
        return proposal_payload(proposal)

    def submit_proposal(
        self,
        proposal_id: UUID,
        value: SubmitProposalRequest,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "repair_proposal.submit"
        self._require_new_action(action, idempotency_key)
        proposal = repo.get_proposal(self.session, proposal_id, for_update=True)
        if proposal is None:
            raise HTTPException(status_code=404, detail="Proposal not found")
        if proposal.proposal_status != value.expected_status:
            raise HTTPException(status_code=409, detail="Proposal status changed")
        if proposal.proposal_status != "dryrun_passed":
            raise HTTPException(
                status_code=409,
                detail="Only a Dryrun-passed proposal can be submitted",
            )
        before = proposal_payload(proposal)
        proposal.proposal_status = "submitted"
        proposal.updated_at_utc = _now()
        case = repo.get_repair_case(self.session, proposal.case_id, for_update=True)
        if case is not None:
            case.case_status = "awaiting_approval"
            case.row_version += 1
            case.updated_at_utc = _now()
        self.session.flush()
        after = proposal_payload(proposal)
        self._audit(
            entity_type="repair_proposal",
            entity_id=str(proposal.proposal_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
        )
        self._commit("Proposal submission or idempotency key already exists")
        self.session.refresh(proposal)
        return proposal_payload(proposal)

    def create_source_version(
        self,
        value: SourceVersionCreate,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "source_version.create"
        self._require_new_action(action, idempotency_key)
        source = self.session.get(MsrpSource, value.source_id)
        if source is None:
            raise HTTPException(status_code=404, detail="MSRP source not found")
        target = repo.get_target(self.session, value.target_id, for_update=True)
        if target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        proposal = repo.get_proposal(self.session, value.proposal_id, for_update=True)
        if proposal is None:
            raise HTTPException(status_code=404, detail="Proposal not found")
        if proposal.proposal_status not in {"dryrun_passed", "submitted"}:
            raise HTTPException(
                status_code=409,
                detail="Source Version requires a Dryrun-passed proposal",
            )
        if proposal.target_id and proposal.target_id != value.target_id:
            raise HTTPException(status_code=409, detail="Proposal target mismatch")
        if proposal.source_id and proposal.source_id != value.source_id:
            raise HTTPException(status_code=409, detail="Proposal source mismatch")
        case = repo.get_repair_case(self.session, proposal.case_id)
        if case is None:
            raise HTTPException(status_code=404, detail="Repair case not found")
        if case.repair_domain not in {"source", "parser", "semantic", "runtime"}:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Only source, parser, semantic, or runtime repairs can create "
                    "a Source Version"
                ),
            )

        parsed_yaml = yaml.safe_load(value.profile_yaml)
        if parsed_yaml != value.profile:
            raise HTTPException(
                status_code=422,
                detail="profile_yaml does not round-trip to the typed profile",
            )
        evidence_refs = self._validated_evidence_references(
            value.evidence_refs,
            target_id=value.target_id,
            required=True,
        )

        previous = (
            repo.get_source_version(self.session, value.previous_version_id)
            if value.previous_version_id
            else repo.get_published_source_version(self.session, value.source_id)
        )
        if previous is not None and previous.source_id != value.source_id:
            raise HTTPException(status_code=409, detail="Previous Source Version mismatch")
        profile_sha256 = hashlib.sha256(value.profile_yaml.encode("utf-8")).hexdigest()
        version = MsrpSourceVersion(
            source_version_id=uuid4(),
            source_id=value.source_id,
            target_id=value.target_id,
            version_number=repo.next_source_version_number(
                self.session,
                value.source_id,
            ),
            profile_json=value.profile,
            profile_yaml=value.profile_yaml,
            profile_sha256=profile_sha256,
            evidence_refs_json=evidence_refs,
            extractor_name=value.extractor_name,
            extractor_type=value.extractor_type,
            extractor_version=value.extractor_version,
            semantic_lane=value.semantic_lane,
            currency=value.currency.upper(),
            tax_mode=value.tax_mode,
            valid_from=value.valid_from,
            valid_until=value.valid_until,
            previous_version_id=previous.source_version_id if previous else None,
            validation_summary_json=proposal.validation_result_json,
            dryrun_summary_json=proposal.dryrun_result_json,
            replay_summary_json=proposal.replay_result_json,
            conflict_summary_json=proposal.conflict_result_json,
            gate_result_json=proposal.gate_result_json,
            version_status="dryrun_passed",
            created_by=actor,
        )
        repo.add(self.session, version)
        proposal.source_version_id = version.source_version_id
        proposal.updated_at_utc = _now()
        self.session.flush()
        after = source_version_payload(version)
        self._audit(
            entity_type="source_version",
            entity_id=str(version.source_version_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            after=after,
            metadata={"proposalId": str(value.proposal_id)},
        )
        self._commit("Source Version, profile, or idempotency key already exists")
        self.session.refresh(version)
        return source_version_payload(version)

    def get_source_version(self, source_version_id: UUID) -> dict[str, object]:
        version = repo.get_source_version(self.session, source_version_id)
        if version is None:
            raise HTTPException(status_code=404, detail="Source Version not found")
        return source_version_payload(version)

    def publish_source_version(
        self,
        source_version_id: UUID,
        value: PublishSourceVersionRequest,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "source_version.publish"
        self._require_new_action(action, idempotency_key)
        version = repo.get_source_version(
            self.session,
            source_version_id,
            for_update=True,
        )
        if version is None:
            raise HTTPException(status_code=404, detail="Source Version not found")
        if version.version_status not in {"dryrun_passed", "approved"}:
            raise HTTPException(
                status_code=409,
                detail="Only a Dryrun-passed Source Version can be published",
            )
        if not _passed(version.validation_summary_json) or not _passed(
            version.dryrun_summary_json
        ):
            raise HTTPException(
                status_code=409,
                detail="Validation and targeted Dryrun must both pass",
            )
        if _blocking_conflict(version.conflict_summary_json):
            raise HTTPException(status_code=409, detail="Blocking source conflict remains")
        target = repo.get_target(self.session, version.target_id, for_update=True)
        if target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        if target.row_version != value.target_row_version:
            raise HTTPException(status_code=409, detail="Monitoring target changed")

        before = source_version_payload(version)
        current = repo.get_published_source_version(
            self.session,
            version.source_id,
            for_update=True,
        )
        if current is not None and current.source_version_id != version.source_version_id:
            current.version_status = "superseded"
            current.updated_at_utc = _now()
            self.session.flush()
        now = _now()
        version.version_status = "published"
        version.approved_by = actor
        version.approved_at_utc = now
        version.published_at_utc = now
        version.decision_reason = value.decision_reason
        version.updated_at_utc = now
        previous_active_id = target.active_source_version_id
        target.active_source_version_id = version.source_version_id
        target.fallback_source_version_id = previous_active_id or version.source_version_id
        target.monitoring_status = "active"
        target.row_version += 1
        target.updated_at_utc = now
        self.session.flush()
        after = source_version_payload(version)
        self._audit(
            entity_type="source_version",
            entity_id=str(version.source_version_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
            metadata={
                "targetId": str(target.target_id),
                "replacedSourceVersionId": str(current.source_version_id)
                if current
                else None,
            },
        )
        self._commit("Source Version publish or idempotency key conflicted")
        self.session.refresh(version)
        return source_version_payload(version)

    def rollback_source_version(
        self,
        source_version_id: UUID,
        value: PublishSourceVersionRequest,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "source_version.rollback"
        self._require_new_action(action, idempotency_key)
        current = repo.get_source_version(
            self.session,
            source_version_id,
            for_update=True,
        )
        if current is None:
            raise HTTPException(status_code=404, detail="Source Version not found")
        if current.version_status != "published":
            raise HTTPException(status_code=409, detail="Source Version is not active")
        target = repo.get_target(self.session, current.target_id, for_update=True)
        if target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        if target.row_version != value.target_row_version:
            raise HTTPException(status_code=409, detail="Monitoring target changed")
        candidate_id = current.previous_version_id
        if candidate_id is None and target.fallback_source_version_id != current.source_version_id:
            candidate_id = target.fallback_source_version_id
        if candidate_id is None:
            raise HTTPException(status_code=409, detail="No prior Source Version to restore")
        candidate = repo.get_source_version(
            self.session,
            candidate_id,
            for_update=True,
        )
        if (
            candidate is None
            or candidate.source_id != current.source_id
            or candidate.target_id != current.target_id
            or candidate.version_status not in {"superseded", "rolled_back"}
            or not _passed(candidate.validation_summary_json)
            or not _passed(candidate.dryrun_summary_json)
            or _blocking_conflict(candidate.conflict_summary_json)
        ):
            raise HTTPException(status_code=409, detail="Rollback candidate is invalid")
        before = source_version_payload(current)
        now = _now()
        current.version_status = "rolled_back"
        current.decision_reason = value.decision_reason
        current.updated_at_utc = now
        self.session.flush()
        candidate.version_status = "published"
        candidate.approved_by = actor
        candidate.approved_at_utc = now
        candidate.published_at_utc = now
        candidate.decision_reason = f"Rollback: {value.decision_reason}"
        candidate.updated_at_utc = now
        target.active_source_version_id = candidate.source_version_id
        target.fallback_source_version_id = candidate.source_version_id
        target.monitoring_status = "active"
        target.row_version += 1
        target.updated_at_utc = now
        self.session.flush()
        after = source_version_payload(candidate)
        self._audit(
            entity_type="source_version",
            entity_id=str(current.source_version_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
            metadata={"restoredSourceVersionId": str(candidate.source_version_id)},
        )
        self._commit("Source Version rollback or idempotency key conflicted")
        self.session.refresh(candidate)
        return source_version_payload(candidate)

    def request_hermes_diagnosis(
        self,
        case_id: UUID,
        value: HermesDiagnosisRequest,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "repair_case.request_hermes_diagnosis"
        self._require_new_action(action, idempotency_key)
        case = repo.get_repair_case(self.session, case_id, for_update=True)
        if case is None:
            raise HTTPException(status_code=404, detail="Repair case not found")
        if case.case_status not in OPEN_CASE_STATUSES:
            raise HTTPException(status_code=409, detail="Repair case is closed")
        if case.target_id is None:
            raise HTTPException(
                status_code=409,
                detail="Hermes diagnosis requires a monitoring target",
            )
        target = repo.get_target(self.session, case.target_id)
        if target is None:
            raise HTTPException(status_code=404, detail="Monitoring target not found")
        run_id = str(uuid4())
        request = AgentRunRequestV1(
            run_id=run_id,
            case_id=case.case_id,
            target_id=case.target_id,
            repair_domain=case.repair_domain,
            evidence_refs=case.evidence_refs_json,
            current_source_version_id=target.active_source_version_id,
            last_known_good_version_id=target.fallback_source_version_id,
            source_gate_snapshot=value.source_gate_snapshot,
            mapping_gate_snapshot=value.mapping_gate_snapshot,
            fx_gate_snapshot=value.fx_gate_snapshot,
            allowed_tool_ids=value.allowed_tool_ids,
            authority_policy_version=value.authority_policy_version,
            composer_policy_version=value.composer_policy_version,
            attempt_budget=value.attempt_budget,
            time_budget_seconds=value.time_budget_seconds,
            token_budget=value.token_budget,
            cost_budget_usd=value.cost_budget_usd,
            requested_by=actor,
        )
        before = repair_case_payload(case)
        case.agent_run_refs_json = [*case.agent_run_refs_json, run_id]
        case.case_status = "diagnosing"
        case.row_version += 1
        case.updated_at_utc = _now()
        self.session.flush()
        after = repair_case_payload(case)
        request_payload = request.model_dump(mode="json", by_alias=True)
        self._audit(
            entity_type="repair_case",
            entity_id=str(case.case_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
            metadata={"agentRunRequest": request_payload},
        )
        self._commit("Hermes request or idempotency key already exists")
        return request_payload

    def record_hermes_run_result(
        self,
        case_id: UUID,
        value: AgentRunResultV1,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "repair_case.record_hermes_run_result"
        self._require_new_action(action, idempotency_key)
        case = repo.get_repair_case(self.session, case_id, for_update=True)
        if case is None:
            raise HTTPException(status_code=404, detail="Repair case not found")
        if value.run_id not in case.agent_run_refs_json:
            raise HTTPException(
                status_code=409,
                detail="Hermes Agent Run is not registered on this Case",
            )
        normalized_status = value.status.casefold()
        if normalized_status not in TERMINAL_AGENT_RUN_STATUSES:
            raise HTTPException(
                status_code=422,
                detail="AgentRunResult must contain a terminal status",
            )

        verified_proposal_refs: list[str] = []
        for reference in value.proposal_refs:
            try:
                proposal_id = UUID(reference)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid Proposal reference: {reference}",
                ) from exc
            proposal = repo.get_proposal(self.session, proposal_id)
            if proposal is None or proposal.case_id != case.case_id:
                raise HTTPException(
                    status_code=422,
                    detail=f"Proposal does not belong to the Case: {reference}",
                )
            verified_proposal_refs.append(reference)

        before = repair_case_payload(case)
        if verified_proposal_refs:
            case.proposal_refs_json = list(
                dict.fromkeys([*case.proposal_refs_json, *verified_proposal_refs])
            )
        escalation_payload = value.human_escalation or {}
        escalation_text = json.dumps(
            escalation_payload,
            sort_keys=True,
            default=str,
        ).casefold()
        needs_evidence = any(
            token in escalation_text
            for token in ("evidence", "official_url", "url", "pdf", "upload")
        )
        if case.case_status not in {"dryrun_passed", "awaiting_approval"}:
            if verified_proposal_refs:
                case.case_status = "proposal_ready"
            elif value.human_escalation:
                case.case_status = "awaiting_evidence" if needs_evidence else "paused"
            else:
                case.case_status = "open"
        if needs_evidence:
            case.manual_evidence_required = True
        case.row_version += 1
        case.updated_at_utc = _now()
        self.session.flush()
        after = repair_case_payload(case)
        result_payload = value.model_dump(mode="json", by_alias=True)
        self._audit(
            entity_type="repair_case",
            entity_id=str(case.case_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
            metadata={
                "agentRunResult": result_payload,
                "agentRuntimePersistence": "hermes_owned",
                "dpv4Actor": False,
            },
        )
        self._commit("Hermes result or idempotency key already exists")
        self.session.refresh(case)
        return {"case": repair_case_payload(case), "agentRunResult": result_payload}

    def resolve_case(
        self,
        case_id: UUID,
        value: ResolveRepairCaseRequest,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "repair_case.resolve"
        self._require_new_action(action, idempotency_key)
        case = repo.get_repair_case(self.session, case_id, for_update=True)
        if case is None:
            raise HTTPException(status_code=404, detail="Repair case not found")
        if case.row_version != value.row_version:
            raise HTTPException(status_code=409, detail="Repair case changed")
        if case.case_status not in OPEN_CASE_STATUSES:
            raise HTTPException(status_code=409, detail="Repair case is already closed")
        before = repair_case_payload(case)
        case.case_status = "resolved"
        case.open_dedupe_key = None
        case.resolution_json = value.resolution
        case.row_version += 1
        case.updated_at_utc = _now()
        self.session.flush()
        after = repair_case_payload(case)
        self._audit(
            entity_type="repair_case",
            entity_id=str(case.case_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
        )
        self._commit("Repair case resolution or idempotency key conflicted")
        self.session.refresh(case)
        return repair_case_payload(case)

    def create_result_correction(
        self,
        value: ResultCorrectionCreate,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "result_correction.create"
        self._require_new_action(action, idempotency_key)
        observation = self.session.get(
            MsrpObservation,
            value.original_observation_id,
        )
        if observation is None:
            raise HTTPException(status_code=404, detail="Original observation not found")
        gate_decision = self._require_persisted_gate_decision(
            value.gate_decision_id,
            value.original_observation_id,
            normalized_lane=False,
        )
        gate_snapshot = gate_decision_payload(gate_decision)
        if not _passed(value.replay_result):
            raise HTTPException(status_code=409, detail="Correction replay did not pass")
        if value.original_current_price_id:
            current = self.session.get(CurrentPrice, value.original_current_price_id)
            if current is None:
                raise HTTPException(status_code=404, detail="CurrentPrice not found")
            if current.effective_observation_id != value.original_observation_id:
                raise HTTPException(
                    status_code=422,
                    detail="CurrentPrice does not originate from the observation",
                )
        if value.original_price_history_id:
            period = self.session.get(PriceHistory, value.original_price_history_id)
            if period is None:
                raise HTTPException(status_code=404, detail="PriceHistory period not found")
            period_observation_ids = {
                period.started_by_observation_id,
                period.ended_by_observation_id,
                period.last_confirmed_by_observation_id,
            }
            if value.original_observation_id not in period_observation_ids:
                raise HTTPException(
                    status_code=422,
                    detail="PriceHistory does not reference the observation",
                )
        if value.source_version_id:
            source_version = repo.get_source_version(
                self.session,
                value.source_version_id,
            )
            if source_version is None:
                raise HTTPException(status_code=404, detail="Source Version not found")
            if source_version.version_status != "published":
                raise HTTPException(
                    status_code=409,
                    detail="Result correction requires a published Source Version",
                )
            if source_version.target_id != gate_decision.target_id:
                raise HTTPException(
                    status_code=422,
                    detail="Source Version belongs to another Gate target",
                )
        evidence_refs = self._validated_evidence_references(
            value.evidence_refs,
            target_id=gate_decision.target_id,
            required=True,
        )
        correction = MsrpResultCorrectionDecision(
            correction_decision_id=uuid4(),
            original_observation_id=value.original_observation_id,
            gate_decision_id=gate_decision.gate_decision_id,
            original_current_price_id=value.original_current_price_id,
            original_price_history_id=value.original_price_history_id,
            correction_type=value.correction_type,
            reason=value.reason,
            evidence_refs_json=evidence_refs,
            source_version_id=value.source_version_id,
            corrected_inputs_json=value.corrected_inputs,
            replay_result_json=value.replay_result,
            gate_result_json=gate_snapshot,
            rematerialization_refs_json=[],
            decision_status="submitted",
            created_by=actor,
        )
        repo.add(self.session, correction)
        self.session.flush()
        after = result_correction_payload(correction)
        self._audit(
            entity_type="result_correction_decision",
            entity_id=str(correction.correction_decision_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            after=after,
            metadata={"factMutationPerformed": False},
        )
        self._commit("Result correction or idempotency key already exists")
        self.session.refresh(correction)
        return result_correction_payload(correction)

    def approve_result_correction(
        self,
        correction_id: UUID,
        value: ApprovalRequest,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "result_correction.approve"
        self._require_new_action(action, idempotency_key)
        correction = repo.get_result_correction(
            self.session,
            correction_id,
            for_update=True,
        )
        if correction is None:
            raise HTTPException(status_code=404, detail="Result correction not found")
        if correction.decision_status != value.expected_status:
            raise HTTPException(status_code=409, detail="Result correction status changed")
        if correction.decision_status != "submitted":
            raise HTTPException(
                status_code=409,
                detail="Only a submitted correction can be approved",
            )
        gate_result = correction.gate_result_json or {}
        if not gate_result.get("eligibleForLocalMaterialization"):
            raise HTTPException(status_code=409, detail="Correction Gate is not eligible")
        before = result_correction_payload(correction)
        correction.decision_status = "approved"
        correction.approved_by = actor
        correction.approved_at_utc = _now()
        correction.updated_at_utc = _now()
        self.session.flush()
        after = result_correction_payload(correction)
        self._audit(
            entity_type="result_correction_decision",
            entity_id=str(correction.correction_decision_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
            metadata={
                "decisionReason": value.decision_reason,
                "factMutationPerformed": False,
                "nextAction": "controlled_rematerialization",
            },
        )
        self._commit("Result correction approval or idempotency key conflicted")
        self.session.refresh(correction)
        return result_correction_payload(correction)

    def create_fx_run(
        self,
        value: FxNormalizationCreate,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "fx_normalization.create"
        self._require_new_action(action, idempotency_key)
        observation = self.session.get(MsrpObservation, value.observation_id)
        if observation is None:
            raise HTTPException(status_code=404, detail="Observation not found")
        gate_decision = self._require_persisted_gate_decision(
            value.gate_decision_id,
            value.observation_id,
            normalized_lane=True,
        )
        gate_snapshot = gate_decision_payload(gate_decision)
        local_currency = value.local_currency.upper()
        if local_currency != str(observation.source_currency).upper():
            raise HTTPException(
                status_code=422,
                detail="FX local currency differs from immutable observation",
            )
        if Decimal(str(observation.source_msrp_value)) != value.local_value:
            raise HTTPException(
                status_code=422,
                detail="FX local value differs from immutable observation",
            )
        normalized_value = (
            value.local_value * value.rate_to_normalized
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        fx_run = MsrpFxNormalizationRun(
            fx_run_id=uuid4(),
            observation_id=value.observation_id,
            gate_decision_id=gate_decision.gate_decision_id,
            local_currency=local_currency,
            local_value=value.local_value,
            fx_provider=value.fx_provider,
            rate_to_normalized=value.rate_to_normalized,
            rate_effective_date=value.rate_effective_date,
            rate_retrieved_at_utc=value.rate_retrieved_at_utc,
            policy_version=value.policy_version,
            normalized_currency=value.normalized_currency.upper(),
            normalized_value=normalized_value,
            gate_result_json=gate_snapshot,
            run_status="validated",
            created_by=actor,
        )
        repo.add(self.session, fx_run)
        self.session.flush()
        after = fx_run_payload(fx_run)
        self._audit(
            entity_type="fx_normalization_run",
            entity_id=str(fx_run.fx_run_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            after=after,
            metadata={
                "immutableLocalCurrency": str(observation.source_currency),
                "immutableLocalValue": str(observation.source_msrp_value),
                "localFactMutationPerformed": False,
            },
        )
        self._commit("FX run or idempotency key already exists")
        self.session.refresh(fx_run)
        return fx_run_payload(fx_run)

    def approve_fx_run(
        self,
        fx_run_id: UUID,
        value: ApprovalRequest,
        *,
        actor: str,
        actor_role: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        action = "fx_normalization.approve"
        self._require_new_action(action, idempotency_key)
        fx_run = repo.get_fx_run(self.session, fx_run_id, for_update=True)
        if fx_run is None:
            raise HTTPException(status_code=404, detail="FX run not found")
        if fx_run.run_status != value.expected_status:
            raise HTTPException(status_code=409, detail="FX run status changed")
        if fx_run.run_status != "validated":
            raise HTTPException(
                status_code=409,
                detail="Only a validated FX run can be approved",
            )
        gate_result = fx_run.gate_result_json or {}
        fx_gate = gate_result.get("fxGate") or {}
        if (
            fx_gate.get("status") != "pass"
            or not gate_result.get("eligibleForNormalizedMaterialization")
        ):
            raise HTTPException(status_code=409, detail="FX Gate is not eligible")
        before = fx_run_payload(fx_run)
        now = _now()
        previous_runs = repo.list_approved_fx_runs_for_observation(
            self.session,
            fx_run.observation_id,
            for_update=True,
        )
        for previous in previous_runs:
            if previous.fx_run_id == fx_run.fx_run_id:
                continue
            previous.run_status = "superseded"
            previous.superseded_run_id = fx_run.fx_run_id
        fx_run.run_status = "approved"
        fx_run.decision_reason = value.decision_reason
        fx_run.approved_by = actor
        fx_run.approved_at_utc = now
        self.session.flush()
        after = fx_run_payload(fx_run)
        self._audit(
            entity_type="fx_normalization_run",
            entity_id=str(fx_run.fx_run_id),
            action=action,
            actor=actor,
            actor_role=actor_role,
            idempotency_key=idempotency_key,
            before=before,
            after=after,
            metadata={"localFactMutationPerformed": False},
        )
        self._commit("FX approval or idempotency key conflicted")
        self.session.refresh(fx_run)
        return fx_run_payload(fx_run)

    def get_result_correction(self, correction_id: UUID) -> dict[str, object]:
        row = repo.get_result_correction(self.session, correction_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Result correction not found")
        return result_correction_payload(row)

    def list_fx_runs(
        self,
        *,
        observation_id: UUID | None,
        run_status: str | None,
        limit: int,
    ) -> dict[str, object]:
        rows = repo.list_fx_runs(
            self.session,
            observation_id=observation_id,
            run_status=run_status,
            limit=limit,
        )
        return {"rows": len(rows), "items": [fx_run_payload(row) for row in rows]}

    def list_audit_events(
        self,
        *,
        entity_type: str | None,
        entity_id: str | None,
        limit: int,
    ) -> dict[str, object]:
        rows = repo.list_audit_events(
            self.session,
            entity_type=entity_type,
            entity_id=entity_id,
            limit=limit,
        )
        return {
            "rows": len(rows),
            "items": [audit_event_payload(row) for row in rows],
        }
