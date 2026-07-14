from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

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


def _value(value):
    if isinstance(value, (UUID, date, datetime)):
        return value.isoformat() if not isinstance(value, UUID) else str(value)
    if isinstance(value, Decimal):
        return str(value)
    return value


def target_payload(row: MsrpMonitoringTarget) -> dict[str, object]:
    return {
        "targetId": str(row.target_id),
        "targetKey": row.target_key,
        "country": row.country,
        "brand": row.brand,
        "model": row.model,
        "trimScope": row.trim_scope,
        "powertrainScope": row.powertrain_scope,
        "rosterType": row.roster_type,
        "rosterRank": row.roster_rank,
        "monitoringStatus": row.monitoring_status,
        "activeSourceVersionId": _value(row.active_source_version_id),
        "fallbackSourceVersionId": _value(row.fallback_source_version_id),
        "schedule": row.schedule_json,
        "owner": row.owner,
        "notes": row.notes,
        "rowVersion": row.row_version,
        "createdAtUtc": _value(row.created_at_utc),
        "updatedAtUtc": _value(row.updated_at_utc),
    }


def gate_decision_payload(row: MsrpGovernanceGateDecision) -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "gateDecisionId": str(row.gate_decision_id),
        "targetId": str(row.target_id),
        "observationId": str(row.observation_id),
        "sourceGate": row.source_gate_json,
        "mappingGate": row.mapping_gate_json,
        "fxGate": row.fx_gate_json,
        "eligibleForLocalMaterialization": (
            row.eligible_for_local_materialization
        ),
        "eligibleForNormalizedMaterialization": (
            row.eligible_for_normalized_materialization
        ),
        "evaluatedAt": _value(row.evaluated_at_utc),
        "evaluationContext": row.evaluation_context_json,
        "createdBy": row.created_by,
        "createdAtUtc": _value(row.created_at_utc),
    }


def evidence_payload(row: MsrpSourceEvidenceAsset) -> dict[str, object]:
    return {
        "evidenceAssetId": str(row.evidence_asset_id),
        "targetId": _value(row.target_id),
        "sourceId": _value(row.source_id),
        "repairCaseId": _value(row.repair_case_id),
        "evidenceType": row.evidence_type,
        "sourceUrl": row.source_url,
        "finalUrl": row.final_url,
        "redirectChain": row.redirect_chain_json or [],
        "officialDomainVerified": row.official_domain_verified,
        "filename": row.filename,
        "mimeType": row.mime_type,
        "mimeSignature": row.mime_signature,
        "sizeBytes": row.size_bytes,
        "storageKey": row.storage_key,
        "sha256": row.sha256,
        "capturedAtUtc": _value(row.captured_at_utc),
        "documentDate": _value(row.document_date),
        "validFrom": _value(row.valid_from),
        "validUntil": _value(row.valid_until),
        "pageCount": row.page_count,
        "contentHash": row.content_hash,
        "textHash": row.text_hash,
        "sourceType": row.source_type,
        "semanticLane": row.semantic_lane,
        "lifecycleState": row.lifecycle_state,
        "createdBy": row.created_by,
        "createdAtUtc": _value(row.created_at_utc),
    }


def evidence_upload_payload(row: MsrpEvidenceUploadSession) -> dict[str, object]:
    total_parts = (
        row.expected_size_bytes + row.chunk_size_bytes - 1
    ) // row.chunk_size_bytes
    return {
        "uploadSessionId": str(row.upload_session_id),
        "targetId": str(row.target_id),
        "sourceId": _value(row.source_id),
        "repairCaseId": _value(row.repair_case_id),
        "completedEvidenceAssetId": _value(row.completed_evidence_asset_id),
        "sourceUrl": row.source_url,
        "officialDomain": row.official_domain,
        "originalFilename": row.original_filename,
        "expectedMimeType": row.expected_mime_type,
        "expectedSizeBytes": row.expected_size_bytes,
        "expectedSha256": row.expected_sha256,
        "chunkSizeBytes": row.chunk_size_bytes,
        "totalParts": total_parts,
        "receivedParts": row.received_parts_json or [],
        "sourceType": row.source_type,
        "semanticLane": row.semantic_lane,
        "documentDate": _value(row.document_date),
        "validFrom": _value(row.valid_from),
        "validUntil": _value(row.valid_until),
        "uploadStatus": row.upload_status,
        "expiresAtUtc": _value(row.expires_at_utc),
        "rowVersion": row.row_version,
        "createdAtUtc": _value(row.created_at_utc),
        "updatedAtUtc": _value(row.updated_at_utc),
    }


def source_version_payload(row: MsrpSourceVersion) -> dict[str, object]:
    return {
        "sourceVersionId": str(row.source_version_id),
        "sourceId": str(row.source_id),
        "targetId": str(row.target_id),
        "versionNumber": row.version_number,
        "profile": row.profile_json,
        "profileYaml": row.profile_yaml,
        "profileSha256": row.profile_sha256,
        "evidenceRefs": row.evidence_refs_json or [],
        "extractorName": row.extractor_name,
        "extractorType": row.extractor_type,
        "extractorVersion": row.extractor_version,
        "semanticLane": row.semantic_lane,
        "currency": row.currency,
        "taxMode": row.tax_mode,
        "validFrom": _value(row.valid_from),
        "validUntil": _value(row.valid_until),
        "previousVersionId": _value(row.previous_version_id),
        "validationSummary": row.validation_summary_json,
        "dryrunSummary": row.dryrun_summary_json,
        "replaySummary": row.replay_summary_json,
        "conflictSummary": row.conflict_summary_json,
        "gateResult": row.gate_result_json,
        "versionStatus": row.version_status,
        "createdBy": row.created_by,
        "approvedBy": row.approved_by,
        "approvedAtUtc": _value(row.approved_at_utc),
        "publishedAtUtc": _value(row.published_at_utc),
        "decisionReason": row.decision_reason,
        "createdAtUtc": _value(row.created_at_utc),
        "updatedAtUtc": _value(row.updated_at_utc),
    }


def repair_case_payload(row: MsrpGovernanceRepairCase) -> dict[str, object]:
    return {
        "caseId": str(row.case_id),
        "repairDomain": row.repair_domain,
        "targetId": _value(row.target_id),
        "sourceId": _value(row.source_id),
        "observationId": _value(row.observation_id),
        "mappingReference": row.mapping_reference,
        "fxRunId": _value(row.fx_run_id),
        "caseType": row.case_type,
        "failureClassifier": row.failure_classifier,
        "severity": row.severity,
        "priority": row.priority,
        "firstSeenAtUtc": _value(row.first_seen_at_utc),
        "lastSeenAtUtc": _value(row.last_seen_at_utc),
        "occurrenceCount": row.occurrence_count,
        "recentRunIds": row.recent_run_ids_json or [],
        "evidenceRefs": row.evidence_refs_json or [],
        "manualEvidenceRequired": row.manual_evidence_required,
        "agentRunRefs": row.agent_run_refs_json or [],
        "proposalRefs": row.proposal_refs_json or [],
        "caseStatus": row.case_status,
        "resolution": row.resolution_json,
        "recurrenceOfCaseId": _value(row.recurrence_of_case_id),
        "owner": row.owner,
        "createdBy": row.created_by,
        "rowVersion": row.row_version,
        "createdAtUtc": _value(row.created_at_utc),
        "updatedAtUtc": _value(row.updated_at_utc),
    }


def proposal_payload(row: MsrpRepairProposal) -> dict[str, object]:
    return {
        "proposalId": str(row.proposal_id),
        "caseId": str(row.case_id),
        "targetId": _value(row.target_id),
        "sourceId": _value(row.source_id),
        "sourceVersionId": _value(row.source_version_id),
        "proposalOrigin": row.proposal_origin,
        "proposalType": row.proposal_type,
        "agentRunId": row.agent_run_id,
        "agentStepId": row.agent_step_id,
        "dpv4Metadata": row.dpv4_metadata_json,
        "inputEvidenceRefs": row.input_evidence_refs_json or [],
        "proposedChange": row.proposed_change_json,
        "fieldDiff": row.field_diff_json or [],
        "assumptions": row.assumptions_json or [],
        "unresolvedQuestions": row.unresolved_questions_json or [],
        "riskFlags": row.risk_flags_json or [],
        "validationResult": row.validation_result_json,
        "dryrunResult": row.dryrun_result_json,
        "replayResult": row.replay_result_json,
        "conflictResult": row.conflict_result_json,
        "gateResult": row.gate_result_json,
        "proposalStatus": row.proposal_status,
        "author": row.author,
        "reviewer": row.reviewer,
        "reviewedAtUtc": _value(row.reviewed_at_utc),
        "decisionReason": row.decision_reason,
        "createdAtUtc": _value(row.created_at_utc),
        "updatedAtUtc": _value(row.updated_at_utc),
    }


def result_correction_payload(
    row: MsrpResultCorrectionDecision,
) -> dict[str, object]:
    return {
        "correctionDecisionId": str(row.correction_decision_id),
        "originalObservationId": str(row.original_observation_id),
        "gateDecisionId": str(row.gate_decision_id),
        "originalCurrentPriceId": _value(row.original_current_price_id),
        "originalPriceHistoryId": _value(row.original_price_history_id),
        "correctionType": row.correction_type,
        "reason": row.reason,
        "evidenceRefs": row.evidence_refs_json or [],
        "sourceVersionId": _value(row.source_version_id),
        "correctedInputs": row.corrected_inputs_json,
        "replayResult": row.replay_result_json,
        "gateResult": row.gate_result_json,
        "replacementObservationId": _value(row.replacement_observation_id),
        "rematerializationRefs": row.rematerialization_refs_json or [],
        "decisionStatus": row.decision_status,
        "createdBy": row.created_by,
        "approvedBy": row.approved_by,
        "approvedAtUtc": _value(row.approved_at_utc),
        "createdAtUtc": _value(row.created_at_utc),
        "updatedAtUtc": _value(row.updated_at_utc),
    }


def fx_run_payload(row: MsrpFxNormalizationRun) -> dict[str, object]:
    return {
        "fxRunId": str(row.fx_run_id),
        "observationId": str(row.observation_id),
        "gateDecisionId": str(row.gate_decision_id),
        "localCurrency": row.local_currency,
        "localValue": _value(row.local_value),
        "fxProvider": row.fx_provider,
        "rateToNormalized": _value(row.rate_to_normalized),
        "rateEffectiveDate": _value(row.rate_effective_date),
        "rateRetrievedAtUtc": _value(row.rate_retrieved_at_utc),
        "policyVersion": row.policy_version,
        "normalizedCurrency": row.normalized_currency,
        "normalizedValue": _value(row.normalized_value),
        "gateResult": row.gate_result_json,
        "runStatus": row.run_status,
        "failureReason": row.failure_reason,
        "decisionReason": row.decision_reason,
        "supersededRunId": _value(row.superseded_run_id),
        "createdBy": row.created_by,
        "approvedBy": row.approved_by,
        "approvedAtUtc": _value(row.approved_at_utc),
        "createdAtUtc": _value(row.created_at_utc),
    }


def audit_event_payload(row: MsrpGovernanceAuditEvent) -> dict[str, object]:
    return {
        "auditEventId": str(row.audit_event_id),
        "entityType": row.entity_type,
        "entityId": row.entity_id,
        "action": row.action,
        "actor": row.actor,
        "actorRole": row.actor_role,
        "idempotencyKey": row.idempotency_key,
        "correlationId": row.correlation_id,
        "before": row.before_json,
        "after": row.after_json,
        "metadata": row.metadata_json,
        "occurredAtUtc": _value(row.occurred_at_utc),
    }
