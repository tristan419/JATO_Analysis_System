from __future__ import annotations

from datetime import datetime, timezone

from app.api.msrp_source_governance_schemas import (
    FxGateInput,
    GateDecisionV1,
    GateResult,
    MappingGateInput,
    SourceGateInput,
)


ACCEPTED_MAPPING_STATUSES = frozenset(
    {
        "accepted",
        "approved",
        "auto_accepted",
        "manual_approved",
        "linked",
    }
)


def _source_gate(value: SourceGateInput) -> GateResult:
    reasons: list[str] = []
    checks = (
        (value.source_version_status == "published", "source_version_not_published"),
        (value.verified_official_evidence, "official_evidence_not_verified"),
        (value.immutable_evidence, "evidence_not_immutable"),
        (value.deterministic_extraction, "extraction_not_deterministic"),
        (value.semantic_lane_valid, "semantic_lane_invalid"),
        (value.currency_valid, "currency_invalid"),
        (value.tax_mode_valid, "tax_mode_invalid"),
        (value.validity_window_valid, "validity_window_invalid"),
        (not value.blocking_conflict, "blocking_source_or_semantic_conflict"),
        (
            not value.unresolved_result_correction,
            "unresolved_result_correction",
        ),
        (value.schema_valid, "source_schema_invalid"),
        (value.targeted_dryrun_passed, "targeted_dryrun_not_passed"),
    )
    reasons.extend(code for passed, code in checks if not passed)
    return GateResult(
        status="pass" if not reasons else "fail",
        reasons=reasons,
        policy_version=value.policy_version,
    )


def _mapping_gate(value: MappingGateInput) -> GateResult:
    reasons: list[str] = []
    if value.decision_status not in ACCEPTED_MAPPING_STATUSES:
        reasons.append("mapping_decision_not_accepted")
    if not value.hard_constraints_passed:
        reasons.append("mapping_hard_constraints_failed")
    if not value.policy_version_accepted:
        reasons.append("mapping_policy_version_not_accepted")
    if not value.human_approved:
        if not value.score_threshold_passed:
            reasons.append("mapping_score_below_threshold")
        if not value.margin_threshold_passed:
            reasons.append("mapping_margin_below_threshold")
    return GateResult(
        status="pass" if not reasons else "fail",
        reasons=reasons,
        policy_version=value.policy_version,
    )


def _fx_gate(value: FxGateInput) -> GateResult:
    reasons: list[str] = []
    checks = (
        (value.provider_approved, "fx_provider_not_approved"),
        (value.rate_positive, "fx_rate_not_positive"),
        (value.effective_date_valid, "fx_effective_date_invalid"),
        (value.freshness_valid, "fx_rate_stale"),
        (value.local_value_unchanged, "fx_repair_changed_local_msrp"),
    )
    reasons.extend(code for passed, code in checks if not passed)
    return GateResult(
        status="pass" if not reasons else "fail",
        reasons=reasons,
        policy_version=value.policy_version,
    )


def evaluate_materialization_eligibility(
    *,
    target_id,
    observation_id,
    source_gate: SourceGateInput,
    mapping_gate: MappingGateInput,
    fx_gate: FxGateInput | None = None,
    evaluated_at: datetime | None = None,
) -> GateDecisionV1:
    """Evaluate every fact-write path against one immutable Gate policy surface.

    FX only governs derived normalized values. A failed or missing FX Gate cannot
    invalidate an otherwise eligible local-currency MSRP fact.
    """

    source_result = _source_gate(source_gate)
    mapping_result = _mapping_gate(mapping_gate)
    fx_result = _fx_gate(fx_gate) if fx_gate is not None else None
    local_eligible = (
        source_result.status == "pass" and mapping_result.status == "pass"
    )
    normalized_eligible = bool(
        local_eligible and fx_result is not None and fx_result.status == "pass"
    )
    return GateDecisionV1(
        target_id=target_id,
        observation_id=observation_id,
        source_gate=source_result,
        mapping_gate=mapping_result,
        fx_gate=fx_result,
        eligible_for_local_materialization=local_eligible,
        eligible_for_normalized_materialization=normalized_eligible,
        evaluated_at=evaluated_at or datetime.now(timezone.utc),
    )
