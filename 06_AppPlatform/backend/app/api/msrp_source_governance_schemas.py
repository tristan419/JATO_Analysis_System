from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


def _to_camel(value: str) -> str:
    head, *tail = value.split("_")
    return head + "".join(part.capitalize() for part in tail)


class GovernanceContract(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
        extra="forbid",
        str_strip_whitespace=True,
    )


SchemaVersion = Literal["1.0"]
GateStatus = Literal["pass", "fail", "not_applicable"]
RepairDomain = Literal[
    "source",
    "parser",
    "semantic",
    "result",
    "mapping",
    "fx",
    "runtime",
]
Severity = Literal["low", "medium", "high", "critical"]


class EvidenceReference(GovernanceContract):
    evidence_asset_id: UUID
    sha256: str = Field(min_length=64, max_length=64)
    evidence_type: str


class GateResult(GovernanceContract):
    status: GateStatus
    reasons: list[str] = Field(default_factory=list)
    policy_version: str


class GateDecisionV1(GovernanceContract):
    schema_version: SchemaVersion = "1.0"
    target_id: UUID
    observation_id: UUID
    source_gate: GateResult
    mapping_gate: GateResult
    fx_gate: GateResult | None = None
    eligible_for_local_materialization: bool
    eligible_for_normalized_materialization: bool
    evaluated_at: datetime


class SourceGateInput(GovernanceContract):
    policy_version: str
    source_version_status: str
    verified_official_evidence: bool
    immutable_evidence: bool
    deterministic_extraction: bool
    semantic_lane_valid: bool
    currency_valid: bool
    tax_mode_valid: bool
    validity_window_valid: bool
    blocking_conflict: bool = False
    unresolved_result_correction: bool = False
    schema_valid: bool
    targeted_dryrun_passed: bool


class MappingGateInput(GovernanceContract):
    policy_version: str
    decision_status: str
    hard_constraints_passed: bool
    score_threshold_passed: bool
    margin_threshold_passed: bool
    policy_version_accepted: bool
    human_approved: bool = False


class FxGateInput(GovernanceContract):
    policy_version: str
    provider_approved: bool
    rate_positive: bool
    effective_date_valid: bool
    freshness_valid: bool
    local_value_unchanged: bool


class GateEvaluationRequest(GovernanceContract):
    target_id: UUID
    observation_id: UUID
    source_gate: SourceGateInput
    mapping_gate: MappingGateInput
    fx_gate: FxGateInput | None = None
    evaluation_context: dict[str, object] | None = None


class SourceRunResultV1(GovernanceContract):
    schema_version: SchemaVersion = "1.0"
    run_id: str
    target_key: str
    source_code: str
    runtime_source_id: UUID
    published_source_version_id: UUID | None = None
    status: str
    failure_class: str | None = None
    retryability: str
    extractor_name: str
    extractor_version: str
    source_url: str
    final_url: str | None = None
    evidence_refs: list[EvidenceReference] = Field(default_factory=list)
    content_hash: str | None = None
    extracted_count: int = Field(ge=0)
    valid_count: int = Field(ge=0)
    rejected_count: int = Field(ge=0)
    last_known_good_run_id: str | None = None
    last_known_good_at: datetime | None = None
    started_at: datetime
    completed_at: datetime


class MonitorAnomalyV1(GovernanceContract):
    schema_version: SchemaVersion = "1.0"
    anomaly_id: str
    country: str
    brand: str
    model: str
    trim: str
    powertrain: str | None = None
    current_price_id: UUID | None = None
    price_history_ids: list[UUID] = Field(default_factory=list)
    observation_ids: list[UUID] = Field(default_factory=list)
    evidence_refs: list[EvidenceReference] = Field(default_factory=list)
    movement_type: str
    local_currency: str
    current_local: Decimal
    previous_local: Decimal | None = None
    normalized_currency: str | None = None
    current_normalized: Decimal | None = None
    previous_normalized: Decimal | None = None
    suspected_repair_domain: RepairDomain
    reason: str
    detected_at: datetime


class AgentRunRequestV1(GovernanceContract):
    schema_version: SchemaVersion = "1.0"
    run_id: str
    case_id: UUID
    target_id: UUID
    repair_domain: RepairDomain
    evidence_refs: list[EvidenceReference] = Field(default_factory=list)
    current_source_version_id: UUID | None = None
    last_known_good_version_id: UUID | None = None
    source_gate_snapshot: GateResult
    mapping_gate_snapshot: GateResult
    fx_gate_snapshot: GateResult | None = None
    allowed_tool_ids: list[str] = Field(default_factory=list)
    authority_policy_version: str
    composer_policy_version: str
    attempt_budget: int = Field(default=3, ge=1, le=20)
    time_budget_seconds: int = Field(default=900, ge=30, le=7200)
    token_budget: int = Field(default=100_000, ge=0)
    cost_budget_usd: Decimal = Field(default=Decimal("5"), ge=0)
    requested_by: str


class AgentRunResultV1(GovernanceContract):
    schema_version: SchemaVersion = "1.0"
    run_id: str
    status: str
    plan_version: str
    step_refs: list[str] = Field(default_factory=list)
    tool_execution_refs: list[str] = Field(default_factory=list)
    llm_invocation_refs: list[str] = Field(default_factory=list)
    proposal_refs: list[str] = Field(default_factory=list)
    evaluation_refs: list[str] = Field(default_factory=list)
    capability_proposal_refs: list[str] = Field(default_factory=list)
    stop_reason: str | None = None
    human_escalation: dict[str, object] | None = None
    completed_at: datetime


class MappingDecisionV1(GovernanceContract):
    schema_version: SchemaVersion = "1.0"
    decision_id: str
    accepted_reference: str | None = None
    decision_status: str
    candidate_scores: list[dict[str, object]] = Field(default_factory=list)
    top1_top2_margin: float | None = None
    hard_conflicts: list[str] = Field(default_factory=list)
    policy_version: str
    decision_actor: str
    decided_at: datetime


class MonitoringTargetCreate(GovernanceContract):
    country: Annotated[str, Field(min_length=2, max_length=3)]
    brand: Annotated[str, Field(min_length=1, max_length=120)]
    model: Annotated[str, Field(min_length=1, max_length=240)]
    trim_scope: str | None = None
    powertrain_scope: str | None = None
    roster_type: Literal["country_top30", "manual", "future_roster"] = "manual"
    roster_rank: int | None = Field(default=None, ge=1)
    schedule: dict[str, object] | None = None
    owner: str | None = None
    notes: str | None = None


class UrlEvidenceCreate(GovernanceContract):
    source_id: UUID | None = None
    repair_case_id: UUID | None = None
    source_url: HttpUrl
    final_url: HttpUrl | None = None
    redirect_chain: list[HttpUrl] = Field(default_factory=list)
    official_domain: str = Field(min_length=3, max_length=253)
    source_type: str
    semantic_lane: str
    captured_at_utc: datetime | None = None
    document_date: date | None = None
    valid_from: date | None = None
    valid_until: date | None = None

    @model_validator(mode="after")
    def validate_dates(self) -> "UrlEvidenceCreate":
        if self.valid_from and self.valid_until and self.valid_until < self.valid_from:
            raise ValueError("valid_until must not precede valid_from")
        return self


class EvidenceUploadInitiate(GovernanceContract):
    target_id: UUID
    source_id: UUID | None = None
    repair_case_id: UUID | None = None
    source_url: HttpUrl
    official_domain: str = Field(min_length=3, max_length=253)
    original_filename: str = Field(min_length=5, max_length=255)
    expected_mime_type: Literal["application/pdf"] = "application/pdf"
    expected_size_bytes: int = Field(gt=0, le=200 * 1024 * 1024)
    expected_sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    chunk_size_bytes: int = Field(
        default=5 * 1024 * 1024,
        ge=256 * 1024,
        le=10 * 1024 * 1024,
    )
    source_type: str
    semantic_lane: str
    document_date: date | None = None
    valid_from: date | None = None
    valid_until: date | None = None

    @model_validator(mode="after")
    def validate_upload(self) -> "EvidenceUploadInitiate":
        if not self.original_filename.casefold().endswith(".pdf"):
            raise ValueError("Only .pdf evidence uploads are accepted")
        if self.valid_from and self.valid_until and self.valid_until < self.valid_from:
            raise ValueError("valid_until must not precede valid_from")
        return self


class EvidenceUploadComplete(GovernanceContract):
    row_version: int = Field(ge=1)


class RepairCaseFindingCreate(GovernanceContract):
    repair_domain: RepairDomain
    target_id: UUID | None = None
    source_id: UUID | None = None
    observation_id: UUID | None = None
    mapping_reference: str | None = None
    fx_run_id: UUID | None = None
    case_type: str
    failure_classifier: str
    severity: Severity = "medium"
    priority: int = Field(default=50, ge=0, le=100)
    run_id: str | None = None
    evidence_refs: list[EvidenceReference] = Field(default_factory=list)
    manual_evidence_required: bool = False
    occurred_at_utc: datetime | None = None
    owner: str | None = None


class RepairProposalCreate(GovernanceContract):
    proposal_origin: Literal["manual", "deterministic", "hermes_agent"]
    proposal_type: str
    source_version_id: UUID | None = None
    agent_run_id: str | None = None
    agent_step_id: str | None = None
    dpv4_metadata: dict[str, object] | None = None
    input_evidence_refs: list[EvidenceReference] = Field(default_factory=list)
    proposed_change: dict[str, object]
    field_diff: list[dict[str, object]] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_provider_boundary(self) -> "RepairProposalCreate":
        if self.dpv4_metadata and self.proposal_origin != "hermes_agent":
            raise ValueError(
                "DPV4 metadata is allowed only on a Hermes-authored proposal"
            )
        if self.proposal_origin == "hermes_agent" and not self.agent_run_id:
            raise ValueError("Hermes proposals require agent_run_id")
        return self


class SourceVersionCreate(GovernanceContract):
    proposal_id: UUID
    source_id: UUID
    target_id: UUID
    profile: dict[str, object]
    profile_yaml: str
    evidence_refs: list[EvidenceReference]
    extractor_name: str
    extractor_type: str
    extractor_version: str
    semantic_lane: str
    currency: Annotated[str, Field(min_length=3, max_length=3)]
    tax_mode: str
    valid_from: date | None = None
    valid_until: date | None = None
    previous_version_id: UUID | None = None

    @model_validator(mode="after")
    def validate_dates(self) -> "SourceVersionCreate":
        if self.valid_from and self.valid_until and self.valid_until < self.valid_from:
            raise ValueError("valid_until must not precede valid_from")
        return self


class ProposalVerificationUpdate(GovernanceContract):
    validation_result: dict[str, object]
    dryrun_result: dict[str, object]
    replay_result: dict[str, object] | None = None
    conflict_result: dict[str, object] | None = None
    gate_result: GateDecisionV1 | None = None


class SubmitProposalRequest(GovernanceContract):
    expected_status: Literal["draft", "validated", "dryrun_passed"]


class PublishSourceVersionRequest(GovernanceContract):
    target_row_version: int = Field(ge=1)
    decision_reason: str = Field(min_length=3)


class ResolveRepairCaseRequest(GovernanceContract):
    row_version: int = Field(ge=1)
    resolution: dict[str, object]


class ResultCorrectionCreate(GovernanceContract):
    original_observation_id: UUID
    gate_decision_id: UUID
    original_current_price_id: UUID | None = None
    original_price_history_id: UUID | None = None
    correction_type: str
    reason: str = Field(min_length=3)
    evidence_refs: list[EvidenceReference]
    source_version_id: UUID | None = None
    corrected_inputs: dict[str, object]
    replay_result: dict[str, object]


class FxNormalizationCreate(GovernanceContract):
    observation_id: UUID
    gate_decision_id: UUID
    local_currency: Annotated[str, Field(min_length=3, max_length=3)]
    local_value: Decimal = Field(gt=0)
    fx_provider: str
    rate_to_normalized: Decimal = Field(gt=0)
    rate_effective_date: date
    rate_retrieved_at_utc: datetime
    policy_version: str
    normalized_currency: Annotated[str, Field(min_length=3, max_length=3)] = "EUR"


class ApprovalRequest(GovernanceContract):
    expected_status: str
    decision_reason: str = Field(min_length=3)


class HermesDiagnosisRequest(GovernanceContract):
    source_gate_snapshot: GateResult
    mapping_gate_snapshot: GateResult
    fx_gate_snapshot: GateResult | None = None
    allowed_tool_ids: list[str] = Field(default_factory=list)
    authority_policy_version: str
    composer_policy_version: str
    attempt_budget: int = Field(default=3, ge=1, le=20)
    time_budget_seconds: int = Field(default=900, ge=30, le=7200)
    token_budget: int = Field(default=100_000, ge=0)
    cost_budget_usd: Decimal = Field(default=Decimal("5"), ge=0)
