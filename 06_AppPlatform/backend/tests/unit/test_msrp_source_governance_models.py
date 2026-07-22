from sqlalchemy import CheckConstraint

import app.db.models  # noqa: F401
import app.db.msrp_source_governance_models  # noqa: F401
from app.db.base import Base


def _constraint_names(table_name: str) -> set[str]:
    table = Base.metadata.tables[f"msrp.{table_name}"]
    return {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint) and constraint.name
    }


def test_governance_foundation_registers_feature_local_tables() -> None:
    assert {
        "msrp.monitoring_targets",
        "msrp.governance_gate_decisions",
        "msrp.source_versions",
        "msrp.source_evidence_assets",
        "msrp.observation_evidence_links",
        "msrp.evidence_upload_sessions",
        "msrp.repair_cases",
        "msrp.repair_proposals",
        "msrp.result_correction_decisions",
        "msrp.fx_normalization_runs",
        "msrp.governance_audit_events",
    }.issubset(Base.metadata.tables)


def test_proposal_schema_enforces_hermes_dpv4_boundary() -> None:
    assert any(
        name.endswith("ck_msrp_repair_proposals_dpv4_boundary")
        for name in _constraint_names("repair_proposals")
    )


def test_evidence_and_source_version_tables_are_immutable_by_api_shape() -> None:
    evidence = Base.metadata.tables["msrp.source_evidence_assets"]
    versions = Base.metadata.tables["msrp.source_versions"]

    assert "sha256" in evidence.c
    assert evidence.c.sha256.nullable is False
    assert "profile_sha256" in versions.c
    assert versions.c.profile_sha256.nullable is False
    assert "updated_at_utc" not in evidence.c


def test_observation_evidence_links_are_append_only_and_restrict_deletion() -> None:
    links = Base.metadata.tables["msrp.observation_evidence_links"]
    observations = Base.metadata.tables["msrp.observations"]
    current_prices = Base.metadata.tables["msrp.current_prices"]
    price_history = Base.metadata.tables["msrp.price_history"]

    observation_fk = next(iter(links.c.observation_id.foreign_keys))
    assert observation_fk.ondelete is None
    assert links.c.source_version_id.nullable is False
    assert links.c.evidence_asset_id.nullable is False
    assert links.c.evidence_sha256.nullable is False
    assert "updated_at_utc" not in links.c
    assert observations.c.source_version_id.nullable is True
    assert current_prices.c.evidence_refs_json.nullable is False
    assert price_history.c.evidence_refs_json.nullable is False


def test_gate_decisions_are_append_only_materialization_snapshots() -> None:
    decisions = Base.metadata.tables["msrp.governance_gate_decisions"]

    assert decisions.c.source_gate_json.nullable is False
    assert decisions.c.mapping_gate_json.nullable is False
    assert decisions.c.evaluated_at_utc.nullable is False
    assert "updated_at_utc" not in decisions.c


def test_result_and_fx_repairs_reference_persisted_gate_decisions() -> None:
    corrections = Base.metadata.tables["msrp.result_correction_decisions"]
    fx_runs = Base.metadata.tables["msrp.fx_normalization_runs"]

    assert corrections.c.gate_decision_id.nullable is False
    assert corrections.c.gate_result_json.nullable is False
    assert fx_runs.c.gate_decision_id.nullable is False
    assert fx_runs.c.gate_result_json.nullable is False
