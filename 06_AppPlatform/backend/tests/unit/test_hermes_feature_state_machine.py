from app.services.hermes_feature_state_machine import compute_feature_state


def test_feature_state_does_not_reach_ready_for_pr_without_tests() -> None:
    result = compute_feature_state(
        {
            "prd_md_exists": True,
            "reuse_candidates_identified": True,
            "backend_implemented": True,
            "frontend_implemented": True,
            "unit_tests_added": False,
        },
        open_gap_count=0,
    )

    assert result["state"] == "implemented"
    assert "unit_tests_added" in result["missingEvidence"]


def test_feature_state_reaches_ready_for_pr_with_tests_and_no_gaps() -> None:
    result = compute_feature_state(
        {
            "prd_md_exists": True,
            "reuse_candidates_identified": True,
            "backend_implemented": True,
            "frontend_implemented": True,
            "unit_tests_added": True,
        },
        open_gap_count=0,
    )

    assert result["state"] == "ready_for_pr"
    assert "pr_opened" in result["missingEvidence"]


def test_feature_state_does_not_reach_verified_without_smoke_evidence() -> None:
    result = compute_feature_state(
        {
            "prd_md_exists": True,
            "reuse_candidates_identified": True,
            "backend_implemented": True,
            "frontend_implemented": True,
            "unit_tests_added": True,
            "pr_opened": True,
            "pr_merged": True,
            "deployed": True,
            "verified": True,
            "smoke_evidence_attached": False,
        },
        open_gap_count=0,
    )

    assert result["state"] == "deployed"
    assert "smoke_evidence_attached" in result["missingEvidence"]


def test_feature_state_reaches_verified_with_smoke_evidence() -> None:
    result = compute_feature_state(
        {
            "prd_md_exists": True,
            "reuse_candidates_identified": True,
            "backend_implemented": True,
            "frontend_implemented": True,
            "unit_tests_added": True,
            "pr_opened": True,
            "pr_merged": True,
            "deployed": True,
            "verified": True,
            "smoke_evidence_attached": True,
        },
        open_gap_count=0,
    )

    assert result["state"] == "verified"


def test_feature_state_blocks_on_high_risk_open_gap() -> None:
    result = compute_feature_state(
        {
            "prd_md_exists": True,
            "backend_implemented": True,
            "unit_tests_added": True,
        },
        risk="high",
        open_gap_count=1,
    )

    assert result["state"] == "blocked"
    assert result["blocked"] is True
