"""Hermes feature lifecycle state machine.

The state machine is intentionally small and pure. Callers provide evidence
booleans; this module decides the computed lifecycle state and missing proof.
"""

from __future__ import annotations

from typing import Any


FEATURE_LIFECYCLE = [
    "draft",
    "prd_ready",
    "ready_for_dev",
    "in_progress",
    "implemented",
    "tested",
    "ready_for_pr",
    "in_review",
    "merged",
    "deployed",
    "verified",
    "done",
]

SIDE_STATES = {"blocked", "archived", "reopened"}

EVIDENCE_CHECKLIST = [
    ("prd_md_exists", "PRD / feature MD exists"),
    ("reuse_candidates_identified", "Reuse candidates identified"),
    ("backend_contract_defined", "Backend contract defined"),
    ("backend_implemented", "Backend implemented"),
    ("frontend_contract_defined", "Frontend contract defined"),
    ("frontend_implemented", "Frontend implemented"),
    ("unit_tests_added", "Unit tests added or updated"),
    ("type_build_checks_passed", "Type/build checks passed"),
    ("smoke_evidence_attached", "Smoke evidence attached"),
    ("docs_updated", "Docs updated"),
    ("pr_opened", "PR opened"),
    ("pr_merged", "PR merged"),
    ("deployed", "Deployed"),
    ("verified", "Verified"),
]

_BLOCKING_RISKS = {"blocking", "critical", "high"}
_ARCHIVED_STATUSES = {"archived", "deprecated"}
_DONE_STATUSES = {"done", "resolved"}


def _checked(evidence: dict[str, Any], key: str) -> bool:
    return bool(evidence.get(key))


def build_evidence_checklist(
    evidence_sources: dict[str, list[str]],
    declared_checks: dict[str, bool] | None = None,
) -> list[dict[str, Any]]:
    """Return canonical checklist items with evidence labels.

    Markdown checkboxes are exposed as ``declaredChecked`` but are not treated as
    evidence by themselves. This keeps Hermes read-only and avoids silently
    accepting an unchecked manual claim as proof.
    """

    declared_checks = declared_checks or {}
    items: list[dict[str, Any]] = []
    for key, label in EVIDENCE_CHECKLIST:
        sources = [source for source in evidence_sources.get(key, []) if str(source).strip()]
        items.append({
            "key": key,
            "label": label,
            "checked": bool(sources),
            "declaredChecked": bool(declared_checks.get(key)),
            "evidenceSources": sources,
        })
    return items


def compute_feature_state(
    evidence: dict[str, Any],
    *,
    declared_status: str = "",
    risk: str = "low",
    open_gap_count: int = 0,
) -> dict[str, Any]:
    """Compute the feature lifecycle state from concrete evidence."""

    normalized_status = str(declared_status or "").strip().lower()
    normalized_risk = str(risk or "low").strip().lower()

    if normalized_status in _ARCHIVED_STATUSES:
        return {
            "state": "archived",
            "nextAction": "Keep archived unless the feature is explicitly reopened.",
            "missingEvidence": [],
            "blocked": False,
        }

    if normalized_status == "blocked" or (
        normalized_risk in _BLOCKING_RISKS and open_gap_count > 0
    ):
        return {
            "state": "blocked",
            "nextAction": "Resolve blocking gaps before advancing this feature.",
            "missingEvidence": ["blocking_gaps_resolved"],
            "blocked": True,
        }

    has_backend_or_frontend = _checked(evidence, "backend_implemented") or _checked(
        evidence,
        "frontend_implemented",
    )
    has_contract = _checked(evidence, "backend_contract_defined") or _checked(
        evidence,
        "frontend_contract_defined",
    )
    has_tests = _checked(evidence, "unit_tests_added")
    has_smoke = _checked(evidence, "smoke_evidence_attached")
    is_verified = _checked(evidence, "verified") and has_smoke

    state = "draft"
    missing: list[str] = ["prd_md_exists"]
    next_action = "Create or link a feature MD with goal, scope, and acceptance criteria."

    if _checked(evidence, "prd_md_exists"):
        state = "prd_ready"
        missing = ["reuse_candidates_identified"]
        next_action = "Run Hermes intake or reuse radar before development starts."

    if _checked(evidence, "reuse_candidates_identified") or has_contract:
        state = "ready_for_dev"
        missing = ["backend_implemented", "frontend_implemented"]
        next_action = "Implement the smallest backend/frontend slice linked to this feature."

    if _checked(evidence, "active_development"):
        state = "in_progress"
        missing = ["backend_implemented", "frontend_implemented"]
        next_action = "Finish the active implementation slice and keep changed files linked to the feature."

    if has_backend_or_frontend:
        state = "implemented"
        missing = ["unit_tests_added"]
        next_action = "Add or run targeted backend/frontend tests before PR review."

    if has_tests:
        state = "tested"
        missing = ["pr_opened"]
        next_action = "Open a PR after confirming no blocking gaps remain."

    if has_tests and has_backend_or_frontend and open_gap_count == 0:
        state = "ready_for_pr"
        missing = ["pr_opened"]
        next_action = "Open the PR and link it back to the feature document."

    if _checked(evidence, "pr_opened"):
        state = "in_review"
        missing = ["pr_merged"]
        next_action = "Address PR review and keep tests/evidence current."

    if _checked(evidence, "pr_merged"):
        state = "merged"
        missing = ["deployed"]
        next_action = "Deploy the merged commit through the tracked pipeline."

    if _checked(evidence, "deployed"):
        state = "deployed"
        missing = ["smoke_evidence_attached", "verified"]
        next_action = "Attach smoke evidence and record manual verification."

    if is_verified:
        state = "done" if normalized_status in _DONE_STATUSES and open_gap_count == 0 else "verified"
        missing = [] if state == "done" else ["done_confirmation"]
        next_action = "Keep monitoring Sentinel and pipeline status." if state == "done" else "Close remaining non-blocking follow-up items or mark done."

    if normalized_status == "reopened":
        state = "reopened"
        missing = ["updated_scope_or_regression_fix"]
        next_action = "Record the regression or scope change, then resume implementation."

    return {
        "state": state,
        "nextAction": next_action,
        "missingEvidence": missing,
        "blocked": False,
    }
