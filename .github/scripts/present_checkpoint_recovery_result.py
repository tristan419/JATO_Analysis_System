#!/usr/bin/env python3
"""Validate, preserve, and present one checkpoint-recovery result."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "03_Scripts" / "deploy"))

import reviewed_recovery_authorization as reviewed  # noqa: E402


MAX_RESULT_BYTES = 1024 * 1024
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
CHECK_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_]{0,127}$")
FIELD_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
FAILURE_FIELDS = {
    "schemaVersion",
    "kind",
    "decision",
    "mode",
    "stage",
    "category",
    "detail",
    "fieldDiffs",
    "passed",
    "notReached",
    "implementationCommit",
    "planSha256",
    "trafficChanged",
    "databaseChanged",
    "jatoDataChanged",
    "checkpointChanged",
    "mutationPerformed",
}
CHANGE_FIELDS = (
    "trafficChanged",
    "databaseChanged",
    "jatoDataChanged",
    "checkpointChanged",
    "mutationPerformed",
)
APPLY_RESULT_FIELDS = reviewed.RESULT_FIELDS | {
    "checkpointSequence",
    "receiptPath",
    "receiptSha256",
    "receiptReused",
    "finalizationReceiptPath",
    "finalizationReceiptSha256",
    "finalizationReceiptReused",
}
ALREADY_ABORTED_FIELDS = {
    "decision",
    "incidentId",
    "implementationCommit",
    "planSha256",
    "checkpointPhase",
    "receiptCreatedAt",
    "activeCommit",
    "candidatePresent",
    "candidateStarted",
    "trafficChanged",
    "databaseChanged",
    "checkpointChanged",
    "mutationPerformed",
    "mode",
}


class PresentationError(ValueError):
    """The runner result cannot be safely presented."""


def _reject_duplicates(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PresentationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _safe_text(value: object, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise PresentationError("diagnostic text is invalid")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise PresentationError("diagnostic text contains control characters")
    return value


def _safe_value(value: object) -> None:
    if value is None or isinstance(value, (bool, int)):
        return
    if isinstance(value, str):
        _safe_text(value)
        return
    if isinstance(value, list) and len(value) <= 32:
        for item in value:
            _safe_value(item)
        return
    raise PresentationError("diagnostic field value is unsafe")


def _read_result(path: Path) -> tuple[Mapping[str, Any], bytes]:
    metadata = path.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_size <= 0
        or metadata.st_size > MAX_RESULT_BYTES
    ):
        raise PresentationError("result must be one bounded regular file")
    raw = path.read_bytes()
    if len(raw) != metadata.st_size:
        raise PresentationError("result changed while reading")
    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicates,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise PresentationError("result is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise PresentationError("result must be one JSON object")
    return payload, raw


def _validate_checks(value: object) -> list[str]:
    if not isinstance(value, list) or len(value) > 64:
        raise PresentationError("diagnostic check list is invalid")
    checks: list[str] = []
    for item in value:
        if not isinstance(item, str) or not CHECK_PATTERN.fullmatch(item):
            raise PresentationError("diagnostic check name is invalid")
        checks.append(item)
    if len(checks) != len(set(checks)):
        raise PresentationError("diagnostic check list contains duplicates")
    return checks


def _validate_failure(
    payload: Mapping[str, Any],
    *,
    mode: str,
    main_sha: str,
    plan_sha256: str,
) -> None:
    if set(payload) != FAILURE_FIELDS:
        raise PresentationError("failure result fields are not exact")
    expected_decision = "dry-run-rejected" if mode == "dry-run" else "apply-failed"
    if (
        payload.get("schemaVersion") != 1
        or payload.get("kind") != "checkpoint_recovery_failure"
        or payload.get("decision") != expected_decision
        or payload.get("mode") != mode
        or payload.get("implementationCommit") != main_sha
        or payload.get("planSha256") != plan_sha256
    ):
        raise PresentationError("failure result identity is invalid")
    _safe_text(payload.get("stage"), maximum=128)
    _safe_text(payload.get("category"), maximum=128)
    _safe_text(payload.get("detail"))
    _validate_checks(payload.get("passed"))
    _validate_checks(payload.get("notReached"))
    differences = payload.get("fieldDiffs")
    if not isinstance(differences, list) or len(differences) > 64:
        raise PresentationError("field differences are invalid")
    seen: set[str] = set()
    for difference in differences:
        if not isinstance(difference, dict) or set(difference) != {
            "field",
            "expected",
            "actual",
        }:
            raise PresentationError("field difference shape is invalid")
        field = difference.get("field")
        if not isinstance(field, str) or not FIELD_PATTERN.fullmatch(field):
            raise PresentationError("field difference name is invalid")
        if field in seen:
            raise PresentationError("field difference is duplicated")
        seen.add(field)
        _safe_value(difference.get("expected"))
        _safe_value(difference.get("actual"))
    for field in CHANGE_FIELDS:
        value = payload.get(field)
        if value is not None and not isinstance(value, bool):
            raise PresentationError("failure change status is invalid")


def _load_plan(plan_path: Path, expected_sha256: str) -> Mapping[str, Any]:
    try:
        plan, raw = reviewed._read_document(plan_path, "recovery plan")
    except reviewed.AuthorizationError as exc:
        raise PresentationError("recovery plan is invalid") from exc
    if (
        hashlib.sha256(raw).hexdigest() != expected_sha256
        or plan.get("schemaVersion") != 3
        or plan.get("incidentId") != reviewed.INCIDENT_ID
    ):
        raise PresentationError("recovery plan identity is invalid")
    return plan


def _validate_already_aborted(
    payload: Mapping[str, Any],
    *,
    mode: str,
    main_sha: str,
    plan_sha256: str,
    plan: Mapping[str, Any],
) -> None:
    if (
        set(payload) != ALREADY_ABORTED_FIELDS
        or payload.get("decision") != "already-pre-switch-aborted"
        or payload.get("incidentId") != reviewed.INCIDENT_ID
        or payload.get("implementationCommit") != main_sha
        or payload.get("planSha256") != plan_sha256
        or payload.get("checkpointPhase") != "pre_switch_aborted"
        or payload.get("activeCommit") != plan["expected"]["activeCommit"]
        or payload.get("mode") != mode
        or any(
            payload.get(field) is not False
            for field in (
                "candidatePresent",
                "candidateStarted",
                "trafficChanged",
                "databaseChanged",
                "checkpointChanged",
            )
        )
        or not isinstance(payload.get("mutationPerformed"), bool)
        or (
            mode == "dry-run"
            and payload.get("mutationPerformed") is not False
        )
    ):
        raise PresentationError("terminal success result contract is invalid")


def _validate_apply_success(
    payload: Mapping[str, Any],
    *,
    main_sha: str,
    plan_sha256: str,
    plan: Mapping[str, Any],
) -> None:
    receipt_paths = (
        payload.get("receiptPath"),
        payload.get("finalizationReceiptPath"),
    )
    receipt_digests = (
        payload.get("receiptSha256"),
        payload.get("finalizationReceiptSha256"),
    )
    if (
        set(payload) != APPLY_RESULT_FIELDS
        or payload.get("decision")
        != "pre-switch-residue-quarantined-and-aborted"
        or payload.get("incidentId") != reviewed.INCIDENT_ID
        or payload.get("implementationCommit") != main_sha
        or payload.get("planSha256") != plan_sha256
        or payload.get("targetIdentity") != plan["checkpoint"]["identity"]
        or payload.get("checkpointPhase") != "pre_switch_aborted"
        or payload.get("checkpointSequence")
        != plan["checkpoint"]["sequence"] + 1
        or payload.get("databaseRevisions") != plan["expected"]["revisions"]
        or payload.get("activeCommit") != plan["expected"]["activeCommit"]
        or payload.get("otherReleaseGate") != "cross-release-safe"
        or payload.get("mode") != "apply"
        or payload.get("candidateResiduePresent") is not False
        or payload.get("inventoryDigest") != reviewed._inventory_digest(plan)
        or any(
            payload.get(field) is not False
            for field in (
                "trafficChanged",
                "databaseChanged",
                "candidateStarted",
                "candidatePresent",
            )
        )
        or payload.get("checkpointChanged") is not True
        or payload.get("mutationPerformed") is not True
        or any(
            not isinstance(path, str) or not path.startswith("/")
            for path in receipt_paths
        )
        or any(
            not isinstance(digest, str)
            or not SHA256_PATTERN.fullmatch(digest)
            for digest in receipt_digests
        )
        or any(
            not isinstance(payload.get(field), bool)
            for field in ("receiptReused", "finalizationReceiptReused")
        )
    ):
        raise PresentationError("apply success result contract is invalid")


def _validate_success(
    payload: Mapping[str, Any],
    *,
    result_path: Path,
    result_raw: bytes,
    plan_path: Path,
    mode: str,
    main_sha: str,
    plan_sha256: str,
) -> None:
    plan = _load_plan(plan_path, plan_sha256)
    if payload.get("decision") == "already-pre-switch-aborted":
        _validate_already_aborted(
            payload,
            mode=mode,
            main_sha=main_sha,
            plan_sha256=plan_sha256,
            plan=plan,
        )
        return
    if mode == "apply":
        _validate_apply_success(
            payload,
            main_sha=main_sha,
            plan_sha256=plan_sha256,
            plan=plan,
        )
        return
    try:
        reviewed.validate_reviewed_dry_run(
            result_path=result_path,
            plan_path=plan_path,
            expected_result_sha256=hashlib.sha256(result_raw).hexdigest(),
            expected_main_sha=main_sha,
            expected_plan_sha256=plan_sha256,
        )
    except reviewed.AuthorizationError as exc:
        raise PresentationError("dry-run success contract is invalid") from exc


def _generic_failure(
    *,
    mode: str,
    main_sha: str,
    plan_sha256: str,
    category: str,
    detail: str,
) -> Mapping[str, Any]:
    return {
        "schemaVersion": 1,
        "kind": "checkpoint_recovery_failure",
        "decision": "dry-run-rejected" if mode == "dry-run" else "apply-failed",
        "mode": mode,
        "stage": "remote_execution",
        "category": category,
        "detail": detail,
        "fieldDiffs": [],
        "passed": [],
        "notReached": [],
        "implementationCommit": main_sha,
        "planSha256": plan_sha256,
        "trafficChanged": None,
        "databaseChanged": None,
        "jatoDataChanged": None,
        "checkpointChanged": None,
        "mutationPerformed": None,
    }


def _atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise PresentationError("result path must not be a symlink")
    raw = (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False).encode(
            "utf-8"
        )
        + b"\n"
    )
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
    )
    temporary_path = Path(temporary)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _summary_text(value: object) -> str:
    return html.escape(str(value), quote=True).replace("|", "&#124;")


def _summary_code(value: object) -> str:
    return f"<code>{_summary_text(value)}</code>"


def _summary_value(value: object) -> str:
    if value is None:
        return "Unknown"
    if value is False:
        return "No change"
    if value is True:
        return "Changed"
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    return _summary_code(text[:512])


def _write_summary(
    path: Path,
    payload: Mapping[str, Any],
    *,
    step_outcome: str,
) -> None:
    lines = [
        "## Checkpoint recovery result",
        "",
        "| Field | Value |",
        "| --- | --- |",
        f"| Step outcome | {_summary_code(step_outcome)} |",
        f"| Mode | {_summary_code(payload.get('mode'))} |",
        f"| Decision | {_summary_code(payload.get('decision'))} |",
        "| Main SHA | "
        f"{_summary_code(payload.get('implementationCommit'))} |",
        "| Plan SHA-256 | "
        f"{_summary_code(payload.get('planSha256'))} |",
    ]
    if payload.get("kind") == "checkpoint_recovery_failure":
        lines.extend(
            [
                f"| Stage | {_summary_code(payload.get('stage'))} |",
                f"| Category | {_summary_code(payload.get('category'))} |",
                "",
                "### Failure detail",
                "",
                f"<p>{_summary_text(payload.get('detail'))}</p>",
            ]
        )
        differences = payload.get("fieldDiffs") or []
        if differences:
            lines.extend(
                [
                    "",
                    "### Field differences",
                    "",
                    "| Field | Expected | Actual |",
                    "| --- | --- | --- |",
                ]
            )
            for item in differences:
                lines.append(
                    "| {} | {} | {} |".format(
                        _summary_code(item["field"]),
                        _summary_value(item["expected"]),
                        _summary_value(item["actual"]),
                    )
                )
        for key, heading in (
            ("passed", "Checks passed"),
            ("notReached", "Checks not reached"),
        ):
            checks = payload.get(key) or []
            if checks:
                lines.extend(["", f"### {heading}", ""])
                lines.extend(f"- {_summary_code(item)}" for item in checks)
    lines.extend(
        [
            "",
            "### Change status",
            "",
            "| Area | Status |",
            "| --- | --- |",
        ]
    )
    labels = {
        "trafficChanged": "Public traffic",
        "databaseChanged": "Database",
        "jatoDataChanged": "JATO data",
        "checkpointChanged": "Checkpoint",
        "mutationPerformed": "Recovery mutation",
    }
    for field in CHANGE_FIELDS:
        lines.append(f"| {labels[field]} | {_summary_value(payload.get(field))} |")
    if payload.get("kind") == "checkpoint_recovery_failure":
        lines.extend(
            [
                "",
                "> A failed result cannot be used as reviewed dry-run or apply authorization.",
            ]
        )
    with path.open("a", encoding="utf-8") as output:
        output.write("\n".join(lines) + "\n")


def present_result(
    *,
    result_path: Path,
    summary_path: Path,
    plan_path: Path,
    step_outcome: str,
    mode: str,
    main_sha: str,
    plan_sha256: str,
) -> int:
    if step_outcome not in {"success", "failure", "cancelled", "skipped"}:
        raise PresentationError("step outcome is invalid")
    if mode not in {"dry-run", "apply"}:
        raise PresentationError("recovery mode is invalid")
    if not GIT_SHA_PATTERN.fullmatch(main_sha):
        raise PresentationError("main SHA is invalid")
    if not SHA256_PATTERN.fullmatch(plan_sha256):
        raise PresentationError("plan SHA-256 is invalid")

    payload: Mapping[str, Any]
    valid = False
    try:
        payload, raw = _read_result(result_path)
        if step_outcome == "success":
            _validate_success(
                payload,
                result_path=result_path,
                result_raw=raw,
                plan_path=plan_path,
                mode=mode,
                main_sha=main_sha,
                plan_sha256=plan_sha256,
            )
        else:
            _validate_failure(
                payload,
                mode=mode,
                main_sha=main_sha,
                plan_sha256=plan_sha256,
            )
        valid = True
    except (
        KeyError,
        OSError,
        PresentationError,
        TypeError,
        reviewed.AuthorizationError,
    ):
        category = (
            "result_validation_failed"
            if step_outcome == "success"
            else "unstructured_recovery_failure"
        )
        payload = _generic_failure(
            mode=mode,
            main_sha=main_sha,
            plan_sha256=plan_sha256,
            category=category,
            detail=(
                "Recovery reported success without a valid structured result."
                if step_outcome == "success"
                else "Recovery failed before a valid structured result was produced."
            ),
        )
        _atomic_write(result_path, payload)

    _write_summary(summary_path, payload, step_outcome=step_outcome)
    return 0 if valid or step_outcome != "success" else 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--summary", required=True, type=Path)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--step-outcome", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--main-sha", required=True)
    parser.add_argument("--plan-sha256", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _build_parser().parse_args(argv)
    try:
        return present_result(
            result_path=arguments.result,
            summary_path=arguments.summary,
            plan_path=arguments.plan,
            step_outcome=arguments.step_outcome,
            mode=arguments.mode,
            main_sha=arguments.main_sha,
            plan_sha256=arguments.plan_sha256,
        )
    except (OSError, PresentationError) as exc:
        print(f"checkpoint recovery presentation error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
