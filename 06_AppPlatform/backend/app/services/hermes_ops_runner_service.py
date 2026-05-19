"""Hermes Ops Runner service.

Centralizes execution controls for the Hermes script runner. The API routes are
thin wrappers; this service owns the allowlist, feature flag, lock, redaction,
activity log, and evidence entry so the runner behaves like a control-plane
operation instead of a generic subprocess endpoint.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4


_project_root: Path | None = None


HERMES_SCRIPTS: dict[str, dict[str, Any]] = {
    "pipeline-audit": {
        "script": "hermes_pipeline_audit.py",
        "label": "Pipeline Audit",
        "desc": "Scan systemd/Airflow/GH Actions -> health report",
    },
    "source-quality": {
        "script": "hermes_source_quality.py",
        "label": "Source Quality",
        "desc": "Score VOC/News/MSRP source health 0-100",
    },
    "cost-report": {
        "script": "hermes_cost_report.py",
        "label": "Cost Report",
        "desc": "Flash/Pro cost vs 500 CNY budget",
    },
    "code-audit": {
        "script": "hermes_code_audit.py",
        "label": "Code Audit",
        "desc": "git diff -> 10-rule scan",
        "args": ["--base", "main", "--head", "HEAD"],
    },
    "intake": {
        "script": "hermes_intake.py",
        "label": "PRD Intake",
        "desc": "PRD -> impact report (needs --prd arg)",
        "args": [],
    },
    "evidence": {
        "script": "hermes_evidence_writer.py",
        "label": "Evidence Writer",
        "desc": "Extract facts from artifacts -> JSONL",
    },
    "answer-audit": {
        "script": "hermes_answer_audit.py",
        "label": "Answer Audit",
        "desc": "Generate sample answer audits",
    },
}

HELP_TEXT = """
Hermes CLI — available commands:

  pipeline-audit  Scan systemd/Airflow/GH Actions -> health report
  source-quality  Score VOC/News/MSRP source health 0-100
  cost-report     Flash/Pro cost vs 500 CNY budget
  code-audit      git diff -> 10-rule audit scan
  intake          PRD impact analysis (needs --prd)
  evidence        Extract structured evidence -> JSONL
  answer-audit    Generate sample answer audits

Usage: POST /v1/hermes/run/{command}
       GET  /v1/hermes/run/{command}/help
"""

RUN_TIMEOUT_SECONDS = 120
LOCK_STALE_SECONDS = 10 * 60

SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(
            r"(?i)\b(api[_-]?key|token|secret|password|passwd|authorization)"
            r"(\s*[:=]\s*)([\"']?)([^\s\"']+)"
        ),
        r"\1\2\3[REDACTED]",
    ),
    (re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}"), "Bearer [REDACTED]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{8,}"), "sk-[REDACTED]"),
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{12,}"), "gh_[REDACTED]"),
]


class HermesRunError(RuntimeError):
    status_code = 500


class HermesRunnerDisabled(HermesRunError):
    status_code = 403


class HermesRunBusy(HermesRunError):
    status_code = 409


class HermesRunUnknownCommand(HermesRunError):
    status_code = 400


class HermesRunScriptMissing(HermesRunError):
    status_code = 500


def _root() -> Path:
    global _project_root
    if _project_root is None:
        _project_root = Path(__file__).resolve().parents[4]
    return _project_root


def _hermes_dir() -> Path:
    return _root() / "hermes"


def _scripts_dir() -> Path:
    return _root() / "03_Scripts" / "hermes"


def _activity_log_path() -> Path:
    return _hermes_dir() / "activity_log.jsonl"


def _evidence_ledger_path() -> Path:
    return _hermes_dir() / "evidence_ledger.jsonl"


def _lock_path() -> Path:
    return _hermes_dir() / "run_locks" / "ops_runner.lock"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _compact_iso() -> str:
    return _now().strftime("%Y%m%d_%H%M%S")


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def is_runner_enabled() -> bool:
    return _parse_bool(os.getenv("HERMES_RUN_ENABLED"), True)


def redact_secrets(text: str) -> str:
    redacted = text or ""
    for pattern, replacement in SECRET_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def _safe_tail(text: str, limit: int) -> str:
    return redact_secrets(text)[-limit:]


def _write_jsonl(path: Path, record: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"[hermes] Failed to write {path.name}: {exc}", file=sys.stderr)


def _log_activity(record: dict[str, Any]) -> None:
    _write_jsonl(_activity_log_path(), record)


def _log_evidence(record: dict[str, Any]) -> None:
    _write_jsonl(_evidence_ledger_path(), record)


def _read_lock(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _lock_is_stale(path: Path) -> bool:
    try:
        age_seconds = _now().timestamp() - path.stat().st_mtime
    except OSError:
        return True
    return age_seconds > LOCK_STALE_SECONDS


@contextmanager
def _runner_lock(run_id: str, command_id: str, actor: str) -> Iterator[None]:
    path = _lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_record = {
        "runId": run_id,
        "commandId": command_id,
        "actor": actor,
        "startedAt": _now_iso(),
        "pid": os.getpid(),
    }
    while True:
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(lock_record, fh, ensure_ascii=False, sort_keys=True)
            break
        except FileExistsError as exc:
            if _lock_is_stale(path):
                try:
                    path.unlink()
                    continue
                except OSError:
                    pass
            current = _read_lock(path)
            detail = current.get("runId") or current.get("commandId") or "another run"
            raise HermesRunBusy(f"Hermes runner is busy with {detail}.") from exc

    try:
        yield
    finally:
        try:
            current = _read_lock(path)
            if current.get("runId") == run_id:
                path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            print(f"[hermes] Failed to release runner lock: {exc}", file=sys.stderr)


def _set_arg(args: list[str], flag: str, value: Any) -> None:
    clean_value = str(value or "").strip()
    if not clean_value:
        return
    if flag in args:
        idx = args.index(flag)
        if idx + 1 < len(args):
            args[idx + 1] = clean_value
        else:
            args.append(clean_value)
        return
    args.extend([flag, clean_value])


def _resolve_args(command_id: str, parameters: dict[str, Any] | None) -> list[str]:
    info = HERMES_SCRIPTS[command_id]
    args = list(info.get("args", []))
    params = parameters or {}
    if command_id == "code-audit":
        _set_arg(args, "--base", params.get("base"))
        _set_arg(args, "--head", params.get("head"))
    if command_id == "intake":
        _set_arg(args, "--prd", params.get("prd") or params.get("prdPath"))
    return args


def _build_run_id(command_id: str) -> str:
    clean_command = re.sub(r"[^a-z0-9]+", "_", command_id.lower()).strip("_")
    return f"run_{_compact_iso()}_{clean_command}_{uuid4().hex[:6]}"


def _activity_record(
    *,
    run_id: str,
    command_id: str,
    script: str,
    actor: str,
    exit_code: int,
    status: str,
    started_at: str,
    finished_at: str,
) -> dict[str, Any]:
    return {
        "timestamp": finished_at,
        "runId": run_id,
        "command": command_id,
        "commandId": command_id,
        "script": script,
        "actor": actor,
        "exitCode": exit_code,
        "status": status,
        "startedAt": started_at,
        "finishedAt": finished_at,
    }


def _evidence_record(activity: dict[str, Any]) -> dict[str, Any]:
    run_id = str(activity.get("runId") or "")
    command_id = str(activity.get("commandId") or activity.get("command") or "")
    status = str(activity.get("status") or "")
    exit_code = activity.get("exitCode")
    return {
        "evidenceId": f"evidence.hermes_run.{run_id}",
        "evidenceType": "hermes_run",
        "claim": f"Hermes Ops Runner executed {command_id} with status {status} and exit code {exit_code}.",
        "sourceRef": f"hermes/activity_log.jsonl::{run_id}",
        "artifactId": "artifact.hermes.activity_log",
        "confidence": 1.0,
        "supportCount": 1,
        "contradictionCount": 0,
        "createdAt": str(activity.get("finishedAt") or _now_iso()),
        "runId": run_id,
        "commandId": command_id,
        "actor": activity.get("actor", "unknown"),
        "status": status,
        "exitCode": exit_code,
    }


def _complete(
    *,
    run_id: str,
    command_id: str,
    script: str,
    actor: str,
    exit_code: int,
    status: str,
    started_at: str,
    stdout: str,
    stderr: str,
) -> dict[str, Any]:
    finished_at = _now_iso()
    activity = _activity_record(
        run_id=run_id,
        command_id=command_id,
        script=script,
        actor=actor,
        exit_code=exit_code,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
    )
    _log_activity(activity)
    _log_evidence(_evidence_record(activity))
    return {
        "command": command_id,
        "commandId": command_id,
        "runId": run_id,
        "script": script,
        "actor": actor,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "exitCode": exit_code,
        "stdout": _safe_tail(stdout, 8000),
        "stderr": _safe_tail(stderr, 2000),
        "status": status,
        "evidenceRef": f"evidence.hermes_run.{run_id}",
    }


def get_command_help(command_id: str) -> dict[str, Any]:
    info = HERMES_SCRIPTS.get(command_id)
    if not info:
        raise HermesRunUnknownCommand(f"Unknown command: {command_id}. Try: {', '.join(HERMES_SCRIPTS)}")
    return {
        "command": command_id,
        "script": info["script"],
        "label": info["label"],
        "desc": info["desc"],
        "defaultArgs": info.get("args", []),
        "runnerEnabled": is_runner_enabled(),
    }


def list_run_commands() -> dict[str, Any]:
    return {
        "runnerEnabled": is_runner_enabled(),
        "commands": {
            command_id: {
                "label": info["label"],
                "desc": info["desc"],
                "hasDefaultArgs": bool(info.get("args")),
            }
            for command_id, info in HERMES_SCRIPTS.items()
        },
    }


def execute_hermes_command(
    command_id: str,
    *,
    parameters: dict[str, Any] | None = None,
    actor: str = "unknown",
    session_id: str = "",
) -> dict[str, Any]:
    if not is_runner_enabled():
        raise HermesRunnerDisabled("Hermes Ops Runner is disabled by HERMES_RUN_ENABLED=false.")
    if command_id not in HERMES_SCRIPTS:
        raise HermesRunUnknownCommand(f"Unknown command: {command_id}. Available: {', '.join(HERMES_SCRIPTS)}")

    info = HERMES_SCRIPTS[command_id]
    script_path = _scripts_dir() / str(info["script"])
    if not script_path.is_file():
        raise HermesRunScriptMissing(f"Script not found: {script_path}")

    args = _resolve_args(command_id, parameters)
    cmd = [sys.executable, str(script_path)] + args
    started_at = _now_iso()
    run_id = _build_run_id(command_id)
    clean_actor = (actor or "unknown").strip() or "unknown"

    with _runner_lock(run_id, command_id, clean_actor):
        try:
            result = subprocess.run(
                cmd,
                cwd=str(_root()),
                capture_output=True,
                text=True,
                timeout=RUN_TIMEOUT_SECONDS,
            )
            status = "success" if result.returncode == 0 else "failed"
            response = _complete(
                run_id=run_id,
                command_id=command_id,
                script=str(info["script"]),
                actor=clean_actor,
                exit_code=result.returncode,
                status=status,
                started_at=started_at,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        except subprocess.TimeoutExpired as exc:
            response = _complete(
                run_id=run_id,
                command_id=command_id,
                script=str(info["script"]),
                actor=clean_actor,
                exit_code=-1,
                status="timeout",
                started_at=started_at,
                stdout=exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or ""),
                stderr=f"Timeout after {RUN_TIMEOUT_SECONDS} seconds",
            )
        except Exception as exc:
            response = _complete(
                run_id=run_id,
                command_id=command_id,
                script=str(info["script"]),
                actor=clean_actor,
                exit_code=-1,
                status="error",
                started_at=started_at,
                stdout="",
                stderr=str(exc),
            )
        if session_id:
            response["sessionId"] = session_id
        return response
