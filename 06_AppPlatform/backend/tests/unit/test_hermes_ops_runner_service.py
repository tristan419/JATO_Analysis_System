import json
import os
from pathlib import Path

import pytest

from app.services import hermes_ops_runner_service as runner


def _write_script(root: Path, name: str, body: str) -> Path:
    script = root / "03_Scripts" / "hermes" / name
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text(body, encoding="utf-8")
    return script


def test_execute_writes_activity_and_evidence(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "_project_root", tmp_path)
    monkeypatch.setenv("HERMES_RUN_ENABLED", "true")
    _write_script(
        tmp_path,
        "hermes_cost_report.py",
        "print('cost ok')\n",
    )

    result = runner.execute_hermes_command("cost-report", actor="tester")

    assert result["status"] == "success"
    assert result["exitCode"] == 0
    assert result["actor"] == "tester"
    assert "cost ok" in result["stdout"]
    activity_lines = (tmp_path / "hermes" / "activity_log.jsonl").read_text(encoding="utf-8").splitlines()
    evidence_lines = (tmp_path / "hermes" / "evidence_ledger.jsonl").read_text(encoding="utf-8").splitlines()
    activity = json.loads(activity_lines[-1])
    evidence = json.loads(evidence_lines[-1])
    assert activity["runId"] == result["runId"]
    assert activity["commandId"] == "cost-report"
    assert evidence["runId"] == result["runId"]
    assert evidence["evidenceType"] == "hermes_run"


def test_execute_redacts_secrets_from_output(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "_project_root", tmp_path)
    monkeypatch.setenv("HERMES_RUN_ENABLED", "true")
    _write_script(
        tmp_path,
        "hermes_cost_report.py",
        "print('api_key=sk-secretvalue123456')\nprint('Authorization: Bearer abcdefghijklmnop')\n",
    )

    result = runner.execute_hermes_command("cost-report", actor="tester")

    assert "sk-secretvalue" not in result["stdout"]
    assert "abcdefghijklmnop" not in result["stdout"]
    assert "[REDACTED]" in result["stdout"]


def test_runner_disabled_blocks_execution(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "_project_root", tmp_path)
    monkeypatch.setenv("HERMES_RUN_ENABLED", "false")
    _write_script(tmp_path, "hermes_cost_report.py", "print('should not run')\n")

    with pytest.raises(runner.HermesRunnerDisabled):
        runner.execute_hermes_command("cost-report", actor="tester")


def test_runner_lock_blocks_concurrent_run(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "_project_root", tmp_path)
    monkeypatch.setenv("HERMES_RUN_ENABLED", "true")
    _write_script(tmp_path, "hermes_cost_report.py", "print('should not run')\n")
    lock = tmp_path / "hermes" / "run_locks" / "ops_runner.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text(json.dumps({"runId": "run_existing", "commandId": "cost-report"}), encoding="utf-8")
    os.utime(lock, None)

    with pytest.raises(runner.HermesRunBusy):
        runner.execute_hermes_command("cost-report", actor="tester")
