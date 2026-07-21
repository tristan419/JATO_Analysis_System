from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "jato_monthly_worker.py"
SPEC = importlib.util.spec_from_file_location("jato_monthly_worker", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
WORKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKER)


def test_required_resource_limit_clamps_to_existing_hard_limit() -> None:
    calls: list[tuple[int, tuple[int, int]]] = []
    resource_module = SimpleNamespace(
        RLIM_INFINITY=-1,
        getrlimit=lambda _kind: (8_000, 2_000),
        setrlimit=lambda kind, limits: calls.append((kind, limits)),
    )

    WORKER._set_required_resource_limit(
        resource_module,
        9,
        4_000,
        label="test",
    )

    assert calls == [(9, (2_000, 2_000))]


def test_required_resource_limit_fails_closed() -> None:
    resource_module = SimpleNamespace(
        RLIM_INFINITY=-1,
        getrlimit=lambda _kind: (8_000, -1),
        setrlimit=lambda _kind, _limits: (_ for _ in ()).throw(
            ValueError("unsupported")
        ),
    )

    with pytest.raises(RuntimeError, match="Unable to enforce"):
        WORKER._set_required_resource_limit(
            resource_module,
            9,
            4_000,
            label="test",
        )


def test_worker_help_keeps_argparse_success_exit_code() -> None:
    help_result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    invalid_result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--supervise-digest-upload", "x"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert help_result.returncode == 0
    assert "--supervise-digest-upload" in help_result.stdout
    assert help_result.stderr == ""
    assert invalid_result.returncode == 2
    assert "requires --attempt-id" in invalid_result.stderr


def test_digest_supervisor_stops_after_consecutive_actual_rss_samples(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeProcess:
        pid = 24680

        def __init__(self) -> None:
            self.returncode: int | None = None

        def poll(self) -> int | None:
            return self.returncode

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

        def wait(self) -> int:
            assert self.returncode is not None
            return self.returncode

    process = FakeProcess()

    def fake_popen(_args: list[str], **kwargs: object) -> FakeProcess:
        captured.update(kwargs)
        return process

    monkeypatch.setattr(WORKER.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(WORKER, "_read_process_rss_bytes", lambda _pid: 200)
    monkeypatch.setattr(WORKER, "_cgroup_memory_snapshot", lambda: None)
    monkeypatch.setattr(WORKER.time, "sleep", lambda _seconds: None)
    monkeypatch.setenv("APP_JATO_DIGEST_RSS_WARNING_BYTES", "100")
    monkeypatch.setenv("APP_JATO_DIGEST_RSS_LIMIT_BYTES", "150")
    monkeypatch.setenv(
        "APP_JATO_DIGEST_RSS_LIMIT_CONSECUTIVE_SAMPLES",
        "2",
    )
    log_path = tmp_path / "attempt.log"
    receipt_path = tmp_path / "attempt.exit.json"

    result = WORKER._supervise_digest_upload(
        upload_id="upload-rss-limit",
        attempt_id="1-test",
        log_path=log_path,
        receipt_path=receipt_path,
    )

    assert result == 0
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "finished"
    assert receipt["returnCode"] == -15
    assert receipt["terminationReason"] == "rss_limit"
    assert receipt["peakRssBytes"] == 200
    assert receipt["rssWarningExceeded"] is True
    child_env = captured["env"]
    assert isinstance(child_env, dict)
    assert child_env["APP_JATO_MONTHLY_WORKER_MEMORY_LIMIT_BYTES"] == "0"
    assert child_env["OMP_NUM_THREADS"] == "1"
    assert child_env["OPENBLAS_NUM_THREADS"] == "1"
    assert child_env["MKL_NUM_THREADS"] == "1"
    assert child_env["NUMEXPR_NUM_THREADS"] == "1"
    assert child_env["MALLOC_ARENA_MAX"] == "2"
    assert "RSS limit reached" in log_path.read_text(encoding="utf-8")
