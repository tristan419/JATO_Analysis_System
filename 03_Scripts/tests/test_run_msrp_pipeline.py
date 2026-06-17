from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "03_Scripts" / "run_msrp_pipeline.sh"


def _fake_runner(path: Path) -> None:
    path.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            echo "${JATO_MSRP_MODE}:${JATO_MSRP_COUNTRIES}" >> "$RUNNER_LOG"
            if [[ "$JATO_MSRP_MODE" == "dryrun" ]]; then
              mkdir -p "$REPO_DIR/03_Scripts/diagnostics/artifacts"
              cat > "$REPO_DIR/03_Scripts/diagnostics/artifacts/dryrun_report.json" <<JSON
            {
              "schemaVersion": "msrp_dryrun_report_v3",
              "runId": "msrp-dryrun-test",
              "summary": {
                "total": 2,
                "pass": ${JATO_TEST_PASS_COUNT:-2},
                "passPct": ${JATO_TEST_PASS_PCT:-91.0},
                "gateStatus": "${JATO_TEST_GATE_STATUS:-allowed}",
                "gateThreshold": 70
              },
              "missingCountries": []
            }
            JSON
            fi
            """
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _run_pipeline(
    tmp_path: Path,
    *,
    gate_status: str,
    pass_pct: str,
    pass_count: str,
) -> subprocess.CompletedProcess[str]:
    runner = tmp_path / "fake_msrp_runner.sh"
    runner_log = tmp_path / "runner.log"
    _fake_runner(runner)
    env = os.environ.copy()
    env.update(
        {
            "REPO_DIR": str(tmp_path),
            "RUNNER_LOG": str(runner_log),
            "JATO_MSRP_RUNNER": str(runner),
            "JATO_MSRP_PYTHON": sys.executable,
            "JATO_MSRP_PIPELINE_COUNTRIES": "se,fi",
            "JATO_TEST_GATE_STATUS": gate_status,
            "JATO_TEST_PASS_PCT": pass_pct,
            "JATO_TEST_PASS_COUNT": pass_count,
        }
    )
    return subprocess.run(
        ["bash", str(SCRIPT_PATH)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def test_script_syntax_is_valid() -> None:
    result = subprocess.run(
        ["bash", "-n", str(SCRIPT_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_pipeline_runs_ingest_when_dryrun_gate_is_allowed(tmp_path: Path) -> None:
    result = _run_pipeline(
        tmp_path,
        gate_status="allowed",
        pass_pct="91.0",
        pass_count="2",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert (tmp_path / "runner.log").read_text(encoding="utf-8").splitlines() == [
        "dryrun:se,fi",
        "ingest:se,fi",
    ]
    status = json.loads(
        (
            tmp_path
            / "hermes"
            / "reports"
            / "pipeline_status"
            / "msrp_pipeline.json"
        ).read_text(encoding="utf-8")
    )
    assert status["status"] == "success"
    assert status["metadata"]["dryrunGateStatus"] == "allowed"
    scheduled = json.loads(
        (
            tmp_path
            / "03_Scripts"
            / "logs"
            / "scheduled_fetch_status.json"
        ).read_text(encoding="utf-8")
    )
    assert scheduled["msrp_pipeline"]["status"] == "success"


def test_pipeline_skips_ingest_when_dryrun_gate_blocks(tmp_path: Path) -> None:
    result = _run_pipeline(
        tmp_path,
        gate_status="blocked",
        pass_pct="42.0",
        pass_count="1",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert (tmp_path / "runner.log").read_text(encoding="utf-8").splitlines() == [
        "dryrun:se,fi",
    ]
    status = json.loads(
        (
            tmp_path
            / "hermes"
            / "reports"
            / "pipeline_status"
            / "msrp_pipeline.json"
        ).read_text(encoding="utf-8")
    )
    assert status["status"] == "skipped"
    assert status["metadata"]["phase"] == "gate"
    assert status["metadata"]["dryrunPassPct"] == 42.0
