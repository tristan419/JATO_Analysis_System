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


def _fake_config_source_sync(path: Path) -> None:
    path.write_text(
        textwrap.dedent(
            """\
            import json
            import os
            from pathlib import Path
            import sys

            repo = Path(os.environ["REPO_DIR"])
            with Path(os.environ["RUNNER_LOG"]).open("a", encoding="utf-8") as handle:
                handle.write("config-sync:" + " ".join(sys.argv[1:]) + "\\n")

            exit_code = int(os.environ.get("JATO_TEST_CONFIG_SYNC_EXIT", "0"))
            if exit_code:
                raise SystemExit(exit_code)

            reports_dir = repo / "hermes" / "reports"
            status_dir = reports_dir / "pipeline_status"
            reports_dir.mkdir(parents=True, exist_ok=True)
            status_dir.mkdir(parents=True, exist_ok=True)
            (reports_dir / "engineering_config_source_sync.json").write_text(
                json.dumps(
                    {
                        "schemaVersion": "engineering_config_source_sync_v1",
                        "status": "passed",
                        "summary": {"sourceCount": 4, "countryCount": 4},
                    }
                )
                + "\\n",
                encoding="utf-8",
            )
            (reports_dir / "engineering_config_source_sync.md").write_text(
                "# Engineering Config Source Sync\\n",
                encoding="utf-8",
            )
            (status_dir / "engineering_config_source_sync.json").write_text(
                json.dumps(
                    {
                        "pipelineId": "engineering_config_source_sync",
                        "status": "success",
                        "sourceCount": 4,
                        "countryCount": 4,
                    }
                )
                + "\\n",
                encoding="utf-8",
            )
            """
        ),
        encoding="utf-8",
    )


def _run_pipeline(
    tmp_path: Path,
    *,
    gate_status: str,
    pass_pct: str,
    pass_count: str,
    config_sync_exit: str = "0",
) -> subprocess.CompletedProcess[str]:
    runner = tmp_path / "fake_msrp_runner.sh"
    config_source_sync = tmp_path / "fake_config_source_sync.py"
    runner_log = tmp_path / "runner.log"
    _fake_runner(runner)
    _fake_config_source_sync(config_source_sync)
    env = os.environ.copy()
    env.update(
        {
            "REPO_DIR": str(tmp_path),
            "RUNNER_LOG": str(runner_log),
            "JATO_MSRP_RUNNER": str(runner),
            "JATO_CONFIG_SOURCE_SYNC": str(config_source_sync),
            "JATO_MSRP_PYTHON": sys.executable,
            "JATO_MSRP_PIPELINE_COUNTRIES": "se,fi",
            "JATO_TEST_CONFIG_SYNC_EXIT": config_sync_exit,
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
    lines = (tmp_path / "runner.log").read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("config-sync:")
    assert "--write-status" in lines[0]
    assert lines[1:] == [
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
    assert status["metadata"]["configSourceSyncStatus"] == "success"
    assert (
        status["metadata"]["configSourceSyncPipelineId"]
        == "engineering_config_source_sync"
    )
    assert "hermes/reports/engineering_config_source_sync.json" in status["artifactRefs"]
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
    lines = (tmp_path / "runner.log").read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("config-sync:")
    assert lines[1:] == [
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


def test_pipeline_fails_before_dryrun_when_config_source_sync_fails(
    tmp_path: Path,
) -> None:
    result = _run_pipeline(
        tmp_path,
        gate_status="allowed",
        pass_pct="91.0",
        pass_count="2",
        config_sync_exit="7",
    )

    assert result.returncode == 1
    lines = (tmp_path / "runner.log").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert lines[0].startswith("config-sync:")
    status = json.loads(
        (
            tmp_path
            / "hermes"
            / "reports"
            / "pipeline_status"
            / "msrp_pipeline.json"
        ).read_text(encoding="utf-8")
    )
    assert status["status"] == "failed"
    assert status["metadata"]["phase"] == "config_source_sync"
