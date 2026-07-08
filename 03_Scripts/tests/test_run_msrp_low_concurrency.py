import re
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "03_Scripts" / "run_msrp_low_concurrency.sh"


def test_script_syntax_is_valid():
    result = subprocess.run(
        ["bash", "-n", str(SCRIPT_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_remove_country_pid_at_handles_last_pid_with_nounset():
    script = SCRIPT_PATH.read_text(encoding="utf-8")
    match = re.search(
        r"(remove_country_pid_at\(\) \{.*?\n\})\n\nwhile ",
        script,
        flags=re.S,
    )
    assert match, "remove_country_pid_at function not found"
    snippet = "\n".join([
        "set -Eeuo pipefail",
        "declare -a pids=(123)",
        "declare -a pid_countries=(se)",
        match.group(1),
        "remove_country_pid_at 0",
        '[[ "${#pids[@]}" -eq 0 ]]',
        '[[ "${#pid_countries[@]}" -eq 0 ]]',
    ])

    result = subprocess.run(
        ["bash", "-c", snippet],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_dryrun_refreshes_source_governance_before_hermes_progress():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "JATO_MSRP_REFRESH_SOURCE_GOVERNANCE" in script
    assert "msrp_source_repair_backlog.py" in script
    assert "msrp_reference_evidence.py" in script
    assert "msrp_source_review_queue.py" in script
    assert "hermes_msrp_country_progress.py" in script

    source_repair_pos = script.index("MSRP source repair backlog refreshed")
    reference_pos = script.index("MSRP source reference evidence refreshed")
    review_queue_pos = script.index("MSRP source review queue refreshed")
    hermes_pos = script.index("Hermes MSRP country progress refreshed")

    assert source_repair_pos < reference_pos < review_queue_pos < hermes_pos


def test_current_snapshot_refreshes_price_alert_review_queue():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "JATO_MSRP_REFRESH_PRICE_ALERT_REVIEW_QUEUE" in script
    assert "msrp_price_alert_review_queue.py" in script
    assert "msrp_current_price_snapshot.json" in script
    assert "msrp_price_alert_review_queue.json" in script

    snapshot_pos = script.index("Hermes MSRP current price snapshot refreshed")
    queue_pos = script.index("MSRP price alert review queue refreshed")
    readiness_pos = script.index("MSRP readiness audit refreshed")

    assert snapshot_pos < queue_pos < readiness_pos
