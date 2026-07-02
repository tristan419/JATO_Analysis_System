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
