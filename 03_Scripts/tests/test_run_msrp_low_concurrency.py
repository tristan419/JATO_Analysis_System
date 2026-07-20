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


def _extract_shell_function(name: str) -> str:
    script = SCRIPT_PATH.read_text(encoding="utf-8")
    match = re.search(
        rf"({re.escape(name)}\(\) \{{.*?\n\}})\n\n",
        script,
        flags=re.S,
    )
    assert match, f"{name} function not found"
    return match.group(1)


def test_resolve_python_bin_reuses_shared_worktree_venv(tmp_path):
    repo = tmp_path / "repo"
    worktree = tmp_path / "worktree"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--allow-empty",
            "-qm",
            "initial",
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "worktree", "add", "-q", "-b", "test-worktree", str(worktree)],
        check=True,
    )
    python_bin = repo / ".venv" / "bin" / "python"
    python_bin.parent.mkdir(parents=True)
    python_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    python_bin.chmod(0o755)

    snippet = "\n".join(
        [
            "set -Eeuo pipefail",
            'REPO_DIR="$1"',
            "unset JATO_MSRP_PYTHON || true",
            _extract_shell_function("resolve_python_bin"),
            "resolve_python_bin",
        ]
    )
    result = subprocess.run(
        ["bash", "-c", snippet, "bash", str(worktree)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(python_bin)


def test_resolve_python_bin_prefers_explicit_override(tmp_path):
    explicit_python = tmp_path / "python"
    snippet = "\n".join(
        [
            "set -Eeuo pipefail",
            'REPO_DIR="$1"',
            'JATO_MSRP_PYTHON="$2"',
            _extract_shell_function("resolve_python_bin"),
            "resolve_python_bin",
        ]
    )
    result = subprocess.run(
        ["bash", "-c", snippet, "bash", str(tmp_path), str(explicit_python)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(explicit_python)


def test_remove_country_pid_at_handles_last_pid_with_nounset():
    snippet = "\n".join([
        "set -Eeuo pipefail",
        "declare -a pids=(123)",
        "declare -a pid_countries=(se)",
        _extract_shell_function("remove_country_pid_at"),
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
