from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys
from urllib.parse import unquote, urlsplit

import pytest


ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "03_Scripts/deploy"
sys.path.insert(0, str(DEPLOY))
HELPER = DEPLOY / "release_v2_admission.py"
SPEC = importlib.util.spec_from_file_location("release_v2_admission", HELPER)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def test_jato_idle_reuses_existing_gate(tmp_path: Path) -> None:
    write_json(tmp_path / "job-a/job_state.json", {"status": "success"})
    result = MODULE.inspect_jato_idle(tmp_path)
    assert result["busy"] is False


def test_jato_busy_fails_closed(tmp_path: Path) -> None:
    write_json(tmp_path / "job-a/job_state.json", {"status": "running"})
    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_jato_idle(tmp_path)
    assert caught.value.code == "jato_busy"


def test_jato_invalid_state_preserves_existing_gate_diagnostic(tmp_path: Path) -> None:
    write_json(tmp_path / "job-a/job_state.json", {"status": "unknown"})
    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_jato_idle(tmp_path)
    assert caught.value.code == "jato_state_invalid"


def test_jato_release_locks_are_nonblocking_and_released(tmp_path: Path) -> None:
    job_root = tmp_path / "jobs"
    active_bundle_lock = tmp_path / "processed/active-bundle.lock"
    job_root.mkdir()
    active_bundle_lock.parent.mkdir()

    with MODULE.hold_jato_release_locks(job_root, active_bundle_lock) as proof:
        assert proof["before"]["busy"] is False
        assert proof["afterLocks"]["busy"] is False
        with pytest.raises(MODULE.AdmissionError) as caught:
            with MODULE.hold_jato_release_locks(job_root, active_bundle_lock):
                pass
        assert caught.value.code == "jato_lock_busy"

    with MODULE.hold_jato_release_locks(job_root, active_bundle_lock):
        pass


def test_jato_release_locks_reject_busy_state_without_waiting(tmp_path: Path) -> None:
    job_root = tmp_path / "jobs"
    active_bundle_lock = tmp_path / "processed/active-bundle.lock"
    write_json(job_root / "job-a/job_state.json", {"status": "running"})
    active_bundle_lock.parent.mkdir()

    with pytest.raises(MODULE.AdmissionError) as caught:
        with MODULE.hold_jato_release_locks(job_root, active_bundle_lock):
            pass

    assert caught.value.code == "jato_busy"


def database_config(tmp_path: Path, payload: str) -> MODULE.DatabaseRevisionConfig:
    env = tmp_path / "backend.env"
    env.write_text(payload, encoding="utf-8")
    env.chmod(0o600)
    active = tmp_path / "active"
    candidate = tmp_path / "candidate"
    active.mkdir()
    candidate.mkdir()
    for root in (active, candidate):
        backend = root / "06_AppPlatform/backend"
        backend.mkdir(parents=True)
        (backend / "alembic.ini").write_text(
            "[alembic]\nscript_location = alembic\n",
            encoding="utf-8",
        )
    active_python = active / "python"
    candidate_python = candidate / "python"
    active_python.write_text("python", encoding="utf-8")
    candidate_python.write_text("python", encoding="utf-8")
    active_python.chmod(0o700)
    candidate_python.chmod(0o700)
    return MODULE.DatabaseRevisionConfig(
        backend_env=env,
        active_root=active,
        candidate_root=candidate,
        active_python=active_python,
        candidate_python=candidate_python,
        expected_env_uid=os.getuid(),
    )


def candidate_database_config(
    tmp_path: Path,
    *,
    active_url: str,
    candidate_url: str,
    candidate_overrides: dict[str, str] | None = None,
) -> MODULE.CandidateDatabaseIsolationConfig:
    active = tmp_path / "backend.env"
    candidate = tmp_path / "candidate-database.env"
    active.write_text(
        "APP_DATABASE_ENABLED=true\n"
        f"APP_DATABASE_URL={active_url}\n"
        f"APP_JWT_SECRET={'a' * 64}\n",
        encoding="utf-8",
    )
    sandbox_database = unquote(urlsplit(candidate_url).path.lstrip("/"))
    candidate_values = {
        "APP_DATABASE_ENABLED": "true",
        "APP_DATABASE_URL": candidate_url,
        "APP_CANDIDATE_SANDBOX_DATABASE": sandbox_database,
        "APP_CANDIDATE_SNAPSHOT_AT": "2026-08-09T08:09:10.123456Z",
        "APP_AUTH_ENABLED": "true",
        "APP_AUTH_REQUIRED": "true",
        "APP_AUTH_TOKEN": "",
        "APP_TOKEN_ROLE_MAP": "",
        "APP_RUNTIME_READ_ONLY": "false",
        "APP_JWT_SECRET": "c" * 64,
    }
    candidate_values.update(candidate_overrides or {})
    candidate.write_text(
        "".join(f"{key}={value}\n" for key, value in candidate_values.items()),
        encoding="utf-8",
    )
    active.chmod(0o600)
    candidate.chmod(0o600)
    candidate_root = tmp_path / "candidate-release"
    candidate_root.mkdir()
    candidate_python = candidate_root / "python"
    candidate_python.write_text("python", encoding="utf-8")
    candidate_python.chmod(0o700)
    return MODULE.CandidateDatabaseIsolationConfig(
        active_env=active,
        candidate_env=candidate,
        candidate_root=candidate_root,
        candidate_python=candidate_python,
        expected_env_uid=os.getuid(),
    )


def writable_sandbox_runner(arguments, cwd, environment):
    assert arguments[1:] == ("-c", MODULE.CANDIDATE_DATABASE_PRIVILEGE_PROBE)
    assert cwd == Path(arguments[0]).parent
    assert environment["APP_DATABASE_URL"].startswith("postgresql://")
    assert "default_transaction_read_only" not in environment["PGOPTIONS"]
    assert environment["ACTIVE_DATABASE_NAME"] == "jato"
    assert environment["CANDIDATE_DATABASE_NAME"].startswith("jato_candidate_")
    assert environment["CANDIDATE_DATABASE_ROLE"] == "jato_candidate_writer"
    proof = {
        key: True for key in MODULE.CANDIDATE_DATABASE_PRIVILEGE_KEYS
    }
    return MODULE.CommandResult(0, json.dumps(proof), "")


def test_candidate_database_uses_writable_sandbox_on_same_cluster(tmp_path: Path) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql+asyncpg://jato_active:secret@db.example:5432/jato",
        candidate_url=(
            "postgresql+asyncpg://jato_candidate_writer:other@db.example:5432/"
            "jato_candidate_20260809_abcd1234"
        ),
    )

    assert MODULE.inspect_candidate_database_isolation(
        config,
        runner=writable_sandbox_runner,
    ) == {
        "status": "isolated",
        "sameCluster": True,
        "distinctDatabase": True,
        "distinctRole": True,
        "writable": True,
        "activeConnectDenied": True,
        "privilegeProof": True,
        "candidateEnvMode": "0600",
        "snapshotAt": "2026-08-09T08:09:10.123456Z",
    }


@pytest.mark.parametrize(
    "failed_key",
    MODULE.CANDIDATE_DATABASE_PRIVILEGE_KEYS,
)
def test_candidate_database_rejects_incomplete_sandbox_privileges(
    tmp_path: Path,
    failed_key: str,
) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://active:one@db.example/jato",
        candidate_url=(
            "postgresql://jato_candidate_writer:two@db.example/"
            "jato_candidate_20260809_abcd1234"
        ),
    )

    def incomplete_runner(arguments, cwd, environment):
        del arguments, cwd, environment
        proof = {
            key: True for key in MODULE.CANDIDATE_DATABASE_PRIVILEGE_KEYS
        }
        proof[failed_key] = False
        return MODULE.CommandResult(0, json.dumps(proof), "")

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(
            config,
            runner=incomplete_runner,
        )

    assert caught.value.code == "candidate_database_privileges_unsafe"
    assert failed_key in str(caught.value)


def test_candidate_database_rejects_active_role_reuse(tmp_path: Path) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://shared:one@db.example/jato",
        candidate_url=(
            "postgresql://shared:two@db.example/"
            "jato_candidate_20260809_abcd1234"
        ),
    )

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(config)

    assert caught.value.code == "candidate_database_role_not_isolated"
    assert "one" not in str(caught.value)
    assert "two" not in str(caught.value)


def test_candidate_database_rejects_active_database_reuse(tmp_path: Path) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://active:one@db.example/jato",
        candidate_url="postgresql://candidate:two@db.example/jato",
    )

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(config)

    assert caught.value.code == "candidate_database_not_isolated"


def test_candidate_database_rejects_different_cluster(tmp_path: Path) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://active:one@db.example/jato",
        candidate_url=(
            "postgresql://candidate:two@other.example/"
            "jato_candidate_20260809_abcd1234"
        ),
    )

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(config)

    assert caught.value.code == "candidate_database_cluster_mismatch"


@pytest.mark.parametrize(
    ("marker", "expected_code"),
    [
        ("production", "candidate_database_marker_invalid"),
        ("JATO_CANDIDATE_UPPER", "candidate_database_marker_invalid"),
        ("jato_candidate_other", "candidate_database_marker_mismatch"),
    ],
)
def test_candidate_database_requires_safe_matching_marker(
    tmp_path: Path,
    marker: str,
    expected_code: str,
) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://active:one@db.example/jato",
        candidate_url=(
            "postgresql://candidate:two@db.example/"
            "jato_candidate_20260809_abcd1234"
        ),
        candidate_overrides={"APP_CANDIDATE_SANDBOX_DATABASE": marker},
    )

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(config)

    assert caught.value.code == expected_code


def test_candidate_database_requires_exact_private_mode(tmp_path: Path) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://active:one@db.example/jato",
        candidate_url=(
            "postgresql://candidate:two@db.example/"
            "jato_candidate_20260809_abcd1234"
        ),
    )
    config.candidate_env.chmod(0o640)

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(config)

    assert caught.value.code == "database_env_unsafe"


@pytest.mark.parametrize(
    ("overrides", "expected_code"),
    [
        ({"APP_AUTH_ENABLED": "false"}, "candidate_auth_disabled"),
        ({"APP_AUTH_REQUIRED": "false"}, "candidate_auth_not_required"),
        ({"APP_AUTH_TOKEN": "static-token"}, "candidate_static_auth_enabled"),
        ({"APP_TOKEN_ROLE_MAP": "token:admin"}, "candidate_static_auth_enabled"),
        ({"APP_RUNTIME_READ_ONLY": "true"}, "candidate_runtime_read_only"),
        ({"APP_AUTH_ENABLED": "sometimes"}, "candidate_env_invalid"),
    ],
)
def test_candidate_database_requires_writable_authenticated_runtime(
    tmp_path: Path,
    overrides: dict[str, str],
    expected_code: str,
) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://active:one@db.example/jato",
        candidate_url=(
            "postgresql://candidate:two@db.example/"
            "jato_candidate_20260809_abcd1234"
        ),
        candidate_overrides=overrides,
    )

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(config)

    assert caught.value.code == expected_code


@pytest.mark.parametrize(
    "snapshot_at",
    ["", "2026-08-09T08:09:10+00:00", "2026-02-30T08:09:10Z"],
)
def test_candidate_database_requires_strict_utc_snapshot_time(
    tmp_path: Path,
    snapshot_at: str,
) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://active:one@db.example/jato",
        candidate_url=(
            "postgresql://candidate:two@db.example/"
            "jato_candidate_20260809_abcd1234"
        ),
        candidate_overrides={"APP_CANDIDATE_SNAPSHOT_AT": snapshot_at},
    )

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(config)

    assert caught.value.code == "candidate_snapshot_at_invalid"


@pytest.mark.parametrize(
    ("secret", "expected_code"),
    [
        ("tiny-value", "candidate_jwt_secret_invalid"),
        ("a" * 64, "candidate_jwt_secret_not_isolated"),
    ],
)
def test_candidate_database_requires_independent_jwt_secret(
    tmp_path: Path,
    secret: str,
    expected_code: str,
) -> None:
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://active:one@db.example/jato",
        candidate_url=(
            "postgresql://candidate:two@db.example/"
            "jato_candidate_20260809_abcd1234"
        ),
        candidate_overrides={"APP_JWT_SECRET": secret},
    )

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(config)

    assert caught.value.code == expected_code
    assert secret not in str(caught.value)


def test_candidate_database_probe_failure_does_not_expose_url(tmp_path: Path) -> None:
    secret = "candidate-database-password"
    config = candidate_database_config(
        tmp_path,
        active_url="postgresql://active:one@db.example/jato",
        candidate_url=(
            f"postgresql://candidate:{secret}@db.example/"
            "jato_candidate_20260809_abcd1234"
        ),
    )

    def failed_runner(arguments, cwd, environment):
        assert secret not in " ".join(arguments)
        del cwd, environment
        return MODULE.CommandResult(1, "", "connection failed")

    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_candidate_database_isolation(config, runner=failed_runner)

    assert caught.value.code == "candidate_database_privilege_probe_failed"
    assert secret not in str(caught.value)
    assert secret not in repr(config)


def test_database_revision_match_is_compatible(tmp_path: Path) -> None:
    config = database_config(
        tmp_path,
        "APP_DATABASE_ENABLED=true\n"
        "APP_DATABASE_URL=postgresql://user:secret@db.example/jato\n",
    )
    calls: list[tuple[tuple[str, ...], Path]] = []

    def runner(arguments, cwd, environment):
        calls.append((arguments, cwd))
        assert environment["PGOPTIONS"] == "-c default_transaction_read_only=on"
        return MODULE.CommandResult(0, "20260715_0046 (head)\n", "")

    result = MODULE.inspect_database_compatibility(config, runner=runner)
    assert result == {
        "status": "compatible",
        "reason": "revision-match",
        "current": ["20260715_0046"],
        "heads": ["20260715_0046"],
        "readOnly": True,
    }
    assert [call[0][-1] for call in calls] == ["current", "heads"]
    assert [call[1] for call in calls] == [
        config.active_root / "06_AppPlatform/backend",
        config.candidate_root / "06_AppPlatform/backend",
    ]


def test_database_revision_mismatch_requires_migration(tmp_path: Path) -> None:
    config = database_config(
        tmp_path,
        "APP_DATABASE_ENABLED=true\nAPP_DATABASE_URL=postgresql://u:p@db/jato\n",
    )

    def runner(arguments, cwd, environment):
        revision = "20260715_0046" if arguments[-1] == "current" else "20260801_0047"
        return MODULE.CommandResult(0, revision + " (head)\n", "")

    result = MODULE.inspect_database_compatibility(config, runner=runner)
    assert result["status"] == "migration-required"
    assert result["current"] == ["20260715_0046"]
    assert result["heads"] == ["20260801_0047"]


def test_database_disabled_does_not_run_commands(tmp_path: Path) -> None:
    config = database_config(tmp_path, "APP_DATABASE_ENABLED=false\n")

    def forbidden_runner(arguments, cwd, environment):
        raise AssertionError("database-disabled must not run Alembic")

    result = MODULE.inspect_database_compatibility(config, runner=forbidden_runner)
    assert result["reason"] == "database-disabled"


def test_database_secret_is_not_in_error_or_repr(tmp_path: Path) -> None:
    secret = "very-private-password"
    config = database_config(
        tmp_path,
        f"APP_DATABASE_ENABLED=true\nAPP_DATABASE_URL=mysql://user:{secret}@db/jato\n",
    )
    with pytest.raises(MODULE.AdmissionError) as caught:
        MODULE.inspect_database_compatibility(config)
    assert secret not in str(caught.value)
    assert secret not in repr(config)
