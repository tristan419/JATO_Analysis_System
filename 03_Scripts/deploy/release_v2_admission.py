#!/usr/bin/env python3
"""Read-only JATO, database, and Candidate admission checks for release V2."""
from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
from typing import Any, BinaryIO
from urllib.parse import unquote, urlsplit

from jato_quiescence_gate import GateError, inspect_state


MAX_ENV_BYTES = 2 * 1024 * 1024
MAX_COMMAND_OUTPUT = 2 * 1024 * 1024
REVISION_PATTERN = re.compile(r"(?m)^\s*([0-9]{8}_[0-9]{4})\b")
ENV_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
POSTGRES_SCHEMES = frozenset(
    {"postgres", "postgresql", "postgresql+aiopg", "postgresql+asyncpg",
     "postgresql+psycopg", "postgresql+psycopg2"}
)
SAFE_COMMAND_ENV = {
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}
CANDIDATE_DATABASE_PRIVILEGE_KEYS = (
    "transactionReadOnly", "defaultTransactionReadOnly", "roleAttributesReadOnly",
    "connectAllowed", "databaseCreateDenied", "noUnsafeRoleMemberships",
    "noSchemaCreate", "noTableWrites", "noSequenceWrites",
)
CANDIDATE_DATABASE_PRIVILEGE_PROBE = r'''
import json
import os

import psycopg

keys = (
    "transactionReadOnly",
    "defaultTransactionReadOnly",
    "roleAttributesReadOnly",
    "connectAllowed",
    "databaseCreateDenied",
    "noUnsafeRoleMemberships",
    "noSchemaCreate",
    "noTableWrites",
    "noSequenceWrites",
)
query = r"""
WITH RECURSIVE role_memberships(roleid) AS (
    SELECT roleid
    FROM pg_auth_members
    WHERE member = (SELECT oid FROM pg_roles WHERE rolname = current_user)
    UNION
    SELECT membership.roleid
    FROM pg_auth_members AS membership
    JOIN role_memberships AS parent ON membership.member = parent.roleid
)
SELECT
    current_setting('transaction_read_only') = 'on',
    current_setting('default_transaction_read_only') = 'on',
    NOT (
        role.rolsuper OR role.rolcreaterole OR role.rolcreatedb
        OR role.rolreplication OR role.rolbypassrls
    ),
    has_database_privilege(current_user, current_database(), 'CONNECT'),
    NOT has_database_privilege(current_user, current_database(), 'CREATE'),
    NOT EXISTS (
        SELECT 1
        FROM role_memberships AS membership
        JOIN pg_roles AS inherited ON inherited.oid = membership.roleid
        WHERE inherited.rolname <> 'pg_read_all_data'
    ),
    NOT EXISTS (
        SELECT 1
        FROM pg_namespace AS namespace
        WHERE namespace.nspname NOT LIKE 'pg_%'
          AND namespace.nspname <> 'information_schema'
          AND has_schema_privilege(current_user, namespace.oid, 'CREATE')
    ),
    NOT EXISTS (
        SELECT 1
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname NOT LIKE 'pg_%'
          AND namespace.nspname <> 'information_schema'
          AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND (
              has_table_privilege(current_user, relation.oid, 'INSERT')
              OR has_table_privilege(current_user, relation.oid, 'UPDATE')
              OR has_table_privilege(current_user, relation.oid, 'DELETE')
              OR has_table_privilege(current_user, relation.oid, 'TRUNCATE')
              OR has_table_privilege(current_user, relation.oid, 'REFERENCES')
              OR has_table_privilege(current_user, relation.oid, 'TRIGGER')
          )
    ),
    NOT EXISTS (
        SELECT 1
        FROM pg_class AS sequence
        JOIN pg_namespace AS namespace ON namespace.oid = sequence.relnamespace
        WHERE namespace.nspname NOT LIKE 'pg_%'
          AND namespace.nspname <> 'information_schema'
          AND sequence.relkind = 'S'
          AND (
              has_sequence_privilege(current_user, sequence.oid, 'USAGE')
              OR has_sequence_privilege(current_user, sequence.oid, 'UPDATE')
          )
    )
FROM pg_roles AS role
WHERE role.rolname = current_user
"""
with psycopg.connect(os.environ["APP_DATABASE_URL"], connect_timeout=10) as connection:
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
if row is None or len(row) != len(keys):
    raise RuntimeError("candidate database role proof returned no role")
print(json.dumps(dict(zip(keys, row)), sort_keys=True))
'''.strip()


class AdmissionError(RuntimeError):
    """A release cannot safely proceed."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = field(repr=False)
    stderr: str = field(repr=False)


CommandRunner = Callable[[tuple[str, ...], Path | None, Mapping[str, str]], CommandResult]


@dataclass(frozen=True)
class DatabaseRevisionConfig:
    backend_env: Path
    active_root: Path
    candidate_root: Path
    active_python: Path
    candidate_python: Path
    expected_env_uid: int = 0


@dataclass(frozen=True)
class CandidateDatabaseIsolationConfig:
    active_env: Path
    candidate_env: Path
    candidate_root: Path
    candidate_python: Path
    expected_env_uid: int = 0


def run_command(
    arguments: tuple[str, ...],
    cwd: Path | None,
    environment: Mapping[str, str],
) -> CommandResult:
    completed = subprocess.run(
        arguments,
        cwd=cwd,
        env=dict(environment),
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    stdout = completed.stdout[:MAX_COMMAND_OUTPUT]
    stderr = completed.stderr[:MAX_COMMAND_OUTPUT]
    return CommandResult(completed.returncode, stdout, stderr)


def inspect_jato_idle(job_root: Path) -> dict[str, Any]:
    """Reuse the application's existing fail-closed state interpreter."""

    try:
        result = inspect_state(job_root)
    except GateError as exc:
        raise AdmissionError("jato_state_invalid", str(exc)) from exc
    if result.get("busy") is not False:
        raise AdmissionError("jato_busy", "JATO write activity is in progress")
    return dict(result)


def open_regular_lock_file(path: Path) -> BinaryIO:
    """Open one existing JATO coordination namespace without following links."""

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        parent = path.parent.lstat()
    except OSError as exc:
        raise AdmissionError("jato_lock_path_unsafe", "JATO lock parent is unavailable") from exc
    if path.parent.is_symlink() or not stat.S_ISDIR(parent.st_mode):
        raise AdmissionError("jato_lock_path_unsafe", "JATO lock parent is unsafe")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        descriptor = os.open(path, flags, 0o600)
        metadata = os.fstat(descriptor)
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        raise AdmissionError("jato_lock_path_unsafe", "JATO lock cannot be opened") from exc
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise AdmissionError("jato_lock_path_unsafe", "JATO lock is not a regular file")
    return os.fdopen(descriptor, "a+b", closefd=True)


def _jato_final_lock_paths(job_root: Path, active_bundle_lock: Path) -> tuple[Path, ...]:
    upload_root = job_root / "_upload_sessions"
    paths = {
        job_root / "worker.lock",
        active_bundle_lock,
        upload_root / "upload-initiate.lock",
        job_root / "_maintenance" / "baseline-promotion.lock",
    }
    paths.update(
        state.parent / "state.lock"
        for state in job_root.glob("*/job_state.json")
        if not state.parent.name.startswith("_")
    )
    for state in upload_root.glob("*/upload_state.json"):
        paths.add(state.parent / "digest.lock")
        paths.add(state.parent / "state.lock")
    return tuple(sorted(paths, key=str))


@contextmanager
def hold_jato_release_locks(
    job_root: Path,
    active_bundle_lock: Path,
) -> Iterator[dict[str, Any]]:
    """Fail fast and hold the application's own JATO locks during Active restart.

    This is an admission lease, not a maintenance mode: it never waits, creates a
    marker, edits durable state, or starts/stops a worker.
    """

    maintenance_lock = job_root / "_maintenance" / "maintenance-coordination.lock"
    with ExitStack() as stack:
        maintenance = stack.enter_context(open_regular_lock_file(maintenance_lock))
        try:
            fcntl.flock(maintenance.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AdmissionError(
                "jato_lock_busy",
                "JATO maintenance admission is busy; Active was not changed",
            ) from exc
        first = inspect_jato_idle(job_root)
        for path in _jato_final_lock_paths(job_root, active_bundle_lock):
            handle = stack.enter_context(open_regular_lock_file(path))
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise AdmissionError(
                    "jato_lock_busy",
                    "JATO write activity owns a release lock; Active was not changed",
                ) from exc
        second = inspect_jato_idle(job_root)
        yield {"before": first, "afterLocks": second}


def _read_backend_environment(
    path: Path,
    *,
    expected_uid: int,
    expected_mode: int | None = None,
) -> dict[str, str]:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AdmissionError("database_env_unreadable", "backend env is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or (
            expected_mode is not None
            and stat.S_IMODE(metadata.st_mode) != expected_mode
        )
        or metadata.st_size <= 0
        or metadata.st_size > MAX_ENV_BYTES
    ):
        raise AdmissionError("database_env_unsafe", "backend env ownership or mode is unsafe")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise AdmissionError("database_env_unreadable", "backend env cannot be read") from exc
    if len(raw) != metadata.st_size:
        raise AdmissionError("database_env_changed", "backend env changed while reading")
    values: dict[str, str] = {}
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError as exc:
        raise AdmissionError("database_env_invalid", "backend env is not UTF-8") from exc
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export ") or "=" not in line:
            raise AdmissionError("database_env_invalid", "backend env must use KEY=value")
        key, value = line.split("=", 1)
        if not ENV_KEY_PATTERN.fullmatch(key) or key in values:
            raise AdmissionError("database_env_invalid", "backend env key is invalid or repeated")
        if value[:1] in {"'", '"'}:
            if len(value) < 2 or value[-1] != value[0]:
                raise AdmissionError("database_env_invalid", "backend env quote is incomplete")
            value = value[1:-1]
        values[key] = value
    return values


def _database_enabled(values: Mapping[str, str]) -> bool:
    raw = values.get("APP_DATABASE_ENABLED", "true").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise AdmissionError("database_env_invalid", "APP_DATABASE_ENABLED is invalid")


def _sync_postgresql_url(url: str) -> str:
    if "://" not in url:
        raise AdmissionError("database_url_invalid", "database URL has no scheme")
    scheme, remainder = url.split("://", 1)
    if scheme not in POSTGRES_SCHEMES or not remainder:
        raise AdmissionError("database_url_invalid", "only PostgreSQL is supported")
    return f"postgresql+psycopg://{remainder}"


def _native_postgresql_url(url: str) -> str:
    _sync_postgresql_url(url)
    return f"postgresql://{url.split('://', 1)[1]}"


def _database_identity(url: str) -> tuple[str, str, int, str]:
    _sync_postgresql_url(url)
    try:
        parsed = urlsplit(url)
        username = unquote(parsed.username or "")
        hostname = (parsed.hostname or "").lower()
        port = parsed.port or 5432
        database = unquote(parsed.path.lstrip("/"))
    except ValueError as exc:
        raise AdmissionError("database_url_invalid", "database URL is malformed") from exc
    if not username or not hostname or not database:
        raise AdmissionError(
            "database_url_invalid",
            "database URL must include role, host, and database",
        )
    return username, hostname, port, database


def inspect_candidate_database_isolation(
    config: CandidateDatabaseIsolationConfig,
    *,
    runner: CommandRunner = run_command,
) -> dict[str, Any]:
    """Prove Candidate uses the same database through a SELECT-only role."""

    active = _read_backend_environment(
        config.active_env,
        expected_uid=config.expected_env_uid,
    )
    candidate = _read_backend_environment(
        config.candidate_env,
        expected_uid=config.expected_env_uid,
        expected_mode=0o600,
    )
    if not _database_enabled(active) or not _database_enabled(candidate):
        raise AdmissionError(
            "candidate_database_disabled",
            "Active and Candidate database access must both be enabled",
        )
    active_role, *active_endpoint = _database_identity(
        active.get("APP_DATABASE_URL", "")
    )
    candidate_url = candidate.get("APP_DATABASE_URL", "")
    candidate_role, *candidate_endpoint = _database_identity(candidate_url)
    if candidate_role == active_role:
        raise AdmissionError(
            "candidate_database_role_not_isolated",
            "Candidate must not use the Active database role",
        )
    if candidate_endpoint != active_endpoint:
        raise AdmissionError(
            "candidate_database_target_mismatch",
            "Candidate database target differs from Active",
        )
    _require_runtime(
        config.candidate_root,
        config.candidate_python,
        "candidate",
    )
    environment = dict(SAFE_COMMAND_ENV)
    environment.update(
        {
            "APP_DATABASE_URL": _native_postgresql_url(candidate_url),
            "PGCONNECT_TIMEOUT": "10",
            "PGOPTIONS": (
                "-c default_transaction_read_only=on "
                "-c statement_timeout=120000 -c lock_timeout=5000"
            ),
        }
    )
    result = runner(
        (
            str(config.candidate_python),
            "-c",
            CANDIDATE_DATABASE_PRIVILEGE_PROBE,
        ),
        config.candidate_root,
        environment,
    )
    if result.returncode != 0:
        raise AdmissionError(
            "candidate_database_privilege_probe_failed",
            "Candidate database privilege proof failed",
        )
    try:
        proof = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise AdmissionError(
            "candidate_database_privilege_probe_invalid",
            "Candidate database privilege proof is invalid",
        ) from exc
    if not isinstance(proof, dict) or set(proof) != set(
        CANDIDATE_DATABASE_PRIVILEGE_KEYS
    ):
        raise AdmissionError(
            "candidate_database_privilege_probe_invalid",
            "Candidate database privilege proof is incomplete",
        )
    failed = sorted(
        key
        for key in CANDIDATE_DATABASE_PRIVILEGE_KEYS
        if proof.get(key) is not True
    )
    if failed:
        raise AdmissionError(
            "candidate_database_privileges_not_read_only",
            "Candidate database role is not SELECT-only: " + ", ".join(failed),
        )
    return {
        "status": "isolated",
        "sameDatabase": True,
        "distinctRole": True,
        "privilegeProof": True,
        "candidateEnvMode": "0600",
    }


def _require_runtime(root: Path, python: Path, label: str) -> None:
    try:
        root_mode = root.lstat().st_mode
        python_mode = python.lstat().st_mode
    except OSError as exc:
        raise AdmissionError("database_runtime_unavailable", f"{label} runtime is unavailable") from exc
    if root.is_symlink() or not stat.S_ISDIR(root_mode):
        raise AdmissionError("database_runtime_unsafe", f"{label} root is unsafe")
    if python.is_symlink() or not stat.S_ISREG(python_mode) or not os.access(python, os.X_OK):
        raise AdmissionError("database_runtime_unsafe", f"{label} Python is unsafe")


def _backend_working_directory(root: Path, label: str) -> Path:
    backend = root / "06_AppPlatform/backend"
    alembic_ini = backend / "alembic.ini"
    try:
        backend_mode = backend.lstat().st_mode
        alembic_mode = alembic_ini.lstat().st_mode
    except OSError as exc:
        raise AdmissionError(
            "database_runtime_unavailable",
            f"{label} Alembic runtime is unavailable",
        ) from exc
    if (
        backend.is_symlink()
        or not stat.S_ISDIR(backend_mode)
        or alembic_ini.is_symlink()
        or not stat.S_ISREG(alembic_mode)
    ):
        raise AdmissionError("database_runtime_unsafe", f"{label} Alembic runtime is unsafe")
    return backend


def _revision_set(result: CommandResult, label: str) -> tuple[str, ...]:
    if result.returncode != 0:
        raise AdmissionError("database_revision_failed", f"{label} failed")
    revisions = tuple(sorted(set(REVISION_PATTERN.findall(result.stdout))))
    if not revisions:
        raise AdmissionError("database_revision_invalid", f"{label} returned no revision")
    return revisions


def inspect_database_compatibility(
    config: DatabaseRevisionConfig,
    *,
    runner: CommandRunner = run_command,
) -> dict[str, Any]:
    values = _read_backend_environment(
        config.backend_env,
        expected_uid=config.expected_env_uid,
    )
    if not _database_enabled(values):
        return {"status": "compatible", "reason": "database-disabled", "current": [], "heads": []}
    raw_url = values.get("APP_DATABASE_URL", "")
    sync_url = _sync_postgresql_url(raw_url)
    _require_runtime(config.active_root, config.active_python, "active")
    _require_runtime(config.candidate_root, config.candidate_python, "candidate")
    active_backend = _backend_working_directory(config.active_root, "active")
    candidate_backend = _backend_working_directory(config.candidate_root, "candidate")
    environment = dict(SAFE_COMMAND_ENV)
    environment.update(
        {
            "APP_DATABASE_ENABLED": "true",
            "APP_DATABASE_URL": sync_url,
            "DATABASE_URL": sync_url,
            "PGOPTIONS": "-c default_transaction_read_only=on",
        }
    )
    current = _revision_set(
        runner(
            (str(config.active_python), "-m", "alembic", "current"),
            active_backend,
            environment,
        ),
        "alembic current",
    )
    heads = _revision_set(
        runner(
            (str(config.candidate_python), "-m", "alembic", "heads"),
            candidate_backend,
            environment,
        ),
        "alembic heads",
    )
    status = "compatible" if current == heads else "migration-required"
    return {
        "status": status,
        "reason": "revision-match" if current == heads else "revision-mismatch",
        "current": list(current),
        "heads": list(heads),
        "readOnly": True,
    }
