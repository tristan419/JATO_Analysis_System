#!/usr/bin/env python3
"""Four-operation fixed Active/Candidate release controller V2."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Any, Iterator, Literal
import urllib.error
import urllib.request
import uuid


DEPLOY_DIR = Path(__file__).resolve().parent
if str(DEPLOY_DIR) not in sys.path:
    sys.path.insert(0, str(DEPLOY_DIR))

from release_v2_admission import (  # noqa: E402
    AdmissionError,
    CandidateDatabaseIsolationConfig,
    CommandResult,
    DatabaseRevisionConfig,
    inspect_candidate_database_isolation,
    inspect_database_compatibility,
    inspect_jato_idle,
    hold_jato_release_locks,
    open_regular_lock_file,
)
from release_v2_store import (  # noqa: E402
    PointerPair,
    ReleaseIdentity,
    ReleaseLayout,
    ReleaseManifest,
    ReleaseStoreError,
    atomic_exchange_pointers,
    atomic_symlink,
    clear_pointer,
    collect_archive_cache,
    collect_garbage,
    hash_regular_file,
    promote_staged_release,
    read_manifest_file,
    read_pointer,
    read_pointer_pair,
    remove_if_unreferenced,
    validate_release_directory,
)


Action = Literal["prepare-candidate", "discard-candidate", "update-active", "rollback-active"]
ACTION_CHECKS: dict[Action, tuple[str, ...]] = {
    "prepare-candidate": (
        "production_lock_acquired",
        "active_baseline_verified",
        "fixed_active_routing_verified",
        "release_manifest_verified",
        "candidate_database_isolation_verified",
        "candidate_previous_state_verified",
        "database_revision_compatible",
        "preview_contract_verified",
        "candidate_backend_verified",
        "candidate_monthly_disabled_verified",
        "candidate_preview_verified",
        "active_unchanged",
    ),
    "discard-candidate": (
        "production_lock_acquired",
        "active_baseline_verified",
        "fixed_active_routing_verified",
        "candidate_stopped",
        "candidate_pointers_cleared",
        "active_unchanged",
    ),
    "update-active": (
        "production_lock_acquired",
        "release_manifest_revalidated",
        "fixed_active_routing_verified",
        "fixed_active_owner_verified",
        "active_restore_point_verified",
        "active_baseline_verified",
        "candidate_database_isolation_revalidated",
        "candidate_monthly_disabled_revalidated",
        "candidate_revalidated",
        "jato_release_locks_acquired",
        "jato_idle_before_update",
        "database_revision_compatible",
        "jato_idle_at_restart",
        "active_updated_and_public_verified",
    ),
    "rollback-active": (
        "production_lock_acquired",
        "fixed_active_routing_verified",
        "fixed_active_owner_verified",
        "active_baseline_verified",
        "jato_release_locks_acquired",
        "database_revision_compatible",
        "jato_idle_before_rollback",
        "jato_idle_at_restart",
        "active_rollback_verified",
    ),
}
ACTIVE_SLOT = "8000"
CANDIDATE_SLOT = "8001"
ACTIVE_UNIT = "jato-fullstack-backend@8000.service"
CANDIDATE_UNIT = "jato-fullstack-backend@8001.service"
PREVIEW_UNIT = "jato-candidate-preview.service"
ACTIVE_MEMORY_HIGH = 6 * 1024**3
ACTIVE_MEMORY_MAX = 8 * 1024**3
CANDIDATE_MEMORY_HIGH = 3 * 1024**3
CANDIDATE_MEMORY_MAX = 4 * 1024**3
PREVIEW_MEMORY_HIGH = 256 * 1024**2
PREVIEW_MEMORY_MAX = 512 * 1024**2
EXIT_ACTIVE_RESTORE_UNPROVEN = 81
SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class V2Error(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


@dataclass(frozen=True)
class ControllerConfig:
    layout: ReleaseLayout = ReleaseLayout()
    legacy_active_root: Path = Path("/opt/JATO_Analysis_System-main")
    durable_processed_root: Path = Path(
        "/opt/JATO_Analysis_System-main/04_Processed_data"
    )
    slot_env_root: Path = Path("/etc/jato-fullstack/slots")
    backend_env: Path = Path("/etc/jato-fullstack/backend.env")
    candidate_database_env: Path = Path(
        "/etc/jato-fullstack/candidate-database.env"
    )
    preview_config: Path = Path("/etc/jato-fullstack/candidate-preview-v2.conf")
    preview_unit: Path = Path("/etc/systemd/system/jato-candidate-preview.service")
    preview_config_contract: Path = DEPLOY_DIR / "nginx/jato_candidate_preview_v2.conf"
    preview_unit_contract: Path = DEPLOY_DIR / "systemd/jato-candidate-preview.service"
    candidate_backend_unit: Path = Path(
        "/etc/systemd/system/jato-fullstack-backend@8001.service"
    )
    active_backend_unit: Path = Path(
        "/etc/systemd/system/jato-fullstack-backend@8000.service"
    )
    candidate_backend_unit_contract: Path = (
        DEPLOY_DIR / "systemd/jato-fullstack-backend@.service"
    )
    candidate_readonly_dropin: Path = Path(
        "/etc/systemd/system/jato-fullstack-backend@8001.service.d/"
        "20-candidate-readonly.conf"
    )
    candidate_readonly_contract: Path = (
        DEPLOY_DIR
        / "systemd/jato-fullstack-backend@8001.service.d/20-candidate-readonly.conf"
    )
    active_release_config: Path = Path(
        "/etc/jato-fullstack/nginx/active-release.conf"
    )
    active_release_contract: Path = DEPLOY_DIR / "nginx/jato_active_release_v2.conf"
    active_compat_link: Path = Path("/opt/jato/active")
    preview_runtime_root: Path = Path("/var/cache/jato-candidate-preview")
    reports_root: Path = Path("/opt/jato/operation-reports")
    archive_cache_root: Path | None = None
    production_lock: Path = (
        Path.home() / ".local/state/jato-production-release/production-deploy.lock"
    )
    active_slot_file: Path = Path("/var/lib/jato-release/active-slot")
    deployment_marker: Path = Path("/var/lib/jato-release/deployment-maintenance")
    jato_job_root: Path = Path(
        "/opt/jato/shared/04_Processed_data/ops/jato_monthly_update_jobs"
    )
    public_origin: str = "https://www.ojeur.cloud"
    expected_owner_uid: int = 0


LinkAnchor = tuple[int, int, int, int, str, str]


@dataclass(frozen=True)
class ActiveBaseline:
    identity: ReleaseIdentity | None
    current_anchor: LinkAnchor
    previous_anchor: LinkAnchor | None
    legacy: bool


CommandRunner = Callable[[tuple[str, ...], int], CommandResult]
HttpReader = Callable[[str, int], tuple[int, dict[str, Any]]]
DatabaseInspector = Callable[[DatabaseRevisionConfig], dict[str, Any]]
CandidateDatabaseInspector = Callable[
    [CandidateDatabaseIsolationConfig],
    dict[str, Any],
]
JatoInspector = Callable[[Path], dict[str, Any]]
JatoLockHolder = Callable[[Path, Path], AbstractContextManager[dict[str, Any]]]


def run_command(arguments: tuple[str, ...], timeout_seconds: int) -> CommandResult:
    completed = subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    return CommandResult(completed.returncode, completed.stdout, completed.stderr)


def read_http_json(url: str, timeout_seconds: int) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = int(response.status)
            body = response.read(512 * 1024)
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        body = exc.read(512 * 1024)
    except OSError as exc:
        raise V2Error("http_unavailable", f"endpoint is unavailable: {url}") from exc
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise V2Error("http_json_invalid", f"endpoint did not return JSON: {url}") from exc
    if not isinstance(payload, dict):
        raise V2Error("http_json_invalid", f"endpoint did not return an object: {url}")
    return status, payload


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00",
        "Z",
    )


def _atomic_write(path: Path, payload: str, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink():
        raise V2Error("path_parent_unsafe", f"parent must not be a symlink: {path.parent}")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        parent_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


@contextmanager
def production_lock(path: Path) -> Iterator[None]:
    with open_regular_lock_file(path) as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise V2Error("production_lock_busy", "another release operation is running") from exc
        yield


def _identity_dict(identity: ReleaseIdentity | None) -> dict[str, str] | None:
    if identity is None:
        return None
    return {"commitSha": identity.commit_sha, "archiveSha256": identity.archive_sha256}


def _target_dict(
    identity: ReleaseIdentity | None,
    manifest_sha256: str | None,
) -> dict[str, str] | None:
    result = _identity_dict(identity)
    if result is not None and manifest_sha256 is not None:
        result["manifestSha256"] = manifest_sha256
    return result


def _failure_dict(error: Exception) -> dict[str, Any]:
    message = (
        str(error)
        if isinstance(error, (V2Error, ReleaseStoreError, AdmissionError))
        else type(error).__name__
    )
    result: dict[str, Any] = {
        "code": getattr(error, "code", "unexpected_error"),
        "message": message,
    }
    details = getattr(error, "details", None)
    if details:
        result["details"] = details
    return result


class FixedReleaseController:
    def __init__(
        self,
        config: ControllerConfig,
        *,
        runner: CommandRunner = run_command,
        http_reader: HttpReader = read_http_json,
        database_inspector: DatabaseInspector = inspect_database_compatibility,
        candidate_database_inspector: CandidateDatabaseInspector = (
            inspect_candidate_database_isolation
        ),
        jato_inspector: JatoInspector = inspect_jato_idle,
        jato_lock_holder: JatoLockHolder = hold_jato_release_locks,
    ) -> None:
        self.config = config
        self.runner = runner
        self.http_reader = http_reader
        self.database_inspector = database_inspector
        self.candidate_database_inspector = candidate_database_inspector
        self.jato_inspector = jato_inspector
        self.jato_lock_holder = jato_lock_holder

    def _command(self, *arguments: str, timeout: int = 90) -> str:
        result = self.runner(tuple(arguments), timeout)
        if result.returncode != 0:
            raise V2Error("command_failed", f"command failed: {arguments[0]}")
        return result.stdout.strip()

    def _systemctl(self, *arguments: str, timeout: int = 90) -> str:
        return self._command("systemctl", *arguments, timeout=timeout)

    def _stop_unit(self, unit: str) -> None:
        load_state = self._systemctl("show", unit, "-p", "LoadState", "--value")
        if load_state == "not-found":
            return
        self._systemctl("stop", unit)
        active_state = self._systemctl("show", unit, "-p", "ActiveState", "--value")
        if active_state != "inactive":
            raise V2Error("unit_stop_failed", f"unit did not stop: {unit}")

    def _link_anchor(self, path: Path, *, required: bool) -> LinkAnchor | None:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            if required:
                raise V2Error("active_baseline_missing", "Active current link is missing")
            return None
        except OSError as exc:
            raise V2Error("active_baseline_unreadable", "Active link cannot be inspected") from exc
        if not stat.S_ISLNK(metadata.st_mode) or metadata.st_uid != self.config.expected_owner_uid:
            raise V2Error("active_baseline_unsafe", "Active pointer is not a trusted symlink")
        try:
            raw_target = os.readlink(path)
            resolved_target = path.resolve(strict=True)
        except OSError as exc:
            raise V2Error("active_baseline_unreadable", "Active link cannot be resolved") from exc
        return (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_uid,
            metadata.st_mtime_ns,
            raw_target,
            str(resolved_target),
        )

    def _active_baseline(self) -> ActiveBaseline:
        current_path = self.config.layout.pointer_path(ACTIVE_SLOT, "current")
        previous_path = self.config.layout.pointer_path(ACTIVE_SLOT, "previous")
        current_anchor = self._link_anchor(current_path, required=True)
        assert current_anchor is not None
        previous_anchor = self._link_anchor(previous_path, required=False)
        try:
            identity = read_pointer(self.config.layout, ACTIVE_SLOT, "current")
        except ReleaseStoreError as exc:
            legacy_root = self.config.legacy_active_root
            try:
                legacy_metadata = legacy_root.lstat()
                expected_root = legacy_root.resolve(strict=True)
            except OSError as root_exc:
                raise V2Error(
                    "v2_bootstrap_required",
                    "Active current is neither a V2 release nor the exact legacy root",
                ) from root_exc
            if (
                legacy_root.is_symlink()
                or not stat.S_ISDIR(legacy_metadata.st_mode)
                or Path(current_anchor[-1]) != expected_root
            ):
                raise V2Error(
                    "v2_bootstrap_required",
                    "Active current is neither a V2 release nor the exact legacy root",
                ) from exc
            return ActiveBaseline(None, current_anchor, previous_anchor, True)
        if identity is None:
            raise V2Error("active_baseline_missing", "Active current link is missing")
        return ActiveBaseline(identity, current_anchor, previous_anchor, False)

    def _verify_active_baseline_unchanged(self, baseline: ActiveBaseline) -> None:
        current = self._link_anchor(
            self.config.layout.pointer_path(ACTIVE_SLOT, "current"),
            required=True,
        )
        previous = self._link_anchor(
            self.config.layout.pointer_path(ACTIVE_SLOT, "previous"),
            required=False,
        )
        if current != baseline.current_anchor or previous != baseline.previous_anchor:
            raise V2Error(
                "active_changed_during_candidate_operation",
                "Active pointers changed during a Candidate-only operation",
            )

    def _active_project_root(self, baseline: ActiveBaseline) -> Path:
        if baseline.legacy:
            return self.config.legacy_active_root
        if baseline.identity is None:
            raise V2Error("active_baseline_missing", "Active release identity is missing")
        return validate_release_directory(self.config.layout, baseline.identity)

    def _active_bundle_lock(self, baseline: ActiveBaseline) -> Path:
        project_processed = self._active_project_root(baseline) / "04_Processed_data"
        durable_processed = self.config.durable_processed_root
        try:
            durable_metadata = durable_processed.lstat()
            expected = durable_processed.resolve(strict=True)
            actual = project_processed.resolve(strict=True)
        except OSError as exc:
            raise V2Error(
                "durable_processed_root_unavailable",
                "durable processed-data path cannot be verified",
            ) from exc
        if (
            durable_processed.is_symlink()
            or not stat.S_ISDIR(durable_metadata.st_mode)
            or actual != expected
        ):
            raise V2Error(
                "durable_processed_root_mismatch",
                "Active processed-data path differs from the durable data root",
            )
        return durable_processed / "active-bundle.lock"

    def _pointer_snapshot(self) -> dict[str, Any]:
        return {
            "active": {
                "current": _identity_dict(read_pointer(self.config.layout, ACTIVE_SLOT, "current")),
                "previous": _identity_dict(read_pointer(self.config.layout, ACTIVE_SLOT, "previous")),
            },
            "candidate": {
                "current": _identity_dict(read_pointer(self.config.layout, CANDIDATE_SLOT, "current")),
                "previous": _identity_dict(read_pointer(self.config.layout, CANDIDATE_SLOT, "previous")),
            },
        }

    def _unit_state(self, unit: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for key in ("LoadState", "ActiveState", "SubState", "MemoryHigh", "MemoryMax"):
            try:
                value = self.runner(("systemctl", "show", unit, "-p", key, "--value"), 20)
            except Exception as exc:
                result[key] = f"error:{type(exc).__name__}"
                continue
            result[key] = value.stdout.strip() if value.returncode == 0 else "unknown"
        return result

    def _snapshot(self) -> dict[str, Any]:
        try:
            pointers = self._pointer_snapshot()
        except Exception as exc:
            pointers = {"error": getattr(exc, "code", type(exc).__name__)}
        return {
            "pointers": pointers,
            "activeUnit": self._unit_state(ACTIVE_UNIT),
            "candidateUnit": self._unit_state(CANDIDATE_UNIT),
            "previewUnit": self._unit_state(PREVIEW_UNIT),
        }

    def _write_report(self, report: dict[str, Any]) -> Path:
        operation = str(report["operationId"])
        destination = self.config.reports_root / f"{operation}.json"
        _atomic_write(
            destination,
            json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            0o600,
        )
        return destination

    def _best_effort_archive_cache_gc(
        self,
        mutation: dict[str, bool],
        passed: list[str],
        *,
        legacy_active: bool = False,
    ) -> None:
        if legacy_active:
            passed.append("archive_cache_gc_deferred_for_legacy_active")
            return
        cache_root = self.config.archive_cache_root
        if cache_root is None:
            passed.append("archive_cache_gc_disabled")
            return
        try:
            result = collect_archive_cache(self.config.layout, cache_root)
        except Exception as exc:
            code = getattr(exc, "code", type(exc).__name__)
            passed.append(f"archive_cache_gc_deferred:{code}")
            return
        mutation["releaseStoreChanged"] |= bool(result.removed_paths)
        passed.append(f"archive_cache_files_removed:{len(result.removed_paths)}")
        for code in sorted({item["code"] for item in result.diagnostics}):
            passed.append(f"archive_cache_gc_diagnostic:{code}")

    def _execute(
        self,
        action: Action,
        target: ReleaseIdentity | None,
        body: Callable[[dict[str, bool], list[str]], ReleaseIdentity | None],
        *,
        manifest_sha256: str | None = None,
    ) -> dict[str, Any]:
        started = _utc_now()
        operation_id = f"{started.replace(':', '').replace('.', '')}-{action}-{uuid.uuid4().hex[:8]}"
        mutation = {
            "pointerChanged": False,
            "pointerChangeAttempted": False,
            "serviceChanged": False,
            "serviceChangeAttempted": False,
            "releaseStoreChanged": False,
            "trafficChanged": False,
            "trafficChangeAttempted": False,
            "databaseChanged": False,
            "jatoDataChanged": False,
            "stateRestored": False,
        }
        passed: list[str] = []
        failure: dict[str, Any] | None = None
        resolved_target = target
        before = self._snapshot()
        try:
            with production_lock(self.config.production_lock):
                passed.append("production_lock_acquired")
                body_target = body(mutation, passed)
                if body_target is not None:
                    resolved_target = body_target
        except (V2Error, ReleaseStoreError, AdmissionError) as exc:
            failure = {"code": getattr(exc, "code", "operation_failed"), "message": str(exc)}
            details = getattr(exc, "details", None)
            if details:
                failure["details"] = details
        except Exception as exc:
            failure = {"code": "unexpected_error", "message": type(exc).__name__}
        after = self._snapshot()
        not_reached = [check for check in ACTION_CHECKS[action] if check not in passed]
        report = {
            "schemaVersion": 1,
            "operationId": operation_id,
            "action": action,
            "decision": "completed" if failure is None else "rejected",
            "stage": (
                "completed"
                if failure is None
                else (not_reached or ["operation_failed"])[0]
            ),
            "target": _target_dict(resolved_target, manifest_sha256),
            "startedAt": started,
            "finishedAt": _utc_now(),
            "passed": passed,
            "failed": failure,
            "notReached": [] if failure is None else not_reached,
            "before": before,
            "after": after,
            "mutation": mutation,
        }
        report_path = self._write_report(report)
        report["reportPath"] = str(report_path)
        if failure is not None:
            raise V2Error(
                failure["code"],
                json.dumps(report, sort_keys=True),
                details=failure.get("details"),
            )
        return report

    def _verify_manifest(
        self,
        identity: ReleaseIdentity,
        expected_sha256: str,
    ) -> ReleaseManifest:
        release = validate_release_directory(self.config.layout, identity)
        manifest = read_manifest_file(
            release / "release-v2-manifest.json",
            expected_sha256=expected_sha256,
        )
        if manifest.identity != identity:
            raise V2Error("release_identity_mismatch", "manifest identity differs from target")
        required = (
            release / ".jato-source-seal.json",
            release / ".jato-runtime-seal.json",
            release / "06_AppPlatform/backend",
            release / "06_AppPlatform/frontend/dist",
            release / ".venv/bin/python",
            release / "03_Scripts/deploy/verify_release_source_seal.py",
            release / "03_Scripts/deploy/nginx/jato_candidate_preview_v2.conf",
            release / "03_Scripts/deploy/nginx/jato_active_release_v2.conf",
            release / "03_Scripts/deploy/systemd/jato-candidate-preview.service",
            release / "03_Scripts/deploy/systemd/jato-fullstack-backend@.service",
            release
            / "03_Scripts/deploy/systemd/"
            "jato-fullstack-backend@8001.service.d/20-candidate-readonly.conf",
        )
        for path in required:
            if path.is_symlink() or not path.exists():
                raise V2Error("release_runtime_incomplete", f"required release path is missing: {path}")
        build_metadata = release / "hermes/deploy_release.json"
        try:
            _, build_metadata_sha256 = hash_regular_file(build_metadata)
        except ReleaseStoreError as exc:
            raise V2Error(
                "release_build_metadata_invalid",
                "release build metadata is missing or unsafe",
                details={"storeCode": exc.code},
            ) from exc
        if build_metadata_sha256 != manifest.build_metadata_sha256:
            raise V2Error(
                "release_build_metadata_mismatch",
                "release build metadata differs from the manifest",
                details={
                    "expected": manifest.build_metadata_sha256,
                    "actual": build_metadata_sha256,
                },
            )
        self._verify_release_seals(
            release,
            manifest,
        )
        return manifest

    def _verify_release_seals(
        self,
        release: Path,
        manifest: ReleaseManifest,
    ) -> None:
        helper = release / "03_Scripts/deploy/verify_release_source_seal.py"
        source_seal = release / ".jato-source-seal.json"
        runtime_seal = release / ".jato-runtime-seal.json"
        identity_arguments = (
            "--commit",
            manifest.identity.commit_sha,
            "--archive-sha256",
            manifest.identity.archive_sha256,
            "--frontend-identity",
            manifest.frontend_artifact_identity,
            "--frontend-checksum",
            manifest.frontend_artifact_checksum,
        )

        def run_seal(*arguments: str, operation: str) -> None:
            result = self.runner(
                (sys.executable, "-B", str(helper), *arguments),
                300,
            )
            if result.returncode != 0:
                raise V2Error(
                    "release_seal_invalid",
                    f"release {operation} failed",
                    details={"operation": operation},
                )

        run_seal(
            "verify",
            "--root",
            str(release),
            "--manifest",
            str(source_seal),
            operation="source seal verification",
        )
        try:
            runtime_metadata = runtime_seal.lstat()
        except OSError as exc:
            raise V2Error(
                "release_runtime_incomplete",
                "runtime seal is unavailable",
            ) from exc
        if (
            runtime_seal.is_symlink()
            or not stat.S_ISREG(runtime_metadata.st_mode)
            or runtime_metadata.st_uid != self.config.expected_owner_uid
            or stat.S_IMODE(runtime_metadata.st_mode) != 0o444
        ):
            raise V2Error(
                "release_seal_invalid",
                "runtime seal ownership, type, or mode is unsafe",
            )
        run_seal(
            "verify",
            "--profile",
            "runtime",
            "--root",
            str(release),
            "--manifest",
            str(runtime_seal),
            *identity_arguments,
            operation="runtime seal verification",
        )

    def _verify_self_manifest(self, identity: ReleaseIdentity) -> ReleaseManifest:
        release = validate_release_directory(self.config.layout, identity)
        manifest_path = release / "release-v2-manifest.json"
        _, digest = hash_regular_file(manifest_path)
        return self._verify_manifest(identity, digest)

    def _read_slot_env(self, slot: str) -> str:
        path = self.config.slot_env_root / f"{slot}.env"
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise V2Error("slot_env_unreadable", f"slot env is unavailable: {slot}") from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != self.config.expected_owner_uid
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise V2Error("slot_env_unsafe", f"slot env ownership or mode is unsafe: {slot}")
        try:
            return path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise V2Error("slot_env_unreadable", f"slot env cannot be read: {slot}") from exc

    def _read_optional_managed_text(self, path: Path, *, mode: int) -> str | None:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise V2Error("managed_file_unreadable", f"managed file is unavailable: {path}") from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != self.config.expected_owner_uid
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise V2Error("managed_file_unsafe", f"managed file is unsafe: {path}")
        try:
            return path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise V2Error("managed_file_unreadable", f"managed file cannot be read: {path}") from exc

    def _restore_managed_text(self, path: Path, payload: str | None, *, mode: int) -> None:
        if payload is not None:
            _atomic_write(path, payload, mode)
            return
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise V2Error("managed_file_unreadable", f"managed file is unavailable: {path}") from exc
        if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
            raise V2Error("managed_file_unsafe", f"managed file is unsafe: {path}")
        path.unlink()
        parent_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)

    def _read_optional_link(self, path: Path) -> str | None:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise V2Error("managed_link_unreadable", f"managed link is unavailable: {path}") from exc
        if not stat.S_ISLNK(metadata.st_mode) or metadata.st_uid != self.config.expected_owner_uid:
            raise V2Error("managed_link_unsafe", f"managed link is unsafe: {path}")
        try:
            return os.readlink(path)
        except OSError as exc:
            raise V2Error("managed_link_unreadable", f"managed link cannot be read: {path}") from exc

    def _render_slot_env(self, slot: str, identity: ReleaseIdentity, *, active: bool) -> str:
        enabled = "true" if active else "false"
        execution_mode = "subprocess" if active else "external"
        lines = {
            "APP_RELEASE_ROLE": "active" if active else "candidate",
            "APP_RELEASE_SLOT": slot,
            "APP_RELEASE_SHA": identity.commit_sha,
            "APP_RELEASE_ARCHIVE_SHA256": identity.archive_sha256,
            "APP_PROJECT_ROOT": f"/opt/jato/slots/{slot}/current",
            "PYTHONPATH": f"/opt/jato/slots/{slot}/current/06_AppPlatform/backend",
            "PYTHONDONTWRITEBYTECODE": "1",
            "APP_BACKEND_WORKERS": "2",
            "APP_JATO_MONTHLY_ENABLED": enabled,
            "APP_JATO_MONTHLY_EXECUTION_MODE": execution_mode,
            "APP_RUNTIME_READ_ONLY": "false" if active else "true",
            "APP_JATO_MONTHLY_UPDATE_JOB_ROOT": str(self.config.jato_job_root),
            "JATO_PARQUET_PATH": (
                "/opt/jato/shared/04_Processed_data/jato_full_archive.parquet"
            ),
            "JATO_PARTITIONED_PATH": (
                "/opt/jato/shared/04_Processed_data/partitioned_dataset_v1"
            ),
            "APP_CRUD_DATA_PATH": (
                "/opt/jato/shared/04_Processed_data/app_entities.json"
            ),
            "APP_ENGINEERING_IMPORT_ROOT": "/opt/jato/shared/01_RAW_DATA",
            "MSRP_GOVERNANCE_EVIDENCE_ROOT": (
                "/opt/jato/shared/04_Processed_data/ops/msrp_source_evidence"
            ),
            "APP_LOCAL_WIKI_DB_PATH": (
                "/opt/jato/shared/04_Processed_data/chroma_db"
            ),
            "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED": "false",
            "APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED": "false",
            "APP_METADATA_PREWARM_ENABLED": "false",
            "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED": "false",
            "HERMES_RUN_ENABLED": "false" if not active else "true",
        }
        if active:
            lines.update(
                {
                    "APP_JATO_MONTHLY_ACTIVE_SLOT_FILE": str(
                        self.config.active_slot_file
                    ),
                    "APP_JATO_MONTHLY_DEPLOYMENT_MARKER": str(
                        self.config.deployment_marker
                    ),
                }
            )
        return "".join(f"{key}={value}\n" for key, value in lines.items())

    def _write_slot_env(self, slot: str, identity: ReleaseIdentity, *, active: bool) -> None:
        _atomic_write(
            self.config.slot_env_root / f"{slot}.env",
            self._render_slot_env(slot, identity, active=active),
            0o600,
        )

    def _verify_active_monthly_owner(self) -> None:
        path = self.config.active_slot_file
        try:
            before = path.lstat()
        except OSError as exc:
            raise V2Error(
                "active_slot_unavailable",
                "fixed Active slot ownership cannot be inspected",
            ) from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(before.st_mode)
            or before.st_uid != self.config.expected_owner_uid
            or stat.S_IMODE(before.st_mode) != 0o644
            or before.st_size <= 0
            or before.st_size > 256
        ):
            raise V2Error(
                "active_slot_unsafe",
                "fixed Active slot ownership file is unsafe",
            )
        try:
            raw = path.read_bytes()
            after = path.lstat()
        except OSError as exc:
            raise V2Error(
                "active_slot_unavailable",
                "fixed Active slot ownership cannot be read",
            ) from exc
        identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        final_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if identity != final_identity or len(raw) != before.st_size:
            raise V2Error("active_slot_changed", "fixed Active slot changed while read")
        try:
            active_slot = raw.decode("utf-8").strip()
        except UnicodeError as exc:
            raise V2Error("active_slot_unavailable", "fixed Active slot is not UTF-8") from exc
        if active_slot != ACTIVE_SLOT:
            raise V2Error(
                "active_slot_mismatch",
                "fixed Active slot ownership is not 8000",
            )
        try:
            marker = self.config.deployment_marker.lstat()
        except FileNotFoundError:
            marker = None
        except OSError as exc:
            raise V2Error(
                "deployment_marker_unavailable",
                "JATO deployment marker cannot be inspected",
            ) from exc
        if marker is None:
            return
        raise V2Error(
            "deployment_marker_retained",
            "legacy JATO deployment marker is still present",
        )

    def _set_limits(self, unit: str, *, high: int, maximum: int) -> None:
        self._systemctl(
            "set-property",
            unit,
            f"MemoryHigh={high}",
            f"MemoryMax={maximum}",
            "CPUQuota=200%",
        )

    def _verify_unit(self, unit: str, *, high: int, maximum: int) -> None:
        state = self._systemctl("show", unit, "-p", "ActiveState", "--value")
        if state != "active":
            raise V2Error("unit_not_active", f"backend unit is not active: {unit}")
        actual_high = self._systemctl("show", unit, "-p", "MemoryHigh", "--value")
        actual_max = self._systemctl("show", unit, "-p", "MemoryMax", "--value")
        if actual_high != str(high) or actual_max != str(maximum):
            raise V2Error(
                "unit_memory_mismatch",
                f"backend memory contract differs: {unit}",
                details={
                    "expected": {"MemoryHigh": str(high), "MemoryMax": str(maximum)},
                    "actual": {"MemoryHigh": actual_high, "MemoryMax": actual_max},
                },
            )

    def _verify_candidate_runtime_isolation(self) -> None:
        properties = {
            name: self._systemctl(
                "show",
                CANDIDATE_UNIT,
                "-p",
                name,
                "--value",
            )
            for name in (
                "Environment",
                "EnvironmentFiles",
                "DropInPaths",
                "ProtectSystem",
                "NoNewPrivileges",
                "PrivateTmp",
                "ReadOnlyPaths",
                "ReadWritePaths",
                "FragmentPath",
                "WorkingDirectory",
                "ExecStart",
            )
        }
        required_fragments = {
            "Environment": (
                "APP_RUNTIME_READ_ONLY=true",
                "default_transaction_read_only=on",
            ),
            "EnvironmentFiles": (str(self.config.candidate_database_env),),
            "ReadOnlyPaths": (
                "/opt/jato/shared",
                "/opt/JATO_Analysis_System-main/01_RAW_DATA",
                "/opt/JATO_Analysis_System-main/04_Processed_data",
            ),
            "ReadWritePaths": ("/var/cache/jato-candidate",),
            "FragmentPath": (str(self.config.candidate_backend_unit),),
            "WorkingDirectory": (
                "/opt/jato/slots/8001/current/06_AppPlatform/backend",
            ),
            "ExecStart": (
                "/opt/jato/slots/8001/current/.venv/bin/python",
                "--port 8001",
            ),
        }
        expected_values = {
            "EnvironmentFiles": " ".join(
                (
                    f"{self.config.backend_env} (ignore_errors=yes)",
                    f"{self.config.slot_env_root / f'{CANDIDATE_SLOT}.env'} "
                    "(ignore_errors=no)",
                    f"{self.config.candidate_database_env} (ignore_errors=no)",
                )
            ),
            "DropInPaths": str(self.config.candidate_readonly_dropin),
            "ProtectSystem": "strict",
            "NoNewPrivileges": "yes",
            "PrivateTmp": "yes",
        }
        for name, expected in expected_values.items():
            if properties[name] != expected:
                raise V2Error(
                    "candidate_runtime_isolation_mismatch",
                    f"Candidate systemd isolation differs: {name}",
                    details={"expected": expected, "actual": properties[name]},
                )
        for name, fragments in required_fragments.items():
            if any(fragment not in properties[name] for fragment in fragments):
                raise V2Error(
                    "candidate_runtime_isolation_mismatch",
                    f"Candidate systemd isolation differs: {name}",
                )

    def _verify_backend(self, slot: str, identity: ReleaseIdentity, *, active: bool) -> None:
        unit = ACTIVE_UNIT if active else CANDIDATE_UNIT
        high = ACTIVE_MEMORY_HIGH if active else CANDIDATE_MEMORY_HIGH
        maximum = ACTIVE_MEMORY_MAX if active else CANDIDATE_MEMORY_MAX
        self._verify_unit(unit, high=high, maximum=maximum)
        if not active:
            self._verify_candidate_runtime_isolation()
        env = self._read_slot_env(slot)
        expected_enabled = "true" if active else "false"
        if f"APP_RELEASE_SHA={identity.commit_sha}\n" not in env:
            raise V2Error("runtime_sha_env_mismatch", "slot env does not bind target SHA")
        if f"APP_RELEASE_ARCHIVE_SHA256={identity.archive_sha256}\n" not in env:
            raise V2Error(
                "runtime_archive_env_mismatch",
                "slot env does not bind target archive",
            )
        expected_role = "active" if active else "candidate"
        if f"APP_RELEASE_ROLE={expected_role}\n" not in env:
            raise V2Error("runtime_role_mismatch", "slot env does not bind its fixed role")
        if f"APP_JATO_MONTHLY_ENABLED={expected_enabled}\n" not in env:
            raise V2Error("monthly_gate_mismatch", "slot monthly-update role is incorrect")
        if f"APP_RUNTIME_READ_ONLY={'false' if active else 'true'}\n" not in env:
            raise V2Error(
                "runtime_read_only_mismatch",
                "slot read-only role is incorrect",
            )
        if active:
            expected_lines = (
                f"APP_JATO_MONTHLY_ACTIVE_SLOT_FILE={self.config.active_slot_file}\n",
                f"APP_JATO_MONTHLY_DEPLOYMENT_MARKER={self.config.deployment_marker}\n",
            )
            if any(line not in env for line in expected_lines):
                raise V2Error(
                    "monthly_gate_mismatch",
                    "Active monthly-update ownership inputs are incomplete",
                )
            self._verify_active_monthly_owner()
        status, payload = self.http_reader(f"http://127.0.0.1:{slot}/readyz", 20)
        release = payload.get("release") if isinstance(payload.get("release"), dict) else {}
        if status != 200 or payload.get("status") != "ready":
            raise V2Error("backend_not_ready", f"backend readiness failed on {slot}")
        if release.get("commitSha") != identity.commit_sha:
            raise V2Error(
                "runtime_sha_mismatch",
                f"backend SHA differs on {slot}",
                details={
                    "expected": identity.commit_sha,
                    "actual": release.get("commitSha"),
                },
            )

    def _verify_candidate_monthly_disabled(self) -> None:
        status, payload = self.http_reader(
            "http://127.0.0.1:8001/v1/msrp/monthly-update-jobs",
            20,
        )
        detail = payload.get("detail")
        if (
            status != 423
            or not isinstance(detail, dict)
            or detail.get("enabled") is not False
            or detail.get("reason") != "explicitly_disabled"
        ):
            raise V2Error(
                "candidate_monthly_runtime_enabled",
                "Candidate monthly-update runtime did not fail closed",
                details={
                    "expectedStatus": 423,
                    "actualStatus": status,
                    "actualReason": (
                        detail.get("reason") if isinstance(detail, dict) else None
                    ),
                },
            )

    def _verify_frontend(
        self,
        base_url: str,
        identity: ReleaseIdentity,
        manifest: ReleaseManifest,
    ) -> None:
        status, payload = self.http_reader(f"{base_url}/build-meta.json", 20)
        if status != 200:
            raise V2Error("frontend_not_ready", "frontend build metadata is unavailable")
        actual_commit = payload.get("deployCommit")
        actual_build_id = payload.get("frontendBuildId")
        if actual_commit != identity.commit_sha:
            raise V2Error(
                "frontend_sha_mismatch",
                "frontend deploy SHA differs from the target release",
                details={"expected": identity.commit_sha, "actual": actual_commit},
            )
        if actual_build_id != manifest.frontend_build_id:
            raise V2Error(
                "frontend_build_mismatch",
                "frontend build ID differs from the release manifest",
                details={
                    "expected": manifest.frontend_build_id,
                    "actual": actual_build_id,
                },
            )

    def _database_gate(
        self,
        active: ReleaseIdentity | None,
        candidate: ReleaseIdentity,
    ) -> dict[str, Any]:
        candidate_root = validate_release_directory(self.config.layout, candidate)
        active_root = (
            validate_release_directory(self.config.layout, active)
            if active is not None
            else candidate_root
        )
        result = self.database_inspector(
            DatabaseRevisionConfig(
                backend_env=self.config.backend_env,
                active_root=active_root,
                candidate_root=candidate_root,
                active_python=active_root / ".venv/bin/python",
                candidate_python=candidate_root / ".venv/bin/python",
                expected_env_uid=self.config.expected_owner_uid,
            )
        )
        if result["status"] != "compatible":
            raise V2Error(
                "migration_required",
                "database current/heads differ",
                details={"current": result.get("current"), "heads": result.get("heads")},
            )
        return result

    def _candidate_database_isolation_gate(
        self,
        identity: ReleaseIdentity,
    ) -> dict[str, Any]:
        candidate_root = validate_release_directory(self.config.layout, identity)
        try:
            return self.candidate_database_inspector(
                CandidateDatabaseIsolationConfig(
                    active_env=self.config.backend_env,
                    candidate_env=self.config.candidate_database_env,
                    candidate_root=candidate_root,
                    candidate_python=candidate_root / ".venv/bin/python",
                    expected_env_uid=self.config.expected_owner_uid,
                )
            )
        except AdmissionError as exc:
            raise V2Error(exc.code, str(exc)) from exc

    def _verify_installed_contract(
        self,
        installed: Path,
        contract: Path,
    ) -> None:
        try:
            metadata = installed.lstat()
            contract_metadata = contract.lstat()
        except OSError as exc:
            raise V2Error(
                "preview_contract_missing",
                f"fixed preview contract is unavailable: {installed}",
            ) from exc
        if (
            installed.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != self.config.expected_owner_uid
            or stat.S_IMODE(metadata.st_mode) != 0o644
            or contract.is_symlink()
            or not stat.S_ISREG(contract_metadata.st_mode)
        ):
            raise V2Error(
                "preview_contract_unsafe",
                f"fixed preview contract is unsafe: {installed}",
            )
        try:
            installed_bytes = installed.read_bytes()
            contract_bytes = contract.read_bytes()
        except OSError as exc:
            raise V2Error(
                "preview_contract_unreadable",
                f"fixed preview contract cannot be read: {installed}",
            ) from exc
        if installed_bytes != contract_bytes:
            raise V2Error(
                "preview_contract_drift",
                f"installed fixed preview contract differs: {installed}",
            )

    def _verify_preview_contracts(self) -> None:
        self._verify_installed_contract(
            self.config.preview_config,
            self.config.preview_config_contract,
        )
        self._verify_installed_contract(
            self.config.preview_unit,
            self.config.preview_unit_contract,
        )
        self._verify_installed_contract(
            self.config.candidate_backend_unit,
            self.config.candidate_backend_unit_contract,
        )
        self._verify_installed_contract(
            self.config.candidate_readonly_dropin,
            self.config.candidate_readonly_contract,
        )

    def _verify_active_routing_contract(self) -> None:
        try:
            self._verify_installed_contract(
                self.config.active_release_config,
                self.config.active_release_contract,
            )
        except V2Error as exc:
            raise V2Error(
                "active_routing_contract_invalid",
                "public Nginx is not fixed to Active 8000/current",
                details={"cause": _failure_dict(exc)},
            ) from exc
        effective = self._command("nginx", "-T", timeout=30)
        required = (
            f"include {self.config.active_release_config};",
            "server 127.0.0.1:8000",
            'default "/opt/jato/slots/8000/current/06_AppPlatform/frontend/dist"',
        )
        if any(fragment not in effective for fragment in required):
            raise V2Error(
                "active_routing_contract_invalid",
                "effective public Nginx is not fixed to Active 8000/current",
            )

    def _verify_active_compat_link(self) -> None:
        expected_pointer = self.config.layout.pointer_path(ACTIVE_SLOT, "current")
        expected_raw = os.path.relpath(expected_pointer, self.config.active_compat_link.parent)
        actual_raw = self._read_optional_link(self.config.active_compat_link)
        try:
            actual_root = self.config.active_compat_link.resolve(strict=True)
            expected_root = expected_pointer.resolve(strict=True)
        except OSError as exc:
            raise V2Error(
                "active_compat_link_invalid",
                "current Active compatibility link cannot be resolved",
            ) from exc
        if actual_raw != expected_raw or actual_root != expected_root:
            raise V2Error(
                "active_compat_link_invalid",
                "current Active compatibility link does not follow fixed 8000/current",
            )

    def _verify_active_runtime_contract(self) -> None:
        self._verify_installed_contract(
            self.config.active_backend_unit,
            self.config.candidate_backend_unit_contract,
        )
        properties = {
            name: self._systemctl("show", ACTIVE_UNIT, "-p", name, "--value")
            for name in ("EnvironmentFiles", "FragmentPath", "WorkingDirectory", "ExecStart")
        }
        expected_env = " ".join(
            (
                f"{self.config.backend_env} (ignore_errors=yes)",
                f"{self.config.slot_env_root / f'{ACTIVE_SLOT}.env'} (ignore_errors=no)",
            )
        )
        expected = {
            "EnvironmentFiles": expected_env,
            "FragmentPath": str(self.config.active_backend_unit),
            "WorkingDirectory": "/opt/jato/slots/8000/current/06_AppPlatform/backend",
        }
        for name, value in expected.items():
            if properties[name] != value:
                raise V2Error(
                    "active_runtime_contract_mismatch",
                    f"Active systemd contract differs: {name}",
                    details={"expected": value, "actual": properties[name]},
                )
        if (
            "/opt/jato/slots/8000/current/.venv/bin/python" not in properties["ExecStart"]
            or "--port 8000" not in properties["ExecStart"]
        ):
            raise V2Error(
                "active_runtime_contract_mismatch",
                "Active systemd contract differs: ExecStart",
            )

    def _ensure_preview_contracts(self) -> bool:
        pairs = (
            (self.config.preview_config, self.config.preview_config_contract),
            (self.config.preview_unit, self.config.preview_unit_contract),
            (
                self.config.candidate_backend_unit,
                self.config.candidate_backend_unit_contract,
            ),
            (
                self.config.candidate_readonly_dropin,
                self.config.candidate_readonly_contract,
            ),
        )
        missing: list[tuple[Path, Path]] = []
        for installed, contract in pairs:
            try:
                installed.lstat()
            except FileNotFoundError:
                missing.append((installed, contract))
                continue
            except OSError as exc:
                raise V2Error(
                    "preview_contract_unreadable",
                    f"fixed preview contract is unavailable: {installed}",
                ) from exc
            self._verify_installed_contract(installed, contract)
        if missing:
            for installed, contract in missing:
                try:
                    payload = contract.read_text(encoding="utf-8")
                except (OSError, UnicodeError) as exc:
                    raise V2Error(
                        "preview_contract_unreadable",
                        f"packaged fixed preview contract cannot be read: {contract}",
                    ) from exc
                _atomic_write(installed, payload, 0o644)
        # A prior attempt may have written every file and stopped before reload.
        # Always converge and prove the effective contract before Candidate start.
        self._systemctl("daemon-reload")
        self._verify_preview_contracts()
        return bool(missing)

    def _write_preview_identity(self, identity: ReleaseIdentity) -> None:
        self._verify_preview_contracts()
        metadata = json.dumps(
            {
                "schemaVersion": 2,
                "role": "candidate",
                "commitSha": identity.commit_sha,
                "archiveSha256": identity.archive_sha256,
                "candidateSlot": 8001,
                "previewPort": 18002,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        self.config.preview_runtime_root.mkdir(parents=True, exist_ok=True)
        if self.config.preview_runtime_root.is_symlink():
            raise V2Error("preview_runtime_unsafe", "preview runtime root is a symlink")
        _atomic_write(
            self.config.preview_runtime_root / "candidate-preview.json",
            metadata + "\n",
            0o644,
        )

    def _clear_preview_identity(self) -> None:
        identity_file = self.config.preview_runtime_root / "candidate-preview.json"
        try:
            metadata = identity_file.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise V2Error("preview_identity_unreadable", "preview identity is unavailable") from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise V2Error("preview_identity_unsafe", "preview identity is not a regular file")
        identity_file.unlink()

    def _verify_preview(
        self,
        identity: ReleaseIdentity,
        manifest: ReleaseManifest,
    ) -> None:
        self._verify_unit(
            PREVIEW_UNIT,
            high=PREVIEW_MEMORY_HIGH,
            maximum=PREVIEW_MEMORY_MAX,
        )
        status, payload = self.http_reader(
            "http://127.0.0.1:18002/candidate-preview.json",
            10,
        )
        if (
            status != 200
            or payload.get("commitSha") != identity.commit_sha
            or payload.get("archiveSha256") != identity.archive_sha256
            or payload.get("candidateSlot") != 8001
            or payload.get("previewPort") != 18002
        ):
            raise V2Error(
                "preview_identity_mismatch",
                "Candidate preview identity differs",
                details={
                    "expected": {
                        "commitSha": identity.commit_sha,
                        "archiveSha256": identity.archive_sha256,
                        "candidateSlot": 8001,
                        "previewPort": 18002,
                    },
                    "actual": payload,
                },
            )
        self._verify_frontend("http://127.0.0.1:18002", identity, manifest)

    def _restore_pair(
        self,
        slot: str,
        current: ReleaseIdentity | None,
        previous: ReleaseIdentity | None,
    ) -> None:
        for kind, identity in (("current", current), ("previous", previous)):
            if identity is None:
                clear_pointer(self.config.layout, slot, kind)
            else:
                atomic_symlink(self.config.layout, slot, kind, identity)

    def _restore_candidate_after_failure(
        self,
        old: PointerPair,
        old_env: str | None,
        old_preview_identity: str | None,
        old_manifest: ReleaseManifest | None,
        *,
        candidate_was_active: bool,
        preview_was_active: bool,
        trigger: Exception,
        mutation: dict[str, bool],
        passed: list[str],
    ) -> None:
        restore_errors: list[dict[str, Any]] = []

        def attempt(step: str, operation: Callable[[], None]) -> bool:
            try:
                operation()
            except Exception as exc:
                restore_errors.append({"step": step, **_failure_dict(exc)})
                return False
            return True

        mutation["serviceChangeAttempted"] = True
        attempt("stop_preview", lambda: self._stop_unit(PREVIEW_UNIT))
        attempt("stop_candidate", lambda: self._stop_unit(CANDIDATE_UNIT))
        mutation["serviceChanged"] = True
        pointer_restored = attempt(
            "restore_candidate_pointers",
            lambda: self._restore_pair(CANDIDATE_SLOT, old.current, old.previous),
        )
        if pointer_restored:
            candidate_runtime_restored = not candidate_was_active
            attempt(
                "restore_candidate_environment",
                lambda: self._restore_managed_text(
                    self.config.slot_env_root / f"{CANDIDATE_SLOT}.env",
                    old_env,
                    mode=0o600,
                ),
            )
            preview_identity_restored = attempt(
                "restore_preview_identity",
                lambda: self._restore_managed_text(
                    self.config.preview_runtime_root / "candidate-preview.json",
                    old_preview_identity,
                    mode=0o644,
                ),
            )
            if old.current is not None and candidate_was_active:
                restarted = attempt(
                    "restart_previous_candidate",
                    lambda: self._systemctl("restart", CANDIDATE_UNIT, timeout=120),
                )
                if restarted:
                    candidate_runtime_restored = attempt(
                        "verify_previous_candidate",
                        lambda: self._verify_backend(
                            CANDIDATE_SLOT,
                            old.current,
                            active=False,
                        ),
                    )
            if (
                old.current is not None
                and preview_was_active
                and preview_identity_restored
                and candidate_runtime_restored
            ):
                preview_restarted = attempt(
                    "restart_previous_preview",
                    lambda: self._systemctl("restart", PREVIEW_UNIT),
                )
                if preview_restarted:
                    assert old_manifest is not None
                    attempt(
                        "verify_previous_preview",
                        lambda: self._verify_preview(old.current, old_manifest),
                    )
        if restore_errors:
            raise V2Error(
                "candidate_restore_failed",
                "Candidate preparation failed and the previous Candidate could not be restored",
                details={
                    "trigger": _failure_dict(trigger),
                    "restoreErrors": restore_errors,
                },
            ) from trigger
        mutation["stateRestored"] = True
        passed.append("previous_candidate_restored")
        raise trigger

    def prepare_candidate(
        self,
        identity: ReleaseIdentity,
        *,
        manifest_sha256: str,
        staging_root: Path | None = None,
    ) -> dict[str, Any]:
        def body(mutation: dict[str, bool], passed: list[str]) -> ReleaseIdentity:
            active = self._active_baseline()
            passed.append("active_baseline_verified")
            self._verify_active_routing_contract()
            passed.append("fixed_active_routing_verified")
            created = False
            try:
                if staging_root is not None:
                    created = promote_staged_release(
                        self.config.layout,
                        identity,
                        staging_root,
                        expected_manifest_sha256=manifest_sha256,
                    )
                    mutation["releaseStoreChanged"] = created
                    passed.append("release_materialized" if created else "release_reused")
                manifest = self._verify_manifest(identity, manifest_sha256)
                passed.append("release_manifest_verified")
                self._candidate_database_isolation_gate(identity)
                passed.append("candidate_database_isolation_verified")
                old = read_pointer_pair(self.config.layout, CANDIDATE_SLOT)
                old_manifest: ReleaseManifest | None = None
                if old.current is not None:
                    old_manifest = self._verify_self_manifest(old.current)
                    passed.append("previous_candidate_restore_point_verified")
                old_env = self._read_optional_managed_text(
                    self.config.slot_env_root / f"{CANDIDATE_SLOT}.env",
                    mode=0o600,
                )
                old_preview_identity = self._read_optional_managed_text(
                    self.config.preview_runtime_root / "candidate-preview.json",
                    mode=0o644,
                )
                candidate_was_active = (
                    self._systemctl("show", CANDIDATE_UNIT, "-p", "ActiveState", "--value")
                    == "active"
                )
                preview_was_active = (
                    self._systemctl("show", PREVIEW_UNIT, "-p", "ActiveState", "--value")
                    == "active"
                )
                if candidate_was_active and (old.current is None or old_env is None):
                    raise V2Error(
                        "candidate_runtime_inconsistent",
                        "Candidate backend is active without a restorable pointer and environment",
                    )
                if preview_was_active and (
                    not candidate_was_active
                    or old.current is None
                    or old_preview_identity is None
                ):
                    raise V2Error(
                        "candidate_runtime_inconsistent",
                        "Preview is active without a restorable Candidate backend",
                    )
                if old.current is not None:
                    if not candidate_was_active or old_env is None:
                        raise V2Error(
                            "candidate_runtime_inconsistent",
                            "Candidate pointer is not backed by a running verified backend; discard it first",
                        )
                    self._verify_backend(CANDIDATE_SLOT, old.current, active=False)
                if preview_was_active:
                    assert old.current is not None and old_manifest is not None
                    self._verify_preview(old.current, old_manifest)
                passed.append("candidate_previous_state_verified")
                self._database_gate(active.identity, identity)
                passed.append("database_revision_compatible")
                mutation["serviceChangeAttempted"] = True
                if self._ensure_preview_contracts():
                    mutation["serviceChanged"] = True
                    passed.append("preview_contract_installed")
                passed.append("preview_contract_verified")
                try:
                    mutation["pointerChangeAttempted"] = True
                    if old.current is not None and old.current != identity:
                        atomic_symlink(
                            self.config.layout,
                            CANDIDATE_SLOT,
                            "previous",
                            old.current,
                        )
                    atomic_symlink(self.config.layout, CANDIDATE_SLOT, "current", identity)
                    mutation["pointerChanged"] = True
                    self._write_slot_env(CANDIDATE_SLOT, identity, active=False)
                    self._systemctl("restart", CANDIDATE_UNIT, timeout=120)
                    mutation["serviceChanged"] = True
                    self._verify_backend(CANDIDATE_SLOT, identity, active=False)
                    passed.append("candidate_backend_verified")
                    self._verify_candidate_monthly_disabled()
                    passed.append("candidate_monthly_disabled_verified")
                    self._write_preview_identity(identity)
                    self._systemctl("restart", PREVIEW_UNIT)
                    self._verify_preview(identity, manifest)
                    passed.append("candidate_preview_verified")
                    self._verify_active_baseline_unchanged(active)
                    passed.append("active_unchanged")
                except Exception as exc:
                    self._restore_candidate_after_failure(
                        old,
                        old_env,
                        old_preview_identity,
                        old_manifest,
                        candidate_was_active=candidate_was_active,
                        preview_was_active=preview_was_active,
                        trigger=exc,
                        mutation=mutation,
                        passed=passed,
                    )
                try:
                    removed = collect_garbage(self.config.layout)
                except ReleaseStoreError as exc:
                    passed.append(f"release_gc_deferred:{exc.code}")
                else:
                    mutation["releaseStoreChanged"] |= bool(removed)
                    passed.append(f"unreferenced_releases_removed:{len(removed)}")
                self._best_effort_archive_cache_gc(
                    mutation,
                    passed,
                    legacy_active=active.legacy,
                )
            except Exception as trigger:
                if created:
                    try:
                        removed = remove_if_unreferenced(self.config.layout, identity)
                    except ReleaseStoreError as cleanup_error:
                        raise V2Error(
                            "candidate_release_cleanup_failed",
                            "Candidate failed and its new unreferenced release could not be cleaned",
                            details={
                                "trigger": _failure_dict(trigger),
                                "cleanup": _failure_dict(cleanup_error),
                            },
                        ) from trigger
                    if removed:
                        mutation["stateRestored"] = True
                        passed.append("failed_release_removed")
                raise
            return identity

        return self._execute(
            "prepare-candidate",
            identity,
            body,
            manifest_sha256=manifest_sha256,
        )

    def discard_candidate(self) -> dict[str, Any]:
        def body(mutation: dict[str, bool], passed: list[str]) -> ReleaseIdentity | None:
            active = self._active_baseline()
            passed.append("active_baseline_verified")
            self._verify_active_routing_contract()
            passed.append("fixed_active_routing_verified")
            self._stop_unit(PREVIEW_UNIT)
            self._stop_unit(CANDIDATE_UNIT)
            mutation["serviceChanged"] = True
            self._clear_preview_identity()
            cleared = clear_pointer(self.config.layout, CANDIDATE_SLOT, "current")
            cleared |= clear_pointer(self.config.layout, CANDIDATE_SLOT, "previous")
            mutation["pointerChanged"] = cleared
            removed: tuple[ReleaseIdentity, ...] = ()
            if active.legacy:
                passed.append("gc_deferred_for_legacy_active")
            else:
                try:
                    removed = collect_garbage(self.config.layout)
                except ReleaseStoreError as exc:
                    passed.append(f"release_gc_deferred:{exc.code}")
                else:
                    passed.append(f"unreferenced_releases_removed:{len(removed)}")
            mutation["releaseStoreChanged"] = bool(removed)
            self._verify_active_baseline_unchanged(active)
            passed.extend(("candidate_stopped", "candidate_pointers_cleared"))
            passed.append("active_unchanged")
            self._best_effort_archive_cache_gc(
                mutation,
                passed,
                legacy_active=active.legacy,
            )
            return None

        return self._execute("discard-candidate", None, body)

    def _verify_public(
        self,
        identity: ReleaseIdentity,
        manifest: ReleaseManifest,
    ) -> None:
        status, payload = self.http_reader(f"{self.config.public_origin}/healthz", 20)
        if status != 200 or payload.get("status") != "ok":
            raise V2Error("public_not_ready", "public health check failed")
        self._verify_frontend(self.config.public_origin, identity, manifest)

    def _restart_active(
        self,
        identity: ReleaseIdentity,
        manifest: ReleaseManifest,
        *,
        write_env: bool = True,
    ) -> None:
        if write_env:
            self._write_slot_env(ACTIVE_SLOT, identity, active=True)
        self._set_limits(ACTIVE_UNIT, high=ACTIVE_MEMORY_HIGH, maximum=ACTIVE_MEMORY_MAX)
        self._systemctl("restart", ACTIVE_UNIT, timeout=120)
        self._verify_backend(ACTIVE_SLOT, identity, active=True)
        self._verify_active_runtime_contract()
        self._verify_active_compat_link()
        self._verify_public(identity, manifest)

    def _adopt_legacy_active(
        self, baseline: ActiveBaseline, target: ReleaseIdentity,
        target_manifest: ReleaseManifest, mutation: dict[str, bool], passed: list[str],
    ) -> None:
        if baseline.previous_anchor is not None:
            raise V2Error("legacy_active_previous_present", "legacy previous must be empty")
        legacy_env = self._read_slot_env(ACTIVE_SLOT)
        active_unit = self.config.active_backend_unit
        active_unit_before = self._read_optional_managed_text(active_unit, mode=0o644)
        fragment = Path(self._systemctl("show", ACTIVE_UNIT, "-p", "FragmentPath", "--value"))
        legacy_template = active_unit.with_name("jato-fullstack-backend@.service")
        if (
            fragment not in (active_unit, legacy_template)
            or (fragment == active_unit) != (active_unit_before is not None)
            or self._read_optional_managed_text(fragment, mode=0o644) is None
        ):
            raise V2Error("legacy_active_unit_mismatch", "legacy unit is not safely replaceable")
        self._verify_unit(ACTIVE_UNIT, high=ACTIVE_MEMORY_HIGH, maximum=ACTIVE_MEMORY_MAX)
        health_url = f"{self.config.public_origin}/healthz"
        metadata_url = f"{self.config.public_origin}/build-meta.json"
        legacy_health = self.http_reader(health_url, 20)
        legacy_identity = self.http_reader(metadata_url, 20)
        identity_fields = (legacy_identity[1].get("deployCommit"), legacy_identity[1].get("frontendBuildId"))
        if (
            legacy_health[0] != 200 or legacy_health[1].get("status") != "ok"
            or legacy_identity[0] != 200 or not all(identity_fields)
        ):
            raise V2Error("legacy_public_not_ready", "legacy public identity is unavailable")
        compat_before = self._read_optional_link(self.config.active_compat_link)
        current = self.config.layout.pointer_path(ACTIVE_SLOT, "current")
        compat_target = os.path.relpath(current, self.config.active_compat_link.parent)
        if compat_before not in (None, compat_target):
            raise V2Error("active_compat_link_invalid", "legacy compatibility link differs")
        target_unit = self.config.candidate_backend_unit_contract.read_text(encoding="utf-8")
        self._verify_active_baseline_unchanged(baseline)
        passed.extend(("active_restore_point_verified", "active_baseline_verified"))
        try:
            mutation["serviceChangeAttempted"] = True
            if compat_before is None:
                self.config.active_compat_link.symlink_to(compat_target)
            _atomic_write(active_unit, target_unit, 0o644)
            self._systemctl("daemon-reload")
            self._write_slot_env(ACTIVE_SLOT, target, active=True)
            mutation["pointerChangeAttempted"] = mutation["trafficChangeAttempted"] = True
            atomic_symlink(self.config.layout, ACTIVE_SLOT, "previous", target)
            atomic_symlink(self.config.layout, ACTIVE_SLOT, "current", target)
            mutation["pointerChanged"] = mutation["trafficChanged"] = True
            self.jato_inspector(self.config.jato_job_root)
            passed.append("jato_idle_at_restart")
            self._restart_active(target, target_manifest, write_env=False)
            mutation["serviceChanged"] = True
        except Exception as trigger:
            try:
                temporary = current.parent / f".current.{uuid.uuid4().hex}.legacy"
                try:
                    os.symlink(baseline.current_anchor[4], temporary)
                    os.replace(temporary, current)
                finally:
                    temporary.unlink(missing_ok=True)
                clear_pointer(self.config.layout, ACTIVE_SLOT, "previous")
                _atomic_write(self.config.slot_env_root / f"{ACTIVE_SLOT}.env", legacy_env, 0o600)
                self._restore_managed_text(active_unit, active_unit_before, mode=0o644)
                if compat_before is None:
                    actual_compat = self._read_optional_link(self.config.active_compat_link)
                    if actual_compat not in (None, compat_target):
                        raise V2Error(
                            "active_compat_link_invalid",
                            "new Active compatibility link changed before restore",
                        )
                    if actual_compat is not None:
                        self.config.active_compat_link.unlink()
                self._systemctl("daemon-reload")
                self._set_limits(ACTIVE_UNIT, high=ACTIVE_MEMORY_HIGH, maximum=ACTIVE_MEMORY_MAX)
                self._systemctl("restart", ACTIVE_UNIT, timeout=120)
                mutation["serviceChanged"] = True
                self._verify_unit(ACTIVE_UNIT, high=ACTIVE_MEMORY_HIGH, maximum=ACTIVE_MEMORY_MAX)
                restored_health = self.http_reader(health_url, 20)
                if (
                    Path(self._systemctl("show", ACTIVE_UNIT, "-p", "FragmentPath", "--value"))
                    != fragment
                    or restored_health[0] != 200
                    or restored_health[1].get("status") != "ok"
                    or self.http_reader(metadata_url, 20) != legacy_identity
                ):
                    raise V2Error("legacy_active_restore_mismatch", "legacy Active changed")
            except Exception as restore_error:
                raise V2Error(
                    "active_restore_failed",
                    "first Active adoption failed and legacy Active was not restored",
                    details={"trigger": _failure_dict(trigger), "restore": _failure_dict(restore_error)},
                ) from trigger
            mutation["stateRestored"] = True
            passed.append("legacy_active_restored")
            raise trigger
        passed.extend(("legacy_active_adopted_as_b_b", "active_updated_and_public_verified"))
    def _switch_active(
        self,
        active: PointerPair,
        target: ReleaseIdentity,
        current_manifest: ReleaseManifest,
        target_manifest: ReleaseManifest,
        mutation: dict[str, bool],
        passed: list[str],
        *,
        success_label: str,
        restore_label: str,
        rollback: bool,
    ) -> None:
        if active.current is None:
            raise V2Error("v2_bootstrap_required", "active.current is not initialized")
        previous_env = self._read_slot_env(ACTIVE_SLOT)
        already_target = active.current == target
        if already_target:
            try:
                self._verify_backend(ACTIVE_SLOT, target, active=True)
                self._verify_active_runtime_contract()
                self._verify_active_compat_link()
                self._verify_public(target, target_manifest)
            except (V2Error, ReleaseStoreError, AdmissionError):
                passed.append("active_target_reconcile_required")
            else:
                passed.extend(("active_target_already_current", success_label))
                return

        fallback = active.current
        fallback_manifest = current_manifest
        restore_pair = active
        if already_target:
            fallback = active.previous
            fallback_manifest = (
                self._verify_self_manifest(fallback)
                if fallback is not None and fallback != target
                else None
            )
            restore_pair = PointerPair(fallback, target)
        try:
            if active.current != target:
                mutation["pointerChangeAttempted"] = mutation["trafficChangeAttempted"] = True
                if rollback:
                    atomic_exchange_pointers(self.config.layout, ACTIVE_SLOT, active)
                else:
                    atomic_symlink(
                        self.config.layout,
                        ACTIVE_SLOT,
                        "previous",
                        active.current,
                    )
                    atomic_symlink(self.config.layout, ACTIVE_SLOT, "current", target)
                mutation["pointerChanged"] = mutation["trafficChanged"] = True
            self.jato_inspector(self.config.jato_job_root)
            passed.append("jato_idle_at_restart")
            mutation["serviceChangeAttempted"] = True
            mutation["trafficChangeAttempted"] = True
            self._restart_active(target, target_manifest)
            mutation["serviceChanged"] = mutation["trafficChanged"] = True
            passed.append(success_label)
        except Exception as trigger:
            if already_target and not mutation["serviceChangeAttempted"]:
                raise
            restore_errors: list[dict[str, Any]] = []
            def attempt(step: str, operation: Callable[[], None]) -> bool:
                try:
                    operation()
                except Exception as exc:
                    restore_errors.append({"step": step, **_failure_dict(exc)})
                    return False
                return True
            if fallback is None or fallback_manifest is None or fallback == target:
                restore_errors.append(
                    {
                        "step": "restore_active_target",
                        "code": "active_previous_unavailable",
                        "message": "no distinct previous Active is available",
                    }
                )
            else:
                def restore_pointers() -> None:
                    if not (rollback or already_target):
                        self._restore_pair(
                            ACTIVE_SLOT,
                            restore_pair.current,
                            restore_pair.previous,
                        )
                        return
                    actual = read_pointer_pair(self.config.layout, ACTIVE_SLOT)
                    if actual == restore_pair:
                        return
                    if actual != PointerPair(target, fallback):
                        raise V2Error(
                            "active_pointer_pair_unexpected",
                            "Active pointers are neither switched nor restored",
                        )
                    mutation["pointerChangeAttempted"] = mutation["trafficChangeAttempted"] = True
                    atomic_exchange_pointers(self.config.layout, ACTIVE_SLOT, actual)
                    mutation["pointerChanged"] = mutation["trafficChanged"] = True

                pointers_restored = attempt("restore_active_pointers", restore_pointers)
                env_restored = True
                if mutation["serviceChangeAttempted"] and not already_target:
                    env_restored = attempt(
                        "restore_active_environment",
                        lambda: _atomic_write(
                            self.config.slot_env_root / f"{ACTIVE_SLOT}.env",
                            previous_env,
                            0o600,
                        ),
                    )
                if pointers_restored and env_restored and mutation["serviceChangeAttempted"]:
                    mutation["serviceChanged"] = mutation["trafficChanged"] = True
                    attempt(
                        "restart_previous_active",
                        lambda: self._restart_active(fallback, fallback_manifest, write_env=already_target),
                    )
            if restore_errors:
                raise V2Error(
                    "active_restore_failed",
                    "Active operation failed and the previous Active could not be restored",
                    details={
                        "trigger": _failure_dict(trigger),
                        "restoreErrors": restore_errors,
                    },
                ) from trigger
            mutation["stateRestored"] = True
            passed.append(restore_label)
            raise trigger

    def update_active(
        self,
        expected: ReleaseIdentity,
        *,
        manifest_sha256: str,
    ) -> dict[str, Any]:
        def body(mutation: dict[str, bool], passed: list[str]) -> ReleaseIdentity:
            baseline = self._active_baseline()
            candidate = read_pointer(self.config.layout, CANDIDATE_SLOT, "current")
            if candidate != expected:
                raise V2Error(
                    "candidate_identity_mismatch",
                    "reviewed Candidate changed",
                    details={
                        "expected": _identity_dict(expected),
                        "actual": _identity_dict(candidate),
                    },
                )
            candidate_manifest = self._verify_manifest(candidate, manifest_sha256)
            passed.append("release_manifest_revalidated")
            self._verify_active_routing_contract()
            passed.append("fixed_active_routing_verified")
            self._verify_active_monthly_owner()
            passed.append("fixed_active_owner_verified")
            self._verify_preview_contracts()
            active: PointerPair | None = None
            active_manifest: ReleaseManifest | None = None
            if not baseline.legacy:
                active = read_pointer_pair(self.config.layout, ACTIVE_SLOT)
                if active.current is None:
                    raise V2Error(
                        "active_baseline_missing",
                        "active.current is not initialized",
                    )
                active_manifest = self._verify_self_manifest(active.current)
                passed.append("active_restore_point_verified")
                self._verify_active_compat_link()
                self._verify_active_runtime_contract()
                if active.current != candidate:
                    self._verify_backend(ACTIVE_SLOT, active.current, active=True)
                    self._verify_public(active.current, active_manifest)
                passed.append("active_baseline_verified")
            self._candidate_database_isolation_gate(candidate)
            passed.append("candidate_database_isolation_revalidated")
            self._verify_backend(CANDIDATE_SLOT, candidate, active=False)
            self._verify_candidate_monthly_disabled()
            passed.append("candidate_monthly_disabled_revalidated")
            self._verify_preview(candidate, candidate_manifest)
            passed.append("candidate_revalidated")
            active_bundle_lock = self._active_bundle_lock(baseline)
            with self.jato_lock_holder(self.config.jato_job_root, active_bundle_lock):
                passed.append("jato_release_locks_acquired")
                self.jato_inspector(self.config.jato_job_root)
                passed.append("jato_idle_before_update")
                self._database_gate(baseline.identity, candidate)
                passed.append("database_revision_compatible")
                if baseline.legacy:
                    self._adopt_legacy_active(
                        baseline,
                        candidate,
                        candidate_manifest,
                        mutation,
                        passed,
                    )
                else:
                    assert active is not None and active_manifest is not None
                    self._switch_active(
                        active,
                        candidate,
                        active_manifest,
                        candidate_manifest,
                        mutation,
                        passed,
                        success_label="active_updated_and_public_verified",
                        restore_label="previous_active_restored",
                        rollback=False,
                    )
            self._best_effort_archive_cache_gc(mutation, passed)
            return candidate

        return self._execute(
            "update-active",
            expected,
            body,
            manifest_sha256=manifest_sha256,
        )

    def rollback_active(
        self,
        expected: ReleaseIdentity,
        *,
        manifest_sha256: str,
    ) -> dict[str, Any]:
        target: ReleaseIdentity | None = expected

        def body(mutation: dict[str, bool], passed: list[str]) -> None:
            nonlocal target
            active = read_pointer_pair(self.config.layout, ACTIVE_SLOT)
            if (
                active.current is None
                or active.previous is None
                or active.current == active.previous
            ):
                raise V2Error("rollback_unavailable", "active.previous is not available")
            if active.current != expected and active.previous != expected:
                raise V2Error(
                    "rollback_target_mismatch",
                    "reviewed rollback target is no longer current or previous",
                    details={
                        "expected": _identity_dict(expected),
                        "current": _identity_dict(active.current),
                        "previous": _identity_dict(active.previous),
                    },
                )
            target = expected
            self._verify_active_routing_contract()
            passed.append("fixed_active_routing_verified")
            self._verify_active_monthly_owner()
            passed.append("fixed_active_owner_verified")
            self._verify_active_compat_link()
            active_manifest = self._verify_self_manifest(active.current)
            target_manifest = self._verify_manifest(expected, manifest_sha256)
            self._verify_active_runtime_contract()
            passed.append("active_baseline_verified")
            baseline = self._active_baseline()
            if baseline.legacy:
                raise V2Error("rollback_unavailable", "legacy Active has no V2 rollback")
            active_bundle_lock = self._active_bundle_lock(baseline)
            with self.jato_lock_holder(self.config.jato_job_root, active_bundle_lock):
                passed.append("jato_release_locks_acquired")
                self._database_gate(active.current, expected)
                passed.append("database_revision_compatible")
                self.jato_inspector(self.config.jato_job_root)
                passed.append("jato_idle_before_rollback")
                self._switch_active(
                    active,
                    expected,
                    active_manifest,
                    target_manifest,
                    mutation,
                    passed,
                    success_label="active_rollback_verified",
                    restore_label="failed_rollback_reverted",
                    rollback=True,
                )
            self._best_effort_archive_cache_gc(mutation, passed)
            return target

        return self._execute(
            "rollback-active",
            target,
            body,
            manifest_sha256=manifest_sha256,
        )


def _identity(commit: str, archive_sha256: str) -> ReleaseIdentity:
    if not SHA40.fullmatch(commit) or not SHA256.fullmatch(archive_sha256):
        raise V2Error("identity_invalid", "commit/archive identity is malformed")
    return ReleaseIdentity(commit, archive_sha256)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fixed Active/Candidate release V2")
    parser.add_argument("--release-root", type=Path, default=Path("/opt/jato/releases"))
    parser.add_argument("--slots-root", type=Path, default=Path("/opt/jato/slots"))
    parser.add_argument("--reports-root", type=Path, default=Path("/opt/jato/operation-reports"))
    archive_cache_default = os.environ.get("V2_ARCHIVE_CACHE_ROOT")
    parser.add_argument(
        "--archive-cache-root",
        type=Path,
        default=(Path(archive_cache_default) if archive_cache_default else None),
    )
    parser.add_argument(
        "--production-lock",
        type=Path,
        default=Path(
            os.environ.get(
                "PRODUCTION_LOCK_PATH",
                str(
                    Path.home()
                    / ".local/state/jato-production-release/production-deploy.lock"
                ),
            )
        ),
    )
    commands = parser.add_subparsers(dest="action", required=True)
    prepare = commands.add_parser("prepare-candidate")
    prepare.add_argument("--commit", required=True)
    prepare.add_argument("--archive-sha256", required=True)
    prepare.add_argument("--manifest-sha256", required=True)
    prepare.add_argument("--staging-root", type=Path)
    commands.add_parser("discard-candidate")
    update = commands.add_parser("update-active")
    update.add_argument("--commit", required=True)
    update.add_argument("--archive-sha256", required=True)
    update.add_argument("--manifest-sha256", required=True)
    rollback = commands.add_parser("rollback-active")
    rollback.add_argument("--commit", required=True)
    rollback.add_argument("--archive-sha256", required=True)
    rollback.add_argument("--manifest-sha256", required=True)
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = _parser().parse_args(arguments)
    layout = ReleaseLayout(parsed.release_root, parsed.slots_root)
    target_identity: ReleaseIdentity | None = None
    config_arguments: dict[str, Any] = {}
    if parsed.action == "prepare-candidate":
        target_identity = _identity(parsed.commit, parsed.archive_sha256)
        target_root = layout.release_path(target_identity)
        config_arguments.update(
            {
                "preview_config_contract": (
                    target_root
                    / "03_Scripts/deploy/nginx/jato_candidate_preview_v2.conf"
                ),
                "preview_unit_contract": (
                    target_root
                    / "03_Scripts/deploy/systemd/jato-candidate-preview.service"
                ),
                "candidate_backend_unit_contract": (
                    target_root
                    / "03_Scripts/deploy/systemd/jato-fullstack-backend@.service"
                ),
                "candidate_readonly_contract": (
                    target_root
                    / "03_Scripts/deploy/systemd/"
                    "jato-fullstack-backend@8001.service.d/"
                    "20-candidate-readonly.conf"
                ),
            }
        )
    config = ControllerConfig(
        layout=layout,
        reports_root=parsed.reports_root,
        archive_cache_root=parsed.archive_cache_root,
        production_lock=parsed.production_lock,
        **config_arguments,
    )
    controller = FixedReleaseController(config)
    try:
        if parsed.action == "prepare-candidate":
            assert target_identity is not None
            report = controller.prepare_candidate(
                target_identity,
                manifest_sha256=parsed.manifest_sha256,
                staging_root=parsed.staging_root,
            )
        elif parsed.action == "discard-candidate":
            report = controller.discard_candidate()
        elif parsed.action == "update-active":
            report = controller.update_active(
                _identity(parsed.commit, parsed.archive_sha256),
                manifest_sha256=parsed.manifest_sha256,
            )
        else:
            report = controller.rollback_active(
                _identity(parsed.commit, parsed.archive_sha256),
                manifest_sha256=parsed.manifest_sha256,
            )
    except V2Error as exc:
        try:
            failed_report = json.loads(str(exc))
        except json.JSONDecodeError:
            failed_report = None
        if isinstance(failed_report, dict) and failed_report.get("reportPath"):
            print(
                f"V2_OPERATION_REPORT_PATH={failed_report['reportPath']}",
                file=sys.stderr,
            )
        print(str(exc), file=sys.stderr)
        if exc.code == "active_restore_failed":
            return EXIT_ACTIVE_RESTORE_UNPROVEN
        return 1
    print(f"V2_OPERATION_REPORT_PATH={report['reportPath']}", file=sys.stderr)
    print(json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
