from __future__ import annotations

import importlib.util
import json
import os
from contextlib import contextmanager
from pathlib import Path
import stat
import sys
from typing import Iterator

import pytest


ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "03_Scripts/deploy"
sys.path.insert(0, str(DEPLOY))
HELPER = DEPLOY / "fixed_release_v2.py"
SPEC = importlib.util.spec_from_file_location("fixed_release_v2", HELPER)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
STORE = sys.modules["release_v2_store"]
ADMISSION = sys.modules["release_v2_admission"]


ACTIVE = STORE.ReleaseIdentity("a" * 40, "1" * 64)
CANDIDATE = STORE.ReleaseIdentity("b" * 40, "2" * 64)
OLDER = STORE.ReleaseIdentity("c" * 40, "3" * 64)
NEWEST = STORE.ReleaseIdentity("f" * 40, "7" * 64)
LEGACY_COMMIT = "d" * 40
LEGACY_BUILD = "e" * 64
LEGACY_UNIT = "[Unit]\nDescription=Legacy Active\n"
LEGACY_ENV = "LEGACY_ACTIVE=true\n"
LEGACY_WORKDIR = "/opt/JATO_Analysis_System-main/06_AppPlatform/backend"
LEGACY_EXEC_START = "/opt/JATO_Analysis_System-main/.venv/bin/python -m uvicorn app.main:app"


class FakeSystem:
    def __init__(self) -> None:
        self.units = {
            MODULE.ACTIVE_UNIT: {
                "LoadState": "loaded",
                "ActiveState": "active",
                "SubState": "running",
                "MemoryHigh": str(MODULE.ACTIVE_MEMORY_HIGH),
                "MemoryMax": str(MODULE.ACTIVE_MEMORY_MAX),
                "EnvironmentFiles": "",
                "FragmentPath": "",
                "WorkingDirectory": "/opt/jato/slots/8000/current/06_AppPlatform/backend",
                "ExecStart": (
                    "/opt/jato/slots/8000/current/.venv/bin/python -m uvicorn "
                    "app.main:app --host 127.0.0.1 --port 8000 --workers "
                    "${APP_BACKEND_WORKERS}"
                ),
            },
            MODULE.CANDIDATE_UNIT: {
                "LoadState": "loaded",
                "ActiveState": "inactive",
                "SubState": "dead",
                "MemoryHigh": str(MODULE.CANDIDATE_MEMORY_HIGH),
                "MemoryMax": str(MODULE.CANDIDATE_MEMORY_MAX),
                "Environment": (
                    "APP_RUNTIME_READ_ONLY=true "
                    '"PGOPTIONS=-c default_transaction_read_only=on '
                    '-c statement_timeout=120000 -c lock_timeout=5000"'
                ),
                "EnvironmentFiles": "",
                "DropInPaths": "",
                "ProtectSystem": "strict",
                "NoNewPrivileges": "yes",
                "PrivateTmp": "yes",
                "ReadOnlyPaths": (
                    "/opt/jato/shared "
                    "/opt/JATO_Analysis_System-main/01_RAW_DATA "
                    "/opt/JATO_Analysis_System-main/04_Processed_data"
                ),
                "ReadWritePaths": "/var/cache/jato-candidate",
                "FragmentPath": (
                    "/etc/systemd/system/jato-fullstack-backend@8001.service"
                ),
                "WorkingDirectory": (
                    "/opt/jato/slots/8001/current/06_AppPlatform/backend"
                ),
                "ExecStart": (
                    "/opt/jato/slots/8001/current/.venv/bin/python -m uvicorn "
                    "app.main:app --host 127.0.0.1 --port 8001 --workers "
                    "${APP_BACKEND_WORKERS}"
                ),
            },
            MODULE.PREVIEW_UNIT: {
                "LoadState": "loaded",
                "ActiveState": "inactive",
                "SubState": "dead",
                "MemoryHigh": str(MODULE.PREVIEW_MEMORY_HIGH),
                "MemoryMax": str(MODULE.PREVIEW_MEMORY_MAX),
            },
        }
        self.commands: list[tuple[str, ...]] = []
        self.restart_attempts: dict[str, int] = {}
        self.fail_restart_attempts: dict[str, set[int]] = {}
        self.nginx_active_config = "/etc/jato-fullstack/nginx/active-release.conf"
        self.active_instance_unit: Path | None = None
        self.active_template_unit: Path | None = None
        self.active_v2_environment_files = ""
        self.active_legacy_environment_files = ""

    def reload_active_unit(self) -> None:
        assert self.active_instance_unit is not None
        assert self.active_template_unit is not None
        active = self.units[MODULE.ACTIVE_UNIT]
        if self.active_instance_unit.exists():
            active.update(
                {
                    "FragmentPath": str(self.active_instance_unit),
                    "WorkingDirectory": "/opt/jato/slots/8000/current/06_AppPlatform/backend",
                    "EnvironmentFiles": self.active_v2_environment_files,
                    "ExecStart": (
                        "/opt/jato/slots/8000/current/.venv/bin/python -m uvicorn "
                        "app.main:app --host 127.0.0.1 --port 8000 --workers "
                        "${APP_BACKEND_WORKERS}"
                    ),
                }
            )
            return
        active.update(
            {
                "FragmentPath": str(self.active_template_unit),
                "WorkingDirectory": LEGACY_WORKDIR,
                "ExecStart": LEGACY_EXEC_START,
                "EnvironmentFiles": self.active_legacy_environment_files,
            }
        )

    def __call__(self, arguments: tuple[str, ...], timeout: int) -> MODULE.CommandResult:
        self.commands.append(arguments)
        if arguments[:2] == ("systemctl", "show"):
            unit = arguments[2]
            key = arguments[4]
            return MODULE.CommandResult(0, self.units[unit][key] + "\n", "")
        if arguments[:2] == ("systemctl", "set-property"):
            unit = arguments[2]
            for assignment in arguments[3:]:
                key, value = assignment.split("=", 1)
                if key in {"MemoryHigh", "MemoryMax"}:
                    self.units[unit][key] = value
            return MODULE.CommandResult(0, "", "")
        if arguments[:2] == ("systemctl", "restart"):
            unit = arguments[2]
            attempt = self.restart_attempts.get(unit, 0) + 1
            self.restart_attempts[unit] = attempt
            if attempt in self.fail_restart_attempts.get(unit, set()):
                return MODULE.CommandResult(1, "", "injected restart failure")
            self.units[unit]["ActiveState"] = "active"
            self.units[unit]["SubState"] = "running"
            if unit == MODULE.PREVIEW_UNIT:
                self.units[unit]["MemoryHigh"] = str(MODULE.PREVIEW_MEMORY_HIGH)
                self.units[unit]["MemoryMax"] = str(MODULE.PREVIEW_MEMORY_MAX)
            return MODULE.CommandResult(0, "", "")
        if arguments[:2] == ("systemctl", "stop"):
            unit = arguments[2]
            self.units[unit]["ActiveState"] = "inactive"
            self.units[unit]["SubState"] = "dead"
            return MODULE.CommandResult(0, "", "")
        if arguments[:2] == ("systemctl", "daemon-reload"):
            self.reload_active_unit()
            return MODULE.CommandResult(0, "", "")
        if arguments == ("nginx", "-T"):
            return MODULE.CommandResult(
                0,
                f"include {self.nginx_active_config};\n"
                "server 127.0.0.1:8000 max_fails=3 fail_timeout=30s;\n"
                'default "/opt/jato/slots/8000/current/'
                '06_AppPlatform/frontend/dist";\n',
                "",
            )
        if (
            len(arguments) >= 4
            and arguments[0] == sys.executable
            and arguments[1] == "-B"
            and Path(arguments[2]).name == "verify_release_source_seal.py"
        ):
            if arguments[3] == "build":
                output = Path(arguments[arguments.index("--output") + 1])
                output.write_text("{}\n", encoding="utf-8")
            return MODULE.CommandResult(0, "", "")
        return MODULE.CommandResult(1, "", "unexpected command")


def create_release(
    layout: STORE.ReleaseLayout,
    identity: STORE.ReleaseIdentity,
    *,
    runtime_seal: bool = True,
) -> str:
    release = layout.release_path(identity)
    (release / "06_AppPlatform/backend").mkdir(parents=True)
    (release / "06_AppPlatform/frontend/dist").mkdir(parents=True)
    (release / ".jato-source-seal.json").write_text("{}\n", encoding="utf-8")
    (release / ".jato-source-seal.json").chmod(0o444)
    if runtime_seal:
        (release / ".jato-runtime-seal.json").write_text("{}\n", encoding="utf-8")
        (release / ".jato-runtime-seal.json").chmod(0o444)
    python = release / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.write_text("python", encoding="utf-8")
    python.chmod(0o700)
    durable_processed = layout.release_root.parent / "durable-processed"
    durable_processed.mkdir(exist_ok=True)
    (release / "04_Processed_data").symlink_to(durable_processed)
    template = release / "03_Scripts/deploy/nginx/jato_candidate_preview_v2.conf"
    template.parent.mkdir(parents=True)
    template.write_bytes(
        (DEPLOY / "nginx/jato_candidate_preview_v2.conf").read_bytes()
    )
    seal_helper = release / "03_Scripts/deploy/verify_release_source_seal.py"
    seal_helper.write_bytes(
        (DEPLOY / "verify_release_source_seal.py").read_bytes()
    )
    active_contract = release / "03_Scripts/deploy/nginx/jato_active_release_v2.conf"
    active_contract.write_bytes(
        (DEPLOY / "nginx/jato_active_release_v2.conf").read_bytes()
    )
    unit = release / "03_Scripts/deploy/systemd/jato-candidate-preview.service"
    unit.parent.mkdir(parents=True)
    unit.write_bytes((DEPLOY / "systemd/jato-candidate-preview.service").read_bytes())
    candidate_backend_unit = (
        release / "03_Scripts/deploy/systemd/jato-fullstack-backend@.service"
    )
    candidate_backend_unit.write_bytes(
        (DEPLOY / "systemd/jato-fullstack-backend@.service").read_bytes()
    )
    readonly_dropin = (
        release
        / "03_Scripts/deploy/systemd/jato-fullstack-backend@8001.service.d/"
        "20-candidate-readonly.conf"
    )
    readonly_dropin.parent.mkdir(parents=True)
    readonly_dropin.write_bytes(
        (
            DEPLOY
            / "systemd/jato-fullstack-backend@8001.service.d/"
            "20-candidate-readonly.conf"
        ).read_bytes()
    )
    build_metadata = release / "hermes/deploy_release.json"
    build_metadata.parent.mkdir(parents=True)
    build_metadata.write_text("{}\n", encoding="utf-8")
    _, build_metadata_sha256 = STORE.hash_regular_file(build_metadata)
    manifest = STORE.ReleaseManifest(
        repository="tristan419/JATO_Analysis_System",
        identity=identity,
        archive_bytes=1024,
        frontend_artifact_identity="frontend-identity",
        frontend_artifact_checksum="4" * 64,
        frontend_build_id="6" * 64,
        build_metadata_sha256=build_metadata_sha256,
    )
    payload = STORE.canonical_manifest_bytes(manifest)
    (release / "release-v2-manifest.json").write_bytes(payload)
    return STORE.manifest_sha256(payload)


def config(tmp_path: Path) -> MODULE.ControllerConfig:
    release_root = tmp_path / "releases"
    slots_root = tmp_path / "slots"
    release_root.mkdir()
    for slot in (MODULE.ACTIVE_SLOT, MODULE.CANDIDATE_SLOT):
        (slots_root / slot).mkdir(parents=True)
    legacy_active_root = tmp_path / "legacy-active"
    legacy_active_root.mkdir()
    durable_processed_root = tmp_path / "durable-processed"
    durable_processed_root.mkdir()
    (legacy_active_root / "04_Processed_data").symlink_to(
        durable_processed_root
    )
    backend_env = tmp_path / "etc/backend.env"
    backend_env.parent.mkdir(parents=True)
    backend_env.write_text(
        "APP_DATABASE_ENABLED=true\n"
        "APP_DATABASE_URL=postgresql+asyncpg://jato_active:secret@db.example/jato\n",
        encoding="utf-8",
    )
    backend_env.chmod(0o600)
    candidate_database_env = tmp_path / "etc/candidate-database.env"
    candidate_database_env.write_text(
        "APP_DATABASE_ENABLED=true\n"
        "APP_DATABASE_URL=postgresql+asyncpg://"
        "jato_candidate_readonly:other@db.example/jato\n",
        encoding="utf-8",
    )
    candidate_database_env.chmod(0o600)
    job_root = tmp_path / "jobs"
    job_root.mkdir()
    active_slot_file = tmp_path / "var/lib/jato-release/active-slot"
    active_slot_file.parent.mkdir(parents=True)
    active_slot_file.write_text("8000\n", encoding="utf-8")
    preview_config = tmp_path / "etc/candidate-preview-v2.conf"
    preview_config.parent.mkdir(parents=True, exist_ok=True)
    preview_config.write_bytes(
        (DEPLOY / "nginx/jato_candidate_preview_v2.conf").read_bytes()
    )
    preview_config.chmod(0o644)
    preview_unit = tmp_path / "etc/jato-candidate-preview.service"
    preview_unit.write_bytes(
        (DEPLOY / "systemd/jato-candidate-preview.service").read_bytes()
    )
    preview_unit.chmod(0o644)
    candidate_backend_unit = (
        tmp_path / "etc/systemd/system/jato-fullstack-backend@8001.service"
    )
    candidate_backend_unit.parent.mkdir(parents=True, exist_ok=True)
    candidate_backend_unit.write_bytes(
        (DEPLOY / "systemd/jato-fullstack-backend@.service").read_bytes()
    )
    candidate_backend_unit.chmod(0o644)
    active_backend_unit = (
        tmp_path / "etc/systemd/system/jato-fullstack-backend@8000.service"
    )
    active_backend_unit.write_bytes(
        (DEPLOY / "systemd/jato-fullstack-backend@.service").read_bytes()
    )
    active_backend_unit.chmod(0o644)
    candidate_readonly_dropin = (
        tmp_path
        / "etc/systemd/system/jato-fullstack-backend@8001.service.d/"
        "20-candidate-readonly.conf"
    )
    candidate_readonly_dropin.parent.mkdir(parents=True)
    candidate_readonly_dropin.write_bytes(
        (
            DEPLOY
            / "systemd/jato-fullstack-backend@8001.service.d/"
            "20-candidate-readonly.conf"
        ).read_bytes()
    )
    candidate_readonly_dropin.chmod(0o644)
    active_release_config = tmp_path / "etc/active-release.conf"
    active_release_config.write_bytes(
        (DEPLOY / "nginx/jato_active_release_v2.conf").read_bytes()
    )
    active_release_config.chmod(0o644)
    active_compat_link = tmp_path / "opt/jato/active"
    active_compat_link.parent.mkdir(parents=True)
    active_compat_link.symlink_to(
        os.path.relpath(
            slots_root / MODULE.ACTIVE_SLOT / "current",
            active_compat_link.parent,
        )
    )
    return MODULE.ControllerConfig(
        layout=STORE.ReleaseLayout(
            release_root,
            slots_root,
            expected_owner_uid=os.getuid(),
        ),
        legacy_active_root=legacy_active_root,
        durable_processed_root=durable_processed_root,
        slot_env_root=tmp_path / "etc/slots",
        backend_env=backend_env,
        candidate_database_env=candidate_database_env,
        preview_config=preview_config,
        preview_unit=preview_unit,
        candidate_backend_unit=candidate_backend_unit,
        active_backend_unit=active_backend_unit,
        candidate_readonly_dropin=candidate_readonly_dropin,
        active_release_config=active_release_config,
        active_compat_link=active_compat_link,
        preview_runtime_root=tmp_path / "preview-runtime",
        reports_root=tmp_path / "reports",
        archive_cache_root=tmp_path / "archive-cache",
        production_lock=tmp_path / "state/production.lock",
        active_slot_file=active_slot_file,
        deployment_marker=tmp_path / "var/lib/jato-release/deployment-maintenance",
        jato_job_root=job_root,
        public_origin="https://www.example.test",
        expected_owner_uid=os.getuid(),
    )


def database_ok(config: MODULE.DatabaseRevisionConfig) -> dict[str, object]:
    return {"status": "compatible", "current": ["20260715_0046"], "heads": ["20260715_0046"]}


def candidate_database_ok(config) -> dict[str, object]:
    def runner(arguments, cwd, environment):
        del arguments, cwd, environment
        proof = {
            key: True
            for key in ADMISSION.CANDIDATE_DATABASE_PRIVILEGE_KEYS
        }
        return MODULE.CommandResult(0, json.dumps(proof), "")

    return MODULE.inspect_candidate_database_isolation(config, runner=runner)


def jato_idle(path: Path) -> dict[str, object]:
    return {"busy": False}


@contextmanager
def jato_release_locks(
    job_root: Path,
    active_bundle_lock: Path,
) -> Iterator[dict[str, object]]:
    del job_root, active_bundle_lock
    yield {"before": {"busy": False}, "afterLocks": {"busy": False}}


def make_http_reader(
    layout: STORE.ReleaseLayout,
    *,
    fail_candidate: bool = False,
    fail_candidate_for: STORE.ReleaseIdentity | None = None,
    fail_public_for: STORE.ReleaseIdentity | None = None,
    fail_frontend_for: STORE.ReleaseIdentity | None = None,
    candidate_monthly_enabled: bool = False,
):
    def active_identity() -> STORE.ReleaseIdentity | None:
        try:
            return STORE.read_pointer(layout, MODULE.ACTIVE_SLOT, "current")
        except STORE.ReleaseStoreError:
            return None

    def reader(url: str, timeout: int) -> tuple[int, dict[str, object]]:
        if url.endswith("/v1/msrp/monthly-update-jobs"):
            if candidate_monthly_enabled:
                return 200, {"items": []}
            return 423, {
                "detail": {
                    "enabled": False,
                    "reason": "explicitly_disabled",
                }
            }
        if url.endswith("candidate-preview.json"):
            identity = STORE.read_pointer(layout, MODULE.CANDIDATE_SLOT, "current")
            assert identity is not None
            return 200, {
                "commitSha": identity.commit_sha,
                "archiveSha256": identity.archive_sha256,
                "candidateSlot": 8001,
                "previewPort": 18002,
            }
        if url.endswith("build-meta.json"):
            slot = MODULE.CANDIDATE_SLOT if ":18002/" in url else MODULE.ACTIVE_SLOT
            identity = (
                STORE.read_pointer(layout, slot, "current")
                if slot == MODULE.CANDIDATE_SLOT
                else active_identity()
            )
            commit = LEGACY_COMMIT if identity is None else identity.commit_sha
            build_id = (
                LEGACY_BUILD
                if identity is None
                else ("0" * 64 if identity == fail_frontend_for else "6" * 64)
            )
            return 200, {
                "deployCommit": commit,
                "frontendBuildId": build_id,
            }
        if url.endswith("/healthz"):
            identity = active_identity()
            if (
                url.startswith("https://")
                and fail_public_for is not None
                and identity == fail_public_for
            ):
                return 503, {"status": "failed"}
            return 200, {"status": "ok"}
        if ":8001/readyz" in url:
            identity = STORE.read_pointer(layout, MODULE.CANDIDATE_SLOT, "current")
            assert identity is not None
            should_fail = fail_candidate and (
                fail_candidate_for is None or identity == fail_candidate_for
            )
            sha = "0" * 40 if should_fail else identity.commit_sha
            return 200, {"status": "ready", "release": {"commitSha": sha}}
        identity = active_identity()
        if identity is None:
            return 200, {
                "status": "ready",
                "release": {"commitSha": LEGACY_COMMIT},
            }
        return 200, {"status": "ready", "release": {"commitSha": identity.commit_sha}}

    return reader


def controller(
    cfg: MODULE.ControllerConfig,
    system: FakeSystem,
    *,
    fail_candidate: bool = False,
    fail_candidate_for: STORE.ReleaseIdentity | None = None,
    fail_public_for: STORE.ReleaseIdentity | None = None,
    fail_frontend_for: STORE.ReleaseIdentity | None = None,
    candidate_monthly_enabled: bool = False,
    database_inspector: MODULE.DatabaseInspector = database_ok,
    jato_lock_holder: MODULE.JatoLockHolder = jato_release_locks,
) -> MODULE.FixedReleaseController:
    system.nginx_active_config = str(cfg.active_release_config)
    system.active_instance_unit = cfg.active_backend_unit
    system.active_template_unit = cfg.active_backend_unit.with_name(
        "jato-fullstack-backend@.service"
    )
    system.active_v2_environment_files = (
        f"{cfg.backend_env} (ignore_errors=yes) "
        f"{cfg.slot_env_root / '8000.env'} (ignore_errors=no)"
    )
    system.active_legacy_environment_files = f"{cfg.backend_env} (ignore_errors=yes)"
    system.reload_active_unit()
    system.units[MODULE.CANDIDATE_UNIT]["EnvironmentFiles"] = (
        f"{cfg.backend_env} (ignore_errors=yes) "
        f"{cfg.slot_env_root / '8001.env'} (ignore_errors=no) "
        f"{cfg.candidate_database_env} (ignore_errors=no)"
    )
    system.units[MODULE.CANDIDATE_UNIT]["DropInPaths"] = str(
        cfg.candidate_readonly_dropin
    )
    system.units[MODULE.CANDIDATE_UNIT]["FragmentPath"] = str(
        cfg.candidate_backend_unit
    )
    return MODULE.FixedReleaseController(
        cfg,
        runner=system,
        http_reader=make_http_reader(
            cfg.layout,
            fail_candidate=fail_candidate,
            fail_candidate_for=fail_candidate_for,
            fail_public_for=fail_public_for,
            fail_frontend_for=fail_frontend_for,
            candidate_monthly_enabled=candidate_monthly_enabled,
        ),
        database_inspector=database_inspector,
        candidate_database_inspector=candidate_database_ok,
        jato_inspector=jato_idle,
        jato_lock_holder=jato_lock_holder,
    )


def install_active(cfg: MODULE.ControllerConfig, identity: STORE.ReleaseIdentity) -> None:
    create_release(cfg.layout, identity)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", identity)
    MODULE.FixedReleaseController(cfg)._write_slot_env(
        MODULE.ACTIVE_SLOT,
        identity,
        active=True,
    )


def install_legacy_active(cfg: MODULE.ControllerConfig) -> None:
    current = cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current")
    current.symlink_to(cfg.legacy_active_root)
    cfg.active_compat_link.unlink()
    cfg.active_backend_unit.unlink()
    legacy_template = cfg.active_backend_unit.with_name(
        "jato-fullstack-backend@.service"
    )
    legacy_template.write_text(LEGACY_UNIT, encoding="utf-8")
    legacy_template.chmod(0o644)
    legacy_env = cfg.slot_env_root / f"{MODULE.ACTIVE_SLOT}.env"
    legacy_env.parent.mkdir(parents=True, exist_ok=True)
    legacy_env.write_text(LEGACY_ENV, encoding="utf-8")
    legacy_env.chmod(0o600)


def install_candidate(
    ctrl: MODULE.FixedReleaseController,
    identity: STORE.ReleaseIdentity,
    system: FakeSystem,
) -> None:
    STORE.atomic_symlink(ctrl.config.layout, MODULE.CANDIDATE_SLOT, "current", identity)
    ctrl._write_slot_env(MODULE.CANDIDATE_SLOT, identity, active=False)
    system.units[MODULE.CANDIDATE_UNIT].update(
        {
            "ActiveState": "active",
            "SubState": "running",
            "MemoryHigh": str(MODULE.CANDIDATE_MEMORY_HIGH),
            "MemoryMax": str(MODULE.CANDIDATE_MEMORY_MAX),
        }
    )
    ctrl._write_preview_identity(identity)
    system.units[MODULE.PREVIEW_UNIT].update(
        {
            "ActiveState": "active",
            "SubState": "running",
            "MemoryHigh": str(MODULE.PREVIEW_MEMORY_HIGH),
            "MemoryMax": str(MODULE.PREVIEW_MEMORY_MAX),
        }
    )


def latest_report(cfg: MODULE.ControllerConfig) -> dict[str, object]:
    path = next(cfg.reports_root.glob("*.json"))
    return json.loads(path.read_text(encoding="utf-8"))


def test_prepare_candidate_starts_only_candidate_and_preview(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    active_digest = create_release(cfg.layout, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    assert active_digest
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", ACTIVE)
    system = FakeSystem()
    ctrl = controller(cfg, system)

    report = ctrl.prepare_candidate(CANDIDATE, manifest_sha256=candidate_digest)

    assert report["decision"] == "completed"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == CANDIDATE
    assert system.units[MODULE.ACTIVE_UNIT]["ActiveState"] == "active"
    assert system.units[MODULE.CANDIDATE_UNIT]["ActiveState"] == "active"
    assert system.units[MODULE.PREVIEW_UNIT]["ActiveState"] == "active"
    candidate_env = (cfg.slot_env_root / "8001.env").read_text(encoding="utf-8")
    assert "APP_JATO_MONTHLY_ENABLED=false" in candidate_env
    assert "APP_JATO_MONTHLY_EXECUTION_MODE=external" in candidate_env
    assert "APP_RELEASE_ROLE=candidate" in candidate_env
    assert f"APP_RELEASE_ARCHIVE_SHA256={CANDIDATE.archive_sha256}" in candidate_env
    assert "APP_RUNTIME_READ_ONLY=true" in candidate_env
    assert (
        "JATO_PARQUET_PATH=/opt/jato/shared/04_Processed_data/jato_full_archive.parquet"
        in candidate_env
    )
    assert "APP_LOCAL_WIKI_DB_PATH=/opt/jato/shared/04_Processed_data/chroma_db" in candidate_env
    assert report["mutation"]["trafficChanged"] is False
    assert "archive_cache_files_removed:0" in report["passed"]
    assert not any(
        command[:3] == ("systemctl", "set-property", MODULE.CANDIDATE_UNIT)
        for command in system.commands
    )


def test_prepare_promotes_presealed_release_without_rewriting_runtime_seal(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    staging_layout = STORE.ReleaseLayout(
        tmp_path / "staging-source",
        tmp_path / "unused-slots",
    )
    staging_layout.release_root.mkdir()
    digest = create_release(staging_layout, CANDIDATE)
    staging = tmp_path / "staging/prepare-candidate"
    staging.parent.mkdir()
    os.replace(staging_layout.release_path(CANDIDATE), staging)
    staged_runtime_seal = staging / ".jato-runtime-seal.json"
    seal_before = staged_runtime_seal.read_bytes()
    system = FakeSystem()
    ctrl = controller(cfg, system)

    report = ctrl.prepare_candidate(
        CANDIDATE,
        manifest_sha256=digest,
        staging_root=staging,
    )

    final_root = cfg.layout.release_path(CANDIDATE)
    runtime_seal = final_root / ".jato-runtime-seal.json"
    runtime_builds = [
        command
        for command in system.commands
        if len(command) >= 4
        and Path(command[2]).name == "verify_release_source_seal.py"
        and command[3] == "build"
    ]
    assert report["decision"] == "completed"
    assert not staging.exists()
    assert runtime_seal.is_file() and not runtime_seal.is_symlink()
    assert stat.S_IMODE(runtime_seal.stat().st_mode) == 0o444
    assert runtime_seal.read_bytes() == seal_before
    assert runtime_builds == []


def test_prepare_failure_restores_empty_candidate_and_keeps_active(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system, fail_candidate=True)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "runtime_sha_mismatch"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert system.units[MODULE.CANDIDATE_UNIT]["ActiveState"] == "inactive"
    assert not (cfg.slot_env_root / "8001.env").exists()
    assert not (cfg.preview_runtime_root / "candidate-preview.json").exists()
    report = latest_report(cfg)
    assert report["decision"] == "rejected"
    assert report["stage"] == "candidate_backend_verified"
    assert report["failed"]["details"]["expected"] == CANDIDATE.commit_sha
    assert "candidate_preview_verified" in report["notReached"]
    assert report["mutation"]["trafficChanged"] is False
    assert report["mutation"]["stateRestored"] is True


def test_prepare_failure_removes_only_fresh_unreferenced_release(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    staging_layout = STORE.ReleaseLayout(
        tmp_path / "staging-source",
        tmp_path / "unused-slots",
    )
    staging_layout.release_root.mkdir()
    digest = create_release(staging_layout, CANDIDATE)
    staging = tmp_path / "staging/candidate"
    staging.parent.mkdir()
    os.replace(staging_layout.release_path(CANDIDATE), staging)
    system = FakeSystem()
    ctrl = controller(cfg, system, fail_candidate=True)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(
            CANDIDATE,
            manifest_sha256=digest,
            staging_root=staging,
        )

    assert caught.value.code == "runtime_sha_mismatch"
    assert not cfg.layout.release_path(CANDIDATE).exists()
    assert cfg.layout.release_path(ACTIVE).is_dir()
    assert "failed_release_removed" in latest_report(cfg)["passed"]


def test_successive_candidate_prepares_keep_only_two_candidate_versions(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    system = FakeSystem()
    ctrl = controller(cfg, system)

    for identity in (CANDIDATE, OLDER, NEWEST):
        digest = create_release(cfg.layout, identity)
        ctrl.prepare_candidate(identity, manifest_sha256=digest)

    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == NEWEST
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "previous") == OLDER
    assert not cfg.layout.release_path(CANDIDATE).exists()
    assert cfg.layout.release_path(OLDER).is_dir()
    assert cfg.layout.release_path(NEWEST).is_dir()
    assert cfg.layout.release_path(ACTIVE).is_dir()


def test_archive_cache_gc_failure_does_not_undo_healthy_candidate(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    cache_target = tmp_path / "archive-cache-target"
    cache_target.mkdir()
    assert cfg.archive_cache_root is not None
    cfg.archive_cache_root.symlink_to(cache_target)
    system = FakeSystem()
    ctrl = controller(cfg, system)

    report = ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert report["decision"] == "completed"
    assert "archive_cache_gc_deferred:archive_cache_root_unsafe" in report["passed"]
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == CANDIDATE


def test_prepare_rejects_preview_contract_drift_before_mutation(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    cfg.preview_config.write_text("drift\n", encoding="utf-8")
    system = FakeSystem()
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "preview_contract_drift"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    mutations = {"set-property", "restart", "stop"}
    assert not any(command[1] in mutations for command in system.commands)


def test_prepare_rejects_missing_fixed_active_route_before_candidate_mutation(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    cfg.active_release_config.unlink()
    system = FakeSystem()
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "active_routing_contract_invalid"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert not any(
        command[1] in {"restart", "stop"}
        for command in system.commands
        if command[:1] == ("systemctl",)
    )


def test_prepare_installs_missing_fixed_preview_contracts(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    cfg.preview_config.unlink()
    cfg.preview_unit.unlink()
    cfg.candidate_backend_unit.unlink()
    cfg.candidate_readonly_dropin.unlink()
    shared_unit = cfg.candidate_backend_unit.with_name(
        "jato-fullstack-backend@.service"
    )
    shared_unit.write_text("legacy Active template\n", encoding="utf-8")
    shared_before = shared_unit.read_bytes()
    system = FakeSystem()
    ctrl = controller(cfg, system)

    report = ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert report["decision"] == "completed"
    assert cfg.preview_config.read_bytes() == cfg.preview_config_contract.read_bytes()
    assert cfg.preview_unit.read_bytes() == cfg.preview_unit_contract.read_bytes()
    assert (
        cfg.candidate_backend_unit.read_bytes()
        == cfg.candidate_backend_unit_contract.read_bytes()
    )
    assert (
        cfg.candidate_readonly_dropin.read_bytes()
        == cfg.candidate_readonly_contract.read_bytes()
    )
    assert ("systemctl", "daemon-reload") in system.commands
    assert shared_unit.read_bytes() == shared_before
    assert system.commands.index(("systemctl", "daemon-reload")) < system.commands.index(
        ("systemctl", "restart", MODULE.CANDIDATE_UNIT)
    )


def test_prepare_rejects_missing_candidate_database_before_mutation(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    cfg.candidate_database_env.unlink()
    system = FakeSystem()
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "database_env_unreadable"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert not any(
        command[1] in {"set-property", "restart", "stop", "daemon-reload"}
        for command in system.commands
    )


def test_prepare_rejects_candidate_database_role_reuse_before_mutation(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    cfg.candidate_database_env.write_text(
        "APP_DATABASE_ENABLED=true\n"
        "APP_DATABASE_URL=postgresql+asyncpg://jato_active:other@db.example/jato\n",
        encoding="utf-8",
    )
    cfg.candidate_database_env.chmod(0o600)
    system = FakeSystem()
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_database_role_not_isolated"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert not any(
        command[1] in {"set-property", "restart", "stop", "daemon-reload"}
        for command in system.commands
    )


def test_prepare_rejects_candidate_readonly_contract_drift_before_mutation(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    cfg.candidate_readonly_dropin.write_text("drift\n", encoding="utf-8")
    system = FakeSystem()
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "preview_contract_drift"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert not any(
        command[1] in {"set-property", "restart", "stop"}
        for command in system.commands
    )


def test_prepare_rejects_candidate_backend_unit_drift_before_candidate_mutation(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    cfg.candidate_backend_unit.write_text("drift\n", encoding="utf-8")
    system = FakeSystem()
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "preview_contract_drift"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert not any(
        command[1] in {"set-property", "restart", "stop"}
        for command in system.commands
    )


def test_prepare_restores_candidate_when_effective_readonly_contract_is_missing(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    system.units[MODULE.CANDIDATE_UNIT]["ProtectSystem"] = "full"
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_runtime_isolation_mismatch"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert system.units[MODULE.CANDIDATE_UNIT]["ActiveState"] == "inactive"
    assert latest_report(cfg)["mutation"]["stateRestored"] is True


def test_prepare_restores_candidate_when_unmanaged_dropin_is_loaded(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    system.units[MODULE.CANDIDATE_UNIT]["DropInPaths"] += (
        " /etc/systemd/system.control/"
        "jato-fullstack-backend@8001.service.d/50-legacy.conf"
    )

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_runtime_isolation_mismatch"
    assert caught.value.details["actual"].endswith("50-legacy.conf")
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert system.units[MODULE.CANDIDATE_UNIT]["ActiveState"] == "inactive"
    assert latest_report(cfg)["mutation"]["stateRestored"] is True


def test_prepare_restores_candidate_when_extra_environment_file_is_loaded(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    system.units[MODULE.CANDIDATE_UNIT]["EnvironmentFiles"] += (
        " /etc/jato-fullstack/legacy-candidate.env (ignore_errors=no)"
    )

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_runtime_isolation_mismatch"
    assert "legacy-candidate.env" in caught.value.details["actual"]
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert system.units[MODULE.CANDIDATE_UNIT]["ActiveState"] == "inactive"
    assert latest_report(cfg)["mutation"]["stateRestored"] is True


def test_prepare_restores_candidate_when_monthly_runtime_is_not_disabled(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system, candidate_monthly_enabled=True)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_monthly_runtime_enabled"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert system.units[MODULE.CANDIDATE_UNIT]["ActiveState"] == "inactive"
    report = latest_report(cfg)
    assert report["stage"] == "candidate_monthly_disabled_verified"
    assert report["mutation"]["stateRestored"] is True


def test_candidate_readonly_contract_owns_resource_limits() -> None:
    contract = (
        DEPLOY
        / "systemd/jato-fullstack-backend@8001.service.d/"
        "20-candidate-readonly.conf"
    ).read_text(encoding="utf-8")

    assert "MemoryHigh=3G" in contract
    assert "MemoryMax=4G" in contract
    assert "CPUQuota=200%" in contract


def test_prepare_restores_candidate_when_exact_8001_unit_is_not_loaded(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    system.units[MODULE.CANDIDATE_UNIT]["FragmentPath"] = (
        "/etc/systemd/system/jato-fullstack-backend@.service"
    )

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_runtime_isolation_mismatch"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert system.units[MODULE.CANDIDATE_UNIT]["ActiveState"] == "inactive"
    assert latest_report(cfg)["mutation"]["stateRestored"] is True


def test_prepare_rejects_wrong_frontend_and_restores_empty_candidate(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system, fail_frontend_for=CANDIDATE)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "frontend_build_mismatch"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert system.units[MODULE.CANDIDATE_UNIT]["ActiveState"] == "inactive"
    assert latest_report(cfg)["mutation"]["stateRestored"] is True


def test_prepare_failure_restores_running_previous_candidate(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    create_release(cfg.layout, OLDER)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(
        cfg,
        system,
        fail_candidate=True,
        fail_candidate_for=CANDIDATE,
    )
    install_candidate(ctrl, OLDER, system)
    ctrl._write_preview_identity(OLDER)
    system.units[MODULE.PREVIEW_UNIT]["ActiveState"] = "active"
    system.units[MODULE.PREVIEW_UNIT]["SubState"] = "running"
    old_env = (cfg.slot_env_root / "8001.env").read_bytes()
    old_preview = (cfg.preview_runtime_root / "candidate-preview.json").read_bytes()

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "runtime_sha_mismatch"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == OLDER
    assert (cfg.slot_env_root / "8001.env").read_bytes() == old_env
    assert (cfg.preview_runtime_root / "candidate-preview.json").read_bytes() == old_preview
    assert system.units[MODULE.CANDIDATE_UNIT]["ActiveState"] == "active"
    assert system.units[MODULE.PREVIEW_UNIT]["ActiveState"] == "active"
    assert latest_report(cfg)["mutation"]["stateRestored"] is True


def test_prepare_rejects_stale_candidate_pointer_before_any_runtime_mutation(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    create_release(cfg.layout, OLDER)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    STORE.atomic_symlink(cfg.layout, MODULE.CANDIDATE_SLOT, "current", OLDER)
    ctrl._write_slot_env(MODULE.CANDIDATE_SLOT, OLDER, active=False)
    old_env = (cfg.slot_env_root / "8001.env").read_bytes()

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_runtime_inconsistent"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == OLDER
    assert (cfg.slot_env_root / "8001.env").read_bytes() == old_env
    assert not any(command[:2] == ("systemctl", "restart") for command in system.commands)
    report = latest_report(cfg)
    assert report["stage"] == "candidate_previous_state_verified"
    assert report["mutation"]["pointerChangeAttempted"] is False
    assert report["mutation"]["serviceChangeAttempted"] is False

    ctrl.discard_candidate()
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None


def test_prepare_rejects_running_candidate_without_pointer_or_environment(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    system.units[MODULE.CANDIDATE_UNIT].update(
        {"ActiveState": "active", "SubState": "running"}
    )
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_runtime_inconsistent"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert not any(command[:2] == ("systemctl", "restart") for command in system.commands)
    assert latest_report(cfg)["mutation"]["serviceChangeAttempted"] is False


def test_prepare_rejects_preview_without_running_candidate(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    system.units[MODULE.PREVIEW_UNIT].update(
        {"ActiveState": "active", "SubState": "running"}
    )
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_runtime_inconsistent"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert not any(command[:2] == ("systemctl", "restart") for command in system.commands)


def test_prepare_rejects_pointer_env_crash_window_before_overwrite(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    create_release(cfg.layout, OLDER)
    create_release(cfg.layout, CANDIDATE)
    digest = create_release(cfg.layout, NEWEST)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, OLDER, system)
    STORE.atomic_symlink(cfg.layout, MODULE.CANDIDATE_SLOT, "current", CANDIDATE)
    old_env = (cfg.slot_env_root / "8001.env").read_bytes()

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(NEWEST, manifest_sha256=digest)

    assert caught.value.code == "runtime_sha_env_mismatch"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == CANDIDATE
    assert (cfg.slot_env_root / "8001.env").read_bytes() == old_env
    assert not any(command[:2] == ("systemctl", "restart") for command in system.commands)
    assert latest_report(cfg)["mutation"]["pointerChangeAttempted"] is False


def test_prepare_reports_trigger_when_previous_candidate_restart_fails(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    create_release(cfg.layout, OLDER)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    system.fail_restart_attempts[MODULE.CANDIDATE_UNIT] = {2}
    ctrl = controller(
        cfg,
        system,
        fail_candidate=True,
        fail_candidate_for=CANDIDATE,
    )
    install_candidate(ctrl, OLDER, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "candidate_restore_failed"
    report = latest_report(cfg)
    details = report["failed"]["details"]
    assert details["trigger"]["code"] == "runtime_sha_mismatch"
    assert any(item["step"] == "restart_previous_candidate" for item in details["restoreErrors"])


def test_discard_candidate_clears_candidate_only(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    create_release(cfg.layout, OLDER)
    STORE.atomic_symlink(cfg.layout, MODULE.CANDIDATE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.CANDIDATE_SLOT, "previous", OLDER)
    system = FakeSystem()
    ctrl = controller(cfg, system)

    report = ctrl.discard_candidate()

    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "previous") is None
    assert not cfg.layout.release_path(CANDIDATE).exists()
    assert not cfg.layout.release_path(OLDER).exists()
    assert cfg.layout.release_path(ACTIVE).is_dir()
    assert "archive_cache_files_removed:0" in report["passed"]


def test_update_active_uses_reviewed_candidate_and_keeps_candidate(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)

    report = ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert report["decision"] == "completed"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == CANDIDATE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == CANDIDATE
    active_env = (cfg.slot_env_root / "8000.env").read_text(encoding="utf-8")
    assert "APP_JATO_MONTHLY_ENABLED=true" in active_env
    assert "APP_RELEASE_ROLE=active" in active_env
    assert f"APP_JATO_MONTHLY_ACTIVE_SLOT_FILE={cfg.active_slot_file}" in active_env
    assert f"APP_JATO_MONTHLY_DEPLOYMENT_MARKER={cfg.deployment_marker}" in active_env
    assert "archive_cache_files_removed:0" in report["passed"]


def test_update_same_target_retry_preserves_previous_and_does_not_restart(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)

    ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)
    restart_count = system.restart_attempts.get(MODULE.ACTIVE_UNIT, 0)
    retry = ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert retry["decision"] == "completed"
    assert "active_target_already_current" in retry["passed"]
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == CANDIDATE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == ACTIVE
    assert "archive_cache_files_removed:0" in retry["passed"]
    assert system.restart_attempts.get(MODULE.ACTIVE_UNIT, 0) == restart_count


def test_update_retry_converges_pointer_changed_before_active_restart(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", ACTIVE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", CANDIDATE)

    report = ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert report["decision"] == "completed"
    assert "active_target_reconcile_required" in report["passed"]
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == CANDIDATE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == ACTIVE
    active_env = (cfg.slot_env_root / "8000.env").read_text(encoding="utf-8")
    assert f"APP_RELEASE_SHA={CANDIDATE.commit_sha}" in active_env


def test_update_retry_restores_previous_when_current_target_degrades(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    create_release(cfg.layout, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", ACTIVE)
    system = FakeSystem()
    ctrl = controller(cfg, system, fail_public_for=CANDIDATE)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)
    install_candidate(ctrl, CANDIDATE, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert caught.value.code == "public_not_ready"
    assert STORE.read_pointer_pair(cfg.layout, MODULE.ACTIVE_SLOT) == STORE.PointerPair(
        ACTIVE,
        CANDIDATE,
    )
    active_env = (cfg.slot_env_root / "8000.env").read_text(encoding="utf-8")
    assert f"APP_RELEASE_SHA={ACTIVE.commit_sha}" in active_env
    mutation = latest_report(cfg)["mutation"]
    assert mutation["stateRestored"] is True
    assert mutation["pointerChanged"] is True
    assert mutation["serviceChanged"] is True
    assert mutation["trafficChanged"] is True


def test_update_retry_jato_rejection_before_restart_leaves_state_unchanged(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    create_release(cfg.layout, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", ACTIVE)
    system = FakeSystem()
    ctrl = controller(cfg, system, fail_public_for=CANDIDATE)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)
    install_candidate(ctrl, CANDIDATE, system)
    calls = 0

    def fail_second_inspection(path: Path) -> dict[str, object]:
        nonlocal calls
        del path
        calls += 1
        if calls == 2:
            raise MODULE.V2Error("jato_became_busy", "injected busy state")
        return {"busy": False}

    ctrl.jato_inspector = fail_second_inspection
    original_env = (cfg.slot_env_root / "8000.env").read_bytes()
    original_pair = STORE.read_pointer_pair(cfg.layout, MODULE.ACTIVE_SLOT)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert caught.value.code == "jato_became_busy"
    assert STORE.read_pointer_pair(cfg.layout, MODULE.ACTIVE_SLOT) == original_pair
    assert (cfg.slot_env_root / "8000.env").read_bytes() == original_env
    assert system.restart_attempts.get(MODULE.ACTIVE_UNIT, 0) == 0
    mutation = latest_report(cfg)["mutation"]
    assert mutation["pointerChangeAttempted"] is False
    assert mutation["serviceChangeAttempted"] is False
    assert mutation["stateRestored"] is False


def test_update_rejects_busy_jato_release_lock_before_mutation(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()

    @contextmanager
    def busy_lock(job_root: Path, active_bundle_lock: Path):
        del job_root, active_bundle_lock
        raise MODULE.AdmissionError("jato_lock_busy", "busy")
        yield  # pragma: no cover

    ctrl = controller(cfg, system, jato_lock_holder=busy_lock)
    install_candidate(ctrl, CANDIDATE, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert caught.value.code == "jato_lock_busy"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE


def test_update_rejects_stale_legacy_marker_before_active_mutation(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)
    cfg.deployment_marker.parent.mkdir(parents=True, exist_ok=True)
    cfg.deployment_marker.write_text("stale\n", encoding="utf-8")

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert caught.value.code == "deployment_marker_retained"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE


def test_update_failure_restores_previous_active(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system, fail_public_for=CANDIDATE)
    install_candidate(ctrl, CANDIDATE, system)
    active_env = cfg.slot_env_root / "8000.env"
    active_env.write_text(active_env.read_text(encoding="utf-8") + "CUSTOM=value\n", encoding="utf-8")
    original_env = active_env.read_bytes()

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert caught.value.code == "public_not_ready"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") is None
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == CANDIDATE
    assert active_env.read_bytes() == original_env
    assert latest_report(cfg)["mutation"]["stateRestored"] is True


def test_update_reports_original_and_restore_failures(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    system.fail_restart_attempts[MODULE.ACTIVE_UNIT] = {2}
    ctrl = controller(cfg, system, fail_public_for=CANDIDATE)
    install_candidate(ctrl, CANDIDATE, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert caught.value.code == "active_restore_failed"
    details = latest_report(cfg)["failed"]["details"]
    assert details["trigger"]["code"] == "public_not_ready"
    assert any(item["step"] == "restart_previous_active" for item in details["restoreErrors"])


def test_update_rejects_candidate_identity_change_before_active_mutation(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    create_release(cfg.layout, CANDIDATE)
    older_digest = create_release(cfg.layout, OLDER)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(OLDER, manifest_sha256=older_digest)

    assert caught.value.code == "candidate_identity_mismatch"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE


def test_update_rejects_public_routing_drift_before_active_mutation(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)
    cfg.active_release_config.write_text("drift\n", encoding="utf-8")

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert caught.value.code == "active_routing_contract_invalid"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert latest_report(cfg)["mutation"]["pointerChangeAttempted"] is False


def test_rollback_rejects_first_transition_b_b_without_distinct_previous(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    digest = create_release(cfg.layout, CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.rollback_active(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "rollback_unavailable"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == CANDIDATE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == CANDIDATE
    assert latest_report(cfg)["mutation"]["pointerChangeAttempted"] is False


def test_rollback_atomically_exchanges_current_and_previous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config(tmp_path)
    active_digest = create_release(cfg.layout, ACTIVE)
    create_release(cfg.layout, CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", ACTIVE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)
    observed_pairs: list[tuple[STORE.ReleaseIdentity | None, STORE.ReleaseIdentity | None]] = []
    original_exchange = MODULE.atomic_exchange_pointers

    def record_exchange(
        layout: STORE.ReleaseLayout,
        slot: STORE.Slot,
        expected: STORE.PointerPair,
    ) -> None:
        original_exchange(layout, slot, expected)
        if slot == MODULE.ACTIVE_SLOT:
            pair = STORE.read_pointer_pair(layout, MODULE.ACTIVE_SLOT)
            observed_pairs.append((pair.current, pair.previous))

    monkeypatch.setattr(MODULE, "atomic_exchange_pointers", record_exchange)

    report = ctrl.rollback_active(ACTIVE, manifest_sha256=active_digest)

    assert report["decision"] == "completed"
    assert report["target"] == {
        "commitSha": ACTIVE.commit_sha,
        "archiveSha256": ACTIVE.archive_sha256,
        "manifestSha256": active_digest,
    }
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == CANDIDATE
    assert observed_pairs == [(ACTIVE, CANDIDATE)]
    assert (ACTIVE, ACTIVE) not in observed_pairs
    assert (CANDIDATE, CANDIDATE) not in observed_pairs
    assert "archive_cache_files_removed:0" in report["passed"]


def test_rollback_same_target_retry_is_idempotent_and_does_not_toggle(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    active_digest = create_release(cfg.layout, ACTIVE)
    create_release(cfg.layout, CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", ACTIVE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)

    ctrl.rollback_active(ACTIVE, manifest_sha256=active_digest)
    restart_count = system.restart_attempts.get(MODULE.ACTIVE_UNIT, 0)
    retry = ctrl.rollback_active(ACTIVE, manifest_sha256=active_digest)

    assert retry["decision"] == "completed"
    assert "active_target_already_current" in retry["passed"]
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == CANDIDATE
    assert system.restart_attempts.get(MODULE.ACTIVE_UNIT, 0) == restart_count


def test_rollback_reverse_direction_requires_explicit_target(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    active_digest = create_release(cfg.layout, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", ACTIVE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)

    ctrl.rollback_active(ACTIVE, manifest_sha256=active_digest)
    report = ctrl.rollback_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert report["decision"] == "completed"
    assert STORE.read_pointer_pair(cfg.layout, MODULE.ACTIVE_SLOT) == STORE.PointerPair(
        CANDIDATE,
        ACTIVE,
    )


def test_rollback_rejects_unsupported_exchange_without_service_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config(tmp_path)
    active_digest = create_release(cfg.layout, ACTIVE)
    create_release(cfg.layout, CANDIDATE)
    original = STORE.PointerPair(CANDIDATE, ACTIVE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", ACTIVE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)

    def unsupported(*args, **kwargs) -> None:
        del args, kwargs
        raise STORE.ReleaseStoreError(
            "pointer_exchange_unsupported",
            "injected unsupported exchange",
        )

    monkeypatch.setattr(MODULE, "atomic_exchange_pointers", unsupported)
    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.rollback_active(ACTIVE, manifest_sha256=active_digest)

    assert caught.value.code == "pointer_exchange_unsupported"
    assert STORE.read_pointer_pair(cfg.layout, MODULE.ACTIVE_SLOT) == original
    assert system.restart_attempts.get(MODULE.ACTIVE_UNIT, 0) == 0
    assert latest_report(cfg)["mutation"]["serviceChangeAttempted"] is False


def test_rollback_retry_converges_pointer_changed_before_restart(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    active_digest = create_release(cfg.layout, ACTIVE)
    create_release(cfg.layout, CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", ACTIVE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)

    report = ctrl.rollback_active(ACTIVE, manifest_sha256=active_digest)

    assert report["decision"] == "completed"
    assert "active_target_reconcile_required" in report["passed"]
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == CANDIDATE
    active_env = (cfg.slot_env_root / "8000.env").read_text(encoding="utf-8")
    assert f"APP_RELEASE_SHA={ACTIVE.commit_sha}" in active_env


def test_rollback_retry_restores_last_healthy_active_when_target_still_fails(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    active_digest = create_release(cfg.layout, ACTIVE)
    create_release(cfg.layout, CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", ACTIVE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system, fail_public_for=ACTIVE)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.rollback_active(ACTIVE, manifest_sha256=active_digest)

    assert caught.value.code == "public_not_ready"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == CANDIDATE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == ACTIVE
    assert latest_report(cfg)["mutation"]["stateRestored"] is True


def test_rollback_failure_restores_original_active_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config(tmp_path)
    active_digest = create_release(cfg.layout, ACTIVE)
    create_release(cfg.layout, CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", ACTIVE)
    system = FakeSystem()
    ctrl = controller(cfg, system, fail_public_for=ACTIVE)
    ctrl._write_slot_env(MODULE.ACTIVE_SLOT, CANDIDATE, active=True)
    observed_pairs: list[tuple[STORE.ReleaseIdentity | None, STORE.ReleaseIdentity | None]] = []
    original_exchange = MODULE.atomic_exchange_pointers

    def record_exchange(
        layout: STORE.ReleaseLayout,
        slot: STORE.Slot,
        expected: STORE.PointerPair,
    ) -> None:
        original_exchange(layout, slot, expected)
        if slot == MODULE.ACTIVE_SLOT:
            pair = STORE.read_pointer_pair(layout, MODULE.ACTIVE_SLOT)
            observed_pairs.append((pair.current, pair.previous))

    monkeypatch.setattr(MODULE, "atomic_exchange_pointers", record_exchange)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.rollback_active(ACTIVE, manifest_sha256=active_digest)

    assert caught.value.code == "public_not_ready"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == CANDIDATE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == ACTIVE
    assert observed_pairs[0] == (ACTIVE, CANDIDATE)
    assert observed_pairs[-1] == (CANDIDATE, ACTIVE)
    assert (ACTIVE, ACTIVE) not in observed_pairs
    assert (CANDIDATE, CANDIDATE) not in observed_pairs
    assert latest_report(cfg)["mutation"]["stateRestored"] is True


def test_update_uses_verified_real_durable_bundle_lock(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    captured: list[Path] = []

    @contextmanager
    def capture_lock(job_root: Path, active_bundle_lock: Path):
        del job_root
        captured.append(active_bundle_lock)
        yield {"before": {"busy": False}, "afterLocks": {"busy": False}}

    ctrl = controller(cfg, system, jato_lock_holder=capture_lock)
    install_candidate(ctrl, CANDIDATE, system)

    ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert captured == [cfg.durable_processed_root / "active-bundle.lock"]


def test_update_rejects_wrong_processed_data_link_before_active_mutation(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    active_processed = cfg.layout.release_path(ACTIVE) / "04_Processed_data"
    active_processed.unlink()
    wrong = tmp_path / "wrong-processed"
    wrong.mkdir()
    active_processed.symlink_to(wrong)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert caught.value.code == "durable_processed_root_mismatch"
    assert latest_report(cfg)["mutation"]["pointerChangeAttempted"] is False


def test_production_lock_rejects_final_symlink(tmp_path: Path) -> None:
    target = tmp_path / "actual.lock"
    target.touch()
    link = tmp_path / "production.lock"
    link.symlink_to(target)

    with pytest.raises(ADMISSION.AdmissionError) as caught:
        with MODULE.production_lock(link):
            pass

    assert caught.value.code == "jato_lock_path_unsafe"


def test_snapshot_failure_still_writes_structured_report(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)

    def unavailable(arguments: tuple[str, ...], timeout: int) -> MODULE.CommandResult:
        if arguments[:2] == ("systemctl", "show"):
            raise TimeoutError
        if arguments == ("nginx", "-T"):
            return MODULE.CommandResult(
                0,
                f"include {cfg.active_release_config};\n"
                "server 127.0.0.1:8000 max_fails=3 fail_timeout=30s;\n"
                'default "/opt/jato/slots/8000/current/'
                '06_AppPlatform/frontend/dist";\n',
                "",
            )
        return MODULE.CommandResult(1, "", "unavailable")

    ctrl = MODULE.FixedReleaseController(
        cfg,
        runner=unavailable,
        http_reader=make_http_reader(cfg.layout),
        database_inspector=database_ok,
        jato_inspector=jato_idle,
    )

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.discard_candidate()

    assert caught.value.code == "unexpected_error"
    report = latest_report(cfg)
    assert report["decision"] == "rejected"
    assert report["before"]["activeUnit"]["ActiveState"] == "error:TimeoutError"


def test_cli_preserves_distinct_exit_when_active_restore_is_unproven(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailedController:
        def __init__(self, config) -> None:
            del config

        def rollback_active(self, *args, **kwargs):
            del args, kwargs
            raise MODULE.V2Error("active_restore_failed", "restore unavailable")

    monkeypatch.setattr(MODULE, "FixedReleaseController", FailedController)

    assert MODULE.main(
        [
            "rollback-active",
            "--commit",
            ACTIVE.commit_sha,
            "--archive-sha256",
            ACTIVE.archive_sha256,
            "--manifest-sha256",
            "7" * 64,
        ]
    ) == MODULE.EXIT_ACTIVE_RESTORE_UNPROVEN


def test_prepare_cli_binds_candidate_unit_contract_to_promoted_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[MODULE.ControllerConfig] = []

    class CapturingController:
        def __init__(self, controller_config: MODULE.ControllerConfig) -> None:
            captured.append(controller_config)

        def prepare_candidate(self, *args, **kwargs) -> dict[str, object]:
            del args, kwargs
            return {"reportPath": str(tmp_path / "report.json")}

    monkeypatch.setattr(MODULE, "FixedReleaseController", CapturingController)
    release_root = tmp_path / "releases"
    slots_root = tmp_path / "slots"

    result = MODULE.main(
        [
            "--release-root",
            str(release_root),
            "--slots-root",
            str(slots_root),
            "--reports-root",
            str(tmp_path / "reports"),
            "--archive-cache-root",
            str(tmp_path / "archive-cache"),
            "prepare-candidate",
            "--commit",
            CANDIDATE.commit_sha,
            "--archive-sha256",
            CANDIDATE.archive_sha256,
            "--manifest-sha256",
            "7" * 64,
            "--staging-root",
            str(tmp_path / "staging"),
        ]
    )

    expected_root = release_root / CANDIDATE.commit_sha / CANDIDATE.archive_sha256
    assert result == 0
    assert captured[0].candidate_backend_unit_contract == (
        expected_root / "03_Scripts/deploy/systemd/jato-fullstack-backend@.service"
    )
    assert captured[0].archive_cache_root == tmp_path / "archive-cache"


def test_update_rejects_manifest_digest_change_before_active_mutation(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256="f" * 64)

    assert caught.value.code == "manifest_sha256_mismatch"
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("missing", "release_build_metadata_invalid"),
        ("tampered", "release_build_metadata_mismatch"),
    ],
)
def test_update_rejects_invalid_build_metadata_before_active_mutation(
    tmp_path: Path,
    mutation: str,
    expected_code: str,
) -> None:
    cfg = config(tmp_path)
    install_active(cfg, ACTIVE)
    candidate_digest = create_release(cfg.layout, CANDIDATE)
    build_metadata = cfg.layout.release_path(CANDIDATE) / "hermes/deploy_release.json"
    if mutation == "missing":
        build_metadata.unlink()
    else:
        build_metadata.write_text('{"tampered": true}\n', encoding="utf-8")
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=candidate_digest)

    assert caught.value.code == expected_code
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == ACTIVE


def test_prepare_accepts_exact_legacy_active_without_modifying_it(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    digest = create_release(cfg.layout, CANDIDATE)
    legacy_cache = STORE.ReleaseIdentity(LEGACY_COMMIT, "9" * 64)
    assert cfg.archive_cache_root is not None
    legacy_cache_root = cfg.archive_cache_root / legacy_cache.commit_sha
    legacy_cache_root.mkdir(parents=True)
    legacy_archive = legacy_cache_root / f"{legacy_cache.archive_sha256}.tar.gz"
    legacy_archive.write_bytes(b"legacy active archive")
    Path(f"{legacy_archive}.sha256").write_text(
        legacy_cache.archive_sha256 + "\n",
        encoding="utf-8",
    )
    Path(f"{legacy_archive}.lock").touch()
    system = FakeSystem()
    inspected: list[MODULE.DatabaseRevisionConfig] = []

    def inspect_database(
        database_config: MODULE.DatabaseRevisionConfig,
    ) -> dict[str, object]:
        inspected.append(database_config)
        return database_ok(database_config)

    ctrl = controller(cfg, system, database_inspector=inspect_database)
    active_link = cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current")
    active_before = active_link.lstat()
    target_before = os.readlink(active_link)

    report = ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    active_after = active_link.lstat()
    assert report["decision"] == "completed"
    assert "active_baseline_verified" in report["passed"]
    assert "active_unchanged" in report["passed"]
    assert "archive_cache_gc_deferred_for_legacy_active" in report["passed"]
    assert legacy_archive.is_file()
    assert active_after.st_ino == active_before.st_ino
    assert active_after.st_mtime_ns == active_before.st_mtime_ns
    assert os.readlink(active_link) == target_before
    assert active_link.resolve(strict=True) == cfg.legacy_active_root
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == CANDIDATE
    assert inspected[0].active_root == cfg.layout.release_path(CANDIDATE)
    assert inspected[0].candidate_root == cfg.layout.release_path(CANDIDATE)
    active_mutations = {
        ("systemctl", "restart", MODULE.ACTIVE_UNIT),
        ("systemctl", "stop", MODULE.ACTIVE_UNIT),
    }
    assert not active_mutations.intersection(system.commands)


def test_first_update_adopts_reviewed_candidate_as_b_b(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)

    report = ctrl.update_active(CANDIDATE, manifest_sha256=digest)

    assert report["decision"] == "completed"
    assert "legacy_active_adopted_as_b_b" in report["passed"]
    active_link = cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current")
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "current") == CANDIDATE
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == CANDIDATE
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") == CANDIDATE
    assert active_link.resolve(strict=True) == cfg.layout.release_path(CANDIDATE)
    assert cfg.active_backend_unit.read_bytes() == (
        DEPLOY / "systemd/jato-fullstack-backend@.service"
    ).read_bytes()
    assert cfg.active_backend_unit.with_name(
        "jato-fullstack-backend@.service"
    ).read_text(encoding="utf-8") == LEGACY_UNIT
    assert cfg.active_compat_link.resolve(strict=True) == cfg.layout.release_path(
        CANDIDATE
    )
    assert system.units[MODULE.ACTIVE_UNIT]["FragmentPath"] == str(
        cfg.active_backend_unit
    )
    assert system.units[MODULE.ACTIVE_UNIT]["WorkingDirectory"] == (
        "/opt/jato/slots/8000/current/06_AppPlatform/backend"
    )
    active_env = (cfg.slot_env_root / "8000.env").read_text(encoding="utf-8")
    assert "APP_RELEASE_ROLE=active" in active_env
    assert f"APP_RELEASE_SHA={CANDIDATE.commit_sha}" in active_env
    assert "APP_JATO_MONTHLY_ENABLED=true" in active_env
    restart_count = system.restart_attempts[MODULE.ACTIVE_UNIT]
    retry = ctrl.update_active(CANDIDATE, manifest_sha256=digest)
    assert "active_target_already_current" in retry["passed"]
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") == CANDIDATE
    assert system.restart_attempts[MODULE.ACTIVE_UNIT] == restart_count


def test_first_update_failure_restores_exact_legacy_active(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system, fail_public_for=CANDIDATE)
    install_candidate(ctrl, CANDIDATE, system)
    active_link = cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current")
    raw_target = os.readlink(active_link)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "public_not_ready"
    assert os.readlink(active_link) == raw_target
    assert active_link.resolve(strict=True) == cfg.legacy_active_root
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") is None
    assert not cfg.active_backend_unit.exists()
    assert cfg.active_backend_unit.with_name(
        "jato-fullstack-backend@.service"
    ).read_text(encoding="utf-8") == LEGACY_UNIT
    assert (cfg.slot_env_root / "8000.env").read_text(encoding="utf-8") == LEGACY_ENV
    assert not cfg.active_compat_link.exists()
    assert system.units[MODULE.ACTIVE_UNIT]["FragmentPath"] == str(
        cfg.active_backend_unit.with_name("jato-fullstack-backend@.service")
    )
    assert system.units[MODULE.ACTIVE_UNIT]["WorkingDirectory"] == LEGACY_WORKDIR
    assert system.units[MODULE.ACTIVE_UNIT]["ExecStart"] == LEGACY_EXEC_START
    report = latest_report(cfg)
    mutation = report["mutation"]
    assert mutation["stateRestored"] is True
    assert mutation["pointerChanged"] is True
    assert mutation["serviceChanged"] is True
    assert mutation["trafficChanged"] is True
    assert "legacy_active_restored" in report["passed"]


def test_first_update_restart_failure_restores_legacy_then_retry_succeeds(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    system.fail_restart_attempts[MODULE.ACTIVE_UNIT] = {1}
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)
    raw_target = os.readlink(cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current"))

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "command_failed"
    assert os.readlink(cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current")) == raw_target
    assert STORE.read_pointer(cfg.layout, MODULE.ACTIVE_SLOT, "previous") is None
    assert not cfg.active_backend_unit.exists()
    assert not cfg.active_compat_link.exists()
    failed_mutation = latest_report(cfg)["mutation"]
    assert failed_mutation["pointerChanged"] is True
    assert failed_mutation["serviceChanged"] is True
    assert failed_mutation["trafficChanged"] is True
    system.fail_restart_attempts[MODULE.ACTIVE_UNIT].clear()

    report = ctrl.update_active(CANDIDATE, manifest_sha256=digest)

    assert report["decision"] == "completed"
    assert STORE.read_pointer_pair(cfg.layout, MODULE.ACTIVE_SLOT) == STORE.PointerPair(
        CANDIDATE,
        CANDIDATE,
    )


def test_first_update_reports_trigger_and_legacy_restore_failure(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    system.fail_restart_attempts[MODULE.ACTIVE_UNIT] = {1, 2}
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "active_restore_failed"
    details = latest_report(cfg)["failed"]["details"]
    assert details["trigger"]["code"] == "command_failed"
    assert details["restore"]["code"] == "command_failed"


def test_first_update_rejects_stale_explicit_unit_before_mutation(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)
    cfg.active_backend_unit.write_text("stale explicit\n", encoding="utf-8")
    cfg.active_backend_unit.chmod(0o644)
    raw_target = os.readlink(cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current"))

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "legacy_active_unit_mismatch"
    assert cfg.active_backend_unit.read_text(encoding="utf-8") == "stale explicit\n"
    assert os.readlink(cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current")) == raw_target
    assert system.restart_attempts.get(MODULE.ACTIVE_UNIT, 0) == 0
    assert latest_report(cfg)["mutation"]["pointerChangeAttempted"] is False


def test_first_update_compat_creation_failure_is_restored(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)
    original = Path.symlink_to

    def fail_compat(path: Path, target: str | os.PathLike[str], *args, **kwargs) -> None:
        if path == cfg.active_compat_link:
            raise OSError("injected compat failure")
        original(path, target, *args, **kwargs)

    monkeypatch.setattr(Path, "symlink_to", fail_compat)
    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "unexpected_error"
    assert not cfg.active_backend_unit.exists()
    assert not cfg.active_compat_link.exists()
    assert cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current").resolve() == (
        cfg.legacy_active_root
    )
    mutation = latest_report(cfg)["mutation"]
    assert mutation["stateRestored"] is True
    assert mutation["pointerChanged"] is False
    assert mutation["serviceChanged"] is True
    assert mutation["trafficChanged"] is False


@pytest.mark.parametrize(
    ("unsafe_state", "expected_code"),
    [
        ("previous", "legacy_active_previous_present"),
        ("unit", "legacy_active_unit_mismatch"),
    ],
)
def test_first_update_rejects_unsafe_legacy_preimage_before_mutation(
    tmp_path: Path,
    unsafe_state: str,
    expected_code: str,
) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    install_candidate(ctrl, CANDIDATE, system)
    if unsafe_state == "previous":
        STORE.atomic_symlink(cfg.layout, MODULE.ACTIVE_SLOT, "previous", CANDIDATE)
    else:
        system.units[MODULE.ACTIVE_UNIT]["FragmentPath"] = "/etc/other.service"
    active_link = cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current")
    raw_target = os.readlink(active_link)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.update_active(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == expected_code
    assert os.readlink(active_link) == raw_target
    assert not cfg.active_backend_unit.exists()
    assert (cfg.slot_env_root / "8000.env").read_text(encoding="utf-8") == LEGACY_ENV
    assert system.restart_attempts.get(MODULE.ACTIVE_UNIT, 0) == 0
    assert latest_report(cfg)["mutation"]["pointerChangeAttempted"] is False


def test_discard_candidate_defers_gc_while_active_is_legacy(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    create_release(cfg.layout, CANDIDATE)
    create_release(cfg.layout, OLDER)
    STORE.atomic_symlink(cfg.layout, MODULE.CANDIDATE_SLOT, "current", CANDIDATE)
    STORE.atomic_symlink(cfg.layout, MODULE.CANDIDATE_SLOT, "previous", OLDER)
    system = FakeSystem()
    ctrl = controller(cfg, system)
    active_link = cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current")
    target_before = os.readlink(active_link)

    report = ctrl.discard_candidate()

    assert report["decision"] == "completed"
    assert "gc_deferred_for_legacy_active" in report["passed"]
    assert not any(
        item.startswith("unreferenced_releases_removed:")
        for item in report["passed"]
    )
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "previous") is None
    assert cfg.layout.release_path(CANDIDATE).is_dir()
    assert cfg.layout.release_path(OLDER).is_dir()
    assert os.readlink(active_link) == target_before
    assert active_link.resolve(strict=True) == cfg.legacy_active_root


def test_legacy_prepare_rejects_database_mismatch_without_mutating_active(
    tmp_path: Path,
) -> None:
    cfg = config(tmp_path)
    install_legacy_active(cfg)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()

    def migration_required(
        database_config: MODULE.DatabaseRevisionConfig,
    ) -> dict[str, object]:
        assert database_config.active_root == cfg.layout.release_path(CANDIDATE)
        return {
            "status": "migration-required",
            "current": ["old"],
            "heads": ["new"],
        }

    ctrl = controller(cfg, system, database_inspector=migration_required)
    active_link = cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current")
    before = active_link.lstat()
    target_before = os.readlink(active_link)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    after = active_link.lstat()
    assert caught.value.code == "migration_required"
    assert after.st_ino == before.st_ino
    assert after.st_mtime_ns == before.st_mtime_ns
    assert os.readlink(active_link) == target_before
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert not any(
        command[1] in {"set-property", "restart", "stop"}
        for command in system.commands
    )


def test_prepare_rejects_unrecognized_active_target_before_mutation(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    other = tmp_path / "other-active"
    other.mkdir()
    cfg.layout.pointer_path(MODULE.ACTIVE_SLOT, "current").symlink_to(other)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "v2_bootstrap_required"
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert not any(
        command[1] in {"set-property", "restart", "stop", "daemon-reload"}
        for command in system.commands
    )


def test_missing_active_is_rejected_before_candidate_mutation(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    digest = create_release(cfg.layout, CANDIDATE)
    system = FakeSystem()
    ctrl = controller(cfg, system)

    with pytest.raises(MODULE.V2Error) as caught:
        ctrl.prepare_candidate(CANDIDATE, manifest_sha256=digest)

    assert caught.value.code == "active_baseline_missing"
    report = json.loads(str(caught.value))
    assert report["mutation"]["pointerChangeAttempted"] is False
    assert report["mutation"]["serviceChangeAttempted"] is False
    assert report["mutation"]["releaseStoreChanged"] is False
    assert STORE.read_pointer(cfg.layout, MODULE.CANDIDATE_SLOT, "current") is None
    assert system.units[MODULE.ACTIVE_UNIT]["ActiveState"] == "active"


def test_fixed_active_contract_matches_existing_nginx_installer() -> None:
    installer = (DEPLOY / "nginx/install_jato_fullstack_nginx.sh").read_text(
        encoding="utf-8"
    )
    marker = 'payload = f"""'
    start = installer.index(marker) + len(marker)
    end = installer.index('"""\noutput_path.write_text', start)
    rendered = (
        installer[start:end]
        .replace("{port}", MODULE.ACTIVE_SLOT)
        .replace(
            "{frontend_root}",
            "/opt/jato/slots/8000/current/06_AppPlatform/frontend/dist",
        )
        .replace("{{", "{")
        .replace("}}", "}")
        .replace('\\"', '"')
    )

    assert (
        DEPLOY / "nginx/jato_active_release_v2.conf"
    ).read_text(encoding="utf-8") == rendered


def test_candidate_systemd_contract_enforces_production_data_read_only() -> None:
    payload = (
        DEPLOY
        / "systemd/jato-fullstack-backend@8001.service.d/"
        "20-candidate-readonly.conf"
    ).read_text(encoding="utf-8")

    assert "EnvironmentFile=/etc/jato-fullstack/candidate-database.env" in payload
    assert "Environment=APP_RUNTIME_READ_ONLY=true" in payload
    assert "default_transaction_read_only=on" in payload
    assert "ProtectSystem=strict" in payload
    assert "NoNewPrivileges=true" in payload
    assert "ReadOnlyPaths=/opt/jato/shared" in payload
    assert "ReadWritePaths=/var/cache/jato-candidate" in payload
