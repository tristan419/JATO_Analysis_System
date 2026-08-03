from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SYSTEMD_UNIT = (
    REPO_ROOT
    / "03_Scripts/deploy/systemd/jato-fullstack-backend@.service"
)
BOOT_RECONCILE_UNIT = (
    REPO_ROOT
    / "03_Scripts/deploy/systemd/jato-bluegreen-boot-reconcile.service"
)
NGINX_BOOT_RECONCILE_DROPIN = (
    REPO_ROOT
    / "03_Scripts/deploy/systemd/nginx-jato-bluegreen-boot-reconcile.conf"
)
SLOT_ENV_EXAMPLE = (
    REPO_ROOT
    / "03_Scripts/deploy/systemd/jato-fullstack-backend-slot.env.example"
)
NGINX_TEMPLATE = (
    REPO_ROOT
    / "03_Scripts/deploy/nginx/jato_fullstack.conf.example"
)
NGINX_INSTALLER = (
    REPO_ROOT
    / "03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh"
)
HTTPS_INSTALLER = (
    REPO_ROOT
    / "03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh"
)
MSRP_ENV_EXAMPLE = (
    REPO_ROOT
    / "03_Scripts/deploy/systemd/jato-msrp.env.example"
)
SYNC_DATA_TO_CLOUD = (
    REPO_ROOT
    / "03_Scripts/ops/sync_data_to_cloud.sh"
)
SYNC_MSRP_DB_TO_CLOUD = (
    REPO_ROOT
    / "03_Scripts/ops/sync_msrp_db_to_cloud.sh"
)
PRODUCTION_MUTATION_LOCK = (
    REPO_ROOT
    / "03_Scripts/deploy/lib/production_mutation_lock.sh"
)
REMOTE_RELEASE = REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
BLUEGREEN_CONTROLLER = (
    REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
)


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


def _fake_runtime(
    tmp_path: Path,
    *,
    fail_nginx_test: bool = False,
) -> tuple[dict[str, str], Path, Path, Path]:
    nginx_etc = tmp_path / "etc/nginx"
    jato_etc = tmp_path / "etc/jato-fullstack"
    fake_bin = tmp_path / "bin"
    command_log = tmp_path / "commands.log"
    (nginx_etc / "sites-available").mkdir(parents=True)
    (nginx_etc / "sites-enabled").mkdir(parents=True)
    fake_bin.mkdir()

    _write_executable(
        fake_bin / "nginx",
        "#!/usr/bin/env bash\n"
        'printf "nginx %s\\n" "$*" >> "$COMMAND_LOG"\n'
        'if [[ "${FAIL_NGINX_TEST:-false}" == "true" && "${1:-}" == "-t" ]]; then\n'
        "  exit 1\n"
        "fi\n",
    )
    _write_executable(
        fake_bin / "systemctl",
        "#!/usr/bin/env bash\n"
        'printf "systemctl %s\\n" "$*" >> "$COMMAND_LOG"\n'
        'if [[ "${1:-}" == "show" && "$*" == *"LoadState"* ]]; then\n'
        '  printf "%s\\n" "${SWITCH_LOAD_STATE:-not-found}"\n'
        "  exit 0\n"
        "fi\n"
        'if [[ "${1:-}" == "show" && "$*" == *"ActiveState"* ]]; then\n'
        '  printf "%s\\n" "${SWITCH_ACTIVE_STATE:-inactive}"\n'
        "  exit 0\n"
        "fi\n"
        'if [[ "${1:-}" == "show" && "$*" == *"SubState"* ]]; then\n'
        '  printf "%s\\n" "${SWITCH_SUB_STATE:-dead}"\n'
        "  exit 0\n"
        "fi\n"
        'if [[ "${1:-}" == "is-active" ]]; then exit 0; fi\n',
    )
    _write_executable(
        fake_bin / "curl",
        "#!/usr/bin/env bash\n"
        'printf "curl %s\\n" "$*" >> "$COMMAND_LOG"\n',
    )
    _write_executable(fake_bin / "apt-get", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        fake_bin / "flock",
        "#!/usr/bin/env bash\n"
        'if [[ "${1:-}" == "-n" ]]; then exit 1; fi\n'
        "exit 0\n",
    )
    _write_executable(
        fake_bin / "realpath",
        "#!/usr/bin/env python3\n"
        "import os, sys\n"
        "value = sys.argv[-1]\n"
        "print(os.path.abspath(value))\n",
    )

    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "COMMAND_LOG": str(command_log),
        "FAIL_NGINX_TEST": "true" if fail_nginx_test else "false",
        "NGINX_ETC_DIR": str(nginx_etc),
        "JATO_ETC_DIR": str(jato_etc),
        "BACKUP_DIR": str(nginx_etc / "jato-backups"),
        "SKIP_PACKAGE_INSTALL": "true",
        "SKIP_HEALTH_CHECK": "true",
        "JATO_PRODUCTION_DEPLOY_LOCK_PATH": str(
            tmp_path / "production-deploy.lock"
        ),
        "JATO_PRODUCTION_DEPLOY_LOCK_WAIT": "0",
        "SERVER_NAME": "www.ojeur.cloud ojeur.cloud",
        "BACKEND_PORT": "8001",
        "FRONTEND_ROOT": (
            "/opt/jato/slots/8001/current/06_AppPlatform/frontend/dist"
        ),
    }
    return env, nginx_etc, jato_etc, command_log


def _run_installer(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(NGINX_INSTALLER)],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _certbot_site() -> str:
    return """upstream jato_fullstack_api {
    server 127.0.0.1:8000;
}

server {
    listen 80;
    server_name www.ojeur.cloud ojeur.cloud;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name www.ojeur.cloud ojeur.cloud;
    root /opt/JATO_Analysis_System-main/06_AppPlatform/frontend/dist;

    ssl_certificate /etc/letsencrypt/live/ojeur.cloud/fullchain.pem; # managed by Certbot
    ssl_certificate_key /etc/letsencrypt/live/ojeur.cloud/privkey.pem; # managed by Certbot

    location = /healthz {
        proxy_pass http://jato_fullstack_api/healthz;
        add_header Cache-Control "no-store" always;
    }

    location ^~ /v1/ {
        proxy_pass http://jato_fullstack_api;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
"""


def test_systemd_template_uses_slot_release_and_layered_environment() -> None:
    unit = SYSTEMD_UNIT.read_text(encoding="utf-8")
    common_env = "EnvironmentFile=-/etc/jato-fullstack/backend.env"
    slot_env = "EnvironmentFile=/etc/jato-fullstack/slots/%i.env"

    assert "WorkingDirectory=/opt/jato/slots/%i/current/06_AppPlatform/backend" in unit
    assert (
        "ExecStart=/opt/jato/slots/%i/current/.venv/bin/python "
        "-m uvicorn app.main:app --host 127.0.0.1 --port %i "
        "--workers ${APP_BACKEND_WORKERS}"
    ) in unit
    assert unit.index(common_env) < unit.index(slot_env)
    assert "Environment=APP_BACKEND_WORKERS=2" in unit
    assert "KillMode=control-group" in unit
    assert "MemoryAccounting=yes" in unit
    assert "TasksAccounting=yes" in unit
    assert "OOMPolicy=stop" in unit
    assert "/opt/JATO_Analysis_System-main" not in unit

    slot_example = SLOT_ENV_EXAMPLE.read_text(encoding="utf-8")
    for required in (
        "APP_RELEASE_SLOT=__SLOT__",
        "APP_RELEASE_SHA=__RELEASE_SHA__",
        "APP_PROJECT_ROOT=/opt/jato/slots/__SLOT__/current",
        "APP_BACKEND_WORKERS=2",
        "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=false",
        "APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED=false",
        "APP_METADATA_PREWARM_ENABLED=false",
        "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED=false",
        "APP_JATO_MONTHLY_EXECUTION_MODE=subprocess",
        "APP_JATO_MONTHLY_ACTIVE_SLOT_FILE=/var/lib/jato-release/active-slot",
        (
            "APP_JATO_MONTHLY_DEPLOYMENT_MARKER="
            "/var/lib/jato-release/deployment-maintenance"
        ),
    ):
        assert required in slot_example


def test_nginx_boot_requires_fail_closed_route_reconciliation() -> None:
    unit = BOOT_RECONCILE_UNIT.read_text(encoding="utf-8")
    dropin = NGINX_BOOT_RECONCILE_DROPIN.read_text(encoding="utf-8")

    assert "Type=oneshot" in unit
    assert "Before=nginx.service" in unit
    assert "RemainAfterExit=yes" in unit
    assert "TimeoutStartSec=60" in unit
    assert (
        "ExecStart=/usr/bin/python3 -B "
        "/usr/local/libexec/jato-bluegreen-boot-reconcile.py"
    ) in unit
    assert (
        "--nginx-active-release-conf "
        "/etc/jato-fullstack/nginx/active-release.conf"
    ) in unit
    assert (
        "--active-slot-file /var/lib/jato-release/active-slot"
    ) in unit
    assert "WantedBy=multi-user.target" in unit

    assert "Requires=jato-bluegreen-boot-reconcile.service" in dropin
    assert "After=jato-bluegreen-boot-reconcile.service" in dropin


def test_nginx_template_binds_backend_and_frontend_through_one_include() -> None:
    template = NGINX_TEMPLATE.read_text(encoding="utf-8")

    assert template.count(
        "include /etc/jato-fullstack/nginx/active-release.conf;"
    ) == 1
    assert "upstream jato_fullstack_api" not in template
    assert "root $jato_frontend_root;" in template
    assert "location = /readyz" in template
    assert "proxy_pass http://jato_fullstack_api/readyz;" in template
    assert "if (-f /var/lib/jato-release/deployment-maintenance)" in template
    assert "/run/jato/deployment-maintenance" not in template
    assert "__BACKEND_PORT__" not in template
    assert "__FRONTEND_ROOT__" not in template


def test_installer_creates_atomic_release_include_and_reloads_nginx(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, command_log = _fake_runtime(tmp_path)
    default_available = nginx_etc / "sites-available/default"
    default_enabled = nginx_etc / "sites-enabled/default"
    default_available.write_text("server { listen 80 default_server; }\n", encoding="utf-8")
    default_enabled.symlink_to(default_available)

    result = _run_installer(env)

    assert result.returncode == 0, result.stderr + result.stdout
    site = (
        nginx_etc / "sites-available/jato_fullstack.conf"
    ).read_text(encoding="utf-8")
    active = (
        jato_etc / "nginx/active-release.conf"
    ).read_text(encoding="utf-8")
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    commands = command_log.read_text(encoding="utf-8")

    assert "include /etc/jato-fullstack/nginx/active-release.conf;" in site
    assert "root $jato_frontend_root;" in site
    assert "location = /readyz" in site
    assert "server 127.0.0.1:8001" in active
    assert "listen 127.0.0.1:18000;" in active
    assert "proxy_pass http://jato_fullstack_api;" in active
    assert "location ^~ /v1/msrp/monthly-update" in active
    assert "if (-f /var/lib/jato-release/deployment-maintenance)" in active
    assert (
        'default "/opt/jato/slots/8001/current/'
        '06_AppPlatform/frontend/dist";'
    ) in active
    assert enabled.is_symlink()
    assert enabled.readlink() == nginx_etc / "sites-available/jato_fullstack.conf"
    assert not default_enabled.exists()
    assert "nginx -t" in commands
    assert "systemctl reload nginx" in commands
    assert commands.index("nginx -t") < commands.index("systemctl reload nginx")
    assert "systemctl restart nginx" not in commands


def test_installer_adopts_identical_regular_enabled_site(
    tmp_path: Path,
) -> None:
    env, nginx_etc, _, _ = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    original = _certbot_site()
    original_sha256 = hashlib.sha256(original.encode()).hexdigest()
    target.write_text(original, encoding="utf-8")
    enabled.write_text(original, encoding="utf-8")

    result = _run_installer(env)

    assert result.returncode == 0, result.stderr + result.stdout
    assert enabled.is_symlink()
    assert enabled.readlink() == target
    assert f"Approved one-time regular enabled-site adoption: sha256={original_sha256}" in result.stdout
    assert f"Revalidated identical regular enabled/canonical nginx files: sha256={original_sha256}" in result.stdout
    assert "include /etc/jato-fullstack/nginx/active-release.conf;" in target.read_text(
        encoding="utf-8"
    )
    assert target.stat().st_mode & 0o777 == 0o644


def test_installer_candidate_modes_are_deterministic_under_restrictive_umask(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    target.write_text(_certbot_site(), encoding="utf-8")
    enabled.write_text(_certbot_site(), encoding="utf-8")

    result = subprocess.run(
        [
            "bash",
            "-c",
            'umask 077; exec bash "$1"',
            "jato-nginx-umask-test",
            str(NGINX_INSTALLER),
        ],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert target.stat().st_mode & 0o777 == 0o644
    active = jato_etc / "nginx/active-release.conf"
    assert active.stat().st_mode & 0o777 == 0o644


def test_installer_rejects_oversized_regular_enabled_adoption_inputs(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, command_log = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    oversized = b"x" * (4 * 1024 * 1024 + 1)
    target.write_bytes(oversized)
    enabled.write_bytes(oversized)

    result = _run_installer(env)

    assert result.returncode != 0
    assert "exceeds 4194304 bytes" in result.stderr
    assert not (jato_etc / "nginx/active-release.conf").exists()
    assert "nginx -t" not in command_log.read_text(encoding="utf-8")


def test_installer_rejects_mismatched_regular_enabled_site_with_redacted_report(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, command_log = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    canonical = _certbot_site()
    divergent = canonical.replace(
        "server_name www.ojeur.cloud ojeur.cloud;",
        "server_name unexpected.example; # Authorization: secret-sentinel",
        1,
    )
    target.write_text(canonical, encoding="utf-8")
    target.chmod(0o640)
    enabled.write_text(divergent, encoding="utf-8")
    enabled.chmod(0o600)
    preimage = tmp_path / "preimages/mismatch"
    env["NGINX_PREIMAGE_DIR"] = str(preimage)

    result = _run_installer(env)

    assert result.returncode != 0
    assert "Enabled nginx site differs from canonical target" in result.stderr
    assert f"enabled_path={enabled}" in result.stderr
    assert f"canonical_path={target}" in result.stderr
    assert (
        f"enabled_sha256={hashlib.sha256(divergent.encode()).hexdigest()}"
        in result.stderr
    )
    assert (
        f"canonical_sha256={hashlib.sha256(canonical.encode()).hexdigest()}"
        in result.stderr
    )
    assert "difference_content=redacted" in result.stderr
    assert "first_difference_byte=" in result.stderr
    assert "first_difference_line=" in result.stderr
    assert "unexpected.example" not in result.stderr
    assert "secret-sentinel" not in result.stderr
    assert "canonical_block_sha256" not in result.stderr
    assert "difflib" not in NGINX_INSTALLER.read_text(encoding="utf-8")
    assert target.is_file() and not target.is_symlink()
    assert target.read_text(encoding="utf-8") == canonical
    assert target.stat().st_mode & 0o777 == 0o640
    assert enabled.is_file() and not enabled.is_symlink()
    assert enabled.read_text(encoding="utf-8") == divergent
    assert enabled.stat().st_mode & 0o777 == 0o600
    assert not (jato_etc / "nginx/active-release.conf").exists()
    assert not preimage.exists()
    commands = command_log.read_text(encoding="utf-8")
    assert "nginx -t" not in commands
    assert "systemctl reload nginx" not in commands


def test_installer_restores_regular_enabled_site_after_nginx_failure(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(
        tmp_path,
        fail_nginx_test=True,
    )
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    active = jato_etc / "nginx/active-release.conf"
    active.parent.mkdir(parents=True)
    original = _certbot_site()
    original_active = "upstream old { server 127.0.0.1:8000; }\n"
    target.write_text(original, encoding="utf-8")
    target.chmod(0o640)
    enabled.write_text(original, encoding="utf-8")
    enabled.chmod(0o600)
    active.write_text(original_active, encoding="utf-8")
    active.chmod(0o640)

    result = _run_installer(env)

    assert result.returncode != 0
    assert target.is_file() and not target.is_symlink()
    assert target.read_text(encoding="utf-8") == original
    assert target.stat().st_mode & 0o777 == 0o640
    assert enabled.is_file() and not enabled.is_symlink()
    assert enabled.read_text(encoding="utf-8") == original
    assert enabled.stat().st_mode & 0o777 == 0o600
    assert active.read_text(encoding="utf-8") == original_active
    assert active.stat().st_mode & 0o777 == 0o640


def test_installer_rejects_mode_drift_after_regular_enabled_snapshot(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, command_log = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    original = _certbot_site()
    target.write_text(original, encoding="utf-8")
    target.chmod(0o640)
    enabled.write_text(original, encoding="utf-8")
    enabled.chmod(0o600)
    fake_bin = Path(env["PATH"].split(os.pathsep, maxsplit=1)[0])
    env["REAL_CP"] = shutil.which("cp") or "/bin/cp"
    env["ENABLED_PATH"] = str(enabled)
    _write_executable(
        fake_bin / "cp",
        "#!/usr/bin/env bash\n"
        '"$REAL_CP" "$@" || exit $?\n'
        'for argument in "$@"; do\n'
        '  if [[ "$argument" == "$ENABLED_PATH" ]]; then\n'
        '    chmod 0640 "$ENABLED_PATH"\n'
        "  fi\n"
        "done\n",
    )

    result = _run_installer(env)

    assert result.returncode != 0
    assert "nginx adoption inputs changed after validation" in result.stderr
    assert "enabled_mode=0640 enabled_snapshot_mode=0600" in result.stderr
    assert target.read_text(encoding="utf-8") == original
    assert target.stat().st_mode & 0o777 == 0o640
    assert enabled.read_text(encoding="utf-8") == original
    assert enabled.stat().st_mode & 0o777 == 0o640
    assert not (jato_etc / "nginx/active-release.conf").exists()
    assert "nginx -t" not in command_log.read_text(encoding="utf-8")


def test_installer_does_not_overwrite_enabled_drift_before_symlink(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, command_log = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    active = jato_etc / "nginx/active-release.conf"
    active.parent.mkdir(parents=True)
    original = _certbot_site()
    original_active = "upstream old { server 127.0.0.1:8000; }\n"
    external_enabled = "server { # external-certbot-drift }\n"
    target.write_text(original, encoding="utf-8")
    target.chmod(0o640)
    enabled.write_text(original, encoding="utf-8")
    enabled.chmod(0o600)
    active.write_text(original_active, encoding="utf-8")
    active.chmod(0o640)
    preimage = tmp_path / "preimages/final-drift"
    env["NGINX_PREIMAGE_DIR"] = str(preimage)
    fake_bin = Path(env["PATH"].split(os.pathsep, maxsplit=1)[0])
    env["REAL_INSTALL"] = shutil.which("install") or "/usr/bin/install"
    env["ENABLED_PATH"] = str(enabled)
    env["EXTERNAL_ENABLED"] = external_enabled
    _write_executable(
        fake_bin / "install",
        "#!/usr/bin/env bash\n"
        '"$REAL_INSTALL" "$@" || exit $?\n'
        'destination="${@: -1}"\n'
        'if [[ "$destination" == *"sites-available/.jato_fullstack.conf."* ]]; then\n'
        '  printf "%s" "$EXTERNAL_ENABLED" > "$ENABLED_PATH"\n'
        '  chmod 0600 "$ENABLED_PATH"\n'
        "fi\n",
    )

    result = _run_installer(env)

    assert result.returncode == 90
    assert "changed at atomic exchange" in result.stderr
    assert "intentionally not overwritten during rollback" in result.stderr
    assert enabled.is_file() and not enabled.is_symlink()
    assert enabled.read_text(encoding="utf-8") == external_enabled
    assert target.read_text(encoding="utf-8") == original
    assert target.stat().st_mode & 0o777 == 0o640
    assert active.read_text(encoding="utf-8") == original_active
    assert active.stat().st_mode & 0o777 == 0o640
    assert (preimage / "enabled.conf").read_text(encoding="utf-8") == original
    assert "nginx -t" not in command_log.read_text(encoding="utf-8")


def test_installer_preserves_external_changes_to_paths_not_yet_mutated(
    tmp_path: Path,
) -> None:
    env, nginx_etc, _, command_log = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    default = nginx_etc / "sites-enabled/default"
    original = _certbot_site()
    external_target = "server { # externally-updated-canonical }\n"
    external_default = "server { # externally-updated-default }\n"
    target.write_text(original, encoding="utf-8")
    enabled.write_text(original, encoding="utf-8")
    default.write_text("server { # original-default }\n", encoding="utf-8")
    fake_bin = Path(env["PATH"].split(os.pathsep, maxsplit=1)[0])
    env["REAL_INSTALL"] = shutil.which("install") or "/usr/bin/install"
    env["TARGET_PATH"] = str(target)
    env["DEFAULT_PATH"] = str(default)
    env["EXTERNAL_TARGET"] = external_target
    env["EXTERNAL_DEFAULT"] = external_default
    _write_executable(
        fake_bin / "install",
        "#!/usr/bin/env bash\n"
        '"$REAL_INSTALL" "$@" || exit $?\n'
        'destination="${@: -1}"\n'
        'if [[ "$destination" == *"nginx/.active-release.conf."* ]]; then\n'
        '  printf "%s" "$EXTERNAL_TARGET" > "$TARGET_PATH"\n'
        '  printf "%s" "$EXTERNAL_DEFAULT" > "$DEFAULT_PATH"\n'
        "  exit 42\n"
        "fi\n",
    )

    result = _run_installer(env)

    assert result.returncode != 0
    assert target.read_text(encoding="utf-8") == external_target
    assert default.read_text(encoding="utf-8") == external_default
    assert enabled.read_text(encoding="utf-8") == original
    assert not enabled.is_symlink()
    assert "nginx -t" not in command_log.read_text(encoding="utf-8")


def test_installer_reports_failed_regular_enabled_rollback(
    tmp_path: Path,
) -> None:
    env, nginx_etc, _, _ = _fake_runtime(tmp_path, fail_nginx_test=True)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    original = _certbot_site()
    target.write_text(original, encoding="utf-8")
    enabled.write_text(original, encoding="utf-8")
    fake_bin = Path(env["PATH"].split(os.pathsep, maxsplit=1)[0])
    env["REAL_PYTHON"] = sys.executable
    _write_executable(
        fake_bin / "python3",
        "#!/usr/bin/env bash\n"
        'if [[ " $* " == *" exchange-restore "* ]]; then exit 42; fi\n'
        'exec "$REAL_PYTHON" "$@"\n',
    )

    result = _run_installer(env)

    assert result.returncode == 90
    assert "rollback could not restore the exact original" in result.stderr
    assert "Automatic nginx rollback failed closed" in result.stderr


def test_installer_does_not_overwrite_enabled_drift_during_atomic_rollback(
    tmp_path: Path,
) -> None:
    env, nginx_etc, _, _ = _fake_runtime(tmp_path, fail_nginx_test=True)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    original = _certbot_site()
    external_enabled = "server { # external-during-rollback }\n"
    target.write_text(original, encoding="utf-8")
    enabled.write_text(original, encoding="utf-8")
    fake_bin = Path(env["PATH"].split(os.pathsep, maxsplit=1)[0])
    env["REAL_PYTHON"] = sys.executable
    env["ENABLED_PATH"] = str(enabled)
    env["EXTERNAL_ENABLED"] = external_enabled
    _write_executable(
        fake_bin / "python3",
        "#!/usr/bin/env bash\n"
        'if [[ " $* " == *" exchange-restore "* ]]; then\n'
        '  rm -f "$ENABLED_PATH"\n'
        '  printf "%s" "$EXTERNAL_ENABLED" > "$ENABLED_PATH"\n'
        "fi\n"
        'exec "$REAL_PYTHON" "$@"\n',
    )

    result = _run_installer(env)

    assert result.returncode == 90
    assert "refusing rollback overwrite" in result.stderr
    assert "Automatic nginx rollback failed closed" in result.stderr
    assert enabled.is_file() and not enabled.is_symlink()
    assert enabled.read_text(encoding="utf-8") == external_enabled
    assert target.read_text(encoding="utf-8") == original


def test_certbot_migration_preserves_tls_and_is_idempotent(
    tmp_path: Path,
) -> None:
    env, nginx_etc, _, _ = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    target.write_text(_certbot_site(), encoding="utf-8")
    enabled.symlink_to(target)

    first = _run_installer(env)
    second = _run_installer(env)

    assert first.returncode == 0, first.stderr + first.stdout
    assert second.returncode == 0, second.stderr + second.stdout
    assert "regular enabled-site adoption" not in first.stdout + second.stdout
    migrated = target.read_text(encoding="utf-8")
    assert migrated.count(
        "include /etc/jato-fullstack/nginx/active-release.conf;"
    ) == 1
    assert "upstream jato_fullstack_api" not in migrated
    assert migrated.count("location = /readyz") == 1
    assert "root $jato_frontend_root;" in migrated
    assert (
        "ssl_certificate /etc/letsencrypt/live/ojeur.cloud/fullchain.pem; "
        "# managed by Certbot"
    ) in migrated
    assert (
        "ssl_certificate_key /etc/letsencrypt/live/ojeur.cloud/privkey.pem; "
        "# managed by Certbot"
    ) in migrated


def test_certbot_migration_upgrades_legacy_ephemeral_monthly_gate(
    tmp_path: Path,
) -> None:
    env, nginx_etc, _, _ = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    legacy_gate = """    location ^~ /v1/msrp/monthly-update {
        if (-f /run/jato/deployment-maintenance) {
            return 423;
        }
        proxy_pass http://jato_fullstack_api;
    }

"""
    target.write_text(
        _certbot_site().replace(
            "    location ^~ /v1/ {",
            legacy_gate + "    location ^~ /v1/ {",
        ),
        encoding="utf-8",
    )
    enabled.symlink_to(target)

    result = _run_installer(env)

    assert result.returncode == 0, result.stderr + result.stdout
    migrated = target.read_text(encoding="utf-8")
    assert "if (-f /var/lib/jato-release/deployment-maintenance)" in migrated
    assert "/run/jato/deployment-maintenance" not in migrated


def test_nginx_validation_failure_restores_certbot_site_and_active_include(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(
        tmp_path,
        fail_nginx_test=True,
    )
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    active = jato_etc / "nginx/active-release.conf"
    active.parent.mkdir(parents=True)
    original_site = _certbot_site()
    original_active = """upstream jato_fullstack_api {
    server 127.0.0.1:8000;
}
map $host $jato_frontend_root {
    default "/srv/old-frontend";
}
"""
    target.write_text(original_site, encoding="utf-8")
    active.write_text(original_active, encoding="utf-8")
    enabled.symlink_to(target)
    default_available = nginx_etc / "sites-available/default"
    default_enabled = nginx_etc / "sites-enabled/default"
    default_available.write_text("server { listen 80 default_server; }\n", encoding="utf-8")
    default_enabled.symlink_to(default_available)

    result = _run_installer(env)

    assert result.returncode != 0
    assert target.read_text(encoding="utf-8") == original_site
    assert active.read_text(encoding="utf-8") == original_active
    assert enabled.is_symlink()
    assert enabled.readlink() == target
    assert default_enabled.is_symlink()
    assert default_enabled.readlink() == default_available


def test_installer_rejects_unsafe_release_values_before_mutation(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(tmp_path)
    env["FRONTEND_ROOT"] = "/opt/jato/release; include /tmp/unsafe"

    result = _run_installer(env)

    assert result.returncode != 0
    assert not (nginx_etc / "sites-available/jato_fullstack.conf").exists()
    assert not (jato_etc / "nginx/active-release.conf").exists()


def test_msrp_scheduler_uses_stable_loopback_nginx_entry() -> None:
    env_example = MSRP_ENV_EXAMPLE.read_text(encoding="utf-8")

    assert "JATO_API_BASE=http://127.0.0.1:18000/v1" in env_example
    assert "JATO_API_BASE=http://127.0.0.1:8000/v1" not in env_example


def test_https_bluegreen_mode_preserves_active_include_and_checks_all_domains(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    releases_root = tmp_path / "releases"
    slots_root = tmp_path / "slots"
    active_root = releases_root / "release-a"
    frontend = active_root / "06_AppPlatform/frontend/dist"
    active_link = tmp_path / "active"
    active_conf = tmp_path / "active-release.conf"
    site_conf = tmp_path / "jato_fullstack.conf"
    fake_bin = tmp_path / "bin"
    command_log = tmp_path / "commands.log"
    fake_installer = tmp_path / "unexpected-installer"

    frontend.mkdir(parents=True)
    (slots_root / "8001").mkdir(parents=True)
    state_root.mkdir()
    fake_bin.mkdir()
    active_link.symlink_to(active_root)
    (slots_root / "8001/current").symlink_to(active_root)
    (state_root / "active-slot").write_text("8001\n", encoding="utf-8")
    active_payload = f"""upstream jato_fullstack_api {{
    server 127.0.0.1:8001 max_fails=3 fail_timeout=30s;
}}
map $host $jato_frontend_root {{
    default "{frontend}";
}}
server {{
    listen 127.0.0.1:18000;
}}
"""
    active_conf.write_text(active_payload, encoding="utf-8")
    site_conf.write_text(
        """include /etc/jato-fullstack/nginx/active-release.conf;
server {
    root $jato_frontend_root;
    location ^~ /v1/msrp/monthly-update {
        if (-f /var/lib/jato-release/deployment-maintenance) { return 423; }
    }
}
""",
        encoding="utf-8",
    )
    for command in ("apt-get", "certbot", "nginx", "curl"):
        _write_executable(
            fake_bin / command,
            "#!/usr/bin/env bash\n"
            f'printf "{command} %s\\n" "$*" >> "$COMMAND_LOG"\n'
            "exit 0\n",
        )
    _write_executable(
        fake_bin / "systemctl",
        "#!/usr/bin/env bash\n"
        'printf "systemctl %s\\n" "$*" >> "$COMMAND_LOG"\n'
        'if [[ "${1:-}" == "show" && "$*" == *"LoadState"* ]]; then\n'
        "  printf 'not-found\\n'\n"
        "elif [[ \"${1:-}\" == \"show\" && \"$*\" == *\"ActiveState\"* ]]; then\n"
        "  printf 'inactive\\n'\n"
        "elif [[ \"${1:-}\" == \"show\" && \"$*\" == *\"SubState\"* ]]; then\n"
        "  printf 'dead\\n'\n"
        "fi\n"
        "exit 0\n",
    )
    _write_executable(
        fake_bin / "flock",
        "#!/usr/bin/env bash\n"
        'if [[ "${1:-}" == "-n" ]]; then exit 1; fi\n'
        "exit 0\n",
    )
    _write_executable(
        fake_bin / "realpath",
        "#!/usr/bin/env python3\n"
        "import os, sys\n"
        "print(os.path.abspath(sys.argv[-1]))\n",
    )
    _write_executable(
        fake_installer,
        "#!/usr/bin/env bash\n"
        'printf "installer-called\\n" >> "$COMMAND_LOG"\n'
        "exit 99\n",
    )
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "COMMAND_LOG": str(command_log),
        "SERVER_NAME": "ojeur.cloud www.ojeur.cloud",
        "BLUEGREEN_STATE_ROOT": str(state_root),
        "ACTIVE_SLOT_FILE": str(state_root / "active-slot"),
        "DEPLOYMENT_MARKER": str(state_root / "deployment-maintenance"),
        "ACTIVE_RELEASE_LINK": str(active_link),
        "SLOTS_ROOT": str(slots_root),
        "RELEASES_ROOT": str(releases_root),
        "ACTIVE_RELEASE_CONF": str(active_conf),
        "NGINX_SITE_CONF": str(site_conf),
        "NGINX_INSTALL_SCRIPT": str(fake_installer),
        "JATO_PRODUCTION_DEPLOY_LOCK_PATH": str(
            tmp_path / "production-deploy.lock"
        ),
        "JATO_PRODUCTION_DEPLOY_LOCK_WAIT": "0",
    }

    result = subprocess.run(
        ["bash", str(HTTPS_INSTALLER)],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    commands = command_log.read_text(encoding="utf-8")
    assert "installer-called" not in commands
    assert "--resolve ojeur.cloud:443:127.0.0.1" in commands
    assert "--resolve www.ojeur.cloud:443:127.0.0.1" in commands
    assert active_conf.read_text(encoding="utf-8") == active_payload


def test_legacy_data_sync_rejects_bluegreen_before_local_etl(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    command_log = tmp_path / "commands.log"
    input_path = tmp_path / "candidate.xlsx"
    fake_bin.mkdir()
    input_path.write_bytes(b"not-read-before-bluegreen-rejection")
    _write_executable(
        fake_bin / "ssh",
        "#!/usr/bin/env bash\n"
        'printf "ssh %s\\n" "$*" >> "$COMMAND_LOG"\n'
        'if [[ "$*" == *"echo ok"* ]]; then exit 0; fi\n'
        "printf 'bluegreen:8001\\n'\n",
    )
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "COMMAND_LOG": str(command_log),
        "SSH_ALIAS": "fake-cloud",
    }

    result = subprocess.run(
        ["bash", str(SYNC_DATA_TO_CLOUD), str(input_path)],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "禁止此脚本直接覆盖 active JATO 数据" in result.stdout
    assert "Step 1" not in result.stdout
    assert input_path.is_file()


def test_msrp_db_sync_rejects_an_explicit_inactive_slot(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    command_log = tmp_path / "commands.log"
    fake_bin.mkdir()
    _write_executable(
        fake_bin / "ssh",
        "#!/usr/bin/env bash\n"
        'printf "ssh %s\\n" "$*" >> "$COMMAND_LOG"\n'
        "printf 'bluegreen\\t8001\\n'\n",
    )
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "COMMAND_LOG": str(command_log),
        "SSH_ALIAS": "fake-cloud",
        "REMOTE_BACKEND_SERVICE": "jato-fullstack-backend@8000",
    }

    result = subprocess.run(
        ["bash", str(SYNC_MSRP_DB_TO_CLOUD)],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "指向 inactive/错误槽" in result.stdout


def test_msrp_db_sync_revalidates_active_slot_before_service_restart() -> None:
    script = SYNC_MSRP_DB_TO_CLOUD.read_text(encoding="utf-8")

    assert 'REMOTE_BACKEND_SERVICE="${REMOTE_BACKEND_SERVICE:-}"' in script
    assert 'REMOTE_BACKEND_PORT="${REMOTE_BACKEND_PORT:-}"' in script
    assert 'REMOTE_BACKEND_SERVICE="jato-fullstack-backend@$detected_slot"' in script
    assert 'REMOTE_BACKEND_PORT="$detected_slot"' in script
    assert "verify_backend_target" in script
    assert 'refusing to stop or restart an inactive blue/green slot' in script
    assert 'systemctl stop "\\$BACKEND_SERVICE"' in script
    assert 'systemctl start "\\$BACKEND_SERVICE"' in script


def test_installer_rejects_dangling_managed_file_symlinks(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(tmp_path)
    active = jato_etc / "nginx/active-release.conf"
    active.parent.mkdir(parents=True)
    active.symlink_to(tmp_path / "missing-active-release.conf")

    result = _run_installer(env)

    assert result.returncode != 0
    assert active.is_symlink()
    assert not (nginx_etc / "sites-available/jato_fullstack.conf").exists()


def test_host_mutations_share_the_production_lock_and_switch_unit_fence() -> None:
    helper = PRODUCTION_MUTATION_LOCK.read_text(encoding="utf-8")
    installer = NGINX_INSTALLER.read_text(encoding="utf-8")
    https = HTTPS_INSTALLER.read_text(encoding="utf-8")
    sync = SYNC_MSRP_DB_TO_CLOUD.read_text(encoding="utf-8")
    legacy_sync = SYNC_DATA_TO_CLOUD.read_text(encoding="utf-8")
    outer = REMOTE_RELEASE.read_text(encoding="utf-8")
    controller = BLUEGREEN_CONTROLLER.read_text(encoding="utf-8")

    assert "JATO_PRODUCTION_DEPLOY_LOCK_FD=9" in helper
    assert "jato_assert_safe_lock_parent_components" in helper
    assert "os.lstat(cursor)" in helper
    assert "jato_validate_inherited_production_lock" in helper
    assert "jato_pid_is_self_or_ancestor" in helper
    assert 'holder_fd_path="/proc/$holder_pid/fd/$JATO_PRODUCTION_DEPLOY_LOCK_FD"' in helper
    assert 'if flock -n 8; then' in helper
    assert "jato_assert_no_active_bluegreen_switch" in helper
    assert "ActiveState" in helper
    assert "SubState" in helper
    assert "jato_acquire_production_mutation_lock" in installer
    assert "jato_acquire_production_mutation_lock" in https
    assert "acquire_production_mutation_lock" in sync
    assert "assert_bluegreen_switch_quiescent" in sync
    assert 'DEPLOY_LOCK_PATH="$DEPLOY_STATE_DIR/production-deploy.lock"' in outer
    for token in (
        "JATO_PRODUCTION_DEPLOY_LOCK_PATH",
        "DEPLOY_LOCK_HELD",
        "DEPLOY_LOCK_HOLDER_PID",
        "DEPLOY_LOCK_FD",
    ):
        assert token in controller

    remote_mutation = sync.index("trap ensure_backend_started EXIT")
    lock = sync.index("acquire_production_mutation_lock", remote_mutation)
    reverify = sync.index("verify_backend_target", lock)
    stop = sync.index('systemctl stop "\\$BACKEND_SERVICE"', reverify)
    restore = sync.index("pg_restore", stop)
    start = sync.index('systemctl start "\\$BACKEND_SERVICE"', restore)
    health = sync.index('wait_for_http_ok "http://127.0.0.1:', start)
    assert lock < reverify < stop < restore < start < health

    legacy_remote = legacy_sync.index("remote_apply_legacy_payload()")
    legacy_lock = legacy_sync.index('exec 9>"$lock_path"', legacy_remote)
    legacy_flock = legacy_sync.index('flock -w "$lock_wait" 9', legacy_lock)
    legacy_unit = legacy_sync.index("ActiveState", legacy_flock)
    legacy_mode_recheck = legacy_sync.index(
        '[[ -e "$active_slot_file" || -L "$active_slot_file" ]]',
        legacy_unit,
    )
    legacy_receive = legacy_sync.index('cat > "$archive"', legacy_mode_recheck)
    legacy_replace = legacy_sync.index('tar xzf "$archive" -C "$data_dir"', legacy_receive)
    legacy_restart = legacy_sync.index(
        'systemctl restart "$backend_service"',
        legacy_replace,
    )
    legacy_health = legacy_sync.index(
        "legacy backend failed health",
        legacy_restart,
    )
    assert (
        legacy_lock
        < legacy_flock
        < legacy_unit
        < legacy_mode_recheck
        < legacy_receive
        < legacy_replace
        < legacy_restart
        < legacy_health
    )
    assert '"$SSH_ALIAS:/tmp/jato_data_sync.tar.gz"' not in legacy_sync
    assert '"$REMOTE_APPLY_COMMAND" < "$ARCHIVE_PATH"' in legacy_sync


def test_legacy_sync_remote_apply_rejects_bluegreen_before_receiving_payload(
    tmp_path: Path,
) -> None:
    env, _, _, _ = _fake_runtime(tmp_path)
    data_dir = tmp_path / "legacy-data"
    state_root = tmp_path / "bluegreen-state"
    state_root.mkdir()
    active_slot = state_root / "active-slot"
    active_slot.write_text("8000\n", encoding="utf-8")
    marker = state_root / "deployment-maintenance"
    active_release = tmp_path / "active"
    deploy_state = tmp_path / "deploy-state"

    result = subprocess.run(
        [
            "bash",
            str(SYNC_DATA_TO_CLOUD),
            "--remote-apply",
            str(data_dir),
            "jato-fullstack-backend@8000",
            str(active_slot),
            str(marker),
            str(active_release),
            str(deploy_state),
            "jato-bluegreen-production.service",
            "0",
        ],
        cwd=REPO_ROOT,
        env=env,
        input=b"archive bytes must not be consumed",
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert b"changed to blue/green" in result.stderr
    assert not data_dir.exists()
    assert not list(tmp_path.glob("jato_data_sync*.tar.gz"))


def test_installer_fails_closed_while_bluegreen_unit_is_transitioning(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, command_log = _fake_runtime(tmp_path)
    env.update(
        {
            "SWITCH_LOAD_STATE": "loaded",
            "SWITCH_ACTIVE_STATE": "activating",
            "SWITCH_SUB_STATE": "start",
        }
    )

    result = _run_installer(env)

    assert result.returncode != 0
    assert "not quiescent" in result.stderr
    assert not (nginx_etc / "sites-available/jato_fullstack.conf").exists()
    assert not (jato_etc / "nginx/active-release.conf").exists()
    commands = command_log.read_text(encoding="utf-8")
    assert "nginx -t" not in commands


def test_installer_rejects_any_symlink_in_lock_path_ancestors(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(tmp_path)
    real_parent = tmp_path / "real-lock-parent"
    linked_parent = tmp_path / "linked-lock-parent"
    real_parent.mkdir()
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    env["JATO_PRODUCTION_DEPLOY_LOCK_PATH"] = str(
        linked_parent / "state/production-deploy.lock"
    )

    result = _run_installer(env)

    assert result.returncode != 0
    assert "must not be a symlink" in result.stderr
    assert not (real_parent / "state").exists()
    assert not (nginx_etc / "sites-available/jato_fullstack.conf").exists()
    assert not (jato_etc / "nginx/active-release.conf").exists()


def test_installer_fails_closed_when_flock_cannot_acquire(tmp_path: Path) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(tmp_path)
    _write_executable(
        Path(env["PATH"].split(os.pathsep, maxsplit=1)[0]) / "flock",
        "#!/usr/bin/env bash\nexit 127\n",
    )

    result = _run_installer(env)

    assert result.returncode != 0
    assert "server-wide deploy lock" in result.stderr
    assert not (nginx_etc / "sites-available/jato_fullstack.conf").exists()
    assert not (jato_etc / "nginx/active-release.conf").exists()


def test_installer_persists_and_restores_exact_durable_preimage(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    default_available = nginx_etc / "sites-available/default"
    default_enabled = nginx_etc / "sites-enabled/default"
    active = jato_etc / "nginx/active-release.conf"
    active.parent.mkdir(parents=True)
    old_target = "server { listen 80; server_name old.example; }\n"
    old_active = "upstream jato_fullstack_api { server 127.0.0.1:8000; }\n"
    target.write_text(old_target, encoding="utf-8")
    target.chmod(0o640)
    active.write_text(old_active, encoding="utf-8")
    active.chmod(0o600)
    enabled.symlink_to(target)
    default_available.write_text("server { listen 80 default_server; }\n", encoding="utf-8")
    default_enabled.symlink_to(default_available)
    preimage = tmp_path / "preimages/release-a"
    env["NGINX_PREIMAGE_DIR"] = str(preimage)

    installed = _run_installer(env)
    assert installed.returncode == 0, installed.stderr + installed.stdout
    assert (preimage / "manifest.json").is_file()

    target.write_text("newer target\n", encoding="utf-8")
    active.write_text("newer active\n", encoding="utf-8")
    enabled.unlink()
    enabled.symlink_to(tmp_path / "wrong-site")
    default_enabled.unlink(missing_ok=True)

    restored = subprocess.run(
        ["bash", str(NGINX_INSTALLER), "restore-preimage"],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert restored.returncode == 0, restored.stderr + restored.stdout
    assert target.read_text(encoding="utf-8") == old_target
    assert active.read_text(encoding="utf-8") == old_active
    assert target.stat().st_mode & 0o777 == 0o640
    assert active.stat().st_mode & 0o777 == 0o600
    assert enabled.is_symlink() and enabled.readlink() == target
    assert default_enabled.is_symlink()
    assert default_enabled.readlink() == default_available


def test_durable_preimage_restores_regular_enabled_site(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(tmp_path)
    target = nginx_etc / "sites-available/jato_fullstack.conf"
    enabled = nginx_etc / "sites-enabled/jato_fullstack.conf"
    active = jato_etc / "nginx/active-release.conf"
    active.parent.mkdir(parents=True)
    original = _certbot_site()
    original_active = "upstream old { server 127.0.0.1:8000; }\n"
    target.write_text(original, encoding="utf-8")
    target.chmod(0o640)
    enabled.write_text(original, encoding="utf-8")
    enabled.chmod(0o600)
    active.write_text(original_active, encoding="utf-8")
    active.chmod(0o640)
    preimage = tmp_path / "preimages/regular-enabled"
    env["NGINX_PREIMAGE_DIR"] = str(preimage)

    installed = _run_installer(env)

    assert installed.returncode == 0, installed.stderr + installed.stdout
    manifest = json.loads((preimage / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["enabled"]["kind"] == "file"
    assert manifest["enabled"]["backup"] == "enabled.conf"
    assert manifest["enabled"]["mode"] == 0o600
    assert (preimage / "enabled.conf").read_text(encoding="utf-8") == original

    target.write_text("newer target\n", encoding="utf-8")
    active.write_text("newer active\n", encoding="utf-8")
    enabled.unlink()
    enabled.write_text("newer enabled\n", encoding="utf-8")
    restored = subprocess.run(
        ["bash", str(NGINX_INSTALLER), "restore-preimage"],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert restored.returncode == 0, restored.stderr + restored.stdout
    assert target.read_text(encoding="utf-8") == original
    assert target.stat().st_mode & 0o777 == 0o640
    assert active.read_text(encoding="utf-8") == original_active
    assert active.stat().st_mode & 0o777 == 0o640
    assert enabled.is_file() and not enabled.is_symlink()
    assert enabled.read_text(encoding="utf-8") == original
    assert enabled.stat().st_mode & 0o777 == 0o600


@pytest.mark.skipif(
    sys.platform != "linux" or shutil.which("flock") is None,
    reason="requires Linux /proc lock-holder validation and util-linux flock",
)
def test_nested_installer_reuses_only_a_proven_ancestor_lock(tmp_path: Path) -> None:
    env, nginx_etc, _, _ = _fake_runtime(tmp_path)
    fake_bin = Path(env["PATH"].split(os.pathsep, maxsplit=1)[0])
    (fake_bin / "flock").unlink()
    lock_path = env["JATO_PRODUCTION_DEPLOY_LOCK_PATH"]
    result = subprocess.run(
        [
            "bash",
            "-c",
            'exec 9>"$JATO_PRODUCTION_DEPLOY_LOCK_PATH"\n'
            "flock -w 1 9\n"
            "export DEPLOY_LOCK_HELD=1 DEPLOY_LOCK_FD=9\n"
            "DEPLOY_LOCK_HOLDER_PID=$$\n"
            "export DEPLOY_LOCK_HOLDER_PID\n"
            'exec bash "$INSTALLER"\n',
        ],
        cwd=REPO_ROOT,
        env={**env, "INSTALLER": str(NGINX_INSTALLER), "LOCK_PATH": lock_path},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert (nginx_etc / "sites-available/jato_fullstack.conf").is_file()


@pytest.mark.skipif(
    sys.platform != "linux" or shutil.which("flock") is None,
    reason="requires util-linux flock",
)
def test_concurrent_installer_is_blocked_by_production_deploy_lock(
    tmp_path: Path,
) -> None:
    env, nginx_etc, jato_etc, _ = _fake_runtime(tmp_path)
    fake_bin = Path(env["PATH"].split(os.pathsep, maxsplit=1)[0])
    (fake_bin / "flock").unlink()
    lock_path = env["JATO_PRODUCTION_DEPLOY_LOCK_PATH"]
    ready = tmp_path / "holder.ready"
    holder = subprocess.Popen(
        [
            "bash",
            "-c",
            'exec 9>"$JATO_PRODUCTION_DEPLOY_LOCK_PATH"\n'
            "flock -w 1 9\n"
            ': > "$READY"\n'
            "sleep 10\n",
        ],
        env={**env, "READY": str(ready)},
        text=True,
    )
    try:
        for _ in range(100):
            if ready.exists():
                break
            holder.poll()
            if holder.returncode is not None:
                break
            import time

            time.sleep(0.02)
        assert ready.exists()
        result = _run_installer(env)
    finally:
        holder.terminate()
        holder.wait(timeout=5)

    assert result.returncode != 0
    assert "server-wide deploy lock" in result.stderr
    assert not (nginx_etc / "sites-available/jato_fullstack.conf").exists()
    assert not (jato_etc / "nginx/active-release.conf").exists()
