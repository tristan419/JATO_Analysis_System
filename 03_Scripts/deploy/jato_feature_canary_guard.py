#!/usr/bin/env python3
"""Capture and compare the production invariants around a feature canary.

The feature canary intentionally has no route-switch capability.  This helper
keeps the host observations and durable receipt machine-readable while the
shell controller owns the transient systemd lifecycle.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import socket
import stat
import subprocess
import tempfile
from typing import Any
from urllib.parse import urlsplit


SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
ARCHIVE_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MAX_HTTP_BYTES = 256 * 1024
EXPECTED_ACTIVE_MEMORY_HIGH = 6 * 1024 * 1024 * 1024
EXPECTED_ACTIVE_MEMORY_MAX = 8 * 1024 * 1024 * 1024

PRODUCTION_PATHS = (
    "/var/lib/jato-release/active-slot",
    "/var/lib/jato-release/deployment-maintenance",
    "/opt/jato/active",
    "/etc/jato-fullstack/nginx/active-release.conf",
    "/etc/systemd/system/jato-fullstack-backend@.service",
    "/etc/systemd/system/jato-fullstack-backend@8000.service",
    "/etc/systemd/system/jato-fullstack-backend@8001.service",
    "/etc/systemd/system/jato-fullstack-backend@.service.d",
    "/etc/systemd/system/jato-fullstack-backend@8000.service.d",
    "/etc/systemd/system/jato-fullstack-backend@8001.service.d",
    "/etc/jato-fullstack/slots/8000.env",
    "/etc/jato-fullstack/slots/8001.env",
    "/etc/jato-fullstack/backend.env",
)
OBSERVED_UNITS = (
    "jato-fullstack-backend@8000.service",
    "jato-fullstack-backend@8001.service",
    "jato-monthly-worker.service",
    "jato-country-news-sync.timer",
    "jato-country-news-sync-b.timer",
    "jato-msrp-dryrun.timer",
    "jato-msrp-ingest.timer",
    "jato-voc-forum-sync.timer",
    "hermes-source-quality.timer",
    "jato-country-news-sync.service",
    "jato-country-news-sync-b.service",
    "jato-msrp-sync@dryrun.service",
    "jato-msrp-sync@ingest.service",
    "jato-voc-forum-sync.service",
    "hermes-source-quality.service",
)
UNIT_PROPERTIES = (
    "LoadState",
    "ActiveState",
    "SubState",
    "UnitFileState",
    "FragmentPath",
    "MainPID",
    "ExecStart",
    "MemoryHigh",
    "MemoryMax",
    "TasksMax",
    "ControlGroup",
)


class CanaryGuardError(RuntimeError):
    """A fail-closed feature-canary invariant violation."""


def _run(
    command: list[str],
    *,
    timeout: float = 20,
    allow_failure: bool = False,
) -> subprocess.CompletedProcess[bytes]:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CanaryGuardError(
            f"cannot execute {' '.join(command)!r}: {exc}",
        ) from exc
    if result.returncode != 0 and not allow_failure:
        stderr = result.stderr.decode("utf-8", "replace").strip()
        raise CanaryGuardError(
            f"{' '.join(command)!r} failed with {result.returncode}: {stderr}",
        )
    return result


def _atomic_json(path: Path, payload: dict[str, Any], *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sudo_read(path: Path) -> bytes:
    return _run(["sudo", "-n", "cat", "--", str(path)]).stdout


def _path_observation(raw_path: str) -> dict[str, Any]:
    path = Path(raw_path)
    probe = _run(
        [
            "sudo",
            "-n",
            "python3",
            "-B",
            "-c",
            (
                "import json,os,stat,sys;"
                "p=sys.argv[1];"
                "\ntry:s=os.lstat(p)"
                "\nexcept FileNotFoundError:print(json.dumps({'kind':'absent'}));raise SystemExit"
                "\nkind=('symlink' if stat.S_ISLNK(s.st_mode) else "
                "'file' if stat.S_ISREG(s.st_mode) else "
                "'directory' if stat.S_ISDIR(s.st_mode) else 'other');"
                "\nprint(json.dumps({'kind':kind,'mode':stat.S_IMODE(s.st_mode),"
                "'target':os.readlink(p) if kind=='symlink' else None}))"
            ),
            str(path),
        ],
    )
    payload = json.loads(probe.stdout.decode("utf-8"))
    kind = payload["kind"]
    if kind == "file":
        raw = _sudo_read(path)
        payload.update({"bytes": len(raw), "sha256": _sha256(raw)})
    elif kind == "directory":
        manifest = _run(
            [
                "sudo",
                "-n",
                "python3",
                "-B",
                "-c",
                (
                    "import hashlib,json,os,stat,sys;"
                    "root=sys.argv[1];items=[];"
                    "\nfor base,dirs,files in os.walk(root,topdown=True,followlinks=False):"
                    "\n dirs.sort();files.sort()"
                    "\n for name in dirs+files:"
                    "\n  p=os.path.join(base,name);s=os.lstat(p);"
                    "\n  kind=('symlink' if stat.S_ISLNK(s.st_mode) else "
                    "'file' if stat.S_ISREG(s.st_mode) else "
                    "'directory' if stat.S_ISDIR(s.st_mode) else 'other');"
                    "\n  digest=None"
                    "\n  if kind=='file':"
                    "\n   with open(p,'rb') as h:digest=hashlib.sha256(h.read()).hexdigest()"
                    "\n  items.append({'path':os.path.relpath(p,root),'kind':kind,"
                    "'mode':stat.S_IMODE(s.st_mode),'target':os.readlink(p) "
                    "if kind=='symlink' else None,'sha256':digest})"
                    "\nprint(json.dumps(items,sort_keys=True,separators=(',',':')))"
                ),
                str(path),
            ],
        ).stdout
        payload["manifestSha256"] = _sha256(manifest)
        payload["entries"] = len(json.loads(manifest.decode("utf-8")))
    return payload


def _systemd_observation(unit: str) -> dict[str, str]:
    arguments = ["systemctl", "show", unit]
    for property_name in UNIT_PROPERTIES:
        arguments.extend(["-p", property_name])
    result = _run(arguments, allow_failure=True)
    if result.returncode != 0:
        return {
            "LoadState": "query-failed",
            "queryErrorSha256": _sha256(result.stderr),
        }
    payload: dict[str, str] = {}
    for line in result.stdout.decode("utf-8", "replace").splitlines():
        key, separator, value = line.partition("=")
        if separator:
            payload[key] = value
    environment = _run(
        ["systemctl", "show", unit, "-p", "Environment", "--value"],
        allow_failure=True,
    )
    if environment.returncode == 0:
        try:
            values = shlex.split(environment.stdout.decode("utf-8", "replace"))
        except ValueError:
            values = []
        for value in values:
            key, separator, configured = value.partition("=")
            if separator and key == "APP_BACKEND_WORKERS":
                payload["ConfiguredBackendWorkers"] = configured
                payload["ConfiguredBackendWorkersSource"] = "systemd_environment"
                break
    if "ConfiguredBackendWorkers" not in payload and unit.startswith(
        "jato-fullstack-backend@",
    ):
        backend_workers = _run(
            [
                "sudo",
                "-n",
                "python3",
                "-B",
                "-c",
                (
                    "import shlex,sys;"
                    "value='';"
                    "\nfor raw in open(sys.argv[1],encoding='utf-8'):"
                    "\n line=raw.strip()"
                    "\n if not line or line.startswith('#'):continue"
                    "\n try:tokens=shlex.split(line,comments=True,posix=True)"
                    "\n except ValueError:continue"
                    "\n if tokens and tokens[0]=='export':tokens=tokens[1:]"
                    "\n for token in tokens:"
                    "\n  key,sep,candidate=token.partition('=')"
                    "\n  if sep and key=='APP_BACKEND_WORKERS':value=candidate"
                    "\nprint(value)"
                ),
                "/etc/jato-fullstack/backend.env",
            ],
            allow_failure=True,
        )
        if backend_workers.returncode == 0:
            configured = backend_workers.stdout.decode("utf-8", "replace").strip()
            if configured:
                payload["ConfiguredBackendWorkers"] = configured
                payload["ConfiguredBackendWorkersSource"] = "backend_env"
    main_pid = payload.get("MainPID", "")
    group = payload.get("ControlGroup", "")
    if re.fullmatch(r"[1-9][0-9]*", main_pid) and group.startswith("/"):
        workers: list[int] = []
        processes = Path("/sys/fs/cgroup") / group.lstrip("/") / "cgroup.procs"
        try:
            candidates = processes.read_text(encoding="utf-8").splitlines()
        except OSError:
            candidates = []
        for candidate in candidates:
            try:
                command = (
                    Path("/proc") / candidate / "cmdline"
                ).read_bytes().replace(b"\0", b" ")
                status = (Path("/proc") / candidate / "status").read_text(
                    encoding="utf-8",
                )
            except OSError:
                continue
            parent = ""
            for status_line in status.splitlines():
                if status_line.startswith("PPid:"):
                    parent = status_line.partition(":")[2].strip()
                    break
            if (
                parent == main_pid
                and b"multiprocessing.spawn" in command
                and b"spawn_main" in command
            ):
                workers.append(int(candidate))
        payload["LiveBackendWorkerCount"] = str(len(workers))
        payload["LiveBackendWorkerPids"] = ",".join(
            str(worker) for worker in sorted(workers)
        )
    return payload


def _http_json(url: str) -> dict[str, Any]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise CanaryGuardError("public canary baseline requires an HTTPS hostname")
    port = parsed.port or 443
    descriptor, output_name = tempfile.mkstemp(prefix="jato-canary-http-")
    os.close(descriptor)
    try:
        result = _run(
            [
                "curl",
                "--noproxy",
                "*",
                "--silent",
                "--show-error",
                "--max-time",
                "20",
                "--proto",
                "=https",
                "--resolve",
                f"{parsed.hostname}:{port}:127.0.0.1",
                "--header",
                "Accept: application/json",
                "--user-agent",
                "jato-feature-canary-guard/1",
                "--output",
                output_name,
                "--write-out",
                "%{http_code}",
                url,
            ],
            timeout=25,
            allow_failure=True,
        )
        body = Path(output_name).read_bytes()
    finally:
        try:
            Path(output_name).unlink()
        except FileNotFoundError:
            pass
    if len(body) > MAX_HTTP_BYTES:
        raise CanaryGuardError(f"HTTP observation exceeded {MAX_HTTP_BYTES} bytes")
    raw_status = result.stdout.decode("ascii", "replace").strip()
    status = int(raw_status) if raw_status.isdigit() else 0
    try:
        decoded = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        decoded = None
    return {
        "status": status,
        "error": (
            None
            if result.returncode == 0
            else result.stderr.decode("utf-8", "replace").strip()
        ),
        "bodySha256": _sha256(body),
        "json": decoded,
    }


def _port_is_free(port: int) -> bool:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        probe.bind(("127.0.0.1", port))
    except OSError:
        return False
    finally:
        probe.close()
    return True


def verify_port_free(port: int) -> None:
    if port < 1 or port > 65535:
        raise CanaryGuardError("candidate loopback port is outside 1..65535")
    if not _port_is_free(port):
        raise CanaryGuardError(
            "candidate loopback port is not yet available for a strict bind",
        )


def _monthly_worker_processes() -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").strip()
        except OSError:
            continue
        if b"jato_monthly_worker.py" in command:
            found.append(
                {
                    "pid": int(entry.name),
                    "cmdlineSha256": _sha256(command),
                },
            )
    return sorted(found, key=lambda item: item["pid"])


def capture_snapshot(
    *,
    public_origin: str,
    active_unit: str,
    candidate_port: int,
) -> dict[str, Any]:
    origin = public_origin.rstrip("/")
    nginx = _run(
        ["sudo", "-n", "nginx", "-T"],
        timeout=30,
        allow_failure=True,
    )
    nginx_payload = nginx.stdout + b"\n--stderr--\n" + nginx.stderr
    units = dict.fromkeys((*OBSERVED_UNITS, active_unit))
    return {
        "schemaVersion": 1,
        "capturedAt": dt.datetime.now(dt.UTC).isoformat(),
        "public": {
            "origin": origin,
            "healthz": _http_json(f"{origin}/healthz"),
            "buildMeta": _http_json(f"{origin}/build-meta.json"),
        },
        "nginx": {
            "exitCode": nginx.returncode,
            "configurationSha256": _sha256(nginx_payload),
            "candidatePortReferenced": bool(
                re.search(
                    rf"(?<![0-9]){candidate_port}(?![0-9])".encode(),
                    nginx_payload,
                ),
            ),
        },
        "paths": {
            path: _path_observation(path)
            for path in PRODUCTION_PATHS
        },
        "units": {
            unit: _systemd_observation(unit)
            for unit in sorted(units)
        },
        "activeUnit": active_unit,
        "candidatePort": candidate_port,
        "candidatePortFree": _port_is_free(candidate_port),
        "monthlyWorkerProcesses": _monthly_worker_processes(),
    }


def _load_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise CanaryGuardError(f"JSON input is missing or unsafe: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CanaryGuardError(f"cannot read JSON input {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise CanaryGuardError(f"JSON input must be an object: {path}")
    return payload


def verify_baseline(payload: dict[str, Any]) -> None:
    health = payload.get("public", {}).get("healthz", {})
    health_json = health.get("json")
    if (
        health.get("status") != 200
        or not isinstance(health_json, dict)
        or health_json.get("status") != "ok"
    ):
        raise CanaryGuardError("public /healthz baseline is not HTTP 200 status=ok")

    build = payload.get("public", {}).get("buildMeta", {})
    build_json = build.get("json")
    if build.get("status") != 200 or not isinstance(build_json, dict):
        raise CanaryGuardError("public build-meta baseline is unavailable")
    commits = {
        str(build_json.get(name) or "")
        for name in ("deployCommit", "githubSha", "commitSha", "sha")
    }
    if not any(SHA_PATTERN.fullmatch(value) for value in commits):
        raise CanaryGuardError("public build-meta does not contain a full release SHA")

    active_unit = str(payload.get("activeUnit") or "")
    active = payload.get("units", {}).get(active_unit, {})
    if active.get("ActiveState") != "active":
        raise CanaryGuardError("active backend is not running before canary")
    if active.get("MemoryHigh") != str(EXPECTED_ACTIVE_MEMORY_HIGH):
        raise CanaryGuardError("active backend MemoryHigh is not 6G")
    if active.get("MemoryMax") != str(EXPECTED_ACTIVE_MEMORY_MAX):
        raise CanaryGuardError("active backend MemoryMax is not 8G")
    if (
        "--workers ${APP_BACKEND_WORKERS}" not in str(active.get("ExecStart") or "")
        and "--workers 2" not in str(active.get("ExecStart") or "")
    ):
        raise CanaryGuardError("active backend ExecStart lacks its worker contract")
    if active.get("ConfiguredBackendWorkers") != "2":
        raise CanaryGuardError("active backend is not configured for two workers")
    if active.get("LiveBackendWorkerCount") != "2":
        raise CanaryGuardError("active backend does not have two live workers")
    if not re.fullmatch(r"[1-9][0-9]*", str(active.get("MainPID") or "")):
        raise CanaryGuardError("active backend MainPID is unavailable")

    monthly = payload.get("units", {}).get(
        "jato-monthly-worker.service",
        {},
    )
    if (
        monthly.get("ActiveState") != "inactive"
        or monthly.get("UnitFileState") != "disabled"
    ):
        raise CanaryGuardError("JATO monthly worker is not inactive and disabled")
    if payload.get("monthlyWorkerProcesses") != []:
        raise CanaryGuardError("a JATO monthly worker process exists before canary")
    if payload.get("candidatePortFree") is not True:
        raise CanaryGuardError("candidate loopback port is already occupied")
    if payload.get("nginx", {}).get("exitCode") != 0:
        raise CanaryGuardError("nginx -T baseline failed")
    if payload.get("nginx", {}).get("candidatePortReferenced") is not False:
        raise CanaryGuardError(
            "Nginx already references the loopback candidate port",
        )


def compare_snapshots(before: dict[str, Any], after: dict[str, Any]) -> None:
    ignored = {"capturedAt"}
    before_stable = {key: value for key, value in before.items() if key not in ignored}
    after_stable = {key: value for key, value in after.items() if key not in ignored}
    if before_stable == after_stable:
        return
    changed = sorted(
        key
        for key in set(before_stable) | set(after_stable)
        if before_stable.get(key) != after_stable.get(key)
    )
    raise CanaryGuardError(
        "production state changed during feature canary: "
        + ", ".join(changed),
    )


def verify_candidate_evidence(
    payload: dict[str, Any],
    identity: dict[str, Any],
) -> str:
    if (
        payload.get("status") != "verified"
        or payload.get("featureCommit") != identity["commit"]
        or payload.get("port") != identity["port"]
        or payload.get("liveBackendWorkerCount") != 2
        or payload.get("monthlyStatus") != 423
    ):
        raise CanaryGuardError(
            "successful canary receipt has invalid candidate evidence",
        )
    health = payload.get("healthz")
    if not isinstance(health, dict) or health.get("status") != "ok":
        raise CanaryGuardError("candidate evidence lacks healthz status=ok")
    readyz = payload.get("readyz")
    observed = readyz.get("observed") if isinstance(readyz, dict) else None
    release = observed.get("release") if isinstance(observed, dict) else None
    if (
        not isinstance(readyz, dict)
        or readyz.get("ok") is not True
        or not isinstance(observed, dict)
        or observed.get("status") != "ready"
        or not isinstance(release, dict)
        or release.get("commitSha") != identity["commit"]
    ):
        raise CanaryGuardError(
            "candidate evidence lacks exact feature-SHA readyz proof",
        )
    systemd = payload.get("systemd")
    if not isinstance(systemd, dict):
        raise CanaryGuardError("candidate evidence lacks systemd properties")
    required_properties = {
        "ActiveState": "active",
        "UnitFileState": "transient",
        "DynamicUser": "yes",
        "ProtectSystem": "strict",
        "ProtectHome": "yes",
        "NoNewPrivileges": "yes",
        "Restart": "no",
        "MemoryHigh": str(3 * 1024 * 1024 * 1024),
        "MemoryMax": str(4 * 1024 * 1024 * 1024),
        "MemorySwapMax": "0",
        "TasksMax": "512",
    }
    for key, expected in required_properties.items():
        if systemd.get(key) != expected:
            raise CanaryGuardError(
                f"candidate evidence property {key} is not {expected}",
            )
    if "--workers 2" not in str(systemd.get("ExecStart") or ""):
        raise CanaryGuardError("candidate evidence lacks exactly two workers")
    expected_supervisor = (
        "jato-feature-canary-supervisor-"
        f"{identity['commit'][:12]}-{identity['runId']}.service"
    )
    if (
        set(str(systemd.get("StopPropagatedFrom") or "").split())
        != {expected_supervisor}
        or expected_supervisor
        not in str(systemd.get("After") or "").split()
        or str(systemd.get("BindsTo") or "").split()
        or str(systemd.get("PartOf") or "").split()
    ):
        raise CanaryGuardError(
            "candidate evidence lacks its exact stop-only supervisor contract",
        )
    environment = str(systemd.get("Environment") or "")
    required_environment = (
        "APP_DATABASE_ENABLED=false",
        "APP_REDIS_ENABLED=false",
        "APP_JATO_MONTHLY_ENABLED=false",
        "APP_JATO_MONTHLY_EXECUTION_MODE=disabled",
        "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=false",
        "APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED=false",
        "APP_METADATA_PREWARM_ENABLED=false",
        "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED=false",
        "HERMES_RUN_ENABLED=false",
    )
    missing = [value for value in required_environment if value not in environment]
    if missing:
        raise CanaryGuardError(
            "candidate evidence omitted disabled subsystem flags: "
            + ", ".join(missing),
        )
    try:
        environment_tokens = shlex.split(environment)
    except ValueError as exc:
        raise CanaryGuardError(
            "candidate evidence Environment is malformed",
        ) from exc
    supervisor_generation = next(
        (
            token.partition("=")[2]
            for token in environment_tokens
            if token.startswith("CANARY_SUPERVISOR_INVOCATION_ID=")
        ),
        "",
    )
    if (
        re.fullmatch(r"[0-9a-f]{32}", supervisor_generation) is None
        or supervisor_generation == "0" * 32
    ):
        raise CanaryGuardError(
            "candidate evidence lacks the original supervisor generation",
        )
    return supervisor_generation


def record_checkpoint(
    *,
    path: Path,
    identity: dict[str, Any],
    phase: str,
    status: str,
    message: str,
) -> None:
    payload: dict[str, Any] = {
        "schemaVersion": 1,
        "identity": identity,
        "events": [],
    }
    if path.exists():
        payload = _load_json(path)
        if payload.get("identity") != identity:
            raise CanaryGuardError("checkpoint identity changed during canary")
    events = payload.setdefault("events", [])
    if not isinstance(events, list):
        raise CanaryGuardError("checkpoint events are malformed")
    events.append(
        {
            "at": dt.datetime.now(dt.UTC).isoformat(),
            "phase": phase,
            "status": status,
            "message": message,
        },
    )
    payload["phase"] = phase
    payload["status"] = status
    _atomic_json(path, payload)


def verify_checkpoint_marker(
    checkpoint: dict[str, Any],
    identity: dict[str, Any],
    *,
    phase: str,
    status: str,
) -> None:
    if checkpoint.get("schemaVersion") != 1:
        raise CanaryGuardError("canary checkpoint schema is invalid")
    if checkpoint.get("identity") != identity:
        raise CanaryGuardError("checkpoint marker identity differs from canary")
    events = checkpoint.get("events")
    if not isinstance(events, list):
        raise CanaryGuardError("checkpoint marker events are malformed")
    matches = [
        event
        for event in events
        if isinstance(event, dict)
        and event.get("phase") == phase
        and event.get("status") == status
        and isinstance(event.get("at"), str)
        and isinstance(event.get("message"), str)
        and bool(event["message"])
    ]
    if len(matches) != 1:
        raise CanaryGuardError(
            f"checkpoint requires exactly one durable {phase}/{status} marker",
        )


def ensure_checkpoint_marker(
    *,
    path: Path,
    identity: dict[str, Any],
    phase: str,
    status: str,
    message: str,
) -> None:
    checkpoint = _load_json(path)
    if checkpoint.get("schemaVersion") != 1:
        raise CanaryGuardError("canary checkpoint schema is invalid")
    if checkpoint.get("identity") != identity:
        raise CanaryGuardError("checkpoint marker identity differs from canary")
    events = checkpoint.get("events")
    if not isinstance(events, list):
        raise CanaryGuardError("checkpoint marker events are malformed")
    matches = [
        event
        for event in events
        if isinstance(event, dict)
        and event.get("phase") == phase
        and event.get("status") == status
    ]
    if len(matches) > 1:
        raise CanaryGuardError(
            f"checkpoint has duplicate durable {phase}/{status} markers",
        )
    if len(matches) == 1:
        verify_checkpoint_marker(
            checkpoint,
            identity,
            phase=phase,
            status=status,
        )
        return
    record_checkpoint(
        path=path,
        identity=identity,
        phase=phase,
        status=status,
        message=message,
    )


def verify_receipt_payload(
    payload: dict[str, Any],
    identity: dict[str, Any],
) -> None:
    if payload.get("schemaVersion") != 1:
        raise CanaryGuardError("canary receipt schema is invalid")
    if payload.get("identity") != identity:
        raise CanaryGuardError("canary receipt identity differs from expected")
    outcome = payload.get("outcome")
    if outcome not in {"passed", "failed", "expected_failure_verified"}:
        raise CanaryGuardError("canary receipt outcome is invalid")
    if payload.get("terminalWriter") != "supervisor_reconcile":
        raise CanaryGuardError(
            "canary receipt was not written by supervisor reconciliation",
        )
    writer_invocation_id = payload.get("writerInvocationId")
    if (
        not isinstance(writer_invocation_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", writer_invocation_id) is None
        or writer_invocation_id == "0" * 32
    ):
        raise CanaryGuardError(
            "canary receipt lacks a valid supervisor writer generation",
        )
    checkpoint = payload.get("checkpoint")
    if not isinstance(checkpoint, dict):
        raise CanaryGuardError("canary receipt checkpoint is missing")
    if checkpoint.get("identity") != identity:
        raise CanaryGuardError("receipt checkpoint identity differs from canary")
    verify_checkpoint_marker(
        checkpoint,
        identity,
        phase="supervisor_reconciled",
        status="completed",
    )
    if (
        checkpoint.get("phase") != "supervisor_reconciled"
        or checkpoint.get("status") != "completed"
    ):
        raise CanaryGuardError(
            "terminal canary receipt lacks final supervisor reconciliation",
        )
    if outcome == "failed":
        if not isinstance(payload.get("error"), str) or not payload["error"]:
            raise CanaryGuardError("failed canary receipt lacks its failure reason")
        return

    before = payload.get("productionBefore")
    after = payload.get("productionAfter")
    candidate = payload.get("candidate")
    if (
        not isinstance(before, dict)
        or not isinstance(after, dict)
        or not isinstance(candidate, dict)
    ):
        raise CanaryGuardError(
            "successful canary receipt requires complete embedded evidence",
        )
    verify_baseline(before)
    verify_baseline(after)
    compare_snapshots(before, after)
    candidate_invocation_id = verify_candidate_evidence(candidate, identity)
    if candidate_invocation_id != writer_invocation_id:
        raise CanaryGuardError(
            "candidate evidence and terminal receipt use different supervisor generations",
        )
    controller_terminal_phase = (
        "expected_failure_verified"
        if outcome == "expected_failure_verified"
        else "cleanup_verified"
    )
    marker_phase = (
        "fault_observed"
        if outcome == "expected_failure_verified"
        else "controller_completed"
    )
    verify_checkpoint_marker(
        checkpoint,
        identity,
        phase=marker_phase,
        status="completed",
    )
    verify_checkpoint_marker(
        checkpoint,
        identity,
        phase=controller_terminal_phase,
        status="completed",
    )
    events = checkpoint.get("events")
    assert isinstance(events, list)
    ordered_phases = (
        marker_phase,
        controller_terminal_phase,
        "supervisor_reconciled",
    )
    marker_indexes = [
        next(
            index
            for index, event in enumerate(events)
            if isinstance(event, dict)
            and event.get("phase") == phase
            and event.get("status") == "completed"
        )
        for phase in ordered_phases
    ]
    if marker_indexes != sorted(marker_indexes) or len(set(marker_indexes)) != 3:
        raise CanaryGuardError(
            "durable controller, cleanup and supervisor markers are out of order",
        )
    fault = payload.get("faultInjection")
    error = payload.get("error")
    if outcome == "passed" and (fault is not None or error is not None):
        raise CanaryGuardError(
            "passed canary receipt cannot contain a fault or error",
        )
    if outcome == "expected_failure_verified" and (
        fault != "after_candidate_start"
        or not isinstance(error, str)
        or "expected fault injection" not in error
    ):
        raise CanaryGuardError(
            "expected-failure receipt lacks the reviewed fault evidence",
        )


def verify_receipt(path: Path, identity: dict[str, Any]) -> None:
    verify_receipt_payload(_load_json(path), identity)


def finalize_receipt(
    *,
    path: Path,
    identity: dict[str, Any],
    outcome: str,
    fault: str,
    error: str,
    before_path: Path,
    after_path: Path,
    candidate_path: Path | None,
    checkpoint_path: Path,
    terminal_writer: str,
    writer_invocation_id: str,
) -> None:
    if path.exists() or path.is_symlink():
        raise CanaryGuardError(f"canary receipt already exists: {path}")
    candidate: dict[str, Any] | None = None
    if candidate_path is not None and candidate_path.exists():
        candidate = _load_json(candidate_path)
    checkpoint = _load_json(checkpoint_path)
    if checkpoint.get("identity") != identity:
        raise CanaryGuardError("checkpoint identity differs from receipt identity")
    before = _load_json(before_path) if before_path.exists() else None
    after = _load_json(after_path) if after_path.exists() else None
    successful_outcome = outcome in {"passed", "expected_failure_verified"}
    if successful_outcome:
        if before is None or after is None or candidate is None:
            raise CanaryGuardError(
                "successful canary receipt requires before/after/candidate evidence",
            )
        verify_baseline(before)
        verify_baseline(after)
        compare_snapshots(before, after)
        verify_candidate_evidence(candidate, identity)
        controller_terminal_phase = (
            "expected_failure_verified"
            if outcome == "expected_failure_verified"
            else "cleanup_verified"
        )
        verify_checkpoint_marker(
            checkpoint,
            identity,
            phase=controller_terminal_phase,
            status="completed",
        )
        verify_checkpoint_marker(
            checkpoint,
            identity,
            phase="supervisor_reconciled",
            status="completed",
        )
        if (
            checkpoint.get("phase") != "supervisor_reconciled"
            or checkpoint.get("status") != "completed"
        ):
            raise CanaryGuardError(
                "successful canary receipt lacks supervisor reconciliation",
            )
        if outcome == "passed" and (fault or error):
            raise CanaryGuardError(
                "passed canary receipt cannot contain a fault or error",
            )
        if outcome == "expected_failure_verified" and (
            fault != "after_candidate_start"
            or "expected fault injection" not in error
        ):
            raise CanaryGuardError(
                "expected-failure receipt lacks the reviewed fault evidence",
            )
    payload = {
        "schemaVersion": 1,
        "identity": identity,
        "outcome": outcome,
        "faultInjection": fault or None,
        "error": error or None,
        "finishedAt": dt.datetime.now(dt.UTC).isoformat(),
        "terminalWriter": terminal_writer,
        "writerInvocationId": writer_invocation_id,
        "productionBefore": before,
        "productionAfter": after,
        "candidate": candidate,
        "checkpoint": checkpoint,
    }
    verify_receipt_payload(payload, identity)
    _atomic_json(path, payload)


def _identity(arguments: argparse.Namespace) -> dict[str, Any]:
    branch = arguments.branch.strip()
    if not branch or branch == "main":
        raise CanaryGuardError("feature canary branch must be real and non-main")
    if not SHA_PATTERN.fullmatch(arguments.commit):
        raise CanaryGuardError("feature canary commit must be a full lowercase SHA")
    if not ARCHIVE_PATTERN.fullmatch(arguments.archive_sha256):
        raise CanaryGuardError("feature canary archive SHA-256 is invalid")
    if arguments.archive_bytes <= 0:
        raise CanaryGuardError("feature canary archive byte count must be positive")
    return {
        "repository": arguments.repository,
        "featureBranch": branch,
        "commit": arguments.commit,
        "archiveSha256": arguments.archive_sha256,
        "archiveBytes": arguments.archive_bytes,
        "runId": arguments.run_id,
        "port": arguments.port,
    }


def _add_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--archive-bytes", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--port", type=int, required=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    observe = commands.add_parser("observe")
    observe.add_argument("--output", type=Path, required=True)
    observe.add_argument("--public-origin", required=True)
    observe.add_argument("--active-unit", required=True)
    observe.add_argument("--candidate-port", type=int, required=True)

    baseline = commands.add_parser("verify-baseline")
    baseline.add_argument("--snapshot", type=Path, required=True)

    compare = commands.add_parser("compare")
    compare.add_argument("--before", type=Path, required=True)
    compare.add_argument("--after", type=Path, required=True)

    port_free = commands.add_parser("verify-port-free")
    port_free.add_argument("--port", type=int, required=True)

    record = commands.add_parser("record")
    record.add_argument("--path", type=Path, required=True)
    _add_identity_arguments(record)
    record.add_argument("--phase", required=True)
    record.add_argument("--status", required=True)
    record.add_argument("--message", required=True)

    marker = commands.add_parser("verify-marker")
    marker.add_argument("--checkpoint", type=Path, required=True)
    _add_identity_arguments(marker)
    marker.add_argument("--phase", required=True)
    marker.add_argument("--status", required=True)

    ensure_marker = commands.add_parser("ensure-marker")
    ensure_marker.add_argument("--checkpoint", type=Path, required=True)
    _add_identity_arguments(ensure_marker)
    ensure_marker.add_argument("--phase", required=True)
    ensure_marker.add_argument("--status", required=True)
    ensure_marker.add_argument("--message", required=True)

    receipt = commands.add_parser("verify-receipt")
    receipt.add_argument("--path", type=Path, required=True)
    _add_identity_arguments(receipt)

    finalize = commands.add_parser("finalize")
    finalize.add_argument("--path", type=Path, required=True)
    _add_identity_arguments(finalize)
    finalize.add_argument(
        "--outcome",
        choices=("passed", "failed", "expected_failure_verified"),
        required=True,
    )
    finalize.add_argument("--fault", default="")
    finalize.add_argument("--error", default="")
    finalize.add_argument("--before", type=Path, required=True)
    finalize.add_argument("--after", type=Path, required=True)
    finalize.add_argument("--candidate", type=Path)
    finalize.add_argument("--checkpoint", type=Path, required=True)
    finalize.add_argument(
        "--terminal-writer",
        choices=("supervisor_reconcile",),
        required=True,
    )
    finalize.add_argument("--writer-invocation-id", required=True)
    return parser


def main() -> int:
    arguments = _build_parser().parse_args()
    try:
        if arguments.command == "observe":
            snapshot = capture_snapshot(
                public_origin=arguments.public_origin,
                active_unit=arguments.active_unit,
                candidate_port=arguments.candidate_port,
            )
            _atomic_json(arguments.output, snapshot)
        elif arguments.command == "verify-baseline":
            verify_baseline(_load_json(arguments.snapshot))
        elif arguments.command == "compare":
            compare_snapshots(
                _load_json(arguments.before),
                _load_json(arguments.after),
            )
        elif arguments.command == "verify-port-free":
            verify_port_free(arguments.port)
        elif arguments.command == "record":
            record_checkpoint(
                path=arguments.path,
                identity=_identity(arguments),
                phase=arguments.phase,
                status=arguments.status,
                message=arguments.message,
            )
        elif arguments.command == "verify-marker":
            identity = _identity(arguments)
            verify_checkpoint_marker(
                _load_json(arguments.checkpoint),
                identity,
                phase=arguments.phase,
                status=arguments.status,
            )
        elif arguments.command == "verify-receipt":
            verify_receipt(arguments.path, _identity(arguments))
        elif arguments.command == "ensure-marker":
            ensure_checkpoint_marker(
                path=arguments.checkpoint,
                identity=_identity(arguments),
                phase=arguments.phase,
                status=arguments.status,
                message=arguments.message,
            )
        else:
            finalize_receipt(
                path=arguments.path,
                identity=_identity(arguments),
                outcome=arguments.outcome,
                fault=arguments.fault,
                error=arguments.error,
                before_path=arguments.before,
                after_path=arguments.after,
                candidate_path=arguments.candidate,
                checkpoint_path=arguments.checkpoint,
                terminal_writer=arguments.terminal_writer,
                writer_invocation_id=arguments.writer_invocation_id,
            )
    except CanaryGuardError as exc:
        print(
            json.dumps(
                {
                    "check": "feature_candidate_canary",
                    "ok": False,
                    "error": str(exc),
                },
                sort_keys=True,
            ),
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
