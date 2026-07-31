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


def _sha256_json_receipt(payload: dict[str, Any]) -> str:
    encoded = (
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _verify_candidate_build_evidence_v3(
    build: object,
    identity: dict[str, Any],
) -> tuple[int, int]:
    if not isinstance(build, dict) or build.get("schemaVersion") != 3:
        raise CanaryGuardError(
            "candidate evidence lacks schema-v3 trusted build evidence",
        )
    archive = build.get("archiveValidation")
    anchor = build.get("referenceAnchor")
    materialization = build.get("materialization")
    source_seal = build.get("sourceSeal")
    private_materialization = build.get("privateMaterialization")
    if (
        not isinstance(archive, dict)
        or not isinstance(anchor, dict)
        or not isinstance(materialization, dict)
        or not isinstance(source_seal, dict)
        or not isinstance(private_materialization, dict)
    ):
        raise CanaryGuardError(
            "candidate evidence lacks trusted archive, anchor or "
            "materialization metadata",
        )

    expected_mode_policy = {
        "publicFiles": ["0644", "0755"],
        "publicDirectories": ["0755"],
        "privatePrefixes": [
            "01_RAW_DATA",
            "03_Scripts/diagnostics/artifacts",
        ],
        "privateFiles": ["0600", "0711"],
        "privateDirectories": ["0711"],
    }
    required_controls = {
        "03_Scripts/deploy/tencent_feature_candidate_canary.sh",
        "03_Scripts/deploy/jato_feature_canary_guard.py",
        "03_Scripts/deploy/lib/production_mutation_lock.sh",
        "03_Scripts/deploy/verify_backend_readiness.py",
        "03_Scripts/deploy/validate_release_archive.py",
        "03_Scripts/deploy/cleanup_toolkit_egg_info.py",
        "03_Scripts/deploy/verify_release_source_seal.py",
    }
    trusted_controls = archive.get("trustedControls")
    member_count = archive.get("memberCount")
    expanded_bytes = archive.get("expandedBytes")
    member_classes = archive.get("memberClasses")
    private_entries = archive.get("privateEntries")
    private_files = (
        private_entries.get("files")
        if isinstance(private_entries, dict)
        else None
    )
    private_directories = (
        private_entries.get("directories")
        if isinstance(private_entries, dict)
        else None
    )
    if (
        archive.get("schemaVersion") != 2
        or archive.get("status") != "validated"
        or archive.get("archiveSha256") != identity["archiveSha256"]
        or archive.get("archiveBytes") != identity["archiveBytes"]
        or archive.get("rootMode") != "0755"
        or archive.get("modePolicy") != expected_mode_policy
        or isinstance(member_count, bool)
        or not isinstance(member_count, int)
        or not 0 < member_count <= 50_000
        or isinstance(expanded_bytes, bool)
        or not isinstance(expanded_bytes, int)
        or not 0 < expanded_bytes <= 2 * 1024 * 1024 * 1024
        or not isinstance(member_classes, dict)
        or not isinstance(private_files, list)
        or not private_files
        or not isinstance(private_directories, list)
        or not private_directories
        or not isinstance(trusted_controls, dict)
        or set(trusted_controls) != required_controls
        or any(
            not isinstance(digest, str)
            or ARCHIVE_PATTERN.fullmatch(digest) is None
            for digest in trusted_controls.values()
        )
    ):
        raise CanaryGuardError(
            "candidate evidence has an invalid archive-validation receipt",
        )

    expected_files: dict[str, dict[str, Any]] = {}
    for item in private_files:
        if (
            not isinstance(item, dict)
            or set(item) != {"path", "mode", "sha256", "bytes"}
            or not isinstance(item.get("path"), str)
            or not any(
                item["path"] == prefix
                or item["path"].startswith(f"{prefix}/")
                for prefix in expected_mode_policy["privatePrefixes"]
            )
            or item.get("mode") not in {"0600", "0711"}
            or not isinstance(item.get("sha256"), str)
            or ARCHIVE_PATTERN.fullmatch(item["sha256"]) is None
            or isinstance(item.get("bytes"), bool)
            or not isinstance(item.get("bytes"), int)
            or item["bytes"] < 0
            or item["path"] in expected_files
        ):
            raise CanaryGuardError(
                "candidate archive receipt has malformed private files",
            )
        expected_files[item["path"]] = item

    expected_directories: dict[str, dict[str, Any]] = {}
    for item in private_directories:
        if (
            not isinstance(item, dict)
            or set(item) != {"path", "mode"}
            or not isinstance(item.get("path"), str)
            or not any(
                item["path"] == prefix
                or item["path"].startswith(f"{prefix}/")
                for prefix in expected_mode_policy["privatePrefixes"]
            )
            or item.get("mode") != "0711"
            or item["path"] in expected_directories
        ):
            raise CanaryGuardError(
                "candidate archive receipt has malformed private directories",
            )
        expected_directories[item["path"]] = item

    required_private_files = {
        "01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx",
        (
            "03_Scripts/diagnostics/artifacts/msrp_backfill/"
            "sweden_swiss_top30_suv/official_evidence_leads.json"
        ),
        (
            "03_Scripts/diagnostics/artifacts/msrp_backfill/"
            "sweden_swiss_top30_suv/"
            "top30_suv_price_movement_candidates.json"
        ),
    }
    required_private_directories = {
        "01_RAW_DATA",
        "03_Scripts/diagnostics/artifacts",
        "03_Scripts/diagnostics/artifacts/msrp_backfill",
        (
            "03_Scripts/diagnostics/artifacts/msrp_backfill/"
            "sweden_swiss_top30_suv"
        ),
    }
    if (
        not required_private_files.issubset(expected_files)
        or not required_private_directories.issubset(expected_directories)
        or member_classes.get("privateFiles") != len(expected_files)
        or member_classes.get("privateDirectories")
        != len(expected_directories)
    ):
        raise CanaryGuardError(
            "candidate archive receipt lacks complete private assets",
        )
    for relative in set(expected_files) | set(expected_directories):
        parts = relative.split("/")
        for depth in range(1, len(parts)):
            parent = "/".join(parts[:depth])
            if any(
                parent == prefix or parent.startswith(f"{prefix}/")
                for prefix in expected_mode_policy["privatePrefixes"]
            ) and parent not in expected_directories:
                raise CanaryGuardError(
                    "candidate archive receipt lacks a private parent chain",
                )

    roots = materialization.get("roots")
    if (
        materialization.get("referenceRootMode") != "0700"
        or materialization.get("candidateRootMode") != "0711"
        or materialization.get("extractFlags")
        != ["--same-permissions", "--no-overwrite-dir"]
        or materialization.get("copyMethod")
        != "independent-sealed-archive-extraction"
        or not isinstance(roots, dict)
        or set(roots) != {"reference", "candidate"}
    ):
        raise CanaryGuardError(
            "candidate evidence lacks independent trusted materialization",
        )
    reference_root = roots.get("reference")
    candidate_root = roots.get("candidate")
    if (
        reference_root != {"uid": 0, "gid": 0, "mode": "0700"}
        or not isinstance(candidate_root, dict)
        or set(candidate_root) != {"uid", "gid", "mode"}
        or isinstance(candidate_root.get("uid"), bool)
        or not isinstance(candidate_root.get("uid"), int)
        or candidate_root["uid"] <= 0
        or isinstance(candidate_root.get("gid"), bool)
        or not isinstance(candidate_root.get("gid"), int)
        or candidate_root["gid"] <= 0
        or candidate_root.get("mode") != "0711"
    ):
        raise CanaryGuardError(
            "candidate evidence has invalid root ownership or modes",
        )

    if (
        anchor.get("schemaVersion") != 1
        or anchor.get("archiveSha256") != identity["archiveSha256"]
        or anchor.get("archiveBytes") != identity["archiveBytes"]
        or anchor.get("archiveValidationSha256")
        != _sha256_json_receipt(archive)
        or anchor.get("roots") != roots
        or not isinstance(anchor.get("sourceSealSha256"), str)
        or ARCHIVE_PATTERN.fullmatch(anchor["sourceSealSha256"]) is None
        or source_seal.get("profile") != "source"
        or source_seal.get("sha256") != anchor["sourceSealSha256"]
        or source_seal.get("verifiedAfterBuild") is not True
        or build.get("toolkitEggInfo")
        != {
            "cleanBeforeEditableInstall": True,
            "cleanAfterEditableInstall": True,
        }
    ):
        raise CanaryGuardError(
            "candidate evidence has an invalid root-owned reference anchor",
        )

    if set(private_materialization) != {"reference", "candidate"}:
        raise CanaryGuardError(
            "candidate evidence lacks both private materializations",
        )
    expected_root_identity = {
        "reference": reference_root,
        "candidate": candidate_root,
    }
    for label in ("reference", "candidate"):
        observed = private_materialization.get(label)
        observed_files = observed.get("files") if isinstance(observed, dict) else None
        observed_directories = (
            observed.get("directories") if isinstance(observed, dict) else None
        )
        if (
            not isinstance(observed, dict)
            or set(observed) != {"files", "directories"}
            or not isinstance(observed_files, list)
            or not isinstance(observed_directories, list)
        ):
            raise CanaryGuardError(
                "candidate evidence has malformed private materialization",
            )
        root_identity = expected_root_identity[label]
        assert isinstance(root_identity, dict)
        actual_files: dict[str, dict[str, Any]] = {}
        for item in observed_files:
            path = item.get("path") if isinstance(item, dict) else None
            expected = expected_files.get(path) if isinstance(path, str) else None
            if (
                not isinstance(item, dict)
                or set(item) != {
                    "path",
                    "mode",
                    "sha256",
                    "bytes",
                    "uid",
                    "gid",
                }
                or expected is None
                or item.get("mode") != expected["mode"]
                or item.get("sha256") != expected["sha256"]
                or item.get("bytes") != expected["bytes"]
                or item.get("uid") != root_identity["uid"]
                or item.get("gid") != root_identity["gid"]
                or path in actual_files
            ):
                raise CanaryGuardError(
                    "candidate private file materialization differs from "
                    "the archive receipt",
                )
            actual_files[path] = item
        actual_directories: dict[str, dict[str, Any]] = {}
        for item in observed_directories:
            path = item.get("path") if isinstance(item, dict) else None
            expected = (
                expected_directories.get(path) if isinstance(path, str) else None
            )
            if (
                not isinstance(item, dict)
                or set(item) != {"path", "mode", "uid", "gid"}
                or expected is None
                or item.get("mode") != expected["mode"]
                or item.get("uid") != root_identity["uid"]
                or item.get("gid") != root_identity["gid"]
                or path in actual_directories
            ):
                raise CanaryGuardError(
                    "candidate private directory materialization differs from "
                    "the archive receipt",
                )
            actual_directories[path] = item
        if (
            set(actual_files) != set(expected_files)
            or set(actual_directories) != set(expected_directories)
        ):
            raise CanaryGuardError(
                "candidate private materialization path set is incomplete",
            )
    return candidate_root["uid"], candidate_root["gid"]


def verify_candidate_evidence(
    payload: dict[str, Any],
    identity: dict[str, Any],
) -> str:
    if (
        payload.get("evidenceSchemaVersion") != 2
        or
        payload.get("status") != "verified"
        or payload.get("featureCommit") != identity["commit"]
        or payload.get("port") != identity["port"]
        or payload.get("liveBackendWorkerCount") != 2
        or payload.get("monthlyStatus") != 423
    ):
        raise CanaryGuardError(
            "successful canary receipt has invalid candidate evidence",
        )
    build_evidence = payload.get("buildEvidence")
    deploy_uid, deploy_gid = _verify_candidate_build_evidence_v3(
        build_evidence,
        identity,
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
        "UMask": "0022",
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
    environment_raw = systemd.get("Environment")
    if not isinstance(environment_raw, str):
        raise CanaryGuardError("candidate evidence Environment is malformed")
    try:
        environment_tokens = shlex.split(environment_raw)
    except ValueError as exc:
        raise CanaryGuardError(
            "candidate evidence Environment is malformed",
        ) from exc
    environment: dict[str, str] = {}
    for token in environment_tokens:
        key, separator, value = token.partition("=")
        if not separator or not key or key in environment:
            raise CanaryGuardError(
                "candidate evidence Environment is malformed or ambiguous",
            )
        environment[key] = value
    runtime_deploy_uid = environment.get("CANARY_DEPLOY_UID")
    runtime_deploy_gid = environment.get("CANARY_DEPLOY_GID")
    if (
        not isinstance(runtime_deploy_uid, str)
        or re.fullmatch(r"[1-9][0-9]*", runtime_deploy_uid) is None
        or not isinstance(runtime_deploy_gid, str)
        or re.fullmatch(r"[1-9][0-9]*", runtime_deploy_gid) is None
    ):
        raise CanaryGuardError(
            "candidate evidence lacks positive pinned deploy identities",
        )
    if (
        int(runtime_deploy_uid) != deploy_uid
        or int(runtime_deploy_gid) != deploy_gid
    ):
        raise CanaryGuardError(
            "candidate runtime deploy identity differs from trusted "
            "materialization",
        )
    required_environment = {
        "APP_DATABASE_ENABLED": "false",
        "APP_REDIS_ENABLED": "false",
        "APP_JATO_MONTHLY_ENABLED": "false",
        "APP_JATO_MONTHLY_EXECUTION_MODE": "disabled",
        "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED": "false",
        "APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED": "false",
        "APP_METADATA_PREWARM_ENABLED": "false",
        "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED": "false",
        "HERMES_RUN_ENABLED": "false",
    }
    missing = [
        f"{key}={expected}"
        for key, expected in required_environment.items()
        if environment.get(key) != expected
    ]
    if missing:
        raise CanaryGuardError(
            "candidate evidence omitted disabled subsystem flags: "
            + ", ".join(missing),
        )
    supervisor_generation = environment.get(
        "CANARY_SUPERVISOR_INVOCATION_ID",
        "",
    )
    if (
        re.fullmatch(r"[0-9a-f]{32}", supervisor_generation) is None
        or supervisor_generation == "0" * 32
    ):
        raise CanaryGuardError(
            "candidate evidence lacks the original supervisor generation",
        )
    candidate_generation = payload.get("candidateInvocationId")
    systemd_candidate_generation = systemd.get("InvocationID")
    if (
        not isinstance(candidate_generation, str)
        or not isinstance(systemd_candidate_generation, str)
        or re.fullmatch(r"[0-9a-f]{32}", candidate_generation) is None
        or candidate_generation == "0" * 32
        or systemd_candidate_generation.lower() != candidate_generation
    ):
        raise CanaryGuardError(
            "candidate evidence lacks its exact transient generation",
        )
    start_permit = payload.get("startPermit")
    expected_candidate_unit = (
        "jato-feature-canary-"
        f"{identity['commit'][:12]}-{identity['runId']}.service"
    )
    if (
        not isinstance(start_permit, dict)
        or start_permit
        != {
            "supervisorInvocationId": supervisor_generation,
            "candidateInvocationId": candidate_generation,
            "unit": expected_candidate_unit,
        }
    ):
        raise CanaryGuardError(
            "candidate evidence lacks the exact root-owned start permit",
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


def verify_ordered_checkpoint_markers(
    checkpoint: dict[str, Any],
    identity: dict[str, Any],
    *,
    phases: tuple[str, ...],
) -> None:
    events = checkpoint.get("events")
    if not isinstance(events, list):
        raise CanaryGuardError("checkpoint marker events are malformed")
    marker_indexes: list[int] = []
    for phase in phases:
        verify_checkpoint_marker(
            checkpoint,
            identity,
            phase=phase,
            status="completed",
        )
        marker_indexes.append(
            next(
                index
                for index, event in enumerate(events)
                if isinstance(event, dict)
                and event.get("phase") == phase
                and event.get("status") == "completed"
            ),
        )
    if marker_indexes != sorted(marker_indexes) or (
        len(set(marker_indexes)) != len(marker_indexes)
    ):
        raise CanaryGuardError(
            "durable canary source, candidate and terminal markers "
            "are out of order",
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
    ordered_phases = (
        "source_anchored",
        "source_verified",
        "candidate_verified",
        marker_phase,
        controller_terminal_phase,
        "supervisor_reconciled",
    )
    verify_ordered_checkpoint_markers(
        checkpoint,
        identity,
        phases=ordered_phases,
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
        marker_phase = (
            "fault_observed"
            if outcome == "expected_failure_verified"
            else "controller_completed"
        )
        verify_ordered_checkpoint_markers(
            checkpoint,
            identity,
            phases=(
                "source_anchored",
                "source_verified",
                "candidate_verified",
                marker_phase,
                controller_terminal_phase,
                "supervisor_reconciled",
            ),
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


def cleanup_launch_state(
    *,
    state_root: Path,
    run_key: str,
    expected_uid: int,
    expected_gid: int,
    anchor: Path = Path("/var/lib"),
    expected_anchor_uid: int = 0,
    expected_anchor_gid: int = 0,
) -> None:
    if (
        not state_root.is_absolute()
        or not anchor.is_absolute()
        or state_root.parent != anchor
        or state_root.name != "jato-canary"
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", run_key)
        is None
        or min(
            expected_uid,
            expected_gid,
            expected_anchor_uid,
            expected_anchor_gid,
        )
        < 0
    ):
        raise CanaryGuardError(
            "refusing non-canonical canary state cleanup",
        )
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptors: list[int] = []

    def verify_directory(
        descriptor: int,
        *,
        label: str,
        uid: int,
        gid: int,
        exact_mode: int | None,
    ) -> None:
        metadata = os.fstat(descriptor)
        mode = stat.S_IMODE(metadata.st_mode)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != uid
            or metadata.st_gid != gid
            or (exact_mode is None and mode & 0o022)
            or (exact_mode is not None and mode != exact_mode)
        ):
            raise CanaryGuardError(
                "refusing cleanup through unsafe canary state directory: "
                f"{label}",
            )

    def open_directory(
        name: str,
        *,
        parent_fd: int | None,
        label: str,
        uid: int,
        gid: int,
        exact_mode: int | None,
    ) -> int:
        try:
            descriptor = os.open(name, directory_flags, dir_fd=parent_fd)
        except OSError as exc:
            raise CanaryGuardError(
                "refusing cleanup through unavailable or linked canary "
                f"state directory: {label}",
            ) from exc
        descriptors.append(descriptor)
        verify_directory(
            descriptor,
            label=label,
            uid=uid,
            gid=gid,
            exact_mode=exact_mode,
        )
        return descriptor

    try:
        anchor_fd = open_directory(
            str(anchor),
            parent_fd=None,
            label=str(anchor),
            uid=expected_anchor_uid,
            gid=expected_anchor_gid,
            exact_mode=None,
        )
        root_fd = open_directory(
            state_root.name,
            parent_fd=anchor_fd,
            label=str(state_root),
            uid=expected_uid,
            gid=expected_gid,
            exact_mode=0o750,
        )
        state_directories = {
            "": root_fd,
            "checkpoints": open_directory(
                "checkpoints",
                parent_fd=root_fd,
                label=str(state_root / "checkpoints"),
                uid=expected_uid,
                gid=expected_gid,
                exact_mode=0o750,
            ),
            "receipts": open_directory(
                "receipts",
                parent_fd=root_fd,
                label=str(state_root / "receipts"),
                uid=expected_uid,
                gid=expected_gid,
                exact_mode=0o750,
            ),
            "evidence": open_directory(
                "evidence",
                parent_fd=root_fd,
                label=str(state_root / "evidence"),
                uid=expected_uid,
                gid=expected_gid,
                exact_mode=0o750,
            ),
            "snapshots": open_directory(
                "snapshots",
                parent_fd=root_fd,
                label=str(state_root / "snapshots"),
                uid=expected_uid,
                gid=expected_gid,
                exact_mode=0o750,
            ),
        }
        cleanup_entries = (
            ("checkpoints", f"{run_key}.json"),
            ("receipts", f"{run_key}.json"),
            ("evidence", f"{run_key}.json"),
            ("snapshots", f"{run_key}.before.json"),
            ("snapshots", f"{run_key}.after.json"),
            ("", f".{run_key}.supervisor-invocation-id.source"),
            ("", f".{run_key}.candidate-start-permit.source"),
        )
        for directory_name, entry_name in cleanup_entries:
            directory_fd = state_directories[directory_name]
            try:
                metadata = os.stat(
                    entry_name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise CanaryGuardError(
                    f"canary state residue cannot be inspected: {entry_name}",
                ) from exc
            if not (
                stat.S_ISREG(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
            ):
                raise CanaryGuardError(
                    "refusing to unlink non-file canary state residue: "
                    f"{directory_name}/{entry_name}",
                )
            try:
                os.unlink(entry_name, dir_fd=directory_fd)
            except OSError as exc:
                raise CanaryGuardError(
                    f"canary state residue cannot be unlinked: {entry_name}",
                ) from exc
        for directory_name, entry_name in cleanup_entries:
            try:
                os.stat(
                    entry_name,
                    dir_fd=state_directories[directory_name],
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise CanaryGuardError(
                    f"canary state cleanup cannot be verified: {entry_name}",
                ) from exc
            raise CanaryGuardError(
                "pre-supervisor canary state residue remained: "
                f"{directory_name}/{entry_name}",
            )
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


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

    cleanup_state = commands.add_parser("cleanup-launch-state")
    cleanup_state.add_argument("--state-root", type=Path, required=True)
    cleanup_state.add_argument("--run-key", required=True)
    cleanup_state.add_argument("--expected-uid", type=int, required=True)
    cleanup_state.add_argument("--expected-gid", type=int, required=True)

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
        elif arguments.command == "cleanup-launch-state":
            if arguments.state_root != Path("/var/lib/jato-canary"):
                raise CanaryGuardError(
                    "canary launch state cleanup root is not reviewed",
                )
            cleanup_launch_state(
                state_root=arguments.state_root,
                run_key=arguments.run_key,
                expected_uid=arguments.expected_uid,
                expected_gid=arguments.expected_gid,
            )
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
