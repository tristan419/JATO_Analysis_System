#!/usr/bin/env python3
"""Reconcile JATO blue/green backend services from the durable Nginx route.

The managed ``active-release.conf`` is deliberately the only boot-time source
of truth.  The derived ``active-slot`` file is rewritten only after exactly
the routed backend is active, healthy, ready, and bound to the immutable SHA
stored in its generated slot environment.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from typing import Any, NamedTuple
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener


DEFAULT_ACTIVE_RELEASE_CONF = Path(
    "/etc/jato-fullstack/nginx/active-release.conf"
)
DEFAULT_ACTIVE_SLOT_FILE = Path("/var/lib/jato-release/active-slot")
DEFAULT_SLOT_ENV_DIR = Path("/etc/jato-fullstack/slots")
DEFAULT_SYSTEMCTL = Path("/usr/bin/systemctl")
MAX_ACTIVE_RELEASE_BYTES = 64 * 1024
MAX_SLOT_ENV_BYTES = 64 * 1024
MAX_PROBE_RESPONSE_BYTES = 64 * 1024
VALID_SLOTS = ("8000", "8001")
SERVICE_PREFIX = "jato-fullstack-backend@"
RELEASE_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class ReconcileError(RuntimeError):
    """Raised when the durable route cannot be reconciled safely."""


def _read_safe_regular_file(
    path: Path,
    *,
    max_bytes: int,
    description: str = "active release include",
) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(
        os, "O_NOFOLLOW", 0
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ReconcileError(
            f"{description} is missing or unsafe: {path}: {exc}"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ReconcileError(
                f"{description} is not a regular file: {path}"
            )
        if metadata.st_size > max_bytes:
            raise ReconcileError(
                f"{description} exceeds the safety size limit: "
                f"{metadata.st_size} > {max_bytes}"
            )
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > max_bytes:
            raise ReconcileError(
                f"{description} grew beyond the safety size limit"
            )
    finally:
        os.close(descriptor)
    if b"\x00" in payload:
        raise ReconcileError(f"{description} contains a NUL byte")
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReconcileError(
            f"{description} is not valid UTF-8"
        ) from exc


def _strip_nginx_comments(payload: str) -> str:
    """Strip comments without treating a # inside a quoted string as syntax."""

    output: list[str] = []
    quote: str | None = None
    escaped = False
    index = 0
    while index < len(payload):
        character = payload[index]
        if escaped:
            output.append(character)
            escaped = False
        elif character == "\\" and quote is not None:
            output.append(character)
            escaped = True
        elif quote is not None:
            output.append(character)
            if character == quote:
                quote = None
        elif character in {'"', "'"}:
            output.append(character)
            quote = character
        elif character == "#":
            while index < len(payload) and payload[index] != "\n":
                index += 1
            if index < len(payload):
                output.append("\n")
        else:
            output.append(character)
        index += 1
    if quote is not None:
        raise ReconcileError("active release include has an unterminated quote")
    return "".join(output)


def parse_active_slot(payload: str) -> str:
    """Return the unique loopback slot routed by jato_fullstack_api."""

    clean = _strip_nginx_comments(payload)
    declaration = re.compile(r"\bupstream\s+jato_fullstack_api\b")
    if len(declaration.findall(clean)) != 1:
        raise ReconcileError(
            "active release include must declare exactly one "
            "upstream jato_fullstack_api"
        )
    block_pattern = re.compile(
        r"\bupstream\s+jato_fullstack_api\s*\{(?P<body>[^{}]*)\}",
        re.DOTALL,
    )
    blocks = list(block_pattern.finditer(clean))
    if len(blocks) != 1:
        raise ReconcileError(
            "jato_fullstack_api upstream is malformed or ambiguous"
        )

    body = blocks[0].group("body")
    if len(re.findall(r"\bserver\b", body)) != 1:
        raise ReconcileError(
            "jato_fullstack_api must contain exactly one server directive"
        )
    server_directives: list[list[str]] = []
    for raw_directive in body.split(";"):
        tokens = raw_directive.split()
        if tokens and tokens[0] == "server":
            server_directives.append(tokens)
    if len(server_directives) != 1:
        raise ReconcileError(
            "jato_fullstack_api must contain exactly one server directive"
        )
    server = server_directives[0]
    if len(server) < 2:
        raise ReconcileError("jato_fullstack_api server directive is malformed")
    match = re.fullmatch(r"127\.0\.0\.1:(8000|8001)", server[1])
    if match is None:
        raise ReconcileError(
            "jato_fullstack_api must route to 127.0.0.1:8000 or :8001"
        )
    return match.group(1)


def read_active_slot(
    active_release_conf: Path,
    *,
    max_bytes: int = MAX_ACTIVE_RELEASE_BYTES,
) -> str:
    return parse_active_slot(
        _read_safe_regular_file(
            active_release_conf,
            max_bytes=max_bytes,
            description="active release include",
        )
    )


def parse_active_frontend_root(payload: str) -> Path:
    """Return the unique frontend root bound to the active backend route."""

    clean = _strip_nginx_comments(payload)
    declaration = re.compile(
        r"\bmap\s+\$host\s+\$jato_frontend_root\b"
    )
    if len(declaration.findall(clean)) != 1:
        raise ReconcileError(
            "active release include must declare exactly one frontend root map"
        )
    block_pattern = re.compile(
        r"\bmap\s+\$host\s+\$jato_frontend_root\s*"
        r"\{(?P<body>[^{}]*)\}",
        re.DOTALL,
    )
    blocks = list(block_pattern.finditer(clean))
    if len(blocks) != 1:
        raise ReconcileError(
            "active frontend root map is malformed or ambiguous"
        )
    directives: list[list[str]] = []
    for raw_directive in blocks[0].group("body").split(";"):
        try:
            tokens = shlex.split(raw_directive, posix=True)
        except ValueError as exc:
            raise ReconcileError(
                "active frontend root directive has invalid quoting"
            ) from exc
        if tokens:
            directives.append(tokens)
    if len(directives) != 1 or len(directives[0]) != 2:
        raise ReconcileError(
            "active frontend root map must contain one default directive"
        )
    directive, raw_root = directives[0]
    root = Path(raw_root)
    if (
        directive != "default"
        or not root.is_absolute()
        or ".." in root.parts
        or not str(root).startswith("/opt/")
        or not str(root).endswith("/06_AppPlatform/frontend/dist")
    ):
        raise ReconcileError("active frontend root is unsafe")
    return root


def read_active_frontend_root(
    active_release_conf: Path,
    *,
    max_bytes: int = MAX_ACTIVE_RELEASE_BYTES,
) -> Path:
    return parse_active_frontend_root(
        _read_safe_regular_file(
            active_release_conf,
            max_bytes=max_bytes,
            description="active release include",
        )
    )


def parse_release_sha_from_slot_env(payload: str) -> str:
    """Read one unquoted full SHA from the generated per-slot env file."""

    declarations: list[str] = []
    for line in payload.splitlines():
        if not re.match(r"^[ \t]*APP_RELEASE_SHA[ \t]*=", line):
            continue
        declarations.append(line)
    if len(declarations) != 1:
        raise ReconcileError(
            "slot env must contain exactly one APP_RELEASE_SHA assignment"
        )
    match = re.fullmatch(
        r"[ \t]*APP_RELEASE_SHA[ \t]*=[ \t]*([0-9a-f]{40})[ \t]*",
        declarations[0],
    )
    if match is None:
        raise ReconcileError(
            "slot env APP_RELEASE_SHA must be one unquoted full lowercase git SHA"
        )
    release_sha = match.group(1)
    if not RELEASE_SHA_PATTERN.fullmatch(release_sha):
        raise ReconcileError("slot env APP_RELEASE_SHA is unsafe")
    return release_sha


def read_slot_release_sha(
    slot_env_dir: Path,
    slot: str,
    *,
    max_bytes: int = MAX_SLOT_ENV_BYTES,
) -> str:
    if slot not in VALID_SLOTS:
        raise ReconcileError(f"invalid slot env owner: {slot}")
    slot_env = slot_env_dir / f"{slot}.env"
    payload = _read_safe_regular_file(
        slot_env,
        max_bytes=max_bytes,
        description=f"slot {slot} environment",
    )
    return parse_release_sha_from_slot_env(payload)


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise ReconcileError("failed to write durable active-slot")
        offset += written


def atomic_write_active_slot(path: Path, slot: str) -> None:
    """Atomically replace active-slot and fsync both file and parent directory."""

    if slot not in VALID_SLOTS:
        raise ReconcileError(f"invalid active slot: {slot}")
    parent = path.parent
    parent_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(
        os, "O_NOFOLLOW", 0
    )
    try:
        parent_descriptor = os.open(parent, parent_flags)
    except OSError as exc:
        raise ReconcileError(
            f"active-slot parent is missing or unsafe: {parent}: {exc}"
        ) from exc

    temporary_name = f".{path.name}.boot-{os.getpid()}-{secrets.token_hex(8)}"
    temporary_descriptor: int | None = None
    try:
        create_flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        temporary_descriptor = os.open(
            temporary_name, create_flags, 0o600, dir_fd=parent_descriptor
        )
        _write_all(temporary_descriptor, f"{slot}\n".encode("ascii"))
        os.fchmod(temporary_descriptor, 0o644)
        os.fsync(temporary_descriptor)
        os.close(temporary_descriptor)
        temporary_descriptor = None
        os.replace(
            temporary_name,
            path.name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
        )
        os.fsync(parent_descriptor)
    except OSError as exc:
        raise ReconcileError(f"failed to install durable active-slot: {exc}") from exc
    finally:
        if temporary_descriptor is not None:
            os.close(temporary_descriptor)
        try:
            os.unlink(temporary_name, dir_fd=parent_descriptor)
        except FileNotFoundError:
            pass
        os.close(parent_descriptor)


class CommandResult(NamedTuple):
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[list[str]], CommandResult]
HttpProbe = Callable[[str, float], Mapping[str, Any]]


def subprocess_runner(arguments: list[str]) -> CommandResult:
    completed = subprocess.run(
        arguments,
        check=False,
        capture_output=True,
        text=True,
    )
    return CommandResult(
        completed.returncode,
        completed.stdout,
        completed.stderr,
    )


class Systemctl:
    """Small injectable systemctl adapter used by the boot reconciler."""

    def __init__(
        self,
        executable: Path = DEFAULT_SYSTEMCTL,
        *,
        runner: CommandRunner = subprocess_runner,
    ) -> None:
        self._executable = str(executable)
        self._runner = runner

    def _run(self, *arguments: str) -> CommandResult:
        return self._runner([self._executable, *arguments])

    def no_block(self, operation: str, unit: str) -> None:
        result = self._run(operation, "--no-block", unit)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "no detail"
            raise ReconcileError(
                f"systemctl {operation} failed for {unit}: "
                f"exit={result.returncode}: {detail}"
            )

    def active_state(self, unit: str) -> str:
        result = self._run("is-active", unit)
        state = result.stdout.strip()
        valid_results = {
            ("active", 0),
            ("inactive", 3),
            ("failed", 3),
            ("activating", 3),
            ("deactivating", 3),
        }
        if (state, result.returncode) not in valid_results:
            detail = result.stderr.strip() or "no detail"
            raise ReconcileError(
                f"cannot determine systemd state for {unit}: "
                f"state={state or 'empty'} exit={result.returncode}: {detail}"
            )
        return state


def read_json_endpoint(url: str, timeout_seconds: float) -> Mapping[str, Any]:
    """Read one bounded loopback JSON response without inheriting proxies."""

    if timeout_seconds <= 0:
        raise ReconcileError("backend probe timeout must be positive")
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "jato-bluegreen-boot-reconcile/1",
        },
    )
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                raise ReconcileError(
                    f"{url} returned unexpected HTTP {response.status}"
                )
            body = response.read(MAX_PROBE_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        raise ReconcileError(f"{url} returned HTTP {exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ReconcileError(f"{url} request failed: {exc}") from exc
    if len(body) > MAX_PROBE_RESPONSE_BYTES:
        raise ReconcileError(
            f"{url} response exceeds {MAX_PROBE_RESPONSE_BYTES} bytes"
        )
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReconcileError(f"{url} response is not valid UTF-8 JSON") from exc
    if not isinstance(payload, Mapping):
        raise ReconcileError(f"{url} response must be a JSON object")
    return payload


def verify_backend_probe_payloads(
    *,
    health_payload: Mapping[str, Any],
    readiness_payload: Mapping[str, Any],
    expected_commit: str,
) -> None:
    """Enforce health, readiness, and exact immutable release identity."""

    if not RELEASE_SHA_PATTERN.fullmatch(expected_commit):
        raise ReconcileError("expected backend release SHA is unsafe")
    if health_payload.get("status") != "ok":
        raise ReconcileError("backend /healthz status is not ok")
    if readiness_payload.get("status") != "ready":
        raise ReconcileError("backend /readyz status is not ready")
    release = readiness_payload.get("release")
    if not isinstance(release, Mapping):
        raise ReconcileError("backend /readyz release identity is missing")
    observed_commit = release.get("commitSha")
    if observed_commit != expected_commit:
        raise ReconcileError(
            "backend /readyz release.commitSha does not match slot env "
            f"APP_RELEASE_SHA: expected={expected_commit} "
            f"observed={observed_commit!r}"
        )


def wait_for_backend_ready(
    *,
    target_slot: str,
    expected_commit: str,
    deadline: float,
    poll_interval_seconds: float,
    request_timeout_seconds: float,
    probe: HttpProbe = read_json_endpoint,
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> None:
    """Poll both backend gates until exact-release readiness or deadline."""

    if target_slot not in VALID_SLOTS:
        raise ReconcileError(f"invalid backend probe slot: {target_slot}")
    if poll_interval_seconds <= 0:
        raise ReconcileError("backend probe poll interval must be positive")
    if request_timeout_seconds <= 0:
        raise ReconcileError("backend request timeout must be positive")

    base_url = f"http://127.0.0.1:{target_slot}"
    last_error = "backend did not answer"
    while True:
        now = monotonic()
        remaining = deadline - now
        if remaining <= 0:
            raise ReconcileError(
                "timed out waiting for exact-release backend readiness: "
                f"{last_error}"
            )
        try:
            health_payload = probe(
                f"{base_url}/healthz",
                min(request_timeout_seconds, remaining),
            )
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ReconcileError(
                    "overall boot reconciliation deadline expired after /healthz"
                )
            readiness_payload = probe(
                f"{base_url}/readyz",
                min(request_timeout_seconds, remaining),
            )
            verify_backend_probe_payloads(
                health_payload=health_payload,
                readiness_payload=readiness_payload,
                expected_commit=expected_commit,
            )
            return
        except ReconcileError as exc:
            last_error = str(exc)

        now = monotonic()
        if now >= deadline:
            raise ReconcileError(
                "timed out waiting for exact-release backend readiness: "
                f"{last_error}"
            )
        sleeper(min(poll_interval_seconds, max(0.0, deadline - now)))


def wait_for_exclusive_active(
    systemctl: Systemctl,
    *,
    target_unit: str,
    non_target_unit: str,
    timeout_seconds: float,
    poll_interval_seconds: float,
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> None:
    if timeout_seconds <= 0:
        raise ReconcileError("systemd reconciliation timeout must be positive")
    if poll_interval_seconds <= 0:
        raise ReconcileError("systemd poll interval must be positive")
    deadline = monotonic() + timeout_seconds
    last_target = "unknown"
    last_non_target = "unknown"
    while True:
        last_target = systemctl.active_state(target_unit)
        last_non_target = systemctl.active_state(non_target_unit)
        if last_target == "active" and last_non_target in {"inactive", "failed"}:
            return
        if last_target == "failed":
            raise ReconcileError(f"target backend failed to start: {target_unit}")
        now = monotonic()
        if now >= deadline:
            raise ReconcileError(
                "timed out waiting for exclusive backend ownership: "
                f"{target_unit}={last_target}, "
                f"{non_target_unit}={last_non_target}"
            )
        sleeper(min(poll_interval_seconds, max(0.0, deadline - now)))


def reconcile(
    *,
    active_release_conf: Path,
    active_slot_file: Path,
    slot_env_dir: Path,
    systemctl: Systemctl,
    timeout_seconds: float,
    poll_interval_seconds: float,
    request_timeout_seconds: float,
    probe: HttpProbe = read_json_endpoint,
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> str:
    if timeout_seconds <= 0:
        raise ReconcileError("boot reconciliation timeout must be positive")
    deadline = monotonic() + timeout_seconds
    target_slot = read_active_slot(active_release_conf)
    expected_commit = read_slot_release_sha(slot_env_dir, target_slot)
    non_target_slot = "8001" if target_slot == "8000" else "8000"
    target_unit = f"{SERVICE_PREFIX}{target_slot}"
    non_target_unit = f"{SERVICE_PREFIX}{non_target_slot}"

    # --no-block is mandatory here: this helper itself runs as a systemd
    # oneshot ordered before Nginx, so waiting on a nested transaction can
    # deadlock the boot transaction.
    systemctl.no_block("stop", non_target_unit)
    systemctl.no_block("start", target_unit)
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise ReconcileError(
            "boot reconciliation timed out before systemd converged"
        )
    wait_for_exclusive_active(
        systemctl,
        target_unit=target_unit,
        non_target_unit=non_target_unit,
        timeout_seconds=remaining,
        poll_interval_seconds=poll_interval_seconds,
        monotonic=monotonic,
        sleeper=sleeper,
    )
    wait_for_backend_ready(
        target_slot=target_slot,
        expected_commit=expected_commit,
        deadline=deadline,
        poll_interval_seconds=poll_interval_seconds,
        request_timeout_seconds=request_timeout_seconds,
        probe=probe,
        monotonic=monotonic,
        sleeper=sleeper,
    )
    atomic_write_active_slot(active_slot_file, target_slot)
    return target_slot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile JATO blue/green services from active-release.conf "
            "before Nginx starts."
        )
    )
    parser.add_argument(
        "--active-release-conf",
        "--nginx-active-release-conf",
        dest="active_release_conf",
        type=Path,
        default=DEFAULT_ACTIVE_RELEASE_CONF,
    )
    parser.add_argument(
        "--active-slot-file",
        type=Path,
        default=DEFAULT_ACTIVE_SLOT_FILE,
    )
    parser.add_argument(
        "--slot-env-dir",
        type=Path,
        default=DEFAULT_SLOT_ENV_DIR,
    )
    parser.add_argument("--systemctl", type=Path, default=DEFAULT_SYSTEMCTL)
    # Keep this below the systemd unit's TimeoutStartSec so the helper can
    # report a precise fail-closed reason before systemd terminates it.
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.25)
    parser.add_argument("--request-timeout-seconds", type=float, default=2.0)
    return parser


def main(arguments: list[str] | None = None) -> int:
    options = build_parser().parse_args(arguments)
    try:
        slot = reconcile(
            active_release_conf=options.active_release_conf,
            active_slot_file=options.active_slot_file,
            slot_env_dir=options.slot_env_dir,
            systemctl=Systemctl(options.systemctl),
            timeout_seconds=options.timeout_seconds,
            poll_interval_seconds=options.poll_interval_seconds,
            request_timeout_seconds=options.request_timeout_seconds,
        )
    except ReconcileError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    print(f"[INFO] reconciled JATO blue/green boot owner to slot {slot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
