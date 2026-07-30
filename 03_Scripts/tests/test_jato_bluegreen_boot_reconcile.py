from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "03_Scripts/deploy/jato_bluegreen_boot_reconcile.py"
UNIT = ROOT / "03_Scripts/deploy/systemd/jato-bluegreen-boot-reconcile.service"
SPEC = importlib.util.spec_from_file_location("jato_bluegreen_boot_reconcile", HELPER)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
TEST_RELEASE_SHA = "a" * 40


def active_release(slot: str) -> str:
    return f"""# Managed route
upstream jato_fullstack_api {{
    server 127.0.0.1:{slot} max_fails=3 fail_timeout=30s;
    keepalive 32;
}}
"""


def active_frontend_map(root: str) -> str:
    return f"""map $host $jato_frontend_root {{
    default "{root}";
}}
"""


def test_parses_exact_active_frontend_root() -> None:
    root = (
        "/opt/jato/releases/"
        f"{'a' * 40}/{'b' * 64}/06_AppPlatform/frontend/dist"
    )

    assert MODULE.parse_active_frontend_root(active_frontend_map(root)) == Path(root)


@pytest.mark.parametrize(
    "payload",
    [
        active_frontend_map("../06_AppPlatform/frontend/dist"),
        active_frontend_map("/tmp/release/06_AppPlatform/frontend/dist"),
        """map $host $jato_frontend_root {
            default "/opt/one/06_AppPlatform/frontend/dist";
            example.com "/opt/two/06_AppPlatform/frontend/dist";
        }""",
        (
            active_frontend_map("/opt/one/06_AppPlatform/frontend/dist")
            + active_frontend_map("/opt/two/06_AppPlatform/frontend/dist")
        ),
    ],
)
def test_rejects_ambiguous_or_unsafe_active_frontend_root(payload: str) -> None:
    with pytest.raises(MODULE.ReconcileError):
        MODULE.parse_active_frontend_root(payload)


def test_active_frontend_reader_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.conf"
    target.write_text(
        active_frontend_map("/opt/legacy/06_AppPlatform/frontend/dist"),
        encoding="utf-8",
    )
    link = tmp_path / "active-release.conf"
    link.symlink_to(target)

    with pytest.raises(MODULE.ReconcileError, match="unsafe"):
        MODULE.read_active_frontend_root(link)


def write_slot_env(
    tmp_path: Path,
    slot: str,
    *,
    release_sha: str = TEST_RELEASE_SHA,
) -> Path:
    slot_env_dir = tmp_path / "slots"
    slot_env_dir.mkdir(exist_ok=True)
    (slot_env_dir / f"{slot}.env").write_text(
        f"APP_RELEASE_SLOT={slot}\nAPP_RELEASE_SHA={release_sha}\n",
        encoding="utf-8",
    )
    return slot_env_dir


def ready_probe(url: str, timeout_seconds: float) -> dict[str, object]:
    assert timeout_seconds > 0
    if url.endswith("/healthz"):
        return {"status": "ok"}
    if url.endswith("/readyz"):
        return {
            "status": "ready",
            "release": {"commitSha": TEST_RELEASE_SHA},
        }
    raise AssertionError(f"unexpected probe URL: {url}")


class FakeSystemctl:
    def __init__(
        self,
        *,
        states: dict[str, str],
        fail_operation: str | None = None,
        converge: bool = True,
    ) -> None:
        self.states = states
        self.fail_operation = fail_operation
        self.converge = converge
        self.commands: list[tuple[str, str]] = []

    def no_block(self, operation: str, unit: str) -> None:
        self.commands.append((operation, unit))
        if operation == self.fail_operation:
            raise MODULE.ReconcileError(f"injected {operation} failure")
        if not self.converge:
            return
        if operation == "stop":
            self.states[unit] = "inactive"
        elif operation == "start":
            self.states[unit] = "active"

    def active_state(self, unit: str) -> str:
        return self.states[unit]


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.mark.parametrize(
    ("slot", "other"),
    [("8000", "8001"), ("8001", "8000")],
)
def test_reconciles_old_or_new_slot_from_nginx_only(
    tmp_path: Path,
    slot: str,
    other: str,
) -> None:
    active_conf = tmp_path / "active-release.conf"
    active_conf.write_text(active_release(slot), encoding="utf-8")
    active_slot = tmp_path / "active-slot"
    active_slot.write_text(f"{other}\n", encoding="utf-8")
    systemctl = FakeSystemctl(
        states={
            f"jato-fullstack-backend@{slot}": "inactive",
            f"jato-fullstack-backend@{other}": "active",
        }
    )

    result = MODULE.reconcile(
        active_release_conf=active_conf,
        active_slot_file=active_slot,
        slot_env_dir=write_slot_env(tmp_path, slot),
        systemctl=systemctl,
        timeout_seconds=1,
        poll_interval_seconds=0.01,
        request_timeout_seconds=0.1,
        probe=ready_probe,
    )

    assert result == slot
    assert active_slot.read_text(encoding="utf-8") == f"{slot}\n"
    assert systemctl.commands == [
        ("stop", f"jato-fullstack-backend@{other}"),
        ("start", f"jato-fullstack-backend@{slot}"),
    ]
    assert systemctl.states == {
        f"jato-fullstack-backend@{slot}": "active",
        f"jato-fullstack-backend@{other}": "inactive",
    }


def test_reboot_after_route_install_before_candidate_enable_starts_routed_slot(
    tmp_path: Path,
) -> None:
    """Model the crash window after Nginx route persistence but before start."""

    active_conf = tmp_path / "active-release.conf"
    active_conf.write_text(active_release("8001"), encoding="utf-8")
    active_slot = tmp_path / "active-slot"
    active_slot.write_text("8000\n", encoding="utf-8")
    systemctl = FakeSystemctl(
        states={
            "jato-fullstack-backend@8000": "inactive",
            "jato-fullstack-backend@8001": "inactive",
        }
    )

    result = MODULE.reconcile(
        active_release_conf=active_conf,
        active_slot_file=active_slot,
        slot_env_dir=write_slot_env(tmp_path, "8001"),
        systemctl=systemctl,
        timeout_seconds=1,
        poll_interval_seconds=0.01,
        request_timeout_seconds=0.1,
        probe=ready_probe,
    )

    assert result == "8001"
    assert active_slot.read_text(encoding="utf-8") == "8001\n"
    assert systemctl.commands == [
        ("stop", "jato-fullstack-backend@8000"),
        ("start", "jato-fullstack-backend@8001"),
    ]
    assert systemctl.states == {
        "jato-fullstack-backend@8000": "inactive",
        "jato-fullstack-backend@8001": "active",
    }


def test_active_service_waits_until_exact_release_is_ready(
    tmp_path: Path,
) -> None:
    active_conf = tmp_path / "active-release.conf"
    active_conf.write_text(active_release("8001"), encoding="utf-8")
    active_slot = tmp_path / "active-slot"
    active_slot.write_text("8000\n", encoding="utf-8")
    systemctl = FakeSystemctl(
        states={
            "jato-fullstack-backend@8000": "inactive",
            "jato-fullstack-backend@8001": "active",
        }
    )
    clock = FakeClock()
    ready_attempts = 0

    def starting_then_ready(
        url: str,
        timeout_seconds: float,
    ) -> dict[str, object]:
        nonlocal ready_attempts
        assert 0 < timeout_seconds <= 0.2
        if url.endswith("/healthz"):
            return {"status": "ok"}
        ready_attempts += 1
        if ready_attempts == 1:
            return {
                "status": "starting",
                "release": {"commitSha": TEST_RELEASE_SHA},
            }
        return {
            "status": "ready",
            "release": {"commitSha": TEST_RELEASE_SHA},
        }

    result = MODULE.reconcile(
        active_release_conf=active_conf,
        active_slot_file=active_slot,
        slot_env_dir=write_slot_env(tmp_path, "8001"),
        systemctl=systemctl,
        timeout_seconds=1,
        poll_interval_seconds=0.1,
        request_timeout_seconds=0.2,
        probe=starting_then_ready,
        monotonic=clock.monotonic,
        sleeper=clock.sleep,
    )

    assert result == "8001"
    assert ready_attempts == 2
    assert clock.now == pytest.approx(0.1)
    assert active_slot.read_text(encoding="utf-8") == "8001\n"


def test_wrong_ready_release_sha_times_out_without_rewriting_slot(
    tmp_path: Path,
) -> None:
    active_conf = tmp_path / "active-release.conf"
    active_conf.write_text(active_release("8001"), encoding="utf-8")
    active_slot = tmp_path / "active-slot"
    active_slot.write_text("8000\n", encoding="utf-8")
    clock = FakeClock()

    def wrong_release(url: str, timeout_seconds: float) -> dict[str, object]:
        assert timeout_seconds > 0
        if url.endswith("/healthz"):
            return {"status": "ok"}
        return {
            "status": "ready",
            "release": {"commitSha": "b" * 40},
        }

    with pytest.raises(
        MODULE.ReconcileError,
        match=r"timed out.*release\.commitSha does not match",
    ):
        MODULE.reconcile(
            active_release_conf=active_conf,
            active_slot_file=active_slot,
            slot_env_dir=write_slot_env(tmp_path, "8001"),
            systemctl=FakeSystemctl(
                states={
                    "jato-fullstack-backend@8000": "inactive",
                    "jato-fullstack-backend@8001": "active",
                }
            ),
            timeout_seconds=0.3,
            poll_interval_seconds=0.1,
            request_timeout_seconds=0.1,
            probe=wrong_release,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
        )

    assert active_slot.read_text(encoding="utf-8") == "8000\n"


def test_continuous_probe_failure_times_out_without_rewriting_slot(
    tmp_path: Path,
) -> None:
    active_conf = tmp_path / "active-release.conf"
    active_conf.write_text(active_release("8001"), encoding="utf-8")
    active_slot = tmp_path / "active-slot"
    active_slot.write_text("8000\n", encoding="utf-8")
    clock = FakeClock()

    def unavailable(url: str, timeout_seconds: float) -> dict[str, object]:
        raise MODULE.ReconcileError(f"{url} connection refused")

    with pytest.raises(
        MODULE.ReconcileError,
        match="timed out.*connection refused",
    ):
        MODULE.reconcile(
            active_release_conf=active_conf,
            active_slot_file=active_slot,
            slot_env_dir=write_slot_env(tmp_path, "8001"),
            systemctl=FakeSystemctl(
                states={
                    "jato-fullstack-backend@8000": "inactive",
                    "jato-fullstack-backend@8001": "active",
                }
            ),
            timeout_seconds=0.3,
            poll_interval_seconds=0.1,
            request_timeout_seconds=0.1,
            probe=unavailable,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
        )

    assert active_slot.read_text(encoding="utf-8") == "8000\n"


def test_rejects_symlink_active_release(tmp_path: Path) -> None:
    real = tmp_path / "real.conf"
    real.write_text(active_release("8000"), encoding="utf-8")
    active_conf = tmp_path / "active-release.conf"
    active_conf.symlink_to(real)

    with pytest.raises(MODULE.ReconcileError, match="missing or unsafe"):
        MODULE.read_active_slot(active_conf)


@pytest.mark.parametrize(
    "payload",
    [
        """
upstream jato_fullstack_api {
    server 127.0.0.1:8000;
    server 127.0.0.1:8001;
}
""",
        """
upstream jato_fullstack_api { server 127.0.0.1:8000; }
upstream jato_fullstack_api { server 127.0.0.1:8001; }
""",
        "upstream jato_fullstack_api { server 127.0.0.1:9000; }",
        "upstream another_api { server 127.0.0.1:8000; }",
    ],
)
def test_rejects_ambiguous_or_invalid_route(
    tmp_path: Path,
    payload: str,
) -> None:
    active_conf = tmp_path / "active-release.conf"
    active_conf.write_text(payload, encoding="utf-8")

    with pytest.raises(MODULE.ReconcileError):
        MODULE.read_active_slot(active_conf)


def test_rejects_nonregular_and_oversized_active_release(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "active-release-directory"
    directory.mkdir()
    with pytest.raises(MODULE.ReconcileError, match="not a regular file"):
        MODULE.read_active_slot(directory)

    oversized = tmp_path / "oversized.conf"
    oversized.write_bytes(b"x" * (MODULE.MAX_ACTIVE_RELEASE_BYTES + 1))
    with pytest.raises(MODULE.ReconcileError, match="size limit"):
        MODULE.read_active_slot(oversized)


@pytest.mark.parametrize(
    "payload",
    [
        "",
        f"APP_RELEASE_SHA={TEST_RELEASE_SHA}\nAPP_RELEASE_SHA={TEST_RELEASE_SHA}\n",
        "APP_RELEASE_SHA=main\n",
        f'APP_RELEASE_SHA="{TEST_RELEASE_SHA}"\n',
        f"APP_RELEASE_SHA={TEST_RELEASE_SHA} # comment\n",
    ],
)
def test_rejects_missing_ambiguous_or_unsafe_slot_release_sha(
    tmp_path: Path,
    payload: str,
) -> None:
    slot_env_dir = tmp_path / "slots"
    slot_env_dir.mkdir()
    (slot_env_dir / "8001.env").write_text(payload, encoding="utf-8")

    with pytest.raises(MODULE.ReconcileError):
        MODULE.read_slot_release_sha(slot_env_dir, "8001")


def test_rejects_symlink_and_oversized_slot_env(tmp_path: Path) -> None:
    slot_env_dir = tmp_path / "slots"
    slot_env_dir.mkdir()
    real = tmp_path / "real.env"
    real.write_text(
        f"APP_RELEASE_SHA={TEST_RELEASE_SHA}\n",
        encoding="utf-8",
    )
    (slot_env_dir / "8001.env").symlink_to(real)
    with pytest.raises(MODULE.ReconcileError, match="missing or unsafe"):
        MODULE.read_slot_release_sha(slot_env_dir, "8001")

    (slot_env_dir / "8001.env").unlink()
    (slot_env_dir / "8001.env").write_bytes(
        b"x" * (MODULE.MAX_SLOT_ENV_BYTES + 1)
    )
    with pytest.raises(MODULE.ReconcileError, match="size limit"):
        MODULE.read_slot_release_sha(slot_env_dir, "8001")


@pytest.mark.parametrize("failure", ["stop", "start"])
def test_command_failure_keeps_existing_active_slot(
    tmp_path: Path,
    failure: str,
) -> None:
    active_conf = tmp_path / "active-release.conf"
    active_conf.write_text(active_release("8001"), encoding="utf-8")
    active_slot = tmp_path / "active-slot"
    active_slot.write_text("8000\n", encoding="utf-8")
    systemctl = FakeSystemctl(
        states={
            "jato-fullstack-backend@8000": "active",
            "jato-fullstack-backend@8001": "inactive",
        },
        fail_operation=failure,
    )

    with pytest.raises(MODULE.ReconcileError, match="injected"):
        MODULE.reconcile(
            active_release_conf=active_conf,
            active_slot_file=active_slot,
            slot_env_dir=write_slot_env(tmp_path, "8001"),
            systemctl=systemctl,
            timeout_seconds=1,
            poll_interval_seconds=0.01,
            request_timeout_seconds=0.1,
            probe=ready_probe,
        )

    assert active_slot.read_text(encoding="utf-8") == "8000\n"


def test_timeout_keeps_existing_active_slot(tmp_path: Path) -> None:
    active_conf = tmp_path / "active-release.conf"
    active_conf.write_text(active_release("8001"), encoding="utf-8")
    active_slot = tmp_path / "active-slot"
    active_slot.write_text("8000\n", encoding="utf-8")
    systemctl = FakeSystemctl(
        states={
            "jato-fullstack-backend@8000": "active",
            "jato-fullstack-backend@8001": "inactive",
        },
        converge=False,
    )
    ticks = iter([0.0, 1.0])

    with pytest.raises(MODULE.ReconcileError, match="timed out"):
        MODULE.reconcile(
            active_release_conf=active_conf,
            active_slot_file=active_slot,
            slot_env_dir=write_slot_env(tmp_path, "8001"),
            systemctl=systemctl,
            timeout_seconds=0.5,
            poll_interval_seconds=0.01,
            request_timeout_seconds=0.1,
            probe=ready_probe,
            monotonic=lambda: next(ticks),
            sleeper=lambda _: None,
        )

    assert active_slot.read_text(encoding="utf-8") == "8000\n"


def test_systemctl_adapter_uses_no_block_and_strict_state_contract() -> None:
    calls: list[list[str]] = []

    def runner(arguments: list[str]) -> MODULE.CommandResult:
        calls.append(arguments)
        if arguments[1] == "is-active":
            return MODULE.CommandResult(3, "inactive\n", "")
        return MODULE.CommandResult(0, "", "")

    systemctl = MODULE.Systemctl(Path("/fake/systemctl"), runner=runner)
    systemctl.no_block("stop", "jato-fullstack-backend@8001")
    assert systemctl.active_state("jato-fullstack-backend@8001") == "inactive"
    assert calls == [
        [
            "/fake/systemctl",
            "stop",
            "--no-block",
            "jato-fullstack-backend@8001",
        ],
        [
            "/fake/systemctl",
            "is-active",
            "jato-fullstack-backend@8001",
        ],
    ]


def test_systemctl_adapter_rejects_command_failure() -> None:
    def runner(arguments: list[str]) -> MODULE.CommandResult:
        return MODULE.CommandResult(1, "", "transaction rejected")

    systemctl = MODULE.Systemctl(Path("/fake/systemctl"), runner=runner)
    with pytest.raises(MODULE.ReconcileError, match="transaction rejected"):
        systemctl.no_block("start", "jato-fullstack-backend@8000")


def test_atomic_write_replaces_symlink_without_following_it(
    tmp_path: Path,
) -> None:
    victim = tmp_path / "victim"
    victim.write_text("do-not-change\n", encoding="utf-8")
    active_slot = tmp_path / "active-slot"
    active_slot.symlink_to(victim)

    MODULE.atomic_write_active_slot(active_slot, "8001")

    assert not active_slot.is_symlink()
    assert active_slot.read_text(encoding="utf-8") == "8001\n"
    assert victim.read_text(encoding="utf-8") == "do-not-change\n"


def test_systemd_timeout_exceeds_bounded_helper_deadline() -> None:
    unit = UNIT.read_text(encoding="utf-8")
    assert "--slot-env-dir /etc/jato-fullstack/slots" in unit
    assert "--timeout-seconds 45" in unit
    assert "--request-timeout-seconds 2" in unit
    assert "TimeoutStartSec=60" in unit

    options = MODULE.build_parser().parse_args(
        [
            "--nginx-active-release-conf",
            "/tmp/active-release.conf",
        ]
    )
    assert options.active_release_conf == Path("/tmp/active-release.conf")
    assert options.timeout_seconds < 60
