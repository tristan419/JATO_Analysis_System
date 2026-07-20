from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "jato_monthly_worker.py"
SPEC = importlib.util.spec_from_file_location("jato_monthly_worker", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
WORKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKER)


def test_required_resource_limit_clamps_to_existing_hard_limit() -> None:
    calls: list[tuple[int, tuple[int, int]]] = []
    resource_module = SimpleNamespace(
        RLIM_INFINITY=-1,
        getrlimit=lambda _kind: (8_000, 2_000),
        setrlimit=lambda kind, limits: calls.append((kind, limits)),
    )

    WORKER._set_required_resource_limit(
        resource_module,
        9,
        4_000,
        label="test",
    )

    assert calls == [(9, (2_000, 2_000))]


def test_required_resource_limit_fails_closed() -> None:
    resource_module = SimpleNamespace(
        RLIM_INFINITY=-1,
        getrlimit=lambda _kind: (8_000, -1),
        setrlimit=lambda _kind, _limits: (_ for _ in ()).throw(
            ValueError("unsupported")
        ),
    )

    with pytest.raises(RuntimeError, match="Unable to enforce"):
        WORKER._set_required_resource_limit(
            resource_module,
            9,
            4_000,
            label="test",
        )
