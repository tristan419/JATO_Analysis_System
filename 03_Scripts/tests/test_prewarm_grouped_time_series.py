from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "diagnostics"
    / "prewarm_grouped_time_series.py"
)


def load_module():
    module_name = "prewarm_grouped_time_series_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


prewarm = load_module()


def test_build_prewarm_requests_covers_default_dashboard_combinations() -> None:
    requests = prewarm.build_prewarm_requests(
        countries=prewarm.DEFAULT_COUNTRIES,
        powertrains=prewarm.DEFAULT_POWERTRAINS,
        group_bys=prewarm.DEFAULT_GROUP_BYS,
        grains=prewarm.DEFAULT_GRAINS,
        top_n=10,
        include_others=False,
    )

    assert [item.label for item in requests] == ["month:动总规整", "month:国家"]
    assert requests[0].payload["filters"]["国家"] == prewarm.DEFAULT_COUNTRIES
    assert requests[0].payload["filters"]["动总规整"] == ["ICE", "HEV", "BEV", "MHEV", "PHEV"]
    assert requests[0].payload["top_n"] == 10
    assert requests[0].payload["include_others"] is False


def test_validate_attempts_can_require_repeat_cache_hit() -> None:
    attempts = [
        prewarm.PrewarmAttempt(
            label="month:国家",
            attempt=1,
            status=200,
            seconds=1.2,
            rows=12,
            server_cache="MISS",
            edge_cache="MISS",
        ),
        prewarm.PrewarmAttempt(
            label="month:国家",
            attempt=2,
            status=200,
            seconds=0.1,
            rows=12,
            server_cache="MEMORY",
            edge_cache="HIT",
        ),
    ]

    assert prewarm.validate_attempts(
        attempts,
        require_server_cache=True,
        require_repeat_hit=True,
    ) == []


def test_validate_attempts_reports_cold_repeat() -> None:
    attempts = [
        prewarm.PrewarmAttempt(
            label="month:动总规整",
            attempt=2,
            status=200,
            seconds=2.0,
            rows=12,
            server_cache="MISS",
            edge_cache="",
        )
    ]

    errors = prewarm.validate_attempts(
        attempts,
        require_server_cache=True,
        require_repeat_hit=True,
    )

    assert errors == [
        "repeat request still returned MISS for: month:动总规整",
    ]
