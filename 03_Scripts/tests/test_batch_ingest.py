from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "batch_ingest.py"


def load_module():
    module_name = "batch_ingest_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


batch_ingest = load_module()


def test_batch_ingest_has_no_materialize_cli_or_env_default() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'parser.add_argument("--auto-review", action="store_true", default=False)' in source
    assert "--materialize" not in source
    assert "JATO_AUTO_MATERIALIZE" not in source
    assert 'JATO_MSRP_EXECUTION_CONTEXT") or "unspecified"' in source


def test_auto_resolve_reviews_passes_min_score(monkeypatch) -> None:
    requests = []

    def _post(path, payload, *, auth_token, user_name):
        requests.append({
            "path": path,
            "payload": payload,
            "auth_token": auth_token,
            "user_name": user_name,
        })
        return {
            "item": {
                "candidateCases": 2,
                "autoApprovedCount": 1,
                "directAutoReviewApprovedCount": 1,
                "linkAppliedCount": 1,
                "overrideAppliedCount": 0,
                "unresolvedCount": 1,
                "missingObservationCount": 0,
                "scoreRejectedCount": 1,
            }
        }

    monkeypatch.setattr(batch_ingest, "_post_backend_json", _post)

    totals = batch_ingest._auto_resolve_reviews(
        ["se"],
        decided_by="msrp-auto-review",
        limit=50,
        min_score=82.5,
        note="nightly",
        auth_token="token",
        user_name="msrp-cron",
    )

    assert requests == [
        {
            "path": "/review/cases/auto-resolve",
            "payload": {
                "country": "SE",
                "decided_by": "msrp-auto-review",
                "limit": 50,
                "note": "nightly",
                "min_score": 82.5,
            },
            "auth_token": "token",
            "user_name": "msrp-cron",
        }
    ]
    assert totals["autoApprovedCount"] == 1
    assert totals["directAutoReviewApprovedCount"] == 1
    assert totals["scoreRejectedCount"] == 1


def test_float_env_ignores_invalid_values(monkeypatch) -> None:
    monkeypatch.setenv("JATO_MSRP_AUTO_REVIEW_MIN_SCORE", "bad")

    assert batch_ingest._float_env("JATO_MSRP_AUTO_REVIEW_MIN_SCORE") is None


def test_write_ingest_status_includes_review_and_materialize_totals(
    monkeypatch,
    tmp_path,
) -> None:
    captured = {}

    monkeypatch.setattr(batch_ingest, "__file__", str(tmp_path / "batch_ingest.py"))
    monkeypatch.setattr(
        batch_ingest,
        "write_pipeline_status",
        lambda **kwargs: captured.update(kwargs),
    )

    batch_ingest._write_ingest_status(
        ["at"],
        ok_count=8,
        empty_count=1,
        fail_count=1,
        total=10,
        auto_review_totals={
            "autoApprovedCount": 5,
            "directAutoReviewApprovedCount": 4,
            "unresolvedCount": 1,
        },
        materialize_totals={
            "candidateObservations": 7,
            "materializedKeys": 6,
        },
    )

    status_path = tmp_path / "logs" / "scheduled_fetch_status.json"
    status = json.loads(status_path.read_text())
    assert status["msrp_ingest"]["okPct"] == 80.0
    assert captured["extra"]["autoReview"]["directAutoReviewApprovedCount"] == 4
    assert captured["extra"]["materialize"]["materializedKeys"] == 6
