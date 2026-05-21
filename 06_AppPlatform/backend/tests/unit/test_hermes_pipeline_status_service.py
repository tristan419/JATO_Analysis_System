import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.services import hermes_pipeline_status_service as pipeline_status


def _set_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pipeline_status, "_project_root", tmp_path)
    monkeypatch.setattr(pipeline_status, "_jato_monthly_update_job_root", None)


def test_missing_status_is_synthetic_record(monkeypatch, tmp_path: Path):
    _set_root(monkeypatch, tmp_path)

    record = pipeline_status.get_pipeline_status("msrp_dryrun")
    health = pipeline_status.classify_pipeline_health(record)

    assert record["pipelineId"] == "msrp_dryrun"
    assert record["status"] == "missing"
    assert health["type"] == "pipeline_msrp_dryrun_missing"
    assert health["overall"] == "warning"


def test_write_and_read_success_status(monkeypatch, tmp_path: Path):
    _set_root(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    written = pipeline_status.write_pipeline_status({
        "pipelineId": "msrp_dryrun",
        "status": "success",
        "lastRunAt": now,
        "recordsProcessed": 10,
        "artifactRefs": ["03_Scripts/diagnostics/artifacts/dryrun_report.json"],
    })
    record = pipeline_status.get_pipeline_status("msrp_dryrun")
    health = pipeline_status.classify_pipeline_health(record)

    assert written["recordsProcessed"] == 10
    assert record["standardStatusFile"] is True
    assert health["overall"] == "ok"


def test_degraded_failed_count_maps_to_warning(monkeypatch, tmp_path: Path):
    _set_root(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    record = pipeline_status.write_pipeline_status({
        "pipelineId": "voc_forum_sync",
        "status": "success",
        "lastRunAt": now,
        "recordsProcessed": 8,
        "failedCount": 2,
    })
    health = pipeline_status.classify_pipeline_health(record)

    assert health["overall"] == "warning"
    assert health["type"] == "pipeline_voc_forum_sync_degraded"
    assert "2 failed" in health["message"]


def test_failed_ingest_maps_to_critical(monkeypatch, tmp_path: Path):
    _set_root(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    record = pipeline_status.write_pipeline_status({
        "pipelineId": "msrp_ingest",
        "status": "failure",
        "lastRunAt": now,
    })
    health = pipeline_status.classify_pipeline_health(record)

    assert record["status"] == "failed"
    assert health["overall"] == "critical"
    assert health["severity"] == "critical"


def test_stale_last_run_maps_to_warning(monkeypatch, tmp_path: Path):
    _set_root(monkeypatch, tmp_path)
    stale = (datetime.now(timezone.utc) - timedelta(hours=40)).strftime("%Y-%m-%dT%H:%M:%SZ")

    record = pipeline_status.write_pipeline_status({
        "pipelineId": "country_news_sync",
        "status": "success",
        "lastRunAt": stale,
    })
    health = pipeline_status.classify_pipeline_health(record)

    assert health["overall"] == "warning"
    assert health["type"] == "pipeline_country_news_sync_stale"
    assert health["ageHours"] >= 39


def test_detect_missing_pipeline_status(monkeypatch, tmp_path: Path):
    _set_root(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pipeline_status.write_pipeline_status({
        "pipelineId": "jato_etl",
        "status": "success",
        "lastRunAt": now,
    })

    missing = pipeline_status.detect_missing_pipeline_status(["jato_etl", "msrp_dryrun"])

    assert [item["pipelineId"] for item in missing] == ["msrp_dryrun"]


def test_legacy_partial_success_maps_to_degraded(monkeypatch, tmp_path: Path):
    _set_root(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = tmp_path / "03_Scripts" / "logs" / "scheduled_fetch_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "voc": {
            "status": "partial_success",
            "lastRunAt": now,
            "successCount": 10,
            "failedCount": 1,
        }
    }), encoding="utf-8")

    record = pipeline_status.get_pipeline_status("voc_forum_sync")
    health = pipeline_status.classify_pipeline_health(record)

    assert record["status"] == "degraded"
    assert record["standardStatusFile"] is False
    assert health["type"] == "pipeline_voc_forum_sync_degraded"


def test_source_quality_report_is_pipeline_success(monkeypatch, tmp_path: Path):
    _set_root(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = tmp_path / "hermes" / "reports" / "source_quality_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "generatedAt": now,
        "summary": {"totalSources": 7, "degraded": 3, "highRisk": 0},
    }), encoding="utf-8")

    record = pipeline_status.get_pipeline_status("source_quality")
    health = pipeline_status.classify_pipeline_health(record)

    assert record["status"] == "success"
    assert health["overall"] == "ok"


def test_jato_etl_hydrates_from_monthly_update_job_state(monkeypatch, tmp_path: Path):
    _set_root(monkeypatch, tmp_path)
    job_state = tmp_path / "04_Processed_data" / "ops" / "jato_monthly_update_jobs" / "job-1" / "job_state.json"
    job_state.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    job_state.write_text(json.dumps({
        "jobId": "job-1",
        "month": "2026-03",
        "batchId": "2026-03-r1",
        "status": "success",
        "phase": "completed",
        "startedAt": now,
        "finishedAt": now,
        "summaries": {"refresh": {"rowCount": 542988}},
        "artifacts": {"refreshReportPath": "04_Processed_data/ops/jato/monthly/refresh_job_report.json"},
    }), encoding="utf-8")

    record = pipeline_status.get_pipeline_status("jato_etl")
    health = pipeline_status.classify_pipeline_health(record)

    assert record["status"] == "success"
    assert record["recordsProcessed"] == 542988
    assert record["derivedFrom"] == "jato_monthly_update_job_state"
    assert "job_state.json" in record["artifactRefs"][0]
    assert health["overall"] == "ok"
