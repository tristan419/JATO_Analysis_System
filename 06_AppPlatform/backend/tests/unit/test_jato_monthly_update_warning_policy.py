from pathlib import Path

import pandas as pd
import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service as service
from test_jato_monthly_update_candidate_bundle import _make_complete_candidate_bundle


@pytest.fixture
def review_state(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(service, "MONTHLY_UPDATE_JOB_ROOT", tmp_path / "jobs")
    monkeypatch.setattr(service, "_active_dataset_version", lambda: "a" * 64)
    monkeypatch.setattr(service, "_launch_job_thread", lambda _job_id: None)
    state = {
        "jobId": "jato-warning-policy", "month": "2026-09",
        "status": "success", "phase": "completed", "triggeredBy": "admin",
        "activeBaseFingerprint": "a" * 64,
        "artifacts": {"candidateScope": "full_smart_merge"},
        "summaries": {"refresh": {"jobStatus": "success"}},
    }
    service._persist_job_state(state)
    review = {
        "candidateFingerprint": "b" * 64, "reviewFindings": [],
        "historicalReclassificationReport": {"status": "not_required", "countries": []},
    }
    monkeypatch.setattr(service, "get_jato_monthly_update_review", lambda _job_id: review)
    return state, review


@pytest.mark.parametrize("rule", ["SC006", "SC014"])
def test_warning_requires_explicit_acknowledgment_of_exact_candidate(review_state, rule):
    state, review = review_state
    warning = {"ruleId": rule, "severity": "review", "target": "丹麦", "metrics": {}}
    review["reviewFindings"] = [warning]
    for fingerprint, rules in ((None, [rule]), ("b" * 64, [])):
        with pytest.raises(HTTPException) as error:
            service.approve_jato_monthly_update_review(
                job_id=state["jobId"], triggered_by="admin", decision="approve",
                expected_candidate_fingerprint=fingerprint, accepted_warning_rules=rules,
            )
        assert error.value.detail["blockerType"] == "review_warnings_unacknowledged"
        assert not service._load_job_state(state["jobId"]).get("reviewApproval")

    result = service.publish_jato_monthly_update_job(
        job_id=state["jobId"], triggered_by="admin",
        expected_candidate_fingerprint="b" * 64, accepted_warning_rules=[rule],
    )
    assert result["pendingOperation"]["type"] == "publish"
    assert result["pendingOperation"]["status"] == "queued"
    approval = service._load_job_state(state["jobId"])["reviewApproval"]
    assert approval["candidateFingerprint"] == "b" * 64
    assert approval["acceptedWarnings"] == [warning]
    replay = service.publish_jato_monthly_update_job(
        job_id=state["jobId"], triggered_by="admin",
        expected_candidate_fingerprint="b" * 64, accepted_warning_rules=[rule],
    )
    assert replay["pendingOperation"]["operationId"] == result["pendingOperation"]["operationId"]
    assert not service._load_job_state(state["jobId"]).get("publication")


def test_final_confirmation_does_not_approve_a_new_unseen_candidate(review_state):
    state, review = review_state
    review["candidateFingerprint"] = "c" * 64
    with pytest.raises(HTTPException) as error:
        service.publish_jato_monthly_update_job(
            job_id=state["jobId"], triggered_by="admin",
            expected_candidate_fingerprint="b" * 64, accepted_warning_rules=[],
        )
    assert error.value.detail["blockerType"] == "stale_candidate"
    saved = service._load_job_state(state["jobId"])
    assert not saved.get("reviewApproval") and not saved.get("pendingOperation")


@pytest.mark.parametrize("rule", ["SC004", "SC005", "SC007", "SC009"])
def test_warning_acceptance_cannot_bypass_blockers(review_state, rule):
    state, review = review_state
    review["reviewFindings"] = [{"ruleId": rule, "severity": "blocker", "target": "丹麦"}]
    with pytest.raises(HTTPException, match="blocker"):
        service.publish_jato_monthly_update_job(
            job_id=state["jobId"], triggered_by="admin",
            expected_candidate_fingerprint="b" * 64, accepted_warning_rules=[rule],
        )
    assert not service._load_job_state(state["jobId"]).get("pendingOperation")


@pytest.mark.parametrize("column", ["Maximum power hp/PS", "品牌 (奇瑞集团整合)", "length (mm)"])
def test_optional_fields_warn_but_populated_core_fields_still_block(column):
    active = pd.DataFrame({
        "Country": ["Denmark"], "Make": ["BRAND"], "Model": ["MODEL"],
        "Registration type": ["Business"], "MSRP规整": [20000], column: [100],
        "2026 Aug": [10],
    })
    candidate = active.drop(columns=[column, "MSRP规整"])
    contract = service._single_country_schema_contract(active_frame=active, candidate_frame=candidate)
    assert contract["missingDeprecatedOptional"] == [column]
    assert contract["missingMaterial"] == ["MSRP规整"]
    findings = service._schema_contract_review_findings("Denmark", contract)
    assert {(item["ruleId"], item["severity"]) for item in findings} == {
        ("SC009", "blocker"), ("SC014", "review"),
    }


def test_same_month_upload_enters_review_instead_of_digest_block(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "_inspect_upload_scope", lambda _path: {
        "countries": ["丹麦"], "countryLatestMonths": {"丹麦": "2026-09"},
        "latestMonth": "2026-09", "issues": [],
    })
    monkeypatch.setattr(service, "_capture_active_digest_snapshot", lambda _countries: (
        ["丹麦"], {"丹麦": "2026 Sep"}, "a" * 64,
    ))
    digest = service._build_upload_ingest_digest(path=tmp_path / "unused.xlsx", file_sha256="b" * 64, size_bytes=1)
    assert digest["status"] == "ready" and digest["blockers"] == []
    assert [warning["code"] for warning in digest["warnings"]] == ["NO_COUNTRY_ADVANCED"]


@pytest.mark.parametrize("decision,expected", [(None, 10), ("keep_active", 10), ("use_latest", 25)])
def test_same_month_streaming_merge_preserves_missing_months(tmp_path, decision, expected):
    active_path, candidate_path, output_path = [tmp_path / name for name in ("active.parquet", "candidate.parquet", "result.parquet")]
    base = {"Country": "Denmark", "Make": "BRAND", "Model": "MODEL", "Version name": "VERSION"}
    pd.DataFrame([{**base, "2023 Jan": 7, "2026 Sep": 10}]).to_parquet(active_path, index=False)
    pd.DataFrame([{**base, "2026 Sep": 25}]).to_parquet(candidate_path, index=False)
    service._smart_merge_parquet_streaming(
        active_path=active_path, candidate_path=candidate_path, output_path=output_path,
        regressed_countries=[], historical_reclassification_decisions={"Denmark": decision} if decision else None,
    )
    result = pd.read_parquet(output_path)
    assert result["2023 Jan"].sum() == 7
    assert result["2026 Sep"].sum() == expected
    assert pd.read_parquet(active_path)["2026 Sep"].sum() == 10


def test_optional_warning_survives_raw_review_merge_and_complete_review(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "PROJECT_ROOT", tmp_path)
    processed = tmp_path / "04_Processed_data"
    processed.mkdir()
    active_path = processed / "jato_full_archive.parquet"
    raw_path, merged_path = processed / "raw.parquet", processed / "merged.parquet"
    base = {"Country": "Denmark", "Make": "BRAND", "Model": "MODEL", "Version name": "VERSION"}
    pd.DataFrame([{**base, "Maximum power hp/PS": 150, "2026 Aug": 10}]).to_parquet(active_path, index=False)
    pd.DataFrame([{**base, "2026 Aug": 10, "2026 Sep": 12}]).to_parquet(raw_path, index=False)
    payload = {
        "jobId": "jato-optional-roundtrip", "country": "Denmark",
        "artifacts": {"candidateScope": "target_country_partition_only", "stagingOutputPath": str(raw_path)},
    }
    raw = service._build_single_country_review(payload, candidate_fingerprint="a" * 64)
    warnings = [item for item in raw["reviewFindings"] if item["ruleId"] == "SC014"]
    assert len(warnings) == 1 and warnings[0]["severity"] == "review"
    service._smart_merge_parquet_streaming(
        active_path=active_path, candidate_path=raw_path, output_path=merged_path, regressed_countries=[],
    )
    payload["artifacts"] = {
        "candidateScope": "full_smart_merge", "stagingOutputPath": str(merged_path),
        "sourceReviewWarnings": warnings,
    }
    complete = service._build_single_country_review(payload, candidate_fingerprint="b" * 64)
    assert [item for item in complete["reviewFindings"] if item["ruleId"] == "SC014"] == warnings
    assert pd.read_parquet(merged_path)["2026 Sep"].sum() == 12
    assert pd.read_parquet(merged_path)["Maximum power hp/PS"].dropna().eq(150).all()


@pytest.mark.parametrize("accepted_target", [None, "其他国家", "匈牙利"])
def test_publish_warning_exception_is_country_and_candidate_bound(tmp_path, monkeypatch, accepted_target):
    monkeypatch.setattr(service, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(service, "MONTHLY_UPDATE_JOB_ROOT", tmp_path / "jobs")
    paths = _make_complete_candidate_bundle(tmp_path)
    active = tmp_path / "04_Processed_data" / "jato_full_archive.parquet"
    pd.read_parquet(paths["parquet"]).to_parquet(active, index=False)
    artifacts = {field: str(paths[name].relative_to(tmp_path)) for name, field in service.CANDIDATE_ARTIFACT_FIELDS}
    state = {
        "jobId": "jato-warning-publish", "status": "success", "phase": "completed",
        "artifacts": artifacts, "summaries": {"refresh": {"jobStatus": "success"}},
        "reviewApproval": {
            "decision": "approved", "activeBaseFingerprint": "a" * 64,
            "candidateFingerprint": service._candidate_fingerprint_id(artifacts),
            "acceptedWarnings": [{"ruleId": "SC006", "target": accepted_target}] if accepted_target else [],
        },
    }
    service._persist_job_state(state)
    monkeypatch.setattr(service, "_active_dataset_version", lambda: "a" * 64)
    monkeypatch.setattr(service, "_validate_candidate_full_bundle", lambda **_kwargs: None)
    monkeypatch.setattr(service, "_find_publish_historical_configuration_changes", lambda **_kwargs: [])
    monkeypatch.setattr(service, "_find_publish_sales_doubling_anomalies", lambda **_kwargs: [{
        "country": "匈牙利", "suspiciousMonthCount": 2, "sampleMonths": [], "rolling12Ratio": 2,
    }])

    def stop_before_active_write(**_kwargs):
        raise RuntimeError("warning guard passed; stop before active write")

    monkeypatch.setattr(service, "_stage_active_bundle_sources", stop_before_active_write)
    original = active.read_bytes()
    if accepted_target == "匈牙利":
        with pytest.raises(RuntimeError, match="warning guard passed"):
            service._publish_jato_monthly_update_job_locked(job_id=state["jobId"], triggered_by="admin")
    else:
        with pytest.raises(HTTPException) as error:
            service._publish_jato_monthly_update_job_locked(job_id=state["jobId"], triggered_by="admin")
        assert error.value.detail["blockerType"] == "sales_doubling"
    assert active.read_bytes() == original
