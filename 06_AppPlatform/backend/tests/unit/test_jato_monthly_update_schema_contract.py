import pandas as pd
import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service as service


def _active() -> pd.DataFrame:
    return pd.DataFrame([{
        "Country": "Denmark", "Make": "BRAND", "Model": "MODEL",
        "Version name": "VERSION", "Registration type": "Business",
        "MSRP规整": 20000, "2026 Aug": 10,
    }])


def _findings(active: pd.DataFrame, candidate: pd.DataFrame) -> list[dict]:
    return service._schema_contract_review_findings(
        "Denmark", service._single_country_schema_contract(
            active_frame=active, candidate_frame=candidate,
        ),
    )


@pytest.mark.parametrize("column", sorted(service.DEPRECATED_OPTIONAL_STATIC_COLUMNS))
@pytest.mark.parametrize("missing", [True, False])
def test_optional_fields_missing_or_empty_warn(column, missing):
    active = _active().assign(**{column: [100]})
    candidate = active.drop(columns=[column]) if missing else active.assign(**{column: [None]})
    findings = _findings(active, candidate)
    assert [(item["ruleId"], item["severity"]) for item in findings] == [("SC014", "review")]
    assert findings[0]["metrics"]["emptyColumns"] == ([] if missing else [column])


@pytest.mark.parametrize("column", [
    "maximum power hp/ps", "Maximum power hp / PS", "品牌（奇瑞集团整合）",
    " Seating\u00a0capacity ", "Ｂａｔｔｅｒｙ ｋｗｈ",
])
def test_optional_name_variants_do_not_become_core_blockers(column):
    active = _active().assign(**{column: [100]})
    assert _findings(active, active.drop(columns=[column]))[0]["ruleId"] == "SC014"
    result, summary = service._carry_forward_deprecated_static_columns(
        active_frame=active, candidate_frame=active.drop(columns=[column]),
    )
    assert result[column].tolist() == [100]
    assert summary["columnResults"][column]["remainingNullRowCount"] == 0


@pytest.mark.parametrize("column", ["Make", "Model", "Registration type", "MSRP规整"])
@pytest.mark.parametrize("value", [None, "", " \t "])
def test_populated_core_fields_cannot_be_emptied(column, value):
    active = _active()
    findings = _findings(active, active.assign(**{column: [value]}))
    assert findings[0]["ruleId"] == "SC009" and findings[0]["severity"] == "blocker"
    assert findings[0]["metrics"]["emptyColumns"] == [column]
    assert "empty headers" in findings[0]["sourceFeedback"]


def test_zero_partial_nulls_and_preexisting_empty_fields_are_not_lost_columns():
    active = pd.concat([_active(), _active()], ignore_index=True).assign(Unused=None)
    candidate = active.assign(**{"MSRP规整": [0, None]}).drop(columns=["Unused"])
    assert _findings(active, candidate) == []
    assert _findings(_active().assign(**{"MSRP规整": [0]}), _active().drop(columns=["MSRP规整"]))[0]["ruleId"] == "SC009"


def test_price_units_and_similar_fields_are_not_guessed():
    active = _active().assign(**{"Maximum torque Nm": [200]})
    candidate = active.drop(columns=["MSRP规整", "Maximum torque Nm"]).assign(**{
        "Retail price": [20000], "Maximum power hp": [200],
    })
    findings = _findings(active, candidate)
    assert findings[0]["ruleId"] == "SC009"
    assert findings[0]["metrics"]["missingMaterialColumns"] == ["MSRP规整", "Maximum torque Nm"]


def test_core_column_remains_blocked_after_schema_alignment_and_preserved_history():
    active = _active()
    incoming = active.assign(**{"MSRP规整": [None], "2026 Aug": [0], "2026 Sep": [12]})
    merged, _ = service._keep_active_history_country_frame(
        active_frame=active, candidate_frame=incoming,
    )
    assert merged["2026 Aug"].sum() == 10 and merged["2026 Sep"].sum() == 12
    assert merged["MSRP规整"].notna().any()  # Old values must not mask new-month loss.
    assert _findings(active, merged)[0]["ruleId"] == "SC009"


def test_optional_empty_new_month_is_warning_even_with_populated_history():
    active = _active().assign(**{"Maximum power hp/PS": [100]})
    merged = pd.concat([
        active.assign(**{"2026 Sep": [0]}),
        active.assign(**{"2026 Aug": [0], "2026 Sep": [12], "Maximum power hp/PS": [None]}),
    ], ignore_index=True)
    assert _findings(active, merged)[0]["ruleId"] == "SC014"


@pytest.mark.parametrize("scope", ["target_country_partitions_only", "full_smart_merge"])
def test_full_review_checks_country_values_not_global_column_population(tmp_path, scope):
    active = pd.concat([_active(), _active().assign(Country="Sweden")], ignore_index=True)
    candidate = active.copy()
    candidate.loc[candidate.Country.eq("Denmark"), "MSRP规整"] = None
    active_path, candidate_path = [tmp_path / name for name in ("active.parquet", "candidate.parquet")]
    active.to_parquet(active_path, index=False)
    candidate.to_parquet(candidate_path, index=False)
    report = service._build_historical_reclassification_report_from_paths(
        payload={"artifacts": {"candidateScope": scope}}, countries=["Denmark", "Sweden"],
        active_path=active_path, candidate_path=candidate_path,
    )
    assert [(item["target"], item["ruleId"]) for item in report["schemaFindings"]] == [("Denmark", "SC009")]


def test_direct_smart_merge_does_not_erase_source_schema_blocker(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(service, "MONTHLY_UPDATE_JOB_ROOT", tmp_path / "jobs")
    monkeypatch.setattr(service, "_require_no_running_monthly_update_jobs", lambda **kwargs: None)
    processed = tmp_path / "04_Processed_data"
    processed.mkdir()
    active_path, raw_path = [processed / name for name in ("jato_full_archive.parquet", "raw.parquet")]
    _active().to_parquet(active_path, index=False)
    _active().drop(columns=["MSRP规整"]).to_parquet(raw_path, index=False)
    state = {
        "jobId": "jato-schema", "country": "Denmark", "status": "success", "phase": "completed",
        "artifacts": {"candidateScope": "target_country_partition_only", "stagingOutputPath": str(raw_path)},
    }
    service._persist_job_state(state)
    review = service._build_single_country_review(state, candidate_fingerprint="b" * 64)
    assert any(item["ruleId"] == "SC009" for item in review["reviewFindings"])
    monkeypatch.setattr(service, "get_jato_monthly_update_review", lambda job_id: review)
    queued = []
    monkeypatch.setattr(service, "_launch_smart_merge_thread", queued.append)
    original = (active_path.read_bytes(), raw_path.read_bytes())
    with pytest.raises(HTTPException) as error:
        service._create_smart_merge_candidate_locked(job_id=state["jobId"], triggered_by="admin")
    assert error.value.detail["rules"] == ["SC009"]
    assert not queued
    assert service._load_job_state(state["jobId"]) == state
    assert (active_path.read_bytes(), raw_path.read_bytes()) == original


@pytest.mark.parametrize("missing", ["2025", "YTD 2026(Sep)"])
def test_derived_totals_are_not_promoted_to_core_blockers(missing):
    active = _active().assign(**{missing: [10]})
    contract = service._single_country_schema_contract(active_frame=active, candidate_frame=active.drop(columns=[missing]))
    assert contract["missingDerivedYtd"] == [missing]
    assert _findings(active, active.drop(columns=[missing])) == []
