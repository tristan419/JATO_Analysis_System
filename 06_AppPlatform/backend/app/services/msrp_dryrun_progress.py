"""Parse MSRP dryrun logs and reports for progress dashboards."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_REPO_DIR = Path(__file__).resolve().parents[4]
REPO_DIR = Path(os.environ.get("REPO_DIR", str(DEFAULT_REPO_DIR)))
LOG_DIR = REPO_DIR / "03_Scripts" / "logs"
ARTIFACT_DIR = REPO_DIR / "03_Scripts" / "diagnostics" / "artifacts"
LATEST_REPORT_PATH = ARTIFACT_DIR / "dryrun_report.json"
RUNS_INDEX_PATH = ARTIFACT_DIR / "dryrun_runs_index.json"
PIPELINE_STATUS_PATH = REPO_DIR / "hermes" / "reports" / "pipeline_status" / "msrp_dryrun.json"
LOCK_FILE = Path("/tmp/jato-msrp-low-concurrency.lock")
DRYRUN_LOG_PATTERN = re.compile(r"msrp-dryrun-(\d{8})-(\d{6})\.log")
DRYRUN_RUN_DIR_PATTERN = re.compile(r"msrp-dryrun-(\d{8})-(\d{6})$")
DRYRUN_RUN_ID_PATTERN = re.compile(r"\b(msrp-dryrun-\d{8}-\d{6})\b")
COUNTRY_CODE_PATTERN = re.compile(r"^[a-z]{2}$")

_COUNTRY_NAMES: dict[str, str] = {
    "at": "Austria",
    "be": "Belgium",
    "ch": "Switzerland",
    "cz": "Czech Republic",
    "de": "Germany",
    "dk": "Denmark",
    "es": "Spain",
    "fi": "Finland",
    "fr": "France",
    "gr": "Greece",
    "hr": "Croatia",
    "hu": "Hungary",
    "it": "Italy",
    "nl": "Netherlands",
    "no": "Norway",
    "pl": "Poland",
    "pt": "Portugal",
    "ro": "Romania",
    "se": "Sweden",
    "si": "Slovenia",
    "sk": "Slovakia",
}


def _country_label(cc: str) -> str:
    return _COUNTRY_NAMES.get(cc.lower(), cc.upper())


def _country_code(value: Any) -> str:
    code = str(value or "").strip().lower()
    return code if COUNTRY_CODE_PATTERN.fullmatch(code) else ""


def _country_code_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    codes: list[str] = []
    seen: set[str] = set()
    for value in values:
        code = _country_code(value)
        if not code or code in seen:
            continue
        codes.append(code)
        seen.add(code)
    return codes


def _status_for_pass_rate(pass_rate: float) -> str:
    if pass_rate >= 90:
        return "success"
    if pass_rate >= 50:
        return "degraded"
    return "failure"


def _source_status_value(source: dict[str, Any]) -> str:
    return str(source.get("rawStatus") or source.get("status") or "").lower()


def _source_valid_count(source: dict[str, Any]) -> int:
    try:
        return int(source.get("valid") or 0)
    except (TypeError, ValueError):
        return 0


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _source_is_pass(source: dict[str, Any]) -> bool:
    status = _source_status_value(source)
    return _source_valid_count(source) > 0 and not source.get("failureReason") and status not in {"empty", "error", "exception"}


def _source_is_empty(source: dict[str, Any]) -> bool:
    return _source_status_value(source) == "empty"


def _source_is_error(source: dict[str, Any]) -> bool:
    return _source_status_value(source) in {"error", "exception"}


def _parse_log_timestamp(filename: str) -> datetime | None:
    m = DRYRUN_LOG_PATTERN.search(filename)
    if not m:
        return None
    return datetime.strptime(f"{m.group(1)}{m.group(2)}", "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def _parse_run_dir_timestamp(name: str) -> datetime | None:
    m = DRYRUN_RUN_DIR_PATTERN.search(name)
    if not m:
        return None
    return datetime.strptime(f"{m.group(1)}{m.group(2)}", "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def _is_running() -> bool:
    """Return true only while another process actually holds the runner flock."""
    try:
        with LOCK_FILE.open("rb") as lock_handle:
            try:
                fcntl.flock(
                    lock_handle.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except OSError as exc:
                return exc.errno in {errno.EACCES, errno.EAGAIN}
            else:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                return False
    except OSError:
        return False


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _artifact_path_from_ref(path_ref: str | None) -> Path | None:
    if not path_ref:
        return None
    path = Path(path_ref)
    if not path.is_absolute():
        path = REPO_DIR / path
    return path


def _load_v3_report(run_id: str | None = None) -> dict[str, Any] | None:
    path = ARTIFACT_DIR / f"dryrun_report_{run_id}.json" if run_id else LATEST_REPORT_PATH
    data = _load_json(path)
    if data and data.get("schemaVersion") == "msrp_dryrun_report_v3":
        return data
    return None


def _load_latest_indexed_v3_report(index_data: dict[str, Any] | None) -> dict[str, Any] | None:
    latest_run_id = str((index_data or {}).get("latestRunId") or "")
    if not latest_run_id:
        return None

    fallback_paths: list[Path] = [ARTIFACT_DIR / f"dryrun_report_{latest_run_id}.json"]
    for run in (index_data or {}).get("runs") or []:
        if run.get("runId") != latest_run_id:
            continue
        artifact_path = _artifact_path_from_ref(run.get("artifactPath"))
        if artifact_path:
            fallback_paths.insert(0, artifact_path)
        break

    seen: set[Path] = set()
    for path in fallback_paths:
        if path in seen:
            continue
        seen.add(path)
        data = _load_json(path)
        if data and data.get("schemaVersion") == "msrp_dryrun_report_v3":
            return data
    return None


def _run_recency_key(run: dict[str, Any]) -> tuple[str, str]:
    run_id = str(run.get("runId") or "")
    timestamp = str(
        run.get("finishedAt")
        or run.get("startedAt")
        or run.get("updatedAt")
        or ""
    )
    return timestamp, run_id


def _normalize_source(source: dict[str, Any], index: int, total: int) -> dict[str, Any]:
    source_code = str(source.get("sourceCode") or source.get("code") or "")
    raw_status = str(source.get("rawStatus") or source.get("status") or "")
    status = "pass" if _source_is_pass(source) else ("empty" if _source_is_empty(source) else "fail")
    payload = {
        "index": int(source.get("index") or index),
        "totalInCountry": int(source.get("totalInCountry") or total),
        "sourceCode": source_code,
        "status": status,
        "rawStatus": raw_status,
        "valid": int(source.get("valid") or 0),
        "extracted": int(source.get("extracted") or 0),
        "rejected": int(source.get("rejected") or 0),
        "elapsedSeconds": float(source.get("elapsedSeconds") or source.get("elapsed") or 0),
        "failureReason": source.get("failureReason"),
        "recommendedStrategy": source.get("recommendedStrategy"),
        "severity": source.get("severity"),
    }
    for key in (
        "error",
        "extractorError",
        "sourceUrl",
        "httpStatus",
        "finalUrl",
        "financeObservationCandidates",
        "financeMonthlyPaymentCount",
        "financeSemanticsCounts",
        "financeTypeCounts",
        "sampleFinanceContexts",
    ):
        value = source.get(key)
        if value not in (None, ""):
            payload[key] = value
    return payload


def _source_from_log_match(match: re.Match[str]) -> dict[str, Any]:
    emoji = match.group(3)
    raw_status = "pass" if "✅" in emoji else ("empty" if "⬚" in emoji else "fail")
    total = int(match.group(2))
    return _normalize_source(
        {
            "index": int(match.group(1)),
            "totalInCountry": total,
            "sourceCode": match.group(4),
            "status": raw_status,
            "valid": int(match.group(5)),
            "extracted": int(match.group(6)),
            "rejected": int(match.group(7)),
            "elapsedSeconds": float(match.group(8)),
        },
        int(match.group(1)),
        total,
    )


def _source_counts(sources: list[dict[str, Any]]) -> tuple[int, int, int, int]:
    pass_count = sum(1 for source in sources if source["status"] == "pass")
    empty_count = sum(1 for source in sources if source["status"] == "empty")
    error_count = sum(1 for source in sources if _source_is_error(source))
    fail_count = sum(1 for source in sources if source["status"] == "fail" and not _source_is_error(source))
    return pass_count, empty_count, fail_count, error_count


def _aggregate_country_counter(countries: list[dict[str, Any]], key: str) -> dict[str, int]:
    counter: dict[str, int] = {}
    for country in countries:
        for name, count in (country.get(key) or {}).items():
            if not name:
                continue
            try:
                value = int(count or 0)
            except (TypeError, ValueError):
                continue
            counter[str(name)] = counter.get(str(name), 0) + value
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def _normalize_count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, int] = {}
    for name, count in value.items():
        label = str(name or "").strip() or "unknown"
        normalized[label] = normalized.get(label, 0) + _int_value(count)
    return dict(sorted(normalized.items(), key=lambda item: (-item[1], item[0])))


def _finance_summary_from_sources(sources: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "financeObservationCandidates": sum(
            _int_value(source.get("financeObservationCandidates"))
            for source in sources
        ),
        "financeMonthlyPaymentCount": sum(
            _int_value(source.get("financeMonthlyPaymentCount"))
            for source in sources
        ),
        "financeSemanticsCounts": _aggregate_country_counter(
            sources,
            "financeSemanticsCounts",
        ),
        "financeTypeCounts": _aggregate_country_counter(sources, "financeTypeCounts"),
    }


def _finance_summary_from_country(
    country: dict[str, Any],
    sources: list[dict[str, Any]],
) -> dict[str, Any]:
    from_sources = _finance_summary_from_sources(sources)
    semantics_counts = _normalize_count_map(country.get("financeSemanticsCounts"))
    type_counts = _normalize_count_map(country.get("financeTypeCounts"))
    return {
        "financeObservationCandidates": (
            _int_value(country.get("financeObservationCandidates"))
            or from_sources["financeObservationCandidates"]
        ),
        "financeMonthlyPaymentCount": (
            _int_value(country.get("financeMonthlyPaymentCount"))
            or from_sources["financeMonthlyPaymentCount"]
        ),
        "financeSemanticsCounts": semantics_counts or from_sources["financeSemanticsCounts"],
        "financeTypeCounts": type_counts or from_sources["financeTypeCounts"],
    }


def _finance_summary_from_countries(
    countries: list[dict[str, Any]],
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    finance = {
        "financeObservationCandidates": sum(
            _int_value(country.get("financeObservationCandidates"))
            for country in countries
        ),
        "financeMonthlyPaymentCount": sum(
            _int_value(country.get("financeMonthlyPaymentCount"))
            for country in countries
        ),
        "financeSemanticsCounts": _aggregate_country_counter(
            countries,
            "financeSemanticsCounts",
        ),
        "financeTypeCounts": _aggregate_country_counter(countries, "financeTypeCounts"),
    }
    if not fallback:
        return finance
    if finance["financeObservationCandidates"] == 0:
        finance["financeObservationCandidates"] = _int_value(
            fallback.get("financeObservationCandidates"),
        )
    if finance["financeMonthlyPaymentCount"] == 0:
        finance["financeMonthlyPaymentCount"] = _int_value(
            fallback.get("financeMonthlyPaymentCount"),
        )
    if not finance["financeSemanticsCounts"]:
        finance["financeSemanticsCounts"] = _normalize_count_map(
            fallback.get("financeSemanticsCounts"),
        )
    if not finance["financeTypeCounts"]:
        finance["financeTypeCounts"] = _normalize_count_map(
            fallback.get("financeTypeCounts"),
        )
    return finance


def _dedupe_countries(countries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for country in countries:
        code = _country_code(country.get("countryCode") or country.get("country"))
        if not code or code in deduped:
            continue
        deduped[code] = country
    return list(deduped.values())


def _normalize_country_from_v3(
    country: dict[str, Any],
    *,
    run_meta: dict[str, Any] | None = None,
    latest_run_id: str = "",
) -> dict[str, Any] | None:
    code = _country_code(country.get("countryCode") or country.get("country"))
    if not code:
        return None
    total = int(country.get("total") or 0)
    sources = [
        _normalize_source(source, index, total)
        for index, source in enumerate(country.get("sources") or [], start=1)
    ]
    if sources:
        pass_count, empty_count, fail_count, error_count = _source_counts(sources)
    else:
        pass_count = int(country.get("pass") or 0)
        empty_count = int(country.get("empty") or 0)
        error_count = int(country.get("errors") or 0)
        fail_count = int(country.get("fail") or 0)
    pass_rate = round(pass_count / max(total, 1) * 100, 1) if total > 0 else 0
    payload: dict[str, Any] = {
        "countryCode": code,
        "countryLabel": _country_label(code),
        "total": total,
        "pass": pass_count,
        "empty": empty_count,
        "fail": fail_count,
        "errors": error_count,
        "completed": country.get("status") != "missing",
        "passRate": pass_rate,
        "status": "missing" if country.get("status") == "missing" else _status_for_pass_rate(pass_rate),
        "topFailureReason": country.get("topFailureReason"),
        "failureBreakdown": country.get("failureBreakdown") or {},
        "strategyRecommendations": country.get("strategyRecommendations") or {},
        **_finance_summary_from_country(country, sources),
        "sources": sources,
    }
    if run_meta:
        run_id = str(run_meta.get("runId") or "")
        payload.update({
            "runId": run_id,
            "batch": run_meta.get("batch") or "",
            "timestamp": run_meta.get("finishedAt") or run_meta.get("startedAt") or "",
            "gateStatus": run_meta.get("gateStatus"),
            "runStatus": run_meta.get("status"),
            "isLatestRun": run_id == latest_run_id,
        })
    return payload


def _country_gate_threshold(
    run_meta: dict[str, Any],
    report: dict[str, Any],
) -> float:
    summary = report.get("summary") or {}
    for value in (run_meta.get("gateThreshold"), summary.get("gateThreshold")):
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 70.0


def _is_stable_country_observation(
    normalized: dict[str, Any],
    run_meta: dict[str, Any],
    report: dict[str, Any],
) -> bool:
    if not normalized.get("completed", True):
        return False
    try:
        pass_rate = float(normalized.get("passRate") or 0)
    except (TypeError, ValueError):
        pass_rate = 0.0
    return pass_rate >= _country_gate_threshold(run_meta, report)


def _all_country_latest_from_runs_index(index_data: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not index_data:
        return []

    latest_run_id = str(index_data.get("latestRunId") or "")
    stable_by_code: dict[str, dict[str, Any]] = {}
    fallback_by_code: dict[str, dict[str, Any]] = {}
    for run in sorted(
        index_data.get("runs") or [],
        key=_run_recency_key,
        reverse=True,
    ):
        run_id = str(run.get("runId") or "")
        artifact_path = _artifact_path_from_ref(run.get("artifactPath"))
        report = _load_json(artifact_path) if artifact_path else None
        if not report or report.get("schemaVersion") != "msrp_dryrun_report_v3":
            continue
        for country in report.get("countriesDetail") or []:
            normalized = _normalize_country_from_v3(
                country,
                run_meta=run,
                latest_run_id=latest_run_id,
            )
            if not normalized:
                continue
            code = normalized["countryCode"]
            fallback_by_code.setdefault(code, normalized)
            if code not in stable_by_code and _is_stable_country_observation(
                normalized,
                run,
                report,
            ):
                stable_by_code[code] = normalized

    countries_by_code = {
        code: stable_by_code.get(code, fallback)
        for code, fallback in fallback_by_code.items()
    }

    return sorted(
        countries_by_code.values(),
        key=lambda country: (
            0 if country.get("isLatestRun") else 1,
            str(country.get("countryLabel") or country.get("countryCode") or ""),
        ),
    )


def _stable_coverage_summary(all_countries: list[dict[str, Any]], current: dict[str, Any]) -> dict[str, Any]:
    gate_threshold = float(current.get("gateThreshold") or 70)
    ready_countries = [
        country for country in all_countries
        if float(country.get("passRate") or 0) >= gate_threshold
    ]
    total_sources = sum(int(country.get("total") or 0) for country in all_countries)
    total_pass = sum(int(country.get("pass") or 0) for country in all_countries)
    finance = _finance_summary_from_countries(all_countries)
    latest_run_id = str(next(
        (country.get("runId") for country in all_countries if country.get("isLatestRun") and country.get("runId")),
        "",
    ))
    if not latest_run_id:
        latest_run_id = str(next((country.get("runId") for country in all_countries if country.get("runId")), ""))
    active_run_id = str(current.get("runId") or "")
    stable_good_sources: dict[tuple[str, str], dict[str, Any]] = {}
    active_sources: dict[tuple[str, str], dict[str, Any]] = {}
    repair_samples: list[dict[str, Any]] = []
    failure_counts: dict[str, int] = {}
    source_rows_observed = 0
    for country in all_countries:
        country_code = str(country.get("countryCode") or "")
        for source in country.get("sources") or []:
            source_code = str(source.get("sourceCode") or "")
            if not country_code or not source_code:
                continue
            source_rows_observed += 1
            if source.get("status") == "pass":
                stable_good_sources[(country_code, source_code)] = {**source, "_runId": country.get("runId")}
                continue
            reason = str(source.get("failureReason") or country.get("topFailureReason") or source.get("status") or "unknown")
            failure_counts[reason] = failure_counts.get(reason, 0) + 1
            if len(repair_samples) < 8:
                repair_samples.append({
                    "countryCode": country_code,
                    "sourceCode": source_code,
                    "failureReason": reason,
                    "recommendedStrategy": source.get("recommendedStrategy"),
                    "runId": country.get("runId"),
                })
    for country in current.get("countries") or []:
        country_code = str(country.get("countryCode") or "")
        for source in country.get("sources") or []:
            source_code = str(source.get("sourceCode") or "")
            if country_code and source_code:
                active_sources[(country_code, source_code)] = source
    probe_regressions: list[dict[str, Any]] = []
    for (country_code, source_code), stable_source in stable_good_sources.items():
        active_source = active_sources.get((country_code, source_code))
        if not active_source or active_source.get("status") == "pass":
            continue
        probe_regressions.append({
            "countryCode": country_code,
            "sourceCode": source_code,
            "activeStatus": active_source.get("status"),
            "failureReason": active_source.get("failureReason"),
            "recommendedStrategy": active_source.get("recommendedStrategy"),
            "stableRunId": stable_source.get("_runId") or latest_run_id,
            "activeRunId": active_run_id,
            "lastKnownValid": stable_source.get("valid"),
        })
    source_count = source_rows_observed or total_sources
    source_pass_count = len(stable_good_sources) if source_rows_observed else total_pass
    top_failure_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(failure_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
    ]
    return {
        "gateThreshold": gate_threshold,
        "countryCount": len(all_countries),
        "readyCountryCount": len(ready_countries),
        "blockedCountryCount": max(0, len(all_countries) - len(ready_countries)),
        "stablePassRate": round(total_pass / max(total_sources, 1) * 100, 1) if total_sources > 0 else 0,
        "totalSources": total_sources,
        "totalPass": total_pass,
        "financeObservationCandidates": finance["financeObservationCandidates"],
        "financeMonthlyPaymentCount": finance["financeMonthlyPaymentCount"],
        "financeSemanticsCounts": finance["financeSemanticsCounts"],
        "financeTypeCounts": finance["financeTypeCounts"],
        "sourceRowsObserved": source_rows_observed,
        "sourceCount": source_count,
        "readySourceCount": source_pass_count,
        "blockedSourceCount": max(0, source_count - source_pass_count),
        "sourcePassRate": round(source_pass_count / max(source_count, 1) * 100, 1) if source_count > 0 else 0,
        "topFailureReasons": top_failure_reasons,
        "repairSourceSamples": repair_samples,
        "probeRegressionCount": len(probe_regressions),
        "probeRegressionSamples": probe_regressions[:8],
        "latestRunId": latest_run_id,
        "activeRunId": active_run_id,
        "activeRunRunning": bool(current.get("running")),
        "activeRunPartial": bool(current.get("partial")),
        "activeRunPassRate": float(current.get("overallPassRate") or 0),
        "probeDiffersFromStableRun": bool(active_run_id and latest_run_id and active_run_id != latest_run_id),
        "readyCountries": [str(country.get("countryCode") or "") for country in ready_countries],
        "blockedCountries": [
            str(country.get("countryCode") or "")
            for country in all_countries
            if float(country.get("passRate") or 0) < gate_threshold
        ],
    }


def _current_from_v3_report(report: dict[str, Any], index_data: dict[str, Any] | None = None) -> dict[str, Any]:
    summary = report.get("summary") or {}
    run_id = str(report.get("runId") or "")
    run_meta = None
    for run in (index_data or {}).get("runs", []):
        if run.get("runId") == run_id:
            run_meta = run
            break

    countries: list[dict[str, Any]] = []
    for country in report.get("countriesDetail") or []:
        normalized = _normalize_country_from_v3(country)
        if normalized:
            countries.append(normalized)
    finance = _finance_summary_from_countries(countries, summary)

    if countries:
        total_sources = sum(int(country.get("total") or 0) for country in countries)
        total_pass = sum(int(country.get("pass") or 0) for country in countries)
        total_empty = sum(int(country.get("empty") or 0) for country in countries)
        total_fail = sum(int(country.get("fail") or 0) for country in countries)
        total_errors = sum(int(country.get("errors") or 0) for country in countries)
        overall_pass_rate = round(total_pass / max(total_sources, 1) * 100, 1) if total_sources > 0 else 0
    else:
        total_sources = int(summary.get("total") or 0)
        total_pass = int(summary.get("pass") or 0)
        total_empty = int(summary.get("empty") or 0)
        total_fail = int(summary.get("fail") or 0)
        total_errors = int(summary.get("errors") or 0)
        overall_pass_rate = float(summary.get("passPct") or 0)
    return {
        "available": True,
        "running": False,
        "partial": False,
        "runId": run_id,
        "batch": report.get("batch") or "",
        "schemaVersion": report.get("schemaVersion"),
        "gateStatus": summary.get("gateStatus"),
        "gateThreshold": summary.get("gateThreshold"),
        "logFile": (run_meta or {}).get("logFile") or f"dryrun_report_{run_id}.json",
        "startedAt": (run_meta or {}).get("startedAt") or report.get("generatedAt"),
        "finishedAt": (run_meta or {}).get("finishedAt") or report.get("generatedAt"),
        "countries": _dedupe_countries(countries),
        "expectedCountries": _country_code_list(report.get("expectedCountries")),
        "observedCountries": _country_code_list(report.get("observedCountries")),
        "missingCountries": _country_code_list(report.get("missingCountries")),
        "duplicateCountries": _country_code_list(report.get("duplicateCountries")),
        "totalSources": total_sources,
        "totalPass": total_pass,
        "totalEmpty": total_empty,
        "totalFail": total_fail + total_errors,
        "overallPassRate": overall_pass_rate,
        "financeObservationCandidates": finance["financeObservationCandidates"],
        "financeMonthlyPaymentCount": finance["financeMonthlyPaymentCount"],
        "financeSemanticsCounts": finance["financeSemanticsCounts"],
        "financeTypeCounts": finance["financeTypeCounts"],
        "failureBreakdown": summary.get("failureBreakdown") or {},
        "strategyRecommendations": summary.get("strategyRecommendations") or {},
        "recentResults": [],
    }


def _history_from_runs_index() -> list[dict[str, Any]]:
    index_data = _load_json(RUNS_INDEX_PATH)
    if not index_data:
        return []
    history: list[dict[str, Any]] = []
    for run in sorted(
        index_data.get("runs") or [],
        key=_run_recency_key,
        reverse=True,
    ):
        report = None
        artifact_path = _artifact_path_from_ref(run.get("artifactPath"))
        if artifact_path:
            report = _load_json(artifact_path)
        countries_detail: list[dict[str, Any]] = []
        if report and report.get("schemaVersion") == "msrp_dryrun_report_v3":
            for country in report.get("countriesDetail") or []:
                code = str(country.get("countryCode") or "").lower()
                countries_detail.append({
                    "countryCode": code,
                    "countryLabel": _country_label(code),
                    "total": int(country.get("total") or 0),
                    "pass": int(country.get("pass") or 0),
                    "empty": int(country.get("empty") or 0),
                    "fail": int(country.get("fail") or 0) + int(country.get("errors") or 0),
                    "passRate": float(country.get("passPct") or 0),
                    "financeObservationCandidates": _int_value(country.get("financeObservationCandidates")),
                    "financeMonthlyPaymentCount": _int_value(country.get("financeMonthlyPaymentCount")),
                    "financeSemanticsCounts": _normalize_count_map(country.get("financeSemanticsCounts")),
                    "financeTypeCounts": _normalize_count_map(country.get("financeTypeCounts")),
                })
        report_summary = (report or {}).get("summary") or {}
        history.append({
            "runId": run.get("runId") or "",
            "batch": run.get("batch") or "",
            "countries": (report or {}).get("expectedCountries") or [],
            "total": int(run.get("total") or 0),
            "pass": int(run.get("pass") or 0),
            "empty": int(run.get("empty") or 0),
            "fail": int(run.get("fail") or 0) + int(run.get("errors") or 0),
            "errors": int(run.get("errors") or 0),
            "passRate": float(run.get("passPct") or 0),
            "financeObservationCandidates": (
                _int_value(run.get("financeObservationCandidates"))
                or _int_value(report_summary.get("financeObservationCandidates"))
            ),
            "financeMonthlyPaymentCount": (
                _int_value(run.get("financeMonthlyPaymentCount"))
                or _int_value(report_summary.get("financeMonthlyPaymentCount"))
            ),
            "timestamp": run.get("finishedAt") or run.get("startedAt") or "",
            "file": Path(str(run.get("artifactPath") or "")).name or f"dryrun_report_{run.get('runId')}.json",
            "gateStatus": run.get("gateStatus"),
            "status": run.get("status"),
            "countriesDetail": countries_detail,
        })
    return history[:100]


def _list_log_files() -> list[Path]:
    if not LOG_DIR.exists():
        return []
    files = sorted(LOG_DIR.glob("msrp-dryrun-*.log"), reverse=True)
    return files


def _list_run_dirs() -> list[Path]:
    if not LOG_DIR.exists():
        return []
    return sorted(
        [path for path in LOG_DIR.glob("msrp-dryrun-*") if path.is_dir()],
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )


def _run_dir_for_partial(run_id: str | None = None) -> Path | None:
    if run_id:
        path = LOG_DIR / run_id
        return path if path.is_dir() else None
    run_dirs = _list_run_dirs()
    return run_dirs[0] if run_dirs else None


def _expected_countries_from_run_log(run_dir: Path) -> list[str]:
    log_path = run_dir / "run.log"
    if not log_path.is_file():
        return []
    text = log_path.read_text(errors="replace")
    countries: list[str] = []
    seen: set[str] = set()
    info_match = re.search(r"^\[INFO\]\s+Countries:\s+(.+)$", text, flags=re.MULTILINE)
    if info_match:
        for raw_code in re.split(r"[\s,]+", info_match.group(1).strip()):
            code = raw_code.strip().lower()
            if code and code not in seen:
                countries.append(code)
                seen.add(code)
    for match in re.finditer(r"\[RUN\]\s+\d+/\d+\s+country=(\w+)\s+mode=", text):
        code = match.group(1).strip().lower()
        if code and code not in seen:
            countries.append(code)
            seen.add(code)
    return countries


def _parse_country_log(log_path: Path) -> dict[str, Any]:
    if not log_path.is_file():
        return {"sources": [], "completed": False, "total": 0, "pass": 0, "empty": 0, "fail": 0, "errors": 0}
    text = log_path.read_text(errors="replace")
    sources: list[dict[str, Any]] = []
    for m in re.finditer(
        r"\[\s*(\d+)/(\d+)\]\s*(\S+)\s+(\S+)\s+valid=(\d+)\s+extracted=(\d+)\s+rejected=(\d+)\s+\(([\d.]+)s\)",
        text,
    ):
        sources.append(_source_from_log_match(m))
    summary_m = re.search(
        r"Results:\s*(\d+)/(\d+)\s+PASS,\s*(\d+)\s+empty,\s*(\d+)\s+rejected-all,\s*(\d+)\s+errors",
        text,
    )
    if summary_m:
        pass_count, empty_count, fail_count, error_count = _source_counts(sources)
        if not sources:
            pass_count = int(summary_m.group(1))
            empty_count = int(summary_m.group(3))
            fail_count = int(summary_m.group(4))
            error_count = int(summary_m.group(5))
        return {
            "sources": sources,
            "completed": True,
            "total": int(summary_m.group(2)),
            "pass": pass_count,
            "empty": empty_count,
            "fail": fail_count,
            "errors": error_count,
        }
    pass_count, empty_count, fail_count, error_count = _source_counts(sources)
    total = max([int(source.get("totalInCountry") or 0) for source in sources] or [0])
    return {
        "sources": sources,
        "completed": False,
        "total": total,
        "pass": pass_count,
        "empty": empty_count,
        "fail": fail_count,
        "errors": error_count,
    }


def _country_from_artifact(path: Path) -> dict[str, Any] | None:
    artifact = _load_json(path)
    if not artifact or artifact.get("schemaVersion") != "msrp_dryrun_country_v1":
        return None
    code = str(artifact.get("country") or path.stem).strip().lower()
    total = int(artifact.get("total") or 0)
    sources = [
        _normalize_source(source, index, total)
        for index, source in enumerate(artifact.get("results") or [], start=1)
    ]
    failure_breakdown = artifact.get("failureBreakdown") or {}
    top_failure = max(failure_breakdown, key=failure_breakdown.get) if failure_breakdown else None
    if sources:
        pass_count, empty_count, fail_count, error_count = _source_counts(sources)
    else:
        pass_count = int(artifact.get("pass") or 0)
        empty_count = int(artifact.get("empty") or 0)
        error_count = int(artifact.get("errors") or 0)
        fail_count = int(artifact.get("fail") or 0)
    pass_rate = round(pass_count / max(total, 1) * 100, 1) if total > 0 else 0
    return {
        "countryCode": code,
        "countryLabel": _country_label(code),
        "total": total,
        "pass": pass_count,
        "empty": empty_count,
        "fail": fail_count,
        "errors": error_count,
        "completed": True,
        "passRate": pass_rate,
        "status": _status_for_pass_rate(pass_rate),
        "topFailureReason": top_failure,
        "failureBreakdown": failure_breakdown,
        "strategyRecommendations": artifact.get("strategyRecommendations") or {},
        **_finance_summary_from_country(artifact, sources),
        "sources": sources,
    }


def _current_from_partial_run_dir(run_id: str | None = None) -> dict[str, Any] | None:
    run_dir = _run_dir_for_partial(run_id)
    if not run_dir:
        return None

    countries_by_code: dict[str, dict[str, Any]] = {}
    country_artifact_dir = run_dir / "countries"
    if country_artifact_dir.is_dir():
        for path in sorted(country_artifact_dir.glob("*.json")):
            country = _country_from_artifact(path)
            if country:
                countries_by_code[country["countryCode"]] = country

    expected = _expected_countries_from_run_log(run_dir)
    if not expected:
        expected = sorted(countries_by_code)

    for code in expected:
        if code in countries_by_code:
            continue
        parsed_log = _parse_country_log(run_dir / f"{code}.log")
        total = int(parsed_log.get("total") or 0)
        pass_count = int(parsed_log.get("pass") or 0)
        empty_count = int(parsed_log.get("empty") or 0)
        fail_count = int(parsed_log.get("fail") or 0)
        countries_by_code[code] = {
            "countryCode": code,
            "countryLabel": _country_label(code),
            "total": total,
            "pass": pass_count,
            "empty": empty_count,
            "fail": fail_count,
            "errors": int(parsed_log.get("errors") or 0),
            "completed": bool(parsed_log.get("completed")),
            "passRate": round(pass_count / max(total, 1) * 100, 1) if total > 0 else 0,
            "status": "success" if parsed_log.get("completed") else "running",
            "sources": parsed_log.get("sources") or [],
            "failureBreakdown": {},
            "strategyRecommendations": {},
        }

    ordered_codes = expected + [code for code in sorted(countries_by_code) if code not in expected]
    countries = [countries_by_code[code] for code in ordered_codes if code in countries_by_code]
    if not countries:
        return None

    total_sources = sum(int(country.get("total") or 0) for country in countries)
    total_pass = sum(int(country.get("pass") or 0) for country in countries)
    total_empty = sum(int(country.get("empty") or 0) for country in countries)
    total_fail = sum(int(country.get("fail") or 0) + int(country.get("errors") or 0) for country in countries)
    failure_breakdown = _aggregate_country_counter(countries, "failureBreakdown")
    strategy_recommendations = _aggregate_country_counter(countries, "strategyRecommendations")
    finance = _finance_summary_from_countries(countries)
    started = _parse_run_dir_timestamp(run_dir.name)
    # Incomplete country artifacts mean the run is partial, not necessarily live.
    # Treat the lock file as the authoritative local signal for an active run so
    # abandoned partial artifacts do not mask the latest completed v3 report.
    running = _is_running()
    return {
        "available": True,
        "running": running,
        "partial": True,
        "runId": run_dir.name,
        "batch": "",
        "schemaVersion": "msrp_dryrun_partial_v1",
        "gateStatus": "pending" if running else None,
        "gateThreshold": None,
        "logFile": f"{run_dir.name}/run.log",
        "startedAt": started.isoformat() if started else datetime.fromtimestamp(run_dir.stat().st_mtime, tz=timezone.utc).isoformat(),
        "finishedAt": None,
        "countries": countries,
        "expectedCountries": ordered_codes,
        "observedCountries": [country["countryCode"] for country in countries if country.get("completed")],
        "missingCountries": [country["countryCode"] for country in countries if not country.get("completed")],
        "duplicateCountries": [],
        "totalSources": total_sources,
        "totalPass": total_pass,
        "totalEmpty": total_empty,
        "totalFail": total_fail,
        "overallPassRate": round(total_pass / max(total_sources, 1) * 100, 1) if total_sources > 0 else 0,
        "financeObservationCandidates": finance["financeObservationCandidates"],
        "financeMonthlyPaymentCount": finance["financeMonthlyPaymentCount"],
        "financeSemanticsCounts": finance["financeSemanticsCounts"],
        "financeTypeCounts": finance["financeTypeCounts"],
        "failureBreakdown": failure_breakdown,
        "strategyRecommendations": strategy_recommendations,
        "recentResults": [
            source
            for country in countries[-3:]
            for source in (country.get("sources") or [])[-5:]
        ][-20:],
    }


def _current_from_running_pipeline_status() -> dict[str, Any] | None:
    status = _load_json(PIPELINE_STATUS_PATH)
    if not status or str(status.get("status") or "").lower() != "running":
        return None

    message = str(status.get("message") or "")
    match = DRYRUN_RUN_ID_PATTERN.search(message)
    run_id = match.group(1) if match else "msrp-dryrun-pending"

    metadata = status.get("metadata") if isinstance(status.get("metadata"), dict) else {}
    countries = [
        str(country).strip().lower()
        for country in (metadata.get("countries") or [])
        if str(country).strip()
    ]
    if not countries and metadata.get("countriesRaw"):
        countries = [
            country.strip().lower()
            for country in str(metadata.get("countriesRaw") or "").split(",")
            if country.strip()
        ]
    country_entries = [
        {
            "countryCode": code,
            "countryLabel": _country_label(code),
            "total": 0,
            "pass": 0,
            "empty": 0,
            "fail": 0,
            "errors": 0,
            "completed": False,
            "passRate": 0.0,
            "status": "running",
            "topFailureReason": None,
            "failureBreakdown": {},
            "strategyRecommendations": {},
            "sources": [],
        }
        for code in countries
    ]
    started_at = status.get("startedAt") or status.get("lastRunAt") or status.get("finishedAt")
    return {
        "available": True,
        "running": True,
        "partial": True,
        "runId": run_id,
        "batch": "",
        "schemaVersion": "msrp_dryrun_partial_v1",
        "gateStatus": "pending",
        "gateThreshold": None,
        "logFile": "hermes/reports/pipeline_status/msrp_dryrun.json",
        "startedAt": started_at,
        "finishedAt": None,
        "countries": country_entries,
        "expectedCountries": countries,
        "observedCountries": [],
        "missingCountries": countries,
        "duplicateCountries": [],
        "totalSources": 0,
        "totalPass": 0,
        "totalEmpty": 0,
        "totalFail": 0,
        "overallPassRate": 0.0,
        "financeObservationCandidates": 0,
        "financeMonthlyPaymentCount": 0,
        "financeSemanticsCounts": {},
        "financeTypeCounts": {},
        "failureBreakdown": {},
        "strategyRecommendations": {},
        "recentResults": [],
        "pipelineMessage": message,
    }


def _parse_current_progress(log_path: Path | None = None) -> dict[str, Any]:
    """Parse the latest dryrun progress — supports both parallel per-country logs
    (msrp-dryrun-{country}-*.log) and legacy sequential logs (msrp-dryrun-*.log)."""
    if log_path is None:
        # First try parallel per-country logs
        country_logs = sorted(LOG_DIR.glob("msrp-dryrun-??-*.log"), reverse=True) if LOG_DIR.exists() else []
        if country_logs:
            return _parse_parallel_progress(country_logs)
        # Fall back to sequential log
        files = _list_log_files()
        if not files:
            return {"available": False, "reason": "no_log_files"}
        log_path = files[0]

    if not log_path.exists():
        return {"available": False, "reason": "log_not_found", "path": str(log_path)}

    return _parse_sequential_log(log_path)


def _parse_parallel_progress(country_logs: list[Path]) -> dict[str, Any]:
    """Parse per-country parallel dryrun logs."""
    countries = []
    total_sources = total_pass = total_empty = total_fail = 0
    running = _is_running()

    for log_path in sorted(country_logs):
        # Extract country code from filename: msrp-dryrun-se-20260515-HHMMSS.log
        name = log_path.name
        parts = name.replace("msrp-dryrun-", "").split("-")
        if len(parts) < 1:
            continue
        cc = parts[0]
        if len(cc) != 2 or not cc.isalpha():
            # Not a country log, might be the main log
            if name.startswith("msrp-dryrun-20"):
                continue  # skip main sequential logs
            continue

        text = log_path.read_text(errors="replace")
        ts = _parse_log_timestamp(name)

        sources = []
        for m in re.finditer(
            r"\[\s*(\d+)/(\d+)\]\s*(\S+)\s+(\S+)\s+valid=(\d+)\s+extracted=(\d+)\s+rejected=(\d+)\s+\(([\d.]+)s\)",
            text,
        ):
            sources.append(_source_from_log_match(m))

        summary_m = re.search(
            r"Results:\s*(\d+)/(\d+)\s+PASS,\s*(\d+)\s+empty,\s*(\d+)\s+rejected-all,\s*(\d+)\s+errors",
            text,
        )
        total = int(summary_m.group(2)) if summary_m else len(sources)
        pass_count, empty_count, fail_count, error_count = _source_counts(sources)
        if summary_m and not sources:
            pass_count = int(summary_m.group(1))
            empty_count = int(summary_m.group(3))
            fail_count = int(summary_m.group(4))
            error_count = int(summary_m.group(5))

        countries.append({
            "countryCode": cc,
            "countryLabel": _country_label(cc),
            "total": total,
            "pass": pass_count,
            "empty": empty_count,
            "fail": fail_count,
            "errors": error_count,
            "completed": bool(summary_m),
            "passRate": round(pass_count / max(total, 1) * 100, 1),
            "sources": sources,
        })
        total_sources += total
        total_pass += pass_count
        total_empty += empty_count
        total_fail += fail_count + error_count

    return {
        "available": True,
        "running": running,
        "logFile": f"{len(country_logs)} country logs",
        "startedAt": datetime.fromtimestamp(min(l.stat().st_mtime for l in country_logs), tz=timezone.utc).isoformat() if country_logs else None,
        "countries": countries,
        "totalSources": total_sources,
        "totalPass": total_pass,
        "totalEmpty": total_empty,
        "totalFail": total_fail,
        "overallPassRate": round(total_pass / max(total_sources, 1) * 100, 1) if total_sources > 0 else 0,
        "recentResults": [],
    }


def _parse_sequential_log(log_path: Path) -> dict[str, Any]:
    """Parse legacy sequential dryrun log."""
    text = log_path.read_text(errors="replace")
    ts = _parse_log_timestamp(log_path.name)

    result: dict[str, Any] = {
        "available": True,
        "running": _is_running(),
        "logFile": log_path.name,
        "startedAt": ts.isoformat() if ts else None,
        "countries": [],
        "totalSources": 0,
        "totalPass": 0,
        "totalEmpty": 0,
        "totalFail": 0,
        "recentResults": [],
    }

    # Parse country sections
    country_sections = re.split(r"\[RUN\] \d+/\d+ country=(\w+) mode=\w+", text)
    # country_sections[0] = preamble, then alternating [country_code, section_text, ...]
    if len(country_sections) < 2:
        return result

    # Skip preamble
    for i in range(1, len(country_sections), 2):
        cc = country_sections[i]
        section = country_sections[i + 1] if i + 1 < len(country_sections) else ""

        # Parse per-source results
        sources: list[dict[str, Any]] = []
        # Match: [ 5/29] ✅ kia_ev3...   valid=17 extracted=17 rejected=0 (55.4s)
        for m in re.finditer(
            r"\[\s*(\d+)/(\d+)\]\s*(\S+)\s+(\S+)\s+valid=(\d+)\s+extracted=(\d+)\s+rejected=(\d+)\s+\(([\d.]+)s\)",
            section,
        ):
            sources.append(_source_from_log_match(m))

        # Parse country summary from Results line
        summary_m = re.search(
            r"Results:\s*(\d+)/(\d+)\s+PASS,\s*(\d+)\s+empty,\s*(\d+)\s+rejected-all,\s*(\d+)\s+errors",
            section,
        )
        total = int(summary_m.group(2)) if summary_m else len(sources)
        pass_count, empty_count, fail_count, error_count = _source_counts(sources)
        if summary_m and not sources:
            pass_count = int(summary_m.group(1))
            empty_count = int(summary_m.group(3))
            fail_count = int(summary_m.group(4))
            error_count = int(summary_m.group(5))

        country_entry: dict[str, Any] = {
            "countryCode": cc,
            "countryLabel": _country_label(cc),
            "total": total,
            "pass": pass_count,
            "empty": empty_count,
            "fail": fail_count,
            "errors": error_count,
            "completed": bool(summary_m),
            "passRate": round(pass_count / max(total, 1) * 100, 1),
            "sources": sources,
        }
        result["countries"].append(country_entry)
        result["totalSources"] += total
        result["totalPass"] += pass_count
        result["totalEmpty"] += empty_count
        result["totalFail"] += fail_count + error_count

        # Collect recent results (last 20 across all countries)
        if sources:
            result["recentResults"].extend(sources[-5:])

    result["recentResults"] = result["recentResults"][-20:]
    if result["totalSources"] > 0:
        result["overallPassRate"] = round(result["totalPass"] / max(result["totalSources"], 1) * 100, 1)
    else:
        result["overallPassRate"] = 0

    return result


def _list_historical_runs() -> list[dict[str, Any]]:
    """List past dryrun reports from timestamped artifact JSON files.

    Skips dryrun_report.json (the latest-run shortcut) to avoid duplicates.
    """
    runs: list[dict[str, Any]] = []
    if not ARTIFACT_DIR.exists():
        return runs

    for report_file in sorted(ARTIFACT_DIR.glob("dryrun_report_*.json"), reverse=True):
        # Skip the non-timestamped latest copy if it was picked up
        if report_file.name == "dryrun_report.json":
            continue
        try:
            data = json.loads(report_file.read_text())
            saved_at = data.get("savedAt")
            if saved_at:
                ts = saved_at
            else:
                ts = datetime.fromtimestamp(report_file.stat().st_mtime, tz=timezone.utc).isoformat()

            # Per-country breakdown from results array
            by_country: dict[str, dict[str, int]] = {}
            for r in data.get("results") or []:
                cc = str(r.get("country", "")).strip().lower()
                if not cc:
                    continue
                if cc not in by_country:
                    by_country[cc] = {"pass": 0, "empty": 0, "fail": 0, "total": 0}
                st = r.get("status", "empty")
                if st in ("pass", "dry_run"):
                    by_country[cc]["pass"] += 1
                elif st == "empty":
                    by_country[cc]["empty"] += 1
                else:
                    by_country[cc]["fail"] += 1
                by_country[cc]["total"] += 1

            countries_detail = [
                {
                    "countryCode": c, "countryLabel": _country_label(c),
                    "total": v["total"], "pass": v["pass"],
                    "empty": v["empty"], "fail": v["fail"],
                    "passRate": round(v["pass"] / max(v["total"], 1) * 100, 1),
                }
                for c, v in sorted(by_country.items())
            ]
            finance = _finance_summary_from_countries(
                countries_detail,
                data.get("summary") if isinstance(data.get("summary"), dict) else data,
            )

            runs.append({
                "batch": data.get("batch", ""),
                "countries": data.get("countries", []),
                "total": data.get("total", 0),
                "pass": data.get("pass", 0),
                "empty": data.get("empty", 0),
                "fail": data.get("fail", 0),
                "errors": data.get("errors", 0),
                "passRate": round(data.get("pass", 0) / max(data.get("total", 1), 1) * 100, 1),
                "financeObservationCandidates": finance["financeObservationCandidates"],
                "financeMonthlyPaymentCount": finance["financeMonthlyPaymentCount"],
                "timestamp": ts if isinstance(ts, str) else ts.isoformat() if hasattr(ts, 'isoformat') else str(ts),
                "file": report_file.name,
                "countriesDetail": countries_detail,
            })
        except (json.JSONDecodeError, KeyError):
            continue

    return runs[:30]


def get_dryrun_dashboard(run_id: str | None = None) -> dict[str, Any]:
    """Return combined dashboard data: live progress + history."""
    index_data = _load_json(RUNS_INDEX_PATH)
    current = None
    latest_shortcut = _load_json(LATEST_REPORT_PATH) if run_id is None else None
    if run_id is None:
        current = _current_from_partial_run_dir() or _current_from_running_pipeline_status()
    report = _load_v3_report(run_id)
    if (
        not report
        and run_id is None
        and (
            not current
            or (bool(current.get("partial")) and not bool(current.get("running")))
        )
    ):
        report = _load_latest_indexed_v3_report(index_data)
    if current and current.get("running") and (_is_running() or current.get("pipelineMessage")):
        report = None
    if report:
        current = _current_from_v3_report(report, index_data)
        history = _history_from_runs_index() or _list_historical_runs()
    else:
        current = current or _current_from_partial_run_dir(run_id) or _parse_current_progress()
        history = _history_from_runs_index() or _list_historical_runs()

    # Also list available log files for drill-down
    log_files = []
    for f in _list_log_files()[:10]:
        ts = _parse_log_timestamp(f.name)
        log_files.append({
            "name": f.name,
            "size": f.stat().st_size if f.exists() else 0,
            "timestamp": ts.isoformat() if ts else None,
        })

    all_countries = _all_country_latest_from_runs_index(index_data) or current.get("countries", [])
    return {
        "current": current,
        "allCountries": all_countries,
        "stableCoverage": _stable_coverage_summary(all_countries, current),
        "history": history,
        "logFiles": log_files,
        "selectedRunId": run_id,
        "latestRunId": (index_data or {}).get("latestRunId"),
        "serverTime": datetime.now(timezone.utc).isoformat(),
    }


def get_dryrun_country_detail(log_file: str, country_code: str) -> dict[str, Any]:
    """Return detailed results for a specific country from a specific log."""
    log_path = LOG_DIR / log_file
    if not log_path.exists():
        return {"available": False, "reason": "log_not_found"}

    progress = _parse_current_progress(log_path)
    for c in progress.get("countries", []):
        if c["countryCode"] == country_code.lower():
            return {"available": True, "country": c, "logFile": log_file}
    return {"available": False, "reason": "country_not_in_log"}
