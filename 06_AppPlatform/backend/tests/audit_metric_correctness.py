"""
Metric correctness reconciliation harness — v2 (focused).

Checks:
  1. Fuel trend bars vs parquet truth (month / YTD / rolling12)
  2. Fuel/powertrain dimension: breakdown sum = total, unknown ratio
  3. Drivetrain (4WD/2WD) mapping: raw values, coverage
  4. Registration (Business/Private) mapping: raw values, coverage
  5. YoY: prior periods loaded, "New" ratio not suspicious
  6. Custom period: columns loaded, prior columns loaded

Usage:
  cd 06_AppPlatform/backend && python tests/audit_metric_correctness.py
"""

import json
import sys
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import requests

sys.path.insert(0, ".")
from app.infra import parquet_repository as repo
from app.services.market_scan_service import (
    _resolve_columns, _normalize_powertrain, _normalize_drive_type,
    _normalize_registration_type, _available_periods, _period_to_month_column,
    _compute_needed_periods, DEFAULT_FUEL_TYPES,
)

API_BASE = "http://127.0.0.1:8000"
COUNTRY = "瑞典"
RESOLVED = "2026-03"
FUEL_ORDER = list(DEFAULT_FUEL_TYPES)
DD_SEGMENT = "SUV A"

# ── helpers ──────────────────────────────────────────────────────────


def _api(path: str, body: dict) -> dict:
    r = requests.post(f"{API_BASE}{path}", json=body, timeout=60)
    r.raise_for_status()
    return r.json()


def _load_frame(periods_needed: list[str], country: str = COUNTRY,
                segment: str | None = None) -> pd.DataFrame:
    cols = _resolve_columns(repo.current_dataset_token())
    month_cols = [_period_to_month_column(p) for p in periods_needed if p in _available_periods(cols)]
    needed = [cols.country_value, cols.make, cols.model, cols.segment, cols.powertrain]
    for c in [cols.drive_type, cols.registration_type]:
        if c:
            needed.append(c)
    needed += month_cols
    dataset = repo._open_dataset()
    t = dataset.to_table(columns=needed, filter=repo._build_filter_expression({cols.country_value: [country]}))
    df = t.to_pandas()
    df["__brand"] = df[cols.make].astype(str).str.strip()
    df["__model"] = df[cols.model].astype(str).str.strip()
    df["__segment_raw"] = df[cols.segment].astype(str).str.strip()
    df["__powertrain"] = df[cols.powertrain].map(_normalize_powertrain)
    df["__drive_type"] = df[cols.drive_type].map(_normalize_drive_type) if cols.drive_type and cols.drive_type in df.columns else "OTHER"
    df["__registration_type"] = df[cols.registration_type].map(_normalize_registration_type) if cols.registration_type and cols.registration_type in df.columns else "Other"
    if segment:
        df = df[df["__segment_raw"] == segment].copy()
    return df, [c for c in month_cols if c in df.columns]


def _sum_cols(df: pd.DataFrame, cols: list[str]) -> float:
    present = [c for c in cols if c in df.columns]
    if not present:
        return 0.0
    return float(df[present].sum().sum())


@dataclass
class AuditResult:
    metric: str
    period_label: str
    status: str = "PASS"
    api_value: float = 0.0
    truth_value: float = 0.0
    delta: float = 0.0
    delta_pct: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def summary(self) -> str:
        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}[self.status]
        return f"  {icon} {self.metric:40s} truth={self.truth_value:>10,.0f}  delta={self.delta:>+8,.0f}  {self._extra()}"

    def _extra(self) -> str:
        items = []
        for k, v in self.details.items():
            if isinstance(v, float):
                items.append(f"{k}={v:.3f}")
            elif isinstance(v, int) and v > 1000:
                items.append(f"{k}={v:,}")
            elif not isinstance(v, (dict, list)):
                items.append(f"{k}={v}")
        return "  ".join(items[:3])


def check(metric: str, period_label: str, api_val: float, truth_val: float,
          tolerance: float = 0.01, **details) -> AuditResult:
    delta = api_val - truth_val
    delta_pct = delta / truth_val if truth_val != 0 else (float("inf") if api_val != 0 else 0.0)
    if truth_val == 0 and api_val == 0:
        status = "PASS"
    elif abs(delta_pct) <= tolerance:
        status = "PASS"
    elif abs(delta_pct) <= 0.05:
        status = "WARN"
    else:
        status = "FAIL"
    return AuditResult(metric=metric, period_label=period_label, status=status,
                       api_value=api_val, truth_value=truth_val, delta=delta,
                       delta_pct=delta_pct, details=details)


def info(metric: str, period_label: str, status: str = "PASS", **details) -> AuditResult:
    return AuditResult(metric=metric, period_label=period_label, status=status,
                       details=details)


# ── audits ───────────────────────────────────────────────────────────


def audit_trend_bars(results: list[AuditResult]) -> None:
    """Compare fuel trend bars (month/ytd/r12) against parquet."""
    print("\n═══ 1. TREND BARS vs PARQUET ═══")
    available = _available_periods(_resolve_columns(repo.current_dataset_token()))
    all_needed = sorted(set().union(*_compute_needed_periods(
        available_periods=available, resolved_period=RESOLVED,
        trend_window_months=24, origin_window_months=24, body_window_months=24,
        same_month_last_year_period="2025-03", prior_period="2026-02", custom_periods=None,
    ).values()))
    df, _ = _load_frame(all_needed, segment=DD_SEGMENT)

    api = _api("/v1/market-scan/deck", {
        "country": COUNTRY, "target_period": RESOLVED, "fuel_types": FUEL_ORDER,
        "ranking_limit": 10, "drilldown_segment": DD_SEGMENT,
    })
    dd = api["results"]["drilldown"]

    # Month trend bars
    for item in dd.get("monthFuelTrend", {}).get("items", []):
        y = int("20" + item["label"].split(".")[0])
        m = int(item["label"].split(".")[1])
        col = _period_to_month_column(f"{y}-{m:02d}")
        truth = _sum_cols(df, [col])
        results.append(check(f"month_trend_{item['label']}", item["label"],
                            item["totalVolume"], truth))

    # YTD trend bars
    for item in dd.get("ytdFuelTrend", {}).get("items", []):
        y = int("20" + item["label"].split(",")[0])
        m = int(item["label"].split("-")[1])
        ytd_p = [p for p in available if p.startswith(f"{y}-") and int(p[5:7]) <= m]
        ytd_c = [_period_to_month_column(p) for p in ytd_p]
        truth = _sum_cols(df, ytd_c)
        results.append(check(f"ytd_trend_{item['label']}", item["label"],
                            item["totalVolume"], truth))

    # Rolling12 trend bars
    for item in dd.get("rolling12FuelTrend", {}).get("items", []):
        mc = item.get("monthCount", 0)
        cr = item.get("coverageRatio", 0.0)
        if mc < 12:
            results.append(AuditResult(
                metric=f"r12_coverage_{item['label']}", period_label=item["label"],
                status="FAIL", truth_value=mc, delta=mc - 12,
                details={"coverageRatio": cr, "monthCount": mc},
            ))
        else:
            results.append(info(f"r12_coverage_{item['label']}", item["label"],
                               monthCount=mc, coverageRatio=cr))


def audit_fuel_dimension(results: list[AuditResult]) -> None:
    """Fuel breakdown sums to total. Check unknown fuel ratio."""
    print("\n═══ 2. FUEL / POWERTRAIN ═══")
    available = _available_periods(_resolve_columns(repo.current_dataset_token()))
    all_needed = sorted(set().union(*_compute_needed_periods(
        available_periods=available, resolved_period=RESOLVED,
        trend_window_months=24, origin_window_months=24, body_window_months=24,
        same_month_last_year_period="2025-03", prior_period="2026-02", custom_periods=None,
    ).values()))
    df, month_cols = _load_frame(all_needed, segment=DD_SEGMENT)
    cur_col = month_cols[-1] if month_cols else _period_to_month_column(RESOLVED)  # 2026 Mar
    total = _sum_cols(df, [cur_col])

    known_fuels = {"BEV", "PHEV", "HEV", "MHEV", "ICE"}
    fuel_sums: dict[str, float] = {}
    for ft in known_fuels:
        fuel_sums[ft] = _sum_cols(df[df["__powertrain"] == ft], [cur_col])

    unknown = _sum_cols(df[~df["__powertrain"].isin(known_fuels)], [cur_col])
    fuel_total = sum(fuel_sums.values()) + unknown

    # Show breakdown
    for ft in sorted(known_fuels):
        results.append(info(f"fuel_{ft}", RESOLVED, truth_value=fuel_sums[ft]))

    # Gap check
    gap = total - fuel_total
    gap_pct = gap / total if total > 0 else 0
    status = "FAIL" if abs(gap_pct) > 0.05 else ("WARN" if abs(gap_pct) > 0.01 else "PASS")
    results.append(AuditResult(
        metric="fuel_total_vs_sum", period_label=RESOLVED, status=status,
        truth_value=total, delta=gap, delta_pct=gap_pct,
        details={"fuel_sum": fuel_total, "total": total, "unknown": unknown},
    ))

    # Unknown check
    unknown_pct = unknown / total if total > 0 else 0
    ustatus = "FAIL" if unknown_pct > 0.05 else ("WARN" if unknown_pct > 0.01 else "PASS")
    results.append(AuditResult(
        metric="fuel_unknown_ratio", period_label=RESOLVED, status=ustatus,
        truth_value=unknown, delta_pct=unknown_pct,
        details={"unknown_sales": unknown, "total_sales": total, "ratio": unknown_pct},
    ))

    # Show raw powertrain values for data quality
    cols = _resolve_columns(repo.current_dataset_token())
    raw_pts = df[df[cur_col] > 0][cols.powertrain].value_counts().to_dict()
    results.append(info("fuel_raw_values", RESOLVED, raw_types=str(raw_pts)[:200]))


def audit_drivetrain(results: list[AuditResult]) -> None:
    """4WD/2WD/OTHER coverage and raw drive type values."""
    print("\n═══ 3. DRIVETRAIN (4WD/2WD) ═══")
    available = _available_periods(_resolve_columns(repo.current_dataset_token()))
    all_needed = sorted(set().union(*_compute_needed_periods(
        available_periods=available, resolved_period=RESOLVED,
        trend_window_months=24, origin_window_months=24, body_window_months=24,
        same_month_last_year_period="2025-03", prior_period="2026-02", custom_periods=None,
    ).values()))
    df, month_cols = _load_frame(all_needed, segment=DD_SEGMENT)
    cur_col = month_cols[-1] if month_cols else _period_to_month_column(RESOLVED)
    total = _sum_cols(df, [cur_col])

    for dt in ["4WD", "2WD", "OTHER"]:
        val = _sum_cols(df[df["__drive_type"] == dt], [cur_col])
        pct = val / total if total > 0 else 0
        results.append(info(f"drivetrain_{dt}", RESOLVED, truth_value=val, share_pct=f"{pct:.1%}"))

    drive_sum = sum(_sum_cols(df[df["__drive_type"] == dt], [cur_col]) for dt in ["4WD", "2WD", "OTHER"])
    gap = total - drive_sum
    gap_pct = gap / total if total > 0 else 0
    status = "FAIL" if abs(gap_pct) > 0.05 else ("WARN" if abs(gap_pct) > 0.01 else "PASS")
    results.append(AuditResult(
        metric="drivetrain_coverage", period_label=RESOLVED, status=status,
        truth_value=drive_sum, delta=gap, delta_pct=gap_pct,
        details={"total": total, "mapped": drive_sum, "gap": gap},
    ))

    # Raw drive_type values for audit
    cols = _resolve_columns(repo.current_dataset_token())
    if cols.drive_type and cols.drive_type in df.columns:
        raw = df[df[cur_col] > 0][cols.drive_type].value_counts().to_dict()
        results.append(info("drivetrain_raw_values", RESOLVED, raw_types=str(raw)[:300]))


def audit_registration(results: list[AuditResult]) -> None:
    """Business/Private/Other coverage and raw registration type values."""
    print("\n═══ 4. REGISTRATION (Business/Private) ═══")
    available = _available_periods(_resolve_columns(repo.current_dataset_token()))
    all_needed = sorted(set().union(*_compute_needed_periods(
        available_periods=available, resolved_period=RESOLVED,
        trend_window_months=24, origin_window_months=24, body_window_months=24,
        same_month_last_year_period="2025-03", prior_period="2026-02", custom_periods=None,
    ).values()))
    df, month_cols = _load_frame(all_needed, segment=DD_SEGMENT)
    cur_col = month_cols[-1] if month_cols else _period_to_month_column(RESOLVED)
    total = _sum_cols(df, [cur_col])

    for rt in ["Business", "Private", "Other"]:
        val = _sum_cols(df[df["__registration_type"] == rt], [cur_col])
        pct = val / total if total > 0 else 0
        results.append(info(f"registration_{rt}", RESOLVED, truth_value=val, share_pct=f"{pct:.1%}"))

    reg_sum = sum(_sum_cols(df[df["__registration_type"] == rt], [cur_col]) for rt in ["Business", "Private", "Other"])
    gap = total - reg_sum
    gap_pct = gap / total if total > 0 else 0
    status = "FAIL" if abs(gap_pct) > 0.05 else ("WARN" if abs(gap_pct) > 0.01 else "PASS")
    results.append(AuditResult(
        metric="registration_coverage", period_label=RESOLVED, status=status,
        truth_value=reg_sum, delta=gap, delta_pct=gap_pct,
        details={"total": total, "mapped": reg_sum, "gap": gap},
    ))

    # Raw registration type values
    cols = _resolve_columns(repo.current_dataset_token())
    if cols.registration_type and cols.registration_type in df.columns:
        raw = df[df[cur_col] > 0][cols.registration_type].value_counts().to_dict()
        results.append(info("registration_raw_values", RESOLVED, raw_types=str(raw)[:300]))


def audit_yoy(results: list[AuditResult]) -> None:
    """Check YoY prior periods are loaded; 'New' ratio is not suspicious."""
    print("\n═══ 5. YoY PRIOR PERIODS ═══")
    available = _available_periods(_resolve_columns(repo.current_dataset_token()))
    periods_info = _compute_needed_periods(
        available_periods=available, resolved_period=RESOLVED,
        trend_window_months=24, origin_window_months=24, body_window_months=24,
        same_month_last_year_period="2025-03", prior_period="2026-02", custom_periods=None,
    )

    # same_month
    sm = periods_info["same_month"]
    results.append(AuditResult(
        metric="yoy_same_month", period_label=RESOLVED,
        status="PASS" if sm else "FAIL",
        truth_value=len(sm), details={"periods": sm},
    ))

    # prior_ytd
    py = periods_info["prior_ytd"]
    results.append(AuditResult(
        metric="yoy_prior_ytd_months", period_label=RESOLVED,
        status="PASS" if len(py) > 0 else "FAIL",
        truth_value=len(py), details={"periods": py},
    ))

    # prior_r12
    pr = periods_info["prior_r12"]
    results.append(AuditResult(
        metric="yoy_prior_r12_months", period_label=RESOLVED,
        status="PASS" if len(pr) > 0 else "FAIL",
        truth_value=len(pr), details={"periods": pr},
    ))

    # New ratio
    api = _api("/v1/market-scan/deck", {
        "country": COUNTRY, "target_period": RESOLVED, "fuel_types": FUEL_ORDER,
        "ranking_limit": 10, "drilldown_segment": DD_SEGMENT,
    })
    dd = api["results"]["drilldown"]
    new_count = 0
    total_count = 0
    for fp in dd.get("fuelPanels", []):
        for item in fp.get("monthRanking", []):
            total_count += 1
            if item.get("yoy", {}).get("display") == "New":
                new_count += 1

    new_ratio = new_count / total_count if total_count > 0 else 0
    status = "FAIL" if new_ratio > 0.8 else ("WARN" if new_ratio > 0.5 else "PASS")
    results.append(AuditResult(
        metric="yoy_new_ratio", period_label=RESOLVED, status=status,
        api_value=new_count, truth_value=total_count, delta_pct=new_ratio,
        details={"new_count": new_count, "total_models": total_count},
    ))


def audit_custom_period(results: list[AuditResult]) -> None:
    """Custom period: verify columns are loaded, prior columns loaded, sales non-zero."""
    print("\n═══ 6. CUSTOM PERIOD ═══")
    available = _available_periods(_resolve_columns(repo.current_dataset_token()))

    test_periods = [
        ("2023-04", "2024-03", "early_r12"),
        ("2024-01", "2024-03", "q1_2024"),
        ("2025-01", "2025-12", "full_2025"),
        ("2025-10", "2026-03", "recent_6m"),
        ("2024-03", "2024-03", "single_month_2024"),
        ("2024-11", "2025-02", "cross_year_4m"),
    ]

    for start, end, label in test_periods:
        if start not in available or end not in available:
            results.append(info(f"custom_{label}", f"{start}..{end}", status="WARN",
                               note=f"outside available range ({available[0]}..{available[-1]})"))
            continue

        api = _api("/v1/market-scan/deck", {
            "country": COUNTRY, "target_period": RESOLVED, "fuel_types": FUEL_ORDER,
            "ranking_limit": 10, "drilldown_segment": DD_SEGMENT,
            "time_range": {"start": start, "end": end},
        })
        dd = api["results"]["drilldown"]

        # Check customRangeTotalRanking exists and has data
        cr = dd.get("customRangeTotalRanking")
        if cr is None:
            results.append(AuditResult(
                metric=f"custom_{label}", period_label=f"{start}..{end}",
                status="FAIL" if start != end else "WARN",
                details={"note": "customRangeTotalRanking is null"},
            ))
            continue

        api_total = sum(it["volume"] for it in cr.get("items", []))
        # Truth
        all_p = [p for p in available if start <= p <= end]
        df, _ = _load_frame(all_p, segment=DD_SEGMENT)
        month_cols = [_period_to_month_column(p) for p in all_p]
        truth_total = _sum_cols(df, [c for c in month_cols if c in df.columns])

        # Compare (tolerance relaxed: ranking is top-N)
        delta_pct = (api_total - truth_total) / truth_total if truth_total > 0 else 0
        if truth_total == 0 and api_total == 0:
            status = "PASS"
        elif abs(delta_pct) > 0.5:  # flag only large gaps (ranking limit expected)
            status = "FAIL"
        else:
            status = "PASS"  # OK within ranking limit tolerance

        results.append(AuditResult(
            metric=f"custom_{label}", period_label=f"{start}..{end}", status=status,
            api_value=api_total, truth_value=truth_total, delta_pct=delta_pct,
            details={"n_months": len(all_p), "note": "top-N ranking only"},
        ))

        # Prior columns check
        periods_info = _compute_needed_periods(
            available_periods=available, resolved_period=RESOLVED,
            trend_window_months=24, origin_window_months=24, body_window_months=24,
            same_month_last_year_period="2025-03", prior_period="2026-02",
            custom_periods=[p for p in available if start <= p <= end],
        )
        prior_custom = periods_info.get("prior_custom", [])
        results.append(AuditResult(
            metric=f"custom_prior_{label}", period_label=f"{start}..{end}",
            status="PASS" if len(prior_custom) == len(all_p) else ("WARN" if len(prior_custom) > 0 else "FAIL"),
            api_value=len(prior_custom), truth_value=len(all_p),
            details={"prior_periods": prior_custom[:5], "expected": len(all_p)},
        ))


# ── main ─────────────────────────────────────────────────────────────


def main():
    all_results: list[AuditResult] = []

    audit_trend_bars(all_results)
    audit_fuel_dimension(all_results)
    audit_drivetrain(all_results)
    audit_registration(all_results)
    audit_yoy(all_results)
    audit_custom_period(all_results)

    passes = sum(1 for r in all_results if r.status == "PASS")
    warns = sum(1 for r in all_results if r.status == "WARN")
    fails = sum(1 for r in all_results if r.status == "FAIL")

    for r in all_results:
        print(r.summary)

    print(f"\n{'='*60}")
    print(f"FINAL: {passes} PASS  {warns} WARN  {fails} FAIL  (of {len(all_results)} checks)")

    if fails:
        print("\n❌ FAILURES:")
        for r in all_results:
            if r.status == "FAIL":
                extra = " | ".join(f"{k}={v}" for k, v in r.details.items() if not isinstance(v, (dict, list)))
                print(f"  {r.metric}: {extra}")
    if warns:
        print("\n⚠️  WARNINGS:")
        for r in all_results:
            if r.status == "WARN":
                extra = " | ".join(f"{k}={v}" for k, v in r.details.items() if not isinstance(v, (dict, list)))
                print(f"  {r.metric}: {extra}")

    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
