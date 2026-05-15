"""Parse MSRP dryrun logs and reports for progress dashboards."""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_DIR = Path(os.environ.get("REPO_DIR", "/opt/JATO_Analysis_System-main"))
LOG_DIR = REPO_DIR / "03_Scripts" / "logs"
ARTIFACT_DIR = REPO_DIR / "03_Scripts" / "diagnostics" / "artifacts"
LOCK_FILE = Path("/tmp/jato-msrp-low-concurrency.lock")
DRYRUN_LOG_PATTERN = re.compile(r"msrp-dryrun-(\d{8})-(\d{6})\.log")

_COUNTRY_NAMES: dict[str, str] = {
    "se": "Sweden", "fi": "Finland", "no": "Norway", "dk": "Denmark",
    "hu": "Hungary", "hr": "Croatia", "at": "Austria", "cz": "Czech Republic",
    "de": "Germany", "fr": "France", "it": "Italy", "pl": "Poland",
    "ch": "Switzerland",
}


def _country_label(cc: str) -> str:
    return _COUNTRY_NAMES.get(cc.lower(), cc.upper())


def _parse_log_timestamp(filename: str) -> datetime | None:
    m = DRYRUN_LOG_PATTERN.search(filename)
    if not m:
        return None
    return datetime.strptime(f"{m.group(1)}{m.group(2)}", "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def _is_running() -> bool:
    return LOCK_FILE.exists()


def _list_log_files() -> list[Path]:
    if not LOG_DIR.exists():
        return []
    files = sorted(LOG_DIR.glob("msrp-dryrun-*.log"), reverse=True)
    return files


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
            emoji = m.group(3)
            sources.append({
                "index": int(m.group(1)),
                "totalInCountry": int(m.group(2)),
                "sourceCode": m.group(4),
                "status": "pass" if "✅" in emoji else ("empty" if "⬚" in emoji else "fail"),
                "valid": int(m.group(5)),
                "extracted": int(m.group(6)),
                "rejected": int(m.group(7)),
                "elapsedSeconds": float(m.group(8)),
            })

        summary_m = re.search(
            r"Results:\s*(\d+)/(\d+)\s+PASS,\s*(\d+)\s+empty,\s*(\d+)\s+rejected-all,\s*(\d+)\s+errors",
            text,
        )
        pass_count = int(summary_m.group(1)) if summary_m else 0
        total = int(summary_m.group(2)) if summary_m else len(sources)
        empty_count = int(summary_m.group(3)) if summary_m else 0
        fail_count = int(summary_m.group(5)) if summary_m else 0

        countries.append({
            "countryCode": cc,
            "countryLabel": _country_label(cc),
            "total": total,
            "pass": pass_count,
            "empty": empty_count,
            "fail": fail_count,
            "completed": bool(summary_m),
            "passRate": round(pass_count / max(total, 1) * 100, 1),
            "sources": sources,
        })
        total_sources += total
        total_pass += pass_count
        total_empty += empty_count
        total_fail += fail_count

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
            emoji = m.group(3)  # ✅ / ⬚ / ❌
            source_code = m.group(4)
            sources.append({
                "index": int(m.group(1)),
                "totalInCountry": int(m.group(2)),
                "sourceCode": source_code,
                "status": "pass" if "✅" in emoji else ("empty" if "⬚" in emoji else "fail"),
                "valid": int(m.group(5)),
                "extracted": int(m.group(6)),
                "rejected": int(m.group(7)),
                "elapsedSeconds": float(m.group(8)),
            })

        # Parse country summary from Results line
        summary_m = re.search(
            r"Results:\s*(\d+)/(\d+)\s+PASS,\s*(\d+)\s+empty,\s*(\d+)\s+rejected-all,\s*(\d+)\s+errors",
            section,
        )
        pass_count = int(summary_m.group(1)) if summary_m else 0
        total = int(summary_m.group(2)) if summary_m else len(sources)
        empty_count = int(summary_m.group(3)) if summary_m else 0
        fail_count = int(summary_m.group(5)) if summary_m else 0

        country_entry: dict[str, Any] = {
            "countryCode": cc,
            "countryLabel": _country_label(cc),
            "total": total,
            "pass": pass_count,
            "empty": empty_count,
            "fail": fail_count,
            "completed": bool(summary_m),
            "passRate": round(pass_count / max(total, 1) * 100, 1),
            "sources": sources,
        }
        result["countries"].append(country_entry)
        result["totalSources"] += total
        result["totalPass"] += pass_count
        result["totalEmpty"] += empty_count
        result["totalFail"] += fail_count

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

            runs.append({
                "batch": data.get("batch", ""),
                "countries": data.get("countries", []),
                "total": data.get("total", 0),
                "pass": data.get("pass", 0),
                "empty": data.get("empty", 0),
                "fail": data.get("fail", 0),
                "errors": data.get("errors", 0),
                "passRate": round(data.get("pass", 0) / max(data.get("total", 1), 1) * 100, 1),
                "timestamp": ts if isinstance(ts, str) else ts.isoformat() if hasattr(ts, 'isoformat') else str(ts),
                "file": report_file.name,
                "countriesDetail": countries_detail,
            })
        except (json.JSONDecodeError, KeyError):
            continue

    return runs[:30]


def get_dryrun_dashboard() -> dict[str, Any]:
    """Return combined dashboard data: live progress + history."""
    current = _parse_current_progress()
    history = _list_historical_runs()

    # Also list available log files for drill-down
    log_files = []
    for f in _list_log_files()[:10]:
        ts = _parse_log_timestamp(f.name)
        log_files.append({
            "name": f.name,
            "size": f.stat().st_size if f.exists() else 0,
            "timestamp": ts.isoformat() if ts else None,
        })

    return {
        "current": current,
        "history": history,
        "logFiles": log_files,
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
