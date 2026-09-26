from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_MEMORY_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "hermes" / "agent_memory"
_MEMORY_FILE_NAME = "agent_runs.jsonl"
_MAX_RUNS = 500


def _memory_file_path() -> Path:
    _MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    return _MEMORY_DIR / _MEMORY_FILE_NAME


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_run_id() -> str:
    return f"run_{uuid.uuid4().hex[:12]}"


def _read_all_runs() -> list[dict[str, Any]]:
    path = _memory_file_path()
    if not path.exists():
        return []
    runs: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                runs.append(json.loads(stripped))
            except json.JSONDecodeError:
                continue
    return runs


def _write_all_runs(runs: list[dict[str, Any]]) -> None:
    runs_to_keep = runs[-_MAX_RUNS:]
    path = _memory_file_path()
    with open(path, "w", encoding="utf-8") as fh:
        for run in runs_to_keep:
            fh.write(json.dumps(run, ensure_ascii=False) + "\n")


def save_agent_run(
    *,
    profile_id: str,
    skill_id: str,
    skill_name: str,
    country: str,
    mode: str,
    question: str,
    selected_tool: str,
    route_reason: str,
    evidence_source: str,
    evidence_count: int,
    display_cards: list[dict[str, str]],
    result_summary: str,
    limitations: list[str] | None = None,
    truncated: bool = False,
    primary_result_tool: str | None = None,
) -> dict[str, Any]:
    run_record: dict[str, Any] = {
        "runId": _generate_run_id(),
        "createdAt": _now_iso(),
        "profileId": profile_id,
        "skillId": skill_id,
        "skillName": skill_name,
        "country": country,
        "mode": mode,
        "question": question,
        "selectedTool": selected_tool,
        "routeReason": route_reason,
        "evidenceSource": evidence_source,
        "evidenceCount": evidence_count,
        "displayCards": display_cards,
        "resultSummary": result_summary,
        "limitations": limitations or [],
        "truncated": truncated,
        "primaryResultTool": primary_result_tool or selected_tool,
    }
    runs = _read_all_runs()
    runs.append(run_record)
    _write_all_runs(runs)
    return dict(run_record)


def list_agent_runs(
    *,
    skill_id: str | None = None,
    country: str | None = None,
    mode: str | None = None,
    selected_tool: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    runs = _read_all_runs()
    # newest first
    runs.reverse()

    filtered: list[dict[str, Any]] = []
    for run in runs:
        if skill_id and run.get("skillId") != skill_id:
            continue
        if country and run.get("country", "").lower() != country.lower():
            continue
        if mode and run.get("mode") != mode:
            continue
        if selected_tool and run.get("selectedTool") != selected_tool:
            continue
        filtered.append(run)

    total = len(filtered)
    page = filtered[offset : offset + max(1, min(limit, 100))]
    return {
        "items": page,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def get_agent_run(run_id: str) -> dict[str, Any] | None:
    runs = _read_all_runs()
    for run in runs:
        if run.get("runId") == run_id:
            return dict(run)
    return None


def compare_agent_runs(run_ids: list[str]) -> dict[str, Any]:
    runs_map: dict[str, dict[str, Any]] = {}
    for run_id in run_ids:
        record = get_agent_run(run_id)
        if record:
            runs_map[run_id] = record

    comparison_fields = [
        "skillName",
        "country",
        "mode",
        "selectedTool",
        "evidenceSource",
        "evidenceCount",
        "resultSummary",
        "limitations",
    ]
    rows: list[dict[str, Any]] = []
    for field in comparison_fields:
        row: dict[str, Any] = {"field": field}
        for run_id in run_ids:
            record = runs_map.get(run_id)
            row[run_id] = record.get(field) if record else None
        rows.append(row)

    return {
        "runIds": run_ids,
        "found": list(runs_map.keys()),
        "missing": [rid for rid in run_ids if rid not in runs_map],
        "comparison": rows,
        "runs": [runs_map[rid] for rid in run_ids if rid in runs_map],
    }


def delete_agent_run(run_id: str) -> bool:
    runs = _read_all_runs()
    original_len = len(runs)
    filtered = [run for run in runs if run.get("runId") != run_id]
    if len(filtered) == original_len:
        return False
    _write_all_runs(filtered)
    return True


def get_memory_stats() -> dict[str, Any]:
    runs = _read_all_runs()
    total = len(runs)
    skills: dict[str, int] = {}
    countries: dict[str, int] = {}
    tools: dict[str, int] = {}
    for run in runs:
        sid = str(run.get("skillId") or "unknown")
        skills[sid] = skills.get(sid, 0) + 1
        c = str(run.get("country") or "unknown")
        countries[c] = countries.get(c, 0) + 1
        t = str(run.get("selectedTool") or "unknown")
        tools[t] = tools.get(t, 0) + 1
    return {
        "totalRuns": total,
        "maxRuns": _MAX_RUNS,
        "bySkill": skills,
        "byCountry": countries,
        "byTool": tools,
        "latestRunAt": runs[-1].get("createdAt") if runs else None,
    }
