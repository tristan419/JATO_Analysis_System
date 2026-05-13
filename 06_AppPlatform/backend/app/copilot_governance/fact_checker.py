"""Fact Checker — verifies numerical claims in LLM answers against snapshot data."""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, Field


class FactIssue(BaseModel):
    claim: str
    severity: str = "warning"
    detail: str = ""


class FactCheckReport(BaseModel):
    status: str = "pass"
    issues: list[FactIssue] = []
    verified_claims: int = 0
    unverifiable_claims: int = 0


# ── Number extraction ─────────────────────────────────────────────

_NUMBER_PATTERNS = [
    (r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:辆|台|units?)", "销量"),
    (r"(\d+(?:\.\d+)?)\s*[%％]", "百分比"),
    (r"(?:EUR|SEK|NOK|DKK|CZK|HUF|€|kr)\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)", "价格"),
    (r"(?:排名|第)\s*(\d+)\s*(?:名|位)", "排名"),
]


def _extract_numbers(text: str) -> list[tuple[str, str, float]]:
    """Extract numerical claims from answer text. Returns (raw_text, claim_type, value)."""
    results: list[tuple[str, str, float]] = []
    for pattern, claim_type in _NUMBER_PATTERNS:
        for match in re.finditer(pattern, text):
            raw = match.group(0)
            num_str = match.group(1).replace(",", "")
            try:
                value = float(num_str)
                if value > 0:
                    results.append((raw, claim_type, value))
            except ValueError:
                continue
    return results


# ── Snapshot number extraction ─────────────────────────────────────

def _extract_snapshot_numbers(snapshot: dict) -> dict[str, set[float]]:
    """Build a lookup of all numbers in the snapshot data."""
    nums: dict[str, set[float]] = {"销量": set(), "百分比": set(), "排名": set()}

    for item in snapshot.get("topBrands", []) or []:
        if isinstance(item, dict):
            v = item.get("value", 0)
            if v:
                nums["销量"].add(float(v))
            s = item.get("share", 0)
            if s:
                nums["百分比"].add(float(s))

    for item in snapshot.get("powertrainMix", []) or []:
        if isinstance(item, dict):
            v = item.get("value", 0)
            if v:
                nums["销量"].add(float(v))

    for item in snapshot.get("topModels", []) or []:
        if isinstance(item, dict):
            v = item.get("value", 0)
            if v:
                nums["销量"].add(float(v))

    cross_tabs = snapshot.get("crossTabs", {})
    if isinstance(cross_tabs, dict):
        for ct_data in cross_tabs.values():
            if isinstance(ct_data, list):
                for row in ct_data:
                    if isinstance(row, dict):
                        for key, val in row.items():
                            if key.endswith("_pct") and isinstance(val, (int, float)):
                                nums["百分比"].add(float(val))
                            if key == "_total" and isinstance(val, (int, float)):
                                nums["销量"].add(float(val))

    kpis = snapshot.get("kpis", {})
    if isinstance(kpis, dict):
        for v in kpis.values():
            if isinstance(v, (int, float)) and v > 0:
                nums["销量"].add(float(v))

    return nums


# ── Verification ──────────────────────────────────────────────────

def _fuzzy_match(value: float, candidates: set[float], tolerance: float = 0.02) -> bool:
    """Check if value approximately matches any candidate."""
    if not candidates:
        return False
    if value in candidates:
        return True
    for c in candidates:
        if c == 0:
            continue
        if abs(value - c) / c <= tolerance:
            return True
    return False


def check_answer_facts(answer: str, snapshot: dict) -> FactCheckReport:
    claims = _extract_numbers(answer)
    if not claims:
        return FactCheckReport(status="pass", verified_claims=0)

    snapshot_nums = _extract_snapshot_numbers(snapshot)
    issues: list[FactIssue] = []
    verified = 0
    unverifiable = 0

    for raw, claim_type, value in claims:
        candidates = snapshot_nums.get(claim_type, set())
        if _fuzzy_match(value, candidates):
            verified += 1
        else:
            unverifiable += 1
            issues.append(FactIssue(
                claim=f"'{raw}'",
                severity="warning",
                detail=f"{claim_type} {value} 在 JATO 快照数据中未找到匹配值",
            ))

    status = "pass"
    if unverifiable > verified and verified == 0:
        status = "fail"
    elif unverifiable > 0:
        status = "warning"

    return FactCheckReport(
        status=status,
        issues=issues[:5],
        verified_claims=verified,
        unverifiable_claims=unverifiable,
    )
