"""Audit logging — records each copilot answer for traceability."""

from __future__ import annotations

import time
import uuid
from typing import Any

from pydantic import BaseModel, Field


class CopilotAuditRecord(BaseModel):
    audit_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    created_at: str = Field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    country: str | None = None
    question: str = ""
    intent: str = ""
    intent_route: str = ""
    provider: str = ""
    model: str | None = None
    answer_mode: str = ""
    latency_ms: int | None = None
    token_usage: dict[str, int] = Field(default_factory=dict)
    cost_rmb: float | None = None
    source_plan: dict[str, Any] | None = None
    evidence_pack_sources: list[str] = Field(default_factory=list)
    verification_status: str = ""
    status: str = "ok"
    error: str | None = None


# In-memory ring buffer (MVP, not persisted)
_AUDIT_LOG: list[CopilotAuditRecord] = []
_MAX_RECORDS = 200


def record_audit(record: CopilotAuditRecord) -> None:
    _AUDIT_LOG.append(record)
    if len(_AUDIT_LOG) > _MAX_RECORDS:
        _AUDIT_LOG.pop(0)


def recent_audits(limit: int = 20) -> list[CopilotAuditRecord]:
    return list(reversed(_AUDIT_LOG[-limit:]))


def build_audit_record(
    country: str = "",
    question: str = "",
    intent: str = "",
    intent_route: str = "",
    provider: str = "",
    model: str | None = None,
    answer_mode: str = "",
    elapsed_ms: int = 0,
    token_usage: dict[str, int] | None = None,
    cost_rmb: float | None = None,
    source_plan: dict[str, Any] | None = None,
    evidence_pack_sources: list[str] | None = None,
    verification_status: str = "",
    error: str | None = None,
) -> CopilotAuditRecord:
    record = CopilotAuditRecord(
        country=country,
        question=question,
        intent=intent,
        intent_route=intent_route,
        provider=provider,
        model=model,
        answer_mode=answer_mode,
        latency_ms=elapsed_ms,
        token_usage=token_usage or {},
        cost_rmb=cost_rmb,
        source_plan=source_plan,
        evidence_pack_sources=evidence_pack_sources or [],
        verification_status=verification_status,
        error=error,
    )
    record_audit(record)
    return record
