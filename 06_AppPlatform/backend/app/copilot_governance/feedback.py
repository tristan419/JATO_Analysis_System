"""Feedback collection — user ratings for copilot answers."""

from __future__ import annotations

import time
from typing import Literal

from pydantic import BaseModel, Field


class CopilotFeedback(BaseModel):
    audit_id: str
    rating: Literal["up", "down", "issue"] = "up"
    issue_type: str | None = None
    comment: str | None = None
    created_at: str = Field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))


_FEEDBACK_LOG: list[CopilotFeedback] = []


def submit_feedback(
    audit_id: str,
    rating: str = "up",
    issue_type: str | None = None,
    comment: str | None = None,
) -> CopilotFeedback:
    feedback = CopilotFeedback(
        audit_id=audit_id,
        rating=rating,
        issue_type=issue_type,
        comment=comment,
    )
    _FEEDBACK_LOG.append(feedback)
    return feedback


def recent_feedback(limit: int = 20) -> list[CopilotFeedback]:
    return list(reversed(_FEEDBACK_LOG[-limit:]))
