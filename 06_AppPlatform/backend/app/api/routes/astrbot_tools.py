from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from app.core.security import require_min_role
from app.services.astrbot_runtime_status_service import read_astrbot_runtime_status
from app.services.jato_agent_memory_service import (
    compare_agent_runs,
    delete_agent_run,
    get_agent_run,
    get_memory_stats,
    list_agent_runs,
    save_agent_run,
)
from app.services.jato_conversation_store import get_history as get_conversation_history
from app.services.jato_conversation_store import list_sessions as list_conversation_sessions
from app.services.jato_eval_service import (
    get_latest_codex_review_scoring_artifacts,
    get_business_validation_report,
    get_eval_summary,
    list_codex_review_notes,
    list_eval_results,
    list_eval_side_by_side_results,
    load_business_validation_questions,
    load_eval_questions,
    run_business_validation_all,
    run_business_validation_category,
    run_business_validation_judge_existing,
    run_business_validation_question,
    run_eval_category,
    run_eval_full,
    run_eval_question,
    run_eval_side_by_side_category,
    run_eval_side_by_side_question,
    update_eval_side_by_side_human_score,
)
from app.services.jato_agent_eval_v2_service import check_golden_question_v2
from app.services.jato_agent_eval_v2_service import list_golden_questions_v2
from app.services.jato_agent_eval_v2_service import run_eval_v2
from app.services.jato_agent_llm_judge_service import preflight_judge_provider
from app.services.jato_usage_tracker import get_eval_usage_summary
from app.services.jato_usage_tracker import get_agent_usage_summary
from app.services.jato_usage_tracker import get_followup_quality_summary
from app.services.jato_usage_tracker import track_followup_click
from fastapi.responses import StreamingResponse

from app.services.jato_agent_stream_service import stream_agent_response
from app.services.jato_channel_adapter_service import handle_mock_channel_message
from app.services.jato_channel_adapter_service import list_channel_audit_records
from app.services.jato_channel_adapter_service import read_channel_adapter_status
from app.services.jato_mcp_tools_service import call_jato_mcp_tool
from app.services.jato_mcp_tools_service import list_jato_mcp_tools


router = APIRouter(prefix="/astrbot/tools", tags=["astrbot-tools"])
memory_router = APIRouter(prefix="/astrbot/memory", tags=["astrbot-memory"])
agent_router = APIRouter(prefix="/astrbot/agent", tags=["astrbot-agent"])
usage_router = APIRouter(prefix="/astrbot/usage", tags=["astrbot-usage"])
channel_router = APIRouter(prefix="/astrbot/channels", tags=["astrbot-channels"])


class AstrBotToolCallRequest(BaseModel):
    name: str = Field(..., min_length=1)
    arguments: dict[str, Any] = Field(default_factory=dict)


class ChannelMessageRequest(BaseModel):
    channel: str = Field(default="mock")
    channelUserId: str = Field(default="mock-user", min_length=1)
    channelConversationId: str = Field(default="mock-conversation", min_length=1)
    jatoUserName: str = Field(default="channel_mock_user")
    text: str = Field(..., min_length=1)
    country: str = Field(default="Sweden", min_length=1)
    skillId: str = Field(default="")
    mode: str = Field(default="")
    includeSecondaryPaths: bool = True
    attachments: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


@router.get("/metadata")
def metadata(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return list_jato_mcp_tools()


@router.get("/status")
def status(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return read_astrbot_runtime_status()


@router.post("/call")
def call_tool(
    payload: AstrBotToolCallRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    try:
        return call_jato_mcp_tool(payload.name, payload.arguments)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ── Channel adapter routes ─────────────────────────────────────


@channel_router.get("/status")
def channel_status(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return read_channel_adapter_status()


@channel_router.post("/mock/message")
def mock_channel_message(
    payload: ChannelMessageRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    try:
        data = payload.model_dump()
    except AttributeError:
        data = payload.dict()
    data["channel"] = "mock"
    try:
        return handle_mock_channel_message(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@channel_router.get("/audit")
def channel_audit(
    limit: int = Query(default=20, ge=1, le=100),
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return list_channel_audit_records(limit=limit)


# ── Memory routes ──────────────────────────────────────────────


class AgentRunRecord(BaseModel):
    profile_id: str = Field(..., min_length=1)
    skill_id: str = Field(..., min_length=1)
    skill_name: str = Field(..., min_length=1)
    country: str = Field(..., min_length=1)
    mode: str = Field(default="auto")
    question: str = Field(..., min_length=1)
    selected_tool: str = Field(..., min_length=1)
    route_reason: str = Field(default="")
    evidence_source: str = Field(default="jato")
    evidence_count: int = Field(default=0, ge=0)
    display_cards: list[dict[str, str]] = Field(default_factory=list)
    result_summary: str = Field(default="")
    limitations: list[str] = Field(default_factory=list)
    truncated: bool = False
    primary_result_tool: str | None = None


class CompareRequest(BaseModel):
    run_ids: list[str] = Field(..., min_length=2, max_length=5)


@memory_router.get("/runs")
def list_runs(
    skill_id: str | None = None,
    country: str | None = None,
    mode: str | None = None,
    selected_tool: str | None = None,
    limit: int = 20,
    offset: int = 0,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return list_agent_runs(
        skill_id=skill_id,
        country=country,
        mode=mode,
        selected_tool=selected_tool,
        limit=limit,
        offset=offset,
    )


@memory_router.get("/runs/{run_id}")
def get_run(
    run_id: str,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    record = get_agent_run(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return record


@memory_router.post("/runs")
def save_run(
    payload: AgentRunRecord,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return save_agent_run(
        profile_id=payload.profile_id,
        skill_id=payload.skill_id,
        skill_name=payload.skill_name,
        country=payload.country,
        mode=payload.mode,
        question=payload.question,
        selected_tool=payload.selected_tool,
        route_reason=payload.route_reason,
        evidence_source=payload.evidence_source,
        evidence_count=payload.evidence_count,
        display_cards=payload.display_cards,
        result_summary=payload.result_summary,
        limitations=payload.limitations,
        truncated=payload.truncated,
        primary_result_tool=payload.primary_result_tool,
    )


@memory_router.delete("/runs/{run_id}")
def delete_run(
    run_id: str,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    deleted = delete_agent_run(run_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return {"runId": run_id, "deleted": True}


@memory_router.get("/stats")
def memory_stats(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return get_memory_stats()


@memory_router.post("/compare")
def compare_runs(
    payload: CompareRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return compare_agent_runs(payload.run_ids)


# ── Agent usage routes ─────────────────────────────────────────


@usage_router.get("/summary")
def agent_usage_summary(
    limit: int = 20,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return get_agent_usage_summary(limit=limit)


# ── Phase 7: Eval routes ──

eval_router = APIRouter(prefix="/astrbot/eval", tags=["astrbot-eval"])


class CamelRequestModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class EvalRunRequest(CamelRequestModel):
    question_id: str = Field(..., alias="questionId", min_length=1)


class EvalCategoryRunRequest(BaseModel):
    category: str = Field(..., min_length=1)
    limit: int = Field(default=10, ge=1, le=20)


class EvalFullRunRequest(CamelRequestModel):
    questions_per_category: int = Field(
        default=5,
        alias="questionsPerCategory",
        ge=1,
        le=20,
    )


class EvalSideBySideQuestionRequest(CamelRequestModel):
    question_id: str = Field(..., alias="questionId", min_length=1)


class EvalSideBySideCategoryRequest(BaseModel):
    category: str = Field(..., min_length=1)
    limit: int = Field(default=5, ge=1, le=20)


class EvalSideBySideHumanScoreRequest(BaseModel):
    status: str = Field(default="scored", min_length=1)
    source: str | None = Field(default=None)
    judgeProvider: dict[str, Any] | None = None
    winner: str = Field(default="")
    notes: str = Field(default="", max_length=4000)
    dimensions: list[str] | None = None
    astrbotTotal: int | float | None = None
    countryCopilotTotal: int | float | None = None
    copilotTotal: int | float | None = None
    astrbotScores: dict[str, int] | None = None
    countryCopilotScores: dict[str, int] | None = None
    copilotScores: dict[str, int] | None = None
    failureTags: list[str] | None = None


class EvalV2CheckRequest(CamelRequestModel):
    question_id: str = Field(..., alias="questionId", min_length=1)


class EvalV2RunRequest(BaseModel):
    limit: int | None = Field(default=None, ge=1, le=100)


class BusinessValidationQuestionRequest(CamelRequestModel):
    question_id: str = Field(..., alias="questionId", min_length=1)


class BusinessValidationCategoryRequest(BaseModel):
    category: str = Field(..., min_length=1)
    limit: int = Field(default=5, ge=1, le=10)


class BusinessValidationAllRequest(BaseModel):
    limit: int = Field(default=30, ge=1, le=30)


class BusinessValidationJudgeExistingRequest(CamelRequestModel):
    category: str | None = None
    limit: int = Field(default=30, ge=1, le=30)
    latest_per_question: bool = Field(default=True, alias="latestPerQuestion")
    score_ready_only: bool = Field(default=False, alias="scoreReadyOnly")


@eval_router.get("/questions")
def get_questions(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return load_eval_questions()


@eval_router.get("/summary")
def eval_summary(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return get_eval_summary()


@eval_router.get("/results")
def eval_results(
    category: str | None = None,
    limit: int = 30,
    offset: int = 0,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return list_eval_results(category=category, limit=limit, offset=offset)


@eval_router.get("/side-by-side/results")
def eval_side_by_side_results(
    category: str | None = None,
    limit: int = 30,
    offset: int = 0,
    latest_per_question: bool = Query(default=False, alias="latestPerQuestion"),
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return list_eval_side_by_side_results(
        category=category,
        limit=limit,
        offset=offset,
        latest_per_question=latest_per_question,
    )


@eval_router.get("/business/questions")
def eval_business_questions(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return load_business_validation_questions()


@eval_router.get("/business/report")
def eval_business_report(
    category: str | None = None,
    limit: int = 100,
    latest_per_question: bool = Query(default=True, alias="latestPerQuestion"),
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return get_business_validation_report(
        category=category,
        limit=limit,
        latest_per_question=latest_per_question,
    )


@eval_router.get("/judge/preflight")
def eval_judge_preflight(
    live_check: bool = Query(default=True, alias="liveCheck"),
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return preflight_judge_provider(live_check=live_check)


@eval_router.get("/codex-review/notes")
def eval_codex_review_notes(
    limit: int = Query(default=100, ge=1, le=100),
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return list_codex_review_notes(limit=limit)


@eval_router.get("/codex-review/scoring-artifacts/latest")
def eval_latest_codex_review_scoring_artifacts(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return get_latest_codex_review_scoring_artifacts()


@eval_router.post("/run/question")
def eval_run_question(
    payload: EvalRunRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    try:
        return run_eval_question(payload.question_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@eval_router.post("/run/category")
def eval_run_category(
    payload: EvalCategoryRunRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return run_eval_category(payload.category, limit=payload.limit)


@eval_router.post("/run/full")
def eval_run_full(
    payload: EvalFullRunRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return run_eval_full(questions_per_category=payload.questions_per_category)


@eval_router.post("/side-by-side/run/question")
def eval_run_side_by_side_question(
    payload: EvalSideBySideQuestionRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    try:
        return run_eval_side_by_side_question(payload.question_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@eval_router.post("/side-by-side/run/category")
def eval_run_side_by_side_category(
    payload: EvalSideBySideCategoryRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return run_eval_side_by_side_category(payload.category, limit=payload.limit)


@eval_router.post("/business/run/question")
def eval_run_business_question(
    payload: BusinessValidationQuestionRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    try:
        return run_business_validation_question(payload.question_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@eval_router.post("/business/run/category")
def eval_run_business_category(
    payload: BusinessValidationCategoryRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return run_business_validation_category(payload.category, limit=payload.limit)


@eval_router.post("/business/run/all")
def eval_run_business_all(
    payload: BusinessValidationAllRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return run_business_validation_all(limit=payload.limit)


@eval_router.post("/business/judge-existing")
def eval_judge_existing_business_records(
    payload: BusinessValidationJudgeExistingRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return run_business_validation_judge_existing(
        category=payload.category,
        limit=payload.limit,
        latest_per_question=payload.latest_per_question,
        score_ready_only=payload.score_ready_only,
    )


@eval_router.patch("/side-by-side/results/{comparison_id}/human-score")
def eval_update_side_by_side_human_score(
    comparison_id: str,
    payload: EvalSideBySideHumanScoreRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    try:
        return update_eval_side_by_side_human_score(comparison_id, payload.model_dump(exclude_none=True))
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc


@eval_router.get("/usage")
def eval_usage(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return get_eval_usage_summary()


@eval_router.get("/v2/golden-questions")
def eval_v2_golden_questions(
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return list_golden_questions_v2()


@eval_router.post("/v2/check")
def eval_v2_check(
    payload: EvalV2CheckRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    try:
        return check_golden_question_v2(payload.question_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@eval_router.post("/v2/run")
def eval_v2_run(
    payload: EvalV2RunRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return run_eval_v2(limit=payload.limit)


# ── SSE Streaming Agent endpoint ──


class AgentStreamRequest(BaseModel):
    country: str = Field(..., min_length=1)
    question: str = Field(..., min_length=1)
    max_rounds: int = Field(default=3, ge=1, le=5)
    session_id: str = Field(default="")
    skill_id: str = Field(default="")
    mode: str = Field(default="auto")
    research_mode: str = Field(default="standard")
    source_followup: dict[str, Any] = Field(default_factory=dict)


class AgentFollowUpClickRequest(BaseModel):
    session_id: str = Field(default="")
    country: str = Field(default="")
    source_question: str = Field(default="")
    follow_up: dict[str, Any] = Field(default_factory=dict)


@agent_router.get("/sessions")
def agent_sessions(
    limit: int = Query(default=20, ge=1, le=50),
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return {"items": list_conversation_sessions(limit=limit)}


@agent_router.get("/sessions/{session_id}")
def agent_session_history(
    session_id: str,
    limit: int = Query(default=20, ge=1, le=50),
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return get_conversation_history(session_id, limit=limit)


@agent_router.post("/followups/click")
def agent_followup_click(
    payload: AgentFollowUpClickRequest,
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    try:
        record = track_followup_click(
            session_id=payload.session_id,
            country=payload.country,
            source_question=payload.source_question,
            follow_up=payload.follow_up,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "eventId": record.get("eventId")}


@agent_router.get("/followups/metrics")
def agent_followup_metrics(
    limit: int = Query(default=500, ge=1, le=5000),
    _=Depends(require_min_role("viewer")),
) -> dict[str, Any]:
    return get_followup_quality_summary(limit=limit)


@agent_router.post("/stream")
async def agent_stream(
    payload: AgentStreamRequest,
    _=Depends(require_min_role("viewer")),
):
    return StreamingResponse(
        stream_agent_response(
            country=payload.country,
            question=payload.question,
            max_rounds=payload.max_rounds,
            session_id=payload.session_id,
            requested_mode=payload.mode,
            research_mode=payload.research_mode,
            skill_id=payload.skill_id,
            source_followup=payload.source_followup or None,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
