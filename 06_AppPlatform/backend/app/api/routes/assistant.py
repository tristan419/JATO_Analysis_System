from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.api.schemas import (
    CountryChatDeckRequest,
    CountryChatRequest,
    CountryNewsRefreshRequest,
)
from app.core.security import require_min_role
from app.services.country_chat_service import (
    answer_country_question,
    answer_country_question_stream,
    build_country_chart_deck,
    get_country_chat_metadata,
)
from app.services import news_digest_service


router = APIRouter(prefix="/assistant/country", tags=["assistant"])


@router.get("/metadata")
def metadata(
    _=Depends(require_min_role("viewer")),
) -> dict:
    return get_country_chat_metadata()


@router.post("/chat/stream")
async def chat_stream(
    payload: CountryChatRequest,
    _=Depends(require_min_role("viewer")),
):
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[str | None] = asyncio.Queue()

    def _run_blocking() -> None:
        try:
            for chunk in answer_country_question_stream(
                country=payload.country,
                question=payload.question,
                history=[turn.model_dump() for turn in payload.history],
                chat_model=payload.model,
            ):
                loop.call_soon_threadsafe(lambda c=chunk: queue.put_nowait(c))
        except Exception:
            pass
        finally:
            loop.call_soon_threadsafe(lambda: queue.put_nowait(None))

    ThreadPoolExecutor(max_workers=1).submit(_run_blocking)

    async def _drain():
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            yield chunk

    return StreamingResponse(
        _drain(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/chat")
def chat(
    payload: CountryChatRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    try:
        news_payload_override = None
        refresh_note = ""
        if payload.refresh_news:
            news_payload_override = news_digest_service.refresh_country_news(
                payload.country,
                persist=True,
            )
            refresh_note = "已在线抓取最新新闻快照并更新上下文。"

        response = answer_country_question(
            country=payload.country,
            question=payload.question,
            history=[turn.model_dump() for turn in payload.history],
            news_payload_override=news_payload_override,
            chat_model=payload.model,
        )
        if refresh_note:
            provider_reason = str(response.get("providerReason") or "").strip()
            response["providerReason"] = "；".join(
                part for part in [refresh_note, provider_reason] if part
            )
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/chart-deck")
def chart_deck(
    payload: CountryChatDeckRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    try:
        return build_country_chart_deck(
            country=payload.country,
            question=payload.question,
            intents=payload.intents,
            extracted_params=payload.extracted_params,
            selected_year=payload.selected_year,
            selected_model=payload.selected_model,
            model_top_n=payload.model_top_n,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/news/status")
def news_status(
    country: str = Query(..., min_length=1),
    _=Depends(require_min_role("viewer")),
) -> dict:
    assistant_meta = get_country_chat_metadata()
    news_status_payload = news_digest_service.get_country_news_ops_status(
        country,
    )
    return {
        **news_status_payload,
        "chatProvider": {
            "provider": assistant_meta.get("provider"),
            "available": assistant_meta.get("providerAvailable"),
            "reason": assistant_meta.get("providerReason"),
            "model": assistant_meta.get("defaultModel"),
        },
        "providerRoles": [
            {
                "capability": "news-fetch",
                "provider": "rss-atom",
                "mode": "scheduled-or-on-demand",
            },
            {
                "capability": "news-enrichment",
                "provider": (
                    "gemini"
                    if news_status_payload.get("geminiConfigured")
                    else "rss-fallback"
                ),
                "model": news_status_payload.get("geminiModel"),
            },
            {
                "capability": "chat-analysis",
                "provider": assistant_meta.get("provider"),
                "model": assistant_meta.get("defaultModel"),
            },
        ],
    }


@router.post("/news/refresh")
def refresh_news(
    payload: CountryNewsRefreshRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    try:
        refreshed_payload = news_digest_service.refresh_country_news(
            payload.country,
            limit=payload.limit,
            persist=payload.persist,
            enrich_with_gemini=payload.enrich_with_gemini,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "payload": refreshed_payload,
        "status": news_digest_service.get_country_news_ops_status(
            payload.country,
        ),
    }
