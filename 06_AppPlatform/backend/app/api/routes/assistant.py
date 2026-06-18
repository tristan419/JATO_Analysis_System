from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

from app.api.cache_headers import set_strong_json_cache_headers
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
    response: Response,
    _=Depends(require_min_role("viewer")),
) -> dict:
    payload = get_country_chat_metadata()
    set_strong_json_cache_headers(
        response,
        payload,
        namespace="country-chat-metadata",
    )
    return payload


@router.post("/chat/stream")
async def chat_stream(
    payload: CountryChatRequest,
    _=Depends(require_min_role("viewer")),
):
    import asyncio
    import json as _json
    from concurrent.futures import ThreadPoolExecutor

    # Build snapshot first (blocking) then stream DeepSeek tokens
    country = payload.country.strip()
    question = payload.question.strip()
    history = [turn.model_dump() for turn in payload.history]

    # Phase 1: build snapshot synchronously (this is the 15s wait)
    from app.services.country_chat_service import (
        build_country_snapshot,
        _enrich_snapshot_for_intents,
        extract_user_params,
        _merge_followup_user_params,
        infer_country_chat_intents,
        _build_country_chat_route,
        _select_context_for_intents,
        _DEEPSEEK_STABLE_SYSTEM_PROMPT,
        _build_deepseek_history_messages,
        _json_for_model_prompt,
        CONTEXT_CHAR_BUDGET,
        _deepseek_api_key,
        _stream_deepseek_chat_completion,
        _parse_report_suggestions,
        DEEPSEEK_CHAT_TIMEOUT_SECONDS,
        country_chat_models,
    )

    loop = asyncio.get_running_loop()

    def _build_snapshot() -> dict:
        api_key = _deepseek_api_key()
        if not api_key:
            raise ValueError("DeepSeek API key 未配置")
        user_params = _merge_followup_user_params(
            question=question,
            user_params=extract_user_params(question),
            history=history,
        )
        raw_intents = infer_country_chat_intents(question)
        route_plan = _build_country_chat_route(question, user_params, raw_intents)
        focused_intents = route_plan["focusedIntents"]
        intent_route = route_plan["intentRoute"]

        snapshot = build_country_snapshot(country, news_payload_override=None)
        try:
            snapshot = _enrich_snapshot_for_intents(snapshot, focused_intents)
        except Exception:
            pass

        context = _select_context_for_intents(snapshot, focused_intents)
        cross_tabs = snapshot.get("crossTabs", {})
        if not isinstance(cross_tabs, dict):
            cross_tabs = {}

        model = country_chat_models.get_default_deepseek_chat_model()

        messages = [
            {"role": "system", "content": _DEEPSEEK_STABLE_SYSTEM_PROMPT},
            {"role": "system", "content": "crossSectionData 是交叉维度数据，dashboardContext 是单维度。优先用交叉数据做因果分析。"},
            {"role": "user", "content": "证据包(JSON):\n" + _json_for_model_prompt({
                "country": country, "question": question, "route": intent_route,
                "intents": focused_intents, "parsedParams": user_params,
                "dashboardContext": context,
                "crossSectionData": {
                    "driveByFuel": cross_tabs.get("driveByFuel", []),
                    "registrationByFuel": cross_tabs.get("registrationByFuel", []),
                    "segmentByFuel": cross_tabs.get("segmentByFuel", []),
                    "availableDimensions": cross_tabs.get("availableDimensions", []),
                },
            }, max_chars=CONTEXT_CHAR_BUDGET)},
            *_build_deepseek_history_messages(history),
            {"role": "user", "content": f"当前用户问题: {question}\n请按 #核心发现 #数据证据 #因果分析 #市场背景 #趋势展望 #进一步分析建议 的6节结构生成分析报告。"},
        ]

        return {
            "api_key": api_key, "model": model, "messages": messages,
            "intent_route": intent_route, "focused_intents": focused_intents,
            "snapshot": snapshot,
        }

    try:
        prep = await loop.run_in_executor(ThreadPoolExecutor(max_workers=1), _build_snapshot)
    except ValueError as exc:
        def _err():
            yield f"event: error\ndata: {_json.dumps({'message': str(exc)})}\n\n"
        return StreamingResponse(_err(), media_type="text/event-stream")

    queue: asyncio.Queue[str | None] = asyncio.Queue()

    def _stream_tokens() -> None:
        full_text = ""
        try:
            loop.call_soon_threadsafe(lambda: queue.put_nowait(
                f"event: meta\ndata: {_json.dumps({'country': country, 'intentRoute': prep['intent_route'], 'focusedIntents': prep['focused_intents'], 'provider': 'deepseek', 'model': prep['model']})}\n\n"
            ))
            for token in _stream_deepseek_chat_completion(
                api_key=prep["api_key"], model=prep["model"],
                messages=prep["messages"], temperature=0.25,
                timeout_seconds=DEEPSEEK_CHAT_TIMEOUT_SECONDS,
            ):
                full_text += token
                loop.call_soon_threadsafe(lambda t=token: queue.put_nowait(
                    f"event: token\ndata: {_json.dumps({'text': t})}\n\n"
                ))
            suggestions = _parse_report_suggestions(full_text)
            loop.call_soon_threadsafe(lambda: queue.put_nowait(
                f"event: done\ndata: {_json.dumps({'suggestedPrompts': suggestions})}\n\n"
            ))
        except Exception as exc:
            loop.call_soon_threadsafe(lambda: queue.put_nowait(
                f"event: error\ndata: {_json.dumps({'message': str(exc)})}\n\n"
            ))
        finally:
            loop.call_soon_threadsafe(lambda: queue.put_nowait(None))

    ThreadPoolExecutor(max_workers=1).submit(_stream_tokens)

    async def _drain():
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            yield chunk

    return StreamingResponse(
        _drain(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
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
