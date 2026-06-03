import os
import threading
import time
from collections.abc import Callable
from typing import TypeVar

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.schemas import (
    AdvancedAnalysisCellAttributionRequest,
    AdvancedAnalysisCompetitorSetRequest,
    AdvancedAnalysisDrilldownRequest,
    AdvancedAnalysisKpiRequest,
    AdvancedAnalysisNestedShiftShareRequest,
    AdvancedAnalysisSeasonalRequest,
    AdvancedAnalysisShiftShareRequest,
    AdvancedAnalysisTransferMartRequest,
    AdvancedAnalysisTransferMatrixRequest,
)
from app.core.security import optional_viewer
from app.services.advanced_analysis_service import (
    clear_advanced_analysis_cache,
    compute_cell_attribution,
    compute_competitor_set,
    compute_drilldown,
    compute_kpi_table,
    compute_transfer_mart,
    compute_nested_shift_share,
    compute_probabilistic_transfer_matrix,
    compute_seasonal_decomposition,
    compute_shift_share_decomposition,
    list_profile_filter_options,
    warmup_cache,
)

router = APIRouter(prefix="/advanced-analysis", tags=["advanced-analysis"])

_Result = TypeVar("_Result")
_MAX_CONCURRENT = max(1, int(os.getenv("APP_ADVANCED_ANALYSIS_MAX_CONCURRENT", "1")))
_ACQUIRE_TIMEOUT_SECONDS = max(1.0, float(os.getenv("APP_ADVANCED_ANALYSIS_ACQUIRE_TIMEOUT_SECONDS", "20")))
_ADVANCED_ANALYSIS_SEMAPHORE = threading.BoundedSemaphore(_MAX_CONCURRENT)


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _run_guarded(operation: str, compute: Callable[[], _Result]) -> _Result:
    acquired = _ADVANCED_ANALYSIS_SEMAPHORE.acquire(timeout=_ACQUIRE_TIMEOUT_SECONDS)
    if not acquired:
        raise HTTPException(
            status_code=429,
            detail="Advanced Analysis is busy. Please retry shortly.",
        )
    start = time.perf_counter()
    try:
        return compute()
    finally:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        if elapsed_ms > 10000:
            print(f"[advanced-analysis] {operation} completed in {elapsed_ms}ms")
        _ADVANCED_ANALYSIS_SEMAPHORE.release()


if _bool_env("APP_ADVANCED_ANALYSIS_WARMUP_ENABLED", False):
    threading.Thread(target=warmup_cache, daemon=True).start()


@router.post("/kpi")
def advanced_analysis_kpi(payload: AdvancedAnalysisKpiRequest, _=Depends(optional_viewer)) -> dict:
    return _run_guarded("kpi", lambda: compute_kpi_table(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        group_by=payload.group_by,
        top_n=payload.top_n,
    ))


@router.post("/shift-share")
def advanced_analysis_shift_share(payload: AdvancedAnalysisShiftShareRequest, _=Depends(optional_viewer)) -> dict:
    return _run_guarded("shift-share", lambda: compute_shift_share_decomposition(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        base_period=payload.base_period,
        cell_dims=payload.cell_dims,
    ))


@router.post("/seasonal")
def advanced_analysis_seasonal(payload: AdvancedAnalysisSeasonalRequest, _=Depends(optional_viewer)) -> dict:
    return _run_guarded("seasonal", lambda: compute_seasonal_decomposition(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        model_filter=payload.model_filter,
        segment_filter=payload.segment_filter,
    ))


@router.post("/cell-attribution")
def advanced_analysis_cell_attribution(payload: AdvancedAnalysisCellAttributionRequest, _=Depends(optional_viewer)) -> dict:
    return _run_guarded("cell-attribution", lambda: compute_cell_attribution(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        cell_dims=payload.cell_dims,
        top_n_cells=payload.top_n_cells,
    ))


@router.post("/transfer-matrix")
def advanced_analysis_transfer_matrix(payload: AdvancedAnalysisTransferMatrixRequest, _=Depends(optional_viewer)) -> dict:
    return _run_guarded("transfer-matrix", lambda: compute_probabilistic_transfer_matrix(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        cell_dims=payload.cell_dims,
        top_n_models=payload.top_n_models,
    ))


@router.post("/nested-shift-share")
def advanced_analysis_nested_shift_share(payload: AdvancedAnalysisNestedShiftShareRequest, _=Depends(optional_viewer)) -> dict:
    return _run_guarded("nested-shift-share", lambda: compute_nested_shift_share(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        base_period=payload.base_period,
        hierarchy=payload.hierarchy,
    ))


@router.post("/drilldown")
def advanced_analysis_drilldown(payload: AdvancedAnalysisDrilldownRequest, _=Depends(optional_viewer)) -> dict:
    return _run_guarded("drilldown", lambda: compute_drilldown(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        scope_filters=payload.scope_filters,
        base_period=payload.base_period,
        top_n=payload.top_n,
    ))


@router.post("/transfer-mart")
def advanced_analysis_transfer_mart(payload: AdvancedAnalysisTransferMartRequest, _=Depends(optional_viewer)) -> dict:
    return _run_guarded("transfer-mart", lambda: compute_transfer_mart(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        scope_filters=payload.scope_filters,
        base_period=payload.base_period,
        sales_mode=payload.sales_mode,
        top_n=payload.top_n,
    ))


@router.post("/competitor-set")
def advanced_analysis_competitor_set(payload: AdvancedAnalysisCompetitorSetRequest, _=Depends(optional_viewer)) -> dict:
    return _run_guarded("competitor-set", lambda: compute_competitor_set(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        scope_filters=payload.scope_filters,
        base_period=payload.base_period,
        sales_mode=payload.sales_mode,
        target_model=payload.target_model,
        profile_specs=payload.profile_specs,
        top_n=payload.top_n,
    ))


@router.get("/segments")
def list_available_segments(
    country: str = Query(default="瑞典"),
) -> dict:
    """Return distinct segments available in the parquet data."""
    from app.services.advanced_analysis_service import build_fact_sales_monthly

    def _compute() -> dict:
        fact = build_fact_sales_monthly(country=country)
        segments = sorted(fact["segment"].dropna().unique().tolist()) if "segment" in fact.columns else []
        return {"country": country, "segments": segments}

    return _run_guarded("segments", _compute)


@router.get("/profile-options")
def advanced_analysis_profile_options(
    country: str = Query(default="瑞典"),
    _=Depends(optional_viewer),
) -> dict:
    return _run_guarded("profile-options", lambda: list_profile_filter_options(country=country))


@router.delete("/cache")
def advanced_analysis_clear_cache(_=Depends(optional_viewer)) -> dict:
    return clear_advanced_analysis_cache()
