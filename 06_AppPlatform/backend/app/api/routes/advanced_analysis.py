from fastapi import APIRouter, Depends, Query

from app.api.schemas import (
    AdvancedAnalysisCellAttributionRequest,
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
    compute_drilldown,
    compute_kpi_table,
    compute_transfer_mart,
    compute_nested_shift_share,
    compute_probabilistic_transfer_matrix,
    compute_seasonal_decomposition,
    compute_shift_share_decomposition,
)

router = APIRouter(prefix="/advanced-analysis", tags=["advanced-analysis"])


@router.post("/kpi")
def advanced_analysis_kpi(payload: AdvancedAnalysisKpiRequest, _=Depends(optional_viewer)) -> dict:
    return compute_kpi_table(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        group_by=payload.group_by,
        top_n=payload.top_n,
    )


@router.post("/shift-share")
def advanced_analysis_shift_share(payload: AdvancedAnalysisShiftShareRequest, _=Depends(optional_viewer)) -> dict:
    return compute_shift_share_decomposition(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        base_period=payload.base_period,
        cell_dims=payload.cell_dims,
    )


@router.post("/seasonal")
def advanced_analysis_seasonal(payload: AdvancedAnalysisSeasonalRequest, _=Depends(optional_viewer)) -> dict:
    return compute_seasonal_decomposition(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        model_filter=payload.model_filter,
        segment_filter=payload.segment_filter,
    )


@router.post("/cell-attribution")
def advanced_analysis_cell_attribution(payload: AdvancedAnalysisCellAttributionRequest, _=Depends(optional_viewer)) -> dict:
    return compute_cell_attribution(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        cell_dims=payload.cell_dims,
        top_n_cells=payload.top_n_cells,
    )


@router.post("/transfer-matrix")
def advanced_analysis_transfer_matrix(payload: AdvancedAnalysisTransferMatrixRequest, _=Depends(optional_viewer)) -> dict:
    return compute_probabilistic_transfer_matrix(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        cell_dims=payload.cell_dims,
        top_n_models=payload.top_n_models,
    )


@router.post("/nested-shift-share")
def advanced_analysis_nested_shift_share(payload: AdvancedAnalysisNestedShiftShareRequest, _=Depends(optional_viewer)) -> dict:
    return compute_nested_shift_share(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        base_period=payload.base_period,
        hierarchy=payload.hierarchy,
    )


@router.post("/drilldown")
def advanced_analysis_drilldown(payload: AdvancedAnalysisDrilldownRequest, _=Depends(optional_viewer)) -> dict:
    return compute_drilldown(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        scope_filters=payload.scope_filters,
        base_period=payload.base_period,
        top_n=payload.top_n,
    )


@router.post("/transfer-mart")
def advanced_analysis_transfer_mart(payload: AdvancedAnalysisTransferMartRequest, _=Depends(optional_viewer)) -> dict:
    return compute_transfer_mart(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        segments=payload.segments,
        scope_filters=payload.scope_filters,
        base_period=payload.base_period,
        sales_mode=payload.sales_mode,
        top_n=payload.top_n,
    )


@router.get("/segments")
def list_available_segments(
    country: str = Query(default="瑞典"),
) -> dict:
    """Return distinct segments available in the parquet data."""
    from app.services.advanced_analysis_service import build_fact_sales_monthly
    fact = build_fact_sales_monthly(country=country)
    segments = sorted(fact["segment"].dropna().unique().tolist()) if "segment" in fact.columns else []
    return {"country": country, "segments": segments}


@router.delete("/cache")
def advanced_analysis_clear_cache(_=Depends(optional_viewer)) -> dict:
    return clear_advanced_analysis_cache()
