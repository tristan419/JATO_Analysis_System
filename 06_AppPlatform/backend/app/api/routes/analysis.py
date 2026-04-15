from fastapi import APIRouter, Depends, Response

from app.api.schemas import (
    AdvancedChartRequest,
    AnalysisRequest,
    DetailCsvRequest,
    DetailQueryRequest,
    GroupedTimeSeriesRequest,
    ModelVersionsRequest,
    OverviewRequest,
    PositioningMapRequest,
    RvFinanceRequest,
    TimeSeriesRequest,
)
from app.core.config import MAX_DETAIL_PAGE_SIZE, MAX_EXPORT_ROWS
from app.core.security import require_min_role
from app.services.query_service import (
    export_detail_csv,
    get_data_freshness,
    query_analysis,
    query_advanced_chart,
    query_detail,
    query_grouped_time_series,
    query_model_versions,
    query_overview,
    query_positioning_map,
    query_rv_finance,
    query_time_series,
)

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.post("/query")
def query(
    payload: AnalysisRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_analysis(
        filters=payload.filters,
        group_by=payload.group_by,
        metric_candidates=payload.metric_candidates,
        top_n=payload.top_n,
        prefer_precomputed=payload.prefer_precomputed,
    )


@router.post("/time-series")
def time_series(
    payload: TimeSeriesRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_time_series(
        filters=payload.filters,
        grain=payload.grain,
        top_n=payload.top_n,
    )


@router.post("/overview")
def overview(
    payload: OverviewRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_overview(
        filters=payload.filters,
        prefer_precomputed=payload.prefer_precomputed,
        top_n=payload.top_n,
    )


@router.get("/data-freshness")
def data_freshness(
    _=Depends(require_min_role("viewer")),
) -> dict:
    return {"items": get_data_freshness()}


@router.post("/detail")
def detail(
    payload: DetailQueryRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    page_size = min(max(1, int(payload.page_size)), MAX_DETAIL_PAGE_SIZE)
    return query_detail(
        filters=payload.filters,
        columns=payload.columns,
        page=payload.page,
        page_size=page_size,
        exclude_zero_sales=payload.exclude_zero_sales,
    )


@router.post("/detail-csv")
def detail_csv(
    payload: DetailCsvRequest,
    _=Depends(require_min_role("viewer")),
) -> Response:
    max_rows = min(max(1, int(payload.max_rows)), MAX_EXPORT_ROWS)
    csv_bytes = export_detail_csv(
        filters=payload.filters,
        columns=payload.columns,
        max_rows=max_rows,
        exclude_zero_sales=payload.exclude_zero_sales,
    )
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": (
                "attachment; filename=jato_detail_preview.csv"
            ),
        },
    )


@router.post("/time-series-grouped")
def time_series_grouped(
    payload: GroupedTimeSeriesRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_grouped_time_series(
        filters=payload.filters,
        grain=payload.grain,
        group_by=payload.group_by,
        top_n=payload.top_n,
        include_others=payload.include_others,
    )


@router.post("/advanced-chart")
def advanced_chart(
    payload: AdvancedChartRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_advanced_chart(
        group=payload.group,
        chart=payload.chart,
        filters=payload.filters,
        top_n=payload.top_n,
        options=payload.options,
    )


@router.post("/model-versions")
def model_versions(
    payload: ModelVersionsRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_model_versions(
        filters=payload.filters,
        model_name=payload.model_name,
        top_n=payload.top_n,
    )


@router.post("/positioning-map")
def positioning_map(
    payload: PositioningMapRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_positioning_map(
        filters=payload.filters,
        target_length=payload.target_length,
        target_msrp=payload.target_msrp,
        length_range=payload.length_range,
        manual_competitors=payload.manual_competitors,
        top_n=payload.top_n,
        n_clusters=payload.n_clusters,
    )


@router.post("/rv-finance")
def rv_finance(
    payload: RvFinanceRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_rv_finance(
        vehicles=[v.model_dump() for v in payload.vehicles],
        currency=payload.currency,
        fx_rate=payload.fx_rate,
        sensitivity_vehicle_idx=payload.sensitivity_vehicle_idx,
    )
