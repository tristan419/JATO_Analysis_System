import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import { AdminToolsNav } from "../components/AdminToolsNav";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { CollapsibleFilterSidebar } from "../components/CollapsibleFilterSidebar";
import { LoadingSurface } from "../components/LoadingSurface";
import { SearchSelectFilter } from "../components/SearchSelectFilter";
import { useSharedFilterScope } from "../contexts/SharedFilterScopeContext";
import { FILTER_ORDER } from "../dashboardFilters";
import type { DetailResponse } from "../types";
import { getCachedPageValue, setCachedPageValue } from "../utils/pageCache";

function pickDefaultDetailColumns(columns: string[]): string[] {
  const years = columns
    .filter((column) => /^\d{4}$/.test(column))
    .sort()
    .reverse()
    .slice(0, 2);
  const preferred = [
    "国家",
    "Country",
    "Make",
    "品牌",
    "Model",
    "Version name",
    "Version Name",
    "动总规整",
    "细分市场（按车长）",
    ...years,
  ].filter((column) => columns.includes(column));

  return Array.from(new Set([...preferred, ...columns])).slice(0, 10);
}

const SPECIFICATION_CACHE_KEY = "specification-page";
const PAGE_CACHE_TTL_MS = 30 * 60 * 1000;

interface SpecificationPageCache {
  search: string;
  detail: DetailResponse | null;
  detailPage: number;
  detailPageSize: number;
  selectedCols: string[];
  excludeZeroSales: boolean;
}

export function SpecificationPage() {
  const currentSearch = typeof window !== "undefined" ? window.location.search : "";
  const cachedPageRef = useRef<SpecificationPageCache | null>(null);
  if (cachedPageRef.current === null) {
    const cached = getCachedPageValue<SpecificationPageCache>(
      SPECIFICATION_CACHE_KEY,
    );
    cachedPageRef.current = cached && cached.search === currentSearch ? cached : null;
  }
  const cachedPage = cachedPageRef.current;

  const {
    columns,
    selections,
    optionsMap,
    overview,
    filtersReady,
    loading: sharedLoading,
    optionsSyncPending,
    error: sharedError,
    activeFilters,
    activeFilterSummary,
    dashboardHref,
    dashboardSearch,
    heroCollapsed,
    sidebarCollapsed,
    setHeroCollapsed,
    setSidebarCollapsed,
    buildFilterPayload,
    filterPayloadStr,
    onFilterChange,
    resetFilters,
  } = useSharedFilterScope();

  const [detail, setDetail] = useState<DetailResponse | null>(
    () => cachedPage?.detail ?? null,
  );
  const [detailPage, setDetailPage] = useState(
    () => cachedPage?.detailPage ?? 1,
  );
  const [detailPageSize, setDetailPageSize] = useState(
    () => cachedPage?.detailPageSize ?? 100,
  );
  const [selectedCols, setSelectedCols] = useState<string[]>(
    () => cachedPage?.selectedCols ?? [],
  );
  const [excludeZeroSales, setExcludeZeroSales] = useState(
    () => cachedPage?.excludeZeroSales ?? true,
  );
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState("");
  const skipInitialDetailResetRef = useRef(Boolean(cachedPage));
  const skipInitialDetailFetchRef = useRef(Boolean(cachedPage));

  useEffect(() => {
    if (columns.length === 0) return;
    setSelectedCols((current) => {
      const sanitized = current.filter((column) => columns.includes(column));
      if (sanitized.length > 0) return sanitized;
      return pickDefaultDetailColumns(columns);
    });
  }, [columns]);

  const selectedColsStr = useMemo(
    () => JSON.stringify(selectedCols),
    [selectedCols],
  );
  const visibleCols = selectedCols.length ? selectedCols : columns.slice(0, 8);
  const combinedError = sharedError || error;
  const detailDeckState = detailLoading || sharedLoading ? "SYNC" : "READY";
  const detailDeckRows = detail?.total ?? 0;
  const detailDeckPageLabel = `${detail?.page ?? detailPage} / ${Math.max(1, Math.ceil(detailDeckRows / detailPageSize))}`;

  useEffect(() => {
    if (skipInitialDetailResetRef.current) {
      skipInitialDetailResetRef.current = false;
      return;
    }
    setDetailPage(1);
  }, [filterPayloadStr, selectedColsStr, detailPageSize, excludeZeroSales]);

  useEffect(() => {
    if (
      !filtersReady ||
      columns.length === 0 ||
      selectedCols.length === 0 ||
      optionsSyncPending
    ) {
      return;
    }
    if (skipInitialDetailFetchRef.current) {
      skipInitialDetailFetchRef.current = false;
      return;
    }

    let cancelled = false;
    const timer = setTimeout(async () => {
      setDetailLoading(true);
      setError("");
      try {
        const detailResponse = await api.detail({
          filters: JSON.parse(filterPayloadStr) as Record<string, string[]>,
          columns: selectedCols,
          page: detailPage,
          page_size: detailPageSize,
          exclude_zero_sales: excludeZeroSales,
        });
        if (!cancelled) {
          setDetail(detailResponse);
          setDetailPage(detailResponse.page);
        }
      } catch (err) {
        if (!cancelled) setError((err as Error).message);
      } finally {
        if (!cancelled) setDetailLoading(false);
      }
    }, 250);

    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [
    columns.length,
    detailPage,
    detailPageSize,
    excludeZeroSales,
    filterPayloadStr,
    filtersReady,
    optionsSyncPending,
    selectedCols,
    selectedColsStr,
  ]);

  const specificationCacheSnapshot = useMemo<SpecificationPageCache>(
    () => ({
      search: dashboardSearch,
      detail,
      detailPage,
      detailPageSize,
      selectedCols,
      excludeZeroSales,
    }),
    [
      dashboardSearch,
      detail,
      detailPage,
      detailPageSize,
      excludeZeroSales,
      selectedCols,
    ],
  );

  useEffect(() => {
    if (!filtersReady || columns.length === 0) return;
    setCachedPageValue(
      SPECIFICATION_CACHE_KEY,
      specificationCacheSnapshot,
      PAGE_CACHE_TTL_MS,
    );
  }, [columns.length, filtersReady, specificationCacheSnapshot]);

  async function exportCsv() {
    try {
      const blob = await api.detailCsv({
        filters: buildFilterPayload(),
        columns: selectedCols,
        max_rows: 10000,
        exclude_zero_sales: excludeZeroSales,
      });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = "jato_specification_export.csv";
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      setError((err as Error).message);
    }
  }

  return (
    <div className="dashboard-shell specification-shell">
      <CollapsibleFilterSidebar
        collapsed={sidebarCollapsed}
        onToggle={() => setSidebarCollapsed((current) => !current)}
        kicker="02 / Filter Stack"
        title="规格明细"
        summary={activeFilterSummary}
        expandedLabel="展开规格筛选面板"
        collapsedLabel="收起规格筛选面板"
        expandedTitle="Expand specification filters"
        collapsedTitle="Collapse specification filters"
        className="sidebar sidebar-panel"
      >
        <div className="sidebar-header">
          <span className="panel-kicker">02 / Filter Stack</span>
          <h2>规格明细</h2>
          <p>共享 Dashboard 的筛选口径，只在这里承载明细表、列选择和 CSV 导出。</p>
        </div>

        <div className="sidebar-meta">
          <div className="sidebar-stat">
            <span className="sidebar-stat-label">Active filters</span>
            <strong className="sidebar-stat-value">
              {String(activeFilters.length).padStart(2, "0")}
            </strong>
          </div>
          <div className="sidebar-stat">
            <span className="sidebar-stat-label">Visible columns</span>
            <strong className="sidebar-stat-value">
              {String(visibleCols.length).padStart(2, "0")}
            </strong>
          </div>
        </div>

        <div className="sidebar-actions">
          <Link className="btn btn-secondary" to={dashboardHref}>
            返回 Dashboard
          </Link>
          <button className="btn btn-ghost" onClick={resetFilters}>
            重置筛选
          </button>
        </div>

        {FILTER_ORDER.map(({ key, label }) => (
          <SearchSelectFilter
            key={key}
            label={label}
            options={optionsMap[key] ?? []}
            selected={selections[key]}
            onChange={(values) => void onFilterChange(key, values)}
            showSuvShortcut={key === "segment"}
          />
        ))}
      </CollapsibleFilterSidebar>

      <section className="content specification-content">
        {combinedError && <div className="alert alert-error">{combinedError}</div>}

        <CollapsibleDeckHero
          collapsed={heroCollapsed}
          onToggle={() => setHeroCollapsed((current) => !current)}
          expandedLabel="展开规格概览面板"
          collapsedLabel="收起规格概览面板"
          expandedTitle="Expand specification overview"
          collapsedTitle="Collapse specification overview"
          className="header-card dashboard-hero specification-deck-hero"
          head={(
            <>
              <div className="dashboard-hero-copy specification-header-copy">
                <span className="page-kicker">02 / Specification Intelligence</span>
                <h1>Specification / Detail Explorer</h1>
                
                <div className="dashboard-hero-inline-summary">
                  <span className="selection-ribbon-label">Current scope</span>
                  <span className="selection-ribbon-value">{activeFilterSummary}</span>
                </div>
              </div>

              <div className="dashboard-hero-actions specification-hero-meta">
                <div className="hero-meta-block hero-meta-block-immersive">
                  <span className="hero-meta-label">Filter scope</span>
                  <strong className="hero-meta-value">
                    {activeFilters.length
                      ? String(activeFilters.length).padStart(2, "0")
                      : "FULL"}
                  </strong>
                  <span className="hero-meta-subvalue">Shared with dashboard</span>
                </div>
                <div className={`hero-meta-block hero-meta-block-immersive${detailLoading || sharedLoading ? " is-loading" : ""}`}>
                  <span className="hero-meta-label">Data state</span>
                  <strong className="hero-meta-value">
                    {detailLoading || sharedLoading ? "SYNC" : "READY"}
                  </strong>
                  <span className="hero-meta-subvalue">Detail + overview linked</span>
                  {(detailLoading || sharedLoading) && (
                    <span className="hero-meta-loader">SYNCING SPEC GRID</span>
                  )}
                </div>
              </div>
            </>
          )}
          body={(
            <div className="dashboard-hero-rail">
              <div className="dashboard-hero-chip-row">
                <span className="dashboard-hero-chip">Shared filter state</span>
                <span className="dashboard-hero-chip">{activeFilterSummary}</span>
                <span className="dashboard-hero-chip">Visible columns {visibleCols.length}</span>
              </div>
              <div className="dashboard-hero-rail-actions">
                <Link className="btn btn-sm btn-secondary" to={dashboardHref}>
                  Return dashboard
                </Link>
                <button type="button" className="btn btn-sm btn-primary" onClick={resetFilters}>
                  Reset filters
                </button>
              </div>
            </div>
          )}
        />

        <div className="metrics-grid">
          <div className="kpi-card">
            <span className="kpi-label">筛选后记录数</span>
            <strong className="kpi-value">
              {overview?.kpis.totalRows?.toLocaleString() ?? "-"}
            </strong>
          </div>
          <div className="kpi-card">
            <span className="kpi-label">品牌数</span>
            <strong className="kpi-value">
              {overview?.kpis.brandCount?.toLocaleString() ?? "-"}
            </strong>
          </div>
          <div className="kpi-card">
            <span className="kpi-label">Model 数</span>
            <strong className="kpi-value">
              {overview?.kpis.modelCount?.toLocaleString() ?? "-"}
            </strong>
          </div>
          <div className="kpi-card">
            <span className="kpi-label">Version 数</span>
            <strong className="kpi-value">
              {overview?.kpis.versionCount?.toLocaleString() ?? "-"}
            </strong>
          </div>
        </div>

        {sharedLoading && !overview && (
          <LoadingSurface
            mode="overlay"
            label="正在初始化规格页"
            detail="复用共享筛选作用域，并同步概览指标与首屏字段配置"
            kicker="Spec"
          />
        )}

        <div className="card analysis-deck-card specification-detail-card">
          <div className="analysis-deck-head">
            <div className="analysis-deck-copy">
              <span className="panel-kicker">03 / Detail Grid</span>
              <h3>Specification Detail Preview</h3>
              <p>列选择、分页和 CSV 导出在本页集中处理，适合在共享筛选口径下做按需明细查询和字段投影。</p>
              <div className="analysis-chip-row">
                <span className="analysis-chip">{activeFilterSummary}</span>
                <span className="analysis-chip">Visible columns {visibleCols.length}</span>
                <span className="analysis-chip">Page size {detailPageSize}</span>
                {excludeZeroSales && <span className="analysis-chip">Only non-zero sales</span>}
              </div>
            </div>
            <div className="analysis-deck-meta">
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Data State</span>
                <strong className="analysis-deck-stat-value">{detailDeckState}</strong>
                <span className="analysis-deck-stat-subvalue">Detail rows + overview linked</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Selected Columns</span>
                <strong className="analysis-deck-stat-value">{String(selectedCols.length).padStart(2, "0")}</strong>
                <span className="analysis-deck-stat-subvalue">自动补齐默认关键列</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Row Volume</span>
                <strong className="analysis-deck-stat-value">{detailDeckRows.toLocaleString()}</strong>
                <span className="analysis-deck-stat-subvalue">当前筛选下总记录数</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Page Window</span>
                <strong className="analysis-deck-stat-value">{detailDeckPageLabel}</strong>
                <span className="analysis-deck-stat-subvalue">分页状态与导出共用同一列配置</span>
              </div>
            </div>
          </div>

          <div className="analysis-chart-block analysis-chart-block--compact">
            <div className="detail-toolbar specification-toolbar">
              <div className="detail-toolbar-cluster">
                <button className="btn btn-accent" disabled={!selectedCols.length || detailLoading} onClick={exportCsv}>
                  CSV 下载
                </button>
                <label className="toolbar-checkbox">
                  <input
                    type="checkbox"
                    checked={excludeZeroSales}
                    onChange={(event) => setExcludeZeroSales(event.target.checked)}
                  />
                  仅显示有销量版型
                </label>
              </div>
              <div className="detail-toolbar-cluster detail-toolbar-cluster--push">
                <div className="filter-group">
                  <label>每页</label>
                  <select
                    value={detailPageSize}
                    onChange={(event) => setDetailPageSize(Number(event.target.value))}
                  >
                    <option value={50}>50</option>
                    <option value={100}>100</option>
                    <option value={200}>200</option>
                  </select>
                </div>
              </div>
            </div>
          </div>

          <div className="analysis-subsection specification-subsection">
            <div className="analysis-subsection-head">
              <div>
                <div className="analysis-subsection-title">Column Projection</div>
                <p className="analysis-inline-note">点击列名切换可见字段。当前未选择时，会自动显示默认关键列。</p>
              </div>
            </div>
            <div className="col-picker">
              {columns.map((column) => (
                <span
                  key={column}
                  className={`col-chip${selectedCols.includes(column) ? " selected" : ""}`}
                  onClick={() =>
                    setSelectedCols((current) =>
                      current.includes(column)
                        ? current.filter((item) => item !== column)
                        : [...current, column],
                    )
                  }
                >
                  {column}
                </span>
              ))}
            </div>
          </div>

          <div className="analysis-subsection specification-subsection">
            <div className="analysis-subsection-head">
              <div>
                <div className="analysis-subsection-title">Detail Table</div>
                <p className="analysis-inline-note">当前显示第 {detail?.page ?? detailPage} 页，按共享筛选结果回填明细与导出范围。</p>
              </div>
            </div>

            {detailLoading && (
              <LoadingSurface
                label="正在刷新明细"
                detail="按当前列选择、分页和销量过滤条件更新结果"
                kicker="Detail"
              />
            )}

            <div className="analysis-table-wrap">
              <div className="table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      {visibleCols.map((column) => (
                        <th key={column}>{column}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {(detail?.items ?? []).map((row, index) => (
                      <tr key={index}>
                        {visibleCols.map((column) => (
                          <td key={column}>
                            {String((row as Record<string, unknown>)[column] ?? "")}
                          </td>
                        ))}
                      </tr>
                    ))}
                    {!detail?.items?.length && !detailLoading && (
                      <tr>
                        <td
                          colSpan={Math.max(visibleCols.length, 1)}
                          className="table-empty-cell"
                        >
                          当前筛选下暂无明细数据
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="pagination">
              <span className="pagination-status">
                第 {detail?.page ?? detailPage} 页 · 共 {(detail?.total ?? 0).toLocaleString()} 条
              </span>
              <div className="btn-group">
                <button
                  className="btn btn-sm btn-secondary"
                  disabled={detailLoading || detailPage <= 1}
                  onClick={() => setDetailPage((current) => Math.max(1, current - 1))}
                >
                  上一页
                </button>
                <button
                  className="btn btn-sm btn-secondary"
                  disabled={detailLoading || !detail || detail.items.length < detailPageSize}
                  onClick={() => setDetailPage((current) => current + 1)}
                >
                  下一页
                </button>
              </div>
            </div>
          </div>
        </div>
        <AdminToolsNav />
      </section>
    </div>
  );
}
