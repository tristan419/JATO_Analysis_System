import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import { SearchSelectFilter } from "../components/SearchSelectFilter";
import {
  DIM,
  FILTER_KEYS,
  FILTER_ORDER,
  buildSearchFromSelections,
  createEmptySelections,
  getDefaultPowertrainValues,
  hasSelections,
  readSelectionsFromSearch,
  resolve,
  type FilterKey,
  type FilterSelections,
} from "../dashboardFilters";
import type { DetailResponse, OverviewResponse } from "../types";

function pickDefaultDetailColumns(columns: string[]): string[] {
  const years = columns.filter((column) => /^\d{4}$/.test(column)).sort().reverse().slice(0, 2);
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

export function SpecificationPage() {
  const [columns, setColumns] = useState<string[]>([]);
  const [selections, setSelections] = useState<FilterSelections>(createEmptySelections);
  const [optionsMap, setOptionsMap] = useState<Partial<Record<FilterKey, string[]>>>({});
  const [overview, setOverview] = useState<OverviewResponse | null>(null);
  const [detail, setDetail] = useState<DetailResponse | null>(null);
  const [detailPage, setDetailPage] = useState(1);
  const [detailPageSize, setDetailPageSize] = useState(100);
  const [selectedCols, setSelectedCols] = useState<string[]>([]);
  const [excludeZeroSales, setExcludeZeroSales] = useState(false);
  const [bootLoading, setBootLoading] = useState(false);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [filtersReady, setFiltersReady] = useState(false);
  const [error, setError] = useState("");

  const res = useMemo(() => {
    const mapped = {} as Record<FilterKey, string | null>;
    for (const key of FILTER_KEYS) mapped[key] = resolve(columns, DIM[key]);
    return mapped;
  }, [columns]);

  const buildFilterPayload = useCallback((): Record<string, string[]> => {
    const payload: Record<string, string[]> = {};
    for (const { key } of FILTER_ORDER) {
      const column = res[key];
      const values = selections[key];
      if (column && values.length) payload[column] = values;
    }
    return payload;
  }, [res, selections]);

  const filterPayloadStr = useMemo(() => JSON.stringify(buildFilterPayload()), [buildFilterPayload]);
  const selectedColsStr = useMemo(() => JSON.stringify(selectedCols), [selectedCols]);
  const filterSearch = useMemo(() => buildSearchFromSelections(selections), [selections]);
  const dashboardHref = filterSearch ? `/${filterSearch}` : "/";
  const visibleCols = selectedCols.length ? selectedCols : columns.slice(0, 8);
  const activeFilters = useMemo(
    () => FILTER_ORDER.filter(({ key }) => selections[key].length > 0),
    [selections]
  );
  const activeFilterSummary = useMemo(() => {
    if (!activeFilters.length) return "默认动力总成视角，无额外维度约束";
    return activeFilters
      .map(({ key, label }) => `${label} ${selections[key].length}`)
      .join(" / ");
  }, [activeFilters, selections]);

  const syncOptionsForColumns = useCallback(
    async (sourceColumns: string[], nextSelections: FilterSelections) => {
      const nextOptions: Partial<Record<FilterKey, string[]>> = {};
      const sanitized = createEmptySelections();
      const prefixFilters: Record<string, string[]> = {};

      for (const { key } of FILTER_ORDER) {
        const column = resolve(sourceColumns, DIM[key]);
        if (!column) continue;

        const response = await api.filterOptions({ column, filters: prefixFilters });
        nextOptions[key] = response.options;

        const validSelections = nextSelections[key].filter((value) => response.options.includes(value));
        sanitized[key] = validSelections;

        if (validSelections.length) prefixFilters[column] = validSelections;
      }

      setOptionsMap(nextOptions);
      return { selections: sanitized, options: nextOptions };
    },
    []
  );

  useEffect(() => {
    let cancelled = false;

    (async () => {
      setBootLoading(true);
      setError("");
      try {
        const { items } = await api.columns();
        if (cancelled) return;

        setColumns(items);
        setSelectedCols(pickDefaultDetailColumns(items));

        let initialSelections = readSelectionsFromSearch(window.location.search);
        let syncResult = await syncOptionsForColumns(items, initialSelections);
        if (cancelled) return;

        if (!hasSelections(syncResult.selections)) {
          const defaults = getDefaultPowertrainValues(syncResult.options.powertrain ?? []);
          if (defaults.length) {
            initialSelections = {
              ...syncResult.selections,
              powertrain: defaults,
            };
            syncResult = await syncOptionsForColumns(items, initialSelections);
            if (cancelled) return;
          }
        }

        setSelections(syncResult.selections);
        setFiltersReady(true);
      } catch (err) {
        if (!cancelled) setError((err as Error).message);
      } finally {
        if (!cancelled) setBootLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [syncOptionsForColumns]);

  useEffect(() => {
    const newUrl = `${window.location.pathname}${filterSearch}`;
    window.history.replaceState(null, "", newUrl);
  }, [filterSearch]);

  useEffect(() => {
    if (!filtersReady || columns.length === 0) return;
    let cancelled = false;

    const timer = setTimeout(async () => {
      setOverviewLoading(true);
      setError("");
      try {
        const overviewResponse = await api.overview({
          filters: JSON.parse(filterPayloadStr) as Record<string, string[]>,
          prefer_precomputed: true,
          top_n: 120,
        });
        if (!cancelled) setOverview(overviewResponse);
      } catch (err) {
        if (!cancelled) setError((err as Error).message);
      } finally {
        if (!cancelled) setOverviewLoading(false);
      }
    }, 250);

    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [columns.length, filterPayloadStr, filtersReady]);

  useEffect(() => {
    setDetailPage(1);
  }, [filterPayloadStr, selectedColsStr, detailPageSize, excludeZeroSales]);

  useEffect(() => {
    if (!filtersReady || columns.length === 0 || selectedCols.length === 0) return;
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
  }, [columns.length, detailPage, detailPageSize, excludeZeroSales, filterPayloadStr, filtersReady, selectedCols, selectedColsStr]);

  async function onFilterChange(dimKey: FilterKey, newVals: string[]) {
    const nextSelections: FilterSelections = {
      ...selections,
      [dimKey]: newVals,
    };
    setError("");
    try {
      const { selections: syncedSelections } = await syncOptionsForColumns(columns, nextSelections);
      setSelections(syncedSelections);
    } catch (err) {
      setError((err as Error).message);
    }
  }

  function resetFilters() {
    void (async () => {
      setError("");
      try {
        const defaults = getDefaultPowertrainValues(optionsMap.powertrain ?? []);
        const nextSelections: FilterSelections = {
          ...createEmptySelections(),
          powertrain: defaults,
        };
        const { selections: syncedSelections } = await syncOptionsForColumns(columns, nextSelections);
        setSelections(syncedSelections);
      } catch (err) {
        setError((err as Error).message);
      }
    })();
  }

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
      <aside className="sidebar sidebar-panel">
        <div className="sidebar-header">
          <span className="panel-kicker">02 / Filter Stack</span>
          <h2>规格明细</h2>
          <p>独立承载明细表、列选择和 CSV 导出，避免 Dashboard 首页额外挂载大表状态。</p>
        </div>

        <div className="sidebar-meta">
          <div className="sidebar-stat">
            <span className="sidebar-stat-label">Active filters</span>
            <strong className="sidebar-stat-value">{String(activeFilters.length).padStart(2, "0")}</strong>
          </div>
          <div className="sidebar-stat">
            <span className="sidebar-stat-label">Visible columns</span>
            <strong className="sidebar-stat-value">{String(visibleCols.length).padStart(2, "0")}</strong>
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
      </aside>

      <section className="content specification-content">
        <div className="page-header specification-header">
          <div className="specification-header-copy">
            <span className="page-kicker">02 / Specification Intelligence</span>
            <h1>Specification / Detail Explorer</h1>
            <p>当前页自动跟随筛选刷新，专注车型规格、行级数据和导出，不再占用主看板首屏预算。</p>
          </div>
          <div className="hero-meta specification-hero-meta">
            <div className="hero-meta-block">
              <span className="hero-meta-label">Filter scope</span>
              <strong className="hero-meta-value">{activeFilters.length ? String(activeFilters.length).padStart(2, "0") : "FULL"}</strong>
            </div>
            <div className="hero-meta-block">
              <span className="hero-meta-label">Data state</span>
              <strong className="hero-meta-value">{detailLoading || overviewLoading ? "SYNC" : "READY"}</strong>
            </div>
          </div>
        </div>

        {error && <div className="alert alert-error">{error}</div>}

        <div className="selection-ribbon">
          <span className="selection-ribbon-label">Current scope</span>
          <span className="selection-ribbon-value">{activeFilterSummary}</span>
        </div>

        <div className="metrics-grid">
          <div className="kpi-card">
            <span className="kpi-label">筛选后记录数</span>
            <strong className="kpi-value">{overview?.kpis.totalRows?.toLocaleString() ?? "-"}</strong>
          </div>
          <div className="kpi-card">
            <span className="kpi-label">品牌数</span>
            <strong className="kpi-value">{overview?.kpis.brandCount?.toLocaleString() ?? "-"}</strong>
          </div>
          <div className="kpi-card">
            <span className="kpi-label">Model 数</span>
            <strong className="kpi-value">{overview?.kpis.modelCount?.toLocaleString() ?? "-"}</strong>
          </div>
          <div className="kpi-card">
            <span className="kpi-label">Version 数</span>
            <strong className="kpi-value">{overview?.kpis.versionCount?.toLocaleString() ?? "-"}</strong>
          </div>
        </div>

        {(bootLoading || overviewLoading) && !overview && (
          <div className="loading-banner">
            <span className="spinner" /> 正在初始化规格页…
          </div>
        )}

        <div className="card">
          <div className="detail-section-head">
            <div>
              <div className="card-title">规格与明细预览</div>
              <p className="section-note">列选择、分页和 CSV 导出在本页集中处理，适合带宽受限场景下做按需查询。</p>
            </div>
            <div className="table-status-chip">
              <span>已选列</span>
              <strong>{selectedCols.length}</strong>
            </div>
          </div>
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

          <div className="col-picker-header">
            <span className="section-note">点击列名切换可见字段。当前未选择时，会自动显示默认关键列。</span>
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
                      : [...current, column]
                  )
                }
              >
                {column}
              </span>
            ))}
          </div>

          {detailLoading && (
            <div className="loading-banner">
              <span className="spinner" /> 正在刷新明细…
            </div>
          )}

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
                      <td key={column}>{String((row as Record<string, unknown>)[column] ?? "")}</td>
                    ))}
                  </tr>
                ))}
                {!detail?.items?.length && !detailLoading && (
                  <tr>
                    <td colSpan={Math.max(visibleCols.length, 1)} className="table-empty-cell">
                      当前筛选下暂无明细数据
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
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
      </section>
    </div>
  );
}