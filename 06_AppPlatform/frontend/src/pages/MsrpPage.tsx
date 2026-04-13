import { Fragment, useEffect, useRef, useState } from "react";

import { api } from "../api/client";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { LoadingSurface } from "../components/LoadingSurface";
import { PriceHistoryTimeline } from "../components/PriceHistoryTimeline";
import {
  TextSearchFilters,
  useTextSearchFilters,
} from "../components/TextSearchFilters";
import type { CurrentPrice, PriceHistoryEntry } from "../types";
import {
  getCurrentPriceMatchStatusBadgeClass,
  getCurrentPriceMatchStatusLabel,
} from "../utils/reviewStatus";
import {
  buildCurrentPriceGroupKey,
  formatCurrentPriceDate,
  formatCurrentPriceNumber,
  resolveCurrentPriceGroupModel,
  resolveCurrentMsrpValue,
  resolveLastPriceChangeAtUtc,
  resolveUpdatedAtUtc,
} from "../utils/msrpCurrentPrice";


function getReviewerName() {
  return (localStorage.getItem("jato_user_name") || "anonymous").trim() || "anonymous";
}


interface CurrentPriceGroup {
  key: string;
  country: string;
  brand: string;
  model: string;
  items: CurrentPrice[];
}

function groupCurrentPrices(prices: CurrentPrice[]) {
  const groups = new Map<string, CurrentPriceGroup>();

  prices.forEach((price) => {
    const key = buildCurrentPriceGroupKey(price);
    const existing = groups.get(key);

    if (existing) {
      existing.items.push(price);
      return;
    }

    groups.set(key, {
      key,
      country: price.country,
      brand: price.brand,
      model: resolveCurrentPriceGroupModel(price),
      items: [price],
    });
  });

  return Array.from(groups.values());
}

function summarizeCurrentPriceTrims(prices: CurrentPrice[]) {
  const trims = Array.from(new Set(prices.map((price) => price.officialTrim || price.jatoTrim || "未命名 Trim")));

  if (trims.length <= 3) {
    return trims.join(" / ");
  }

  return `${trims.slice(0, 3).join(" / ")} +${trims.length - 3}`;
}

function formatCurrentPriceRange(prices: CurrentPrice[]) {
  const values = prices
    .map((price) => resolveCurrentMsrpValue(price))
    .filter((value): value is number => value !== null && value !== undefined && !Number.isNaN(value));

  if (values.length === 0) {
    return "-";
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const minLabel = formatCurrentPriceNumber(min);
  const maxLabel = formatCurrentPriceNumber(max);

  return min === max ? minLabel : `${minLabel} - ${maxLabel}`;
}

function formatSourceLink(url?: string) {
  if (!url) {
    return "-";
  }
  try {
    const parsed = new URL(url);
    const path = parsed.pathname === "/"
      ? ""
      : parsed.pathname.length > 24
        ? `${parsed.pathname.slice(0, 24)}…`
        : parsed.pathname;
    return `${parsed.hostname}${path}`;
  } catch {
    return url.length > 36 ? `${url.slice(0, 36)}…` : url;
  }
}

function formatPriceHistoryWindow(entry: PriceHistoryEntry) {
  const start = formatCurrentPriceDate(entry.validFromUtc);
  const end = entry.validToUtc
    ? formatCurrentPriceDate(entry.validToUtc)
    : entry.lastConfirmedAtUtc && entry.lastConfirmedAtUtc !== entry.validFromUtc
      ? `当前（最近确认 ${formatCurrentPriceDate(entry.lastConfirmedAtUtc)}）`
      : "当前";
  return `${start} → ${end}`;
}

export function MsrpPage() {
  const currentPricePageSize = 500;

  /* ── state ──────────────────────────────────────── */
  const [prices, setPrices] = useState<CurrentPrice[]>([]);
  const [priceAlertCount, setPriceAlertCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [heroCollapsed, setHeroCollapsed] = useState(false);
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({});
  const refreshPricesRequestRef = useRef(0);

  /* filters */
  const {
    applied: appliedTextFilters,
    commitDraft: commitTextFilters,
    draft: draftTextFilters,
    isPending: textFiltersPending,
    reset: resetTextFilters,
    setField: setTextFilterField,
  } = useTextSearchFilters();
  const countryFilter = appliedTextFilters.country;
  const brandFilter = appliedTextFilters.brand;
  const modelFilter = appliedTextFilters.model;

  /* detail drawer */
  const [selectedPrice, setSelectedPrice] = useState<CurrentPrice | null>(null);
  const [detailCollapsed, setDetailCollapsed] = useState(false);
  const [priceHistory, setPriceHistory] = useState<PriceHistoryEntry[]>([]);
  const [priceHistoryLoading, setPriceHistoryLoading] = useState(false);
  const [priceHistoryError, setPriceHistoryError] = useState("");

  /* materialize */
  const [materializing, setMaterializing] = useState(false);
  const [materializeResult, setMaterializeResult] = useState<Record<string, unknown> | null>(null);
  const [remapping, setRemapping] = useState(false);

  /* ── fetchers ───────────────────────────────────── */
  async function refreshPrices(filters = appliedTextFilters) {
    const requestId = ++refreshPricesRequestRef.current;
    setLoading(true);
    setError("");
    try {
      const nextPrices: CurrentPrice[] = [];
      let nextPriceAlertCount = 0;
      let offset = 0;
      let total: number | null = null;

      while (total === null || offset < total) {
        const res = await api.listCurrentPrices({
          country: filters.country || undefined,
          brand: filters.brand || undefined,
          jato_model: filters.model || undefined,
          limit: currentPricePageSize,
          offset,
        });
        total = res.total;
        nextPriceAlertCount = res.priceAlertCount;
        nextPrices.push(...res.items);
        if (res.items.length === 0) {
          break;
        }
        offset += res.items.length;
      }

      if (requestId !== refreshPricesRequestRef.current) {
        return;
      }

      setPrices(nextPrices);
      setPriceAlertCount(nextPriceAlertCount);
    } catch (err) {
      if (requestId !== refreshPricesRequestRef.current) {
        return;
      }

      setError((err as Error).message);
      setPrices([]);
      setPriceAlertCount(0);
    } finally {
      if (requestId === refreshPricesRequestRef.current) {
        setLoading(false);
      }
    }
  }

  async function handleMaterialize() {
    const activeFilters = textFiltersPending ? draftTextFilters : appliedTextFilters;
    setMaterializing(true);
    setError("");
    setNotice("");
    setMaterializeResult(null);
    try {
      if (textFiltersPending) {
        commitTextFilters();
      }
      const res = await api.materializeCurrentPrices({
        country: activeFilters.country || undefined,
        brand: activeFilters.brand || undefined,
        jato_model: activeFilters.model || undefined,
        limit: 500,
      });
      setMaterializeResult(res.item);
      await refreshPrices(activeFilters);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setMaterializing(false);
    }
  }

  async function handleReturnToReview() {
    if (!selectedPrice) {
      return;
    }

    const price = selectedPrice;
    setRemapping(true);
    setError("");
    setNotice("");
    try {
      await api.remapCurrentPrice(price.id, {
        decided_by: getReviewerName(),
        note: `Returned from MSRP current price deck: ${price.brand} ${price.jatoModel} / ${price.jatoTrim}`,
      });
      setSelectedPrice(null);
      setDetailCollapsed(false);
      setNotice(`${price.brand} ${price.jatoModel} / ${price.jatoTrim} 已打回 Review，并从 Current Prices 中移除。`);
      await refreshPrices(appliedTextFilters);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setRemapping(false);
    }
  }

  useEffect(() => {
    void refreshPrices();
  }, [appliedTextFilters]);

  useEffect(() => {
    if (!selectedPrice) {
      setPriceHistory([]);
      setPriceHistoryError("");
      setPriceHistoryLoading(false);
      return;
    }

    const currentPrice = selectedPrice;
    let cancelled = false;

    async function refreshPriceHistory() {
      setPriceHistoryLoading(true);
      setPriceHistoryError("");
      try {
        const res = await api.listPriceHistory({
          country: currentPrice.country,
          brand: currentPrice.brand,
          jato_model: currentPrice.jatoModel,
          jato_trim: currentPrice.jatoTrim,
          limit: 50,
        });
        if (!cancelled) {
          setPriceHistory(res.items);
        }
      } catch (err) {
        if (!cancelled) {
          setPriceHistoryError((err as Error).message);
          setPriceHistory([]);
        }
      } finally {
        if (!cancelled) {
          setPriceHistoryLoading(false);
        }
      }
    }

    void refreshPriceHistory();

    return () => {
      cancelled = true;
    };
  }, [selectedPrice?.country, selectedPrice?.brand, selectedPrice?.jatoModel, selectedPrice?.jatoTrim]);

  /* ── derived ────────────────────────────────────── */
  const uniqueCountries = new Set(prices.map((p) => p.country)).size;
  const uniqueBrands = new Set(prices.map((p) => p.brand)).size;
  const priceGroups = groupCurrentPrices(prices);
  const showLoadingOverlay = loading && prices.length === 0;

  function isGroupExpanded(group: CurrentPriceGroup) {
    return expandedGroups[group.key] ?? (
      group.items.length <= 1
      || priceGroups.length <= 1
      || modelFilter.trim().length > 0
    );
  }

  function toggleGroup(group: CurrentPriceGroup) {
    const expanded = isGroupExpanded(group);
    setExpandedGroups((current) => ({
      ...current,
      [group.key]: !expanded,
    }));
  }

  function setAllGroupsExpanded(nextExpanded: boolean) {
    setExpandedGroups(
      priceGroups.reduce<Record<string, boolean>>((accumulator, group) => {
        accumulator[group.key] = nextExpanded;
        return accumulator;
      }, {}),
    );
  }

  function openPriceDetail(price: CurrentPrice) {
    setSelectedPrice(price);
    setDetailCollapsed(false);
  }

  return (
    <section className={`crud-shell msrp-page-shell${selectedPrice ? " has-detail-dock" : ""}${selectedPrice && detailCollapsed ? " is-detail-dock-collapsed" : ""}`}>
      <CollapsibleDeckHero
        collapsed={heroCollapsed}
        onToggle={() => setHeroCollapsed((c) => !c)}
        expandedLabel="展开 MSRP 概览"
        collapsedLabel="收起 MSRP 概览"
        expandedTitle="Expand MSRP overview"
        collapsedTitle="Collapse MSRP overview"
        className="header-card dashboard-hero crud-hero"
        head={(
          <>
            <div className="dashboard-hero-copy crud-hero-copy">
              <span className="page-kicker">06 / MSRP</span>
              <h1>Current Prices Deck</h1>
              <p>查看已物化的 MSRP 当前价格，触发 materialize 重新计算最新价格快照。</p>
              <div className="dashboard-hero-inline-summary">
                <span className="selection-ribbon-label">Scope</span>
                <span className="selection-ribbon-value">
                  {countryFilter || "All countries"} · {brandFilter || "All brands"} · {modelFilter || "All models"}
                </span>
              </div>
            </div>
            <div className="dashboard-hero-actions crud-hero-actions">
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Prices</span>
                <strong className="hero-meta-value">{prices.length}</strong>
                <span className="hero-meta-subvalue">物化记录数</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Countries</span>
                <strong className="hero-meta-value">{uniqueCountries}</strong>
                <span className="hero-meta-subvalue">覆盖市场</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Brands</span>
                <strong className="hero-meta-value">{uniqueBrands}</strong>
                <span className="hero-meta-subvalue">覆盖品牌</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Price Alerts</span>
                <strong className="hero-meta-value">{priceAlertCount}</strong>
                <span className="hero-meta-subvalue">发生过价格波动</span>
              </div>
            </div>
          </>
        )}
        body={(
          <div className="dashboard-hero-rail">
            <div className="dashboard-hero-chip-row">
              <span className="dashboard-hero-chip">{countryFilter || "All countries"}</span>
              <span className="dashboard-hero-chip">{brandFilter || "All brands"}</span>
              <span className="dashboard-hero-chip">{modelFilter || "All models"}</span>
            </div>
            <div className="dashboard-hero-rail-actions">
              <button type="button" className="btn btn-sm btn-ghost" onClick={resetTextFilters}>重置</button>
              <button
                type="button"
                className="btn btn-sm btn-secondary"
                onClick={() => {
                  if (textFiltersPending) {
                    commitTextFilters();
                    return;
                  }
                  void refreshPrices(appliedTextFilters);
                }}
              >
                刷新
              </button>
              <button
                type="button"
                className="btn btn-sm btn-primary"
                disabled={materializing}
                onClick={handleMaterialize}
              >
                {materializing ? "Materializing…" : "Materialize"}
              </button>
            </div>
          </div>
        )}
      />

      {error && <div className="alert alert-error">{error}</div>}

      {notice && <div className="alert alert-info">{notice}</div>}

      {materializeResult && (
        <div className="card crud-card admin-materialize-result">
          <div className="detail-section-head">
            <div>
              <div className="card-title">Materialize Result</div>
            </div>
            <button type="button" className="btn btn-sm btn-ghost" onClick={() => setMaterializeResult(null)}>关闭</button>
          </div>
          <div className="admin-json-preview">
            <pre>{JSON.stringify(materializeResult, null, 2)}</pre>
          </div>
        </div>
      )}

      {/* ── Filters ─────────────────────────────── */}
      <div className="card crud-card">
        <div className="detail-section-head">
          <div><div className="card-title">过滤</div></div>
        </div>
        <div className="crud-toolbar-grid">
          <TextSearchFilters
            value={draftTextFilters}
            onChange={setTextFilterField}
            modelLabel="Model"
            modelPlaceholder="e.g. XC60 / Model 3"
          />
        </div>
        <p className="section-note">Country 支持别名检索，例如 Sweden、se、SE、瑞典 会命中同一市场。Brand 和 Model 支持模糊搜索，停止输入约 0.3 秒后会自动应用。</p>
      </div>

      {/* ── Prices Table ────────────────────────── */}
      <div className="card crud-table-card">
        <div className="detail-section-head">
          <div>
            <div className="card-title">Current Prices</div>
            <p className="section-note">按 country / brand / model 收纳，由 materialize 端点生成最新价格快照。{loading && prices.length > 0 ? " 正在后台同步最新筛选…" : ""}</p>
          </div>
          <div className="crud-table-toolbar">
            <div className="btn-group">
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                disabled={priceGroups.length === 0}
                onClick={() => setAllGroupsExpanded(true)}
              >
                全部展开
              </button>
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                disabled={priceGroups.length === 0}
                onClick={() => setAllGroupsExpanded(false)}
              >
                全部收起
              </button>
            </div>
            <div className="table-status-chip table-status-chip--compact">
              <span>Model Groups</span>
              <strong>{priceGroups.length}</strong>
              <span>{prices.length} prices</span>
            </div>
          </div>
        </div>

        {showLoadingOverlay && (
          <LoadingSurface mode="overlay" label="正在加载" detail="同步当前价格" kicker="MSRP" />
        )}

        <div className="table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Country</th>
                <th>Brand</th>
                <th>JATO Model</th>
                <th>JATO Trim</th>
                <th>JATO Powertrain</th>
                <th>Official Model</th>
                <th>MSRP (EUR)</th>
                <th>Currency</th>
                <th>Match</th>
                <th>Source</th>
                <th>Last Price Change</th>
                <th>Updated</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {priceGroups.map((group) => {
                const expanded = isGroupExpanded(group);
                const latestUpdatedAt = group.items.reduce<string | null>((currentLatest, price) => {
                  const value = resolveUpdatedAtUtc(price);

                  if (!value) {
                    return currentLatest;
                  }

                  if (!currentLatest || value > currentLatest) {
                    return value;
                  }

                  return currentLatest;
                }, null);

                return (
                  <Fragment key={group.key}>
                    <tr className={`data-table-group-row${expanded ? " is-expanded" : ""}`}>
                      <td colSpan={13}>
                        <div className="data-table-group-cell">
                          <button
                            type="button"
                            className="data-table-group-toggle"
                            aria-expanded={expanded}
                            onClick={() => toggleGroup(group)}
                          >
                            <span className="data-table-group-toggle-mark">{expanded ? "-" : "+"}</span>
                            <span className="data-table-group-copy">
                              <span className="data-table-group-title">{group.country} / {group.brand} / {group.model}</span>
                              <span className="data-table-group-subtitle">
                                {group.items.length} trims · {summarizeCurrentPriceTrims(group.items)}
                              </span>
                            </span>
                          </button>
                          <div className="data-table-group-meta">
                            <span className="data-table-group-pill">MSRP {formatCurrentPriceRange(group.items)}</span>
                            <span className="data-table-group-pill">Updated {formatCurrentPriceDate(latestUpdatedAt)}</span>
                          </div>
                        </div>
                      </td>
                    </tr>
                    {expanded && group.items.map((p) => {
                      const currentMsrpValue = resolveCurrentMsrpValue(p);
                      const lastPriceChangeAtUtc = resolveLastPriceChangeAtUtc(p);
                      const updatedAtUtc = resolveUpdatedAtUtc(p);

                      return (
                        <tr key={p.id} className={selectedPrice?.id === p.id ? "is-selected" : ""}>
                          <td>{p.country}</td>
                          <td><strong>{p.brand}</strong></td>
                          <td>{p.jatoModel}</td>
                          <td>{p.jatoTrim}</td>
                          <td>{p.jatoPowertrain || "—"}</td>
                          <td>{p.officialModel}</td>
                          <td className="text-right"><strong>{formatCurrentPriceNumber(currentMsrpValue)}</strong></td>
                          <td>{p.currency}</td>
                          <td>
                            <span className={`badge ${getCurrentPriceMatchStatusBadgeClass(p.matchStatus)}`}>
                              {getCurrentPriceMatchStatusLabel(p.matchStatus)}
                            </span>
                          </td>
                          <td className="review-table-meta-cell">
                            {p.sourceUrl ? (
                              <a href={p.sourceUrl} target="_blank" rel="noreferrer" className="review-table-link">
                                {formatSourceLink(p.sourceUrl)}
                              </a>
                            ) : (
                              <span className="review-table-muted">No source URL</span>
                            )}
                          </td>
                          <td>{formatCurrentPriceDate(lastPriceChangeAtUtc)}</td>
                          <td>{formatCurrentPriceDate(updatedAtUtc)}</td>
                          <td><button type="button" className="btn btn-xs btn-ghost" onClick={() => openPriceDetail(p)}>详情</button></td>
                        </tr>
                      );
                    })}
                  </Fragment>
                );
              })}
              {prices.length === 0 && !loading && (
                <tr><td colSpan={13}><div className="crud-empty-state">暂无价格记录，请先执行 Materialize</div></td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Detail Drawer ───────────────────────── */}
      {selectedPrice && (
        <div className={`card crud-card admin-detail-drawer review-detail-dock${detailCollapsed ? " is-collapsed" : ""}`}>
          <div className="detail-section-head review-detail-dock-head">
            <div>
              <div className="card-title">Price Detail</div>
              <p className="section-note">{selectedPrice.brand} {selectedPrice.jatoModel} — {selectedPrice.country}</p>
            </div>
            <div className="review-detail-dock-actions">
              {selectedPrice.sourceUrl && (
                <a href={selectedPrice.sourceUrl} target="_blank" rel="noreferrer" className="btn btn-sm btn-ghost">
                  Source URL
                </a>
              )}
              <button
                type="button"
                className="btn btn-sm btn-danger"
                disabled={remapping}
                onClick={() => {
                  void handleReturnToReview();
                }}
              >
                {remapping ? "打回中…" : "打回 Review"}
              </button>
              <button
                type="button"
                className="btn btn-sm btn-secondary"
                onClick={() => setDetailCollapsed((current) => !current)}
              >
                {detailCollapsed ? "展开" : "收起"}
              </button>
              <button type="button" className="btn btn-sm btn-ghost" onClick={() => { setSelectedPrice(null); setDetailCollapsed(false); }}>关闭</button>
            </div>
          </div>

          {!detailCollapsed && (
            <div className="review-detail-dock-body">
              <div className="admin-detail-grid">
            <div className="admin-detail-item">
              <span className="admin-detail-label">Country</span>
              <span className="admin-detail-value">{selectedPrice.country}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Brand</span>
              <span className="admin-detail-value">{selectedPrice.brand}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">JATO Model</span>
              <span className="admin-detail-value">{selectedPrice.jatoModel}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">JATO Trim</span>
              <span className="admin-detail-value">{selectedPrice.jatoTrim}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Official Model</span>
              <span className="admin-detail-value">{selectedPrice.officialModel}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Official Trim</span>
              <span className="admin-detail-value">{selectedPrice.officialTrim || "—"}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Official Edition</span>
              <span className="admin-detail-value">{selectedPrice.officialEdition || "—"}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">JATO Powertrain</span>
              <span className="admin-detail-value">{selectedPrice.jatoPowertrain || "—"}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Official Powertrain</span>
              <span className="admin-detail-value">{selectedPrice.officialPowertrain || "—"}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">MSRP (EUR)</span>
              <span className="admin-detail-value"><strong>{formatCurrentPriceNumber(resolveCurrentMsrpValue(selectedPrice))}</strong></span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Source MSRP</span>
              <span className="admin-detail-value">{selectedPrice.sourceMsrpValue != null ? `${formatCurrentPriceNumber(selectedPrice.sourceMsrpValue)} ${selectedPrice.sourceCurrency || ""}` : "—"}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Currency</span>
              <span className="admin-detail-value">{selectedPrice.currency}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">FX Rate → EUR</span>
              <span className="admin-detail-value">{selectedPrice.fxRateToEur != null ? `${selectedPrice.fxRateToEur.toFixed(6)} (${selectedPrice.fxSource || "—"})` : "—"}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">FX Date</span>
              <span className="admin-detail-value">{selectedPrice.fxRateAsOfDate || "—"}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Tax Included</span>
              <span className="admin-detail-value">{selectedPrice.taxIncluded ? "Yes" : "No"}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Match Confidence</span>
              <span className="admin-detail-value">{(selectedPrice.matchConfidence * 100).toFixed(1)}%</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Match Status</span>
              <span className={`badge ${getCurrentPriceMatchStatusBadgeClass(selectedPrice.matchStatus)}`}>
                {getCurrentPriceMatchStatusLabel(selectedPrice.matchStatus)}
              </span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Source URL</span>
              <span className="admin-detail-value">
                {selectedPrice.sourceUrl ? <a href={selectedPrice.sourceUrl} target="_blank" rel="noreferrer">打开来源</a> : "—"}
              </span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Last Price Change</span>
              <span className="admin-detail-value">{formatCurrentPriceDate(resolveLastPriceChangeAtUtc(selectedPrice))}</span>
            </div>
            <div className="admin-detail-item">
              <span className="admin-detail-label">Updated</span>
              <span className="admin-detail-value">{formatCurrentPriceDate(resolveUpdatedAtUtc(selectedPrice))}</span>
            </div>
              </div>

              <div className="detail-section-head">
                <div>
                  <div className="card-title">Price History</div>
                  <p className="section-note">按 valid_from / valid_to 压缩后的价格时段；同价时刷新最近确认时间，不重复开新段。</p>
                </div>
              </div>

              {priceHistoryError && <div className="alert alert-error">{priceHistoryError}</div>}

              {priceHistoryLoading ? (
                <LoadingSurface mode="inline" label="正在加载价格历史" detail="同步价格时段" kicker="MSRP" />
              ) : (
                <div className="price-history-visual-stack">
                  <div className="price-history-visual-card">
                    <PriceHistoryTimeline entries={priceHistory} />
                  </div>
                  <div className="table-wrapper">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Validity Window</th>
                          <th>MSRP</th>
                          <th>Source MSRP</th>
                          <th>Started By</th>
                          <th>Ended By</th>
                        </tr>
                      </thead>
                      <tbody>
                        {priceHistory.map((entry) => (
                          <tr key={entry.id}>
                            <td>{formatPriceHistoryWindow(entry)}</td>
                            <td>{formatCurrentPriceNumber(entry.msrpValue)} {entry.currency}</td>
                            <td>{formatCurrentPriceNumber(entry.sourceMsrpValue)} {entry.sourceCurrency}</td>
                            <td>{entry.startedByObservationId}</td>
                            <td>{entry.endedByObservationId || "当前"}</td>
                          </tr>
                        ))}
                        {priceHistory.length === 0 && (
                          <tr>
                            <td colSpan={5}>
                              <div className="crud-empty-state">暂无价格历史记录</div>
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </section>
  );
}
