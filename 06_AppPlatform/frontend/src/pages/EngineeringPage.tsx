import { ChangeEvent, useEffect, useState } from "react";

import { api } from "../api/client";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { LoadingSurface } from "../components/LoadingSurface";
import type { ConfigImportBatch, ConfigVariant } from "../types";

export function EngineeringPage() {
  /* ── state ──────────────────────────────────────── */
  const [batches, setBatches] = useState<ConfigImportBatch[]>([]);
  const [variants, setVariants] = useState<ConfigVariant[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [heroCollapsed, setHeroCollapsed] = useState(false);

  /* filters */
  const [statusFilter, setStatusFilter] = useState("");
  const [projectFilter, setProjectFilter] = useState("");
  const [modelFilter, setModelFilter] = useState("");
  const [activeTab, setActiveTab] = useState<"imports" | "variants">("imports");

  /* detail drawer */
  const [selectedBatchId, setSelectedBatchId] = useState<string | null>(null);
  const [pageData, setPageData] = useState<Record<string, unknown> | null>(null);
  const [pageDataLoading, setPageDataLoading] = useState(false);

  /* ── fetchers ───────────────────────────────────── */
  async function refreshBatches() {
    setLoading(true);
    setError("");
    try {
      const res = await api.listImportBatches({
        import_status: statusFilter || undefined,
        project_id: projectFilter || undefined,
        limit: 100,
      });
      setBatches(res.items);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  async function refreshVariants() {
    setLoading(true);
    setError("");
    try {
      const res = await api.listVariants({
        project_id: projectFilter || undefined,
        model: modelFilter || undefined,
        limit: 200,
      });
      setVariants(res.items);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  async function openPageData(batchId: string) {
    setSelectedBatchId(batchId);
    setPageDataLoading(true);
    try {
      const res = await api.getImportPageData(batchId);
      setPageData(res.item);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setPageDataLoading(false);
    }
  }

  useEffect(() => {
    if (activeTab === "imports") refreshBatches();
    else refreshVariants();
  }, [activeTab, statusFilter, projectFilter, modelFilter]);

  /* ── derived ────────────────────────────────────── */
  const completedCount = batches.filter((b) => b.importStatus === "completed").length;
  const failedCount = batches.filter((b) => b.importStatus === "failed").length;
  const totalImported = batches.reduce((s, b) => s + (b.importedRows ?? 0), 0);

  /* ── render ─────────────────────────────────────── */
  return (
    <section className="crud-shell">
      <CollapsibleDeckHero
        collapsed={heroCollapsed}
        onToggle={() => setHeroCollapsed((c) => !c)}
        expandedLabel="展开工程导入概览"
        collapsedLabel="收起工程导入概览"
        expandedTitle="Expand engineering import overview"
        collapsedTitle="Collapse engineering import overview"
        className="header-card dashboard-hero crud-hero"
        head={(
          <>
            <div className="dashboard-hero-copy crud-hero-copy">
              <span className="page-kicker">04 / Engineering</span>
              <h1>Config Import Control</h1>
              
              <div className="dashboard-hero-inline-summary">
                <span className="selection-ribbon-label">Active tab</span>
                <span className="selection-ribbon-value">
                  {activeTab === "imports" ? "Import Batches" : "Config Variants"}
                </span>
              </div>
            </div>
            <div className="dashboard-hero-actions crud-hero-actions">
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Total batches</span>
                <strong className="hero-meta-value">{batches.length}</strong>
                <span className="hero-meta-subvalue">{completedCount} completed</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Imported rows</span>
                <strong className="hero-meta-value">{totalImported.toLocaleString()}</strong>
                <span className="hero-meta-subvalue">across all batches</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Failed</span>
                <strong className="hero-meta-value">{failedCount}</strong>
                <span className="hero-meta-subvalue">需要人工检查</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Data state</span>
                <strong className="hero-meta-value">{loading ? "SYNC" : "READY"}</strong>
                <span className="hero-meta-subvalue">过滤联动</span>
                {loading && <span className="hero-meta-loader">SYNCING VIEW</span>}
              </div>
            </div>
          </>
        )}
        body={(
          <div className="dashboard-hero-rail">
            <div className="dashboard-hero-chip-row">
              <span className="dashboard-hero-chip">{statusFilter || "All statuses"}</span>
              <span className="dashboard-hero-chip">{projectFilter ? `Project ${projectFilter.slice(0, 8)}…` : "All projects"}</span>
            </div>
            <div className="dashboard-hero-rail-actions">
              <button type="button" className="btn btn-sm btn-ghost" onClick={() => { setStatusFilter(""); setProjectFilter(""); setModelFilter(""); }}>重置</button>
              <button type="button" className="btn btn-sm btn-secondary" onClick={() => activeTab === "imports" ? refreshBatches() : refreshVariants()}>刷新</button>
            </div>
          </div>
        )}
      />

      {error && <div className="alert alert-error">{error}</div>}

      {/* ── Tab bar ─────────────────────────────── */}
      <div className="admin-tabs">
        <button
          type="button"
          className={`admin-tab${activeTab === "imports" ? " is-active" : ""}`}
          onClick={() => setActiveTab("imports")}
        >Import Batches</button>
        <button
          type="button"
          className={`admin-tab${activeTab === "variants" ? " is-active" : ""}`}
          onClick={() => setActiveTab("variants")}
        >Config Variants</button>
      </div>

      {/* ── Filters ─────────────────────────────── */}
      <div className="card crud-card">
        <div className="detail-section-head">
          <div>
            <div className="card-title">filter</div>
          </div>
        </div>
        <div className="crud-toolbar-grid">
          {activeTab === "imports" && (
            <div className="filter-group">
              <label>Import Status</label>
              <select value={statusFilter} onChange={(e: ChangeEvent<HTMLSelectElement>) => setStatusFilter(e.target.value)}>
                <option value="">All</option>
                <option value="completed">Completed</option>
                <option value="running">Running</option>
                <option value="failed">Failed</option>
                <option value="pending">Pending</option>
              </select>
            </div>
          )}
          <div className="filter-group">
            <label>Project ID</label>
            <input
              type="text"
              value={projectFilter}
              onChange={(e: ChangeEvent<HTMLInputElement>) => setProjectFilter(e.target.value)}
              placeholder="UUID prefix..."
            />
          </div>
          {activeTab === "variants" && (
            <div className="filter-group">
              <label>Model</label>
              <input
                type="text"
                value={modelFilter}
                onChange={(e: ChangeEvent<HTMLInputElement>) => setModelFilter(e.target.value)}
                placeholder="e.g. X5"
              />
            </div>
          )}
        </div>
      </div>

      {/* ── Import Batches Table ────────────────── */}
      {activeTab === "imports" && (
        <div className="card crud-table-card">
          <div className="detail-section-head">
            <div>
              <div className="card-title">Import Batches</div>
              <p className="section-note">点击行查看 page-data 导入明细</p>
            </div>
            <div className="table-status-chip">
              <span>Count</span>
              <strong>{batches.length}</strong>
            </div>
          </div>

          {loading && (
            <LoadingSurface mode="overlay" label="正在加载" detail="同步导入批次列表" kicker="Engineering" />
          )}

          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Source File</th>
                  <th>Sheet</th>
                  <th>Replace Mode</th>
                  <th>Imported</th>
                  <th>Skipped</th>
                  <th>Errors</th>
                  <th>Created</th>
                  <th style={{ width: 96 }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {batches.map((b) => (
                  <tr key={b.id} className={selectedBatchId === b.id ? "is-selected" : ""}>
                    <td>
                      <span className={`badge ${b.importStatus === "completed" ? "badge-active" : b.importStatus === "failed" ? "badge-danger" : "badge-inactive"}`}>
                        {b.importStatus}
                      </span>
                    </td>
                    <td title={b.sourceFilePath}>{b.sourceFilePath.split("/").pop()}</td>
                    <td>{b.sheetName}</td>
                    <td>{b.replaceMode}</td>
                    <td><strong>{b.importedRows}</strong></td>
                    <td>{b.skippedRows}</td>
                    <td>{b.errorCount > 0 ? <span className="badge badge-danger">{b.errorCount}</span> : 0}</td>
                    <td>{new Date(b.createdAt).toLocaleDateString()}</td>
                    <td>
                      <div className="crud-row-actions">
                        <button type="button" className="btn btn-sm btn-secondary" onClick={() => openPageData(b.id)}>详情</button>
                      </div>
                    </td>
                  </tr>
                ))}
                {batches.length === 0 && !loading && (
                  <tr><td colSpan={9}><div className="crud-empty-state">暂无导入批次</div></td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Variants Table ──────────────────────── */}
      {activeTab === "variants" && (
        <div className="card crud-table-card">
          <div className="detail-section-head">
            <div>
              <div className="card-title">Config Variants</div>
              <p className="section-note">导入批次关联的变体配置记录</p>
            </div>
            <div className="table-status-chip">
              <span>Count</span>
              <strong>{variants.length}</strong>
            </div>
          </div>

          {loading && (
            <LoadingSurface mode="overlay" label="正在加载" detail="同步变体记录" kicker="Variants" />
          )}

          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Model</th>
                  <th>Trim</th>
                  <th>Market</th>
                  <th>Active</th>
                  <th>Batch</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {variants.map((v) => (
                  <tr key={v.id}>
                    <td><strong>{v.model}</strong></td>
                    <td>{v.trim}</td>
                    <td>{v.marketCountry}</td>
                    <td>
                      <span className={`badge ${v.isActive ? "badge-active" : "badge-inactive"}`}>
                        {v.isActive ? "Active" : "Inactive"}
                      </span>
                    </td>
                    <td className="text-mono">{v.configImportBatchId.slice(0, 8)}…</td>
                    <td>{new Date(v.createdAt).toLocaleDateString()}</td>
                  </tr>
                ))}
                {variants.length === 0 && !loading && (
                  <tr><td colSpan={6}><div className="crud-empty-state">暂无变体记录</div></td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Page Data Drawer ────────────────────── */}
      {selectedBatchId && (
        <div className="card crud-card admin-detail-drawer">
          <div className="detail-section-head">
            <div>
              <div className="card-title">Import Page Data</div>
              <p className="section-note">Batch {selectedBatchId.slice(0, 8)}…</p>
            </div>
            <button type="button" className="btn btn-sm btn-ghost" onClick={() => { setSelectedBatchId(null); setPageData(null); }}>关闭</button>
          </div>
          {pageDataLoading && (
            <LoadingSurface mode="inline" label="正在加载 page-data" detail="" kicker="Detail" />
          )}
          {pageData && !pageDataLoading && (
            <div className="admin-json-preview">
              <pre>{JSON.stringify(pageData, null, 2)}</pre>
            </div>
          )}
        </div>
      )}
    </section>
  );
}
