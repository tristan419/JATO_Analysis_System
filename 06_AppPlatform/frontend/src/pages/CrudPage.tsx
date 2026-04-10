import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from "react";

import { api } from "../api/client";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { LoadingSurface } from "../components/LoadingSurface";
import type { CrudItem } from "../types";

export function CrudPage() {
  const [items, setItems] = useState<CrudItem[]>([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [total, setTotal] = useState(0);
  const [sortBy, setSortBy] = useState<"code" | "name" | "status" | "created" | "updated">("code");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("asc");
  const [query, setQuery] = useState("");
  const [code, setCode] = useState("");
  const [name, setName] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [heroCollapsed, setHeroCollapsed] = useState(false);

  async function refresh() {
    setLoading(true);
    setError("");
    try {
      const res = await api.listItems({ page, page_size: pageSize, sort_by: sortBy, sort_order: sortOrder, query });
      setItems(res.items);
      setTotal(res.total);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { refresh(); }, [page, pageSize, sortBy, sortOrder, query]);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setError("");
    try {
      await api.createItem({ code, name, status: "active", notes: "" });
      setCode("");
      setName("");
      setPage(1);
      await refresh();
    } catch (err) { setError((err as Error).message); }
  }

  async function remove(id: string) {
    try { await api.deleteItem(id); await refresh(); }
    catch (err) { setError((err as Error).message); }
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const activeCount = useMemo(() => items.filter((item) => item.status === "active").length, [items]);
  const visibleStart = total === 0 ? 0 : (page - 1) * pageSize + 1;
  const visibleEnd = Math.min(page * pageSize, total);
  const querySummary = query.trim() ? `搜索 ${query.trim()}` : "当前未设置搜索过滤";
  const windowSummary = total === 0 ? "当前无可见记录" : `当前显示 ${visibleStart}-${visibleEnd} / ${total}`;
  const sortSummary = `${sortBy.toUpperCase()} / ${sortOrder.toUpperCase()}`;
  const pageSizeSummary = `${pageSize} rows per page`;

  function resetView() {
    setQuery("");
    setSortBy("code");
    setSortOrder("asc");
    setPageSize(20);
    setPage(1);
  }

  function clearForm() {
    setCode("");
    setName("");
  }

  return (
    <section className="crud-shell">
      <CollapsibleDeckHero
        collapsed={heroCollapsed}
        onToggle={() => setHeroCollapsed((current) => !current)}
        expandedLabel="展开数据管理概览"
        collapsedLabel="收起数据管理概览"
        expandedTitle="Expand data control overview"
        collapsedTitle="Collapse data control overview"
        className="header-card dashboard-hero crud-hero"
        head={(
          <>
            <div className="dashboard-hero-copy crud-hero-copy">
              <span className="page-kicker">03 / Data Control</span>
              <h1>CRUD Control Deck</h1>
              <p>把基础实体管理、搜索、分页和列表操作统一进 Dashboard 的 hero 节奏里，避免这个工作视图继续保留旧壳层语言。</p>
              <div className="dashboard-hero-inline-summary">
                <span className="selection-ribbon-label">Current scope</span>
                <span className="selection-ribbon-value">{querySummary}</span>
              </div>
            </div>

            <div className="dashboard-hero-actions crud-hero-actions">
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Total records</span>
                <strong className="hero-meta-value">{total.toLocaleString()}</strong>
                <span className="hero-meta-subvalue">{pageSizeSummary}</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Active on page</span>
                <strong className="hero-meta-value">{activeCount.toLocaleString()}</strong>
                <span className="hero-meta-subvalue">当前页激活状态记录</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Window</span>
                <strong className="hero-meta-value">{total === 0 ? "0" : `${visibleStart}-${visibleEnd}`}</strong>
                <span className="hero-meta-subvalue">{windowSummary}</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Data state</span>
                <strong className="hero-meta-value">{loading ? "SYNC" : "READY"}</strong>
                <span className="hero-meta-subvalue">排序、分页和查询已联动</span>
                {loading && <span className="hero-meta-loader">SYNCING CRUD VIEW</span>}
              </div>
            </div>
          </>
        )}
        body={(
          <div className="dashboard-hero-rail">
            <div className="dashboard-hero-chip-row">
              <span className="dashboard-hero-chip">{querySummary}</span>
              <span className="dashboard-hero-chip">Sort {sortSummary}</span>
              <span className="dashboard-hero-chip">{pageSizeSummary}</span>
              <span className="dashboard-hero-chip">{windowSummary}</span>
            </div>
            <div className="dashboard-hero-rail-actions">
              <button type="button" className="btn btn-sm btn-ghost" onClick={resetView}>重置视图</button>
              <button type="button" className="btn btn-sm btn-secondary" onClick={() => void refresh()}>刷新列表</button>
            </div>
          </div>
        )}
      />

      {error && <div className="alert alert-error">{error}</div>}

      <div className="crud-grid">
        <div className="card crud-card">
          <div className="detail-section-head">
            <div>
              <div className="card-title">检索与排序</div>
              <p className="section-note">保留简单 CRUD 逻辑，但把筛选和排序入口做成更接近控制台的信息面板。</p>
            </div>
            <div className="table-status-chip">
              <span>Pages</span>
              <strong>{totalPages}</strong>
            </div>
          </div>

          <div className="crud-toolbar-grid">
            <div className="filter-group">
              <label>搜索</label>
              <input
                type="search"
                value={query}
                onChange={(e: ChangeEvent<HTMLInputElement>) => { setQuery(e.target.value); setPage(1); }}
                placeholder="code / name / status / notes"
              />
            </div>
            <div className="filter-group">
              <label>排序</label>
              <select value={sortBy} onChange={(e: ChangeEvent<HTMLSelectElement>) => setSortBy(e.target.value as typeof sortBy)}>
                <option value="code">Code</option>
                <option value="name">Name</option>
                <option value="status">Status</option>
                <option value="created">Created</option>
                <option value="updated">Updated</option>
              </select>
            </div>
            <div className="filter-group">
              <label>方向</label>
              <select value={sortOrder} onChange={(e: ChangeEvent<HTMLSelectElement>) => setSortOrder(e.target.value as "asc" | "desc")}>
                <option value="asc">升序</option>
                <option value="desc">降序</option>
              </select>
            </div>
            <div className="filter-group">
              <label>每页</label>
              <select value={pageSize} onChange={(e: ChangeEvent<HTMLSelectElement>) => { setPageSize(Number(e.target.value)); setPage(1); }}>
                <option value={10}>10</option>
                <option value={20}>20</option>
                <option value={50}>50</option>
                <option value={100}>100</option>
              </select>
            </div>
          </div>

          <div className="crud-toolbar-footer">
            <div className="crud-inline-status">
              <span className="selection-ribbon-label">Window</span>
              <span className="selection-ribbon-value">{windowSummary}</span>
            </div>
          </div>
        </div>

        <form onSubmit={submit} className="card crud-card crud-form-card">
          <div className="detail-section-head">
            <div>
              <div className="card-title">新建实体</div>
              <p className="section-note">保持输入项最少，只暴露当前真实需要维护的字段。</p>
            </div>
            <div className="table-status-chip">
              <span>Queue</span>
              <strong>{items.length}</strong>
            </div>
          </div>
          <div className="crud-form-grid">
            <div className="filter-group">
              <label>Code</label>
              <input type="text" value={code} onChange={(e: ChangeEvent<HTMLInputElement>) => setCode(e.target.value)} required />
            </div>
            <div className="filter-group">
              <label>Name</label>
              <input type="text" value={name} onChange={(e: ChangeEvent<HTMLInputElement>) => setName(e.target.value)} required />
            </div>
            <div className="crud-form-actions">
              <button type="submit" className="btn btn-primary">新建</button>
              <button type="button" className="btn btn-ghost" onClick={clearForm}>清空</button>
            </div>
          </div>
        </form>
      </div>

      <div className="card crud-table-card">
        <div className="detail-section-head">
          <div>
            <div className="card-title">实体列表</div>
            <p className="section-note">当前显示 {visibleStart}-{visibleEnd} / {total}，删除操作保持最小暴露，不新增额外交互层。</p>
          </div>
          <div className="table-status-chip">
            <span>Page</span>
            <strong>{page}</strong>
          </div>
        </div>

        {loading && (
          <LoadingSurface
            mode="overlay"
            label="正在加载实体列表"
            detail="同步当前页、排序条件和检索窗口"
            kicker="CRUD"
          />
        )}

        <div className="table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Code</th>
                <th>Name</th>
                <th>Status</th>
                <th>Notes</th>
                <th style={{ width: 96 }}>操作</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.id}>
                  <td><strong className="crud-code">{item.code}</strong></td>
                  <td>{item.name}</td>
                  <td><span className={`badge ${item.status === "active" ? "badge-active" : "badge-inactive"}`}>{item.status}</span></td>
                  <td>{item.notes || "-"}</td>
                  <td>
                    <div className="crud-row-actions">
                      <button type="button" className="btn btn-sm btn-danger" onClick={() => remove(item.id)}>删除</button>
                    </div>
                  </td>
                </tr>
              ))}
              {items.length === 0 && !loading && (
                <tr>
                  <td colSpan={5}>
                    <div className="crud-empty-state">暂无数据</div>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="pagination">
          <span className="pagination-status">第 {page} / {totalPages} 页</span>
          <div className="btn-group">
            <button type="button" className="btn btn-sm btn-secondary" disabled={page <= 1} onClick={() => setPage((p) => p - 1)}>上一页</button>
            <button type="button" className="btn btn-sm btn-secondary" disabled={page >= totalPages} onClick={() => setPage((p) => p + 1)}>下一页</button>
          </div>
        </div>
      </div>
    </section>
  );
}
