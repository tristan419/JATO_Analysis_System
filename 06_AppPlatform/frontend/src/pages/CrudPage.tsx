import { ChangeEvent, FormEvent, useEffect, useState } from "react";

import { api } from "../api/client";
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

  async function refresh() {
    setLoading(true);
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

  return (
    <section>
      <div className="page-header">
        <h1>数据管理</h1>
        <p>CRUD 实体管理 · 共 {total} 条记录</p>
      </div>

      {error && <div className="alert alert-error">{error}</div>}

      {/* ── toolbar ────────────────────────────────── */}
      <div className="filter-bar">
        <div className="filter-group">
          <label>搜索</label>
          <input
            type="search"
            value={query}
            onChange={(e: ChangeEvent<HTMLInputElement>) => { setQuery(e.target.value); setPage(1); }}
            placeholder="code / name / status / notes"
            style={{ minWidth: 200 }}
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

      {/* ── create form ────────────────────────────── */}
      <form onSubmit={submit} className="card" style={{ marginBottom: 20, display: "flex", gap: 12, alignItems: "flex-end", flexWrap: "wrap" }}>
        <div className="filter-group">
          <label>Code</label>
          <input type="text" value={code} onChange={(e: ChangeEvent<HTMLInputElement>) => setCode(e.target.value)} required />
        </div>
        <div className="filter-group">
          <label>Name</label>
          <input type="text" value={name} onChange={(e: ChangeEvent<HTMLInputElement>) => setName(e.target.value)} required />
        </div>
        <button type="submit" className="btn btn-primary">新建</button>
      </form>

      {/* ── table ──────────────────────────────────── */}
      {loading && <div className="loading-overlay"><span className="spinner" /> 加载中…</div>}
      <div className="table-wrapper">
        <table className="data-table">
          <thead>
            <tr>
              <th>Code</th>
              <th>Name</th>
              <th>Status</th>
              <th>Notes</th>
              <th style={{ width: 80 }}>操作</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td><strong>{item.code}</strong></td>
                <td>{item.name}</td>
                <td><span className={`badge ${item.status === "active" ? "badge-active" : "badge-inactive"}`}>{item.status}</span></td>
                <td>{item.notes}</td>
                <td><button className="btn btn-sm btn-danger" onClick={() => remove(item.id)}>删除</button></td>
              </tr>
            ))}
            {items.length === 0 && !loading && (
              <tr><td colSpan={5} style={{ textAlign: "center", color: "var(--c-text-muted)", padding: 24 }}>暂无数据</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* ── pagination ─────────────────────────────── */}
      <div className="pagination">
        <span>第 {page} / {totalPages} 页</span>
        <div className="btn-group">
          <button className="btn btn-sm btn-secondary" disabled={page <= 1} onClick={() => setPage((p) => p - 1)}>上一页</button>
          <button className="btn btn-sm btn-secondary" disabled={page >= totalPages} onClick={() => setPage((p) => p + 1)}>下一页</button>
        </div>
      </div>
    </section>
  );
}
