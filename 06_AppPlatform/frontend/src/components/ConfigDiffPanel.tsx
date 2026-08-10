import React, { useState, useEffect, useCallback } from "react";
import { api } from "../api/client";
import type { AuditLogItem } from "../types/engineeringConfig";

export function ConfigDiffPanel() {
  const [logs, setLogs] = useState<AuditLogItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reverting, setReverting] = useState<string | null>(null);

  const loadLogs = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const data = await api.listEngineeringConfigAuditLog({ limit: 100 });
      setLogs((data.items || []) as unknown as AuditLogItem[]);
    } catch (err) { setError(err instanceof Error ? err.message : "加载失败"); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { loadLogs(); }, []);

  const handleRevert = useCallback(async (entry: AuditLogItem) => {
    if (entry.entityType !== "trim_feature_value" || !entry.oldValue) return;
    setReverting(entry.auditId);
    try {
      await api.updateEngineeringConfigFeatureValue(entry.entityId, {
        raw_value: entry.oldValue, expected_version: 1,
        comment: `Rollback from audit ${entry.auditId}`,
      });
      await loadLogs();
    } catch (err) { setError(err instanceof Error ? err.message : "回滚失败"); }
    finally { setReverting(null); }
  }, [loadLogs]);

  const fmt = (iso: string) => new Date(iso).toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });

  return (
    <div className="diff-panel">
      <div className="diff-toolbar"><button className="btn btn-secondary" onClick={loadLogs} disabled={loading}>{loading ? "加载中..." : "刷新"}</button></div>
      {error && <div className="alert alert-error" style={{ margin: "8px 0" }}>{error}<button onClick={() => setError(null)} style={{ marginLeft: 8 }}>×</button></div>}
      <div className="diff-list">
        {logs.length === 0 && !loading && <p className="text-muted">暂无变更记录</p>}
        {logs.map((entry) => (
          <div key={entry.auditId} className="diff-entry">
            <div className="diff-entry-header">
              <span className="diff-time">{fmt(entry.changedAtUtc)}</span>
              <span className="diff-user">{entry.changedBy || "unknown"}</span>
            </div>
            <div className="diff-change">
              {entry.oldValue !== null ? <><span className="diff-old">- {entry.oldValue}</span><span className="diff-arrow">→</span></> : <span className="diff-new-label">+ 新建</span>}
              <span className="diff-new">+ {entry.newValue || "(空)"}</span>
            </div>
            {entry.comment && <div className="diff-comment">{entry.comment}</div>}
            {entry.entityType === "trim_feature_value" && entry.oldValue && (
              <button className="btn btn-sm btn-warning" onClick={() => handleRevert(entry)} disabled={reverting === entry.auditId}>{reverting === entry.auditId ? "回滚中..." : "回滚"}</button>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
