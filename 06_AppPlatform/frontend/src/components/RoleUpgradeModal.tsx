import { useState } from "react";
import { api } from "../api/client";

interface Props {
  currentRole: string;
  onClose: () => void;
}

export function RoleUpgradeModal({ currentRole, onClose }: Props) {
  const [role, setRole] = useState(currentRole === "viewer" ? "editor" : "admin");
  const [reason, setReason] = useState("");
  const [status, setStatus] = useState<"idle" | "loading" | "done" | "error">("idle");
  const [msg, setMsg] = useState("");

  const submit = async () => {
    setStatus("loading");
    try {
      const res = await api.requestRoleUpgrade({ requested_role: role, reason });
      setMsg(`申请已提交。当前状态: ${(res as Record<string,unknown>).status}`);
      setStatus("done");
    } catch (err) {
      setMsg(err instanceof Error ? err.message : "提交失败");
      setStatus("error");
    }
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()} style={{ maxWidth: 400 }}>
        <div className="modal-header">
          <h3>申请权限升级</h3>
          <button className="btn btn-sm" onClick={onClose}>×</button>
        </div>
        <div className="modal-body">
          {status === "done" ? (
            <div className="alert alert-success">{msg}</div>
          ) : (
            <>
              <div className="form-group">
                <label>当前角色: <strong>{currentRole}</strong></label>
              </div>
              <div className="form-group">
                <label>申请角色</label>
                <select className="input" value={role} onChange={(e) => setRole(e.target.value)}>
                  {currentRole === "viewer" && <option value="editor">Editor（编辑者）</option>}
                  <option value="admin">Admin（管理员）</option>
                </select>
              </div>
              <div className="form-group">
                <label>申请理由</label>
                <textarea className="input" rows={3} value={reason} onChange={(e) => setReason(e.target.value)} placeholder="请简述申请理由..." />
              </div>
              {status === "error" && <div className="alert alert-error">{msg}</div>}
              <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
                <button className="btn btn-primary" onClick={submit} disabled={status === "loading"}>
                  {status === "loading" ? "提交中..." : "提交申请"}
                </button>
                <button className="btn btn-secondary" onClick={onClose}>取消</button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

export function AdminRequestsPanel() {
  const [requests, setRequests] = useState<Record<string,unknown>[]>([]);
  const [loaded, setLoaded] = useState(false);

  const load = async () => {
    try {
      const res = await api.listRoleUpgradeRequests({ status: "pending" });
      setRequests((res as Record<string,unknown>).requests as Record<string,unknown>[] || []);
      setLoaded(true);
    } catch { /* ignore */ }
  };

  const review = async (id: string, action: "approved" | "rejected") => {
    try {
      await api.reviewRoleUpgradeRequest(id, { status: action });
      load();
    } catch { /* ignore */ }
  };

  if (!loaded) {
    return <button className="btn btn-sm btn-secondary" onClick={load}>查看升级申请</button>;
  }

  return (
    <div style={{ marginTop: 8 }}>
      <button className="btn btn-sm btn-secondary" onClick={load} style={{ marginBottom: 8 }}>刷新</button>
      {requests.length === 0 ? (
        <span className="text-muted">暂无待处理申请</span>
      ) : (
        requests.map((r) => (
          <div key={r.requestId as string} className="trim-card" style={{ marginTop: 4, padding: 8 }}>
            <span><strong>{r.username as string}</strong>: {r.currentRole as string} → {r.requestedRole as string}</span>
            <span className="text-muted" style={{ marginLeft: 8 }}>{r.reason as string || "无理由"}</span>
            <div style={{ marginTop: 4 }}>
              <button className="btn btn-sm btn-primary" onClick={() => review(r.requestId as string, "approved")}>批准</button>
              <button className="btn btn-sm btn-secondary" style={{ marginLeft: 4 }} onClick={() => review(r.requestId as string, "rejected")}>拒绝</button>
            </div>
          </div>
        ))
      )}
    </div>
  );
}
