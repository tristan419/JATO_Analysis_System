import { useState } from "react";
import { api } from "../api/client";
import { ORDERING_BRANDS, type User } from "../contexts/AuthContext";

interface Props {
  currentRole: string;
  onClose: () => void;
  kind?: "role" | "brands";
}

export function RoleUpgradeModal({ currentRole, onClose, kind = "role" }: Props) {
  const [role, setRole] = useState("editor");
  const [reason, setReason] = useState("");
  const [brands, setBrands] = useState<string[]>([]);
  const [status, setStatus] = useState<"idle" | "loading" | "done" | "error">("idle");
  const [msg, setMsg] = useState("");

  const submit = async () => {
    setStatus("loading");
    try {
      const res = await api.requestRoleUpgrade({ requested_role: role, reason,
        ...(kind === "brands" ? { requestedBrands: brands } : {}) });
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
          <h3>{kind === "brands" ? "Request brand access / 申请品牌权限" : "申请权限升级"}</h3>
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
                {kind === "brands" ? <>
                  <label>Brands / 申请品牌</label>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 12 }}>
                    {ORDERING_BRANDS.map((brand) => <label key={brand}>
                      <input type="checkbox" checked={brands.includes(brand)} onChange={() => setBrands(
                        brands.includes(brand) ? brands.filter((value) => value !== brand) : [...brands, brand],
                      )} /> {brand}
                    </label>)}
                  </div>
                  <p className="text-muted">Admin approval assigns brands, not a new role. / 批准仅分配品牌，不提升角色。</p>
                </> : <>
                <label>申请角色</label>
                <select className="input" value={role} onChange={(e) => setRole(e.target.value)}>
                  {currentRole === "viewer" && <option value="editor">Editor（编辑者）</option>}
                </select>
                <p style={{ fontSize: 12, color: "#64748b", marginTop: 6 }}>
                  Admin 权限只能由现有管理员在 Access Control 中手动分配。
                </p>
                </>}
              </div>
              <div className="form-group">
                <label>申请理由</label>
                <textarea className="input" rows={3} value={reason} onChange={(e) => setReason(e.target.value)} placeholder="请简述申请理由..." />
              </div>
              {status === "error" && <div className="alert alert-error">{msg}</div>}
              <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
                <button className="btn btn-primary" onClick={submit} disabled={status === "loading" || (kind === "brands" && (!brands.length || !reason.trim()))}>
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

export function OrderingBrandNotice({ user }: { user: User | null }) {
  const [requestOpen, setRequestOpen] = useState(false);
  if (!user || !["editor", "order_filler"].includes(user.role) || user.brands.length) return null;
  return <div className="alert alert-info" role="status">
    <p style={{ margin: "0 0 8px" }}>No brands assigned. Request access from admin to use this page. / 尚未分配品牌，请申请权限后使用本页。</p>
    <button className="btn btn-sm" onClick={() => setRequestOpen(true)}>Request brand access / 申请品牌</button>
    {requestOpen && <RoleUpgradeModal currentRole={user.role} kind="brands" onClose={() => setRequestOpen(false)} />}
  </div>;
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
