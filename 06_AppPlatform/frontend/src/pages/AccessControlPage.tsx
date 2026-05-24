import { useCallback, useEffect, useState } from "react";
import { api } from "../api/client";
import { useAuth } from "../contexts/AuthContext";

type Tab = "users" | "requests" | "matrix" | "audit";

const TABS: { key: Tab; label: string }[] = [
  { key: "users", label: "Users" },
  { key: "requests", label: "Role Requests" },
  { key: "matrix", label: "Permissions" },
  { key: "audit", label: "Audit Log" },
];

const ROLES = ["viewer", "editor", "admin"] as const;

const PERMISSION_MATRIX: { feature: string; viewer: boolean; editor: boolean; admin: boolean }[] = [
  { feature: "Dashboard / Market Scan 查看", viewer: true, editor: true, admin: true },
  { feature: "Order Genius 查看", viewer: true, editor: true, admin: true },
  { feature: "Order 数量编辑", viewer: false, editor: true, admin: true },
  { feature: "Material 上传", viewer: false, editor: true, admin: true },
  { feature: "Publish Material Baseline", viewer: false, editor: false, admin: true },
  { feature: "Payment Term 勘误", viewer: false, editor: false, admin: true },
  { feature: "BOM 底表编辑", viewer: false, editor: false, admin: true },
  { feature: "用户管理 / 权限审批", viewer: false, editor: false, admin: true },
  { feature: "JATO 月更发布", viewer: false, editor: true, admin: true },
  { feature: "Hermes 治理面板", viewer: false, editor: false, admin: true },
  { feature: "工程配置管理", viewer: true, editor: true, admin: true },
  { feature: "MSRP 价格管理", viewer: true, editor: true, admin: true },
];

export function AccessControlPage() {
  const { user } = useAuth();
  const [tab, setTab] = useState<Tab>("users");
  const [users, setUsers] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [showCreate, setShowCreate] = useState(false);
  const [newUser, setNewUser] = useState({ username: "", password: "", role: "viewer" });

  const loadUsers = useCallback(async () => {
    setLoading(true); setError("");
    try {
      const res = await api.get<any>("/auth/users");
      setUsers(res.users || []);
    } catch (e: any) { setError(e.message || "Failed to load"); }
    setLoading(false);
  }, []);

  useEffect(() => { loadUsers(); }, [loadUsers]);

  const createUser = async () => {
    if (!newUser.username || newUser.password.length < 6) return setError("Username required, password min 6 chars");
    setError("");
    try {
      await api.post("/auth/register", newUser);
      setShowCreate(false);
      setNewUser({ username: "", password: "", role: "viewer" });
      loadUsers();
    } catch (e: any) { setError(e.message || "Create failed"); }
  };

  const updateRole = async (userId: string, role: string) => {
    try {
      await api.patch(`/auth/users/${userId}/role`, { role });
      loadUsers();
    } catch (e: any) { setError(e.message || "Update failed"); }
  };

  return (
    <section className="crud-shell">
      <header className="crud-hero">
        <h1>Access Control</h1>
        <p>用户管理 · 权限矩阵 · 审计日志</p>
      </header>

      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        {TABS.map((t) => (
          <button key={t.key} className={`btn btn-sm ${tab === t.key ? "btn-primary" : "btn-ghost"}`}
            onClick={() => setTab(t.key)}>{t.label}</button>
        ))}
      </div>

      {error ? <div className="alert alert-error" style={{ marginBottom: 12 }}>{error}</div> : null}

      {tab === "users" && (
        <div className="card crud-card" style={{ padding: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 12 }}>
            <h3 style={{ margin: 0 }}>Users ({users.length})</h3>
            <button className="btn btn-sm btn-primary" onClick={() => setShowCreate(!showCreate)}>
              {showCreate ? "Cancel" : "+ New User"}
            </button>
          </div>
          {showCreate && (
            <div style={{ display: "flex", gap: 8, marginBottom: 12, alignItems: "center" }}>
              <input placeholder="Username" value={newUser.username} onChange={(e) => setNewUser({ ...newUser, username: e.target.value })} style={{ width: 120 }} />
              <input type="password" placeholder="Password (min 6)" value={newUser.password} onChange={(e) => setNewUser({ ...newUser, password: e.target.value })} style={{ width: 140 }} />
              <select value={newUser.role} onChange={(e) => setNewUser({ ...newUser, role: e.target.value })}>
                {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
              </select>
              <button className="btn btn-sm btn-primary" onClick={createUser}>Create</button>
            </div>
          )}
          {loading ? <p style={{ color: "#64748b" }}>Loading...</p> : (
            <table className="data-table" style={{ fontSize: 13 }}>
              <thead><tr><th>Username</th><th>Role</th><th>Status</th><th>Created</th><th>Actions</th></tr></thead>
              <tbody>
                {users.map((u: any) => (
                  <tr key={u.id}>
                    <td>{u.username}</td>
                    <td>
                      <select value={u.role} onChange={(e) => updateRole(u.id, e.target.value)}
                        style={{ padding: "2px 4px", fontSize: 12 }}>
                        {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
                      </select>
                    </td>
                    <td style={{ color: u.is_active ? "#16a34a" : "#dc2626" }}>{u.is_active ? "Active" : "Inactive"}</td>
                    <td style={{ fontSize: 11, color: "#64748b" }}>{u.created_at_utc?.slice(0, 10) || "—"}</td>
                    <td>
                      <button className="btn btn-sm btn-ghost" disabled={u.username === user?.username}
                        onClick={() => {/* TODO: deactivate */}}>Deactivate</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}

      {tab === "requests" && (
        <div className="card crud-card" style={{ padding: 16 }}>
          <h3 style={{ margin: "0 0 12px" }}>Role Upgrade Requests</h3>
          <p style={{ color: "#64748b", fontSize: 13 }}>Requests are managed via the existing role-upgrade endpoint. Full admin review UI coming in a future update.</p>
          {/* Future: fetch and display pending role-upgrade requests */}
        </div>
      )}

      {tab === "matrix" && (
        <div className="card crud-card" style={{ padding: 16 }}>
          <h3 style={{ margin: "0 0 12px" }}>Permissions Matrix</h3>
          <table className="data-table" style={{ fontSize: 13 }}>
            <thead><tr><th>Feature</th><th style={{ textAlign: "center" }}>Viewer</th><th style={{ textAlign: "center" }}>Editor</th><th style={{ textAlign: "center" }}>Admin</th></tr></thead>
            <tbody>
              {PERMISSION_MATRIX.map((row) => (
                <tr key={row.feature}>
                  <td>{row.feature}</td>
                  <td style={{ textAlign: "center" }}>{row.viewer ? "✅" : "—"}</td>
                  <td style={{ textAlign: "center" }}>{row.editor ? "✅" : "—"}</td>
                  <td style={{ textAlign: "center" }}>{row.admin ? "✅" : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {tab === "audit" && (
        <div className="card crud-card" style={{ padding: 16 }}>
          <h3 style={{ margin: "0 0 12px" }}>Audit Log</h3>
          <p style={{ color: "#64748b", fontSize: 13 }}>
            Payment term changes are logged to <code>ordering.payment_term_audit_log</code>.
            A unified audit log viewer covering user management, role changes, and data corrections
            will be added in a future update.
          </p>
        </div>
      )}
    </section>
  );
}
