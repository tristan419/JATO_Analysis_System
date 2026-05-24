import { useCallback, useEffect, useState } from "react";
import { api } from "../api/client";
import { useAuth } from "../contexts/AuthContext";
import type { CountryPaymentTerm } from "../types/orderGenius";

type Tab = "users" | "requests" | "matrix" | "audit";

const TABS: { key: Tab; label: string }[] = [
  { key: "users", label: "Users" },
  { key: "requests", label: "Role Requests" },
  { key: "matrix", label: "Permissions" },
  { key: "audit", label: "Audit Log" },
];

const ROLES = ["viewer", "editor", "admin"] as const;

interface AccessUser {
  id: string;
  username: string;
  role: string;
  is_active?: boolean;
  isActive?: boolean;
  primary_country_code?: string | null;
  primaryCountry?: string | null;
  secondary_country_codes?: string[];
  secondaryCountries?: string[];
  preferred_landing_page?: string | null;
  preferredLandingPage?: string | null;
  created_at_utc?: string | null;
}

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
  const [users, setUsers] = useState<AccessUser[]>([]);
  const [countries, setCountries] = useState<CountryPaymentTerm[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [showCreate, setShowCreate] = useState(false);
  const [newUser, setNewUser] = useState({ username: "", password: "", role: "viewer" });

  const loadUsers = useCallback(async () => {
    setLoading(true); setError("");
    try {
      const res = await api.get<{ users: AccessUser[] }>("/auth/users");
      setUsers(res.users || []);
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Failed to load"); }
    setLoading(false);
  }, []);

  useEffect(() => { loadUsers(); }, [loadUsers]);
  useEffect(() => {
    api.getOrderGeniusCountries()
      .then((res) => setCountries(res.items || []))
      .catch(() => setCountries([]));
  }, []);

  const createUser = async () => {
    if (!newUser.username || newUser.password.length < 6) return setError("Username required, password min 6 chars");
    setError("");
    try {
      await api.post("/auth/register", newUser);
      setShowCreate(false);
      setNewUser({ username: "", password: "", role: "viewer" });
      loadUsers();
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Create failed"); }
  };

  const updateRole = async (userId: string, role: string) => {
    try {
      await api.patch(`/auth/users/${userId}/role`, { role });
      loadUsers();
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Update failed"); }
  };

  const updateProfile = async (
    targetUser: AccessUser,
    primaryCountry: string,
    secondaryRaw: string,
  ) => {
    try {
      const secondaryCountries = secondaryRaw
        .split(",")
        .map((item) => item.trim().toUpperCase())
        .filter(Boolean);
      await api.patch(`/auth/users/${targetUser.id}/profile`, {
        primaryCountry: primaryCountry || null,
        secondaryCountries,
        preferredLandingPage: targetUser.preferredLandingPage ?? targetUser.preferred_landing_page ?? "/dashboard",
      });
      loadUsers();
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Profile update failed"); }
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
              <thead>
                <tr>
                  <th>Username</th>
                  <th>Role</th>
                  <th>Primary Country</th>
                  <th>Secondary</th>
                  <th>Status</th>
                  <th>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {users.map((u) => {
                  const active = u.isActive ?? u.is_active ?? true;
                  const primary = u.primaryCountry ?? u.primary_country_code ?? "";
                  const secondary = u.secondaryCountries ?? u.secondary_country_codes ?? [];
                  return (
                  <tr key={u.id}>
                    <td>{u.username}</td>
                    <td>
                      <select value={u.role} onChange={(e) => updateRole(u.id, e.target.value)}
                        style={{ padding: "2px 4px", fontSize: 12 }}>
                        {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
                      </select>
                    </td>
                    <td>
                      <select
                        value={primary}
                        onChange={(e) => updateProfile(u, e.target.value, secondary.join(","))}
                        style={{ padding: "2px 4px", fontSize: 12, minWidth: 120 }}
                      >
                        <option value="">Unset</option>
                        {countries.map((country) => (
                          <option key={country.countryCode} value={country.countryCode}>
                            {country.countryCode} {country.countryName}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td>
                      <input
                        defaultValue={secondary.join(",")}
                        placeholder="SE,CZ"
                        onBlur={(e) => updateProfile(u, primary, e.target.value)}
                        style={{ width: 110, padding: "2px 4px", fontSize: 12 }}
                      />
                    </td>
                    <td style={{ color: active ? "#16a34a" : "#dc2626" }}>{active ? "Active" : "Inactive"}</td>
                    <td style={{ fontSize: 11, color: "#64748b" }}>{u.created_at_utc?.slice(0, 10) || "—"}</td>
                    <td>
                      <button className="btn btn-sm btn-ghost" disabled={u.username === user?.username}
                        onClick={() => {/* TODO: deactivate */}}>Deactivate</button>
                    </td>
                  </tr>
                );})}
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
                  <td style={{ textAlign: "center" }}>{row.viewer ? "Yes" : "No"}</td>
                  <td style={{ textAlign: "center" }}>{row.editor ? "Yes" : "No"}</td>
                  <td style={{ textAlign: "center" }}>{row.admin ? "Yes" : "No"}</td>
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
