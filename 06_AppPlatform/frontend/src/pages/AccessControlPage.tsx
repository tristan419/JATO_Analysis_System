import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../api/client";
import { useAuth } from "../contexts/AuthContext";
import { useAccountCountryOptions } from "../hooks/useAccountCountryOptions";
import {
  formatJatoCountryOption,
  type JatoCountryOption,
} from "../utils/jatoCountries";

type Tab = "users" | "requests" | "matrix" | "audit";

const TABS: { key: Tab; label: string }[] = [
  { key: "users", label: "Users" },
  { key: "requests", label: "Role Requests" },
  { key: "matrix", label: "Permissions" },
  { key: "audit", label: "Audit Log" },
];

const ROLES = ["viewer", "order_filler", "editor", "admin"] as const;

interface AccessUser {
  id: string;
  username: string;
  role: string;
  email?: string | null;
  display_name?: string | null;
  displayName?: string | null;
  oauth_provider?: string | null;
  oauthProvider?: string | null;
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

interface RoleUpgradeRequestItem {
  requestId: string;
  username: string;
  currentRole: string;
  requestedRole: string;
  reason: string;
  status: string;
  createdAtUtc: string;
}

const PERMISSION_MATRIX: { feature: string; order_filler: boolean; viewer: boolean; editor: boolean; admin: boolean }[] = [
  { feature: "Dashboard / Market Scan 查看", order_filler: true, viewer: true, editor: true, admin: true },
  { feature: "Order Genius 查看 (本人国家)", order_filler: true, viewer: true, editor: true, admin: true },
  { feature: "Order 数量编辑 / 导入导出", order_filler: true, viewer: false, editor: true, admin: true },
  { feature: "Material 上传", order_filler: false, viewer: false, editor: true, admin: true },
  { feature: "Publish Material Baseline", order_filler: false, viewer: false, editor: false, admin: true },
  { feature: "Payment Term 勘误", order_filler: false, viewer: false, editor: false, admin: true },
  { feature: "BOM 底表编辑", order_filler: false, viewer: false, editor: false, admin: true },
  { feature: "用户管理 / 权限审批", order_filler: false, viewer: false, editor: false, admin: true },
  { feature: "JATO 月更发布", order_filler: false, viewer: false, editor: true, admin: true },
  { feature: "Hermes 治理面板", order_filler: false, viewer: false, editor: false, admin: true },
  { feature: "工程配置管理", order_filler: false, viewer: true, editor: true, admin: true },
  { feature: "MSRP 价格管理", order_filler: false, viewer: true, editor: true, admin: true },
];

function CountryMultiSelect({
  options,
  selected,
  onChange,
  embedded = false,
}: {
  options: JatoCountryOption[];
  selected: string[];
  onChange: (codes: string[]) => void;
  embedded?: boolean;
}) {
  const [search, setSearch] = useState("");
  const filtered = options.filter((c) => {
    if (!search) return true;
    const q = search.toLowerCase();
    return c.countryCode.toLowerCase().includes(q) || c.countryName.toLowerCase().includes(q);
  });

  const toggle = (code: string) => {
    onChange(selected.includes(code) ? selected.filter((c) => c !== code) : [...selected, code]);
  };

  return (
    <div style={{ background: "#fff", borderRadius: 6, ...(embedded ? {} : { border: "1px solid #d1d5db", boxShadow: "0 4px 16px rgba(0,0,0,0.12)", padding: 8 }) }}>
      <input
        type="text"
        placeholder="Search countries..."
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        style={{
          width: "100%", padding: "4px 8px", fontSize: 12, borderRadius: 4,
          border: "1px solid #d1d5db", marginBottom: 6, boxSizing: "border-box",
        }}
      />
      <div style={{ fontSize: 11, color: "#64748b", marginBottom: 4, fontWeight: 600 }}>
        {selected.length} selected
      </div>
      <div style={{ maxHeight: 200, overflowY: "auto" }}>
        {filtered.length === 0 ? (
          <div style={{ fontSize: 12, color: "#94a3b8", padding: 8 }}>No countries match</div>
        ) : (
          filtered.map((c) => (
            <label key={c.countryCode}
              style={{
                display: "flex", alignItems: "center", gap: 6,
                padding: "3px 4px", fontSize: 12, cursor: "pointer",
                borderRadius: 3, userSelect: "none",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.background = "#f1f5f9")}
              onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
            >
              <input type="checkbox" checked={selected.includes(c.countryCode)} onChange={() => toggle(c.countryCode)} style={{ margin: 0 }} />
              <span>{c.countryName} {c.countryCode}</span>
            </label>
          ))
        )}
      </div>
    </div>
  );
}

export function AccessControlPage() {
  const { user } = useAuth();
  const { countryOptions: allCountryOptions } = useAccountCountryOptions();
  const [tab, setTab] = useState<Tab>("users");
  const [users, setUsers] = useState<AccessUser[]>([]);
  const [requests, setRequests] = useState<RoleUpgradeRequestItem[]>([]);
  const [requestStatus, setRequestStatus] = useState("pending");
  const [requestsLoading, setRequestsLoading] = useState(false);
  const [reviewingRequestId, setReviewingRequestId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [showCreate, setShowCreate] = useState(false);
  const [newUser, setNewUser] = useState({ username: "", password: "", role: "viewer" });

  // ── Filters & inline editing ────────────────────────────────────
  const [roleFilter, setRoleFilter] = useState<string>("all");
  const [searchQuery, setSearchQuery] = useState("");
  // Edit modal
  const [editingUser, setEditingUser] = useState<AccessUser | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null);
  const [editForm, setEditForm] = useState({ role: "", primaryCountry: "", secondaryCodes: [] as string[], newPassword: "", isActive: true });

  // Multi-select popover for secondary countries
  const secondaryPopoverRef = useRef<HTMLDivElement | null>(null);
  const [secondaryPopoverUser, setSecondaryPopoverUser] = useState<string | null>(null);
  const [secondaryDraft, setSecondaryDraft] = useState<string[]>([]);
  const [popoverPos, setPopoverPos] = useState({ top: 0, left: 0 });

  const loadUsers = useCallback(async () => {
    setLoading(true); setError("");
    try {
      const res = await api.get<{ users: AccessUser[] }>("/auth/users");
      setUsers(res.users || []);
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Failed to load"); }
    setLoading(false);
  }, []);

  const loadRequests = useCallback(async (status = requestStatus) => {
    setRequestsLoading(true);
    setError("");
    try {
      const res = await api.listRoleUpgradeRequests(status === "all" ? undefined : { status });
      const rawRequests = Array.isArray(res.requests) ? res.requests : [];
      setRequests(rawRequests.map((item) => {
        const raw = item as Record<string, unknown>;
        return {
          requestId: String(raw.requestId ?? ""),
          username: String(raw.username ?? ""),
          currentRole: String(raw.currentRole ?? ""),
          requestedRole: String(raw.requestedRole ?? ""),
          reason: String(raw.reason ?? ""),
          status: String(raw.status ?? ""),
          createdAtUtc: String(raw.createdAtUtc ?? ""),
        };
      }).filter((item) => item.requestId));
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to load role requests");
    } finally {
      setRequestsLoading(false);
    }
  }, [requestStatus]);

  useEffect(() => { loadUsers(); }, [loadUsers]);
  useEffect(() => {
    if (tab === "requests") void loadRequests();
  }, [loadRequests, tab]);
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

  const deleteUser = async (userId: string) => {
    try {
      await api.delete(`/auth/users/${userId}`);
      setDeleteConfirm(null);
      setEditingUser(null);
      loadUsers();
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Delete failed"); }
  };

  const openEdit = (u: AccessUser) => {
    setEditingUser(u);
    setEditForm({
      role: u.role,
      primaryCountry: u.primaryCountry ?? u.primary_country_code ?? "",
      secondaryCodes: [...(u.secondaryCountries ?? u.secondary_country_codes ?? [])],
      newPassword: "",
      isActive: u.isActive ?? u.is_active ?? true,
    });
  };

  const saveEdit = async () => {
    if (!editingUser) return;
    try {
      if (editForm.role !== editingUser.role) {
        await api.patch(`/auth/users/${editingUser.id}/role`, { role: editForm.role });
      }
      const currentPrimary = editingUser.primaryCountry ?? editingUser.primary_country_code ?? "";
      const currentSecondary = editingUser.secondaryCountries ?? editingUser.secondary_country_codes ?? [];
      const newSecondarySorted = [...editForm.secondaryCodes].sort();
      const curSecondarySorted = [...currentSecondary].sort();
      if (editForm.primaryCountry !== currentPrimary || newSecondarySorted.join(",") !== curSecondarySorted.join(",")) {
        await api.patch(`/auth/users/${editingUser.id}/profile`, {
          primaryCountry: editForm.primaryCountry || null,
          secondaryCountries: editForm.secondaryCodes,
          preferredLandingPage: editingUser.preferredLandingPage ?? editingUser.preferred_landing_page ?? "/dashboard",
        });
      }
      const currentActive = editingUser.isActive ?? editingUser.is_active ?? true;
      if (editForm.isActive !== currentActive) {
        await api.patch(`/auth/users/${editingUser.id}/toggle-active`);
      }
      if (editForm.newPassword.length >= 6) {
        await api.patch(`/auth/users/${editingUser.id}/password`, { password: editForm.newPassword });
      }
      setEditingUser(null);
      loadUsers();
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Edit failed"); }
  };

  const startEditSecondary = (u: AccessUser, event: React.MouseEvent) => {
    const codes = u.secondaryCountries ?? u.secondary_country_codes ?? [];
    const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
    setPopoverPos({ top: rect.bottom + 4, left: rect.left });
    setSecondaryPopoverUser(u.id);
    setSecondaryDraft([...codes]);
  };

  const saveSecondary = async (targetUser: AccessUser) => {
    const primary = targetUser.primaryCountry ?? targetUser.primary_country_code ?? "";
    await updateProfile(targetUser, primary, secondaryDraft.join(","));
    setSecondaryPopoverUser(null);
  };

  // Close popover on outside click
  useEffect(() => {
    if (!secondaryPopoverUser) return;
    const handler = (e: MouseEvent) => {
      if (secondaryPopoverRef.current && !secondaryPopoverRef.current.contains(e.target as Node)) {
        setSecondaryPopoverUser(null);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [secondaryPopoverUser]);

  const filteredUsers = users.filter((u) => {
    if (roleFilter !== "all" && u.role !== roleFilter) return false;
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      const name = (u.displayName ?? u.display_name ?? u.username).toLowerCase();
      if (!name.includes(q) && !u.username.toLowerCase().includes(q)) return false;
    }
    return true;
  });

  const reviewRequest = async (requestId: string, status: "approved" | "rejected") => {
    setReviewingRequestId(requestId);
    setError("");
    try {
      await api.reviewRoleUpgradeRequest(requestId, { status });
      await Promise.all([loadRequests(), loadUsers()]);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Role request review failed");
    } finally {
      setReviewingRequestId(null);
    }
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
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 12, alignItems: "center" }}>
            <h3 style={{ margin: 0 }}>Users ({filteredUsers.length}{roleFilter !== "all" ? ` / ${users.length}` : ""})</h3>
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
          {/* ── Filters ── */}
          <div style={{ display: "flex", gap: 8, marginBottom: 12, alignItems: "center" }}>
            <input
              placeholder="Search username / display name..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              style={{ padding: "4px 8px", fontSize: 12, width: 200, borderRadius: 4, border: "1px solid #d1d5db" }}
            />
            <select
              value={roleFilter}
              onChange={(e) => setRoleFilter(e.target.value)}
              style={{ padding: "4px 8px", fontSize: 12, borderRadius: 4, border: "1px solid #d1d5db" }}
            >
              <option value="all">All Roles</option>
              {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
            </select>
          </div>
          {loading ? <p style={{ color: "#64748b" }}>Loading...</p> : (
            <table className="data-table" style={{ fontSize: 13 }}>
              <thead>
                <tr>
                  <th>Display Name</th>
                  <th>Username</th>
                  <th>Email</th>
                  <th>OAuth</th>
                  <th>Role</th>
                  <th>Primary Country</th>
                  <th>Secondary Countries</th>
                  <th>Status</th>
                  <th>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredUsers.map((u) => {
                  const active = u.isActive ?? u.is_active ?? true;
                  const primary = u.primaryCountry ?? u.primary_country_code ?? "";
                  const secondary = u.secondaryCountries ?? u.secondary_country_codes ?? [];
                  return (
                  <tr key={u.id} style={!active ? { opacity: 0.5 } : undefined}>
                    <td style={{ fontWeight: 500 }}>{u.displayName ?? u.display_name ?? u.username}</td>
                    <td style={{ fontSize: 12, color: "#64748b" }}>{u.username}</td>
                    <td style={{ fontSize: 12 }}>{u.email ?? "—"}</td>
                    <td style={{ fontSize: 11, color: "#64748b" }}>{u.oauthProvider ?? u.oauth_provider ?? "password"}</td>
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
                        {allCountryOptions.map((country) => (
                          <option key={country.countryCode} value={country.countryCode}>
                            {formatJatoCountryOption(country)}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td style={{ minWidth: 160 }}>
                      <span
                        onClick={(e) => startEditSecondary(u, e)}
                        title="Click to edit secondary countries"
                        style={{ cursor: "pointer", fontSize: 11, color: secondary.length > 0 ? "#1e293b" : "#94a3b8", borderBottom: "1px dashed #cbd5e1" }}
                      >
                        {secondary.length > 0 ? secondary.join(", ") : "Click to set"}
                      </span>
                      {secondaryPopoverUser === u.id && (
                        <div ref={secondaryPopoverRef}
                          style={{
                            position: "fixed", top: popoverPos.top, left: popoverPos.left, zIndex: 9999,
                            minWidth: 260, maxWidth: 340,
                          }}
                        >
                          <CountryMultiSelect
                            options={allCountryOptions}
                            selected={secondaryDraft}
                            onChange={setSecondaryDraft}
                          />
                          <div style={{ display: "flex", gap: 6, marginTop: -1, padding: "6px 8px", background: "#fff", border: "1px solid #d1d5db", borderTop: "none", borderRadius: "0 0 6px 6px", boxShadow: "0 4px 16px rgba(0,0,0,0.12)" }}>
                            <button className="btn btn-sm btn-primary" style={{ padding: "2px 10px", fontSize: 11 }}
                              onClick={() => saveSecondary(u)}>Save</button>
                            <button className="btn btn-sm btn-ghost" style={{ padding: "2px 10px", fontSize: 11 }}
                              onClick={() => setSecondaryPopoverUser(null)}>Cancel</button>
                          </div>
                        </div>
                      )}
                    </td>
                    <td style={{ color: active ? "#16a34a" : "#dc2626", fontWeight: 500 }}>{active ? "Active" : "Inactive"}</td>
                    <td style={{ fontSize: 11, color: "#64748b" }}>{u.created_at_utc?.slice(0, 10) || "—"}</td>
                    <td>
                      <button className="btn btn-sm btn-ghost"
                        onClick={() => openEdit(u)}
                        style={{ fontSize: 11, fontWeight: 600 }}>
                        Edit
                      </button>
                    </td>
                  </tr>
                );})}
              </tbody>
            </table>
          )}

          {/* ── Edit User Modal ── */}
          {editingUser && (
            <div style={{
              position: "fixed", inset: 0, zIndex: 10000,
              background: "rgba(0,0,0,0.4)", display: "flex", alignItems: "center", justifyContent: "center",
            }} onClick={() => setEditingUser(null)}>
              <div style={{
                background: "#fff", borderRadius: 10, padding: 24, width: 420,
                boxShadow: "0 8px 32px rgba(0,0,0,0.2)",
              }} onClick={(e) => e.stopPropagation()}>
                <h3 style={{ margin: "0 0 16px" }}>Edit: {editingUser.username}</h3>

                {/* Role */}
                <label style={{ display: "block", marginBottom: 12, fontSize: 13 }}>
                  <span style={{ fontWeight: 600 }}>Role</span>
                  <select value={editForm.role} onChange={(e) => setEditForm({ ...editForm, role: e.target.value })}
                    style={{ display: "block", width: "100%", marginTop: 4, padding: "6px 8px", borderRadius: 4, border: "1px solid #d1d5db", fontSize: 13 }}>
                    {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
                  </select>
                </label>

                {/* Primary Country */}
                <label style={{ display: "block", marginBottom: 12, fontSize: 13 }}>
                  <span style={{ fontWeight: 600 }}>Primary Country</span>
                  <select value={editForm.primaryCountry} onChange={(e) => setEditForm({ ...editForm, primaryCountry: e.target.value })}
                    style={{ display: "block", width: "100%", marginTop: 4, padding: "6px 8px", borderRadius: 4, border: "1px solid #d1d5db", fontSize: 13 }}>
                    <option value="">Unset</option>
                    {allCountryOptions.map((c) => (
                      <option key={c.countryCode} value={c.countryCode}>{formatJatoCountryOption(c)}</option>
                    ))}
                  </select>
                </label>

                {/* Secondary Countries */}
                <div style={{ marginBottom: 12, fontSize: 13 }}>
                  <span style={{ fontWeight: 600, display: "block", marginBottom: 4 }}>Secondary Countries</span>
                  <CountryMultiSelect
                    options={allCountryOptions}
                    selected={editForm.secondaryCodes}
                    onChange={(codes) => setEditForm({ ...editForm, secondaryCodes: codes })}
                    embedded
                  />
                </div>

                {/* New Password */}
                <label style={{ display: "block", marginBottom: 12, fontSize: 13 }}>
                  <span style={{ fontWeight: 600 }}>New Password</span>
                  <input type="text" value={editForm.newPassword}
                    onChange={(e) => setEditForm({ ...editForm, newPassword: e.target.value })}
                    placeholder="Leave blank to keep current (password is hashed — cannot view original)"
                    style={{ display: "block", width: "100%", marginTop: 4, padding: "6px 8px", borderRadius: 4, border: "1px solid #d1d5db", fontSize: 12, color: "#64748b" }} />
                </label>

                {/* Active toggle */}
                <label style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16, fontSize: 13, cursor: "pointer" }}>
                  <input type="checkbox" checked={editForm.isActive}
                    onChange={(e) => setEditForm({ ...editForm, isActive: e.target.checked })} />
                  <span style={{ fontWeight: 600, color: editForm.isActive ? "#16a34a" : "#dc2626" }}>
                    {editForm.isActive ? "Active" : "Inactive"}
                  </span>
                </label>

                {/* Buttons */}
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <div>
                    {deleteConfirm === editingUser.id ? (
                      <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                        <span style={{ fontSize: 12, color: "#dc2626" }}>Delete this user?</span>
                        <button className="btn btn-sm btn-primary" style={{ background: "#dc2626", padding: "4px 12px", fontSize: 12 }}
                          onClick={() => deleteUser(editingUser.id)}>Yes, Delete</button>
                        <button className="btn btn-sm btn-ghost" style={{ padding: "4px 12px", fontSize: 12 }}
                          onClick={() => setDeleteConfirm(null)}>Cancel</button>
                      </div>
                    ) : (
                      <button className="btn btn-sm btn-ghost"
                        onClick={() => setDeleteConfirm(editingUser.id)}
                        style={{ color: "#dc2626", fontSize: 12 }}>
                        Delete User
                      </button>
                    )}
                  </div>
                  <div style={{ display: "flex", gap: 8 }}>
                    <button className="btn btn-sm btn-ghost" onClick={() => setEditingUser(null)} style={{ fontSize: 13 }}>Cancel</button>
                    <button className="btn btn-sm btn-primary" onClick={saveEdit} style={{ fontSize: 13 }}>Save Changes</button>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {tab === "requests" && (
        <div className="card crud-card" style={{ padding: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 12, alignItems: "center" }}>
            <div>
              <h3 style={{ margin: "0 0 4px" }}>Role Upgrade Requests</h3>
              <p style={{ color: "#64748b", fontSize: 13, margin: 0 }}>
                Viewer users can request editor access. Admin approval updates the user role immediately.
              </p>
            </div>
            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <select
                value={requestStatus}
                onChange={(e) => {
                  const nextStatus = e.target.value;
                  setRequestStatus(nextStatus);
                  void loadRequests(nextStatus);
                }}
                style={{ padding: "5px 8px", fontSize: 12 }}
              >
                <option value="pending">Pending</option>
                <option value="approved">Approved</option>
                <option value="rejected">Rejected</option>
                <option value="all">All</option>
              </select>
              <button className="btn btn-sm btn-secondary" onClick={() => void loadRequests()}>
                Refresh
              </button>
            </div>
          </div>
          {requestsLoading ? <p style={{ color: "#64748b" }}>Loading requests...</p> : null}
          {!requestsLoading && requests.length === 0 ? (
            <div className="alert" style={{ marginTop: 8 }}>
              No {requestStatus === "all" ? "" : requestStatus} role requests.
            </div>
          ) : null}
          {requests.length > 0 ? (
            <table className="data-table" style={{ fontSize: 13 }}>
              <thead>
                <tr>
                  <th>User</th>
                  <th>Current</th>
                  <th>Requested</th>
                  <th>Reason</th>
                  <th>Status</th>
                  <th>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {requests.map((request) => {
                  const pending = request.status === "pending";
                  const busy = reviewingRequestId === request.requestId;
                  return (
                    <tr key={request.requestId}>
                      <td>{request.username}</td>
                      <td>{request.currentRole}</td>
                      <td>{request.requestedRole}</td>
                      <td style={{ maxWidth: 360, whiteSpace: "normal" }}>{request.reason || "-"}</td>
                      <td>{request.status}</td>
                      <td style={{ fontSize: 11, color: "#64748b" }}>{request.createdAtUtc.slice(0, 10) || "—"}</td>
                      <td>
                        <div style={{ display: "flex", gap: 6 }}>
                          <button
                            className="btn btn-sm btn-primary"
                            disabled={!pending || busy}
                            onClick={() => void reviewRequest(request.requestId, "approved")}
                          >
                            Approve
                          </button>
                          <button
                            className="btn btn-sm btn-ghost"
                            disabled={!pending || busy}
                            onClick={() => void reviewRequest(request.requestId, "rejected")}
                          >
                            Reject
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ) : null}
        </div>
      )}

      {tab === "matrix" && (
        <div className="card crud-card" style={{ padding: 16 }}>
          <h3 style={{ margin: "0 0 12px" }}>Permissions Matrix</h3>
          <table className="data-table" style={{ fontSize: 13 }}>
            <thead><tr><th>Feature</th><th style={{ textAlign: "center" }}>Order Filler</th><th style={{ textAlign: "center" }}>Viewer</th><th style={{ textAlign: "center" }}>Editor</th><th style={{ textAlign: "center" }}>Admin</th></tr></thead>
            <tbody>
              {PERMISSION_MATRIX.map((row) => (
                <tr key={row.feature}>
                  <td>{row.feature}</td>
                  <td style={{ textAlign: "center" }}>{row.order_filler ? "Yes" : "No"}</td>
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
