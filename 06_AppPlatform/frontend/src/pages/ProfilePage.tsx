import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { useAuth } from "../contexts/AuthContext";
import { JATO_COUNTRIES, formatJatoCountryOption } from "../utils/jatoCountries";

export function ProfilePage() {
  const { user, updateProfile } = useAuth();
  const navigate = useNavigate();
  const isOrderFiller = user?.role === "order_filler";
  const [displayName, setDisplayName] = useState(user?.displayName ?? "");
  const [primaryCountry, setPrimaryCountry] = useState(user?.primaryCountry ?? "");
  const [secondaryCountries, setSecondaryCountries] = useState<string[]>(
    user?.secondaryCountries ?? [],
  );
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!user) return;
    setDisplayName(user.displayName ?? "");
    setPrimaryCountry(user.primaryCountry ?? "");
    setSecondaryCountries(user.secondaryCountries);
  }, [user]);

  const secondaryOptions = useMemo(
    () => JATO_COUNTRIES.filter((country) => country.countryCode !== primaryCountry),
    [primaryCountry],
  );

  function toggleSecondary(countryCode: string): void {
    setSecondaryCountries((current) => {
      if (current.includes(countryCode)) {
        return current.filter((code) => code !== countryCode);
      }
      return [...current, countryCode];
    });
  }

  async function saveProfile(): Promise<void> {
    if (!primaryCountry) {
      setError("请选择一个主国家。");
      return;
    }
    setSaving(true);
    setError("");
    try {
      await updateProfile({
        primaryCountry,
        secondaryCountries: secondaryCountries.filter((code) => code !== primaryCountry),
        preferredLandingPage: "/dashboard",
        displayName: displayName.trim() || null,
      });
      navigate("/dashboard", { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : "保存失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="crud-shell">
      <header className="crud-hero">
        <h1>Profile</h1>
        <p>管理你的 JATO 账户资料与国家偏好。</p>
      </header>

      <div className="card crud-card" style={{ padding: 20, maxWidth: 760 }}>
        {error ? <div className="alert alert-error" style={{ marginBottom: 12 }}>{error}</div> : null}

        {/* Profile overview */}
        <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 24, paddingBottom: 20, borderBottom: "1px solid #e5e7eb" }}>
          {user?.avatarUrl ? (
            <img src={user.avatarUrl} alt="" style={{ width: 56, height: 56, borderRadius: "50%", objectFit: "cover", flexShrink: 0 }} referrerPolicy="no-referrer" />
          ) : (
            <span style={{ width: 56, height: 56, borderRadius: "50%", background: "#1c69d4", color: "#fff", display: "inline-flex", alignItems: "center", justifyContent: "center", fontSize: 22, fontWeight: 700, flexShrink: 0 }}>
              {user?.displayName?.[0] ?? user?.username?.[0] ?? "?"}
            </span>
          )}
          <div>
            <div style={{ fontSize: 16, fontWeight: 700, color: "#1e293b" }}>{user?.displayName ?? user?.username ?? ""}</div>
            <div style={{ fontSize: 13, color: "#64748b" }}>{user?.email ?? user?.username ?? ""}</div>
            <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 2 }}>
              {user?.oauthProvider === "google" ? "Google Account" : "Password Account"} · {user?.role}
            </div>
          </div>
        </div>

        {/* Display name */}
        <label style={{ display: "block", marginBottom: 20 }}>
          <span style={{ display: "block", fontSize: 13, fontWeight: 700, marginBottom: 6 }}>
            Display Name / 显示名称
          </span>
          <input
            type="text"
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
            placeholder={user?.username ?? "Enter your display name"}
            maxLength={64}
            style={{ width: "100%", maxWidth: 320, padding: "8px 12px", borderRadius: 4, border: "1px solid #d1d5db", fontSize: 14 }}
          />
          <span style={{ fontSize: 11, color: "#94a3b8", marginTop: 4, display: "block" }}>
            留空则显示账号名 ({user?.username})
          </span>
        </label>

        {isOrderFiller ? (
          <div style={{ marginBottom: 16, padding: 12, background: "#fef3c7", borderRadius: 6, border: "1px solid #f59e0b" }}>
            <span style={{ fontSize: 13, color: "#92400e" }}>
              Your country assignments are managed by your administrator. Contact an admin to change your primary or secondary countries.
            </span>
            <div style={{ marginTop: 8, fontSize: 12, color: "#a16207" }}>
              Primary: {primaryCountry || "Not set"} &middot; Secondary: {secondaryCountries.length > 0 ? secondaryCountries.join(", ") : "None"}
            </div>
          </div>
        ) : (
          <>
            <label style={{ display: "block", marginBottom: 16 }}>
              <span style={{ display: "block", fontSize: 13, fontWeight: 700, marginBottom: 6 }}>
                Primary Country / 主国家
              </span>
              <select
                value={primaryCountry}
                onChange={(event) => setPrimaryCountry(event.target.value)}
                style={{ minWidth: 260 }}
              >
                <option value="">Select country...</option>
                {JATO_COUNTRIES.map((country) => (
                  <option key={country.countryCode} value={country.countryCode}>
                    {formatJatoCountryOption(country)}
                  </option>
                ))}
              </select>
            </label>

            <div style={{ marginBottom: 16 }}>
              <span style={{ display: "block", fontSize: 13, fontWeight: 700, marginBottom: 8 }}>
                Secondary Countries / 副国家（可选）
              </span>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 8 }}>
                {secondaryOptions.map((country) => (
                  <label key={country.countryCode} style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13 }}>
                    <input
                      type="checkbox"
                      checked={secondaryCountries.includes(country.countryCode)}
                      onChange={() => toggleSecondary(country.countryCode)}
                    />
                    <span>{formatJatoCountryOption(country)}</span>
                  </label>
                ))}
              </div>
            </div>
          </>
        )}

        <div style={{ display: "flex", gap: 8 }}>
          <button type="button" className="btn btn-sm btn-primary" onClick={saveProfile} disabled={saving}>
            {saving ? "Saving..." : "Save Profile"}
          </button>
          {user?.profileComplete ? (
            <button type="button" className="btn btn-sm btn-ghost" onClick={() => navigate(-1)}>
              Cancel
            </button>
          ) : null}
        </div>
      </div>
    </section>
  );
}
