import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { useAuth } from "../contexts/AuthContext";
import { JATO_COUNTRIES, formatJatoCountryOption } from "../utils/jatoCountries";

export function CountrySetupPage() {
  const { user, updateProfile } = useAuth();
  const navigate = useNavigate();
  const [primaryCountry, setPrimaryCountry] = useState(user?.primaryCountry ?? "");
  const [secondaryCountries, setSecondaryCountries] = useState<string[]>(
    user?.secondaryCountries ?? [],
  );
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!user) return;
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
        <h1>Country Setup</h1>
        <p>选择你负责的 JATO 国家范围。JATO 看板和 MarketScan 默认读取主国家，Order Genius 后续会读取主国家加副国家。</p>
      </header>

      <div className="card crud-card" style={{ padding: 20, maxWidth: 760 }}>
        {error ? <div className="alert alert-error" style={{ marginBottom: 12 }}>{error}</div> : null}

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

        <div style={{ display: "flex", gap: 8 }}>
          <button type="button" className="btn btn-sm btn-primary" onClick={saveProfile} disabled={saving}>
            {saving ? "Saving..." : "Save Country Preferences"}
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
