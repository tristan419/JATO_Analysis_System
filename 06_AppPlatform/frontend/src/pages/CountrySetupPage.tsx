import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../api/client";
import { useAuth } from "../contexts/AuthContext";
import type { CountryPaymentTerm } from "../types/orderGenius";

const FALLBACK_COUNTRIES: CountryPaymentTerm[] = [
  { countryCode: "SE", countryName: "Sweden", paymentTermCode: "LC90", paymentMethod: "LC", lcDays: 90 },
  { countryCode: "CZ", countryName: "Czech Republic", paymentTermCode: "LC90", paymentMethod: "LC", lcDays: 90 },
  { countryCode: "SK", countryName: "Slovakia", paymentTermCode: "LC90", paymentMethod: "LC", lcDays: 90 },
  { countryCode: "RO", countryName: "Romania", paymentTermCode: "LC120", paymentMethod: "LC", lcDays: 120 },
];

export function CountrySetupPage() {
  const { user, updateProfile } = useAuth();
  const navigate = useNavigate();
  const [countries, setCountries] = useState<CountryPaymentTerm[]>([]);
  const [primaryCountry, setPrimaryCountry] = useState(user?.primaryCountry ?? "");
  const [secondaryCountries, setSecondaryCountries] = useState<string[]>(
    user?.secondaryCountries ?? [],
  );
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.getOrderGeniusCountries()
      .then((res) => setCountries(res.items.length > 0 ? res.items : FALLBACK_COUNTRIES))
      .catch(() => setCountries(FALLBACK_COUNTRIES));
  }, []);

  useEffect(() => {
    if (!user) return;
    setPrimaryCountry(user.primaryCountry ?? "");
    setSecondaryCountries(user.secondaryCountries);
  }, [user]);

  const availableCountries = countries.length > 0 ? countries : FALLBACK_COUNTRIES;

  const secondaryOptions = useMemo(
    () => availableCountries.filter((country) => country.countryCode !== primaryCountry),
    [availableCountries, primaryCountry],
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
        <p>选择主国家和可快速切换的副国家。后续订单、问答和国家页面会优先读取这个偏好。</p>
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
            {availableCountries.map((country) => (
              <option key={country.countryCode} value={country.countryCode}>
                {country.countryName} ({country.countryCode})
              </option>
            ))}
          </select>
        </label>

        <div style={{ marginBottom: 16 }}>
          <span style={{ display: "block", fontSize: 13, fontWeight: 700, marginBottom: 8 }}>
            Secondary Countries / 副国家
          </span>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 8 }}>
            {secondaryOptions.map((country) => (
              <label key={country.countryCode} style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13 }}>
                <input
                  type="checkbox"
                  checked={secondaryCountries.includes(country.countryCode)}
                  onChange={() => toggleSecondary(country.countryCode)}
                />
                <span>{country.countryName} ({country.countryCode})</span>
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
