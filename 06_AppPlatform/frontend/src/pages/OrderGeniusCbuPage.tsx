import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { api } from "../api/client";
import { MaterialFinanceWorkbench } from "../components/finance";
import type {
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
  CountryPaymentTerm,
  OrderGeniusOptions,
} from "../types/orderGenius";

function firstCountry(countries: CountryPaymentTerm[], preferred: string): string {
  const codes = countries.map((country) => country.countryCode);
  if (codes.includes(preferred)) return preferred;
  if (codes.includes("NL")) return "NL";
  return codes[0] ?? preferred;
}

function cleanParam(value: string | null): string {
  return (value ?? "").trim();
}

export function OrderGeniusCbuPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [countryOptions, setCountryOptions] = useState<CountryPaymentTerm[]>([]);
  const [countryCode, setCountryCode] = useState(() => cleanParam(searchParams.get("country")).toUpperCase() || "NL");
  const [brand, setBrand] = useState(() => cleanParam(searchParams.get("brand")));
  const [modelName, setModelName] = useState(() => cleanParam(searchParams.get("model")));
  const [powertrain, setPowertrain] = useState(() => cleanParam(searchParams.get("powertrain")));
  const [version, setVersion] = useState(() => cleanParam(searchParams.get("version")));
  const [options, setOptions] = useState<OrderGeniusOptions | null>(null);
  const [rows, setRows] = useState<CountryMaterialFinanceRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [savingMaterialCode, setSavingMaterialCode] = useState<string | null>(null);

  const countryCodes = useMemo(() => {
    const codes = countryOptions.map((country) => country.countryCode);
    const rest = codes.filter((code) => code !== "NL").sort();
    return codes.includes("NL") ? ["NL", ...rest] : rest;
  }, [countryOptions]);

  const scopeLabel = useMemo(
    () => [brand || "All brands", modelName || "All models", powertrain || "All powertrains", version || "All versions"].join(" · "),
    [brand, modelName, powertrain, version],
  );

  const syncUrl = (next: {
    countryCode?: string;
    brand?: string;
    modelName?: string;
    powertrain?: string;
    version?: string;
  }) => {
    const params = new URLSearchParams();
    const nextCountry = next.countryCode ?? countryCode;
    const nextBrand = next.brand ?? brand;
    const nextModel = next.modelName ?? modelName;
    const nextPowertrain = next.powertrain ?? powertrain;
    const nextVersion = next.version ?? version;
    if (nextCountry) params.set("country", nextCountry);
    if (nextBrand) params.set("brand", nextBrand);
    if (nextModel) params.set("model", nextModel);
    if (nextPowertrain) params.set("powertrain", nextPowertrain);
    if (nextVersion) params.set("version", nextVersion);
    setSearchParams(params, { replace: true });
  };

  useEffect(() => {
    let cancelled = false;
    api.getAccountCountryOptions()
      .then((response) => {
        if (cancelled) return;
        setCountryOptions(response.items);
        const resolvedCountry = firstCountry(response.items, countryCode);
        if (resolvedCountry !== countryCode) {
          setCountryCode(resolvedCountry);
          syncUrl({ countryCode: resolvedCountry });
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      });
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (!countryCode) return;
    let cancelled = false;
    api.getOrderGeniusOptions({
      country: countryCode,
      brand: brand || undefined,
      model: modelName || undefined,
      powertrain: powertrain || undefined,
      version: version || undefined,
    })
      .then((nextOptions) => {
        if (!cancelled) setOptions(nextOptions);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      });
    return () => { cancelled = true; };
  }, [countryCode, brand, modelName, powertrain, version]);

  const loadRows = async () => {
    if (!countryCode) return;
    setLoading(true);
    setError("");
    try {
      const response = await api.listCountryMaterialFinance({
        country: countryCode,
        brand: brand || undefined,
        model: modelName || undefined,
        powertrain: powertrain || undefined,
        version: version || undefined,
      });
      setRows(response.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadRows();
  }, [countryCode, brand, modelName, powertrain, version]);

  const handleCountryChange = async (nextCountryCode: string) => {
    setCountryCode(nextCountryCode);
    syncUrl({ countryCode: nextCountryCode });
  };

  const handleSaveRow = async (
    row: CountryMaterialFinanceRow,
    update: CountryMaterialFinanceUpdate,
  ) => {
    setSavingMaterialCode(row.materialCode);
    setError("");
    try {
      const saved = await api.updateMaterialCountryFinance(row.materialCode, update);
      setRows((current) => current.map((item) => item.materialCode === saved.materialCode ? saved : item));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingMaterialCode(null);
    }
  };

  return (
    <section className="crud-shell material-finance-page">
      <header className="crud-hero">
        <h1>CBU Finance Detail</h1>
        <p>Country CBU matrix at BOM-template level.</p>
      </header>
      <div className="material-finance-page-controls">
        <label>
          <span>Country</span>
          <select
            value={countryCode}
            onChange={(event) => {
              const nextCountry = event.target.value;
              setCountryCode(nextCountry);
              syncUrl({ countryCode: nextCountry });
            }}
          >
            {countryCodes.map((code) => (
              <option key={code} value={code}>{code}</option>
            ))}
          </select>
        </label>
        <label>
          <span>Brand</span>
          <select
            value={brand}
            onChange={(event) => {
              const value = event.target.value;
              setBrand(value);
              setModelName("");
              setPowertrain("");
              setVersion("");
              syncUrl({ brand: value, modelName: "", powertrain: "", version: "" });
            }}
          >
            <option value="">All brands</option>
            {(options?.brands ?? []).map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label>
          <span>Model</span>
          <select
            value={modelName}
            onChange={(event) => {
              const value = event.target.value;
              setModelName(value);
              setPowertrain("");
              setVersion("");
              syncUrl({ modelName: value, powertrain: "", version: "" });
            }}
          >
            <option value="">All models</option>
            {(options?.models ?? []).map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label>
          <span>Powertrain</span>
          <select
            value={powertrain}
            onChange={(event) => {
              const value = event.target.value;
              setPowertrain(value);
              setVersion("");
              syncUrl({ powertrain: value, version: "" });
            }}
          >
            <option value="">All powertrains</option>
            {(options?.powertrains ?? []).map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label>
          <span>Version</span>
          <select
            value={version}
            onChange={(event) => {
              const value = event.target.value;
              setVersion(value);
              syncUrl({ version: value });
            }}
          >
            <option value="">All versions</option>
            {(options?.versions ?? []).map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <button type="button" className="btn btn-sm btn-primary" onClick={() => void loadRows()}>
          Refresh
        </button>
      </div>
      <div className="material-finance-page-workbench">
        <MaterialFinanceWorkbench
          countryCode={countryCode}
          countryCodes={countryCodes}
          scopeLabel={scopeLabel}
          rows={rows}
          loading={loading}
          error={error}
          savingMaterialCode={savingMaterialCode}
          onCountryChange={handleCountryChange}
          onSaveRow={handleSaveRow}
        />
      </div>
    </section>
  );
}
