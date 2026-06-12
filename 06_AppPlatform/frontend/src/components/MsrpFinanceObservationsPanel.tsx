import { useEffect, useState, type FormEvent } from "react";

import { api } from "../api/client";
import type {
  MsrpFinanceObservation,
  MsrpFinanceObservationsResponse,
} from "../types";

interface FinanceFilters {
  country: string;
  brand: string;
  jatoModel: string;
  priceSemantics: string;
  financeType: string;
}

const PRICE_SEMANTICS_OPTIONS = [
  { value: "", label: "All semantics" },
  { value: "finance_monthly", label: "Finance monthly" },
  { value: "lease_monthly", label: "Lease monthly" },
  { value: "net_after_subsidy", label: "Net after subsidy" },
];

function defaultFilters(): FinanceFilters {
  return {
    country: "",
    brand: "",
    jatoModel: "",
    priceSemantics: "",
    financeType: "",
  };
}

function formatNumber(value: number | null, suffix = ""): string {
  if (value === null || !Number.isFinite(value)) return "-";
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}${suffix}`;
}

function formatCurrency(value: number | null, currency = "EUR"): string {
  if (value === null || !Number.isFinite(value)) return "-";
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 0 })} ${currency}`;
}

function formatDateTime(value: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function firstCountLabel(counts: Record<string, number>): string {
  const [first] = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  return first ? `${first[0]} · ${first[1]}` : "-";
}

function rowFinanceLabel(item: MsrpFinanceObservation): string {
  return item.financeType || item.priceSemantics || "-";
}

export function MsrpFinanceObservationsPanel() {
  const [filters, setFilters] = useState<FinanceFilters>(defaultFilters);
  const [data, setData] = useState<MsrpFinanceObservationsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadFinanceObservations(nextFilters = filters): Promise<void> {
    setLoading(true);
    setError("");
    try {
      const response = await api.listMsrpFinanceObservations({
        country: nextFilters.country.trim() || undefined,
        brand: nextFilters.brand.trim() || undefined,
        jato_model: nextFilters.jatoModel.trim() || undefined,
        price_semantics: nextFilters.priceSemantics || undefined,
        finance_type: nextFilters.financeType.trim() || undefined,
        limit: 50,
      });
      setData(response);
    } catch (err) {
      setError((err as Error).message || String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadFinanceObservations(defaultFilters());
  }, []);

  function handleSubmit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    void loadFinanceObservations();
  }

  const summary = data?.summary;
  const rows = data?.items ?? [];

  return (
    <div className="msrp-finance-panel">
      <div className="admin-card-header">
        <div>
          <h2>MSRP Finance Observations</h2>
        </div>
        <button
          type="button"
          className="btn btn-sm btn-secondary"
          onClick={() => void loadFinanceObservations()}
          disabled={loading}
        >
          {loading ? "Loading" : "Refresh"}
        </button>
      </div>

      <form className="msrp-finance-filters" onSubmit={handleSubmit}>
        <input
          value={filters.country}
          onChange={(event) => setFilters((current) => ({ ...current, country: event.target.value }))}
          placeholder="Country"
        />
        <input
          value={filters.brand}
          onChange={(event) => setFilters((current) => ({ ...current, brand: event.target.value }))}
          placeholder="Brand"
        />
        <input
          value={filters.jatoModel}
          onChange={(event) => setFilters((current) => ({ ...current, jatoModel: event.target.value }))}
          placeholder="JATO model"
        />
        <select
          value={filters.priceSemantics}
          onChange={(event) => setFilters((current) => ({ ...current, priceSemantics: event.target.value }))}
        >
          {PRICE_SEMANTICS_OPTIONS.map((option) => (
            <option key={option.value || "all"} value={option.value}>{option.label}</option>
          ))}
        </select>
        <input
          value={filters.financeType}
          onChange={(event) => setFilters((current) => ({ ...current, financeType: event.target.value }))}
          placeholder="Finance type"
        />
        <button type="submit" className="btn btn-sm btn-primary" disabled={loading}>
          Apply
        </button>
      </form>

      {error ? (
        <div className="market-scan-state-card market-scan-state-card--error">
          <strong>Error</strong>
          <p>{error}</p>
        </div>
      ) : null}

      {data?.warning ? (
        <div className="market-scan-empty">{data.warning}</div>
      ) : null}

      <div className="msrp-finance-metrics">
        <div className="data-management-metric">
          <span>Total rows</span>
          <strong>{formatNumber(data?.total ?? 0)}</strong>
        </div>
        <div className="data-management-metric">
          <span>Monthly payments</span>
          <strong>{formatNumber(summary?.monthlyPaymentCount ?? 0)}</strong>
          <small>
            {formatCurrency(summary?.monthlyPaymentEurMin ?? null)} - {formatCurrency(summary?.monthlyPaymentEurMax ?? null)}
          </small>
        </div>
        <div className="data-management-metric">
          <span>Net price rows</span>
          <strong>{formatNumber(summary?.netPriceAfterSubsidyCount ?? 0)}</strong>
          <small>
            {formatCurrency(summary?.netPriceAfterSubsidyEurMin ?? null)} - {formatCurrency(summary?.netPriceAfterSubsidyEurMax ?? null)}
          </small>
        </div>
        <div className="data-management-metric">
          <span>Top finance type</span>
          <strong>{firstCountLabel(summary?.financeTypeCounts ?? {})}</strong>
          <small>Subsidy rows {formatNumber(summary?.subsidyObservationCount ?? 0)}</small>
        </div>
      </div>

      {rows.length > 0 ? (
        <div className="msrp-finance-table-wrap">
          <table className="msrp-finance-table">
            <thead>
              <tr>
                <th>Observed</th>
                <th>Country</th>
                <th>Model</th>
                <th>Type</th>
                <th>Monthly</th>
                <th>Net price</th>
                <th>Subsidy</th>
                <th>Term</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 20).map((item) => (
                <tr key={item.financeObservationId}>
                  <td>{formatDateTime(item.observedAtUtc)}</td>
                  <td>{item.country}</td>
                  <td>
                    <strong>{item.brand} {item.jatoModel}</strong>
                    <span>{item.jatoTrim}</span>
                  </td>
                  <td>
                    <strong>{rowFinanceLabel(item)}</strong>
                    <span>{item.priceSemantics}</span>
                  </td>
                  <td>
                    <strong>{formatCurrency(item.monthlyPayment, item.currency)}</strong>
                    <span>{formatCurrency(item.monthlyPaymentEur)}</span>
                  </td>
                  <td>
                    <strong>{formatCurrency(item.netPriceAfterSubsidy, item.currency)}</strong>
                    <span>{formatCurrency(item.netPriceAfterSubsidyEur)}</span>
                  </td>
                  <td>{formatCurrency(item.subsidyAmountEur)}</td>
                  <td>
                    {item.termMonths ? `${item.termMonths} mo` : "-"}
                    {item.annualMileageLimit ? <span>{formatNumber(item.annualMileageLimit)} km/y</span> : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="market-scan-empty">
          {loading ? "Loading finance observations" : "No finance observations"}
        </div>
      )}
    </div>
  );
}
