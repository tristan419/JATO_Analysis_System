import { useEffect, useState, type FormEvent } from "react";

import { api } from "../api/client";
import type {
  MsrpFinanceObservation,
  MsrpFinanceObservationsResponse,
} from "../types";
import {
  formatFinanceCurrency,
  formatFinanceDate,
  formatFinanceMonthlyPayment,
  formatFinanceNumber,
  getFinanceObservationLabel,
  getFinanceObservationModelLabel,
  getFinanceObservationValidityBadgeClass,
  getFinanceObservationValidityLabel,
} from "../utils/msrpFinance";

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

function financeModelLabel(item: MsrpFinanceObservation): string {
  return getFinanceObservationModelLabel(item) || item.officialModel || item.financeObservationId;
}

function officialModelLabel(item: MsrpFinanceObservation): string {
  return [item.officialModel, item.officialTrim].filter(Boolean).join(" ") || "-";
}

function financeDetailRows(item: MsrpFinanceObservation): Array<{ label: string; value: string }> {
  return [
    { label: "Monthly", value: formatFinanceCurrency(item.monthlyPayment, item.currency) },
    { label: "Monthly EUR", value: formatFinanceCurrency(item.monthlyPaymentEur) },
    { label: "Down payment", value: formatFinanceCurrency(item.downPayment, item.currency) },
    { label: "Down EUR", value: formatFinanceCurrency(item.downPaymentEur) },
    { label: "Term", value: item.termMonths ? `${item.termMonths} mo` : "-" },
    { label: "Mileage", value: item.annualMileageLimit ? `${formatFinanceNumber(item.annualMileageLimit)} km/y` : "-" },
    { label: "APR", value: item.apr !== null ? `${formatFinanceNumber(item.apr, 2)}%` : "-" },
    { label: "Effective APR", value: item.effectiveApr !== null ? `${formatFinanceNumber(item.effectiveApr, 2)}%` : "-" },
    { label: "Balloon", value: formatFinanceCurrency(item.balloonPayment, item.currency) },
    { label: "Credit cost EUR", value: formatFinanceCurrency(item.totalCreditCostEur) },
    { label: "Subsidy EUR", value: formatFinanceCurrency(item.subsidyAmountEur) },
    { label: "Net after subsidy EUR", value: formatFinanceCurrency(item.netPriceAfterSubsidyEur) },
  ];
}

export function MsrpFinanceObservationsPanel() {
  const [filters, setFilters] = useState<FinanceFilters>(defaultFilters);
  const [data, setData] = useState<MsrpFinanceObservationsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [selectedFinance, setSelectedFinance] = useState<MsrpFinanceObservation | null>(null);
  const [detailFlipped, setDetailFlipped] = useState(false);

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
  const cardRows = rows.slice(0, 8);

  function openFinanceDetail(item: MsrpFinanceObservation): void {
    setSelectedFinance(item);
    setDetailFlipped(false);
  }

  function closeFinanceDetail(): void {
    setSelectedFinance(null);
    setDetailFlipped(false);
  }

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
            {formatFinanceCurrency(summary?.monthlyPaymentEurMin ?? null)} - {formatFinanceCurrency(summary?.monthlyPaymentEurMax ?? null)}
          </small>
        </div>
        <div className="data-management-metric">
          <span>Net price rows</span>
          <strong>{formatNumber(summary?.netPriceAfterSubsidyCount ?? 0)}</strong>
          <small>
            {formatFinanceCurrency(summary?.netPriceAfterSubsidyEurMin ?? null)} - {formatFinanceCurrency(summary?.netPriceAfterSubsidyEurMax ?? null)}
          </small>
        </div>
        <div className="data-management-metric">
          <span>Top finance type</span>
          <strong>{firstCountLabel(summary?.financeTypeCounts ?? {})}</strong>
          <small>Subsidy rows {formatNumber(summary?.subsidyObservationCount ?? 0)}</small>
        </div>
      </div>

      {cardRows.length > 0 ? (
        <div className="msrp-finance-card-grid">
          {cardRows.map((item) => (
            <button
              key={`card-${item.financeObservationId}`}
              type="button"
              className="msrp-finance-detail-card"
              aria-label={`Open finance details for ${financeModelLabel(item)}`}
              onClick={() => openFinanceDetail(item)}
            >
              <div className="msrp-finance-detail-card-head">
                <div>
                  <strong>{financeModelLabel(item)}</strong>
                  <span>{officialModelLabel(item)}</span>
                </div>
                <span className={`badge ${getFinanceObservationValidityBadgeClass(item)}`}>
                  {getFinanceObservationValidityLabel(item)}
                </span>
              </div>
              <div className="msrp-finance-card-monthly">{formatFinanceMonthlyPayment(item)}</div>
              <div className="msrp-finance-card-meta">
                <span>{item.country}</span>
                <span>{getFinanceObservationLabel(item)}</span>
                <span>{item.termMonths ? `${item.termMonths} mo` : "-"}</span>
              </div>
            </button>
          ))}
        </div>
      ) : null}

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
                    <strong>{formatFinanceCurrency(item.monthlyPayment, item.currency)}</strong>
                    <span>{formatFinanceCurrency(item.monthlyPaymentEur)}</span>
                  </td>
                  <td>
                    <strong>{formatFinanceCurrency(item.netPriceAfterSubsidy, item.currency)}</strong>
                    <span>{formatFinanceCurrency(item.netPriceAfterSubsidyEur)}</span>
                  </td>
                  <td>{formatFinanceCurrency(item.subsidyAmountEur)}</td>
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

      {selectedFinance ? (
        <div className="msrp-finance-overlay" role="dialog" aria-modal="true" onClick={closeFinanceDetail}>
          <div
            className={`msrp-finance-flip-card${detailFlipped ? " is-flipped" : ""}`}
            onClick={(event) => event.stopPropagation()}
          >
            <div className="msrp-finance-flip-inner">
              <div className="msrp-finance-flip-face">
                <div>
                  <div className="msrp-finance-detail-card-head">
                    <div>
                      <strong>{financeModelLabel(selectedFinance)}</strong>
                      <span>{officialModelLabel(selectedFinance)}</span>
                    </div>
                    <span className={`badge ${getFinanceObservationValidityBadgeClass(selectedFinance)}`}>
                      {getFinanceObservationValidityLabel(selectedFinance)}
                    </span>
                  </div>
                  <div className="msrp-finance-flip-hero-value">
                    {formatFinanceMonthlyPayment(selectedFinance)}
                    <span>{getFinanceObservationLabel(selectedFinance)}</span>
                  </div>
                </div>
                <div className="msrp-finance-flip-grid">
                  {financeDetailRows(selectedFinance).slice(0, 6).map((row) => (
                    <div key={row.label}>
                      <span>{row.label}</span>
                      <strong>{row.value}</strong>
                    </div>
                  ))}
                </div>
                <div className="msrp-finance-flip-actions">
                  <button type="button" className="btn btn-sm btn-secondary" onClick={() => setDetailFlipped(true)}>
                    Details
                  </button>
                  <button type="button" className="btn btn-sm btn-ghost" onClick={closeFinanceDetail}>
                    Close
                  </button>
                </div>
              </div>
              <div className="msrp-finance-flip-face msrp-finance-flip-back">
                <div className="msrp-finance-detail-card-head">
                  <div>
                    <strong>Finance Context</strong>
                    <span>{selectedFinance.country} / {selectedFinance.brand}</span>
                  </div>
                  <span className="badge badge-inactive">{selectedFinance.priceSemantics}</span>
                </div>
                <div className="msrp-finance-flip-grid">
                  {financeDetailRows(selectedFinance).map((row) => (
                    <div key={row.label}>
                      <span>{row.label}</span>
                      <strong>{row.value}</strong>
                    </div>
                  ))}
                  <div>
                    <span>Offer valid</span>
                    <strong>{formatFinanceDate(selectedFinance.offerValidUntil)}</strong>
                  </div>
                  <div>
                    <span>Observed</span>
                    <strong>{formatDateTime(selectedFinance.observedAtUtc)}</strong>
                  </div>
                </div>
                <div className="msrp-finance-flip-actions">
                  {selectedFinance.sourceUrl ? (
                    <a className="btn btn-sm btn-secondary" href={selectedFinance.sourceUrl} target="_blank" rel="noreferrer">
                      Source
                    </a>
                  ) : null}
                  <button type="button" className="btn btn-sm btn-secondary" onClick={() => setDetailFlipped(false)}>
                    Summary
                  </button>
                  <button type="button" className="btn btn-sm btn-ghost" onClick={closeFinanceDetail}>
                    Close
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
