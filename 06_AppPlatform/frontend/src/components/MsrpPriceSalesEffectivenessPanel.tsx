import { useEffect, useState, type FormEvent } from "react";

import { api } from "../api/client";
import type {
  MsrpPriceSalesEffectivenessItem,
  MsrpPriceSalesEffectivenessResponse,
} from "../types";

interface EffectivenessFilters {
  country: string;
  brand: string;
  jatoModel: string;
  thresholdPct: string;
  baselineWindowMonths: string;
  postWindowMonths: string;
  postLagMonths: string;
  minMonths: string;
  limit: string;
}

interface EffectivenessQueryParams {
  country?: string;
  brand?: string;
  jato_model?: string;
  threshold_pct: number;
  baseline_window_months: number;
  post_window_months: number;
  post_lag_months: number;
  min_months: number;
  limit: number;
}

function defaultFilters(): EffectivenessFilters {
  return {
    country: "",
    brand: "",
    jatoModel: "",
    thresholdPct: "3",
    baselineWindowMonths: "3",
    postWindowMonths: "3",
    postLagMonths: "1",
    minMonths: "1",
    limit: "100",
  };
}

function formatNumber(value: number | null | undefined, suffix = ""): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "-";
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}${suffix}`;
}

function formatDateTime(value: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function parseBoundedNumber(
  value: string,
  fallback: number,
  min: number,
  max: number,
): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function paramsFromFilters(filters: EffectivenessFilters): EffectivenessQueryParams {
  return {
    country: filters.country.trim() || undefined,
    brand: filters.brand.trim() || undefined,
    jato_model: filters.jatoModel.trim() || undefined,
    threshold_pct: parseBoundedNumber(filters.thresholdPct, 3, 0, 50),
    baseline_window_months: parseBoundedNumber(filters.baselineWindowMonths, 3, 1, 12),
    post_window_months: parseBoundedNumber(filters.postWindowMonths, 3, 1, 12),
    post_lag_months: parseBoundedNumber(filters.postLagMonths, 1, 0, 12),
    min_months: parseBoundedNumber(filters.minMonths, 1, 1, 12),
    limit: parseBoundedNumber(filters.limit, 100, 1, 500),
  };
}

function labelClassName(label: string): string {
  return `msrp-effectiveness-label msrp-effectiveness-label--${String(label || "unknown").replace(/[^a-z0-9_-]/gi, "-")}`;
}

function labelText(label: string): string {
  if (label === "positive") return "Positive";
  if (label === "negative") return "Negative";
  if (label === "neutral") return "Neutral";
  if (label === "insufficient_data") return "Insufficient";
  return label || "-";
}

function directionText(direction: string): string {
  if (direction === "down") return "Down";
  if (direction === "up") return "Up";
  if (direction === "unchanged") return "Unchanged";
  return direction || "-";
}

function alertAction(item: MsrpPriceSalesEffectivenessItem): string {
  const action = item.sourcePriceAlert.recommendedAction;
  return typeof action === "string" && action ? action : "-";
}

function monthsLabel(months: string[]): string {
  if (months.length === 0) return "-";
  if (months.length <= 3) return months.join(", ");
  return `${months[0]} - ${months[months.length - 1]}`;
}

function modelLabel(item: MsrpPriceSalesEffectivenessItem): string {
  return [item.brand, item.jatoModel].filter(Boolean).join(" ") || item.analysisId;
}

export function MsrpPriceSalesEffectivenessPanel() {
  const [filters, setFilters] = useState<EffectivenessFilters>(defaultFilters);
  const [data, setData] = useState<MsrpPriceSalesEffectivenessResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadEffectiveness(nextFilters = filters): Promise<void> {
    setLoading(true);
    setError("");
    try {
      const response = await api.listMsrpPriceSalesEffectiveness(
        paramsFromFilters(nextFilters),
      );
      setData(response);
    } catch (err) {
      setError((err as Error).message || String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadEffectiveness(defaultFilters());
  }, []);

  function handleSubmit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    void loadEffectiveness();
  }

  const rows = data?.items ?? [];
  const labelCounts = data?.summary.labelCounts ?? {};
  const analyzedCount = data?.summary.analyzedEventCount ?? 0;

  return (
    <div className="msrp-effectiveness-panel">
      <div className="admin-card-header">
        <div>
          <h2>MSRP Price Sales Effectiveness</h2>
        </div>
        <button
          type="button"
          className="btn btn-sm btn-secondary"
          onClick={() => void loadEffectiveness()}
          disabled={loading}
        >
          {loading ? "Loading" : "Refresh"}
        </button>
      </div>

      <form className="msrp-effectiveness-filters" onSubmit={handleSubmit}>
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
        <input
          type="number"
          min="0"
          max="50"
          step="0.1"
          value={filters.thresholdPct}
          onChange={(event) => setFilters((current) => ({ ...current, thresholdPct: event.target.value }))}
          placeholder="Alert %"
        />
        <input
          type="number"
          min="1"
          max="12"
          step="1"
          value={filters.baselineWindowMonths}
          onChange={(event) => setFilters((current) => ({ ...current, baselineWindowMonths: event.target.value }))}
          placeholder="Baseline mo"
        />
        <input
          type="number"
          min="1"
          max="12"
          step="1"
          value={filters.postWindowMonths}
          onChange={(event) => setFilters((current) => ({ ...current, postWindowMonths: event.target.value }))}
          placeholder="Post mo"
        />
        <input
          type="number"
          min="0"
          max="12"
          step="1"
          value={filters.postLagMonths}
          onChange={(event) => setFilters((current) => ({ ...current, postLagMonths: event.target.value }))}
          placeholder="Lag mo"
        />
        <input
          type="number"
          min="1"
          max="12"
          step="1"
          value={filters.minMonths}
          onChange={(event) => setFilters((current) => ({ ...current, minMonths: event.target.value }))}
          placeholder="Min mo"
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

      {(data?.warnings ?? []).length > 0 ? (
        <div className="market-scan-empty">
          {data?.warnings.join(" · ")}
        </div>
      ) : null}

      <div className="msrp-effectiveness-metrics">
        <div className="data-management-metric">
          <span>Price events</span>
          <strong>{formatNumber(data?.summary.priceEventCount ?? 0)}</strong>
        </div>
        <div className="data-management-metric">
          <span>Analyzed</span>
          <strong>{formatNumber(analyzedCount)}</strong>
        </div>
        <div className="data-management-metric">
          <span>Positive</span>
          <strong>{formatNumber(labelCounts.positive ?? 0)}</strong>
        </div>
        <div className="data-management-metric">
          <span>Insufficient</span>
          <strong>{formatNumber(labelCounts.insufficient_data ?? 0)}</strong>
        </div>
      </div>

      {rows.length > 0 ? (
        <div className="msrp-effectiveness-table-wrap">
          <table className="msrp-effectiveness-table">
            <thead>
              <tr>
                <th>Effect</th>
                <th>Price event</th>
                <th>Sales window</th>
                <th>Sales delta</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 30).map((item) => (
                <tr key={item.analysisId}>
                  <td>
                    <strong className={labelClassName(item.effectivenessLabel)}>
                      {labelText(item.effectivenessLabel)}
                    </strong>
                    <span>{item.country || "-"}</span>
                  </td>
                  <td>
                    <strong>{modelLabel(item)}</strong>
                    <span>
                      {item.priceEventMonth || "-"} · {directionText(item.priceChangeDirection)}
                      {item.priceChangePct !== null ? ` · ${formatNumber(item.priceChangePct, "%")}` : ""}
                    </span>
                  </td>
                  <td>
                    <strong>{monthsLabel(item.baselineWindowMonths)}</strong>
                    <span>{monthsLabel(item.postWindowMonths)}</span>
                  </td>
                  <td>
                    <strong>{formatNumber(item.baselineAvgSales)}{" -> "}{formatNumber(item.postAvgSales)}</strong>
                    <span>
                      {formatNumber(item.salesDelta)}
                      {item.salesDeltaPct !== null ? ` (${formatNumber(item.salesDeltaPct, "%")})` : ""}
                    </span>
                  </td>
                  <td>
                    <strong>{alertAction(item)}</strong>
                    <span title={item.confidenceNote}>{formatDateTime(item.generatedAtUtc)}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="market-scan-empty">
          {loading ? "Loading effectiveness" : "No price sales effectiveness events"}
        </div>
      )}
    </div>
  );
}
