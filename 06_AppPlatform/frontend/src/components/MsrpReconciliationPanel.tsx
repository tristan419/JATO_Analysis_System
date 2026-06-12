import { useEffect, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import type {
  MsrpReconciliationItem,
  MsrpReconciliationResponse,
  MsrpReconciliationReviewQueueResponse,
  MsrpReconciliationStatus,
  MsrpReconciliationSourceObservation,
} from "../types";

interface ReconciliationFilters {
  country: string;
  brand: string;
  jatoModel: string;
  thresholdPct: string;
  limit: string;
}

function defaultFilters(): ReconciliationFilters {
  return {
    country: "",
    brand: "",
    jatoModel: "",
    thresholdPct: "1",
    limit: "200",
  };
}

function formatNumber(value: number | null | undefined, suffix = ""): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "-";
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}${suffix}`;
}

function formatCurrency(value: number | null | undefined, currency = "EUR"): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "-";
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 0 })} ${currency}`;
}

function formatDateTime(value: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function parsePositiveNumber(value: string, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseThreshold(value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return 1;
  return Math.min(parsed, 50);
}

function statusLabel(status: MsrpReconciliationStatus): string {
  if (status === "conflict") return "Conflict";
  if (status === "single_source") return "Single source";
  if (status === "aligned") return "Aligned";
  return status || "-";
}

function sourceSummary(sources: MsrpReconciliationSourceObservation[]): string {
  if (sources.length === 0) return "-";
  return sources
    .slice(0, 3)
    .map((source) => `${source.sourceCode || source.sourceType || "source"} ${formatCurrency(source.msrpValue, source.currency)}`)
    .join(" · ");
}

function actionLabel(action: string): string {
  if (action === "review_conflicting_sources") return "Review";
  if (action === "add_secondary_source") return "Add source";
  if (action === "keep_current_price") return "Keep";
  return action || "-";
}

function statusClassName(status: MsrpReconciliationStatus): string {
  return `msrp-reconciliation-status msrp-reconciliation-status--${String(status || "unknown").replace(/[^a-z0-9_-]/gi, "-")}`;
}

export function MsrpReconciliationPanel() {
  const [filters, setFilters] = useState<ReconciliationFilters>(defaultFilters);
  const [data, setData] = useState<MsrpReconciliationResponse | null>(null);
  const [queueResult, setQueueResult] = useState<MsrpReconciliationReviewQueueResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [queueing, setQueueing] = useState(false);
  const [error, setError] = useState("");

  function paramsFromFilters(nextFilters: ReconciliationFilters) {
    return {
      country: nextFilters.country.trim() || undefined,
      brand: nextFilters.brand.trim() || undefined,
      jato_model: nextFilters.jatoModel.trim() || undefined,
      threshold_pct: parseThreshold(nextFilters.thresholdPct),
      limit: parsePositiveNumber(nextFilters.limit, 200),
    };
  }

  async function loadReconciliation(nextFilters = filters): Promise<void> {
    setLoading(true);
    setError("");
    try {
      const response = await api.listMsrpReconciliation(paramsFromFilters(nextFilters));
      setData(response);
    } catch (err) {
      setError((err as Error).message || String(err));
    } finally {
      setLoading(false);
    }
  }

  async function queueConflicts(): Promise<void> {
    setQueueing(true);
    setError("");
    try {
      const response = await api.queueMsrpReconciliationReviewCases(paramsFromFilters(filters));
      setQueueResult(response);
      await loadReconciliation(filters);
    } catch (err) {
      setError((err as Error).message || String(err));
    } finally {
      setQueueing(false);
    }
  }

  useEffect(() => {
    void loadReconciliation(defaultFilters());
  }, []);

  function handleSubmit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    void loadReconciliation();
  }

  const rows = data?.items ?? [];
  const statusCounts = data?.summary.statusCounts ?? {};
  const conflictCount = statusCounts.conflict ?? 0;

  return (
    <div className="msrp-reconciliation-panel">
      <div className="admin-card-header">
        <div>
          <h2>MSRP Multi-source Reconciliation</h2>
        </div>
        <div className="msrp-reconciliation-actions">
          <Link className="btn btn-sm btn-secondary" to="/review">
            Review queue
          </Link>
          <button
            type="button"
            className="btn btn-sm btn-primary"
            onClick={() => void queueConflicts()}
            disabled={queueing || loading}
          >
            {queueing ? "Queueing" : "Queue conflicts"}
          </button>
          <button
            type="button"
            className="btn btn-sm btn-secondary"
            onClick={() => void loadReconciliation()}
            disabled={loading || queueing}
          >
            {loading ? "Loading" : "Refresh"}
          </button>
        </div>
      </div>

      <form className="msrp-reconciliation-filters" onSubmit={handleSubmit}>
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
          placeholder="Threshold %"
        />
        <input
          type="number"
          min="1"
          max="5000"
          step="1"
          value={filters.limit}
          onChange={(event) => setFilters((current) => ({ ...current, limit: event.target.value }))}
          placeholder="Limit"
        />
        <button type="submit" className="btn btn-sm btn-primary" disabled={loading || queueing}>
          Apply
        </button>
      </form>

      {error ? (
        <div className="market-scan-state-card market-scan-state-card--error">
          <strong>Error</strong>
          <p>{error}</p>
        </div>
      ) : null}

      {queueResult ? (
        <div className="msrp-reconciliation-queue-result">
          <strong>{formatNumber(queueResult.summary.reviewCasesQueued)} queued</strong>
          <span>{formatNumber(queueResult.summary.reviewCasesCreated)} created</span>
          <span>{formatNumber(queueResult.summary.reviewCasesReused)} reused</span>
        </div>
      ) : null}

      <div className="msrp-reconciliation-metrics">
        <div className="data-management-metric">
          <span>Conflict groups</span>
          <strong>{formatNumber(conflictCount)}</strong>
        </div>
        <div className="data-management-metric">
          <span>Aligned groups</span>
          <strong>{formatNumber(statusCounts.aligned ?? 0)}</strong>
        </div>
        <div className="data-management-metric">
          <span>Single source</span>
          <strong>{formatNumber(statusCounts.single_source ?? 0)}</strong>
        </div>
        <div className="data-management-metric">
          <span>Observation rows</span>
          <strong>{formatNumber(data?.summary.observationRows ?? 0)}</strong>
        </div>
      </div>

      {rows.length > 0 ? (
        <div className="msrp-reconciliation-table-wrap">
          <table className="msrp-reconciliation-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Model</th>
                <th>Spread</th>
                <th>Current price</th>
                <th>Sources</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 30).map((item: MsrpReconciliationItem) => (
                <tr key={`${item.country}-${item.brand}-${item.jatoModel}-${item.jatoTrim}-${item.jatoPowertrain ?? ""}`}>
                  <td>
                    <strong className={statusClassName(item.status)}>{statusLabel(item.status)}</strong>
                    <span>{formatNumber(item.sourceCount)} sources</span>
                  </td>
                  <td>
                    <strong>{item.brand} {item.jatoModel}</strong>
                    <span>{item.country} · {item.jatoTrim}{item.jatoPowertrain ? ` · ${item.jatoPowertrain}` : ""}</span>
                  </td>
                  <td>
                    <strong>{formatNumber(item.spreadPct, "%")}</strong>
                    <span>{formatCurrency(item.minMsrpValue)} - {formatCurrency(item.maxMsrpValue)}</span>
                  </td>
                  <td>
                    <strong>{formatCurrency(item.currentPrice?.currentMsrpValue ?? null, item.currentPrice?.currency ?? "EUR")}</strong>
                    <span>{item.currentPrice?.sourceCode ?? item.currentPrice?.sourceType ?? "-"}</span>
                  </td>
                  <td title={sourceSummary(item.sourceObservations)}>
                    <strong>{formatNumber(item.observationCount)} observations</strong>
                    <span>{sourceSummary(item.sourceObservations)}</span>
                  </td>
                  <td>
                    <strong>{actionLabel(item.recommendedAction)}</strong>
                    <span>{formatDateTime(item.sourceObservations[0]?.observedAtUtc ?? "")}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="market-scan-empty">
          {loading ? "Loading reconciliation" : "No reconciliation rows"}
        </div>
      )}
    </div>
  );
}
