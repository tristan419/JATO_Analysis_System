import { useCallback, useEffect, useState } from "react";
import { api } from "../api/client";
import { DeckFloatingDrawer } from "../components/deckControls";
import { useAuth } from "../contexts/AuthContext";
import { useResolvedCountry } from "../hooks/useResolvedCountry";
import type { MsrpFinanceObservation, MsrpFinanceObservationSummary } from "../types";
import type { LeaseOffer, LeaseCompareSet, SolveResult } from "../types/leaseComparison";
import {
  formatFinanceCurrency,
  formatFinanceCurrencyRange,
  formatFinanceDate,
  formatFinanceNumber,
  getFinanceObservationLabel,
  getFinanceObservationModelLabel,
  getFinanceObservationValidityBadgeClass,
  getFinanceObservationValidityLabel,
  matchesFinanceObservationFilters,
} from "../utils/msrpFinance";

const LEASE_TYPES = ["private", "fleet", "financial"] as const;
const STATUSES = ["draft", "active", "expired", "archived", "scenario"] as const;
const UPFRONT_TREATMENTS = ["cap_cost_reduction", "due_at_signing", "refundable_deposit", "unknown"];
const SOLVE_FOR_OPTIONS = [
  { value: "monthly_payment", label: "Monthly Payment" },
  { value: "money_factor", label: "APR / MF" },
  { value: "cap_cost", label: "Cap Cost" },
  { value: "residual_value", label: "Residual Value" },
];

const EMPTY_OFFER = {
  countryCode: "", currency: "EUR", brand: "", modelName: "",
  version: "", powertrain: "", segment: "", leaseType: "private" as const,
  provider: "", termMonths: 36, mileagePerYear: 15000,
  monthlyPayment: 0, downPayment: 0, capCost: 0, residualValue: 0,
  residualValuePercent: 0, aprPercent: 0, moneyFactor: 0,
};

export function LeaseComparisonPage() {
  const { user } = useAuth();
  const { primaryCountryISO } = useResolvedCountry("iso");
  const isEditor = user?.role === "editor" || user?.role === "admin";

  // ── State ──
  const [offers, setOffers] = useState<LeaseOffer[]>([]);
  const [compareIds, setCompareIds] = useState<Set<string>>(new Set());
  const [selectedOffer, setSelectedOffer] = useState<LeaseOffer | null>(null);
  const [editForm, setEditForm] = useState<Partial<LeaseOffer>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [aiSummary, setAiSummary] = useState<string | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [showFormula, setShowFormula] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [viewMode, setViewMode] = useState<"cards" | "table">("cards");
  const [showVersions, setShowVersions] = useState<string | null>(null);
  const [versionList, setVersionList] = useState<LeaseOffer["versions"]>([]);
  const [countryFilter, setCountryFilter] = useState(primaryCountryISO);
  const [brandFilter, setBrandFilter] = useState("");
  const [modelFilter, setModelFilter] = useState("");
  const [leaseTypeFilter, setLeaseTypeFilter] = useState<string>("");
  const [financeTypeFilter, setFinanceTypeFilter] = useState("");
  const [filterDeckOpen, setFilterDeckOpen] = useState(true);
  const [financeOffers, setFinanceOffers] = useState<MsrpFinanceObservation[]>([]);
  const [financeSummary, setFinanceSummary] = useState<MsrpFinanceObservationSummary | null>(null);
  const [financeTotal, setFinanceTotal] = useState(0);
  const [financeLoading, setFinanceLoading] = useState(false);
  const [financeError, setFinanceError] = useState<string | null>(null);

  // ── Block A: Solver state ──
  const [solver, setSolver] = useState({
    monthlyPayment: 0, capCost: 0, residualValue: 0, termMonths: 36, moneyFactor: 0,
    solveFor: "monthly_payment" as string,
  });
  const [solveResult, setSolveResult] = useState<SolveResult | null>(null);

  // ── Load offers ──
  const loadOffers = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.get<{ offers: LeaseOffer[] }>(
        `/lease-comparison/offers?country=${countryFilter}${leaseTypeFilter ? `&lease_type=${leaseTypeFilter}` : ""}`
      );
      setOffers(res.offers);
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Load failed"); }
    setLoading(false);
  }, [countryFilter, leaseTypeFilter]);

  useEffect(() => { loadOffers(); }, [loadOffers]);

  const loadFinanceOffers = useCallback(async () => {
    setFinanceLoading(true);
    setFinanceError(null);
    try {
      const response = await api.listMsrpFinanceObservations({
        country: countryFilter.trim() || undefined,
        brand: brandFilter.trim() || undefined,
        jato_model: modelFilter.trim() || undefined,
        has_monthly_payment: true,
        limit: 100,
      });
      setFinanceSummary(response.summary);
      setFinanceTotal(response.total);
      setFinanceOffers(
        response.items.filter((item) => matchesFinanceObservationFilters(item, {
          country: countryFilter,
          brand: brandFilter,
          model: modelFilter,
          financeType: financeTypeFilter,
        })),
      );
    } catch (e: unknown) {
      setFinanceError(e instanceof Error ? e.message : "Load finance observations failed");
      setFinanceOffers([]);
      setFinanceSummary(null);
      setFinanceTotal(0);
    } finally {
      setFinanceLoading(false);
    }
  }, [brandFilter, countryFilter, financeTypeFilter, modelFilter]);

  useEffect(() => { void loadFinanceOffers(); }, [loadFinanceOffers]);

  // ── Block A: Solve ──
  async function runSolver() {
    try {
      const res = await api.post<SolveResult>("/lease-comparison/offers/solve", solver);
      setSolveResult(res);
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Solver failed"); }
  }

  // ── Block B: Create / Edit ──
  function openCreate() { setShowCreate(true); setEditForm({ ...EMPTY_OFFER, countryCode: countryFilter }); }
  function openEdit(offer: LeaseOffer) { setSelectedOffer(offer); setEditForm({ ...offer }); }

  async function saveOffer() {
    setError(null);
    try {
      if (selectedOffer) {
        await api.patch(`/lease-comparison/offers/${selectedOffer.offerId}`, editForm);
      } else {
        await api.post("/lease-comparison/offers", editForm);
      }
      setShowCreate(false); setSelectedOffer(null); setEditForm({});
      loadOffers();
      setNotice("Offer saved");
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Save failed"); }
  }

  async function loadVersions(offerId: string) {
    try {
      const res = await api.get<LeaseOffer>(`/lease-comparison/offers/${offerId}`);
      setVersionList(res.versions || []);
      setShowVersions(offerId);
    } catch { setVersionList([]); }
  }

  async function deleteOffer(id: string) {
    if (!confirm("Delete this offer?")) return;
    try {
      await api.delete(`/lease-comparison/offers/${id}`);
      loadOffers();
      setSelectedOffer(null);
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Delete failed"); }
  }

  function toggleCompare(id: string) {
    setCompareIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else if (next.size < 6) next.add(id);
      return next;
    });
  }

  // ── Block D: AI Summary ──
  async function runAiSummary() {
    if (compareIds.size === 0) return;
    setAiLoading(true); setAiSummary(null);
    try {
      const selectedOffers = offers.filter((o) => compareIds.has(o.offerId));
      const res = await api.post<{ summary: string }>("/lease-comparison/ai-summary", { offers: selectedOffers });
      setAiSummary(res.summary);
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "AI failed"); }
    setAiLoading(false);
  }

  const comparedOffers = offers.filter((o) => compareIds.has(o.offerId));
  const filteredOffers = offers.filter((offer) => {
    const brandMatches = !brandFilter.trim()
      || offer.brand.toLowerCase().includes(brandFilter.trim().toLowerCase());
    const modelNeedle = modelFilter.trim().toLowerCase();
    const modelMatches = !modelNeedle
      || [offer.modelName, offer.version, offer.powertrain].filter(Boolean).join(" ").toLowerCase().includes(modelNeedle);
    return brandMatches && modelMatches;
  });
  const primaryFinanceType = financeSummary
    ? Object.entries(financeSummary.financeTypeCounts).sort((left, right) => right[1] - left[1])[0]?.[0] ?? "-"
    : "-";

  function useFinanceOfferInSolver(item: MsrpFinanceObservation) {
    setSolver((current) => ({
      ...current,
      monthlyPayment: item.monthlyPayment ?? current.monthlyPayment,
      capCost: item.netPriceAfterSubsidy ?? current.capCost,
      residualValue: item.balloonPayment ?? current.residualValue,
      termMonths: item.termMonths ?? current.termMonths,
      solveFor: "money_factor",
    }));
    setNotice(`${getFinanceObservationModelLabel(item)} loaded into solver`);
  }

  return (
    <section className="crud-shell" style={{ padding: 16 }}>
      <header className="crud-hero" style={{ marginBottom: 16 }}>
        <h1>Lease Comparison</h1>
        <p>Compare private leasing, fleet leasing and financial leasing offers across countries. All normalized to EUR.</p>
      </header>

      <DeckFloatingDrawer
        open={filterDeckOpen}
        onOpenChange={setFilterDeckOpen}
        triggerPrimary="Lease filters"
        triggerSecondaryOpen="Hide"
        triggerSecondaryClosed="Open"
        title="Lease Compare Filters"
        eyebrow="Floating Deck"
        ariaLabel="Lease comparison filters"
      >
        <div className="crud-toolbar-grid">
          <div className="filter-group">
            <label htmlFor="lease-filter-country">Country</label>
            <select id="lease-filter-country" value={countryFilter} onChange={(e) => setCountryFilter(e.target.value)}>
              <option value="">All Countries</option>
              <option value="SE">Sweden</option><option value="NO">Norway</option><option value="DK">Denmark</option>
              <option value="FI">Finland</option><option value="DE">Germany</option><option value="NL">Netherlands</option>
            </select>
          </div>
          <div className="filter-group">
            <label htmlFor="lease-filter-brand">Brand</label>
            <input id="lease-filter-brand" value={brandFilter} onChange={(event) => setBrandFilter(event.target.value)} placeholder="e.g. Volvo / BMW" />
          </div>
          <div className="filter-group">
            <label htmlFor="lease-filter-model">Model</label>
            <input id="lease-filter-model" value={modelFilter} onChange={(event) => setModelFilter(event.target.value)} placeholder="e.g. XC60 / iX1" />
          </div>
          <div className="filter-group">
            <label htmlFor="lease-filter-lease-type">Lease type</label>
            <select id="lease-filter-lease-type" value={leaseTypeFilter} onChange={(e) => setLeaseTypeFilter(e.target.value)}>
              <option value="">All Types</option>
              {LEASE_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div className="filter-group">
            <label htmlFor="lease-filter-finance-type">Official finance type</label>
            <input id="lease-filter-finance-type" value={financeTypeFilter} onChange={(event) => setFinanceTypeFilter(event.target.value)} placeholder="private_lease / finance" />
          </div>
        </div>
      </DeckFloatingDrawer>

      {error && <div className="alert alert-error" style={{ marginBottom: 12 }}>{error}</div>}
      {notice && <div className="alert" style={{ marginBottom: 12, background: "#ecfdf5", border: "1px solid #10b981" }}>{notice}</div>}
      {financeError && <div className="alert alert-error" style={{ marginBottom: 12 }}>{financeError}</div>}

      {/* ═══ Block A: Parameter Solver ═══ */}
      <details className="card crud-card" style={{ padding: 16, marginBottom: 16 }}>
        <summary style={{ cursor: "pointer", fontWeight: 700, fontSize: 15 }}>Parameter Solver / 参数反推器</summary>
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 12, alignItems: "flex-end" }}>
          <label style={{ fontSize: 12 }}>
            Solve for
            <select value={solver.solveFor} onChange={(e) => setSolver({ ...solver, solveFor: e.target.value })}
              style={{ display: "block", padding: "4px 8px", borderRadius: 4, border: "1px solid #d1d5db", marginTop: 4 }}>
              {SOLVE_FOR_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </label>
          {solver.solveFor !== "monthly_payment" && (
            <label style={{ fontSize: 12 }}>Monthly Payment<input type="number" value={solver.monthlyPayment || ""}
              onChange={(e) => setSolver({ ...solver, monthlyPayment: Number(e.target.value) })}
              style={inputStyle} /></label>
          )}
          {solver.solveFor !== "cap_cost" && (
            <label style={{ fontSize: 12 }}>Cap Cost<input type="number" value={solver.capCost || ""}
              onChange={(e) => setSolver({ ...solver, capCost: Number(e.target.value) })}
              style={inputStyle} /></label>
          )}
          {solver.solveFor !== "residual_value" && (
            <label style={{ fontSize: 12 }}>RV<input type="number" value={solver.residualValue || ""}
              onChange={(e) => setSolver({ ...solver, residualValue: Number(e.target.value) })}
              style={inputStyle} /></label>
          )}
          <label style={{ fontSize: 12 }}>Term (mo)<input type="number" value={solver.termMonths}
            onChange={(e) => setSolver({ ...solver, termMonths: Number(e.target.value) })}
            style={inputStyle} /></label>
          {solver.solveFor !== "money_factor" && (
            <label style={{ fontSize: 12 }}>MF<input type="number" step="0.0001" value={solver.moneyFactor || ""}
              onChange={(e) => setSolver({ ...solver, moneyFactor: Number(e.target.value) })}
              style={inputStyle} /></label>
          )}
          <button className="btn btn-sm btn-primary" onClick={runSolver}>Solve</button>
        </div>
        {solveResult && (
          <div style={{ marginTop: 12, padding: 8, background: "#f0fdf4", borderRadius: 6, fontSize: 13 }}>
            {Object.entries(solveResult).map(([k, v]) => (
              <span key={k} style={{ marginRight: 16 }}>
                <strong>{k}:</strong> {typeof v === "number" ? v.toLocaleString(undefined, { maximumFractionDigits: 4 }) : String(v)}
              </span>
            ))}
          </div>
        )}
      </details>

      {/* ═══ Block B + C: Offer Manager + Comparison Board ═══ */}
      <div style={{ display: "flex", gap: 16, marginBottom: 16, alignItems: "center", flexWrap: "wrap" }}>
        <span style={{ fontSize: 13, color: "#64748b" }}>
          {countryFilter || "All countries"} · {brandFilter || "All brands"} · {modelFilter || "All models"} · {leaseTypeFilter || "All lease types"}
        </span>
        {isEditor && <button className="btn btn-sm btn-primary" onClick={openCreate}>+ New Offer</button>}
        {compareIds.size > 0 && (
          <>
            <span style={{ fontSize: 13, color: "#64748b" }}>{compareIds.size} selected</span>
            <button className="btn btn-sm btn-primary" onClick={runAiSummary} disabled={aiLoading}>
              {aiLoading ? "Analyzing..." : "AI Summary"}
            </button>
            <button className="btn btn-sm btn-ghost" onClick={() => setCompareIds(new Set())}>Clear</button>
          </>
        )}
        <button className="btn btn-sm btn-ghost" onClick={() => setShowFormula(!showFormula)}>
          {showFormula ? "Hide" : "Show"} Formulas
        </button>
        <button className={`btn btn-sm ${viewMode === "cards" ? "btn-primary" : "btn-ghost"}`}
          onClick={() => setViewMode("cards")}>Cards</button>
        <button className={`btn btn-sm ${viewMode === "table" ? "btn-primary" : "btn-ghost"}`}
          onClick={() => setViewMode("table")}>Table</button>
      </div>

      <div className="card crud-card" style={{ padding: 16, marginBottom: 16 }}>
        <div className="detail-section-head">
          <div>
            <div className="card-title">Official Finance Observations</div>
            <p className="section-note">从 MSRP 抓取 pipeline 入库的月供 / lease / finance offer；用于和手工 lease offer 一起判断真实市场可用方案。</p>
          </div>
          <button type="button" className="btn btn-sm btn-secondary" onClick={() => void loadFinanceOffers()} disabled={financeLoading}>
            {financeLoading ? "Loading" : "Refresh"}
          </button>
        </div>
        {financeSummary && (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 10, marginBottom: 12 }}>
            <div style={financeMetricStyle}>
              <span style={financeMetricLabelStyle}>API total</span>
              <strong>{financeTotal.toLocaleString()}</strong>
            </div>
            <div style={financeMetricStyle}>
              <span style={financeMetricLabelStyle}>Monthly EUR</span>
              <strong>{formatFinanceCurrencyRange(financeSummary.monthlyPaymentEurMin, financeSummary.monthlyPaymentEurMax)}</strong>
            </div>
            <div style={financeMetricStyle}>
              <span style={financeMetricLabelStyle}>Net price rows</span>
              <strong>{financeSummary.netPriceAfterSubsidyCount.toLocaleString()}</strong>
            </div>
            <div style={financeMetricStyle}>
              <span style={financeMetricLabelStyle}>Subsidy rows</span>
              <strong>{financeSummary.subsidyObservationCount.toLocaleString()}</strong>
            </div>
            <div style={financeMetricStyle}>
              <span style={financeMetricLabelStyle}>Main type</span>
              <strong>{primaryFinanceType}</strong>
            </div>
          </div>
        )}
        {financeOffers.length > 0 ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 12 }}>
            {financeOffers.map((item) => (
              <div key={item.financeObservationId} className="card crud-card" style={{ padding: 14 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "flex-start" }}>
                  <div>
                    <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 6 }}>
                      <span className="badge badge-active">{item.country}</span>
                      <span className="badge badge-warning">{getFinanceObservationLabel(item)}</span>
                      <span className={`badge ${getFinanceObservationValidityBadgeClass(item)}`}>
                        {getFinanceObservationValidityLabel(item)}
                      </span>
                    </div>
                    <strong>{getFinanceObservationModelLabel(item)}</strong>
                    <div style={{ fontSize: 12, color: "#64748b", marginTop: 2 }}>{item.officialModel} {item.officialTrim}</div>
                  </div>
                  <div style={{ textAlign: "right" }}>
                    <strong>{formatFinanceCurrency(item.monthlyPayment, item.currency)}</strong>
                    <div style={{ fontSize: 12, color: "#64748b" }}>{formatFinanceCurrency(item.monthlyPaymentEur)}</div>
                  </div>
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 12px", fontSize: 12, marginTop: 12 }}>
                  <span>Term {item.termMonths ? `${item.termMonths} mo` : "-"}</span>
                  <span>Mileage {item.annualMileageLimit ? `${formatFinanceNumber(item.annualMileageLimit)} km/y` : "-"}</span>
                  <span>Down {formatFinanceCurrency(item.downPayment, item.currency)}</span>
                  <span>APR {item.apr !== null ? `${formatFinanceNumber(item.apr, 2)}%` : "-"}</span>
                  <span>Net {formatFinanceCurrency(item.netPriceAfterSubsidyEur)}</span>
                  <span>Valid {formatFinanceDate(item.offerValidUntil)}</span>
                  <span>Observed {formatFinanceDate(item.observedAtUtc)}</span>
                </div>
                <div style={{ display: "flex", gap: 8, marginTop: 12, alignItems: "center", justifyContent: "space-between" }}>
                  <button type="button" className="btn btn-xs btn-secondary" onClick={() => useFinanceOfferInSolver(item)}>
                    Use in solver
                  </button>
                  {item.sourceUrl ? (
                    <a href={item.sourceUrl} target="_blank" rel="noreferrer" className="review-table-link">Source</a>
                  ) : (
                    <span style={{ fontSize: 12, color: "#94a3b8" }}>No source</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="crud-empty-state">{financeLoading ? "Loading official finance observations" : "No official finance observations for current filters"}</div>
        )}
      </div>

      {/* ── Table View ── */}
      {viewMode === "table" && (
        <div className="va-table-scroll" style={{ marginBottom: 16 }}>
          <table className="data-table" style={{ fontSize: 12 }}>
            <thead><tr>
              <th>Country</th><th>Brand</th><th>Model</th><th>Type</th><th>Provider</th>
              <th>Monthly (orig)</th><th>Monthly EUR</th><th>Effective EUR</th>
              <th>Down EUR</th><th>Term</th><th>Mileage</th>
              <th>Cap Cost EUR</th><th>RV EUR</th><th>RV%</th><th>APR%</th>
              <th>RV Guar.</th><th>Service</th><th>TCO EUR</th><th>Risk</th>
              <th>Status</th><th></th>
            </tr></thead>
            <tbody>
              {filteredOffers.map((o) => (
                <tr key={o.offerId}>
                  <td>{o.countryCode}</td><td>{o.brand}</td><td>{o.modelName} {o.version || ""}</td>
                  <td>{o.leaseType}</td><td>{o.provider || "-"}</td>
                  <td>{o.monthlyPayment?.toLocaleString()} {o.currency}</td>
                  <td>{o.monthlyPaymentEur?.toLocaleString()}</td>
                  <td>{o.effectiveMonthlyEur?.toLocaleString()}</td>
                  <td>{o.downPaymentEur?.toLocaleString()}</td>
                  <td>{o.termMonths}mo</td><td>{o.mileagePerYear?.toLocaleString()}</td>
                  <td>{o.capCostEur?.toLocaleString()}</td>
                  <td>{o.residualValueEur?.toLocaleString()}</td>
                  <td>{o.residualValuePercent}%</td>
                  <td>{o.aprPercent}%{o.aprSource === "reverse_calculated" ? "≈" : ""}</td>
                  <td>{o.rvGuaranteed === false ? "No" : o.rvGuaranteed === true ? "Yes" : "?"}</td>
                  <td>{o.serviceIncluded ? "Yes" : "No"}</td>
                  <td>{o.totalContractCostEur?.toLocaleString()}</td>
                  <td><span style={{ color: o.riskLevel === "high" ? "#dc2626" : o.riskLevel === "medium" ? "#d97706" : "#16a34a" }}>{o.riskLevel}</span></td>
                  <td>{o.status}</td>
                  <td>
                    <input type="checkbox" checked={compareIds.has(o.offerId)} onChange={() => toggleCompare(o.offerId)} />
                    {isEditor && <button className="btn btn-sm btn-ghost" style={{ fontSize: 10 }} onClick={() => openEdit(o)}>Edit</button>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* ── Offer cards grid ── */}
      {viewMode === "cards" && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(360px, 1fr))", gap: 12, marginBottom: 16 }}>
          {filteredOffers.map((o) => (
            <div key={o.offerId} className="card crud-card"
              style={{
                padding: 16, cursor: "pointer", position: "relative",
                border: compareIds.has(o.offerId) ? "2px solid #3b82f6" : (o.riskLevel === "high" ? "1px solid #f59e0b" : undefined),
              }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
              <div>
                <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                  <span style={{ fontSize: 11, background: "#e0e7ff", color: "#3730a3", padding: "2px 6px", borderRadius: 4 }}>{o.leaseType}</span>
                  <span style={{ fontSize: 11, background: "#fef3c7", color: "#92400e", padding: "2px 6px", borderRadius: 4 }}>{o.countryCode}</span>
                  {o.powertrain && <span style={{ fontSize: 11, background: "#d1fae5", color: "#065f46", padding: "2px 6px", borderRadius: 4 }}>{o.powertrain}</span>}
                  <span style={{ fontSize: 11, background: o.status === "active" ? "#dcfce7" : "#f1f5f9", color: o.status === "active" ? "#166534" : "#64748b", padding: "2px 6px", borderRadius: 4 }}>{o.status}</span>
                </div>
                <div style={{ fontWeight: 700, fontSize: 15, marginTop: 6 }}>{o.brand} {o.modelName}</div>
                <div style={{ fontSize: 12, color: "#64748b" }}>{o.version} {o.provider ? `· ${o.provider}` : ""}</div>
              </div>
              <input type="checkbox" checked={compareIds.has(o.offerId)} onChange={() => toggleCompare(o.offerId)}
                style={{ margin: 0 }} title="Add to compare" />
            </div>

            {/* Core financials */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 16px", fontSize: 13, marginBottom: 8 }}>
              <div><strong>Monthly:</strong> {o.monthlyPayment?.toLocaleString()} {o.currency}</div>
              <div><strong>EUR:</strong> {o.monthlyPaymentEur?.toLocaleString()} EUR</div>
              {o.effectiveMonthlyEur && o.effectiveMonthlyEur !== o.monthlyPaymentEur && (
                <div style={{ gridColumn: "1 / -1" }}><strong>Effective:</strong> {o.effectiveMonthlyEur.toLocaleString()} EUR/mo</div>
              )}
              <div><strong>Down:</strong> {o.downPayment?.toLocaleString()} {o.currency}</div>
              <div><strong>Term:</strong> {o.termMonths}mo / {o.mileagePerYear?.toLocaleString()} km</div>
              <div><strong>Cap Cost:</strong> {o.capCostEur?.toLocaleString()} EUR</div>
              <div><strong>RV:</strong> {o.residualValueEur?.toLocaleString()} EUR ({o.residualValuePercent}%)</div>
              <div><strong>APR:</strong> {o.aprPercent}% {o.aprSource === "reverse_calculated" ? "≈" : ""}</div>
              <div><strong>TCO:</strong> {o.totalContractCostEur?.toLocaleString()} EUR</div>
            </div>

            {/* Flags */}
            <div style={{ display: "flex", gap: 4, flexWrap: "wrap", fontSize: 11 }}>
              {o.rvGuaranteed === false && <span style={{ color: "#dc2626", background: "#fef2f2", padding: "1px 6px", borderRadius: 3 }}>RV not guaranteed</span>}
              {!o.serviceIncluded && <span style={{ color: "#d97706", background: "#fffbeb", padding: "1px 6px", borderRadius: 3 }}>Service?</span>}
              {!o.vatIncluded && <span style={{ color: "#64748b", background: "#f1f5f9", padding: "1px 6px", borderRadius: 3 }}>excl. VAT</span>}
              {o.riskLevel === "high" && <span style={{ color: "#dc2626", background: "#fef2f2", padding: "1px 6px", borderRadius: 3, fontWeight: 600 }}>High Risk</span>}
            </div>

            {isEditor && (
              <div style={{ marginTop: 8, display: "flex", gap: 6 }}>
                <button className="btn btn-sm btn-ghost" style={{ fontSize: 11 }} onClick={() => openEdit(o)}>Edit</button>
                {user?.role === "admin" && (
                  <button className="btn btn-sm btn-ghost" style={{ fontSize: 11, color: "#dc2626" }} onClick={() => deleteOffer(o.offerId)}>Delete</button>
                )}
              </div>
            )}
            </div>
          ))}
        </div>
      )}

      {/* ═══ Block E: Formula Drawer ═══ */}
      {showFormula && (
        <div className="card crud-card" style={{ padding: 16, marginBottom: 16, fontSize: 13, lineHeight: 1.8 }}>
          <h3 style={{ margin: "0 0 8px" }}>Formulas & Calculation</h3>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
            <div>
              <strong>Monthly Payment</strong>
              <pre style={{ background: "#f8fafc", padding: 8, borderRadius: 4, fontSize: 12 }}>
                P = (Cap Cost - RV) / Months + (Cap Cost + RV) × MF{"\n"}
                MF = APR / 2400{"\n"}
                APR = MF × 2400
              </pre>
            </div>
            <div>
              <strong>Effective Monthly</strong>
              <pre style={{ background: "#f8fafc", padding: 8, borderRadius: 4, fontSize: 12 }}>
                Eff = (P × M + Upfront) / M{"\n"}
                (excl. refundable deposit)
              </pre>
            </div>
          </div>
        </div>
      )}

      {/* ═══ Block D: AI Summary ═══ */}
      {aiSummary && (
        <div className="card crud-card" style={{ padding: 16, marginBottom: 16, background: "#faf5ff", border: "1px solid #c4b5fd" }}>
          <h3 style={{ margin: "0 0 8px" }}>AI Summary</h3>
          <div style={{ fontSize: 13, whiteSpace: "pre-wrap", lineHeight: 1.7 }}>{aiSummary}</div>
        </div>
      )}

      {/* ═══ Create / Edit Modal ═══ */}
      {(showCreate || selectedOffer) && (
        <div style={{ position: "fixed", inset: 0, zIndex: 10000, background: "rgba(0,0,0,0.4)", display: "flex", alignItems: "center", justifyContent: "center" }}
          onClick={() => { setShowCreate(false); setSelectedOffer(null); }}>
          <div style={{ background: "#fff", borderRadius: 10, padding: 24, width: 560, maxHeight: "90vh", overflow: "auto", boxShadow: "0 8px 32px rgba(0,0,0,0.2)" }}
            onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: "0 0 16px" }}>{selectedOffer ? "Edit Offer" : "New Offer"}</h3>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px 12px", fontSize: 13 }}>
              {field("Country", "countryCode")}
              {field("Currency", "currency")}
              {field("Brand", "brand")}
              {field("Model", "modelName")}
              {field("Version", "version")}
              {field("Powertrain", "powertrain")}
              {field("Segment", "segment")}
              <label>Lease Type<select value={editForm.leaseType || "private"}
                onChange={(e) => setEditForm({ ...editForm, leaseType: e.target.value as never })} style={inputStyle}>
                {LEASE_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
              </select></label>
              {field("Provider", "provider")}
              {fieldNum("Monthly Payment", "monthlyPayment")}
              {fieldNum("Down Payment", "downPayment")}
              {fieldNum("Cap Cost", "capCost")}
              {fieldNum("RV", "residualValue")}
              {fieldNum("RV %", "residualValuePercent")}
              {fieldNum("APR %", "aprPercent")}
              {fieldNum("Money Factor", "moneyFactor")}
              {fieldNum("Term (mo)", "termMonths")}
              {fieldNum("Mileage/yr", "mileagePerYear")}
              {fieldNum("FX Rate to EUR", "fxRateToEur")}
              <label>Upfront Treatment<select value={editForm.upfrontTreatment || ""}
                onChange={(e) => setEditForm({ ...editForm, upfrontTreatment: e.target.value })} style={inputStyle}>
                <option value="">—</option>
                {UPFRONT_TREATMENTS.map((t) => <option key={t} value={t}>{t}</option>)}
              </select></label>
              <label>Status<select value={editForm.status || "draft"}
                onChange={(e) => setEditForm({ ...editForm, status: e.target.value as never })} style={inputStyle}>
                {STATUSES.map((s) => <option key={s} value={s}>{s}</option>)}
              </select></label>
              <label>APR Source<select value={editForm.aprSource || "manual"}
                onChange={(e) => setEditForm({ ...editForm, aprSource: e.target.value })} style={inputStyle}>
                <option value="official">Official</option>
                <option value="reverse_calculated">Reverse Calculated</option>
                <option value="manual">Manual</option>
              </select></label>
              {field("Provider", "provider")}
              {field("Notes", "notes")}
            </div>
            {/* Boolean toggles */}
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 12, fontSize: 12 }}>
              {boolToggle("RV Guaranteed", "rvGuaranteed")}
              {boolToggle("Service", "serviceIncluded")}
              {boolToggle("Insurance", "insuranceIncluded")}
              {boolToggle("Tyre", "tyreIncluded")}
              {boolToggle("VAT Included", "vatIncluded")}
            </div>
            <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 16 }}>
              <button className="btn btn-sm btn-ghost" onClick={() => { setShowCreate(false); setSelectedOffer(null); }}>Cancel</button>
              <button className="btn btn-sm btn-primary" onClick={saveOffer}>Save</button>
            </div>
          </div>
        </div>
      )}
    </section>
  );

  function field(label: string, key: string) {
    return (
      <label style={{ fontSize: 12 }}>{label}
        <input type="text" value={(editForm as any)[key] ?? ""}
          onChange={(e) => setEditForm({ ...editForm, [key]: e.target.value })}
          style={inputStyle} />
      </label>
    );
  }

  function fieldNum(label: string, key: string) {
    return (
      <label style={{ fontSize: 12 }}>{label}
        <input type="number" step="any" value={(editForm as any)[key] ?? ""}
          onChange={(e) => setEditForm({ ...editForm, [key]: e.target.value ? Number(e.target.value) : null })}
          style={inputStyle} />
      </label>
    );
  }

  function boolToggle(label: string, key: string) {
    return (
      <label style={{ cursor: "pointer", display: "flex", alignItems: "center", gap: 4 }}>
        <input type="checkbox" checked={!!(editForm as any)[key]}
          onChange={(e) => setEditForm({ ...editForm, [key]: e.target.checked })} />
        {label}
      </label>
    );
  }
}

const inputStyle: React.CSSProperties = {
  display: "block", width: "100%", marginTop: 2, padding: "4px 8px",
  borderRadius: 4, border: "1px solid #d1d5db", fontSize: 12,
};

const financeMetricStyle: React.CSSProperties = {
  border: "1px solid #e2e8f0",
  borderRadius: 6,
  padding: "8px 10px",
  minHeight: 58,
  display: "flex",
  flexDirection: "column",
  justifyContent: "center",
  gap: 3,
};

const financeMetricLabelStyle: React.CSSProperties = {
  color: "#64748b",
  fontSize: 11,
  textTransform: "uppercase",
};
