import { useState } from "react";

import { api } from "../../api/client";
import type {
  ColourSurchargeRepriceApplyResult,
  ColourSurchargeRepriceAudit,
  ColourSurchargeRepriceItem,
} from "../../types/orderGenius";

type BomFobRepriceAuditCardProps = {
  onApplied?: () => void | Promise<void>;
};

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isMoneyChange(item: ColourSurchargeRepriceItem): boolean {
  return item.category === "auto_reprice"
    && item.currentFinalFobEur !== item.expectedFinalFobEur;
}

function formatEur(value: number | null | undefined): string {
  return value == null ? "-" : value.toLocaleString();
}

function formatException(item: ColourSurchargeRepriceItem): string {
  if (item.category === "ambiguous_base") {
    const candidates = item.singleBaseCandidates ?? [];
    return candidates.length > 0
      ? `Single bases conflict: ${candidates.map(formatEur).join(" / ")}`
      : "More than one Single base exists for this template and country";
  }
  if (item.category === "missing_base") return "No Single base exists for this template and country";
  if (item.category === "missing_tier") return "Colour tier is not configured in BOM Admin";
  if (item.category === "missing_rule") return "No surcharge is configured for the saved tier";
  return item.reason?.replaceAll("_", " ") ?? "Review required";
}

export function BomFobRepriceAuditCard({ onApplied }: BomFobRepriceAuditCardProps) {
  const [audit, setAudit] = useState<ColourSurchargeRepriceAudit | null>(null);
  const [applyResult, setApplyResult] = useState<ColourSurchargeRepriceApplyResult | null>(null);
  const [reviewOpen, setReviewOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [applying, setApplying] = useState(false);
  const [error, setError] = useState("");

  const moneyChanges = audit?.items.filter(isMoneyChange) ?? [];
  const metadataOnly = audit
    ? Math.max(0, audit.summary.autoReprice - moneyChanges.length)
    : 0;
  const exceptions = audit?.items.filter((item) => (
    item.category === "ambiguous_base"
    || item.category === "missing_base"
    || item.category === "missing_tier"
    || item.category === "missing_rule"
  )) ?? [];

  const refreshAudit = async (): Promise<ColourSurchargeRepriceAudit | null> => {
    setLoading(true);
    setError("");
    try {
      const nextAudit = await api.auditOrderGeniusColourSurchargeReprice();
      setAudit(nextAudit);
      return nextAudit;
    } catch (cause) {
      setError(getErrorMessage(cause));
      return null;
    } finally {
      setLoading(false);
    }
  };

  const applySafeRepricing = async () => {
    if (!audit || audit.summary.autoReprice === 0) return;
    setApplying(true);
    setError("");
    try {
      const result = await api.applyOrderGeniusColourSurchargeReprice(audit.fingerprint);
      setApplyResult(result);
      await refreshAudit();
      await onApplied?.();
    } catch (cause) {
      setError(getErrorMessage(cause));
    } finally {
      setApplying(false);
    }
  };

  return (
    <div className="bom-admin-tool-tile" style={{ minHeight: 124 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 7 }}>
        <span style={{ fontSize: 11, fontWeight: 800, color: "#334155" }}>FOB Colour Reprice</span>
        <button
          className="btn btn-sm btn-ghost"
          type="button"
          disabled={loading || applying}
          onClick={() => void refreshAudit()}
        >
          {loading ? "Checking..." : "Refresh FOB Audit"}
        </button>
      </div>
      <div style={{ fontSize: 10, color: "#64748b", lineHeight: 1.4 }}>
        Uses each saved colour tier with the unique Single base for the same BOM template and country. It never moves a colour between tiers.
      </div>
      {audit ? (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 4, marginTop: 8 }}>
            <div style={{ padding: 5, background: "#fff7ed", color: "#9a3412", fontSize: 9 }}><strong>{moneyChanges.length}</strong><br />price changes</div>
            <div style={{ padding: 5, background: "#f1f5f9", color: "#475569", fontSize: 9 }}><strong>{metadataOnly}</strong><br />metadata only</div>
            <div style={{ padding: 5, background: exceptions.length ? "#fef2f2" : "#f0fdf4", color: exceptions.length ? "#b91c1c" : "#15803d", fontSize: 9 }}><strong>{exceptions.length}</strong><br />need review</div>
          </div>
          <button
            className="btn btn-sm btn-primary"
            type="button"
            onClick={() => setReviewOpen(true)}
            style={{ marginTop: 7, width: "100%" }}
          >
            Review audit
          </button>
        </>
      ) : (
        <div style={{ marginTop: 8, fontSize: 10, color: "#94a3b8" }}>Not checked yet. Refresh is read-only.</div>
      )}
      {applyResult ? (
        <div style={{ marginTop: 7, fontSize: 10, color: "#0f766e", fontWeight: 700 }}>
          Applied: {applyResult.totals.updated} updated · {applyResult.totals.unchanged} unchanged · {applyResult.totals.skipped} skipped.
        </div>
      ) : null}
      {error ? <div style={{ marginTop: 7, fontSize: 10, color: "#b91c1c" }}>{error}</div> : null}

      {reviewOpen && audit ? (
        <div className="bom-finance-modal-backdrop" onClick={() => { if (!applying) setReviewOpen(false); }}>
          <div className="bom-colour-code-edit-modal-shell" onClick={(event) => event.stopPropagation()}>
            <section className="bom-colour-code-edit-card" role="dialog" aria-modal="true" aria-label="FOB colour reprice audit">
              <div className="bom-colour-code-edit-head">
                <div>
                  <span className="bom-finance-eyebrow">BOM ADMIN · FOB AUDIT</span>
                  <h4>Review tier-based FOB repricing</h4>
                  <p>{audit.summary.rows} active FOB rows checked · colour tiers remain unchanged</p>
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 8, marginBottom: 10 }}>
                <div><strong>{moneyChanges.length}</strong><br /><span style={{ fontSize: 10 }}>price changes</span></div>
                <div><strong>{metadataOnly}</strong><br /><span style={{ fontSize: 10 }}>metadata only</span></div>
                <div><strong>{audit.summary.alreadyCorrect}</strong><br /><span style={{ fontSize: 10 }}>already correct</span></div>
                <div><strong>{exceptions.length}</strong><br /><span style={{ fontSize: 10 }}>need review</span></div>
              </div>
              <div style={{ padding: 8, background: "#eff6ff", color: "#1d4ed8", fontSize: 10, lineHeight: 1.4 }}>
                Formula: country Single base + surcharge for the saved Single / Dual / Special tier. Colour name, code and swatch never choose the tier.
              </div>
              {moneyChanges.length > 0 ? (
                <div style={{ marginTop: 10, maxHeight: "24vh", overflowY: "auto", border: "1px solid #fed7aa" }}>
                  {moneyChanges.slice(0, 30).map((item) => (
                    <div key={`${item.materialCode}|${item.countryCode}|${item.paymentTermCode ?? ""}`} style={{ padding: 7, borderBottom: "1px solid #ffedd5", fontSize: 11 }}>
                      <strong>{item.materialCode} · {item.countryCode}</strong> · {item.colourTier ?? "tier missing"} · {formatEur(item.currentFinalFobEur)} → {formatEur(item.expectedFinalFobEur)}
                    </div>
                  ))}
                  {moneyChanges.length > 30 ? <div style={{ padding: 7, fontSize: 10 }}>+ {moneyChanges.length - 30} more safe price changes</div> : null}
                </div>
              ) : null}
              {exceptions.length > 0 ? (
                <div style={{ marginTop: 10, maxHeight: "24vh", overflowY: "auto", border: "1px solid #fecaca" }}>
                  {exceptions.slice(0, 30).map((item) => (
                    <div key={`${item.materialCode}|${item.countryCode}|${item.paymentTermCode ?? ""}`} style={{ padding: 7, borderBottom: "1px solid #fee2e2", fontSize: 11 }}>
                      <strong>{item.bomTemplate || item.materialCode} · {item.countryCode}</strong> · {item.category.replaceAll("_", " ")} · {formatException(item)}
                    </div>
                  ))}
                  {exceptions.length > 30 ? <div style={{ padding: 7, fontSize: 10 }}>+ {exceptions.length - 30} more rows needing review</div> : null}
                </div>
              ) : null}
              {error ? <div className="bom-colour-code-edit-error">{error}</div> : null}
              <div className="bom-finance-action-bar">
                <button
                  type="button"
                  className="btn btn-sm btn-primary"
                  disabled={applying || audit.summary.autoReprice === 0}
                  onClick={() => void applySafeRepricing()}
                >
                  {applying ? "Applying..." : `Apply ${audit.summary.autoReprice} safe rows`}
                </button>
                <button type="button" className="btn btn-sm btn-ghost" disabled={applying} onClick={() => setReviewOpen(false)}>Close</button>
              </div>
            </section>
          </div>
        </div>
      ) : null}
    </div>
  );
}
