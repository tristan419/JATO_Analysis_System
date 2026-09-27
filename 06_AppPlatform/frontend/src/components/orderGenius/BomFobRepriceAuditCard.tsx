import { useState } from "react";
import { createPortal } from "react-dom";

import { api } from "../../api/client";
import type {
  ColourSurchargeRepriceApplyResult,
  ColourSurchargeRepriceAudit,
  ColourSurchargeRepriceCategory,
  ColourSurchargeRepriceItem,
} from "../../types/orderGenius";

export type BomFobAuditTemplateTarget = {
  bomTemplate: string;
  brand: string;
  modelName: string | null;
  version: string | null;
};

type BomFobRepriceAuditCardProps = {
  onApplied?: () => void | Promise<void>;
  onOpenBomTemplate?: (target: BomFobAuditTemplateTarget) => void | Promise<void>;
};

type RepriceExceptionGroup = {
  key: string;
  category: ColourSurchargeRepriceCategory;
  bomTemplate: string;
  brand: string;
  modelName: string | null;
  version: string | null;
  materialCodes: string[];
  colourCodes: string[];
  colourTiers: string[];
  countryCodes: string[];
  singleBaseConflicts: Array<{ countryCode: string; candidates: number[] }>;
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

function exceptionLabel(category: ColourSurchargeRepriceCategory): string {
  if (category === "ambiguous_base") return "Ambiguous Single base";
  if (category === "missing_base") return "Missing Single base";
  if (category === "missing_tier") return "Missing colour tier";
  if (category === "missing_rule") return "Missing surcharge rule";
  return category.replaceAll("_", " ");
}

function exceptionGuidance(group: RepriceExceptionGroup): string {
  if (group.category === "ambiguous_base") {
    return "This template has more than one saved Single base in the affected countries. Open the template, keep the intended base colour in Single, move any misclassified charged colour to Dual or Special, or save one template-country Single base. Then refresh the audit.";
  }
  if (group.category === "missing_base") {
    return "Open the template and save its country Single base. The audit will not derive a Dual or Special final price without that base.";
  }
  if (group.category === "missing_tier") {
    return "Open the template and place the colour in Single, Dual or Special. The audit never infers a tier from its name, code or swatch.";
  }
  if (group.category === "missing_rule") {
    return "The saved tier has no applicable surcharge. Configure the brand/tier rule or an explicit custom rule, then refresh the audit.";
  }
  return "Review this template in BOM Admin, then refresh the audit.";
}

function sortedValues(values: Set<string>): string[] {
  return [...values].filter(Boolean).sort((left, right) => left.localeCompare(right));
}

function groupExceptions(items: ColourSurchargeRepriceItem[]): RepriceExceptionGroup[] {
  const groups = new Map<string, {
    category: ColourSurchargeRepriceCategory;
    bomTemplate: string;
    brand: string;
    modelName: string | null;
    version: string | null;
    materialCodes: Set<string>;
    colourCodes: Set<string>;
    colourTiers: Set<string>;
    countryCodes: Set<string>;
    singleBaseCandidatesByCountry: Map<string, Set<number>>;
  }>();

  for (const item of items) {
    const bomTemplate = item.bomTemplate?.trim() || item.materialCode;
    const key = [item.category, bomTemplate, item.brand, item.modelName ?? "", item.version ?? ""].join("|");
    const group = groups.get(key) ?? {
      category: item.category,
      bomTemplate,
      brand: item.brand,
      modelName: item.modelName,
      version: item.version,
      materialCodes: new Set<string>(),
      colourCodes: new Set<string>(),
      colourTiers: new Set<string>(),
      countryCodes: new Set<string>(),
      singleBaseCandidatesByCountry: new Map<string, Set<number>>(),
    };
    group.materialCodes.add(item.materialCode);
    if (item.colourCode) group.colourCodes.add(item.colourCode);
    if (item.colourTier) group.colourTiers.add(item.colourTier);
    if (item.countryCode) group.countryCodes.add(item.countryCode);
    const countryCandidates = group.singleBaseCandidatesByCountry.get(item.countryCode) ?? new Set<number>();
    for (const candidate of item.singleBaseCandidates ?? []) {
      countryCandidates.add(candidate);
    }
    group.singleBaseCandidatesByCountry.set(item.countryCode, countryCandidates);
    groups.set(key, group);
  }

  return [...groups.entries()].map(([key, group]) => ({
    key,
    category: group.category,
    bomTemplate: group.bomTemplate,
    brand: group.brand,
    modelName: group.modelName,
    version: group.version,
    materialCodes: sortedValues(group.materialCodes),
    colourCodes: sortedValues(group.colourCodes),
    colourTiers: sortedValues(group.colourTiers),
    countryCodes: sortedValues(group.countryCodes),
    singleBaseConflicts: [...group.singleBaseCandidatesByCountry.entries()]
      .map(([countryCode, candidates]) => ({
        countryCode,
        candidates: [...candidates].sort((left, right) => left - right),
      }))
      .sort((left, right) => left.countryCode.localeCompare(right.countryCode)),
  })).sort((left, right) => left.bomTemplate.localeCompare(right.bomTemplate));
}

export function BomFobRepriceAuditCard({ onApplied, onOpenBomTemplate }: BomFobRepriceAuditCardProps) {
  const [audit, setAudit] = useState<ColourSurchargeRepriceAudit | null>(null);
  const [applyResult, setApplyResult] = useState<ColourSurchargeRepriceApplyResult | null>(null);
  const [reviewOpen, setReviewOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [applying, setApplying] = useState(false);
  const [openingTemplateKey, setOpeningTemplateKey] = useState<string | null>(null);
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
  const exceptionGroups = groupExceptions(exceptions);

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

  const openBomTemplate = async (group: RepriceExceptionGroup) => {
    if (!onOpenBomTemplate) return;
    setOpeningTemplateKey(group.key);
    setError("");
    try {
      await onOpenBomTemplate({
        bomTemplate: group.bomTemplate,
        brand: group.brand,
        modelName: group.modelName,
        version: group.version,
      });
      setReviewOpen(false);
    } catch (cause) {
      setError(getErrorMessage(cause));
    } finally {
      setOpeningTemplateKey(null);
    }
  };

  const reviewDialog = reviewOpen && audit && typeof document !== "undefined"
    ? createPortal(
      <div className="bom-finance-modal-backdrop" onClick={() => { if (!applying) setReviewOpen(false); }}>
        <div className="bom-fob-audit-modal-shell" onClick={(event) => event.stopPropagation()}>
          <section className="bom-fob-audit-card" role="dialog" aria-modal="true" aria-label="FOB colour reprice audit">
            <header className="bom-fob-audit-header">
              <div>
                <span className="bom-finance-eyebrow">BOM ADMIN · FOB AUDIT</span>
                <h4>Review tier-based FOB repricing</h4>
                <p>{audit.summary.rows} active FOB rows checked · colour tiers remain unchanged</p>
              </div>
              <button type="button" className="btn btn-sm btn-ghost" disabled={applying} onClick={() => setReviewOpen(false)}>Close</button>
            </header>

            <div className="bom-fob-audit-summary">
              <div><strong>{moneyChanges.length}</strong><span>price changes</span></div>
              <div><strong>{metadataOnly}</strong><span>metadata only</span></div>
              <div><strong>{audit.summary.alreadyCorrect}</strong><span>already correct</span></div>
              <div className={exceptionGroups.length > 0 ? "is-warning" : "is-clear"}><strong>{exceptionGroups.length}</strong><span>issues to resolve</span></div>
            </div>

            <div className="bom-fob-audit-formula">
              Formula: country Single base + surcharge for the saved Single / Dual / Special tier. Colour name, code and swatch never choose the tier.
            </div>

            <div className="bom-fob-audit-body">
              {exceptionGroups.length > 0 ? (
                <section className="bom-fob-audit-section" aria-label="Issues to resolve">
                  <div className="bom-fob-audit-section-head">
                    <div>
                      <h5>Resolve before applying</h5>
                      <p>{exceptions.length} affected rows grouped into {exceptionGroups.length} actionable issue{exceptionGroups.length === 1 ? "" : "s"}.</p>
                    </div>
                    <button type="button" className="btn btn-sm btn-secondary" disabled={loading || applying} onClick={() => void refreshAudit()}>
                      {loading ? "Refreshing..." : "Refresh after correction"}
                    </button>
                  </div>
                  <div className="bom-fob-audit-issue-list">
                    {exceptionGroups.map((group) => (
                      <article key={group.key} className="bom-fob-audit-issue">
                        <div className="bom-fob-audit-issue-main">
                          <div className="bom-fob-audit-issue-title">
                            <strong>{group.bomTemplate}</strong>
                            <span>{exceptionLabel(group.category)}</span>
                          </div>
                          <div className="bom-fob-audit-issue-context">
                            {[group.brand, group.modelName, group.version].filter(Boolean).join(" · ")}
                          </div>
                          <div className="bom-fob-audit-issue-facts">
                            <span>{group.countryCodes.length} countr{group.countryCodes.length === 1 ? "y" : "ies"}: {group.countryCodes.join(", ")}</span>
                            <span>{group.materialCodes.length} affected colour SKU{group.materialCodes.length === 1 ? "" : "s"}: {group.colourCodes.join(", ") || group.materialCodes.join(", ")}</span>
                            {group.colourTiers.length > 0 ? <span>Saved affected tier: {group.colourTiers.join(", ")}</span> : null}
                            {group.singleBaseConflicts.length > 0 ? (
                              <span>
                                Single conflicts: {group.singleBaseConflicts.slice(0, 4).map((conflict) => (
                                  `${conflict.countryCode} ${conflict.candidates.map(formatEur).join(" / ")}`
                                )).join(" · ")}
                                {group.singleBaseConflicts.length > 4 ? ` · +${group.singleBaseConflicts.length - 4} countries` : ""}
                              </span>
                            ) : null}
                          </div>
                          <p className="bom-fob-audit-issue-guidance">{exceptionGuidance(group)}</p>
                        </div>
                        <div className="bom-fob-audit-issue-actions">
                          {onOpenBomTemplate ? (
                            <button
                              type="button"
                              className="btn btn-sm btn-primary"
                              disabled={applying || openingTemplateKey !== null}
                              onClick={() => void openBomTemplate(group)}
                            >
                              {openingTemplateKey === group.key ? "Opening..." : "Open BOM template"}
                            </button>
                          ) : null}
                        </div>
                      </article>
                    ))}
                  </div>
                </section>
              ) : (
                <div className="bom-fob-audit-clear-state">No base, tier or surcharge configuration issues need review.</div>
              )}

              {moneyChanges.length > 0 ? (
                <section className="bom-fob-audit-section" aria-label="Safe price changes">
                  <div className="bom-fob-audit-section-head">
                    <div>
                      <h5>Safe price changes</h5>
                      <p>These rows use an unambiguous Single base and the already saved colour tier.</p>
                    </div>
                  </div>
                  <div className="bom-fob-audit-change-list">
                    {moneyChanges.slice(0, 100).map((item) => (
                      <div key={`${item.materialCode}|${item.countryCode}|${item.paymentTermCode ?? ""}`}>
                        <strong>{item.materialCode} · {item.countryCode}</strong>
                        <span>{item.colourTier ?? "tier missing"} · {formatEur(item.currentFinalFobEur)} → {formatEur(item.expectedFinalFobEur)}</span>
                      </div>
                    ))}
                    {moneyChanges.length > 100 ? <p>+ {moneyChanges.length - 100} more safe price changes</p> : null}
                  </div>
                </section>
              ) : null}

              {error ? <div className="bom-colour-code-edit-error">{error}</div> : null}
            </div>

            <footer className="bom-fob-audit-actions">
              <span>{exceptionGroups.length > 0 ? `${exceptionGroups.length} unresolved issue${exceptionGroups.length === 1 ? "" : "s"} will be skipped.` : "No unresolved pricing issues."}</span>
              <div>
                <button type="button" className="btn btn-sm btn-ghost" disabled={applying} onClick={() => setReviewOpen(false)}>Keep unresolved</button>
                <button
                  type="button"
                  className="btn btn-sm btn-primary"
                  disabled={applying || audit.summary.autoReprice === 0}
                  onClick={() => void applySafeRepricing()}
                >
                  {applying ? "Applying..." : `Apply ${audit.summary.autoReprice} safe rows`}
                </button>
              </div>
            </footer>
          </section>
        </div>
      </div>,
      document.body,
    )
    : null;

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
            <div style={{ padding: 5, background: exceptionGroups.length ? "#fef2f2" : "#f0fdf4", color: exceptionGroups.length ? "#b91c1c" : "#15803d", fontSize: 9 }}><strong>{exceptionGroups.length}</strong><br />issues</div>
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
      {error && !reviewOpen ? <div style={{ marginTop: 7, fontSize: 10, color: "#b91c1c" }}>{error}</div> : null}
      {reviewDialog}
    </div>
  );
}
