import { useState } from "react";
import { createPortal } from "react-dom";
import { api } from "../../api/client";
import type { BomTemplateLifecycleUpdateResponse } from "../../types/orderGenius";

interface Props {
  materialCode: string;
  bomTemplate: string;
  status: "active" | "phase_out" | "historical";
  effectiveFrom: string | null;
  effectiveTo: string | null;
  rowVersion: number;
  onSaved: () => void;
}

const STATUS_OPTIONS = [
  { value: "active", label: "Active" },
  { value: "phase_out", label: "Phase out" },
  { value: "historical", label: "Historical" },
] as const;

export function BomTemplateLifecycleEditor({ materialCode, bomTemplate, status, effectiveFrom, effectiveTo, rowVersion, onSaved }: Props) {
  const [draft, setDraft] = useState({ lifecycleStatus: status, effectiveFrom: effectiveFrom || "", effectiveTo: effectiveTo || "" });
  const [preview, setPreview] = useState<BomTemplateLifecycleUpdateResponse | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [open, setOpen] = useState(false);
  const hasDates = Boolean(draft.effectiveFrom || draft.effectiveTo);
  const close = (): void => {
    if (busy) return;
    setOpen(false);
    setDraft({ lifecycleStatus: status, effectiveFrom: effectiveFrom || "", effectiveTo: effectiveTo || "" });
    setPreview(null);
    setError("");
  };

  const submit = async (previewOnly: boolean): Promise<void> => {
    setBusy(true);
    setError("");
    if (previewOnly) setPreview(null);
    try {
      const result = await api.updateSkuLifecycle(materialCode, {
        ...draft, effectiveFrom: draft.effectiveFrom || null, effectiveTo: draft.effectiveTo || null,
        rowVersion, previewOnly,
      });
      if (previewOnly) setPreview(result);
      else { setPreview(null); setOpen(false); onSaved(); }
    } catch (failure: unknown) {
      setError(failure instanceof Error ? failure.message : String(failure));
    } finally { setBusy(false); }
  };

  return <div className="bom-lifecycle-editor">
    <div className="bom-lifecycle-segment" role="group" aria-label="Lifecycle status" title={hasDates ? "Status follows template dates. Open Dates to edit or clear them." : "Choose a status, then preview and confirm."}>
      {STATUS_OPTIONS.map((option) => <button key={option.value} type="button"
        className={`bom-lifecycle-option bom-lifecycle-option-${option.value}${draft.lifecycleStatus === option.value ? " is-active" : ""}`}
        aria-pressed={draft.lifecycleStatus === option.value} disabled={busy || hasDates}
        onClick={() => { setDraft({ ...draft, lifecycleStatus: option.value }); setPreview(null); setOpen(true); }}>
        {option.label}
      </button>)}
    </div>
    <button type="button" className="btn btn-sm btn-ghost bom-lifecycle-dates-trigger" disabled={busy}
      title={hasDates ? `${draft.effectiveFrom || "Open start"} → ${draft.effectiveTo || "Open end"} (inclusive). Status follows dates.` : "No date limits. Add optional template dates."}
      aria-haspopup="dialog" onClick={() => setOpen(true)}>
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true"><rect x="3" y="5" width="18" height="16" rx="2" /><path d="M7 3v4m10-4v4M3 11h18" /></svg>
      Dates{hasDates ? " · Set" : ""}
    </button>
    {open ? createPortal(<div className="bom-finance-modal-backdrop" onClick={close}>
      <section className="bom-lifecycle-date-dialog" role="dialog" aria-modal="true" aria-label="Template lifecycle dates"
        onClick={(event) => event.stopPropagation()} onKeyDown={(event) => { if (event.key === "Escape") { event.stopPropagation(); close(); } }}>
        <header>
          <div><span className="bom-finance-eyebrow">BOM ADMIN · LIFECYCLE</span><h4>Template lifecycle</h4><p>{bomTemplate}</p></div>
          <button type="button" className="btn btn-sm btn-ghost" autoFocus disabled={busy} onClick={close}>Close</button>
        </header>
        <div className="bom-lifecycle-date-body">
          <p>{hasDates ? "Status follows these dates. Clear both dates to choose an undated status." : "No date limits. Dates are optional."}</p>
          <label>Status without dates <select aria-label="Undated lifecycle status" disabled={busy || hasDates} value={draft.lifecycleStatus} onChange={(event) => {
            const option = STATUS_OPTIONS.find((item) => item.value === event.target.value);
            if (option) { setDraft({ ...draft, lifecycleStatus: option.value }); setPreview(null); }
          }}>{STATUS_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
          <div className="bom-lifecycle-window">
            <label><span>First order date</span><input aria-label="First order date" disabled={busy} type="date" value={draft.effectiveFrom} onChange={(event) => { setDraft({ ...draft, effectiveFrom: event.target.value }); setPreview(null); }} /></label>
            <label><span>Final order date (inclusive)</span><input aria-label="Final order date" disabled={busy} type="date" value={draft.effectiveTo} onChange={(event) => { setDraft({ ...draft, effectiveTo: event.target.value }); setPreview(null); }} /></label>
          </div>
          <small>Dates apply to every colour in this template. A final date determines Phase out / Historical.</small>
          <button type="button" className="btn btn-sm btn-ghost" disabled={busy} onClick={() => void submit(true)}>Preview template dates</button>
          {preview ? <div role="status">
            <p>{preview.materialCodes.length} colours follow this template.</p>
            {preview.affectedPeriods.map((period) => <p key={period.periodId}>
              {period.countryCode}: {period.validFrom} → {period.validTo || "Open"} exceeds the proposed dates.<br />
              Edit this country's price period, extend the template dates, or cancel.<br />
              国家价格区间越界：请修改国家区间、延长模板日期或取消。
            </p>)}
            <button type="button" className="btn btn-sm btn-primary" disabled={busy || !preview.canApply} onClick={() => void submit(false)}>Confirm template dates</button>
            <button type="button" className="btn btn-sm btn-ghost" disabled={busy} onClick={() => setPreview(null)}>Cancel preview</button>
          </div> : null}
          {error ? <div className="form-error" role="alert">{error}</div> : null}
        </div>
      </section>
    </div>, document.body) : null}
  </div>;
}
