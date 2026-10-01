import { useState } from "react";
import { api } from "../../api/client";
import type { BomTemplateLifecycleUpdateResponse } from "../../types/orderGenius";

interface Props {
  materialCode: string;
  status: "active" | "phase_out" | "historical";
  effectiveFrom: string | null;
  effectiveTo: string | null;
  rowVersion: number;
  onSaved: () => void;
}

export function BomTemplateLifecycleEditor({ materialCode, status, effectiveFrom, effectiveTo, rowVersion, onSaved }: Props) {
  const [draft, setDraft] = useState({ lifecycleStatus: status, effectiveFrom: effectiveFrom || "", effectiveTo: effectiveTo || "" });
  const [preview, setPreview] = useState<BomTemplateLifecycleUpdateResponse | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

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
      else { setPreview(null); onSaved(); }
    } catch (failure: unknown) {
      setError(failure instanceof Error ? failure.message : String(failure));
    } finally { setBusy(false); }
  };

  return <div className="bom-lifecycle-editor">
    <label>Undated status
      <select aria-label="Undated lifecycle status" disabled={busy} value={draft.lifecycleStatus} onChange={(event) => {
        const value = event.target.value;
        if (value === "active" || value === "phase_out" || value === "historical") {
          setDraft({ ...draft, lifecycleStatus: value }); setPreview(null);
        }
      }}>
        <option value="active">Active</option><option value="phase_out">Phase out</option><option value="historical">Historical</option>
      </select>
    </label>
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
  </div>;
}
