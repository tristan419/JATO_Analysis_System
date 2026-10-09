import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { PiInvoiceContext, PiInvoiceOptions, PiOrderHeader } from "../types/orderGeniusVehicle";
import { ConfirmDialog, type ConfirmDialogError } from "./ConfirmDialog";

const EMPTY_COUNTRIES: readonly string[] = [];
interface Props {
  piCode?: string;
  countries?: readonly string[];
  month?: string;
  onClose: () => void;
}

export function PiInvoiceExportDialog({ piCode, countries = EMPTY_COUNTRIES, month, onClose }: Props) {
  const [pis, setPis] = useState<PiOrderHeader[]>([]);
  const [selected, setSelected] = useState(piCode ?? "");
  const [context, setContext] = useState<PiInvoiceContext | null>(null);
  const [options, setOptions] = useState<PiInvoiceOptions | null>(null);
  const [freight, setFreight] = useState("");
  const [insurance, setInsurance] = useState("");
  const [handling, setHandling] = useState("0");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ConfirmDialogError | null>(null);
  const fail = (cause: unknown) => setError({ title: "PI export unavailable / 无法导出 PI",
    message: cause instanceof Error ? cause.message : String(cause) });

  useEffect(() => {
    if (piCode) return;
    let cancelled = false;
    void Promise.all(countries.map((country) => api.getVehicleAllocationPis({ country, month, pageSize: 200 })))
      .then((responses) => {
        if (cancelled) return;
        if (responses.some((response) => response.total > response.items.length)) {
          throw new Error("Choose a month to narrow the saved PI list / 请选择月份缩小已保存 PI 范围");
        }
        const rows = [...new Map(responses.flatMap((response) => response.items).map((header) => [header.piCode, header])).values()];
        setPis(rows);
        setSelected(rows[0]?.piCode ?? "");
      }).catch((cause: unknown) => { if (!cancelled) fail(cause); });
    return () => { cancelled = true; };
  }, [piCode, countries, month]);

  useEffect(() => {
    let cancelled = false;
    setContext(null); setOptions(null); setError(null);
    setFreight(""); setInsurance(""); setHandling("0");
    if (!selected) return;
    void api.getPiInvoiceContext(selected).then((next) => {
      if (!cancelled) { setContext(next); setOptions(next.defaults); }
    }).catch((cause: unknown) => { if (!cancelled) fail(cause); });
    return () => { cancelled = true; };
  }, [selected]);

  async function download(): Promise<void> {
    if (!context || !options || busy) return;
    const amounts: Record<"freightEur" | "insuranceEur" | "handlingEur", string> = {
      freightEur: freight, insuranceEur: insurance, handlingEur: handling,
    };
    const payload: PiInvoiceOptions = { ...options };
    for (const key of ["freightEur", "insuranceEur", "handlingEur"] as const) {
      const text = amounts[key].trim();
      if (!text) continue;
      const value = Number(text);
      if (!Number.isFinite(value) || value < 0 || value >= 1e12) {
        fail(new Error("Enter non-negative costs; zero is allowed / 费用必须为非负数字，可明确填写 0")); return;
      }
      payload[key] = value;
    }
    if ((context.missingFreightUnits > 0 && payload.freightEur === undefined)
      || (context.missingInsuranceUnits > 0 && payload.insuranceEur === undefined)) {
      fail(new Error("Some vehicles have no saved costs. Fill costs here or update vehicles first / 部分车辆运保费为空，请在此填写或先维护车辆费用")); return;
    }
    setError(null); setBusy(true);
    try {
      const blob = await api.exportPiInvoice(selected, payload);
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url; link.download = `Proforma_${selected}.xlsx`;
      link.click(); URL.revokeObjectURL(url);
      onClose();
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }

  return <ConfirmDialog title="Export PI" description="Complete saved PI · historical FOB unchanged / 导出完整已保存 PI，不更改成交价"
    cancelLabel="Close" confirmLabel="Download XLSX" loadingLabel="Exporting..." submitting={busy}
    confirmDisabled={!context || !options} error={error} onCancel={onClose} onConfirm={() => void download()}>
    {!piCode ? <label>Saved PI / 已保存 PI<select aria-label="Saved PI" value={selected} onChange={(event) => setSelected(event.target.value)} disabled={busy}>
      {!pis.length ? <option value="">No saved PI in this country / month</option> : null}
      {pis.map((header) => <option key={header.piCode} value={header.piCode}>{header.piCode}</option>)}
    </select></label> : <p>{piCode}</p>}
    {context && options ? <>
      <p>{context.templateName} · {context.unitCount} vehicles · {context.lineCount} PI lines</p>
      <p><strong>{context.buyerName}</strong><br />{context.buyerAddress}{context.buyerEmail ? <><br />{context.buyerEmail}</> : null}</p>
      <details><summary>Template details / 模板固定资料</summary>
        <p>{context.sellerName}<br />{context.sellerAddress}<br />{context.priceTerm} · {context.paymentTerm}</p>
        <pre className="pi-invoice-bank-details">{context.bankDetails}</pre>
        <p>Fixed details are reused each month; template management is a later step / 固定资料每月复用，模板管理页后续提供</p>
      </details>
      <div className="pi-invoice-export-fields">
        {(["invoiceDate", "referenceNo", "portOfShipment", "portOfDischarge"] as const).map((key) => <label key={key}>
          {{ invoiceDate: "Invoice date / 发票日期", referenceNo: "Ref. No / PI 编号", portOfShipment: "Port of shipment / 装运港", portOfDischarge: "Port of discharge / 卸货港" }[key]}
          <input type={key === "invoiceDate" ? "date" : "text"} value={options[key]} disabled={busy}
            onChange={(event) => setOptions({ ...options, [key]: event.target.value })} />
        </label>)}
        <label>Freight / 运费 (EUR / unit)<input type="number" min="0" step="0.01" placeholder="Blank: saved vehicle costs" value={freight} disabled={busy} onChange={(event) => setFreight(event.target.value)} /></label>
        <label>Insurance / 保费 (EUR / unit)<input type="number" min="0" step="0.01" placeholder="Blank: saved vehicle costs" value={insurance} disabled={busy} onChange={(event) => setInsurance(event.target.value)} /></label>
        {context.templateKey === "CH_OJ" ? <label>Handling Charges / 操作费 (EUR / unit)<input type="number" min="0" step="0.01" value={handling} disabled={busy} onChange={(event) => setHandling(event.target.value)} /></label> : null}
      </div>
      <p>Missing saved costs / 未填费用：Freight {context.missingFreightUnits} · Insurance {context.missingInsuranceUnits}. Entered costs override every unit for this file only; blank preserves each unit / 本次填写的费用统一用于本文件，留空保留每车费用。不写回订单。</p>
      <p>Check ports before each export, especially when the LC destination differs / 每次导出请核对港口，尤其 LC 目的港不同的情况。</p>
    </> : selected && !error ? <p>Loading template and PI / 正在读取模板与 PI...</p> : null}
    <style>{`
      .pi-invoice-export-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
      .pi-invoice-export-fields label{display:grid;gap:6px;font-size:13px;min-width:0}
      .pi-invoice-export-fields input{width:100%;min-width:0;padding:8px;border:1px solid #cbd5e1;border-radius:6px}
      .pi-invoice-bank-details{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit;font-size:12px}
      @media(max-width:640px){.pi-invoice-export-fields{grid-template-columns:1fr}}
    `}</style>
  </ConfirmDialog>;
}
