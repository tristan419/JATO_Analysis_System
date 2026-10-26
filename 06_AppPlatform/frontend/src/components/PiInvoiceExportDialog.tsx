import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { PiInvoiceContext, PiInvoiceOptions, PiTemplateCatalog } from "../types/orderGeniusVehicle";
import { ConfirmDialog, type ConfirmDialogError } from "./ConfirmDialog";
import { PiInvoiceTemplateManager } from "./PiInvoiceTemplateManager";

const EMPTY_COUNTRIES: readonly string[] = [];
interface Props {
  countries?: readonly string[];
  month?: string;
  onClose: () => void;
}

export function PiInvoiceExportDialog({ countries = EMPTY_COUNTRIES, month, onClose }: Props) {
  const [country, setCountry] = useState(countries[0] ?? "");
  const [period, setPeriod] = useState(month ?? "");
  const [context, setContext] = useState<PiInvoiceContext | null>(null);
  const [catalog, setCatalog] = useState<PiTemplateCatalog | null>(null);
  const [templateId, setTemplateId] = useState("");
  const [manage, setManage] = useState(false);
  const [revision, setRevision] = useState(0);
  const [options, setOptions] = useState<PiInvoiceOptions | null>(null);
  const [freight, setFreight] = useState("");
  const [freightByModel, setFreightByModel] = useState(false);
  const [modelFreight, setModelFreight] = useState<Record<string, string>>({});
  const [insurance, setInsurance] = useState("");
  const [handling, setHandling] = useState("0");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ConfirmDialogError | null>(null);
  const fail = (cause: unknown) => setError({ title: "PI export unavailable / 无法导出 PI",
    message: cause instanceof Error ? cause.message : String(cause) });

  useEffect(() => {
    let cancelled = false;
    setCatalog(null); setContext(null); setOptions(null); setError(null);
    setFreight(""); setInsurance(""); setHandling("0");
    setFreightByModel(false); setModelFreight({});
    if (!country || !/^\d{4}-\d{2}$/.test(period)) return;
    void api.getMonthlyInvoiceContext(country, period, templateId).then((next) => {
      if (cancelled) return;
      setCatalog(next.catalog); setContext(next.context); setOptions(next.context?.defaults ?? null);
      if (!next.context) fail(new Error(next.error ?? "No matching template. Upload your country's XLSX / 无匹配模板，请上传本国XLSX模板"));
    }).catch((cause: unknown) => { if (!cancelled) fail(cause); });
    return () => { cancelled = true; };
  }, [country, period, templateId, revision]);

  async function download(): Promise<void> {
    if (!context || !options || busy) return;
    const amounts: Record<"freightEur" | "insuranceEur" | "handlingEur", string> = {
      freightEur: freight, insuranceEur: insurance, handlingEur: handling,
    };
    const payload: PiInvoiceOptions = { ...options, templateId: context.templateKey };
    if (context.templateRevision) payload.templateRevision = context.templateRevision;
    if (freightByModel) {
      const models = context.freightModels ?? [];
      if (!models.length) { fail(new Error("Reload the monthly plan / 请重读月计划车型")); return; }
      const missing = models.filter((model) => !modelFreight[JSON.stringify([model.brand, model.modelName])]?.trim());
      if (missing.length) {
        fail(new Error(`Enter freight for / 请填写车型运费：${missing.map((model) => `${model.brand} · ${model.modelName}`).join(", ")}`)); return;
      }
      payload.modelFreightEur = [];
      for (const model of models) {
        const value = Number(modelFreight[JSON.stringify([model.brand, model.modelName])]);
        if (!Number.isFinite(value) || value < 0 || value >= 1e12) {
          fail(new Error(`Invalid freight / 车型运费无效：${model.brand} · ${model.modelName}`)); return;
        }
        payload.modelFreightEur.push({ brand: model.brand, modelName: model.modelName, freightEur: value });
      }
    }
    for (const key of ["freightEur", "insuranceEur", "handlingEur"] as const) {
      if (key === "freightEur" && freightByModel) continue;
      if (context.charges && !context.charges.includes(key)) continue;
      const text = amounts[key].trim();
      if (!text) continue;
      const value = Number(text);
      if (!Number.isFinite(value) || value < 0 || value >= 1e12) {
        fail(new Error("Enter non-negative costs; zero is allowed / 费用必须为非负数字，可明确填写 0")); return;
      }
      payload[key] = value;
    }
    if ((context.missingFreightUnits > 0 && !freightByModel && payload.freightEur === undefined)
      || (context.missingInsuranceUnits > 0 && payload.insuranceEur === undefined)) {
      fail(new Error("Enter unit freight and insurance; zero is allowed / 月计划没有每车费用，请填写运保费，可明确填0")); return;
    }
    setError(null); setBusy(true);
    try {
      const blob = await api.exportMonthlyInvoice(country, period, payload);
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url; link.download = `Proforma_${country}_${period}.xlsx`;
      link.click(); URL.revokeObjectURL(url);
      onClose();
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }

  if (manage && catalog) return <PiInvoiceTemplateManager key={`${country}-${period}`} initial={catalog} onClose={() => { setManage(false); setTemplateId(""); setRevision((value) => value + 1); }} />;
  return <ConfirmDialog title="Export PI" description="Monthly plan → template file only / 月计划直接套版，仅生成文件，不创建PI或车辆"
    cancelLabel="Close" confirmLabel="Download XLSX" loadingLabel="Exporting..." submitting={busy}
    confirmDisabled={!context || !options} error={error} onCancel={onClose} onConfirm={() => void download()}>
    <div className="pi-invoice-export-fields">
      <label>Country / 国家<select aria-label="Invoice country" value={country} disabled={busy} onChange={(event) => { setContext(null); setCountry(event.target.value); setTemplateId(""); }}>
        {!countries.length ? <option value="">Choose a country / 请先选择国家</option> : null}
        {countries.map((code) => <option key={code} value={code}>{code}</option>)}
      </select></label>
      <label>Plan month / 计划月份<input aria-label="Plan month" type="month" value={period} disabled={busy} onChange={(event) => { setContext(null); setPeriod(event.target.value); setTemplateId(""); }} /></label>
    </div>
    <p>All positive quantities for this country/month; templates are reusable across months / 读取该国家当月全部非零月计划，模板跨月复用，不扣分车余量。</p>
    {catalog ? <p><label>PI template / 导出模板<select aria-label="PI template" value={templateId || catalog.items[0]?.id || ""} disabled={busy} onChange={(event) => { setContext(null); setTemplateId(event.target.value); }}>
      {!catalog.items.length ? <option value="">No matching template / 无匹配模板</option> : null}
      {catalog.items.map((item, index) => <option key={item.id} value={item.id}>{item.name} · {item.owner}{index === 0 ? " · Default" : ""}</option>)}
    </select></label> <button type="button" disabled={busy} onClick={() => setManage(true)}>Manage templates / 管理模板</button></p> : null}
    {context && options ? <>
      <p>{country} · {period} · {context.templateName} · {context.unitCount} units / 台 · {context.lineCount} material lines / 物料行</p>
      <p><strong>{context.buyerName}</strong><br />{context.buyerAddress}{context.buyerEmail ? <><br />{context.buyerEmail}</> : null}</p>
      <details><summary>Template details / 模板固定资料</summary>
        <p>{context.sellerName}<br />{context.sellerAddress}<br />{context.priceTerm} · {context.paymentTerm}</p>
        <pre className="pi-invoice-bank-details">{context.bankDetails}</pre>
        <p>Fixed details are reused; date, reference and ports are reviewed each export / 固定资料复用，日期、编号及港口每次核对。</p>
      </details>
      <div className="pi-invoice-export-fields">
        {(["invoiceDate", "referenceNo", "portOfShipment", "portOfDischarge"] as const).map((key) => <label key={key}>
          {{ invoiceDate: "Invoice date / 发票日期", referenceNo: "Ref. No / PI 编号", portOfShipment: "Port of shipment / 装运港", portOfDischarge: "Port of discharge / 卸货港" }[key]}
          <input type={key === "invoiceDate" ? "date" : "text"} value={options[key]} disabled={busy}
            onChange={(event) => setOptions({ ...options, [key]: event.target.value })} />
        </label>)}
        {!context.charges || context.charges.includes("freightEur") ? <fieldset className="pi-invoice-freight">
          <legend>Freight settings / 运费设置</legend>
          <label className="pi-invoice-freight-toggle"><input type="checkbox" checked={freightByModel} disabled={busy || !context.freightModels?.length}
            onChange={(event) => {
              setFreightByModel(event.target.checked);
              if (event.target.checked) setModelFreight((current) => Object.fromEntries((context.freightModels ?? []).map((model) => {
                const key = JSON.stringify([model.brand, model.modelName]);
                return [key, current[key] ?? freight];
              })));
            }} />By model / 按车型设置运费</label>
          {freightByModel ? (context.freightModels ?? []).map((model) => {
            const key = JSON.stringify([model.brand, model.modelName]);
            return <label key={key} className="pi-invoice-model-freight"><span>{model.brand} · {model.modelName} · {model.unitCount} units / 台</span>
              <input aria-label={`${model.brand} · ${model.modelName} freight / 运费`} type="number" min="0" step="0.01" placeholder="EUR / unit" value={modelFreight[key] ?? ""} disabled={busy}
                onChange={(event) => setModelFreight({ ...modelFreight, [key]: event.target.value })} />
            </label>;
          }) : <label>Freight / 运费 (EUR / unit)<input type="number" min="0" step="0.01" placeholder="Required / 必填，可填0" value={freight} disabled={busy} onChange={(event) => setFreight(event.target.value)} /></label>}
        </fieldset> : null}
        {!context.charges || context.charges.includes("insuranceEur") ? <label>Insurance / 保费 (EUR / unit)<input type="number" min="0" step="0.01" placeholder="Required / 必填，可填0" value={insurance} disabled={busy} onChange={(event) => setInsurance(event.target.value)} /></label> : null}
        {context.templateKey === "CH_OJ" || context.charges?.includes("handlingEur") ? <label>Handling Charges / 操作费 (EUR / unit)<input type="number" min="0" step="0.01" value={handling} disabled={busy} onChange={(event) => setHandling(event.target.value)} /></label> : null}
      </div>
      <p>Freight is uniform unless grouped by model; insurance and handling stay uniform. File only; no order, vehicle, quantity or quote changes / 默认统一运费，勾选才按车型；保费及操作费仍统一。仅用于本文件，不创建订单/车辆，不修改数量或报价。</p>
      <p>Check ports before each export, especially when the LC destination differs / 每次导出请核对港口，尤其 LC 目的港不同的情况。</p>
    </> : country && period && !error ? <p>Loading monthly plan and template / 正在读取月计划与模板...</p> : !error ? <p>Choose a country and plan month / 请选择国家和计划月份。</p> : null}
    <style>{`
      .pi-invoice-export-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
      .pi-invoice-export-fields label{display:grid;gap:6px;font-size:13px;min-width:0}
      .pi-invoice-export-fields input,.pi-invoice-export-fields select{width:100%;min-width:0;padding:8px;border:1px solid #cbd5e1;border-radius:6px}
      .pi-invoice-bank-details{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit;font-size:12px}
      .pi-invoice-freight{grid-column:1/-1;min-width:0;margin:0;padding:12px;border:1px solid #cbd5e1;border-radius:6px;display:grid;gap:10px}
      .pi-invoice-export-fields .pi-invoice-freight-toggle{display:flex;align-items:center;gap:8px}
      .pi-invoice-freight-toggle input[type=checkbox]{width:auto;flex:none}
      .pi-invoice-export-fields .pi-invoice-model-freight{display:grid;grid-template-columns:minmax(0,1fr) 130px;align-items:center}
      @media(max-width:640px){.pi-invoice-export-fields{grid-template-columns:1fr}}
    `}</style>
  </ConfirmDialog>;
}
