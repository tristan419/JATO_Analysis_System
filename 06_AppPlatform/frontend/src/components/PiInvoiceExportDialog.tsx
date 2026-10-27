import { useEffect, useRef, useState } from "react";
import { api } from "../api/client";
import type { PiExportFormat, PiInvoiceContext, PiInvoiceOptions, PiOrderHeader, PiTemplateCatalog } from "../types/orderGeniusVehicle";
import { orderGeniusIssue } from "../utils/orderGeniusIssue";
import { ConfirmDialog, type ConfirmDialogError } from "./ConfirmDialog";
import { PiInvoiceTemplateManager } from "./PiInvoiceTemplateManager";

const EMPTY_COUNTRIES: readonly string[] = [];
interface Props {
  countries?: readonly string[];
  month?: string;
  onClose: () => void;
  onCreateBatch?: (country: string, month: string) => void;
}

export function PiInvoiceExportDialog({ countries = EMPTY_COUNTRIES, month, onClose, onCreateBatch }: Props) {
  const [country, setCountry] = useState(countries[0] ?? "");
  const [period, setPeriod] = useState(month ?? "");
  const [scope, setScope] = useState<"single" | "monthly">("single");
  const [outputFormat, setOutputFormat] = useState<PiExportFormat>("template");
  const [piCode, setPiCode] = useState("");
  const [batches, setBatches] = useState<PiOrderHeader[]>([]);
  const fieldsRef = useRef<HTMLDivElement>(null);
  const previousScope = useRef("");
  const previewPiCode = useRef("");
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
  const fail = (cause: unknown, field?: string) => {
    const issue = orderGeniusIssue(cause);
    setError({ title: "PI export unavailable / 无法导出 PI",
      message: issue ? `${issue.message} ${issue.suggestion}` : cause instanceof Error ? cause.message : String(cause),
      actions: field ? [{ label: "Go to field / 定位填写项", onClick: () => fieldsRef.current?.querySelector<HTMLElement>(`[name="${field}"]`)?.focus() }]
        : issue?.code === "no_saved_pi" && onCreateBatch ? [{ label: "Go to PI Batch / 前往创建批次", onClick: () => onCreateBatch(country, period) }]
        : [{ label: "Reload source / 重读来源（保留输入）", onClick: () => setRevision((value) => value + 1) }] });
  };

  useEffect(() => {
    let cancelled = false;
    const key = JSON.stringify([country, period, scope, piCode, templateId]);
    const reset = previousScope.current !== key;
    previousScope.current = key;
    setContext(null); setError(null);
    if (reset) {
      previewPiCode.current = "";
      setCatalog(null); setBatches([]);
      setOptions(null); setFreight(""); setInsurance(""); setHandling("0");
      setFreightByModel(false); setModelFreight({});
    }
    if (!country || !/^\d{4}-\d{2}$/.test(period)) return;
    void api.getMonthlyInvoiceContext(country, period, templateId, scope, piCode || (scope === "single" ? previewPiCode.current : ""), outputFormat).then((next) => {
      if (cancelled) return;
      setCatalog(next.catalog); setBatches(next.batches); setContext(next.context);
      if (scope === "single" && next.context) previewPiCode.current = next.context.piCode;
      setOptions((current) => reset || !current ? next.context?.defaults ?? null : current);
      if (!next.context) {
        if (next.issue) fail(next.issue);
        else setError({ title: "Template unavailable / 模板不可用", message: `${next.error ?? "No matching template / 无匹配模板"}. Choose general details or PI data without a template, or manage templates / 可选无需模板的通用明细、PI数据表，或核对模板。`,
          actions: [{ label: "Use general details / 使用通用明细", onClick: () => setOutputFormat("details") },
            { label: "Manage templates / 定位模板", onClick: () => setManage(true) }] });
      }
    }).catch((cause: unknown) => { if (!cancelled) fail(cause); });
    return () => { cancelled = true; };
  }, [country, period, templateId, revision, scope, piCode, outputFormat]);

  async function download(): Promise<void> {
    if (!context || !options || busy) return;
    const amounts: Record<"freightEur" | "insuranceEur" | "handlingEur", string> = {
      freightEur: freight, insuranceEur: insurance, handlingEur: handling,
    };
    const payload: PiInvoiceOptions = { ...options, outputFormat, scope,
      piCode: scope === "single" ? context.piCode : undefined, sourceRevision: context.sourceRevision };
    if (outputFormat === "template") {
      payload.templateId = context.templateKey;
      if (context.templateRevision) payload.templateRevision = context.templateRevision;
    }
    if (freightByModel) {
      const models = context.freightModels ?? [];
      if (!models.length) { fail(new Error("Reload the saved PI source / 请重读已保存PI来源")); return; }
      const missing = models.filter((model) => !modelFreight[JSON.stringify([model.brand, model.modelName])]?.trim());
      if (missing.length) {
        fail(new Error(`Enter freight for / 请填写车型运费：${missing.map((model) => `${model.brand} · ${model.modelName}`).join(", ")}`), `freight-${models.indexOf(missing[0])}`); return;
      }
      payload.modelFreightEur = [];
      for (const model of models) {
        const value = Number(modelFreight[JSON.stringify([model.brand, model.modelName])]);
        if (!Number.isFinite(value) || value < 0 || value >= 1e12) {
          fail(new Error(`Invalid freight / 车型运费无效：${model.brand} · ${model.modelName}`), `freight-${models.indexOf(model)}`); return;
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
        fail(new Error("Enter non-negative costs; zero is allowed / 费用必须为非负数字，可明确填写 0"), key); return;
      }
      payload[key] = value;
    }
    if ((context.missingFreightUnits > 0 && !freightByModel && payload.freightEur === undefined)
      || (context.missingInsuranceUnits > 0 && payload.insuranceEur === undefined)) {
      fail(new Error("Saved costs are missing. Enter unit costs; zero is allowed / 原订单费用未填，请填写本次运保费，可明确填0"), context.missingFreightUnits > 0 && !freightByModel && payload.freightEur === undefined ? "freightEur" : "insuranceEur"); return;
    }
    if (outputFormat === "template") {
      for (const key of ["invoiceDate", "referenceNo", "portOfShipment", "portOfDischarge"] as const) {
        if (!options[key].trim()) { fail(new Error("Complete invoice date, reference and ports / 请补全发票日期、编号及港口"), key); return; }
      }
    }
    setError(null); setBusy(true);
    try {
      const blob = await api.exportMonthlyInvoice(country, period, payload);
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url; link.download = `${{ template: "Proforma", details: "Order-details", data: "PI-data" }[outputFormat]}_${context.piCode}.xlsx`;
      link.click(); URL.revokeObjectURL(url);
      onClose();
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }

  if (manage && catalog) return <PiInvoiceTemplateManager key={`${country}-${period}`} initial={catalog} onClose={() => { setManage(false); setTemplateId(""); setRevision((value) => value + 1); }} />;
  return <ConfirmDialog title="Export PI" description="Saved PI → template, general details or data / 已保存PI导出模板、通用明细或数据表；不重价、不创建新PI或车辆"
    cancelLabel="Close" confirmLabel="Download XLSX" loadingLabel="Exporting..." submitting={busy}
    confirmDisabled={!context || !options} error={error} onCancel={onClose} onConfirm={() => void download()}>
    <div ref={fieldsRef}>
    <div className="pi-invoice-export-fields">
      <label>Country / 国家<select aria-label="Invoice country" value={country} disabled={busy} onChange={(event) => { setContext(null); setCountry(event.target.value); setTemplateId(""); setPiCode(""); }}>
        {!countries.length ? <option value="">Choose a country / 请先选择国家</option> : null}
        {countries.map((code) => <option key={code} value={code}>{code}</option>)}
      </select></label>
      <label>Order month / 订单月份<input aria-label="Plan month" type="month" value={period} disabled={busy} onChange={(event) => { setContext(null); setPeriod(event.target.value); setTemplateId(""); setPiCode(""); }} /></label>
      <label>Export scope / 导出范围<select aria-label="Export scope" value={scope} disabled={busy} onChange={(event) => { setScope(event.target.value === "monthly" ? "monthly" : "single"); setPiCode(""); setTemplateId(""); }}>
        <option value="single">Single PI / 单次PI</option><option value="monthly">Monthly PI summary / 当月PI汇总</option>
      </select></label>
      <label>Format / 输出格式<select aria-label="Export format" value={outputFormat} disabled={busy} onChange={(event) => {
        const value = event.target.value;
        if (value === "template" || value === "details" || value === "data") { setContext(null); setOutputFormat(value); }
      }}>
        <option value="template">Template PI / 模板PI</option>
        <option value="details">General order details / 通用订单明细（无需模板）</option>
        <option value="data">PI data / PI数据表（无需模板）</option>
      </select></label>
      {scope === "single" ? <label>Saved PI / 已保存批次<select aria-label="Saved PI" disabled={busy || !batches.length} value={piCode || context?.piCode || batches[0]?.piCode || ""} onChange={(event) => { setPiCode(event.target.value); setTemplateId(""); }}>
        {!batches.length ? <option value="">No saved PI / 尚无已保存PI</option> : batches.map((batch) => <option key={batch.piCode} value={batch.piCode}>{batch.piCode}</option>)}
      </select></label> : null}
    </div>
    <p>{scope === "monthly" ? "Saved effective PIs only. Source PI sheet included / 仅汇总当月已保存有效PI，附来源PI表。" : "One saved batch, original transaction prices / 单次已保存批次，使用原成交价。"} Unbatched quantities are excluded / 尚未创建PI的数量不包含在本次导出中。
      {onCreateBatch && country && period ? <> <button type="button" disabled={busy} onClick={() => onCreateBatch(country, period)}>Create next PI / 前往创建下一批</button></> : null}</p>
    {catalog && outputFormat === "template" ? <p><label>PI template / 导出模板<select aria-label="PI template" value={templateId || catalog.items[0]?.id || ""} disabled={busy} onChange={(event) => { setContext(null); setTemplateId(event.target.value); }}>
      {!catalog.items.length ? <option value="">No matching template / 无匹配模板</option> : null}
      {catalog.items.map((item, index) => <option key={item.id} value={item.id}>{item.name} · {item.owner}{index === 0 ? " · Default" : ""}</option>)}
    </select></label> <button type="button" disabled={busy} onClick={() => setManage(true)}>Manage templates / 管理模板</button></p> : null}
    {context && options ? <>
      <p>{country} · {period} · {context.templateName} · {context.unitCount} units / 台 · {context.lineCount} material lines / 物料行</p>
      {outputFormat === "template" ? <><p><strong>{context.buyerName}</strong><br />{context.buyerAddress}{context.buyerEmail ? <><br />{context.buyerEmail}</> : null}</p>
      <details><summary>Template details / 模板固定资料</summary>
        <p>{context.sellerName}<br />{context.sellerAddress}<br />{context.priceTerm} · {context.paymentTerm}</p>
        <pre className="pi-invoice-bank-details">{context.bankDetails}</pre>
        <p>Fixed details are reused; date, reference and ports are reviewed each export / 固定资料复用，日期、编号及港口每次核对。</p>
      </details></> : <p>No template required. Order details only; no company, bank or trade terms are inferred / 无需模板，仅订单明细，不推测公司、银行或贸易条款。</p>}
      <div className="pi-invoice-export-fields">
        {outputFormat === "template" ? (["invoiceDate", "referenceNo", "portOfShipment", "portOfDischarge"] as const).map((key) => <label key={key}>
          {{ invoiceDate: "Invoice date / 发票日期", referenceNo: "Ref. No / PI 编号", portOfShipment: "Port of shipment / 装运港", portOfDischarge: "Port of discharge / 卸货港" }[key]}
          <input name={key} type={key === "invoiceDate" ? "date" : "text"} value={options[key]} disabled={busy}
            onChange={(event) => setOptions({ ...options, [key]: event.target.value })} />
        </label>) : null}
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
          {freightByModel ? (context.freightModels ?? []).map((model, index) => {
            const key = JSON.stringify([model.brand, model.modelName]);
            return <label key={key} className="pi-invoice-model-freight"><span>{model.brand} · {model.modelName} · {model.unitCount} units / 台</span>
              <input name={`freight-${index}`} aria-label={`${model.brand} · ${model.modelName} freight / 运费`} type="number" min="0" step="0.01" placeholder="EUR / unit" value={modelFreight[key] ?? ""} disabled={busy}
                onChange={(event) => setModelFreight({ ...modelFreight, [key]: event.target.value })} />
            </label>;
          }) : <label>Freight / 运费 (EUR / unit)<input name="freightEur" type="number" min="0" step="0.01" placeholder="Blank = saved / 留空沿用已存费用" value={freight} disabled={busy} onChange={(event) => setFreight(event.target.value)} /></label>}
        </fieldset> : null}
        {!context.charges || context.charges.includes("insuranceEur") ? <label>Insurance / 保费 (EUR / unit)<input name="insuranceEur" type="number" min="0" step="0.01" placeholder="Blank = saved / 留空沿用已存费用" value={insurance} disabled={busy} onChange={(event) => setInsurance(event.target.value)} /></label> : null}
        {context.templateKey === "CH_OJ" || context.charges?.includes("handlingEur") ? <label>Handling Charges / 操作费 (EUR / unit)<input name="handlingEur" type="number" min="0" step="0.01" value={handling} disabled={busy} onChange={(event) => setHandling(event.target.value)} /></label> : null}
      </div>
      <p>Freight is uniform unless grouped by model; insurance and handling stay uniform. File only; no order, vehicle, quantity or quote changes / 默认统一运费，勾选才按车型；保费及操作费仍统一。仅用于本文件，不创建订单/车辆，不修改数量或报价。</p>
      {outputFormat === "template" ? <p>Templates are reusable across months. Check ports before each export, especially when the LC destination differs / 模板跨月复用，每次导出请核对港口，尤其 LC 目的港不同的情况。</p> : null}
    </> : country && period && !error ? <p>Loading saved PIs and export settings / 正在读取已保存PI与导出设置...</p> : !error ? <p>Choose a country and order month / 请选择国家和订单月份。</p> : null}
    </div>
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
