import { useState } from "react";
import { api } from "../api/client";
import type { PiInvoiceTemplate, PiTemplateCatalog, PiTemplateColumn, PiTemplateConfig, PiTemplateField, PiTemplateSave } from "../types/orderGeniusVehicle";
import { ConfirmDialog, type ConfirmDialogError } from "./ConfirmDialog";

const FIELDS: { key: PiTemplateField; label: string; hint: string; optional?: boolean; dynamic?: boolean }[] = [
  { key: "sellerName", label: "Seller / 卖方", hint: "销售主体完整公司名称；不要把买方当卖方。" },
  { key: "sellerAddress", label: "Seller address / 卖方地址", optional: true, hint: "原卖方地址及税号；可留空。" },
  { key: "buyerName", label: "Buyer / 买方", hint: "收票买方完整名称；按原文件核对，不按国家推定。" },
  { key: "buyerAddress", label: "Buyer address / 买方地址", hint: "原文件完整买方地址。" },
  { key: "buyerEmail", label: "Buyer email / 买方邮箱", optional: true, hint: "买方联系邮箱；未提供可留空。" },
  { key: "bankDetails", label: "Bank details / 银行资料", hint: "完整收款主体、账号、银行、SWIFT等原文；不要只填银行名称。" },
  { key: "priceTerm", label: "Price term / 价格条款", hint: "原价格条款，如CIF/CFR；合计费用在上方独立核对。" },
  { key: "paymentTerm", label: "Payment term / 付款条款", hint: "完整付款条件，如LC或TT及期限，不能从国家猜测。" },
  { key: "invoiceDate", label: "Invoice date / 每次发票日期", dynamic: true, hint: "选择实际日期填写格，不是Date标签格；每次导出重新生成。" },
  { key: "referenceNo", label: "Reference / 每次PI编号", dynamic: true, hint: "选择实际编号填写格，不是Ref. No标签格；每次导出重新生成。" },
  { key: "portOfShipment", label: "Loading port / 默认装运港", hint: "默认装运港；值可留空，导出时填写；单元格必填。" },
  { key: "portOfDischarge", label: "Discharge port / 默认卸货港", hint: "默认卸货港；值可留空，导出时填写；单元格必填。" },
];
const COLUMNS: { key: PiTemplateColumn; label: string }[] = [
  { key: "number", label: "No" }, { key: "modelName", label: "Model*" }, { key: "version", label: "Version" },
  { key: "powertrain", label: "Powertrain / Capacity" }, { key: "exteriorColorName", label: "Exterior" },
  { key: "interiorColorName", label: "Interior" }, { key: "materialCode", label: "Material" },
  { key: "quantity", label: "Quantity*" }, { key: "fobEur", label: "Unit FOB*" }, { key: "fobAmount", label: "FOB amount" },
  { key: "freightEur", label: "Unit freight" }, { key: "insuranceEur", label: "Unit insurance" },
  { key: "handlingEur", label: "Unit handling" }, { key: "total", label: "Total amount*" },
];
type Props = { initial: PiTemplateCatalog; onClose: () => void } | {
  countries: string[]; brands: string[]; defaultCountry?: string; onClose: () => void;
};
interface Draft { payload: PiTemplateSave; file?: File; cells: { cell: string; value: string }[] }
interface MappingIssue { target: string; message: string }

function mappingIssues(config: PiTemplateConfig, name: string, chargesConfirmed: boolean): MappingIssue[] {
  const issues: MappingIssue[] = [];
  if (!name.trim()) issues.push({ target: "template-name", message: "填写模板名称" });
  if (![config.headerRow, config.dataStart, config.totalRow].every(Number.isInteger)
    || !(0 < config.headerRow && config.headerRow < config.dataStart && config.dataStart < config.totalRow)) {
    issues.push({ target: "headerRow", message: "核对表头、首条明细、总计的行号及顺序" });
  }
  const columns = Object.values(config.columns);
  for (const { key, label } of COLUMNS) {
    const col = config.columns[key];
    if ((!col && ["modelName", "quantity", "fobEur", "total"].includes(key))
      || (col && (!/^[A-Z]{1,2}$/.test(col) || columns.filter((value) => value === col).length > 1))) {
      issues.push({ target: `column-${key}`, message: `${label}：填写有效、不重复的列字母` });
    }
  }
  const addresses = Object.values(config.fields).map((field) => field?.cell.trim().toUpperCase());
  for (const { key, label, optional, dynamic } of FIELDS) {
    const field = config.fields[key]; const address = field?.cell.trim().toUpperCase() ?? "";
    if ((!optional && !address) || (address && (!/^[A-Z]{1,2}[1-9][0-9]{0,3}$/.test(address)
      || addresses.filter((value) => value === address).length > 1))) {
      issues.push({ target: `${key}-cell`, message: `${label}：选择有效、不重复的单元格` });
    }
    if (!optional && !dynamic && !["portOfShipment", "portOfDischarge"].includes(key) && !field?.value.trim()) {
      issues.push({ target: `${key}-value`, message: `${label}：填入完整原文` });
    }
  }
  if (config.charges.some((charge) => !config.columns[charge])) issues.push({ target: "column-total", message: "参与合计的费用必须有对应列" });
  if (!chargesConfirmed) issues.push({ target: "charges-confirmed", message: "明确确认合计费用口径（包括仅FOB）" });
  return issues;
}

function locateField(target: string): void {
  const element = document.getElementById(`pi-template-${target}`);
  element?.scrollIntoView({ block: "center" }); element?.focus();
}

export function PiInvoiceTemplateManager(props: Props) {
  return "initial" in props ? <TemplateEditor {...props} /> : <TemplateScopePicker {...props} />;
}

function TemplateScopePicker({ countries, brands, defaultCountry, onClose }: Extract<Props, { countries: string[] }>) {
  const [country, setCountry] = useState(countries.includes(defaultCountry ?? "") ? defaultCountry ?? "" : countries[0] ?? "");
  const [selectedBrands, setSelectedBrands] = useState<string[]>([]);
  const [catalog, setCatalog] = useState<PiTemplateCatalog | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ConfirmDialogError | null>(null);
  async function open(): Promise<void> {
    if (busy || !country || !selectedBrands.length) return;
    setBusy(true); setError(null);
    try { setCatalog(await api.getInvoiceTemplates({ country, brands: selectedBrands })); }
    catch (cause: unknown) { setError({ title: "Cannot load templates / 无法读取模板", message: cause instanceof Error ? cause.message : String(cause) }); }
    finally { setBusy(false); }
  }
  if (catalog) return <TemplateEditor initial={catalog} onClose={() => { setCatalog(null); setError(null); }} />;
  return <ConfirmDialog title="PI Templates / PI模板" description="Upload before creating a PI / 无需先创建PI即可上传模板"
    cancelLabel="Close" confirmLabel="Open templates / 打开模板" loadingLabel="Loading..." submitting={busy}
    confirmDisabled={!country || !selectedBrands.length} error={error} onCancel={onClose} onConfirm={() => void open()}>
    <label>Country / 国家<select aria-label="Template country" value={country} disabled={busy} onChange={(event) => { setCountry(event.target.value); setError(null); }}>
      {!countries.length ? <option value="">No countries assigned / 尚未分配国家</option> : null}
      {countries.map((code) => <option key={code}>{code}</option>)}
    </select></label>
    <fieldset disabled={busy}><legend>Brands / 品牌（可多选）</legend>{brands.map((brand) => <label key={brand} style={{ marginRight: 12 }}>
      <input type="checkbox" checked={selectedBrands.includes(brand)} onChange={(event) => {
        setSelectedBrands(event.target.checked ? [...selectedBrands, brand] : selectedBrands.filter((value) => value !== brand)); setError(null);
      }} />{brand}
    </label>)}</fieldset>
    <p>Templates are personal unless explicitly shared. This does not create a PI or change prices / 模板默认个人，主动共享后他人才可用；不会创建PI或修改价格。</p>
  </ConfirmDialog>;
}

function TemplateEditor({ initial, onClose }: { initial: PiTemplateCatalog; onClose: () => void }) {
  const [catalog, setCatalog] = useState(initial);
  const [items, setItems] = useState(initial.items);
  const [draft, setDraft] = useState<Draft | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<PiInvoiceTemplate | null>(null);
  const [review, setReview] = useState(false);
  const [busy, setBusy] = useState(false);
  const [dirtyOrder, setDirtyOrder] = useState(false);
  const [dragged, setDragged] = useState("");
  const [feedback, setFeedback] = useState("");
  const [error, setError] = useState<ConfirmDialogError | null>(null);
  const fail = (cause: unknown) => setError({ title: "Template action failed / 模板操作失败", message: cause instanceof Error ? cause.message : String(cause) });
  const [chargesConfirmed, setChargesConfirmed] = useState(false);
  const [triedReview, setTriedReview] = useState(false);
  const [savedId, setSavedId] = useState("");
  const scope = { country: catalog.country, brands: catalog.brands };

  async function reload(owner = catalog.owner): Promise<void> {
    const next = await api.getInvoiceTemplates(scope, owner);
    setCatalog(next); setItems(next.items); setDirtyOrder(false);
  }
  async function parse(file: File): Promise<void> {
    setBusy(true); setError(null); setDraft(null); setReview(false); setFeedback(""); setChargesConfirmed(false); setTriedReview(false);
    try {
      const analysis = await api.previewPiTemplate(scope, file);
      const { sha256, sourceCells, sheets: _sheets, ...config } = analysis;
      setDraft({ file, cells: sourceCells, payload: { name: file.name.replace(/\.xlsx$/i, ""), country: catalog.country,
        brands: catalog.brands, shared: false, sha256, config } });
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }
  function edit(template: PiInvoiceTemplate): void {
    if (!template.config || !template.sha256) return;
    setDraft({ payload: { id: template.id, name: template.name, country: template.country, brands: template.brands,
      shared: template.shared, sha256: template.sha256, config: template.config }, cells: [] });
    setReview(false); setError(null); setFeedback(""); setChargesConfirmed(false); setTriedReview(false);
  }
  function updateConfig(config: PiTemplateConfig): void {
    if (draft) setDraft({ ...draft, payload: { ...draft.payload, config } });
    setReview(false); setError(null);
    if (draft && config.charges.join() !== draft.payload.config.charges.join()) setChargesConfirmed(false);
  }
  function move(id: string, target: string): void {
    if (busy || id === target) return;
    const source = items.find((item) => item.id === id);
    if (!source) return;
    const next = items.filter((item) => item.id !== id);
    next.splice(next.findIndex((item) => item.id === target), 0, source);
    setItems(next); setDirtyOrder(true); setFeedback("");
  }
  async function save(): Promise<void> {
    if (!draft || busy) return;
    if (!review) {
      setTriedReview(true);
      const issues = mappingIssues(draft.payload.config, draft.payload.name, chargesConfirmed);
      if (issues.length) {
        setError({ title: "Mapping incomplete / 映射未完成", message: `还有 ${issues.length} 项需要填写或核对；点击表单顶部清单定位。草稿已保留。` });
        window.requestAnimationFrame(() => locateField("issues"));
        return;
      }
      setReview(true); setError(null); return;
    }
    setBusy(true); setError(null);
    try {
      const saved = await api.savePiTemplate(draft.payload, draft.file);
      // A successful write is never retried merely because the following read failed.
      setDraft(null); setReview(false); setSavedId(saved.id); setFeedback(`Saved: ${saved.name} / 已保存，正在重读列表`);
      try { await reload(saved.owner); setFeedback(`Saved and refreshed: ${saved.name} / 已保存并刷新`); }
      catch (cause: unknown) { setFeedback("Saved, but refresh failed. Reload only / 已保存但刷新失败，请只重读"); fail(cause); }
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }
  async function saveOrder(): Promise<void> {
    setBusy(true); setError(null);
    try {
      await api.orderPiTemplates(scope, items.map((item) => item.id), catalog.owner);
      setDirtyOrder(false); setFeedback(`Order saved for ${catalog.owner} / 已保存该账号排序；顶部为默认`);
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }
  async function copy(template: PiInvoiceTemplate): Promise<void> {
    setBusy(true); setError(null);
    try {
      await api.copyPiTemplate(scope, template.id);
      setFeedback("Personal copy saved / 已保存为自己的个人副本");
      try { await reload(catalog.actor); } catch (cause: unknown) { fail(cause); }
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }
  async function remove(): Promise<void> {
    if (!deleteTarget || busy) return;
    setBusy(true); setError(null);
    try {
      await api.deletePiTemplate(scope, deleteTarget.id);
      setDeleteTarget(null); setFeedback("Template deleted. Re-upload original file to restore / 模板已删除，可重新上传原文件恢复");
      try { await reload(); } catch (cause: unknown) { fail(cause); }
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }
  if (deleteTarget) return <ConfirmDialog title="Delete PI template / 删除模板" description={`${deleteTarget.name} · ${deleteTarget.owner} · ${deleteTarget.country} · ${deleteTarget.brands.join(" / ")}`}
    cancelLabel="Cancel" confirmLabel="Delete template" loadingLabel="Deleting..." submitting={busy} error={error} tone="danger"
    onCancel={() => { setDeleteTarget(null); setError(null); }} onConfirm={() => void remove()}>
    <p>Deletes this source file and configuration, not any saved PI or exported file. Shared users lose access; copies remain independent / 删除该源文件与配置，不删除PI或已导出文件。共享用户将无法再使用此模板，个人副本不受影响。可重新上传原文件恢复。</p>
  </ConfirmDialog>;
  const config = draft?.payload.config;
  const issues = config && draft ? mappingIssues(config, draft.payload.name, chargesConfirmed) : [];
  const sourceFormula = config && draft?.cells.find((cell) => cell.cell === `${config.columns.total}${config.dataStart}` && cell.value.startsWith("="));
  return <ConfirmDialog title={draft ? (review ? "Confirm PI template / 确认模板" : "Review template mapping / 核对模板映射") : "PI templates / PI模板"}
    description={<>{catalog.country} · {catalog.brands.join(" / ")} · Personal order / 个人排序：{catalog.owner}
      {feedback ? <p role="status">{feedback}</p> : null}</>}
    cancelLabel={draft ? (review ? "Change mapping / 返回调整" : "Back to list / 返回列表") : "Close"} confirmLabel={draft ? (review ? "Save template / 保存模板" : "Review before saving / 保存前核对") : "Save order / 保存排序"}
    loadingLabel="Working..." submitting={busy} confirmDisabled={!draft && !dirtyOrder} error={error}
    onCancel={() => { if (review) { setReview(false); setError(null); } else if (draft) { setDraft(null); setTriedReview(false); setError(null); } else onClose(); }}
    onConfirm={() => void (draft ? save() : saveOrder())}>
    {!draft ? <>
      <p>Upload defaults personal. Share explicitly to the country + brand library. Others can use/copy, not edit your source. Sorting affects this account only / 上传默认个人；主动共享后授权账号可用/复制；排序仅影响此账号。</p>
      {catalog.admin ? <label>Manage owner / 管理账号<select aria-label="Manage template owner" value={catalog.owner} disabled={busy || dirtyOrder}
        onChange={(event) => { setBusy(true); void reload(event.target.value).catch(fail).finally(() => setBusy(false)); }}>
        {[...new Set([catalog.actor, ...catalog.owners])].map((owner) => <option key={owner}>{owner}</option>)}
      </select></label> : null}
      <label>Upload XLSX / 上传自己的模板<input aria-label="Upload PI template" type="file" accept=".xlsx" disabled={busy || dirtyOrder}
        onChange={(event) => { const file = event.target.files?.[0]; if (file) void parse(file); event.target.value = ""; }} /></label>
      {!items.length ? <p>No matching templates yet. Upload the first XLSX above / 暂无匹配模板，请从上方上传第一份XLSX。</p> : null}
      <p>One worksheet, one PI table, EUR; no macros or external links. Existing example orders are replaced, not imported / 单工作表、单PI明细表、EUR；不支持宏/外链，旧示例订单不会导入。</p>
      <ol className="pi-template-list">{items.map((item, index) => <li key={item.id} style={item.id === savedId ? { borderColor: "#2563eb", background: "#eff6ff" } : undefined} draggable={!busy} onDragStart={() => setDragged(item.id)}
        onDragOver={(event) => event.preventDefault()} onDrop={(event) => { event.preventDefault(); move(dragged, item.id); }}>
        <span aria-hidden="true">⠿ </span><strong>{item.name}</strong> {item.id === savedId ? "· Saved / 刚保存" : ""} {index === 0 ? "· Default / 默认" : ""}<small> {item.owner} · {item.system ? "System / 系统" : item.shared ? "Shared / 共享" : "Personal / 个人"}</small>
        {!item.system && (catalog.admin || item.owner === catalog.actor) ? <button type="button" disabled={busy || dirtyOrder} onClick={() => edit(item)}>Edit / 编辑</button> : null}
        {!item.system && (catalog.admin || item.owner === catalog.actor) ? <button type="button" disabled={busy || dirtyOrder} onClick={() => { setDeleteTarget(item); setError(null); }}>Delete / 删除</button> : null}
        {!item.system ? <button type="button" disabled={busy || dirtyOrder} onClick={() => void copy(item)}>Copy / 复制</button> : null}
        {index > 0 ? <button type="button" disabled={busy} aria-label={`Move ${item.name} up`} onClick={() => move(item.id, items[index - 1].id)}>↑</button> : null}
      </li>)}</ol>
      {dirtyOrder ? <p>Unsaved order: save below, or reload to discard / 排序未保存；请保存，或重读放弃。</p> : null}
      <button type="button" disabled={busy} onClick={() => { setBusy(true); void reload().then(() => {
        setError(null); setFeedback(savedId ? "Saved and refreshed / 已保存并刷新" : "List refreshed / 列表已重读");
      }).catch(fail).finally(() => setBusy(false)); }}>Reload list / 重读列表</button>
    </> : config ? <>
      {triedReview && issues.length ? <section id="pi-template-issues" tabIndex={-1} aria-label="Required mapping issues"><strong>待填写／核对 {issues.length} 项（点击定位）</strong>
        <ul>{issues.map((issue) => <li key={issue.target}><button type="button" disabled={busy} onClick={() => locateField(issue.target)}>{issue.message}</button></li>)}</ul>
      </section> : null}
      <p>Template scope / 模板范围：{draft.payload.country} · {draft.payload.brands.join(" / ")}。No BOM, quantities or PI snapshots are changed / 不修改BOM、计划数量或PI快照。</p>
      <p>解析只是候选，不等于已可保存。*为必填；未识别可从原文选入或手动填写。单元格是写入位置，前缀是需保留的标签，内容是完整商业资料。</p>
      <div className="pi-template-fields">
        <label>Name / 模板名*<input id="pi-template-template-name" aria-label="Template name" value={draft.payload.name} disabled={review} onChange={(event) => setDraft({ ...draft, payload: { ...draft.payload, name: event.target.value } })} /></label>
        <label><input type="checkbox" checked={draft.payload.shared} disabled={review} onChange={(event) => setDraft({ ...draft, payload: { ...draft.payload, shared: event.target.checked } })} />Share to country + brands / 共享至该国家品牌库</label>
        {(["headerRow", "dataStart", "totalRow"] as const).map((key) => <label key={key}>{{ headerRow: "表头首行", dataStart: "明细起始行", totalRow: "总计行" }[key]}<input id={`pi-template-${key}`} type="number" aria-label={key} value={config[key]} disabled={review}
          onChange={(event) => updateConfig({ ...config, [key]: Number(event.target.value) })} /></label>)}
      </div>
      <fieldset disabled={review}><legend>Detail columns / 明细列（输入A、B…；*必填）</legend><div className="pi-template-fields">
        {COLUMNS.map(({ key, label }) => <label key={key}>{label}<input id={`pi-template-column-${key}`} aria-label={`Column ${key}`} maxLength={2} value={config.columns[key] ?? ""}
          onChange={(event) => { const columns = { ...config.columns }; const value = event.target.value.trim().toUpperCase(); if (value) columns[key] = value; else delete columns[key]; updateConfig({ ...config, columns }); }} /></label>)}
      </div></fieldset>
      <fieldset disabled={review}><legend>Total = saved FOB + selected unit charges / 合计＝已存FOB＋以下每台费用</legend>
        {sourceFormula ? <p>原文件公式参考：{sourceFormula.cell} <code>{sourceFormula.value}</code>；不自动采用样例费用。</p> : null}
        {(["freightEur", "insuranceEur", "handlingEur"] as const).map((key) => <label key={key}><input type="checkbox" aria-label={key} checked={config.charges.includes(key)} onChange={(event) => updateConfig({ ...config, charges: event.target.checked ? [...config.charges, key] : config.charges.filter((charge) => charge !== key) })} />{{ freightEur: "Freight / 运费", insuranceEur: "Insurance / 保险", handlingEur: "Handling / 操作费" }[key]} </label>)}
        <p>本次公式：（FOB {config.charges.map((charge) => `＋${({ freightEur: "运费", insuranceEur: "保险", handlingEur: "操作费" })[charge]}`).join("")}）×数量 · EUR</p>
        <label><input id="pi-template-charges-confirmed" type="checkbox" checked={chargesConfirmed} onChange={(event) => { setChargesConfirmed(event.target.checked); setError(null); }} />I confirm the total calculation / 我已核对合计口径{!config.charges.length ? "：仅FOB，不含运保费／操作费" : ""}</label>
      </fieldset>
      <fieldset disabled={review}><legend>Fixed details and dynamic cell positions / 固定资料与动态字段位置</legend>
        <p>Cell must be the top-left of a merged field. Prefix is preserved; date/ref use each exported PI, not sample values / 合并字段用左上角；前缀保留，日期/编号每次重新生成。</p>
        {FIELDS.map(({ key, label, hint, optional, dynamic }) => { const field = config.fields[key] ?? { cell: "", value: "", prefix: "" };
          const complete = Boolean(field.cell && (optional || dynamic || ["portOfShipment", "portOfDischarge"].includes(key) || field.value.trim()));
          return <div key={key}><strong>{label}{optional ? "（可选）" : " *"}</strong><small style={{ display: "block" }}>{hint}</small>
            <small>{complete ? `待确认 · 来源/写入格 ${field.cell}` : optional ? "未填写，可留空" : "未识别／需填写"}</small>
            {draft.cells.length && !review ? <label style={{ display: "block" }}>从原单元格带入<select aria-label={`Source ${key}`} value="" onChange={(event) => {
              const source = draft.cells.find((cell) => cell.cell === event.target.value);
              if (source) updateConfig({ ...config, fields: { ...config.fields, [key]: { cell: source.cell, value: dynamic ? "" : source.value, prefix: "" } } });
            }}><option value="">请选择原文（不猜商业身份）</option>{draft.cells.filter((cell) => !cell.value.startsWith("=")).map((cell) => <option key={cell.cell} value={cell.cell}>{cell.cell} · {cell.value.slice(0, 70)}</option>)}</select></label> : null}
          <div className="pi-template-field-row">{(["cell", "prefix", ...(dynamic ? [] : ["value"])] as ("cell" | "prefix" | "value")[]).map((part) => <label key={part}>{{ cell: "单元格", prefix: "保留前缀（可空）", value: "完整内容" }[part]}<textarea id={`pi-template-${key}-${part}`} aria-label={`${key} ${part}`} rows={part === "value" ? 2 : 1} value={field[part]}
            onChange={(event) => updateConfig({ ...config, fields: { ...config.fields, [key]: { ...field, [part]: event.target.value } } })} /></label>)}</div>
          </div>;
        })}
      </fieldset>
      {draft.cells.length && !review ? <details><summary>Original cell text / 原文件单元格参考</summary><dl>{draft.cells.map((cell) => <div key={cell.cell}><dt>{cell.cell}</dt><dd>{cell.value}</dd></div>)}</dl></details> : null}
      {review ? <p>Confirm buyer, seller, bank, terms, charges and mappings above. Sharing exposes these template details to authorized users / 请核对以上买卖方、银行、条款、费用及映射；共享会向授权用户开放这些资料。返回调整不丢草稿。</p> : null}
    </> : null}
    <style>{`.pi-template-fields{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.pi-template-fields label,.pi-template-field-row label{display:grid;gap:4px;min-width:0}.pi-template-fields input,.pi-template-field-row textarea{width:100%;min-width:0;padding:6px;border:1px solid #cbd5e1;border-radius:4px}.pi-template-fields input[type=checkbox]{width:auto}.pi-template-field-row{display:grid;grid-template-columns:90px 120px minmax(0,1fr);gap:8px;margin:10px 0}.pi-template-list{padding-left:20px}.pi-template-list li{border:1px solid #cbd5e1;padding:10px;margin:8px 0;border-radius:6px;cursor:grab;overflow-wrap:anywhere}.pi-template-list button{margin-left:8px}.pi-template-field-row textarea{resize:vertical}@media(max-width:640px){.pi-template-fields{grid-template-columns:repeat(2,minmax(0,1fr))}.pi-template-field-row{grid-template-columns:1fr 1fr}.pi-template-field-row label:last-child{grid-column:1/-1}}`}</style>
  </ConfirmDialog>;
}
