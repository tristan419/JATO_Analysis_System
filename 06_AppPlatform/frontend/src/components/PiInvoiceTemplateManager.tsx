import { useState } from "react";
import { api } from "../api/client";
import type { PiInvoiceTemplate, PiTemplateCatalog, PiTemplateColumn, PiTemplateConfig, PiTemplateField, PiTemplateSave } from "../types/orderGeniusVehicle";
import { ConfirmDialog, type ConfirmDialogError } from "./ConfirmDialog";

const FIELDS: { key: PiTemplateField; label: string; dynamic?: boolean }[] = [
  { key: "sellerName", label: "Seller / 卖方" }, { key: "sellerAddress", label: "Seller address / 卖方地址（可选）" },
  { key: "buyerName", label: "Buyer / 买方" }, { key: "buyerAddress", label: "Buyer address / 买方地址" },
  { key: "buyerEmail", label: "Buyer email / 邮箱（可选）" }, { key: "bankDetails", label: "Bank details / 银行资料" },
  { key: "priceTerm", label: "Price term / 价格条款" }, { key: "paymentTerm", label: "Payment term / 付款条款" },
  { key: "invoiceDate", label: "Invoice date / 每次发票日期", dynamic: true }, { key: "referenceNo", label: "Reference / 每次PI编号", dynamic: true },
  { key: "portOfShipment", label: "Loading port / 默认装运港" }, { key: "portOfDischarge", label: "Discharge port / 默认卸货港" },
];
const COLUMNS: { key: PiTemplateColumn; label: string }[] = [
  { key: "number", label: "No" }, { key: "modelName", label: "Model*" }, { key: "version", label: "Version" },
  { key: "powertrain", label: "Powertrain / Capacity" }, { key: "exteriorColorName", label: "Exterior" },
  { key: "interiorColorName", label: "Interior" }, { key: "materialCode", label: "Material" },
  { key: "quantity", label: "Quantity*" }, { key: "fobEur", label: "Unit FOB*" }, { key: "fobAmount", label: "FOB amount" },
  { key: "freightEur", label: "Unit freight" }, { key: "insuranceEur", label: "Unit insurance" },
  { key: "handlingEur", label: "Unit handling" }, { key: "total", label: "Total amount*" },
];
interface Props { piCode: string; initial: PiTemplateCatalog; onClose: () => void }
interface Draft { payload: PiTemplateSave; file?: File; cells: { cell: string; value: string }[] }

export function PiInvoiceTemplateManager({ piCode, initial, onClose }: Props) {
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

  async function reload(owner = catalog.owner): Promise<void> {
    const next = await api.getPiInvoiceTemplates(piCode, owner);
    setCatalog(next); setItems(next.items); setDirtyOrder(false);
  }
  async function parse(file: File): Promise<void> {
    setBusy(true); setError(null); setDraft(null); setReview(false); setFeedback("");
    try {
      const analysis = await api.previewPiTemplate(piCode, file);
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
    setReview(false); setError(null); setFeedback("");
  }
  function updateConfig(config: PiTemplateConfig): void {
    if (draft) setDraft({ ...draft, payload: { ...draft.payload, config } });
    setReview(false);
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
    if (!review) { setReview(true); setError(null); return; }
    setBusy(true); setError(null);
    try {
      const saved = await api.savePiTemplate(piCode, draft.payload, draft.file);
      // A successful write is never retried merely because the following read failed.
      setDraft(null); setReview(false); setFeedback("Saved. Reloading list / 已保存，正在重读列表");
      try { await reload(saved.owner); setFeedback("Saved and refreshed / 已保存并刷新"); }
      catch (cause: unknown) { setFeedback("Saved, but refresh failed. Reload only / 已保存但刷新失败，请只重读"); fail(cause); }
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }
  async function saveOrder(): Promise<void> {
    setBusy(true); setError(null);
    try {
      await api.orderPiTemplates(piCode, items.map((item) => item.id), catalog.owner);
      setDirtyOrder(false); setFeedback(`Order saved for ${catalog.owner} / 已保存该账号排序；顶部为默认`);
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }
  async function copy(template: PiInvoiceTemplate): Promise<void> {
    setBusy(true); setError(null);
    try {
      await api.copyPiTemplate(piCode, template.id);
      setFeedback("Personal copy saved / 已保存为自己的个人副本");
      try { await reload(catalog.actor); } catch (cause: unknown) { fail(cause); }
    } catch (cause: unknown) { fail(cause); }
    finally { setBusy(false); }
  }
  async function remove(): Promise<void> {
    if (!deleteTarget || busy) return;
    setBusy(true); setError(null);
    try {
      await api.deletePiTemplate(piCode, deleteTarget.id);
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
  return <ConfirmDialog title={draft ? (review ? "Confirm PI template / 确认模板" : "Review template mapping / 核对模板映射") : "PI templates / PI模板"}
    description={`${catalog.country} · ${catalog.brands.join(" / ")} · Personal order / 个人排序：${catalog.owner}`}
    cancelLabel={draft ? "Back to list / 返回列表" : "Close"} confirmLabel={draft ? (review ? "Save template / 保存模板" : "Review before saving / 保存前核对") : "Save order / 保存排序"}
    loadingLabel="Working..." submitting={busy} confirmDisabled={!draft && !dirtyOrder} error={error}
    onCancel={() => { if (draft) { setDraft(null); setReview(false); setError(null); } else onClose(); }}
    onConfirm={() => void (draft ? save() : saveOrder())}>
    {feedback ? <p role="status">{feedback}</p> : null}
    {!draft ? <>
      <p>Upload defaults personal. Share explicitly to the country + brand library. Others can use/copy, not edit your source. Sorting affects this account only / 上传默认个人；主动共享后授权账号可用/复制；排序仅影响此账号。</p>
      {catalog.admin ? <label>Manage owner / 管理账号<select aria-label="Manage template owner" value={catalog.owner} disabled={busy || dirtyOrder}
        onChange={(event) => { setBusy(true); void reload(event.target.value).catch(fail).finally(() => setBusy(false)); }}>
        {[...new Set([catalog.actor, ...catalog.owners])].map((owner) => <option key={owner}>{owner}</option>)}
      </select></label> : null}
      <label>Upload XLSX / 上传自己的模板<input aria-label="Upload PI template" type="file" accept=".xlsx" disabled={busy || dirtyOrder}
        onChange={(event) => { const file = event.target.files?.[0]; if (file) void parse(file); event.target.value = ""; }} /></label>
      <p>One worksheet, one PI table, EUR; no macros or external links. Existing example orders are replaced, not imported / 单工作表、单PI明细表、EUR；不支持宏/外链，旧示例订单不会导入。</p>
      <ol className="pi-template-list">{items.map((item, index) => <li key={item.id} draggable={!busy} onDragStart={() => setDragged(item.id)}
        onDragOver={(event) => event.preventDefault()} onDrop={(event) => { event.preventDefault(); move(dragged, item.id); }}>
        <span aria-hidden="true">⠿ </span><strong>{item.name}</strong> {index === 0 ? "· Default / 默认" : ""}<small> {item.owner} · {item.system ? "System / 系统" : item.shared ? "Shared / 共享" : "Personal / 个人"}</small>
        {!item.system && (catalog.admin || item.owner === catalog.actor) ? <button type="button" disabled={busy || dirtyOrder} onClick={() => edit(item)}>Edit / 编辑</button> : null}
        {!item.system && (catalog.admin || item.owner === catalog.actor) ? <button type="button" disabled={busy || dirtyOrder} onClick={() => { setDeleteTarget(item); setError(null); }}>Delete / 删除</button> : null}
        {!item.system ? <button type="button" disabled={busy || dirtyOrder} onClick={() => void copy(item)}>Copy / 复制</button> : null}
        {index > 0 ? <button type="button" disabled={busy} aria-label={`Move ${item.name} up`} onClick={() => move(item.id, items[index - 1].id)}>↑</button> : null}
      </li>)}</ol>
      {dirtyOrder ? <p>Unsaved order: save below, or reload to discard / 排序未保存；请保存，或重读放弃。</p> : null}
      <button type="button" disabled={busy} onClick={() => { setBusy(true); void reload().catch(fail).finally(() => setBusy(false)); }}>Reload list / 重读列表</button>
    </> : config ? <>
      <p>Template scope / 模板范围：{draft.payload.country} · {draft.payload.brands.join(" / ")}。No BOM, quantities or PI snapshots are changed / 不修改BOM、计划数量或PI快照。</p>
      <div className="pi-template-fields">
        <label>Name / 模板名<input aria-label="Template name" value={draft.payload.name} disabled={review} onChange={(event) => setDraft({ ...draft, payload: { ...draft.payload, name: event.target.value } })} /></label>
        <label><input type="checkbox" checked={draft.payload.shared} disabled={review} onChange={(event) => setDraft({ ...draft, payload: { ...draft.payload, shared: event.target.checked } })} />Share to country + brands / 共享至该国家品牌库</label>
        {(["headerRow", "dataStart", "totalRow"] as const).map((key) => <label key={key}>{key}<input type="number" aria-label={key} value={config[key]} disabled={review}
          onChange={(event) => updateConfig({ ...config, [key]: Number(event.target.value) })} /></label>)}
      </div>
      <fieldset disabled={review}><legend>Detail columns / 明细列（输入A、B…；*必填）</legend><div className="pi-template-fields">
        {COLUMNS.map(({ key, label }) => <label key={key}>{label}<input aria-label={`Column ${key}`} maxLength={2} value={config.columns[key] ?? ""}
          onChange={(event) => { const columns = { ...config.columns }; const value = event.target.value.trim().toUpperCase(); if (value) columns[key] = value; else delete columns[key]; updateConfig({ ...config, columns }); }} /></label>)}
      </div></fieldset>
      <fieldset disabled={review}><legend>Total = saved FOB + selected unit charges / 合计＝已存FOB＋以下每台费用</legend>
        {(["freightEur", "insuranceEur", "handlingEur"] as const).map((key) => <label key={key}><input type="checkbox" checked={config.charges.includes(key)} onChange={(event) => updateConfig({ ...config, charges: event.target.checked ? [...config.charges, key] : config.charges.filter((charge) => charge !== key) })} />{key} </label>)}
        <p>{config.charges.length ? config.charges.join(" + ") : "FOB only / 仅FOB"} · EUR only / 不自动换汇</p>
      </fieldset>
      <fieldset disabled={review}><legend>Fixed details and dynamic cell positions / 固定资料与动态字段位置</legend>
        <p>Cell must be the top-left of a merged field. Prefix is preserved; date/ref use each exported PI, not sample values / 合并字段用左上角；前缀保留，日期/编号每次重新生成。</p>
        {FIELDS.map(({ key, label, dynamic }) => { const field = config.fields[key] ?? { cell: "", value: "", prefix: "" };
          return <div className="pi-template-field-row" key={key}><strong>{label}</strong>{(["cell", "prefix", ...(dynamic ? [] : ["value"])] as ("cell" | "prefix" | "value")[]).map((part) => <label key={part}>{part}<textarea aria-label={`${key} ${part}`} rows={part === "value" ? 2 : 1} value={field[part]}
            onChange={(event) => updateConfig({ ...config, fields: { ...config.fields, [key]: { ...field, [part]: event.target.value } } })} /></label>)}</div>;
        })}
      </fieldset>
      {draft.cells.length && !review ? <details><summary>Original cell text / 原文件单元格参考</summary><dl>{draft.cells.map((cell) => <div key={cell.cell}><dt>{cell.cell}</dt><dd>{cell.value}</dd></div>)}</dl></details> : null}
      {review ? <><p>Confirm buyer, seller, bank, terms, charges and mappings above. Sharing exposes these template details to authorized users / 请核对以上买卖方、银行、条款、费用及映射；共享会向授权用户开放这些资料。</p><button type="button" disabled={busy} onClick={() => setReview(false)}>Change mapping / 返回调整</button></> : null}
    </> : null}
    <style>{`.pi-template-fields{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.pi-template-fields label,.pi-template-field-row label{display:grid;gap:4px;min-width:0}.pi-template-fields input,.pi-template-field-row textarea{width:100%;min-width:0;padding:6px;border:1px solid #cbd5e1;border-radius:4px}.pi-template-fields input[type=checkbox]{width:auto}.pi-template-field-row{display:grid;grid-template-columns:1fr 70px 100px 2fr;gap:8px;margin:10px 0}.pi-template-list{padding-left:20px}.pi-template-list li{border:1px solid #cbd5e1;padding:10px;margin:8px 0;border-radius:6px;cursor:grab;overflow-wrap:anywhere}.pi-template-list button{margin-left:8px}.pi-template-field-row textarea{resize:vertical}.pi-template-field-row strong{font-size:12px}@media(max-width:640px){.pi-template-fields{grid-template-columns:repeat(2,minmax(0,1fr))}.pi-template-field-row{grid-template-columns:1fr 1fr}.pi-template-field-row strong{grid-column:1/-1}}`}</style>
  </ConfirmDialog>;
}
