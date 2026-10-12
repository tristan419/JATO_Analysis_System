import { useState } from "react";
import { api } from "../../api/client";
import { ConfirmDialog } from "../ConfirmDialog";
import type { MaterialReplacementCatalog } from "../../types/orderGenius";
import { orderGeniusPowertrainColor } from "../../utils/orderGeniusBomAdmin";
import "./MaterialTimelineBoard.css";

interface Props { brand: string; bomTemplate: string; onSaved: () => void }
interface Draft { from: string; to: string; note: string }

export function MaterialReplacementEditor({ brand, bomTemplate, onSaved }: Props) {
  const [open, setOpen] = useState(false);
  const [catalog, setCatalog] = useState<MaterialReplacementCatalog | null>(null);
  const [draft, setDraft] = useState<Draft>({ from: bomTemplate, to: "", note: "" });
  const [query, setQuery] = useState("");
  const [review, setReview] = useState(false);
  const [busy, setBusy] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState("");
  const chooseSource = (from: string, data: MaterialReplacementCatalog): void => {
    const link = data.links.find((item) => item.brand === brand && item.from === from);
    setDraft({ from, to: link?.to ?? "", note: link?.note ?? "" }); setReview(false);
  };
  const load = async (): Promise<void> => {
    setBusy(true); setError("");
    try {
      const data = await api.getMaterialReplacements();
      setCatalog(data); chooseSource(bomTemplate, data);
    } catch (failure: unknown) { setError(failure instanceof Error ? failure.message : String(failure)); }
    finally { setBusy(false); }
  };
  const current = catalog?.links.find((item) => item.brand === brand && item.from === draft.from);
  const source = catalog?.nodes.find((item) => item.brand === brand && item.bomTemplate === draft.from);
  const nodes = catalog?.nodes.filter((item) => item.brand === brand && (query
    ? `${item.bomTemplate} ${item.modelName} ${item.version}`.toLowerCase().includes(query.toLowerCase())
    : item.modelName === source?.modelName)) ?? [];
  const save = async (): Promise<void> => {
    if (!review) { setReview(true); return; }
    if (saved) { setOpen(false); onSaved(); return; }
    setBusy(true); setError("");
    try {
      const result = await api.saveMaterialReplacement({ brand, ...draft, to: draft.to || null, rowVersion: current?.rowVersion ?? 0 });
      setSaved(true);
      setError(result.changed ? "Saved / 已保存。关闭后重读关联与提醒；不会修改订单快照。" : "No actual change / 无实际变化。关闭后重读。" );
    } catch (failure: unknown) { setError(failure instanceof Error ? failure.message : String(failure)); }
    finally { setBusy(false); }
  };
  return <>
    <button type="button" className="btn btn-sm btn-ghost" onClick={() => {
      setOpen(true); setSaved(false); setReview(false); setCatalog(null); setQuery(""); void load();
    }}>Link versions / 关联版本</button>
    {open ? <ConfirmDialog title={review ? "Confirm base relationship / 确认基准物料关联" : `${brand} · Link versions / 关联版本`}
      description="Shared brand relation, including related history. Order dates, SKU codes, FOB, quantities and existing PIs stay unchanged. / 共享品牌关联；下单日期、物料号、价格、数量及旧PI均不变。"
      cancelLabel={saved ? "Close & reload / 关闭并重读" : "Cancel / 取消"}
      confirmLabel={saved ? "Close & reload / 关闭并重读" : review ? "Save relation / 保存关联" : "Preview relation / 预览关联"}
      loadingLabel="Loading / 保存中…" submitting={busy} confirmDisabled={!catalog || !source}
      error={error && !saved ? { title: "Relation not saved / 未保存", message: error } : null}
      onCancel={() => { setOpen(false); if (saved) onSaved(); }} onConfirm={() => void save()}>
      {saved ? <p role="status">{error}</p> : review ? <div className="og-replacement-preview">
        <p><strong>{draft.from}</strong></p><p>{current?.to ?? "No successor / 未关联"} → {draft.to || "Disconnect / 断开"}</p>
        <p>{draft.note || "No relation note / 无关联备注"}</p>
        <p>{source?.materialCodes.length} source SKUs / 源模板颜色；影响此品牌相关国家的时间轴。缺日期不补日期，已读不等于切换完成。</p>
        <button type="button" className="btn btn-sm btn-ghost" onClick={() => setReview(false)}>Back / 返回编辑</button>
      </div> : <div className="og-replacement-editor">
        {!catalog && !busy ? <button type="button" className="btn btn-sm" onClick={() => void load()}>Retry / 重读</button> : null}
        <label>Find base / 搜索基准物料<input value={query} onChange={(event) => setQuery(event.target.value)} /></label>
        <p>Drag a source card onto its successor, or choose below. / 从旧版卡片拖到新版，或用下方选择；不会自动保存。</p>
        <div className="og-replacement-canvas">
          {catalog && query && nodes.length === 0 ? <p role="status">No matching base / 未找到基准物料：{query}。请先维护缺失BOM；不能跳过中间版本或按证书自动新增。</p> : null}
          {nodes.map((node) => <button key={node.bomTemplate} type="button" draggable
            style={{ borderLeftColor: orderGeniusPowertrainColor(node.powertrain) }}
            className={draft.from === node.bomTemplate ? "is-source" : draft.to === node.bomTemplate ? "is-target" : ""}
            onDragStart={(event) => event.dataTransfer.setData("text/plain", node.bomTemplate)}
            onDragOver={(event) => event.preventDefault()}
            onDrop={(event) => { event.preventDefault(); const from = event.dataTransfer.getData("text/plain");
              if (!catalog?.nodes.some((item) => item.brand === brand && item.bomTemplate === from)) return;
              const original = catalog.links.find((item) => item.brand === brand && item.from === from);
              setDraft({ from, to: node.bomTemplate, note: original?.note ?? "" });
            }} onClick={() => { if (catalog) chooseSource(node.bomTemplate, catalog); }}>
            <strong>{node.bomTemplate}</strong><span>{node.modelName} · {node.version} · {node.powertrain}</span>
            <small>{catalog?.links.find((item) => item.brand === brand && item.from === node.bomTemplate)?.to ?? "No successor / 未关联"}</small>
          </button>)}
        </div>
        <label>Source / 旧base<select value={draft.from} onChange={(event) => { if (catalog) chooseSource(event.target.value, catalog); }}>
          {catalog?.nodes.filter((item) => item.brand === brand).map((node) => <option key={node.bomTemplate} value={node.bomTemplate}>{node.bomTemplate} · {node.modelName} · {node.version}</option>)}
        </select></label>
        <label>Successor / 新base<select value={draft.to} onChange={(event) => setDraft({ ...draft, to: event.target.value })}>
          <option value="">No successor / 断开关联</option>
          {catalog?.nodes.filter((item) => item.brand === brand && item.bomTemplate !== draft.from).map((node) => <option key={node.bomTemplate} value={node.bomTemplate}>{node.bomTemplate} · {node.modelName} · {node.version}</option>)}
        </select></label>
        <label>Change note / 更替说明<textarea value={draft.note} onChange={(event) => setDraft({ ...draft, note: event.target.value })} /></label>
      </div>}
    </ConfirmDialog> : null}
  </>;
}
