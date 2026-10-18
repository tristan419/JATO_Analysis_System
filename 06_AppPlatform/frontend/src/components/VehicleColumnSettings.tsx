import { useState } from "react";
import { ConfirmDialog } from "./ConfirmDialog";
import type { PiVehicleUnit, VehicleCustomColumn } from "../types/orderGeniusVehicle";
import { VEHICLE_COLUMNS, type VehicleColumnKey } from "./vehicleAllocationFields";

interface Props {
  columns: VehicleCustomColumn[]; visible: ReadonlySet<VehicleColumnKey>; targets: PiVehicleUnit[]; busy: boolean;
  error: string; onRetry: () => void; onVisible: (keys: Set<VehicleColumnKey>) => void;
  onCreate: (label: string) => Promise<void>; onDelete: (id: string) => Promise<void>;
  onSave: (id: string, value: string, targets: PiVehicleUnit[]) => Promise<void>;
}
export function VehicleColumnSettings({ columns, visible, targets, busy, error, onRetry, onVisible, onCreate, onDelete, onSave }: Props) {
  const [name, setName] = useState("");
  const [message, setMessage] = useState("");
  const [pending, setPending] = useState(false);
  const [deleting, setDeleting] = useState<VehicleCustomColumn | null>(null);
  const [editing, setEditing] = useState<VehicleCustomColumn | null>(null);
  const [value, setValue] = useState("");
  const [confirmValue, setConfirmValue] = useState(false);
  const [valueTargets, setValueTargets] = useState<PiVehicleUnit[]>([]);
  async function action(work: () => Promise<void>): Promise<void> {
    setPending(true); setMessage("");
    try { await work(); } catch (reason: unknown) { setMessage(reason instanceof Error ? reason.message : "未保存，请重试"); }
    finally { setPending(false); }
  }
  function toggle(key: VehicleColumnKey): void {
    const next = new Set(visible); if (next.has(key)) next.delete(key); else next.add(key); onVisible(next);
  }
  const waiting = busy || pending;
  return <fieldset className="va-columns" aria-label="Columns / 显示列"><legend>Columns / 显示列</legend>
    <p>显示和拖动顺序仅影响自己；系统列不能删除。自定义内容按车辆权限共享。</p>
    {error && <p role="alert">{error}<button type="button" disabled={waiting} onClick={onRetry}>重试</button></p>}
    {message && <p role="status">{message}</p>}
    <details><summary>系统列</summary><div className="va-column-options">{VEHICLE_COLUMNS.map((column) => <label key={column.key}>
      <input type="checkbox" checked={visible.has(column.key)} onChange={() => toggle(column.key)} />{column.label}</label>)}</div></details>
    <div className="va-column-add"><input aria-label="New column name / 新列名" maxLength={60} value={name} onChange={(event) => setName(event.target.value)} placeholder="新增共享文本列…" />
      <button type="button" disabled={waiting || !name.trim() || Boolean(error)} onClick={() => void action(async () => { await onCreate(name.trim()); setName(""); setMessage("已新增并保存"); })}>新增列</button></div>
    {columns.map((column) => <div className="va-custom-column" key={column.id}>
      <label><input type="checkbox" checked={visible.has(`custom:${column.id}`)} onChange={() => toggle(`custom:${column.id}`)} />{column.label}</label>
      <small>创建者：{column.createdBy}</small>
      <button type="button" className="btn-secondary" disabled={waiting || !targets.length} onClick={() => {
        setEditing(column); setValueTargets([...targets]); const first = targets[0].customValues?.[column.id] ?? "";
        setValue(targets.every((v) => (v.customValues?.[column.id] ?? "") === first) ? first : ""); setMessage("");
      }}>填写已勾选 {targets.length} 台</button>
      {column.canDelete && <button type="button" className="btn-ghost" disabled={waiting} onClick={() => { setDeleting(column); setMessage(""); }}>删除</button>}
    </div>)}
    {editing && <section aria-label="Edit shared values / 填写共享内容"><label>{editing.label} · {valueTargets.length} 台<textarea aria-label="Custom value / 自定义内容" maxLength={2000} value={value} onChange={(event) => setValue(event.target.value)} /></label>
      <button type="button" disabled={waiting} onClick={() => setConfirmValue(true)}>保存内容</button><button type="button" className="btn-secondary" disabled={waiting} onClick={() => setEditing(null)}>取消</button></section>}
    {(deleting || (editing && confirmValue)) && <ConfirmDialog title={deleting ? `删除列：${deleting.label}` : `保存：${editing?.label}`}
      description={deleting ? `将对所有账号停用此列。当前授权范围已勾选车辆中 ${targets.filter((v) => Boolean(v.customValues?.[deleting.id])).length} 台有内容；历史内容保留。系统列和其他数据不变。` : `只填写明确选定的 ${valueTargets.length} 台车辆；${value ? "覆盖该列原内容" : "清空该列内容"}，不改价格、VIN或其他列。`}
      cancelLabel="取消" confirmLabel={deleting ? "确认删除列" : "确认保存内容"} loadingLabel="保存中…" submitting={pending} error={message ? { title: "未保存", message } : null}
      onCancel={() => { setDeleting(null); setConfirmValue(false); }} onConfirm={() => void action(async () => {
        if (deleting) { await onDelete(deleting.id); setDeleting(null); }
        else if (editing) { await onSave(editing.id, value, valueTargets); setConfirmValue(false); setEditing(null); setMessage("内容已保存"); }
      })} />}
  </fieldset>;
}
