import { useState } from "react";
import { api } from "../../api/client";
import { ConfirmDialog, type ConfirmDialogError } from "../ConfirmDialog";

type TrashItem = { countryCode: string; rows: number };
type CountryAction = { country: string; kind: "delete" | "restore" };

export function BomCountryColumns({ countries, onChanged }: {
  countries: string[];
  onChanged: () => Promise<boolean>;
}) {
  const [country, setCountry] = useState("");
  const [trash, setTrash] = useState<TrashItem[] | null>(null);
  const [trashError, setTrashError] = useState("");
  const [action, setAction] = useState<CountryAction | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState<ConfirmDialogError | null>(null);

  async function readTrash(): Promise<void> {
    setTrash(null);
    setTrashError("");
    try {
      setTrash((await api.getCountryFobTrash()).items);
    } catch (cause) {
      setTrashError(`Failed to load deleted columns / 读取已删除国家列失败：${cause instanceof Error ? cause.message : String(cause)}`);
      throw cause;
    }
  }

  function choose(next: CountryAction): void {
    setError(null);
    setMessage("");
    setAction(next);
  }

  async function apply(): Promise<void> {
    if (!action || busy) return;
    setBusy(true);
    setError(null);
    try {
      let feedback: string;
      if (action.kind === "delete") {
        const result = await api.deleteCountryFobs(action.country);
        feedback = `${result.countryCode}: deleted ${result.cleared} records; available below for restore. / 已删除 ${result.cleared} 条记录，可在下方恢复。`;
        setCountry("");
      } else {
        const result = await api.restoreCountryFobs(action.country);
        feedback = `${result.countryCode}: restored ${result.restored}; skipped ${result.skippedActiveConflict} conflicts (not overwritten). / 已恢复 ${result.restored} 条；${result.skippedActiveConflict} 条冲突已跳过，未覆盖。`;
      }
      setAction(null);
      setMessage(feedback);
      // A successful write is not a failed save when a subsequent read fails.
      try {
        await readTrash();
        if (!await onChanged()) throw new Error("BOM refresh failed");
      } catch {
        setMessage(`${feedback} Refresh failed; reload BOM Admin to verify. / 重读失败，请刷新核对。`);
      }
    } catch (cause) {
      setError({ title: "Not saved / 未保存", message: cause instanceof Error ? cause.message : String(cause) });
    } finally {
      setBusy(false);
    }
  }

  return <div className="bom-admin-tool-tile">
    <div style={{ fontSize: 11, fontWeight: 800, marginBottom: 8 }}>Country columns / 国家列</div>
    <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
      <select aria-label="Country column to delete / 待删除国家列" value={country} disabled={busy}
        onChange={(event) => setCountry(event.target.value)}>
        <option value="">Select country / 选择国家</option>
        {countries.map((code) => <option key={code} value={code}>{code}</option>)}
      </select>
      <button type="button" className="btn btn-sm btn-ghost" disabled={busy || !country || !countries.includes(country)}
        onClick={() => choose({ country, kind: "delete" })}>Delete column / 删除国家列</button>
    </div>
    <p style={{ fontSize: 11 }}>All authorized materials, including phases; not just current search. Saved PIs stay unchanged.<br />全部授权物料及阶段，不限当前搜索；已保存 PI 不变。</p>
    <details onToggle={(event) => {
      if (event.currentTarget.open) void readTrash().catch(() => { /* Displayed in the section. */ });
    }}>
      <summary>Deleted columns / 已删除国家列</summary>
      {trashError ? <p role="alert">{trashError}</p> : trash === null ? <p role="status">Loading / 读取中…</p>
        : trash.length === 0 ? <p>No deleted columns / 暂无已删除国家列</p> : trash.map((item) => <div key={item.countryCode}>
        {item.countryCode} · {item.rows} records / 条
        <button type="button" className="btn btn-sm btn-ghost" disabled={busy}
          onClick={() => choose({ country: item.countryCode, kind: "restore" })}>Restore {item.countryCode} / 恢复</button>
      </div>)}
    </details>
    {message ? <p role="status" style={{ fontSize: 11 }}>{message}</p> : null}
    {action ? <ConfirmDialog
      title={`${action.kind === "delete" ? "Delete country column / 删除国家列" : "Restore country column / 恢复国家列"}: ${action.country}`}
      description={action.kind === "delete"
        ? "Stop this country's current BOM quotations and phases for all authorized materials, regardless of search filters. Amounts, dates and colour selections are retained for restore. Country records, monthly quantities and saved PIs are not deleted. / 将停用该国全部授权物料的当前报价及阶段，不限当前搜索。金额、日期及颜色选择保留，可恢复；不删除国家资料、月计划数量或已保存 PI。"
        : "Restore only records deleted with the country column. New quotations or overlapping phases are not overwritten; conflicts will be reported. / 仅恢复随国家列删除的记录，不覆盖新报价或重叠阶段；会报告冲突。"}
      cancelLabel="Cancel / 取消" confirmLabel={action.kind === "delete" ? "Delete / 删除" : "Restore / 恢复"}
      loadingLabel="Saving / 保存中" submitting={busy} error={error}
      tone={action.kind === "delete" ? "danger" : "default"}
      onCancel={() => { if (!busy) setAction(null); }} onConfirm={() => void apply()}
    /> : null}
  </div>;
}
