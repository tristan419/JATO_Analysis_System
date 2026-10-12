import { useEffect, useState } from "react";
import { api } from "../../api/client";
import type { MaterialNotice } from "../../types/orderGenius";

interface Props { countries: string[]; authorizationKey: string; revision: number; onViewModel: (brand: string, model: string, country?: string) => void }
interface Load { scope: string; status: "loading" | "ready" | "error"; notices: MaterialNotice[] }
export function MaterialNotices({ countries, authorizationKey, revision, onViewModel }: Props) {
  const countryKey = [...new Set(countries)].sort().join(",");
  const scope = `${authorizationKey}|${countryKey}`;
  const [load, setLoad] = useState<Load>({ scope, status: "loading", notices: [] });
  const [retry, setRetry] = useState(0);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    let current = true;
    setLoad({ scope, status: "loading", notices: [] }); setError("");
    const requested = countryKey ? countryKey.split(",") : [];
    void Promise.all(requested.map((country) => api.getMaterialNotices(country))).then((items) => {
      if (!current) return;
      if (items.some((item, index) => item.countryCode !== requested[index])) throw new Error("Scope changed");
      setLoad({ scope, status: "ready", notices: items.flatMap((item) => item.notices) });
    }).catch(() => { if (current) setLoad({ scope, status: "error", notices: [] }); });
    return () => { current = false; };
  }, [scope, countryKey, revision, retry]);
  const notices = load.scope === scope && load.status === "ready" ? load.notices : [];
  const mark = async (notice: MaterialNotice): Promise<void> => {
    setBusy(notice.key); setError("");
    try { await api.readMaterialNotice(notice.key, notice.countryCode, !notice.read); setRetry((value) => value + 1); }
    catch (failure: unknown) { setError(failure instanceof Error ? failure.message : String(failure)); }
    finally { setBusy(null); }
  };
  const groups = new Map<string, MaterialNotice[]>();
  for (const notice of notices) {
    const key = `${notice.brand}|${notice.modelName}`;
    groups.set(key, [...(groups.get(key) ?? []), notice]);
  }
  return <details className="og-material-notices">
    <summary>My material reminders / 我的物料提醒 · {load.scope !== scope || load.status === "loading" ? "Loading / 加载中" : load.status === "error" ? "Unavailable / 读取失败" : `${notices.filter((notice) => !notice.read).length} unread / 未读`}</summary>
    <p>Saved account scope and actual country quotes. Past/future 30 days, not the selected order month. / 当前授权国家品牌及报价范围，过去/未来30日，不随选品月份；已读不代表切换完成。</p>
    {load.status === "error" ? <button type="button" className="btn btn-sm" onClick={() => setRetry((value) => value + 1)}>Retry / 重读</button> : null}
    {[...groups].map(([key, items]) => <details key={key}><summary>{items[0].brand} · {items[0].modelName} · {items.length}</summary>
      {items.map((notice) => <div className={`og-material-notice${notice.read ? " is-read" : ""}`} key={notice.key}>
        <p>{notice.countryCode} · {notice.date} · {notice.kind === "start" ? "First order date / 开始下单" : notice.kind === "end" ? "Final order date / 最后下单日" : "Relation updated / 更替关系更新"}</p>
        <p>{notice.bomTemplate}{notice.kind === "relation" ? ` → ${notice.target ?? "Disconnected / 已断开"}` : ""} {notice.note}</p>
        <button type="button" className="btn btn-sm btn-ghost" onClick={() => onViewModel(notice.brand, notice.modelName, notice.countryCode)}>View model / 查看车型</button>
        <button type="button" className="btn btn-sm btn-ghost" disabled={busy !== null} onClick={() => void mark(notice)}>{notice.read ? "Mark unread / 标为未读" : "Mark read / 标为已读"}</button>
      </div>)}
    </details>)}
    {load.status === "ready" && notices.length === 0 ? <p>No material reminders in this scope / 当前范围暂无物料提醒，不代表BOM日期已完整维护。</p> : null}
    {error ? <p role="alert">{error}</p> : null}
  </details>;
}
