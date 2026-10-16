import { useEffect, useRef, useState } from "react";
import { api } from "../../api/client";
import type { MaterialNotice } from "../../types/orderGenius";

interface Props { countries: string[]; authorizationKey: string; revision: number; defaultExpanded?: boolean; onViewModel: (brand: string, model: string, country?: string, base?: string) => void }
interface Load { scope: string; status: "loading" | "ready" | "error"; notices: MaterialNotice[] }
interface EventGroup { key: string; brand: string; model: string; date: string; kind: MaterialNotice["kind"]; items: MaterialNotice[] }
const LABELS = { start: "First order date / 开始下单", end: "Final order date / 最后下单日", relation: "Relation updated / 更替关系更新" };

function groupEvents(notices: MaterialNotice[]): EventGroup[] {
  const groups = new Map<string, EventGroup>();
  for (const notice of notices) {
    const key = JSON.stringify([notice.brand, notice.modelName, notice.date, notice.kind,
      ...(notice.kind === "relation" ? [notice.bomTemplate, notice.target, notice.previousTarget, notice.note, notice.changedBy] : [])]);
    const group = groups.get(key) ?? { key, brand: notice.brand, model: notice.modelName, date: notice.date, kind: notice.kind, items: [] };
    group.items.push(notice); groups.set(key, group);
  }
  return [...groups.values()];
}

export function MaterialNotices({ countries, authorizationKey, revision, defaultExpanded = false, onViewModel }: Props) {
  const countryKey = [...new Set(countries)].sort().join(",");
  const scope = `${authorizationKey}|${countryKey}`;
  const currentScope = useRef(scope);
  currentScope.current = scope;
  const [load, setLoad] = useState<Load>({ scope, status: "loading", notices: [] });
  const [retry, setRetry] = useState(0);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [expanded, setExpanded] = useState(defaultExpanded);
  const [openModels, setOpenModels] = useState<string[]>([]);
  const [viewCountries, setViewCountries] = useState<Record<string, string>>({});
  useEffect(() => { setExpanded(defaultExpanded); setOpenModels([]); setViewCountries({}); setError(""); setBusy(null); }, [scope, defaultExpanded]);
  useEffect(() => {
    let current = true;
    setLoad({ scope, status: "loading", notices: [] });
    const requested = countryKey ? countryKey.split(",") : [];
    void Promise.all(requested.map((country) => api.getMaterialNotices(country))).then((items) => {
      if (!current) return;
      if (items.some((item, index) => item.countryCode !== requested[index])) throw new Error("Scope changed");
      setLoad({ scope, status: "ready", notices: items.flatMap((item) => item.notices) });
    }).catch(() => { if (current) setLoad({ scope, status: "error", notices: [] }); });
    return () => { current = false; };
  }, [scope, countryKey, revision, retry]);
  const events = groupEvents(load.scope === scope && load.status === "ready" ? load.notices : []);
  const mark = async (event: EventGroup): Promise<void> => {
    setBusy(event.key); setError("");
    const read = event.items.some((notice) => !notice.read);
    const results = await Promise.allSettled(event.items.filter((notice) => notice.read !== read)
      .map((notice) => api.readMaterialNotice(notice.key, notice.countryCode, read)));
    if (currentScope.current !== scope) return;
    const failed = results.filter((result) => result.status === "rejected").length;
    if (failed) setError(`${failed} records could not be updated. Reloaded current status / ${failed}条记录未能更新，已重读当前状态；未将整组标为成功。`);
    setRetry((value) => value + 1); setBusy(null);
  };
  const models = new Map<string, EventGroup[]>();
  for (const event of events) {
    const key = `${event.brand}|${event.model}`;
    models.set(key, [...(models.get(key) ?? []), event]);
  }
  return <details className="og-material-notices" open={expanded}>
    <summary onClick={(event) => { event.preventDefault(); setExpanded(!expanded); }}>My material reminders / 我的物料提醒 · {load.scope !== scope || load.status === "loading" ? "Loading / 加载中" : load.status === "error" ? "Unavailable / 读取失败" : `${events.filter((event) => event.items.some((notice) => !notice.read)).length} unread changes / 未读变化`}</summary>
    <p>Saved account scope and actual country quotes. Past/future 30 days, not the selected order month. / 当前授权国家品牌及报价范围，过去/未来30日，不随选品月份；已读不代表切换完成。</p>
    {load.status === "error" ? <button type="button" className="btn btn-sm" onClick={() => setRetry((value) => value + 1)}>Retry / 重读</button> : null}
    {[...models].map(([key, items]) => <details key={key} open={openModels.includes(key)}><summary onClick={(event) => {
      event.preventDefault(); setOpenModels((values) => values.includes(key) ? values.filter((value) => value !== key) : [...values, key]);
    }}>{items[0].brand} · {items[0].model} · {items.length} changes / 变化 · {items.filter((event) => event.items.some((notice) => !notice.read)).length} unread / 未读</summary>
      {items.map((event) => {
        const countries = [...new Set(event.items.map((notice) => notice.countryCode))].sort();
        const bases = [...new Set(event.items.map((notice) => notice.bomTemplate))];
        const country = countries.length === 1 ? countries[0] : viewCountries[event.key] ?? "";
        const read = event.items.every((notice) => notice.read);
        return <div className={`og-material-notice${read ? " is-read" : ""}`} key={event.key}>
          <div className="og-material-notice-summary"><strong>{event.date} · {LABELS[event.kind]}</strong>
            <span>{bases.length} bases / 基准物料 · {countries.length} countries / 国家</span>
            {countries.length > 1 ? <label>View country / 查看国家<select aria-label={`View country ${event.date} ${event.kind}`} value={country} onChange={(change) => setViewCountries((values) => ({ ...values, [event.key]: change.target.value }))}>
              <option value="">Choose country / 选择国家</option>{countries.map((value) => <option key={value} value={value}>{value}</option>)}
            </select></label> : <span>{country}</span>}
            <button type="button" className="btn btn-sm btn-ghost" disabled={!country} onClick={() => onViewModel(event.brand, event.model, country, event.items.find((item) => item.countryCode === country)?.bomTemplate)}>View model / 查看泳道</button>
            <button type="button" className="btn btn-sm btn-ghost" disabled={busy !== null} onClick={() => void mark(event)}>{busy === event.key ? "Saving / 保存中…" : read ? "Mark unread / 标为未读" : "Mark read / 标为已读"}</button>
          </div>
          <details><summary>Materials and countries / 物料及国家</summary>
            {event.items.map((notice) => <p key={notice.key}><button type="button" onClick={() => onViewModel(event.brand, event.model, notice.countryCode, notice.bomTemplate)}>{notice.countryCode} · {notice.bomTemplate}</button>{notice.kind === "relation" ? ` → ${notice.target ?? "Disconnected / 已断开"}` : ""}{notice.note ? ` · ${notice.note}` : ""} · {notice.read ? "Read / 已读" : "Unread / 未读"}</p>)}
          </details>
        </div>;
      })}
    </details>)}
    {load.status === "ready" && events.length === 0 ? <p>No material reminders in this scope / 当前范围暂无物料提醒，不代表BOM日期已完整维护。</p> : null}
    {error ? <p role="alert">{error}</p> : null}
  </details>;
}
