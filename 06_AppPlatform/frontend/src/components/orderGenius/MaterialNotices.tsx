import { useEffect, useRef, useState } from "react";
import { api } from "../../api/client";
import type { MaterialNotice } from "../../types/orderGenius";
import type { AssetLanguage } from "./materialAssetUi";
interface Props {
  countries: string[]; authorizationKey: string; revision: number; defaultExpanded?: boolean; language?: AssetLanguage;
  onViewModel: (brand: string, model: string, country?: string, base?: string) => void;
}
interface EventGroup { key: string; notices: MaterialNotice[] }
function groupEvents(notices: MaterialNotice[]): EventGroup[] {
  const groups = new Map<string, EventGroup>();
  for (const notice of notices) {
    const key = JSON.stringify([notice.brand, notice.modelName, notice.date, notice.kind,
      ...(notice.kind === "relation" ? [notice.bomTemplate, notice.target, notice.previousTarget, notice.note, notice.changedBy] : [])]);
    const group = groups.get(key) ?? { key, notices: [] }; group.notices.push(notice); groups.set(key, group);
  }
  return [...groups.values()];
}
export function MaterialNotices({ countries, authorizationKey, revision, defaultExpanded = false, language = "en", onViewModel }: Props) {
  const t = (en: string, zh: string): string => language === "en" ? en : zh;
  const countryKey = [...new Set(countries)].sort().join(","), scope = `${authorizationKey}|${countryKey}`;
  const currentScope = useRef(scope); currentScope.current = scope;
  const [load, setLoad] = useState<{ scope: string; status: "ready" | "error" | "loading"; notices: MaterialNotice[] }>({ scope: "", status: "loading", notices: [] });
  const [retry, setRetry] = useState(0), [busy, setBusy] = useState(false), [error, setError] = useState("");
  useEffect(() => {
    let current = true; setError(""); setBusy(false);
    const requested = countryKey ? countryKey.split(",") : [];
    void Promise.all(requested.map(country => api.getMaterialNotices(country))).then(items => {
      if (!current) return;
      if (items.some((item, i) => item.countryCode !== requested[i])) throw new Error("Scope changed");
      setLoad({ scope, status: "ready", notices: items.flatMap(item => item.notices) });
    }).catch(() => { if (current) setLoad({ scope, status: "error", notices: [] }); });
    return () => { current = false; };
  }, [scope, countryKey, revision, retry]);
  const events = groupEvents(load.scope === scope && load.status === "ready" ? load.notices : []);
  const mark = async (group: EventGroup): Promise<void> => {
    setBusy(true); setError(""); const read = group.notices.some(n => !n.read);
    const results = await Promise.allSettled(group.notices.filter(n => n.read !== read).map(n => api.readMaterialNotice(n.key, n.countryCode, read)));
    if (currentScope.current !== scope) return;
    if (results.some(r => r.status === "rejected")) setError(t("Some updates failed. Current state reloaded.", "部分记录更新失败，已重读当前状态。"));
    setBusy(false); setRetry(v => v + 1);
  };
  return <details className="mw-notices" open={defaultExpanded || undefined}>
    <summary title={t("Personal reminders · ±30 days · independent of the order month", "个人提醒 · 前后30天 · 不随选品月份")}>{t("Reminders", "提醒")} <span className="mw-count">{events.filter(g => g.notices.some(n => !n.read)).length}</span>{load.scope !== scope ? " …" : load.status === "error" ? ` · ${t("Unavailable", "读取失败")}` : ""}</summary>
    {load.status === "error" ? <button type="button" onClick={() => setRetry(v => v + 1)}>{t("Retry", "重试")}</button> : null}
    {events.map(group => {
      const n = group.notices[0], read = group.notices.every(item => item.read);
      return <div key={group.key} className={`mw-notice${read ? " is-read" : ""}`}><span className="mw-notice-dot" />
        <time>{n.date}</time><button type="button" className="mw-link" onClick={() => onViewModel(n.brand, n.modelName, n.countryCode, n.bomTemplate)}>{n.modelName}</button>
        <span>{n.kind === "start" ? t("Starts", "生效") : n.kind === "end" ? t("Ends", "截止") : t("Replaced", "更替")}</span>
        <span title={group.notices.map(item => `${item.countryCode} · ${item.bomTemplate}${item.target ? ` → ${item.target}` : ""}`).join("\n")}>{group.notices.length} {t("records", "条")}</span>
        <button type="button" disabled={busy} onClick={() => void mark(group)}>{read ? t("Mark unread", "标未读") : t("Mark read", "标已读")}</button>
      </div>;
    })}
    {load.scope === scope && load.status === "ready" && !events.length ? <span className="mw-muted">{t("No reminders", "暂无提醒")}</span> : null}
    {error ? <p role="alert">{error}</p> : null}
  </details>;
}
