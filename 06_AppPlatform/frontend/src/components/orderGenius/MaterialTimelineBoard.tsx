import { useEffect, useState, type CSSProperties } from "react";
import { api } from "../../api/client";
import type { MaterialSkuMatrixRow, MaterialTimelineNode, MaterialTimelineResponse, WvtaReferenceRecord } from "../../types/orderGenius";
import { orderGeniusPowertrainColor } from "../../utils/orderGeniusBomAdmin";
import { compareProductModels } from "../../utils/orderGeniusProductSort";
import "./MaterialTimelineBoard.css";
import { WvtaReferenceUpload } from "./WvtaReferenceUpload";

interface Props {
  countries: string[];
  authorizationKey: string;
  selectionRows: Array<MaterialSkuMatrixRow & { _countryCode?: string }>;
  selectionComplete: boolean;
  bomEditing?: boolean;
  canManageReference?: boolean;
  onViewModel: (brand: string, model: string) => void;
}
type LoadState = { scope: string; status: "loading" | "error" | "ready"; data: MaterialTimelineResponse[] };
type NodeRef = { country: string; node: MaterialTimelineNode; certificate?: WvtaReferenceRecord };
interface ModelGroup { key: string; country: string; brand: string; model: string; nodes: MaterialTimelineNode[]; events: MaterialTimelineResponse["events"] }
const STATUS: Record<string, string> = {
  active: "Active / 生效", phase_out: "Phase out / 即将结束",
  not_yet_active: "Not yet active / 尚未生效", historical: "Historical / 历史", review: "Review / 待核对",
};
const dayNumber = (day: string): number => Date.parse(`${day}T00:00:00Z`) / 86400000;

export function MaterialTimelineBoard({ countries, authorizationKey, selectionRows, selectionComplete, bomEditing = false, canManageReference = false, onViewModel }: Props) {
  const [showWvta, setShowWvta] = useState(false);
  const [expandedModels, setExpandedModels] = useState<string[]>([]);
  const countryKey = [...new Set(countries)].sort().join(",");
  const scope = `${authorizationKey}|${countryKey}|${showWvta}`;
  const [load, setLoad] = useState<LoadState>({ scope, status: "loading", data: [] });
  const [revision, setRevision] = useState(0);
  const [expanded, setExpanded] = useState(false);
  const [onlySelection, setOnlySelection] = useState(false);
  const [focusModel, setFocusModel] = useState<string | null>(null);
  const [selected, setSelected] = useState<NodeRef | null>(null);
  useEffect(() => { setExpandedModels([]); }, [authorizationKey, countryKey]);
  useEffect(() => {
    let current = true;
    setLoad({ scope, status: "loading", data: [] });
    setSelected(null); setFocusModel(null);
    if (bomEditing) return () => { current = false; };
    const requested = countryKey ? countryKey.split(",") : [];
    void Promise.all(requested.map((country) => api.getMaterialTimeline(country, showWvta))).then((data) => {
      if (!current) return;
      if (data.some((item, index) => item.countryCode !== requested[index] || item.today !== data[0].today
        || (showWvta && item.wvtaReference?.sha256 !== data[0].wvtaReference?.sha256))) {
        setLoad({ scope, status: "error", data: [] });
        return;
      }
      setLoad({ scope, status: "ready", data });
    }).catch(() => { if (current) setLoad({ scope, status: "error", data: [] }); });
    return () => { current = false; };
  }, [scope, countryKey, revision, bomEditing, showWvta]);

  // Never render a previous country's response during a new request.
  const data = load.scope === scope && load.status === "ready" ? load.data : [];
  const grouped: ModelGroup[] = [];
  for (const item of data) {
    const visible = new Set<string>();
    if (!onlySelection) item.nodes.forEach((node) => visible.add(node.bomTemplate));
    else if (selectionComplete) {
      for (const node of item.nodes) {
        if (selectionRows.some((row) => row._countryCode === item.countryCode &&
          (row.bomTemplate === node.bomTemplate || node.materialCodes.includes(row.materialCode)))) visible.add(node.bomTemplate);
      }
      let changed = true;
      while (changed) {
        changed = false;
        for (const link of item.links) {
          if ((visible.has(link.from) || visible.has(link.to)) && (!visible.has(link.from) || !visible.has(link.to))) {
            visible.add(link.from); visible.add(link.to); changed = true;
          }
        }
      }
    }
    const models = new Map<string, ModelGroup>();
    for (const node of item.nodes.filter((value) => visible.has(value.bomTemplate))) {
      const key = `${item.countryCode}|${node.brand}|${node.modelName}`;
      let model = models.get(key);
      if (!model) {
        model = { key, country: item.countryCode, brand: node.brand, model: node.modelName, nodes: [], events: [] };
        models.set(key, model);
      }
      model.nodes.push(node);
      model.events.push(...item.events.filter((event) => event.bomTemplate === node.bomTemplate));
    }
    grouped.push(...models.values());
  }
  grouped.sort((a, b) => a.country.localeCompare(b.country) || compareProductModels(
    a.brand, a.model, a.nodes[0].powertrain, b.brand, b.model, b.nodes[0].powertrain,
  ));
  const noticeMap = new Map<string, { key: string; brand: string; model: string; countries: Set<string>; starts: Set<string>; ends: Set<string> }>();
  for (const group of grouped.filter((value) => value.events.length > 0)) {
    const key = `${group.brand}|${group.model}`;
    const notice = noticeMap.get(key) ?? { key, brand: group.brand, model: group.model, countries: new Set<string>(), starts: new Set<string>(), ends: new Set<string>() };
    notice.countries.add(group.country);
    for (const event of group.events) (event.kind === "start" ? notice.starts : notice.ends).add(`${event.bomTemplate}|${event.date}`);
    noticeMap.set(key, notice);
  }
  const notices = [...noticeMap.values()];
  const dated = data.flatMap((item) => [item.windowFrom, item.windowTo,
    ...item.nodes.flatMap((node) => [node.effectiveFrom, node.effectiveTo].filter((value): value is string => value !== null)),
    ...(showWvta ? (item.wvtaReference?.records ?? []).filter((record) => record.sourceStatus === "recorded")
      .flatMap((record) => [record.validFrom, record.validTo].filter((value): value is string => value !== null)) : [])]);
  const left = Math.min(...dated.map(dayNumber));
  const right = Math.max(...dated.map(dayNumber));
  const position = (day: string): number => Math.max(0, Math.min(100, 100 * (dayNumber(day) - left) / Math.max(1, right - left)));
  const selectedNode = selected ? data.find((item) => item.countryCode === selected.country)?.nodes.find(
    (node) => node.bomTemplate === selected.node.bomTemplate) : undefined;
  const busy = load.scope !== scope || load.status === "loading";
  const selectionPending = onlySelection && !selectionComplete;
  const undatedCount = new Set(grouped.flatMap((group) => group.nodes.filter((node) => !node.effectiveFrom && !node.effectiveTo)
    .map((node) => `${node.brand}|${node.bomTemplate}`))).size;
  return <section className="og-timeline-board" aria-label="Material changes / 物料变化">
    <div className="og-timeline-summary">
      <strong>Material changes / 物料变化</strong>
      <span role="status">{busy ? "Loading / 读取中…" : load.status === "error" ? "Read failed / 读取失败" : selectionPending ? "Selection loading / 选品读取中…" : `${notices.length} models with recent changes / 近期变化车型 · ${undatedCount} bases undated / 基准日期待维护或核对`}</span>
      <button type="button" className="btn btn-sm btn-ghost" aria-expanded={expanded} onClick={() => setExpanded(!expanded)}>
        {expanded ? "Hide timeline / 收起" : "View timeline / 查看时间轴"}
      </button>
      <button type="button" className="btn btn-sm btn-ghost" disabled={busy} onClick={() => setRevision(revision + 1)}>Reload / 重读</button>
    </div>
    {load.scope === scope && load.status === "error" ? <p role="alert">Unable to read this scope. Reload or sign in again to check current permissions / 当前范围读取失败，请重读或重新登录核对权限。</p> : null}
    {expanded ? <div className="og-wvta-controls">
      <label><input type="checkbox" checked={showWvta} onChange={(event) => setShowWvta(event.currentTarget.checked)} /> Show WVTA reference / 叠加WVTA参考（不改变BOM下单日期）</label>
      {canManageReference ? <WvtaReferenceUpload onSaved={() => setRevision((value) => value + 1)} /> : null}
    </div> : null}
    {expanded && !busy && load.status === "ready" ? <div className="og-timeline-content">
      <p>As of / 按今天 {data[0]?.today} · {data[0]?.windowFrom} – {data[0]?.windowTo} · Not the selected order month / 不随选品月份</p>
      {showWvta ? <p className="og-wvta-source">{data[0]?.wvtaReference?.status === "ready"
        ? `Reference / 参考：${data[0].wvtaReference.filename} · SHA256 ${data[0].wvtaReference.sha256} · ${data[0].wvtaReference.updatedAt}`
        : "No WVTA reference configured / 尚未配置WVTA参考，请联系管理员；BOM日期不变。"}</p> : null}
      <label><input type="checkbox" checked={onlySelection} disabled={!selectionComplete && !onlySelection} onChange={(event) => {
        setOnlySelection(event.currentTarget.checked); setFocusModel(null); setSelected(null);
      }} /> Only current selection products, including linked history / 仅当前选品产品及已关联历史</label>
      {!selectionComplete ? <p>Wait for the complete selection data / 请等待完整选品数据，暂不能限定选品产品。</p> : null}
      {!selectionPending ? <><div className="og-timeline-notices">
        {notices.map((notice) => <button key={notice.key} type="button" className="btn btn-sm btn-ghost" onClick={() => {
          setFocusModel(notice.key); setSelected(null);
        }}>
          {[...notice.countries].join(" / ")} · {notice.brand} · {notice.model} · {notice.starts.size} starts / 生效
          {" · "}{notice.ends.size} ends / 截止
        </button>)}
        {notices.length === 0 ? <span>No dated changes in this window / 本窗口无已维护日期的变化；不代表所有日期已维护。</span> : null}
      </div>
      {focusModel ? <button type="button" className="btn btn-sm btn-ghost" onClick={() => setFocusModel(null)}>All models / 全部车型</button> : null}
      {grouped.length === 0 ? <p>No eligible products in this scope / 当前范围没有符合条件的产品。</p> : null}
      <div className="og-timeline-scroll">
        {grouped.filter((group) => !focusModel || `${group.brand}|${group.model}` === focusModel).map((group) => {
          const item = data.find((value) => value.countryCode === group.country);
          const links = item?.links ?? [];
          const byTemplate = new Map(group.nodes.map((node) => [node.bomTemplate, node]));
          const seen = new Set<string>();
          const branches: MaterialTimelineNode[][] = [];
          const heads = group.nodes.filter((node) => !links.some((link) => link.to === node.bomTemplate && byTemplate.has(link.from)));
          for (const head of [...heads, ...group.nodes]) {
            if (seen.has(head.bomTemplate)) continue;
            const branch: MaterialTimelineNode[] = [];
            let node: MaterialTimelineNode | undefined = head;
            while (node && !seen.has(node.bomTemplate)) {
              seen.add(node.bomTemplate); branch.push(node);
              const outgoing = links.filter((link) => link.from === node?.bomTemplate && byTemplate.has(link.to));
              node = outgoing.length === 1 ? byTemplate.get(outgoing[0].to) : undefined;
            }
            branches.push(branch);
          }
          return <details className="og-timeline-model" key={group.key} open={focusModel === `${group.brand}|${group.model}` || expandedModels.includes(group.key)}>
            <summary onClick={(event) => { event.preventDefault(); setExpandedModels((values) => values.includes(group.key) ? values.filter((key) => key !== group.key) : [...values, group.key]); }}>
              {group.country} · {group.brand} · {group.model} · {group.nodes.length} bases / 基准物料</summary>
            <div className="og-timeline-axis"><span>Base / 基准物料</span><div>{[0, 25, 50, 75, 100].map((percent) => <span key={percent} style={{ left: `${percent}%` }}>
              {new Date((left + (right - left) * percent / 100) * 86400000).toISOString().slice(0, 10)}
            </span>)}</div></div>
            {branches.map((branch) => <div className="og-timeline-branch" key={branch[0].bomTemplate}>
              <div className="og-timeline-relationship">{branch.length > 1 ? `Saved relation / 已保存关联：${branch.map((node) => node.bomTemplate).join(" → ")}` : links.some((link) => link.from === branch[0].bomTemplate || link.to === branch[0].bomTemplate)
                ? `Saved relation / 已保存关联：${links.filter((link) => link.from === branch[0].bomTemplate || link.to === branch[0].bomTemplate).map((link) => `${link.from} → ${link.to}`).join("；")}` : "Relation not maintained / 关联未维护"}</div>
              {branch.map((node) => <div className="og-timeline-lane" key={node.bomTemplate}>
                <button type="button" className="og-timeline-node" aria-label={`${group.country} · ${node.bomTemplate} · ${node.version} · ${node.powertrain} · ${STATUS[node.lifecycleStatus]}`}
                  style={{ borderLeftColor: orderGeniusPowertrainColor(node.powertrain) }} onClick={() => setSelected({ country: group.country, node })}>
                  <strong>{node.bomTemplate}</strong><span>{node.version} · {node.powertrain} · {node.materialCodes.length} SKUs</span>
                  <span>{STATUS[node.lifecycleStatus] ?? node.lifecycleStatus}{node.issues.length ? " · Review / 待核对" : ""}</span>
                </button>
                <div className={`og-timeline-track is-${node.lifecycleStatus}`} style={{ "--og-timeline-powertrain": orderGeniusPowertrainColor(node.powertrain) } as CSSProperties}>
                  <i className="og-timeline-today" style={{ left: `${position(item?.today ?? data[0].today)}%` }} aria-hidden="true" />
                  {node.effectiveFrom && node.effectiveTo ? <div className={`og-timeline-range is-${node.lifecycleStatus}`} style={{ left: `${position(node.effectiveFrom)}%`, width: `${Math.max(0.25, position(node.effectiveTo) - position(node.effectiveFrom))}%` }} /> : null}
                  {[node.effectiveFrom, node.effectiveTo].map((day, index) => day && !(index === 1 && day === node.effectiveFrom) ? <button type="button" className="og-timeline-pin" key={`${day}-${index}`} style={{ left: `${position(day)}%` }}
                    aria-label={`${node.bomTemplate} ${index === 0 ? "starts" : "ends"} ${day}`} title={`${index === 0 ? "First order date" : "Final order date"} ${day}`}
                    onClick={() => setSelected({ country: group.country, node })} /> : null)}
                  <span className="og-timeline-dates">{node.effectiveFrom ?? "Start unknown / 起始未设"} → {node.effectiveTo ?? "End not set / 截止未设"}</span>
                  {!node.effectiveFrom && !node.effectiveTo ? <span className="og-timeline-undated">Dates not maintained / 日期待维护</span> : null}
                  {showWvta && item?.wvtaReference?.status === "ready" ? <div className="og-wvta-segments">
                    {item.wvtaReference.records.filter((record) => record.materialGroup === node.bomTemplate).map((record, index) => <button
                      type="button" className={`og-wvta-segment is-${record.sourceStatus}`} key={`${record.pageNumber}-${record.tableRowNumber}-${index}`}
                      onClick={() => setSelected({ country: group.country, node, certificate: record })}>
                      {record.sourceStatus === "recorded" && record.validFrom ? <i className={record.validTo ? "" : "is-open"}
                        style={{ left: `${position(record.validFrom)}%`, width: `${Math.max(.25, (record.validTo ? position(record.validTo) : 100) - position(record.validFrom))}%` }} /> : null}
                      <span>WVTA · {record.wvtaNo} · {record.validFrom ?? "Start not recorded / 起始未记录"} → {record.validTo ?? "End not recorded / 截止未记录"}
                        {record.sourceStatus !== "recorded" ? ` · ${record.sourceStatus === "cancelled" ? "Cancelled / 源行作废" : "Review / 待核对"}` : ""}</span>
                    </button>)}
                    {!item.wvtaReference.records.some((record) => record.materialGroup === node.bomTemplate) ? <span>No exact certificate match / 无精确证书记录，不猜测日期。</span> : null}
                  </div> : null}
                </div>
              </div>)}
            </div>)}
          </details>;
        })}
      </div>
      {selectedNode && selected ? <div className="og-timeline-detail" role="region" aria-label="Material change detail">
        <div><strong>{selected.country} · {selectedNode.bomTemplate}</strong><button type="button" className="btn btn-sm btn-ghost" onClick={() => setSelected(null)}>Close / 关闭</button></div>
        <p>{selectedNode.brand} · {selectedNode.modelName} · {selectedNode.version} · {STATUS[selectedNode.lifecycleStatus]}</p>
        <p>First / 开始：{selectedNode.effectiveFrom ?? "Not set / 未设"} · Final / 最后可下单日：{selectedNode.effectiveTo ?? "Not set / 未设"}</p>
        <p>Quote reference / 报价参考日 {selectedNode.quoteDate}：{selectedNode.quotedSkuCount}/{selectedNode.materialCodes.length} SKUs ready / 报价就绪。PI still validates its order date / PI 仍须校验实际订单日。</p>
        {selectedNode.issues.map((issue) => <p key={issue} role="alert">{issue}</p>)}
        {selectedNode.quoteReasons.map((reason) => <p key={reason}>{reason}</p>)}
        <p>Notes / 备注：{selectedNode.notes.join("；") || "Not maintained / 未维护"}</p>
        <p className="og-timeline-codes">{selectedNode.materialCodes.join(" · ")}</p>
        {selected.certificate ? <div className="og-wvta-detail">
          <strong>Certificate reference only / 仅证书参考：{selected.certificate.sourceStatus}</strong>
          <p>{selected.certificate.wvtaNo} · {selected.certificate.cocNo}</p>
          <p>{selected.certificate.validFrom ?? "Not recorded / 未记录"} → {selected.certificate.validTo ?? "End not recorded / 截止未记录"}</p>
          <p>Source / 来源：{data.find((item) => item.countryCode === selected.country)?.wvtaReference?.filename} · P{selected.certificate.pageNumber} · row / 行 {selected.certificate.tableRowNumber}</p>
          <p>{selected.certificate.comments} {selected.certificate.reviewReason}</p>
        </div> : null}
        <button type="button" className="btn btn-sm btn-primary" disabled={!selectedNode.brand} onClick={() => onViewModel(selectedNode.brand, selectedNode.modelName)}>View model selection / 查看车型选品</button>
      </div> : null}
      </> : null}
      <p>Dashed line = today. Order windows are inclusive. Certificate periods are not order dates. Read-only / 虚线＝今天，下单起止日含当天，证书期不替代下单日；本页只读，不改 PI、物料号或价格。</p>
    </div> : null}
  </section>;
}
