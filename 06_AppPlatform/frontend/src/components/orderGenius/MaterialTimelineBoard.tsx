import { useEffect, useState } from "react";
import { api } from "../../api/client";
import type { MaterialSkuMatrixRow, MaterialTimelineNode, MaterialTimelineResponse, WvtaReferenceRecord } from "../../types/orderGenius";
import { compareProductModels } from "../../utils/orderGeniusProductSort";
import "./MaterialTimelineBoard.css";
import { WvtaReferenceUpload } from "./WvtaReferenceUpload";
import { MaterialReplacementEditor } from "./MaterialReplacementEditor";
import { MaterialNotices } from "./MaterialNotices";
import { MaterialTimelineModel } from "./MaterialTimelineModel";

interface Props {
  countries: string[];
  noticeCountries?: string[];
  authorizationKey: string;
  selectionRows: Array<MaterialSkuMatrixRow & { _countryCode?: string }>;
  selectionComplete: boolean;
  bomEditing?: boolean;
  canManageReference?: boolean;
  canMaintainRelations?: boolean;
  onViewModel: (brand: string, model: string, country?: string) => void;
}
type LoadState = { scope: string; status: "loading" | "error" | "ready"; data: MaterialTimelineResponse[] };
type NodeRef = { country: string; node: MaterialTimelineNode; certificate?: WvtaReferenceRecord; relationship?: MaterialTimelineResponse["links"][number] };
interface ModelGroup { key: string; country: string; brand: string; model: string; nodes: MaterialTimelineNode[]; events: MaterialTimelineResponse["events"] }
const STATUS: Record<string, string> = {
  active: "Active / 生效", phase_out: "Phase out / 即将结束",
  not_yet_active: "Not yet active / 尚未生效", historical: "Historical / 历史", review: "Review / 待核对",
};
const dayNumber = (day: string): number => Date.parse(`${day}T00:00:00Z`) / 86400000;

export function MaterialTimelineBoard({ countries, noticeCountries = countries, authorizationKey, selectionRows, selectionComplete, bomEditing = false, canManageReference = false, canMaintainRelations = false, onViewModel }: Props) {
  const [showWvta, setShowWvta] = useState(false);
  const [zoom, setZoom] = useState(1);
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
  const selectedNode = selected ? data.find((item) => item.countryCode === selected.country)?.nodes.find(
    (node) => node.brand === selected.node.brand && node.bomTemplate === selected.node.bomTemplate) : undefined;
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
      <label>Time zoom / 时间缩放<select aria-label="Time zoom" value={zoom} onChange={(event) => setZoom(Number(event.target.value))}><option value={1}>1×</option><option value={2}>2×</option><option value={4}>4×</option></select></label>
      <label><input type="checkbox" checked={showWvta} onChange={(event) => setShowWvta(event.currentTarget.checked)} /> Show WVTA reference / 叠加WVTA参考（不改变BOM下单日期）</label>
      {canManageReference ? <WvtaReferenceUpload onSaved={() => setRevision((value) => value + 1)} /> : null}
    </div> : null}
    {!bomEditing ? <MaterialNotices countries={noticeCountries} authorizationKey={authorizationKey} revision={revision} defaultExpanded={!canMaintainRelations} onViewModel={onViewModel} /> : null}
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
          const response = data.find((value) => value.countryCode === group.country);
          if (!response) return null;
          return <MaterialTimelineModel key={group.key} country={group.country} brand={group.brand} model={group.model}
            nodes={group.nodes} response={response} left={left} right={right} zoom={zoom} showWvta={showWvta}
            open={focusModel === `${group.brand}|${group.model}` || expandedModels.includes(group.key)}
            onToggle={() => setExpandedModels((values) => values.includes(group.key) ? values.filter((key) => key !== group.key) : [...values, group.key])}
            onSelect={(node, certificate, relationship) => setSelected({ country: group.country, node, certificate, relationship })} />;
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
        {selected.relationship ? <p>Replacement / 更替：{selected.relationship.from} → {selected.relationship.to} · {selected.relationship.note || "Note not maintained / 说明未维护"}</p> : null}
        <p className="og-timeline-codes">{selectedNode.materialCodes.join(" · ")}</p>
        {canMaintainRelations && selectedNode.brand ? <MaterialReplacementEditor brand={selectedNode.brand} bomTemplate={selectedNode.bomTemplate} onSaved={() => setRevision((value) => value + 1)} /> : null}
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
      <p>Dashed line = today. Order windows are inclusive. Certificate periods are not order dates. / 虚线＝今天，下单起止日含当天，证书期不替代下单日。关联维护不改PI、物料号或价格，Order Filler只读。</p>
    </div> : null}
  </section>;
}
