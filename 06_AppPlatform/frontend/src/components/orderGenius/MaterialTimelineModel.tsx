import type { CSSProperties } from "react";
import type { MaterialTimelineNode, MaterialTimelineResponse, WvtaReferenceRecord } from "../../types/orderGenius";
import { orderGeniusPowertrainColor } from "../../utils/orderGeniusBomAdmin";

type Link = MaterialTimelineResponse["links"][number];
interface Props {
  country: string; brand: string; model: string; nodes: MaterialTimelineNode[];
  response: MaterialTimelineResponse; left: number; right: number; zoom: number; open: boolean; showWvta: boolean;
  onToggle: () => void;
  onSelect: (node: MaterialTimelineNode, certificate?: WvtaReferenceRecord, relationship?: Link) => void;
}
const dayNumber = (value: string): number => Date.parse(`${value}T00:00:00Z`) / 86400000;

export function MaterialTimelineModel({ country, brand, model, nodes, response, left, right, zoom, open, showWvta, onToggle, onSelect }: Props) {
  const members = nodes.filter((node) => node.brand === brand && node.modelName === model)
    .slice().sort((a, b) => a.version.localeCompare(b.version) || (a.effectiveFrom ?? "9999").localeCompare(b.effectiveFrom ?? "9999") || a.bomTemplate.localeCompare(b.bomTemplate));
  const byCode = new Map(members.map((node) => [node.bomTemplate, node]));
  const links = response.links.filter((link) => byCode.has(link.from) && byCode.has(link.to));
  const certificates = (node: MaterialTimelineNode): WvtaReferenceRecord[] => showWvta
    ? (response.wvtaReference?.records ?? []).filter((record) => record.materialGroup === node.bomTemplate) : [];
  let height = 0, trim = "";
  const layout = members.map((node) => {
    const heading = node.version !== trim;
    trim = node.version;
    if (heading) height += 32;
    const top = height;
    height += 84 + certificates(node).length * 24;
    return { node, top, heading };
  });
  const positions = new Map(layout.map(({ node, top }) => [node.bomTemplate, top]));
  const position = (value: string): number => Math.max(0, Math.min(100, 100 * (dayNumber(value) - left) / Math.max(1, right - left)));
  const color = (node: MaterialTimelineNode): CSSProperties => ({ "--og-timeline-powertrain": orderGeniusPowertrainColor(node.powertrain) } as CSSProperties);
  return <details className="og-timeline-model" open={open} data-model={model}>
    <summary onClick={(event) => { event.preventDefault(); onToggle(); }}>{country} · {brand} · {model} · {members.length} bases / 物料泳道 · {new Set(members.map((n) => n.version)).size} trims / 配置</summary>
    <div className="og-lineage-scroll"><div className="og-swimlane-map" style={{ minWidth: `${zoom * 850}px` }}>
      <div className="og-swimlane-axis"><strong>{model} · Trim / Base</strong><div>{[0, 25, 50, 75, 100].map((percent) => <span key={percent} style={{ left: `${percent}%` }}>{new Date((left + (right - left) * percent / 100) * 86400000).toISOString().slice(0, 10)}</span>)}</div></div>
      <div className="og-swimlane-body" style={{ height }}>
        <div className="og-swimlane-labels">{layout.map(({ node, top, heading }) => <div key={node.bomTemplate}>
          {heading ? <strong className="og-swimlane-trim" style={{ top: top - 32 }}>{node.version || "Trim not set / 配置未维护"}</strong> : null}
          <button type="button" className="og-swimlane-label" aria-label={`${node.bomTemplate} · ${node.version} · ${node.powertrain} · ${node.lifecycleStatus}`} style={{ top, borderLeftColor: orderGeniusPowertrainColor(node.powertrain) }} onClick={() => onSelect(node)}>
            <strong>{node.bomTemplate}</strong><span>{node.powertrain} · {node.lifecycleStatus} · {node.materialCodes.length} SKUs</span>
            {!node.effectiveFrom && !node.effectiveTo ? <small>Dates not maintained / 日期待维护</small> : null}
            {!response.links.some((link) => link.from === node.bomTemplate || link.to === node.bomTemplate) ? <small>Relation not maintained / 关联未维护</small> : null}
            {response.links.some((link) => (link.from === node.bomTemplate && !byCode.has(link.to)) || (link.to === node.bomTemplate && !byCode.has(link.from))) ? <small>External relation / 外部关联待核对</small> : null}
          </button>
        </div>)}</div>
        <div className="og-swimlane-tracks">
          <i className="og-timeline-today" style={{ left: `${position(response.today)}%` }} aria-hidden="true" />
          <svg className="og-lineage-links" viewBox={`0 0 100 ${Math.max(1, height)}`} preserveAspectRatio="none" aria-hidden="true">
            {links.map((link) => {
              const source = byCode.get(link.from), target = byCode.get(link.to);
              if (!source?.effectiveTo || !target?.effectiveFrom) return null;
              const x1 = position(source.effectiveTo), x2 = position(target.effectiveFrom);
              const y1 = (positions.get(link.from) ?? 0) + 26, y2 = (positions.get(link.to) ?? 0) + 26;
              return <path key={link.from} d={`M ${x1} ${y1} C ${x1} ${y1 + 35}, ${x2} ${y2 - 35}, ${x2} ${y2}`} />;
            })}
          </svg>
          {layout.map(({ node, top }) => {
            const outgoing = links.find((link) => link.from === node.bomTemplate);
            const start = node.effectiveFrom, end = node.effectiveTo;
            return <div key={node.bomTemplate} className="og-swimlane-track" style={{ top, ...color(node) }} aria-label={`${node.bomTemplate} swimlane / 物料泳道`}>
              <div className="og-swimlane-baseline" />
              {start && end ? <button type="button" className={`og-swimlane-period is-${node.lifecycleStatus}`} style={{ left: `${position(start)}%`, width: `${Math.max(.3, position(end) - position(start))}%` }}
                title={`${node.bomTemplate} · ${start} → ${end}`} onClick={() => onSelect(node)} /> : null}
              {start ? <button type="button" className="og-swimlane-pin" style={{ left: `${position(start)}%` }} aria-label={`${node.bomTemplate} starts ${start}`} onClick={() => onSelect(node)} /> : null}
              {end ? <button type="button" className="og-swimlane-pin" style={{ left: `${position(end)}%` }} aria-label={`${node.bomTemplate} ends ${end}`} onClick={() => onSelect(node)} /> : null}
              <button type="button" className="og-swimlane-dates" onClick={() => onSelect(node)}>{start ?? "Start not set / 起始未设"} → {end ?? "End not set / 截止未设"}</button>
              {outgoing ? <button type="button" className="og-swimlane-relation" onClick={() => onSelect(node, undefined, outgoing)}>→ {outgoing.to}{outgoing.note ? " · Note / 备注" : ""}</button> : null}
              {certificates(node).map((record, index) => <button key={`${record.pageNumber}-${record.tableRowNumber}-${index}`} className="og-swimlane-wvta" style={{ top: 76 + index * 24 }} onClick={() => onSelect(node, record)}>
                {record.sourceStatus === "recorded" && record.validFrom ? <i style={{ left: `${position(record.validFrom)}%`, width: `${Math.max(.3, (record.validTo ? position(record.validTo) : 100) - position(record.validFrom))}%` }} /> : null}
                WVTA · {record.wvtaNo} · {record.validFrom ?? "Start not recorded"} → {record.validTo ?? "End not recorded"} · {record.sourceStatus}
              </button>)}
            </div>;
          })}
        </div>
      </div>
    </div></div>
  </details>;
}
