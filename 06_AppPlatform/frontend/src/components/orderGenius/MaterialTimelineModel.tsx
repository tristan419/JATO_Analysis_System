import type { CSSProperties } from "react";
import type { MaterialTimelineNode, MaterialTimelineResponse, WvtaReferenceRecord } from "../../types/orderGenius";
import { orderGeniusPowertrainColor } from "../../utils/orderGeniusBomAdmin";

type Link = MaterialTimelineResponse["links"][number];
interface Props {
  country: string;
  brand: string;
  model: string;
  nodes: MaterialTimelineNode[];
  response: MaterialTimelineResponse;
  left: number;
  right: number;
  zoom: number;
  open: boolean;
  showWvta: boolean;
  onToggle: () => void;
  onSelect: (node: MaterialTimelineNode, certificate?: WvtaReferenceRecord, relationship?: Link) => void;
}
const dayNumber = (value: string): number => Date.parse(`${value}T00:00:00Z`) / 86400000;

function productChains(nodes: MaterialTimelineNode[], all: MaterialTimelineNode[], links: Link[]): MaterialTimelineNode[][] {
  const byCode = new Map(all.map((node) => [node.bomTemplate, node]));
  const seen = new Set<string>();
  const chains: MaterialTimelineNode[][] = [];
  for (const seed of nodes) {
    let head = seed;
    const visited = new Set<string>();
    while (!visited.has(head.bomTemplate)) {
      visited.add(head.bomTemplate);
      const parent = links.find((link) => link.to === head.bomTemplate);
      const previous = parent ? byCode.get(parent.from) : undefined;
      if (!previous) break;
      head = previous;
    }
    if (seen.has(head.bomTemplate)) continue;
    const chain: MaterialTimelineNode[] = [];
    let current: MaterialTimelineNode | undefined = head;
    while (current && !seen.has(current.bomTemplate)) {
      seen.add(current.bomTemplate); chain.push(current);
      const outgoing: Link[] = links.filter((link) => link.from === current?.bomTemplate);
      current = outgoing.length === 1 ? byCode.get(outgoing[0].to) : undefined;
    }
    chains.push(chain);
  }
  return chains;
}

function intervalRows(nodes: MaterialTimelineNode[]): Map<string, number> {
  const occupied: Array<Array<[number, number]>> = [];
  const rows = new Map<string, number>();
  for (const node of nodes) {
    const known = node.effectiveFrom ?? node.effectiveTo;
    if (!known) continue;
    const first = dayNumber(node.effectiveFrom ?? known), last = dayNumber(node.effectiveTo ?? known);
    let row = occupied.findIndex((ranges) => ranges.every(([start, end]) => last < start || first > end));
    if (row < 0) { row = occupied.length; occupied.push([]); }
    occupied[row].push([first, last]); rows.set(node.bomTemplate, row);
  }
  return rows;
}

export function MaterialTimelineModel({ country, brand, model, nodes, response, left, right, zoom, open, showWvta, onToggle, onSelect }: Props) {
  // Base identity includes brand; never join a colliding code from another brand.
  const all = response.nodes.filter((node) => node.brand === brand);
  const templates = new Set(all.map((node) => node.bomTemplate));
  const links = response.links.filter((link) => templates.has(link.from) && templates.has(link.to));
  const chains = productChains(nodes, all, links);
  const linked = chains.filter((chain) => chain.length > 1);
  const pending = chains.filter((chain) => chain.length === 1).flat();
  const position = (date: string): number => Math.max(0, Math.min(100, 100 * (dayNumber(date) - left) / Math.max(1, right - left)));
  const color = (node: MaterialTimelineNode): CSSProperties => ({ "--og-timeline-powertrain": orderGeniusPowertrainColor(node.powertrain) } as CSSProperties);
  return <details className="og-timeline-model" open={open}>
    <summary onClick={(event) => { event.preventDefault(); onToggle(); }}>
      {country} · {brand} · {model} · {linked.length} chains / 产品链 · {pending.length} unlinked / 待关联
    </summary>
    {linked.length > 0 ? <div className="og-lineage-scroll">
      <div className="og-lineage-map" style={{ minWidth: `${zoom * 800}px` }}>
        <div className="og-lineage-axis"><span>BOM order dates / 下单日期</span><div>{[0, 25, 50, 75, 100].map((percent) => <span key={percent} style={{ left: `${percent}%` }}>
          {new Date((left + (right - left) * percent / 100) * 86400000).toISOString().slice(0, 10)}
        </span>)}</div></div>
        <div className="og-lineage-tree">
          <div className="og-lineage-root"><strong>{model}</strong><small>{linked.length} product chains / 产品链</small></div>
          <div className="og-lineage-branches">{linked.map((chain) => {
            const rows = intervalRows(chain);
            const height = Math.max(1, ...[...rows.values()].map((row) => row + 1)) * 84;
            const incomplete = chain.filter((node) => !node.effectiveFrom || !node.effectiveTo);
            return <div className="og-timeline-chain og-lineage-branch" key={chain[0].bomTemplate} aria-label="Linked product timeline / 已关联产品时间轴">
              <div className="og-lineage-branch-label">{[...new Set(chain.map((node) => node.version))].join(" → ")}</div>
              <div className="og-timeline-relationship">Saved relation / 已保存关联：{chain.map((node) => node.bomTemplate).join(" → ")}</div>
              {rows.size > 0 ? <div className="og-lineage-track" style={{ height }}>
                <i className="og-timeline-today" style={{ left: `${position(response.today)}%` }} aria-hidden="true" />
                <svg className="og-lineage-links" viewBox={`0 0 100 ${height}`} preserveAspectRatio="none" aria-hidden="true">
                  {chain.slice(0, -1).map((source, index) => {
                    const target = chain[index + 1];
                    if (!source.effectiveTo || !target.effectiveFrom || !rows.has(source.bomTemplate) || !rows.has(target.bomTemplate)) return null;
                    const x1 = position(source.effectiveTo), x2 = position(target.effectiveFrom);
                    const y1 = (rows.get(source.bomTemplate) ?? 0) * 84 + 62, y2 = (rows.get(target.bomTemplate) ?? 0) * 84 + 44;
                    return <path key={source.bomTemplate} d={`M ${x1} ${y1} C ${x1} ${y1 + 24}, ${x2} ${y2 + 24}, ${x2} ${y2}`} />;
                  })}
                </svg>
                {chain.map((node) => node.effectiveFrom || node.effectiveTo ? <div key={node.bomTemplate} className={`og-lineage-version is-${node.lifecycleStatus}`}
                  style={{ ...color(node), top: (rows.get(node.bomTemplate) ?? 0) * 84, left: `${position(node.effectiveFrom ?? node.effectiveTo ?? response.today)}%`, width: `${node.effectiveFrom && node.effectiveTo ? Math.max(.25, position(node.effectiveTo) - position(node.effectiveFrom)) : 0}%` }}>
                  {node.effectiveFrom && node.effectiveTo ? <button type="button" className="og-lineage-period" title={`${node.bomTemplate} · ${node.effectiveFrom} → ${node.effectiveTo}`} onClick={() => onSelect(node)}>{node.bomTemplate}</button> : null}
                  {node.effectiveFrom ? <button type="button" className="og-lineage-pin is-start" aria-label={`${node.bomTemplate} starts ${node.effectiveFrom}`} title={`First order date / 开始下单 ${node.effectiveFrom}`} onClick={() => onSelect(node)} /> : null}
                  {node.effectiveTo ? <button type="button" className="og-lineage-pin is-end" aria-label={`${node.bomTemplate} ends ${node.effectiveTo}`} title={`Final order date / 最后下单 ${node.effectiveTo}`} onClick={() => onSelect(node)} /> : null}
                </div> : null)}
              </div> : null}
              <div className="og-lineage-transitions">{chain.slice(0, -1).map((node, index) => {
                const target = chain[index + 1];
                const link = links.find((value) => value.from === node.bomTemplate && value.to === target.bomTemplate);
                return <button type="button" className="btn btn-sm btn-ghost" key={node.bomTemplate} onClick={() => onSelect(node, undefined, link)}>
                  {node.bomTemplate} → {target.bomTemplate}{link?.note ? " · Note / 备注" : ""}
                </button>;
              })}</div>
              {incomplete.length > 0 ? <div className="og-lineage-undated"><span>Dates not maintained / 日期待维护：</span>{incomplete.map((node) => <button type="button" className="btn btn-sm btn-ghost" key={node.bomTemplate} onClick={() => onSelect(node)}>
                {node.bomTemplate} · {node.effectiveFrom ?? "Start not set / 起始未设"} → {node.effectiveTo ?? "End not set / 截止未设"}
              </button>)}</div> : null}
              {showWvta && response.wvtaReference?.status === "ready" ? <div className="og-lineage-wvta">{chain.map((node) => <div key={node.bomTemplate} style={color(node)}>
                <small>{node.bomTemplate} · WVTA reference / 参考</small>
                {response.wvtaReference?.records.filter((record) => record.materialGroup === node.bomTemplate).map((record, index) => <button type="button"
                  className={`og-wvta-segment is-${record.sourceStatus}`} key={`${record.pageNumber}-${record.tableRowNumber}-${index}`} onClick={() => onSelect(node, record)}>
                  {record.sourceStatus === "recorded" && record.validFrom ? <i className={record.validTo ? "" : "is-open"} style={{ left: `${position(record.validFrom)}%`, width: `${Math.max(.25, (record.validTo ? position(record.validTo) : 100) - position(record.validFrom))}%` }} /> : null}
                  <span>{record.wvtaNo} · {record.validFrom ?? "Start not recorded / 起始未记录"} → {record.validTo ?? "End not recorded / 截止未记录"}{record.sourceStatus !== "recorded" ? ` · ${record.sourceStatus}` : ""}</span>
                </button>)}
                {!response.wvtaReference?.records.some((record) => record.materialGroup === node.bomTemplate) ? <span>No exact certificate match / 无精确证书记录，不猜测日期。</span> : null}
              </div>)}</div> : null}
            </div>;
          })}</div>
        </div>
      </div>
    </div> : <p>No saved product chain / 尚无已保存产品链，请先维护明确关系。</p>}
    {pending.length > 0 ? <details className="og-lineage-pending">
      <summary>Unlinked materials / 待确认关联 · {pending.length}</summary>
      <p>Relation not maintained / 关联未维护；同车型、配置或相邻日期不代表同一升级链。</p>
      <div className="og-lineage-pending-list">{pending.map((node) => <div key={node.bomTemplate}>
        <button type="button" className="og-timeline-node" aria-label={`${country} · ${node.bomTemplate} · ${node.version} · ${node.powertrain} · ${node.lifecycleStatus}`} style={{ borderLeftColor: orderGeniusPowertrainColor(node.powertrain) }} onClick={() => onSelect(node)}>
          <strong>{node.bomTemplate}</strong><span>{node.version} · {node.powertrain} · {node.materialCodes.length} SKUs</span>
          <span>{node.effectiveFrom ?? "Start not set / 起始未设"} → {node.effectiveTo ?? "End not set / 截止未设"}</span>
          {!node.effectiveFrom && !node.effectiveTo ? <span>Dates not maintained / 日期待维护</span> : null}
        </button>
        {showWvta && response.wvtaReference?.status === "ready" ? <div>{response.wvtaReference.records.filter((record) => record.materialGroup === node.bomTemplate).map((record, index) => <button type="button" className={`og-wvta-segment is-${record.sourceStatus}`} style={color(node)}
          key={`${record.pageNumber}-${record.tableRowNumber}-${index}`} onClick={() => onSelect(node, record)}>
          {record.sourceStatus === "recorded" && record.validFrom ? <i className={record.validTo ? "" : "is-open"} style={{ left: `${position(record.validFrom)}%`, width: `${Math.max(.25, (record.validTo ? position(record.validTo) : 100) - position(record.validFrom))}%` }} /> : null}
          <span>WVTA · {record.wvtaNo} · {record.validFrom ?? "Start not recorded / 起始未记录"} → {record.validTo ?? "End not recorded / 截止未记录"}{record.sourceStatus !== "recorded" ? ` · ${record.sourceStatus}` : ""}</span>
        </button>)}</div> : null}
      </div>)}</div>
    </details> : null}
  </details>;
}
