import { useEffect, useRef, useState, type FormEvent } from "react";
import { api } from "../api/client";
import type { PiVehicleUnit, VehicleAllocationSearchResult } from "../types/orderGeniusVehicle";
import { orderGeniusPowertrainColor } from "../utils/orderGeniusBomAdmin";
import { matchesVehicleText, parseVehicleSearch } from "./vehicleAllocationFields";
import "./VehicleAllocationSearch.css";

interface Props {
  vehicles: PiVehicleUnit[]; piCodes: string[]; busy: boolean; applied: string;
  onApply: (query: string, vins: string[] | null) => void;
  onAdd: (pis: string[], cars: string[], query: string, vins: string[] | null) => Promise<void>;
  onSelect?: (cars: string[]) => void;
  statusLabels?: Record<string, string>;
}

export function VehicleAllocationSearch({ vehicles, piCodes, busy, applied, onApply, onAdd, onSelect, statusLabels = {} }: Props) {
  const [mode, setMode] = useState<"pis" | "vehicles">("pis");
  const [query, setQuery] = useState("");
  const [composing, setComposing] = useState(false);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<VehicleAllocationSearchResult | null>(null);
  const [chosen, setChosen] = useState<Set<string>>(new Set());
  const [active, setActive] = useState(0);
  const [rawOpen, setRawOpen] = useState(false);
  const [retry, setRetry] = useState(0);
  const root = useRef<HTMLDivElement>(null);
  const request = useRef(0);
  const abort = useRef<AbortController | null>(null);
  const parsed = parseVehicleSearch(query);
  const eligible = !composing && query.trim().length >= 2 && !parsed.error;
  const searchVehicles = mode === "vehicles" ? vehicles : null;
  const choose = (code: string): void => setChosen((previous) => {
    const next = new Set(previous); if (next.has(code)) next.delete(code); else next.add(code); return next;
  });

  async function preview(page: number, signal: AbortSignal, identity: number): Promise<void> {
    try {
      let next: VehicleAllocationSearchResult;
      if (mode === "pis") next = await api.searchVehicleAllocation(parsed.vins ? "" : query, parsed.vins ?? undefined, page, signal);
      else {
        const matches = vehicles.filter((vehicle) => parsed.vins !== null
          ? Boolean(vehicle.vin && parsed.vins.includes(vehicle.vin.toUpperCase())) : matchesVehicleText(vehicle, query, statusLabels));
        const codes = [...new Set(matches.map((vehicle) => vehicle.piCode))];
        next = { items: codes.slice((page - 1) * 8, page * 8).map((piCode) => {
          const scope = vehicles.filter((vehicle) => vehicle.piCode === piCode);
          const hits = matches.filter((vehicle) => vehicle.piCode === piCode);
          return { piCode, orderMonth: scope[0]?.orderMonth ?? null, countries: [...new Set(scope.map((v) => v.countryCode))],
            powertrains: [...new Set(scope.flatMap((v) => v.powertrain ? [v.powertrain] : []))],
            matched: hits.length, vehicleTotal: scope.length, carCodes: hits.map((v) => v.carCode) };
        }), total: codes.length, matched: matches.length, page, pageSize: 8,
        missingVins: parsed.vins?.filter((vin) => !matches.some((vehicle) => vehicle.vin?.toUpperCase() === vin)) ?? [] };
      }
      if (signal.aborted || identity !== request.current) return;
      setResult((previous) => page > 1 && previous ? { ...next, items: [...previous.items, ...next.items] } : next);
    } catch (reason: unknown) {
      if (!signal.aborted && identity === request.current) setError(reason instanceof Error ? reason.message : "Search failed / 查询失败，请重试");
    } finally { if (identity === request.current && !signal.aborted) setLoading(false); }
  }

  useEffect(() => {
    const identity = ++request.current;
    abort.current?.abort(); setResult(null); setChosen(new Set()); setError(""); setActive(0);
    if (!eligible) { setLoading(false); return; }
    const controller = new AbortController(); abort.current = controller; setLoading(true);
    const timer = window.setTimeout(() => { void preview(1, controller.signal, identity); }, 100);
    return () => { window.clearTimeout(timer); controller.abort(); };
  }, [query, mode, composing, searchVehicles, retry]);

  useEffect(() => {
    const close = (event: PointerEvent): void => { if (event.target instanceof Node && !root.current?.contains(event.target)) setOpen(false); };
    document.addEventListener("pointerdown", close);
    return () => { document.removeEventListener("pointerdown", close); abort.current?.abort(); request.current += 1; };
  }, []);

  function edit(value: string): void {
    // Invalidate synchronously, before an old promise or click can use the preceding result.
    request.current += 1; abort.current?.abort(); setResult(null); setChosen(new Set());
    setQuery(value); setOpen(true);
    if (!value) { setOpen(false); setRawOpen(false); onApply("", null); }
  }

  async function add(select: boolean): Promise<void> {
    if (!result || loading || busy) return;
    const items = result.items.filter((item) => chosen.has(item.piCode));
    if (!items.length) return;
    try {
      await onAdd(items.map((item) => item.piCode), select ? items.flatMap((item) => item.carCodes) : [], query, parsed.vins);
      setOpen(false);
    } catch (reason: unknown) { setError(reason instanceof Error ? reason.message : "Could not load PIs / 读取失败，请重试"); }
  }

  function submit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault(); setOpen(true);
    if (error) { setRetry((value) => value + 1); return; }
    if (mode === "vehicles" && eligible && !loading && result) { onApply(query, parsed.vins); setOpen(false); }
  }
  const selectedItems = result?.items.filter((item) => chosen.has(item.piCode)) ?? [];
  const newCount = selectedItems.filter((item) => !piCodes.includes(item.piCode)).length;
  const cars = selectedItems.reduce((sum, item) => sum + item.matched, 0);
  return <div className="va-search-container" ref={root}>
    <form className="va-search" onSubmit={submit}>
      <div className="va-search-control">
        <select aria-label="Search scope / 搜索范围" value={mode} onChange={(event) => {
          request.current += 1; abort.current?.abort(); setResult(null); setMode(event.target.value === "pis" ? "pis" : "vehicles"); setOpen(true);
        }}><option value="pis">Find PIs / 找 PI</option><option value="vehicles">Current vehicles / 筛当前车辆</option></select>
        <textarea aria-label="Search PI, VIN, material / 搜索" role="combobox" aria-expanded={Boolean(open && result?.items.length)}
          aria-controls="va-search-results" aria-activedescendant={open && !loading && result?.items[active] ? `va-search-result-${active}` : undefined}
          rows={rawOpen ? 4 : 1} readOnly={!rawOpen && parsed.vins !== null} style={rawOpen ? { height: "100px" } : undefined}
          value={!rawOpen && parsed.vins !== null ? `${parsed.vins.length} VINs` : query}
          placeholder="CH JAECOO5 BEV; SE 202608…" onChange={(event) => edit(event.target.value)}
          onPaste={(event) => { const text = event.clipboardData.getData("text"); if (/[\n\r\t]/.test(text)) { event.preventDefault(); edit(text); } }}
          onFocus={() => setOpen(true)} onCompositionStart={() => setComposing(true)} onCompositionEnd={() => setComposing(false)}
          onKeyDown={(event) => {
            if (event.nativeEvent.isComposing) return;
            if (event.key === "Escape") { event.preventDefault(); setOpen(false); }
            if ((event.key === "ArrowDown" || event.key === "ArrowUp") && result?.items.length) {
              event.preventDefault(); setOpen(true); setActive((index) => (index + (event.key === "ArrowDown" ? 1 : -1) + result.items.length) % result.items.length);
            }
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault(); if (loading || busy || !eligible) return;
              if (mode === "vehicles" && result) { onApply(query, parsed.vins); setOpen(false); }
              else if (open && result?.items[active]) choose(result.items[active].piCode);
              else setOpen(true);
            }
          }} />
        {query && <button type="button" className="btn-ghost" aria-label="Clear search / 清空搜索" onClick={() => edit("")}>×</button>}
      </div>
      <button type="submit" disabled={busy || !eligible || loading}>Search</button>
    </form>
    <div className="va-search-feedback" role="status">{loading ? "Searching… / 查询中" : parsed.error || error || (eligible && result && !result.total ? "No matches / 无匹配结果" : "")}</div>
    {applied && <button className="btn-secondary" type="button" onClick={() => edit("")}>Current search / 当前搜索：{parseVehicleSearch(applied).vins ? `${parseVehicleSearch(applied).vins?.length} VINs` : applied} ×</button>}
    {parsed.vins !== null && <div className="va-search-batch">
      <button className="btn-ghost" type="button" onClick={() => setRawOpen(!rawOpen)}>{rawOpen ? "Collapse / 收起" : "Review VINs / 核对原文"}</button>
      <span>{parsed.duplicates} duplicates / 重复 · {parsed.invalid.length} invalid / 格式错误</span>
      {(parsed.invalid.length > 0 || (result?.missingVins.length ?? 0) > 0) && <details><summary>Exceptions / 异常项</summary><textarea readOnly aria-label="VIN exceptions / VIN异常项" value={[...parsed.invalid, ...(result?.missingVins ?? [])].join("\n")} /></details>}
    </div>}
    {open && !loading && !error && !parsed.error && result && result.items.length > 0 && <section className="va-search-preview" aria-label="Search preview / 搜索预览">
      <p>{mode === "pis" ? "All authorized PIs / 全部授权PI" : `${piCodes.length} selected PIs / 已选PI`} · {result.matched} matches / 台 · {result.total} PIs
        {parsed.vins !== null && ` · ${result.missingVins.length} unmatched or inaccessible / 未匹配或无权访问`}</p>
      <div id="va-search-results" role="listbox" aria-label="PI results / PI结果" aria-multiselectable="true">{result.items.map((item, index) => <button type="button" role="option" id={`va-search-result-${index}`} key={item.piCode}
        aria-selected={chosen.has(item.piCode)} className={`va-search-result${active === index ? " is-focused" : ""}`}
        style={{ borderLeftColor: item.powertrains.length === 1 ? orderGeniusPowertrainColor(item.powertrains[0]) : "#94a3b8" }}
        onClick={() => { setActive(index); choose(item.piCode); }}>
        <strong>{chosen.has(item.piCode) ? "☑ " : "☐ "}{item.piCode}</strong><span>{item.countries.join(" / ")} · {item.orderMonth} · {item.matched} / {item.vehicleTotal} vehicles</span>
      </button>)}</div>
      {result.items.length < result.total && <button type="button" className="btn-secondary" onClick={() => {
        const controller = new AbortController(); abort.current = controller; setLoading(true);
        void preview(result.page + 1, controller.signal, ++request.current);
      }}>Show more / 查看更多（{result.items.length}/{result.total}）</button>}
      <div className="va-button-row">
        {mode === "vehicles" && <button type="button" disabled={busy} onClick={() => { onApply(query, parsed.vins); setOpen(false); }}>Apply filter / 应用筛选</button>}
        {mode === "pis" ? <>
          <button type="button" disabled={busy || !chosen.size} onClick={() => void add(false)}>Add {newCount} PIs / 加入工作区</button>
          <button type="button" disabled={busy || !chosen.size} onClick={() => void add(true)}>Add {newCount} PIs and select {cars} vehicles / 加入PI并勾选车辆</button>
        </> : <button type="button" disabled={busy || !chosen.size || !onSelect} onClick={() => {
          onSelect?.(selectedItems.flatMap((item) => item.carCodes)); setOpen(false);
        }}>Select {cars} matched vehicles / 勾选匹配车辆</button>}
      </div>
    </section>}
  </div>;
}
