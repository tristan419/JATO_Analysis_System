import { useEffect, useRef, useState, type FormEvent } from "react";
import { api } from "../api/client";
import type { PiVehicleUnit, VehicleAllocationSearchResult } from "../types/orderGeniusVehicle";
import { parseVehicleSearch } from "./vehicleAllocationFields";
import "./VehicleAllocationSearch.css";

interface Props {
  busy: boolean;
  onApply: (query: string, vins: string[] | null) => void;
  onAdd: (pis: string[], cars: string[], query: string, vins: string[] | null, replace?: boolean) => Promise<void>;
  onPreview: (vehicles: PiVehicleUnit[] | null) => void;
  selectedCount: number;
}

export function VehicleAllocationSearch({ busy, onApply, onAdd, onPreview, selectedCount = 0 }: Props) {
  const [query, setQuery] = useState("");
  const [composing, setComposing] = useState(false);
  const [open, setOpen] = useState(false);
  const [rawOpen, setRawOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<VehicleAllocationSearchResult | null>(null);
  const [retry, setRetry] = useState(0);
  const root = useRef<HTMLDivElement>(null);
  const request = useRef(0);
  const abort = useRef<AbortController | null>(null);
  const showResults = useRef(onPreview); showResults.current = onPreview;
  const parsed = parseVehicleSearch(query);
  const eligible = !composing && query.trim().length >= 2 && !parsed.error;

  useEffect(() => {
    const identity = ++request.current;
    abort.current?.abort(); setResult(null); setError("");
    if (!eligible) { setLoading(false); return; }
    const controller = new AbortController(); abort.current = controller; setLoading(true);
    const timer = window.setTimeout(async () => {
      try {
        const next = await api.searchVehicleAllocation(parsed.vins ? "" : query, parsed.vins ?? undefined, 1, controller.signal);
        if (controller.signal.aborted || identity !== request.current) return;
        setResult(next); showResults.current(next.vehicles);
      } catch (reason: unknown) {
        if (!controller.signal.aborted && identity === request.current) setError(reason instanceof Error ? reason.message : "Search failed / 查询失败");
      } finally { if (identity === request.current && !controller.signal.aborted) setLoading(false); }
    }, 100);
    return () => { window.clearTimeout(timer); controller.abort(); };
  }, [query, composing, retry]);

  useEffect(() => {
    const close = (event: PointerEvent): void => { if (event.target instanceof Node && !root.current?.contains(event.target)) { setOpen(false); setRawOpen(false); } };
    document.addEventListener("pointerdown", close);
    return () => { document.removeEventListener("pointerdown", close); abort.current?.abort(); request.current += 1; };
  }, []);
  function edit(value: string): void {
    request.current += 1; abort.current?.abort(); setResult(null); setError("");
    if (value === query) setRetry((current) => current + 1);
    setQuery(value); setOpen(Boolean(value));
    if (!value) { setRawOpen(false); onApply("", null); showResults.current(null); }
  }
  async function select(replace: boolean): Promise<void> {
    if (!result || loading || busy) return;
    const vehicles = result.vehicles;
    const codes = vehicles.map((v) => v.carCode);
    if (codes.length !== result.matched) { setError("Read all matches before selecting / 请重新搜索完整结果"); return; }
    try {
      await onAdd([...new Set(vehicles.map((v) => v.piCode))], codes, query, parsed.vins, replace);
      setOpen(false); setRawOpen(false);
    } catch (reason: unknown) {
      setResult(null); showResults.current(null);
      setError(reason instanceof Error ? reason.message : "Could not load vehicles / 车辆读取失败");
    }
  }
  function submit(event: FormEvent<HTMLFormElement>): void { event.preventDefault(); setOpen(true); if (error) setRetry((value) => value + 1); }
  const missing = result?.missingVins ?? [];
  return <div className="va-search-container" ref={root} onKeyDown={(event) => { if (event.key === "Escape") { setOpen(false); setRawOpen(false); } }}>
    <form className="va-search" onSubmit={submit}>
      <div className="va-search-control">
        <button type="submit" className="btn-ghost va-search-icon" aria-label="Search / 搜索" disabled={busy || !eligible}><svg aria-hidden="true" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="10.5" cy="10.5" r="6.5" /><path d="m16 16 5 5" /></svg></button>
        <input disabled={busy} aria-label="Search PI, VIN, material / 搜索" role="combobox" aria-expanded={open && Boolean(result?.matched)}
          readOnly={parsed.vins !== null} value={parsed.vins !== null ? `${parsed.vins.length} VINs · 查看` : query}
          placeholder="搜索国家、车型、物料，或粘贴 VIN…" onChange={(event) => edit(event.target.value)}
          onPaste={(event) => { const text = event.clipboardData.getData("text"); if (event.currentTarget.readOnly || /[\n\r\t]/.test(text)) { event.preventDefault(); edit(text); } }}
          onClick={() => { setOpen(true); if (parsed.vins !== null) setRawOpen(true); }} onFocus={() => setOpen(true)}
          onCompositionStart={() => setComposing(true)} onCompositionEnd={() => setComposing(false)}
          onKeyDown={(event) => {
            if (event.nativeEvent.isComposing) return;
            if (event.currentTarget.readOnly && ["Backspace", "Delete"].includes(event.key)) { event.preventDefault(); edit(""); }
            if (event.key === "Escape") { setOpen(false); setRawOpen(false); }
            if (event.key === "ArrowDown") { event.preventDefault(); root.current?.querySelector<HTMLButtonElement>(".va-search-select")?.focus(); }
          }} />
        {query && <button type="button" className="btn-ghost" aria-label="Clear search / 清空搜索" onClick={() => edit("")}>×</button>}
      </div>
    </form>
    <div className="va-search-feedback" role="status">{loading ? "查询中…" : parsed.error || error || (eligible && result && !result.matched ? "No matches / 无匹配结果" : "")}</div>
    {open && (rawOpen || (!loading && !error && Boolean(result?.matched))) && <section className="va-search-preview" aria-label="Search preview / 搜索预览">
      {rawOpen && <label>VIN 原文<textarea aria-label="VIN source / VIN原文" value={query} onChange={(event) => edit(event.target.value)} /><button type="button" className="btn-ghost" onClick={() => setRawOpen(false)}>收起原文</button></label>}
      {parsed.duplicates > 0 && <small>已去重 {parsed.duplicates} 条</small>}
      {(parsed.invalid.length > 0 || missing.length > 0) && <details><summary>异常 {parsed.invalid.length + missing.length} 条（格式错误／未匹配或无权访问）</summary><textarea readOnly aria-label="VIN exceptions / VIN异常项" value={[...parsed.invalid, ...missing].join("\n")} /></details>}
      {!loading && !error && result && result.matched > 0 && <div className="va-search-summary">
        <span>匹配 {result.matched} 台 · {new Set(result.vehicles.map((v) => v.countryCode)).size} 个国家 · {result.total} 张 PI</span>
        <button type="button" className="va-search-select" disabled={busy} onClick={() => void select(true)}>勾选这 {result.matched} 台</button>
        {selectedCount > 0 && <button type="button" className="btn-secondary" disabled={busy} onClick={() => void select(false)}>追加</button>}
      </div>}
    </section>}
  </div>;
}
