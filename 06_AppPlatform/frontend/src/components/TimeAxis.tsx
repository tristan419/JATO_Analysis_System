import { useCallback, useMemo, useState } from "react";

export interface TimeRange {
  start: string;
  end: string;
}

interface Props {
  /** All available time labels in order, e.g. ["2019","2020",...] or ["2022 Jan",...] */
  labels: string[];
  /** Current selected range */
  value: TimeRange | null;
  onChange: (range: TimeRange | null) => void;
  grain: "year" | "month";
  onGrainChange: (g: "year" | "month") => void;
  showTitle?: boolean;
  /** Month-tab sub-grain: month/quarter/year */
  monthGrain?: "month" | "quarter" | "year";
  onMonthGrainChange?: (g: "month" | "quarter" | "year") => void;
}

const MONTH_INDEX: Record<string, number> = {
  Jan: 1, Feb: 2, Mar: 3, Apr: 4, May: 5, Jun: 6,
  Jul: 7, Aug: 8, Sep: 9, Oct: 10, Nov: 11, Dec: 12,
};

function toTimeOrdinal(label: string): number | null {
  const text = label.trim();
  if (/^\d{4}$/.test(text)) return Number(text) * 100 + 12;
  const shortYearMatch = text.match(/^(\d{2})[.\/-](\d{1,2})$/);
  if (shortYearMatch) return (2000 + Number(shortYearMatch[1])) * 100 + Number(shortYearMatch[2]);
  const monthNameMatch = text.match(/^(\d{4})\s+([A-Za-z]{3})$/);
  if (monthNameMatch) return Number(monthNameMatch[1]) * 100 + (MONTH_INDEX[monthNameMatch[2]] ?? 1);
  const numericMatch = text.match(/^(\d{4})[-\/.](\d{1,2})$/);
  if (numericMatch) return Number(numericMatch[1]) * 100 + Number(numericMatch[2]);
  const quarterMatch = text.match(/^(\d{4})-Q([1-4])$/);
  if (quarterMatch) return Number(quarterMatch[1]) * 100 + Number(quarterMatch[2]) * 3;
  return null;
}

export function TimeAxis({
  labels, value, onChange, grain, onGrainChange, showTitle = true, monthGrain, onMonthGrainChange,
}: Props) {
  const [mode, setMode] = useState<"slider" | "calendar">("slider");

  const startIdx = useMemo(() => {
    if (!value || labels.length === 0) return 0;
    const i = labels.indexOf(value.start);
    return i >= 0 ? i : 0;
  }, [labels, value]);

  const endIdx = useMemo(() => {
    if (!value || labels.length === 0) return labels.length - 1;
    const i = labels.indexOf(value.end);
    return i >= 0 ? i : labels.length - 1;
  }, [labels, value]);

  const handleStartChange = useCallback((idx: number) => {
    const s = Math.max(0, Math.min(idx, labels.length - 1));
    const e = Math.max(s, endIdx);
    onChange({ start: labels[s], end: labels[e] });
  }, [labels, endIdx, onChange]);

  const handleEndChange = useCallback((idx: number) => {
    const e = Math.min(labels.length - 1, Math.max(idx, 0));
    const s = Math.min(startIdx, e);
    onChange({ start: labels[s], end: labels[e] });
  }, [labels, startIdx, onChange]);

  const handleSliderChange = useCallback((which: "start" | "end", rawVal: string) => {
    const idx = Number(rawVal);
    if (which === "start") handleStartChange(idx);
    else handleEndChange(idx);
  }, [handleStartChange, handleEndChange]);

  const [calStart, setCalStart] = useState("");
  const [calEnd, setCalEnd] = useState("");

  const applyCalendar = useCallback(() => {
    if (!calStart && !calEnd) { onChange(null); return; }
    const s = calStart || labels[0];
    const e = calEnd || labels[labels.length - 1];
    const sOrd = toTimeOrdinal(s);
    const eOrd = toTimeOrdinal(e);
    const parsedLabels = labels.map(l => ({ label: l, ord: toTimeOrdinal(l) }));
    const si = parsedLabels.findIndex(item => sOrd === null || item.ord === null ? item.label >= s : item.ord >= sOrd);
    const reversed = [...parsedLabels].reverse();
    const revIdx = reversed.findIndex(item => eOrd === null || item.ord === null ? item.label <= e : item.ord <= eOrd);
    const ei = revIdx >= 0 ? labels.length - 1 - revIdx : -1;
    if (si >= 0 && ei >= 0 && si <= ei) {
      onChange({ start: labels[si], end: labels[ei] });
    }
  }, [calStart, calEnd, labels, onChange]);

  const resetRange = useCallback(() => {
    onChange(null);
    setCalStart(""); setCalEnd("");
  }, [onChange]);

  if (labels.length === 0) return null;

  return (
    <div className="time-axis">
      <div className="time-axis-header">
        {showTitle && <span className="time-axis-title">🕐 全局时间轴</span>}
        <div className="tab-bar">
          <button className={"tab-btn" + (grain === "year" ? " active" : "")} onClick={() => onGrainChange("year")}>年度</button>
          <button className={"tab-btn" + (grain === "month" ? " active" : "")} onClick={() => onGrainChange("month")}>月度</button>
        </div>
        {grain === "month" && onMonthGrainChange && (
          <div className="tab-bar" style={{ marginLeft: 12 }}>
            {(["month", "quarter", "year"] as const).map(g => (
              <button key={g} className={"tab-btn tab-btn-sm" + (monthGrain === g ? " active" : "")} onClick={() => onMonthGrainChange(g)}>
                {{ month: "月", quarter: "季", year: "年" }[g]}
              </button>
            ))}
          </div>
        )}
        <div className="tab-bar" style={{ marginLeft: 12 }}>
          <button className={"tab-btn tab-btn-sm" + (mode === "slider" ? " active" : "")} onClick={() => setMode("slider")}>滑动条</button>
          <button className={"tab-btn tab-btn-sm" + (mode === "calendar" ? " active" : "")} onClick={() => setMode("calendar")}>日历输入</button>
        </div>
        <button className="btn btn-sm btn-secondary" style={{ marginLeft: "auto" }} onClick={resetRange}>重置时间</button>
      </div>

      {mode === "slider" && (
        <div className="time-axis-slider">
          {/* B2: dual-endpoint range slider */}
          <div className="time-axis-dual-slider">
            <div className="dual-slider-track">
              <div className="dual-slider-highlight" style={{
                left: `${labels.length > 1 ? (startIdx / (labels.length - 1)) * 100 : 0}%`,
                width: `${labels.length > 1 ? ((endIdx - startIdx) / (labels.length - 1)) * 100 : 100}%`,
              }} />
            </div>
            <input type="range" className="dual-slider-input dual-slider-start" min={0} max={labels.length - 1} value={startIdx}
              onChange={e => handleSliderChange("start", e.target.value)} />
            <input type="range" className="dual-slider-input dual-slider-end" min={0} max={labels.length - 1} value={endIdx}
              onChange={e => handleSliderChange("end", e.target.value)} />
          </div>
          <div className="time-axis-range-display">
            选择范围：<strong>{labels[startIdx]}</strong> — <strong>{labels[endIdx]}</strong>
            （共 {endIdx - startIdx + 1} 期）
          </div>
        </div>
      )}

      {mode === "calendar" && (
        <div className="time-axis-calendar">
          <div className="filter-group">
            <label>起始</label>
            <input type="text" placeholder={labels[0]} value={calStart}
              onChange={e => setCalStart(e.target.value)} style={{ width: 120 }} />
          </div>
          <div className="filter-group">
            <label>结束</label>
            <input type="text" placeholder={labels[labels.length - 1]} value={calEnd}
              onChange={e => setCalEnd(e.target.value)} style={{ width: 120 }} />
          </div>
          <button className="btn btn-sm btn-primary" onClick={applyCalendar}>应用</button>
        </div>
      )}
    </div>
  );
}
