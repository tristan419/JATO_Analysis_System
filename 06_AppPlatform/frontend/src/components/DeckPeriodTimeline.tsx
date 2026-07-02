import { useEffect, useMemo, useRef, useState } from "react";

import type { MarketScanCountryOption, MarketScanPeriodRange } from "../types";

interface DeckPeriodTimelineProps {
  options: MarketScanCountryOption[];
  value: MarketScanPeriodRange | null;
  onChange: (value: MarketScanPeriodRange | null) => void;
  disabled?: boolean;
  label?: string;
  commitOnIdle?: boolean;
}

const AUTO_COMMIT_DELAY_MS = 1000;

function buildRangeState(
  options: MarketScanCountryOption[],
  startIndex: number,
  endIndex: number,
) {
  const latestIndex = options.length - 1;
  const normalizedStartIndex = Math.min(startIndex, endIndex);
  const normalizedEndIndex = Math.max(startIndex, endIndex);
  const startOption = options[normalizedStartIndex];
  const endOption = options[normalizedEndIndex];
  const latestOption = options[latestIndex];
  if (!startOption || !endOption || !latestOption) {
    return null;
  }
  return {
    startIndex: normalizedStartIndex,
    endIndex: normalizedEndIndex,
    startOption,
    endOption,
    latestOption,
    isDefaultLatest: startOption.value === latestOption.value && endOption.value === latestOption.value,
    isCustomRange: startOption.value !== endOption.value,
  };
}

export function DeckPeriodTimeline({
  options,
  value,
  onChange,
  disabled = false,
  label = "Period",
  commitOnIdle = true,
}: DeckPeriodTimelineProps) {
  const [expanded, setExpanded] = useState(false);
  const [activeThumb, setActiveThumb] = useState<"start" | "end">("start");

  const committedRangeState = useMemo(() => {
    if (options.length === 0) {
      return null;
    }
    const latestIndex = options.length - 1;
    const rawStartIndex = value
      ? options.findIndex((option) => option.value === value.start)
      : latestIndex;
    const rawEndIndex = value
      ? options.findIndex((option) => option.value === value.end)
      : latestIndex;
    const normalizedStartIndex = rawStartIndex >= 0 ? rawStartIndex : latestIndex;
    const normalizedEndIndex = rawEndIndex >= 0 ? rawEndIndex : latestIndex;
    const startIndex = Math.min(normalizedStartIndex, normalizedEndIndex);
    const endIndex = Math.max(normalizedStartIndex, normalizedEndIndex);
    return buildRangeState(options, startIndex, endIndex);
  }, [options, value]);
  const [draftRange, setDraftRange] = useState<{ startIndex: number; endIndex: number } | null>(null);
  const draftRangeRef = useRef<{ startIndex: number; endIndex: number } | null>(null);

  useEffect(() => {
    if (expanded || !committedRangeState) {
      return;
    }
    const nextDraftRange = {
      startIndex: committedRangeState.startIndex,
      endIndex: committedRangeState.endIndex,
    };
    draftRangeRef.current = nextDraftRange;
    setDraftRange(nextDraftRange);
  }, [committedRangeState, expanded]);

  useEffect(() => {
    if (!expanded || !commitOnIdle) {
      return;
    }
    const latestDraftRange = draftRangeRef.current;
    if (!latestDraftRange || !committedRangeState) {
      return;
    }
    if (
      latestDraftRange.startIndex === committedRangeState.startIndex
      && latestDraftRange.endIndex === committedRangeState.endIndex
    ) {
      return;
    }
    const timeoutId = window.setTimeout(() => {
      const draftToCommit = draftRangeRef.current;
      if (!draftToCommit) {
        return;
      }
      commitRange(draftToCommit.startIndex, draftToCommit.endIndex);
    }, AUTO_COMMIT_DELAY_MS);
    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [commitOnIdle, committedRangeState, draftRange, expanded]);

  const rangeState = useMemo(() => {
    if (expanded && draftRange) {
      return buildRangeState(options, draftRange.startIndex, draftRange.endIndex);
    }
    return committedRangeState;
  }, [committedRangeState, draftRange, expanded, options]);

  const firstOption = options[0] ?? null;

  const commitRange = (nextStartIndex: number, nextEndIndex: number) => {
    const startIndex = Math.min(nextStartIndex, nextEndIndex);
    const endIndex = Math.max(nextStartIndex, nextEndIndex);
    const startOption = options[startIndex];
    const endOption = options[endIndex];
    const latestOption = options[options.length - 1];
    if (!startOption || !endOption || !latestOption) {
      return;
    }
    if (startOption.value === latestOption.value && endOption.value === latestOption.value) {
      if (!value) {
        return;
      }
      onChange(null);
      return;
    }
    if (value?.start === startOption.value && value.end === endOption.value) {
      return;
    }
    onChange({ start: startOption.value, end: endOption.value });
  };

  const updateDraftRange = (nextStartIndex: number, nextEndIndex: number) => {
    const nextDraftRange = {
      startIndex: Math.min(nextStartIndex, nextEndIndex),
      endIndex: Math.max(nextStartIndex, nextEndIndex),
    };
    draftRangeRef.current = nextDraftRange;
    setDraftRange(nextDraftRange);
  };

  const commitDraftRange = () => {
    const latestDraftRange = draftRangeRef.current;
    if (!latestDraftRange) {
      return;
    }
    commitRange(latestDraftRange.startIndex, latestDraftRange.endIndex);
  };

  return (
    <div className="market-scan-field market-scan-field--timeline">
      <span>{label}</span>
      <div className={`deck-period-timeline${disabled ? " is-disabled" : ""}`}>
        <div className="deck-period-timeline-topline">
          <button
            type="button"
            className="btn btn-sm btn-secondary deck-period-timeline-toggle"
            onClick={() => {
              if (expanded) {
                commitDraftRange();
                setExpanded(false);
                return;
              }
              if (committedRangeState) {
                const nextDraftRange = {
                  startIndex: committedRangeState.startIndex,
                  endIndex: committedRangeState.endIndex,
                };
                draftRangeRef.current = nextDraftRange;
                setDraftRange(nextDraftRange);
              }
              setExpanded(true);
            }}
            disabled={disabled || options.length === 0}
            aria-expanded={expanded}
          >
            {expanded ? "收起时间轴" : "展开时间轴"}
          </button>
          <div className="deck-period-timeline-summary">
            <strong>
              {rangeState
                ? rangeState.startOption.value === rangeState.endOption.value
                  ? rangeState.endOption.label
                  : `${rangeState.startOption.label} - ${rangeState.endOption.label}`
                : "等待加载"}
            </strong>
            <small>
              {rangeState
                ? rangeState.isCustomRange
                  ? "已切换自定义区间累计"
                  : rangeState.isDefaultLatest
                    ? "当前默认最新月当月"
                    : "已切换历史月份"
                : "等待可选月份"}
            </small>
          </div>
        </div>

        {expanded && rangeState && firstOption ? (
          <div className="time-axis-slider deck-period-timeline-panel">
            <div className="deck-period-timeline-axis">
              <div className="deck-period-timeline-track">
                <div
                  className="deck-period-timeline-track-selection"
                  style={{
                    left: `${(rangeState.startIndex / Math.max(1, options.length - 1)) * 100}%`,
                    width: `${((rangeState.endIndex - rangeState.startIndex) / Math.max(1, options.length - 1)) * 100}%`,
                  }}
                />
              </div>
              <input
                type="range"
                min={0}
                max={options.length - 1}
                value={rangeState.startIndex}
                className={`deck-period-timeline-input deck-period-timeline-input--start${activeThumb === "start" ? " is-active" : ""}${rangeState.startIndex === rangeState.endIndex ? " is-overlap" : ""}`}
                aria-label="开始月份"
                onPointerDown={() => setActiveThumb("start")}
                onPointerUp={commitDraftRange}
                onPointerCancel={commitDraftRange}
                onFocus={() => setActiveThumb("start")}
                onChange={(event) => {
                  updateDraftRange(Number(event.target.value), rangeState.endIndex);
                }}
                onKeyUp={commitDraftRange}
                onBlur={commitDraftRange}
                disabled={disabled}
              />
              <input
                type="range"
                min={0}
                max={options.length - 1}
                value={rangeState.endIndex}
                className={`deck-period-timeline-input deck-period-timeline-input--end${activeThumb === "end" ? " is-active" : ""}`}
                aria-label="结束月份"
                onPointerDown={() => setActiveThumb("end")}
                onPointerUp={commitDraftRange}
                onPointerCancel={commitDraftRange}
                onFocus={() => setActiveThumb("end")}
                onChange={(event) => {
                  updateDraftRange(rangeState.startIndex, Number(event.target.value));
                }}
                onKeyUp={commitDraftRange}
                onBlur={commitDraftRange}
                disabled={disabled}
              />
            </div>
            <div className="deck-period-timeline-range-meta">
              <span className="deck-period-timeline-handle-label">
                开始 <strong>{rangeState.startOption.label}</strong>
              </span>
              <span className="deck-period-timeline-handle-label">
                结束 <strong>{rangeState.endOption.label}</strong>
              </span>
            </div>
            <div className="deck-period-timeline-labels">
              <span>{firstOption.label}</span>
              <span>{rangeState.startOption.value === rangeState.endOption.value ? rangeState.startOption.label : `${rangeState.startOption.label} - ${rangeState.endOption.label}`}</span>
              <span>{rangeState.latestOption.label}</span>
            </div>
            <div className="time-axis-range-display">
              当前：<strong>{rangeState.startOption.label}</strong>
              {" · "}
              结束：<strong>{rangeState.endOption.label}</strong>
              {" · "}
              全范围 {firstOption.label} — {rangeState.latestOption.label}
            </div>
            {!rangeState.isDefaultLatest ? (
              <button
                type="button"
                className="btn btn-sm btn-ghost deck-period-timeline-reset"
                onClick={() => {
                  draftRangeRef.current = null;
                  setDraftRange(null);
                  onChange(null);
                }}
                disabled={disabled}
              >
                回到默认最新月
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}
