import type { PriceHistoryEntry } from "../types";
import {
  formatCurrentPriceDate,
  formatCurrentPriceNumber,
} from "../utils/msrpCurrentPrice";

interface TimelineSegment {
  id: string;
  startTs: number;
  endTs: number;
  value: number;
  label: string;
  isCurrent: boolean;
}

const VIEWBOX_WIDTH = 720;
const VIEWBOX_HEIGHT = 95;
const PAD_LEFT = 18;
const PAD_RIGHT = 18;
const PAD_TOP = 10;
const PAD_BOTTOM = 14;

function toTimestamp(value: string | null | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : fallback;
}

function buildSegments(entries: PriceHistoryEntry[]): TimelineSegment[] {
  return entries
    .slice()
    .sort(
      (left, right) =>
        toTimestamp(left.validFromUtc, 0) - toTimestamp(right.validFromUtc, 0),
    )
    .map((entry) => {
      const startTs = toTimestamp(entry.validFromUtc, 0);
      const endFallback = startTs + 1;
      const endTs = Math.max(
        toTimestamp(
          entry.validToUtc ?? entry.lastConfirmedAtUtc ?? entry.validFromUtc,
          endFallback,
        ),
        endFallback,
      );
      return {
        id: entry.id,
        startTs,
        endTs,
        value: Number(entry.msrpValue),
        label: `${formatCurrentPriceNumber(entry.msrpValue)} ${entry.currency}`,
        isCurrent: entry.validToUtc === null,
      };
    });
}

function buildTimelinePath(segments: TimelineSegment[]): {
  linePath: string;
  areaPath: string;
  dots: Array<{ id: string; x: number; y: number; current: boolean }>;
} {
  if (segments.length === 0) {
    return { linePath: "", areaPath: "", dots: [] };
  }

  const width = VIEWBOX_WIDTH - PAD_LEFT - PAD_RIGHT;
  const height = VIEWBOX_HEIGHT - PAD_TOP - PAD_BOTTOM;
  const baseline = PAD_TOP + height;
  const minTs = Math.min(...segments.map((segment) => segment.startTs));
  const maxTs = Math.max(...segments.map((segment) => segment.endTs));
  const minValue = Math.min(...segments.map((segment) => segment.value));
  const maxValue = Math.max(...segments.map((segment) => segment.value));
  const tsRange = Math.max(1, maxTs - minTs);
  const valueRange = Math.max(1, maxValue - minValue);

  const xScale = (timestamp: number) =>
    PAD_LEFT + ((timestamp - minTs) / tsRange) * width;
  const yScale = (value: number) =>
    PAD_TOP + ((maxValue - value) / valueRange) * height;

  let linePath = "";
  let areaPath = "";
  let previousY: number | null = null;
  const dots: Array<{ id: string; x: number; y: number; current: boolean }> = [];

  segments.forEach((segment, index) => {
    const startX = xScale(segment.startTs);
    const endX = xScale(segment.endTs);
    const y = yScale(segment.value);

    if (index === 0) {
      linePath = `M ${startX} ${y}`;
      areaPath = `M ${startX} ${baseline} L ${startX} ${y}`;
    } else if (previousY !== null) {
      linePath += ` L ${startX} ${previousY} L ${startX} ${y}`;
      areaPath += ` L ${startX} ${previousY} L ${startX} ${y}`;
    }

    linePath += ` L ${endX} ${y}`;
    areaPath += ` L ${endX} ${y}`;
    previousY = y;
    dots.push({ id: `${segment.id}-start`, x: startX, y, current: false });
    dots.push({ id: `${segment.id}-end`, x: endX, y, current: segment.isCurrent });
  });

  const lastX = xScale(segments[segments.length - 1].endTs);
  areaPath += ` L ${lastX} ${baseline} Z`;

  return { linePath, areaPath, dots };
}

export function PriceHistoryTimeline({ entries }: { entries: PriceHistoryEntry[] }) {
  const segments = buildSegments(entries);

  if (segments.length === 0) {
    return (
      <div className="price-history-timeline price-history-timeline--empty">
        <div className="crud-empty-state">暂无价格时间轴</div>
      </div>
    );
  }

  const { linePath, areaPath, dots } = buildTimelinePath(segments);
  const firstEntry = entries
    .slice()
    .sort(
      (left, right) =>
        toTimestamp(left.validFromUtc, 0) - toTimestamp(right.validFromUtc, 0),
    )[0];
  const lastEntry = entries
    .slice()
    .sort(
      (left, right) =>
        toTimestamp(right.validFromUtc, 0) - toTimestamp(left.validFromUtc, 0),
    )[0];

  return (
    <div className="price-history-timeline">
      <div className="price-history-timeline-summary">
        <div className="price-history-timeline-stat">
          <span className="admin-detail-label">Periods</span>
          <strong>{segments.length}</strong>
        </div>
        <div className="price-history-timeline-stat">
          <span className="admin-detail-label">Start</span>
          <strong>{formatCurrentPriceDate(firstEntry.validFromUtc)}</strong>
        </div>
        <div className="price-history-timeline-stat">
          <span className="admin-detail-label">Latest</span>
          <strong>
            {formatCurrentPriceDate(
              lastEntry.validToUtc ?? lastEntry.lastConfirmedAtUtc,
            )}
          </strong>
        </div>
      </div>

      <svg
        className="price-history-timeline-svg"
        viewBox={`0 0 ${VIEWBOX_WIDTH} ${VIEWBOX_HEIGHT}`}
        role="img"
        aria-label="Price history timeline"
      >
        <defs>
          <linearGradient id="price-history-timeline-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="rgba(143, 201, 162, 0.28)" />
            <stop offset="100%" stopColor="rgba(143, 201, 162, 0.02)" />
          </linearGradient>
        </defs>
        <line
          x1={PAD_LEFT}
          x2={VIEWBOX_WIDTH - PAD_RIGHT}
          y1={VIEWBOX_HEIGHT - PAD_BOTTOM}
          y2={VIEWBOX_HEIGHT - PAD_BOTTOM}
          className="price-history-timeline-baseline"
        />
        <path d={areaPath} className="price-history-timeline-area" />
        <path d={linePath} className="price-history-timeline-line" />
        {dots.map((dot) => (
          <circle
            key={dot.id}
            cx={dot.x}
            cy={dot.y}
            r={dot.current ? 5 : 3.5}
            className={`price-history-timeline-dot${dot.current ? " is-current" : ""}`}
          />
        ))}
      </svg>

      <div className="price-history-timeline-footer">
        <span>{formatCurrentPriceDate(firstEntry.validFromUtc)}</span>
        <span>{segments[segments.length - 1].label}</span>
        <span>
          {formatCurrentPriceDate(
            lastEntry.validToUtc ?? lastEntry.lastConfirmedAtUtc,
          )}
        </span>
      </div>
    </div>
  );
}
