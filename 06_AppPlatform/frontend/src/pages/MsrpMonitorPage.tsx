import { useEffect, useMemo, useState } from "react";

import { api } from "../api/client";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  MsrpMonitoringModelEvent,
  MsrpMonitoringResponse,
  MsrpMonitoringTimelineEvent,
} from "../types";

const WINDOW_OPTIONS = [
  { value: 7, label: "7D" },
  { value: 30, label: "30D" },
  { value: 90, label: "90D" },
  { value: 180, label: "180D" },
] as const;

const THRESHOLD_OPTIONS = [
  { value: 0, label: "Any" },
  { value: 1, label: ">= 1%" },
  { value: 3, label: ">= 3%" },
  { value: 5, label: ">= 5%" },
] as const;

const CHART_WIDTH = 920;
const CHART_HEIGHT = 560;
const CHART_MARGIN = { top: 38, right: 36, bottom: 54, left: 78 } as const;

type DeckTab = "overview" | "countries" | "timeline" | "source";

function formatNumber(value: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return value.toLocaleString("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPct(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return `${value > 0 ? "+" : ""}${value.toFixed(1)}%`;
}

function formatTime(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function eventLabel(event: MsrpMonitoringModelEvent): string {
  return `${event.brand} ${event.jatoModel}`;
}

function countryKey(item: MsrpMonitoringTimelineEvent): string {
  return `${item.country}|${item.jatoTrim}|${item.changedAtUtc ?? ""}`;
}

function riskLabel(event: MsrpMonitoringModelEvent): string {
  if (event.suspectedFalsePositiveCount > 0) {
    return `${event.suspectedFalsePositiveCount} risk`;
  }
  if (event.sourceRiskCount > 0) {
    return `${event.sourceRiskCount} source`;
  }
  return "clean";
}

function domain(values: number[], fallback: [number, number], paddingRatio = 0.08): [number, number] {
  if (values.length === 0) {
    return fallback;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(1, max - min);
  return [min - span * paddingRatio, max + span * paddingRatio];
}

function niceTicks(min: number, max: number, count: number): number[] {
  const span = Math.max(1, max - min);
  const rawStep = span / Math.max(1, count - 1);
  const power = 10 ** Math.floor(Math.log10(rawStep));
  const ratio = rawStep / power;
  const step = (ratio <= 1 ? 1 : ratio <= 2 ? 2 : ratio <= 5 ? 5 : 10) * power;
  const start = Math.ceil(min / step) * step;
  const ticks: number[] = [];
  for (let value = start; value <= max + step * 0.5; value += step) {
    ticks.push(Math.round(value));
  }
  return ticks;
}

function allTimeline(events: MsrpMonitoringModelEvent[]): MsrpMonitoringTimelineEvent[] {
  return events
    .flatMap((event) => event.timeline.map((item) => ({ ...item, brand: event.brand, jatoModel: event.jatoModel })))
    .sort((left, right) => String(left.changedAtUtc ?? "").localeCompare(String(right.changedAtUtc ?? "")));
}

function selectedCountry(
  event: MsrpMonitoringModelEvent | null,
  selectedKey: string | null,
): MsrpMonitoringTimelineEvent | null {
  if (!event) {
    return null;
  }
  return event.countries.find((item) => countryKey(item) === selectedKey) ?? event.countries[0] ?? null;
}

function ChartPoint({
  event,
  x,
  oldY,
  currentY,
  selected,
  onSelect,
}: {
  event: MsrpMonitoringModelEvent;
  x: number;
  oldY: number;
  currentY: number;
  selected: boolean;
  onSelect: () => void;
}) {
  const radius = Math.min(20, 7 + Math.sqrt(event.affectedCountryCount) * 2.5);
  return (
    <g className="msrp-monitor-point" onClick={onSelect} onKeyDown={(e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onSelect();
      }
    }} tabIndex={0} role="button" aria-label={eventLabel(event)}>
      <line className="msrp-monitor-drop-line" x1={x} x2={x} y1={oldY} y2={currentY} />
      <circle className="msrp-monitor-old-dot" cx={x} cy={oldY} r={radius * 0.62} />
      <line
        className="msrp-monitor-current-line"
        x1={x - radius * 1.25}
        x2={x + radius * 1.25}
        y1={currentY}
        y2={currentY}
        stroke={event.powertrainColor}
      />
      <circle
        className={`msrp-monitor-current-dot${selected ? " is-selected" : ""}`}
        cx={x}
        cy={currentY}
        r={radius}
        fill={event.powertrainColor}
      />
      <text className="msrp-monitor-point-label" x={x + radius + 6} y={currentY + 4}>
        {eventLabel(event)}
      </text>
    </g>
  );
}

function MsrpEventChart({
  events,
  selectedEventId,
  onSelect,
}: {
  events: MsrpMonitoringModelEvent[];
  selectedEventId: string | null;
  onSelect: (eventId: string) => void;
}) {
  const drawableEvents = events.filter(
    (event) => event.lengthMm !== null
      && event.medianCurrentMsrpEur !== null
      && event.medianOldMsrpEur !== null,
  );
  const xValues = drawableEvents.map((event) => Number(event.lengthMm));
  const yValues = drawableEvents.flatMap((event) => [
    Number(event.medianOldMsrpEur),
    Number(event.medianCurrentMsrpEur),
  ]);
  const [xMin, xMax] = domain(xValues, [4000, 5000]);
  const [yMin, yMax] = domain(yValues, [20000, 80000], 0.12);
  const innerWidth = CHART_WIDTH - CHART_MARGIN.left - CHART_MARGIN.right;
  const innerHeight = CHART_HEIGHT - CHART_MARGIN.top - CHART_MARGIN.bottom;
  const scaleX = (value: number) => CHART_MARGIN.left + ((value - xMin) / Math.max(1, xMax - xMin)) * innerWidth;
  const scaleY = (value: number) => CHART_MARGIN.top + (1 - (value - yMin) / Math.max(1, yMax - yMin)) * innerHeight;
  const xTicks = niceTicks(xMin, xMax, 6);
  const yTicks = niceTicks(yMin, yMax, 6);

  return (
    <div className="msrp-monitor-chart-shell">
      <div className="msrp-monitor-chart-head">
        <div>
          <h2>Length x MSRP movement</h2>
          <p>Model-level aggregation first; country spread appears in drilldown.</p>
        </div>
        <div className="msrp-monitor-legend">
          <span><i className="old" /> Old MSRP</span>
          <span><i className="line" /> Price move</span>
          <span><i className="current" /> Current MSRP</span>
        </div>
      </div>
      <svg viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`} className="msrp-monitor-chart" role="img" aria-label="MSRP monitor model event chart">
        <g className="msrp-monitor-grid">
          {xTicks.map((tick) => (
            <line key={`x-${tick}`} x1={scaleX(tick)} x2={scaleX(tick)} y1={CHART_MARGIN.top} y2={CHART_MARGIN.top + innerHeight} />
          ))}
          {yTicks.map((tick) => (
            <line key={`y-${tick}`} x1={CHART_MARGIN.left} x2={CHART_MARGIN.left + innerWidth} y1={scaleY(tick)} y2={scaleY(tick)} />
          ))}
        </g>
        <g className="msrp-monitor-axis">
          <line x1={CHART_MARGIN.left} x2={CHART_MARGIN.left + innerWidth} y1={CHART_MARGIN.top + innerHeight} y2={CHART_MARGIN.top + innerHeight} />
          <line x1={CHART_MARGIN.left} x2={CHART_MARGIN.left} y1={CHART_MARGIN.top} y2={CHART_MARGIN.top + innerHeight} />
          {xTicks.map((tick) => (
            <g key={`xt-${tick}`}>
              <text x={scaleX(tick)} y={CHART_MARGIN.top + innerHeight + 26} textAnchor="middle">{formatNumber(tick)} mm</text>
            </g>
          ))}
          {yTicks.map((tick) => (
            <g key={`yt-${tick}`}>
              <text x={CHART_MARGIN.left - 12} y={scaleY(tick) + 4} textAnchor="end">{formatCurrency(tick)}</text>
            </g>
          ))}
          <text x={CHART_MARGIN.left + innerWidth / 2} y={CHART_HEIGHT - 12} textAnchor="middle">Vehicle length</text>
          <text x={18} y={CHART_MARGIN.top + innerHeight / 2} textAnchor="middle" transform={`rotate(-90 18 ${CHART_MARGIN.top + innerHeight / 2})`}>MSRP EUR normalized</text>
        </g>
        {drawableEvents.map((event) => (
          <ChartPoint
            key={event.eventId}
            event={event}
            x={scaleX(Number(event.lengthMm))}
            oldY={scaleY(Number(event.medianOldMsrpEur))}
            currentY={scaleY(Number(event.medianCurrentMsrpEur))}
            selected={event.eventId === selectedEventId}
            onSelect={() => onSelect(event.eventId)}
          />
        ))}
      </svg>
      {drawableEvents.length === 0 ? (
        <div className="msrp-monitor-empty">No model events with vehicle length are available in this time window.</div>
      ) : null}
    </div>
  );
}

export function MsrpMonitorPage() {
  const [windowDays, setWindowDays] = useState(30);
  const [thresholdPct, setThresholdPct] = useState(0);
  const [countryFilter, setCountryFilter] = useState("all");
  const [brandFilter, setBrandFilter] = useState("all");
  const [data, setData] = useState<MsrpMonitoringResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
  const [selectedCountryKey, setSelectedCountryKey] = useState<string | null>(null);
  const [deckTab, setDeckTab] = useState<DeckTab>("overview");
  const [timelineIndex, setTimelineIndex] = useState(0);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");
    api.getMsrpMonitoringEvents({
      country: countryFilter === "all" ? undefined : countryFilter,
      brand: brandFilter === "all" ? undefined : brandFilter,
      window_days: windowDays,
      threshold_pct: thresholdPct,
      limit: 500,
    }).then((response) => {
      if (!active) return;
      setData(response);
      const firstEvent = response.events[0] ?? null;
      setSelectedEventId((current) => (
        current && response.events.some((event) => event.eventId === current)
          ? current
          : firstEvent?.eventId ?? null
      ));
      setSelectedCountryKey((current) => {
        if (!firstEvent) return null;
        const allCountries = response.events.flatMap((event) => event.countries);
        const firstCountry = firstEvent.countries[0] ?? null;
        return current && allCountries.some((item) => countryKey(item) === current)
          ? current
          : firstCountry
            ? countryKey(firstCountry)
            : null;
      });
    }).catch((err: unknown) => {
      if (!active) return;
      setError(err instanceof Error ? err.message : String(err));
      setData(null);
    }).finally(() => {
      if (active) setLoading(false);
    });
    return () => { active = false; };
  }, [brandFilter, countryFilter, thresholdPct, windowDays]);

  const events = data?.events ?? [];
  const selectedEvent = events.find((event) => event.eventId === selectedEventId) ?? events[0] ?? null;
  const selectedCountryEvent = selectedCountry(selectedEvent, selectedCountryKey);
  const timeline = useMemo(() => allTimeline(events), [events]);
  const countryOptions = useMemo(() => {
    const countries = new Map<string, string>();
    events.forEach((event) => event.countries.forEach((item) => countries.set(item.country, item.countryLabel)));
    return Array.from(countries.entries()).sort((a, b) => a[1].localeCompare(b[1]));
  }, [events]);
  const brandOptions = useMemo(() => Array.from(new Set(events.map((event) => event.brand))).sort(), [events]);
  const missingLengthEvents = events.filter((event) => event.lengthMissing);

  function selectEvent(eventId: string): void {
    const nextEvent = events.find((event) => event.eventId === eventId) ?? null;
    setSelectedEventId(eventId);
    setSelectedCountryKey(nextEvent?.countries[0] ? countryKey(nextEvent.countries[0]) : null);
  }

  function selectTimeline(index: number): void {
    const safeIndex = Math.max(0, Math.min(index, timeline.length - 1));
    const item = timeline[safeIndex];
    if (!item) return;
    const event = events.find((candidate) => (
      candidate.brand === item.brand
      && candidate.jatoModel === item.jatoModel
      && candidate.jatoPowertrain === item.jatoPowertrain
    ));
    setTimelineIndex(safeIndex);
    if (event) {
      setSelectedEventId(event.eventId);
      setSelectedCountryKey(countryKey(item));
      setDeckTab("timeline");
    }
  }

  return (
    <section className="msrp-monitor-page">
      <header className="msrp-monitor-topbar">
        <div>
          <p className="msrp-monitor-kicker">Market Monitor / 市场监控</p>
          <h1>MSRP监控</h1>
          <p className="msrp-monitor-subtitle">跨国家车型调价聚合、国家展开、trim/source evidence 和时间轴追踪。</p>
        </div>
        <div className="msrp-monitor-controls">
          <label>
            Window
            <select value={windowDays} onChange={(event) => setWindowDays(Number(event.target.value))}>
              {WINDOW_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label>
            Change
            <select value={thresholdPct} onChange={(event) => setThresholdPct(Number(event.target.value))}>
              {THRESHOLD_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label>
            Country
            <select value={countryFilter} onChange={(event) => setCountryFilter(event.target.value)}>
              <option value="all">All</option>
              {countryOptions.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
            </select>
          </label>
          <label>
            Brand
            <select value={brandFilter} onChange={(event) => setBrandFilter(event.target.value)}>
              <option value="all">All</option>
              {brandOptions.map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
        </div>
      </header>

      {loading && !data ? <LoadingSurface mode="inline" label="加载 MSRP 监控" detail="读取 price history、source evidence 与调价事件" kicker="MSRP" /> : null}
      {error ? <div className="market-scan-state-card market-scan-state-card--error"><strong>Error</strong><p>{error}</p></div> : null}
      {data?.warnings.length ? (
        <div className="msrp-monitor-warning-list">
          {data.warnings.map((warning) => <span key={warning}>{warning}</span>)}
        </div>
      ) : null}

      {data ? (
        <>
          <div className="msrp-monitor-summary">
            <div><span>Model events</span><strong>{data.summary.eventCount}</strong></div>
            <div><span>Timeline moves</span><strong>{data.summary.timelineEventCount}</strong></div>
            <div><span>Countries</span><strong>{data.summary.affectedCountryCount}</strong></div>
            <div><span>Source risks</span><strong>{data.summary.sourceRiskCount}</strong></div>
            <div><span>Outliers</span><strong>{data.summary.outlierCount}</strong></div>
          </div>

          <div className="msrp-monitor-powertrain-legend">
            {Object.entries(data.powertrainColors).map(([powertrain, color]) => (
              <span key={powertrain}><i style={{ background: color }} />{powertrain}</span>
            ))}
          </div>

          <div className="msrp-monitor-main">
            <MsrpEventChart events={events} selectedEventId={selectedEvent?.eventId ?? null} onSelect={selectEvent} />

            <aside className="msrp-monitor-deck">
              <div className="msrp-monitor-deck-head">
                <span>Floating Deck</span>
                <strong>{selectedEvent ? eventLabel(selectedEvent) : "No event"}</strong>
                <small>
                  {selectedEvent
                    ? `${selectedEvent.jatoPowertrain} · ${selectedEvent.affectedCountryCount} countries · ${formatPct(selectedEvent.medianChangePct)} median`
                    : "Adjust filters to load events"}
                </small>
              </div>
              <div className="msrp-monitor-tabs">
                {(["overview", "countries", "timeline", "source"] as DeckTab[]).map((tab) => (
                  <button key={tab} type="button" className={deckTab === tab ? "is-active" : ""} onClick={() => setDeckTab(tab)}>
                    {tab}
                  </button>
                ))}
              </div>

              {deckTab === "overview" && selectedEvent ? (
                <div className="msrp-monitor-deck-section">
                  <div className="msrp-monitor-deck-stats">
                    <div><span>Drop range</span><strong>{formatPct(selectedEvent.minChangePct)} / {formatPct(selectedEvent.maxChangePct)}</strong></div>
                    <div><span>Current median</span><strong>{formatCurrency(selectedEvent.medianCurrentMsrpEur)}</strong></div>
                    <div>
                      <span>Length</span>
                      <strong>{selectedEvent.lengthMm ? `${selectedEvent.lengthMm} mm` : "Missing"}</strong>
                      <small>{selectedEvent.lengthSource ?? "no length source"}</small>
                    </div>
                    <div><span>Confidence</span><strong>{selectedEvent.confidence}</strong></div>
                  </div>
                  <div className="msrp-monitor-signal-list">
                    <span className={selectedEvent.multiCountrySync ? "is-good" : ""}>Multi-country sync: {selectedEvent.multiCountrySync ? "yes" : "no"}</span>
                    <span>Review flags: {selectedEvent.reviewRequiredCount}</span>
                    <span>Potential false positives: {selectedEvent.suspectedFalsePositiveCount}</span>
                    <span>Risk: {riskLabel(selectedEvent)}</span>
                  </div>
                  {missingLengthEvents.length > 0 ? (
                    <div className="msrp-monitor-missing-length">
                      <strong>Length missing</strong>
                      {missingLengthEvents.slice(0, 5).map((event) => (
                        <button key={event.eventId} type="button" onClick={() => selectEvent(event.eventId)}>
                          {eventLabel(event)} · {event.jatoPowertrain}
                        </button>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : null}

              {deckTab === "countries" && selectedEvent ? (
                <div className="msrp-monitor-country-list">
                  {selectedEvent.countries.map((item) => (
                    <button
                      key={countryKey(item)}
                      type="button"
                      className={countryKey(item) === selectedCountryKey ? "is-selected" : ""}
                      onClick={() => setSelectedCountryKey(countryKey(item))}
                    >
                      <span><strong>{item.countryLabel}</strong><small>{item.jatoTrim}</small></span>
                      <b>{formatPct(item.changePct)}</b>
                    </button>
                  ))}
                </div>
              ) : null}

              {deckTab === "timeline" ? (
                <div className="msrp-monitor-timeline">
                  <input
                    type="range"
                    min={0}
                    max={Math.max(0, timeline.length - 1)}
                    value={Math.min(timelineIndex, Math.max(0, timeline.length - 1))}
                    onChange={(event) => selectTimeline(Number(event.target.value))}
                  />
                  <div className="msrp-monitor-timeline-list">
                    {timeline.slice(-12).map((item, index, list) => {
                      const globalIndex = timeline.length - list.length + index;
                      return (
                        <button key={`${item.priceHistoryId}-${globalIndex}`} type="button" onClick={() => selectTimeline(globalIndex)}>
                          <span>{formatTime(item.changedAtUtc)}</span>
                          <strong>{item.brand} {item.jatoModel}</strong>
                          <small>{item.countryLabel} · {formatPct(item.changePct)}</small>
                        </button>
                      );
                    })}
                  </div>
                </div>
              ) : null}

              {deckTab === "source" && selectedCountryEvent ? (
                <div className="msrp-monitor-source-panel">
                  <dl>
                    <dt>Source status</dt><dd>{selectedCountryEvent.sourceStatus}</dd>
                    <dt>Review flag</dt><dd>{selectedCountryEvent.reviewFlag ? "Yes" : "No"}</dd>
                    <dt>Dryrun run</dt><dd>{selectedCountryEvent.evidence.dryrunRunId ?? "-"}</dd>
                    <dt>Batch</dt><dd>{selectedCountryEvent.evidence.scrapeBatchCode ?? "-"}</dd>
                    <dt>Source code</dt><dd>{selectedCountryEvent.source.sourceCode ?? "-"}</dd>
                    <dt>Payload hash</dt><dd>{selectedCountryEvent.evidence.sourcePayloadHash ?? "-"}</dd>
                  </dl>
                  {selectedCountryEvent.riskReasons.length > 0 ? (
                    <div className="msrp-monitor-risk-reasons">
                      {selectedCountryEvent.riskReasons.map((reason) => <span key={reason}>{reason}</span>)}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </aside>
          </div>

          <section className="msrp-monitor-detail">
            <header>
              <h2>Country drilldown</h2>
              <span>{selectedEvent ? eventLabel(selectedEvent) : "-"}</span>
            </header>
            <div className="msrp-monitor-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Country</th>
                    <th>Trim</th>
                    <th>Change</th>
                    <th>EUR normalized</th>
                    <th>Local currency</th>
                    <th>Source</th>
                    <th>Evidence</th>
                  </tr>
                </thead>
                <tbody>
                  {(selectedEvent?.countries ?? []).map((item) => (
                    <tr key={countryKey(item)} className={countryKey(item) === selectedCountryKey ? "is-selected" : ""} onClick={() => setSelectedCountryKey(countryKey(item))}>
                      <td>{item.countryLabel}</td>
                      <td>{item.jatoTrim || "-"}</td>
                      <td><strong>{formatPct(item.changePct)}</strong><small>{formatTime(item.changedAtUtc)}</small></td>
                      <td>{formatCurrency(item.oldMsrpEur)} → {formatCurrency(item.currentMsrpEur)}</td>
                      <td>{formatNumber(item.oldSourceMsrp)} → {formatNumber(item.currentSourceMsrp)} {item.sourceCurrency}</td>
                      <td>{item.sourceStatus}<small>{item.source.sourceType ?? "-"}</small></td>
                      <td>{item.evidence.dryrunRunId ?? item.evidence.scrapeBatchCode ?? item.currentObservationId}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}
    </section>
  );
}
