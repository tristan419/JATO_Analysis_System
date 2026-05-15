import { useState } from "react";

interface CountryStats {
  totalCountries: number;
  jatoCountries: number;
}

interface WorkbenchCoverage {
  modelSource: number;
  brandSource: number;
  missingSource: number;
}

interface ReviewDeliveryPanelProps {
  totalCases: number;
  pendingCount: number;
  approvedCount: number;
  rejectedCount: number;
  groupCount: number;
  countryStats?: CountryStats | null;
  workbenchCoverage?: WorkbenchCoverage | null;
}

type DeliveryState = "done" | "active" | "planned";

interface DeliveryStep {
  title: string;
  detail: string;
  state: DeliveryState;
}

interface GanttRow {
  label: string;
  start: number;
  span: number;
  state: DeliveryState;
  done?: number;
  total?: number;
  progressPct?: number;
}

const GANTT_COLUMNS = [
  "Scope",
  "Source",
  "Ingest",
  "Review",
  "Materialize",
  "Audit",
];

function resolveDeliveryState(active: boolean, done: boolean): DeliveryState {
  if (done) {
    return "done";
  }
  if (active) {
    return "active";
  }
  return "planned";
}

function formatPct(n: number): string {
  return `${Math.round(Math.max(0, Math.min(100, n)))}%`;
}

export function ReviewDeliveryPanel({
  totalCases,
  pendingCount,
  approvedCount,
  rejectedCount,
  groupCount,
  countryStats,
  workbenchCoverage,
}: ReviewDeliveryPanelProps) {
  const [collapsed, setCollapsed] = useState(false);
  const reviewedCount = approvedCount + rejectedCount;
  const denominator = Math.max(totalCases, 1);

  const flowSteps: DeliveryStep[] = [
    {
      title: "Source & Ingest",
      detail: totalCases > 0 ? `${totalCases} cases 已进入审核体系` : "等待 observation 写入 review 队列",
      state: resolveDeliveryState(totalCases > 0, totalCases > 0),
    },
    {
      title: "Group & Queue",
      detail: groupCount > 0 ? `${groupCount} 个Model已整理` : "等待生成模型分组",
      state: resolveDeliveryState(groupCount > 0, groupCount > 0),
    },
    {
      title: "Manual Review",
      detail: pendingCount > 0 ? `${pendingCount} 条待审` : reviewedCount > 0 ? "当前没有待审积压" : "等待审核开始",
      state: resolveDeliveryState(pendingCount > 0, totalCases > 0 && pendingCount === 0),
    },
    {
      title: "Materialize & Audit",
      detail: approvedCount > 0 ? `${approvedCount} 条已可进入 current price 刷新` : "等待审核通过后刷新 current prices",
      state: resolveDeliveryState(approvedCount > 0, approvedCount > 0 && pendingCount === 0),
    },
  ];

  const progressRows = [
    {
      label: "Reviewed",
      value: reviewedCount,
      percent: (reviewedCount / denominator) * 100,
      state: reviewedCount > 0 ? "done" : "planned",
    },
    {
      label: "Approved",
      value: approvedCount,
      percent: (approvedCount / denominator) * 100,
      state: approvedCount > 0 ? "active" : "planned",
    },
    {
      label: "Pending",
      value: pendingCount,
      percent: (pendingCount / denominator) * 100,
      state: pendingCount > 0 ? "active" : "done",
    },
  ] as const;

  // ── Per-swimlane progress ──────────────────────────────
  const countryTotal = countryStats?.jatoCountries ?? 0;
  const countryDone = countryStats?.totalCountries ?? 0;
  const countryPct = countryTotal > 0 ? (countryDone / countryTotal) * 100 : 0;

  // Source cleanup: sources with coverage / total candidates
  const sourceTotal = workbenchCoverage
    ? workbenchCoverage.modelSource + workbenchCoverage.brandSource + workbenchCoverage.missingSource
    : 0;
  const sourceDone = workbenchCoverage
    ? workbenchCoverage.modelSource + workbenchCoverage.brandSource
    : 0;
  const sourcePct = sourceTotal > 0 ? (sourceDone / sourceTotal) * 100 : 0;

  const reviewPct = totalCases > 0 ? (reviewedCount / totalCases) * 100 : 0;
  const materializePct = totalCases > 0 ? (approvedCount / totalCases) * 100 : 0;
  const qaPct = reviewedCount > 0 ? (approvedCount / reviewedCount) * 100 : 0;

  const ganttRows: GanttRow[] = [
    {
      label: "Country rollout",
      start: 1,
      span: 2,
      state: resolveDeliveryState(countryDone > 0, countryPct >= 100),
      done: countryDone,
      total: countryTotal,
      progressPct: countryPct,
    },
    {
      label: "Source cleanup",
      start: 2,
      span: 2,
      state: resolveDeliveryState(sourceDone > 0, sourcePct >= 100),
      done: sourceDone,
      total: sourceTotal,
      progressPct: sourcePct,
    },
    {
      label: "Review decisions",
      start: 3,
      span: 2,
      state: resolveDeliveryState(reviewedCount > 0, reviewedCount >= totalCases && totalCases > 0),
      done: reviewedCount,
      total: totalCases,
      progressPct: reviewPct,
    },
    {
      label: "Materialize refresh",
      start: 5,
      span: 1,
      state: resolveDeliveryState(approvedCount > 0, approvedCount >= totalCases && totalCases > 0),
      done: approvedCount,
      total: totalCases,
      progressPct: materializePct,
    },
    {
      label: "QA / anomaly pass",
      start: 6,
      span: 1,
      state: resolveDeliveryState(approvedCount > 0, approvedCount >= reviewedCount && reviewedCount > 0),
      done: approvedCount,
      total: reviewedCount,
      progressPct: qaPct,
    },
  ];

  const stackSegments = [
    {
      label: "Approved",
      percent: (approvedCount / denominator) * 100,
      state: "done",
    },
    {
      label: "Rejected",
      percent: (rejectedCount / denominator) * 100,
      state: "planned",
    },
    {
      label: "Pending",
      percent: (pendingCount / denominator) * 100,
      state: "active",
    },
  ] as const;

  return (
    <div className={`card crud-card review-delivery-card${collapsed ? " is-collapsed" : ""}`}>
      <div className="detail-section-head">
        <div>
          <div className="card-title">Review Delivery View</div>
          <p className="section-note">把审核主链路、当前进度和开发甘特收成一个可折叠面板；不再依赖 candidate scope / backlog 工作台。</p>
        </div>
        <div className="review-delivery-actions">
          <span className="table-status-chip table-status-chip--compact">
            <span>Reviewed</span>
            <strong>{reviewedCount}</strong>
            <span>/ {totalCases}</span>
          </span>
          <button
            type="button"
            className="btn btn-sm btn-secondary"
            onClick={() => setCollapsed((current) => !current)}
          >
            {collapsed ? "展开" : "收起"}
          </button>
        </div>
      </div>

      {!collapsed && (
        <div className="review-delivery-grid">
          <section className="review-delivery-pane">
            <div className="review-delivery-pane-head">
              <strong>Flowchart</strong>
              <span className="section-note">审核闭环</span>
            </div>
            <div className="review-delivery-flow">
              {flowSteps.map((step, index) => (
                <article
                  key={step.title}
                  className={`review-delivery-step is-${step.state}`}
                >
                  <div className="review-delivery-step-node">{index + 1}</div>
                  <div className="review-delivery-step-copy">
                    <strong>{step.title}</strong>
                    <p>{step.detail}</p>
                  </div>
                </article>
              ))}
            </div>
          </section>

          <section className="review-delivery-pane">
            <div className="review-delivery-pane-head">
              <strong>Progress</strong>
              <span className="section-note">按当前 case 状态统计</span>
            </div>
            <div className="review-delivery-stack" aria-hidden="true">
              {stackSegments.map((segment) => (
                <span
                  key={segment.label}
                  className={`review-delivery-stack-segment is-${segment.state}`}
                  style={{ width: `${segment.percent}%` }}
                />
              ))}
            </div>
            <div className="review-delivery-legend">
              {stackSegments.map((segment) => (
                <span key={segment.label} className="review-delivery-legend-item">
                  <span className={`review-delivery-legend-dot is-${segment.state}`} />
                  {segment.label}
                </span>
              ))}
            </div>
            <div className="review-progress-list">
              {progressRows.map((row) => (
                <div key={row.label} className="review-progress-row">
                  <div className="review-progress-copy">
                    <strong>{row.label}</strong>
                    <span>{row.value} / {totalCases}</span>
                  </div>
                  <div className="review-progress-track">
                    <span
                      className={`review-progress-fill is-${row.state}`}
                      style={{ width: `${row.percent}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>
          </section>

          <section className="review-delivery-pane review-delivery-pane--gantt">
            <div className="review-delivery-pane-head">
              <strong>Development Gantt</strong>
              <span className="section-note">前端审核交付节奏 · 每泳道显示完成进度</span>
            </div>
            <div className="review-gantt">
              <div className="review-gantt-row review-gantt-row--head">
                <span className="review-gantt-label">Track</span>
                {GANTT_COLUMNS.map((column) => (
                  <span key={column} className="review-gantt-column">{column}</span>
                ))}
                <span className="review-gantt-progress-head">Progress</span>
              </div>
              {ganttRows.map((row) => (
                <div key={row.label} className="review-gantt-row">
                  <span className="review-gantt-label">{row.label}</span>
                  <div className="review-gantt-lane">
                    <span
                      className={`review-gantt-bar is-${row.state}`}
                      style={{
                        gridColumn: `${row.start} / span ${row.span}`,
                      }}
                    >
                      {row.label}
                    </span>
                  </div>
                  <div className="review-gantt-progress" title={row.total != null ? `${row.done ?? 0} / ${row.total}` : undefined}>
                    {row.total != null && row.total > 0 ? (
                      <>
                        <span className="review-gantt-progress-track">
                          <span
                            className={`review-gantt-progress-fill is-${row.state}`}
                            style={{ width: formatPct(row.progressPct ?? 0) }}
                          />
                        </span>
                        <span className="review-gantt-progress-text">
                          {row.done ?? 0}/{row.total} ({formatPct(row.progressPct ?? 0)})
                        </span>
                      </>
                    ) : (
                      <span className="review-gantt-progress-text review-gantt-progress-text--muted">
                        —
                      </span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>
      )}
    </div>
  );
}
