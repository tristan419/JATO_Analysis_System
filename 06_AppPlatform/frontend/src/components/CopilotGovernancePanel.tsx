import type {
  CountryCopilotEvidencePack,
  CountryCopilotGovernanceTrace,
  CountryCopilotSourcePlan,
} from "../types/countryChat";

function laneLabel(lane: string): string {
  const map: Record<string, string> = {
    structured_bi: "BI 数据",
    canonical_entity: "实体价格",
    voc: "VOC 洞察",
    policy_tax: "政策/税务",
    news: "新闻",
    live_web: "联网",
  };
  return map[lane] ?? lane;
}

function laneBadge(lane: string): string {
  const map: Record<string, string> = {
    structured_bi: "copilot-plan-status is-ready",
    canonical_entity: "copilot-plan-status is-ready",
    voc: "copilot-plan-status is-planned",
    policy_tax: "copilot-plan-status is-planned",
    news: "copilot-plan-status is-planned",
    live_web: "copilot-plan-status is-unknown",
  };
  return map[lane] ?? "copilot-plan-status is-unknown";
}

export function CopilotGovernancePanel({
  sourcePlan,
  evidencePack,
  governanceTrace,
  compact = false,
}: {
  sourcePlan?: CountryCopilotSourcePlan | null;
  evidencePack?: CountryCopilotEvidencePack | null;
  governanceTrace?: CountryCopilotGovernanceTrace | null;
  compact?: boolean;
}) {
  if (!sourcePlan && !evidencePack) return null;

  return (
    <details className="copilot-answer-section is-collapsible">
      <summary className="copilot-answer-section-head">
        <strong>治理面板</strong>
        <span className="copilot-answer-section-kicker">
          {sourcePlan?.execution_mode ?? "unknown"}
          {evidencePack ? ` · ${evidencePack.sources.length} sources` : ""}
        </span>
      </summary>

      {governanceTrace ? (
        <div style={{ fontSize: 12, color: "#64748b", marginBottom: 8 }}>
          Intent: <strong>{governanceTrace.intent}</strong>
          {governanceTrace.intentRoute
            ? ` · Route: ${governanceTrace.intentRoute}`
            : ""}
        </div>
      ) : null}

      {sourcePlan?.items?.length ? (
        <div
          className="copilot-plan-grid"
          style={{ marginBottom: compact ? 8 : 12 }}
        >
          {sourcePlan.items
            .slice(0, compact ? 3 : 8)
            .map((item) => (
              <div key={item.source_id} className="copilot-plan-card">
                <div className="copilot-plan-card-head">
                  <strong>{item.source_id}</strong>
                  <span className={laneBadge(item.source_lane)}>
                    {laneLabel(item.source_lane)}
                  </span>
                </div>
                <div className="copilot-plan-card-meta">
                  {item.required ? "必需" : "补充"}
                  {" · "}
                  {item.source_lane}
                </div>
                {item.reason ? (
                  <div className="copilot-plan-card-body">{item.reason}</div>
                ) : null}
              </div>
            ))}
        </div>
      ) : null}

      {evidencePack?.sources?.length ? (
        <div style={{ fontSize: 12, color: "#64748b", marginBottom: 4 }}>
          <strong>Evidence Coverage:</strong>{" "}
          {evidencePack.sources
            .map((s) => `${s.source_id} (${s.coverage})`)
            .join(", ")}
        </div>
      ) : null}

      {evidencePack?.limitations?.length ? (
        <div style={{ fontSize: 12, color: "#ef4444", marginTop: 4 }}>
          Limitations: {evidencePack.limitations.join("; ")}
        </div>
      ) : null}
    </details>
  );
}
