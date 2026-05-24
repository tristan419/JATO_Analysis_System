import type {
  CountryCopilotEvidencePack,
  CountryCopilotGovernanceTrace,
  CountryCopilotSourcePlan,
  CountryCopilotStructuredAnswer,
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
  structuredAnswer,
  compact = false,
}: {
  sourcePlan?: CountryCopilotSourcePlan | null;
  evidencePack?: CountryCopilotEvidencePack | null;
  governanceTrace?: CountryCopilotGovernanceTrace | null;
  structuredAnswer?: CountryCopilotStructuredAnswer | null;
  compact?: boolean;
}) {
  if (!sourcePlan && !evidencePack && !structuredAnswer) return null;

  const blocks = structuredAnswer?.blocks ?? [];
  const structuredLimitations = structuredAnswer?.limitations ?? [];
  const evidenceSources = evidencePack?.sources ?? [];
  const evidenceLimitations = evidencePack?.limitations ?? [];

  return (
    <details className="copilot-answer-section is-collapsible">
      <summary className="copilot-answer-section-head">
        <strong>治理面板</strong>
        <span className="copilot-answer-section-kicker">
          {sourcePlan?.execution_mode ?? "unknown"}
          {evidencePack ? ` · ${evidenceSources.length} sources` : ""}
          {blocks.length > 0 ? ` · ${blocks.length} blocks` : ""}
        </span>
      </summary>

      {blocks.length > 0 ? (
        <div style={{ marginBottom: 12 }}>
          {blocks.slice(0, compact ? 3 : 6).map((block, i) => (
            <div key={i} style={{ fontSize: 12, color: "#475569", marginBottom: 6, padding: "4px 8px", background: "#f8fafc", borderRadius: 4 }}>
              <strong>{block.block_type}: {block.title}</strong>
              {block.content ? <div style={{ marginTop: 2 }}>{block.content}</div> : null}
            </div>
          ))}
        </div>
      ) : null}

      {structuredLimitations.length ? (
        <div style={{ fontSize: 12, color: "#ef4444", marginBottom: 4 }}>
          Warning: {structuredLimitations.join(" · ")}
        </div>
      ) : null}

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

      {evidenceSources.length ? (
        <div style={{ fontSize: 12, color: "#64748b", marginBottom: 4 }}>
          <strong>Evidence Coverage:</strong>{" "}
          {evidenceSources
            .map((s) => `${s.source_id} (${s.coverage})`)
            .join(", ")}
        </div>
      ) : null}

      {evidenceLimitations.length ? (
        <div style={{ fontSize: 12, color: "#ef4444", marginTop: 4 }}>
          Limitations: {evidenceLimitations.join("; ")}
        </div>
      ) : null}
    </details>
  );
}
