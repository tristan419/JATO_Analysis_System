import type { CountryChatTranscriptMessage } from "../contexts/CountryChatContext";
import {
  buildCountryChatAnswerPath,
  buildCountryChatAnswerSections,
} from "../contexts/countryChatHelpers";

function formatFreshness(value: string | null | undefined): string {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return text;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

function answerModeLabel(value: string | null | undefined): string {
  switch (value) {
    case "grounded-direct":
      return "直接回答";
    case "grounded-model":
      return "证据润色";
    case "grounded-fallback":
      return "降级回答";
    default:
      return "综合回答";
  }
}

export function CountryChatGroundedAnswer({
  message,
  compact = false,
}: {
  message: CountryChatTranscriptMessage;
  compact?: boolean;
}) {
  const grounding = message.grounding;
  const isAssistant = message.role === "assistant";
  if (!grounding && !isAssistant) {
    return <div className="copilot-message-body">{message.content}</div>;
  }

  const evidenceTables = compact
    ? grounding?.evidenceTables.slice(0, 2) ?? []
    : grounding?.evidenceTables ?? [];
  const answerPath = buildCountryChatAnswerPath({
    country: message.country ?? message.contextSnapshot?.country,
    intentRoute: message.intentRoute,
    answerMode: message.answerMode,
    layers: grounding?.layers,
    extractedParams: message.extractedParams,
  });
  const answerSections = buildCountryChatAnswerSections({
    content: message.content,
    summary: grounding?.summary,
  });
  const keyFindings = grounding?.keyFindings ?? [];
  const layers = grounding?.layers ?? [];
  const showAnswerPath = isAssistant && (
    answerPath.focusTags.length > 0
    || answerPath.steps.length > 0
    || Boolean(message.intentRoute)
    || Boolean(message.answerMode)
    || layers.length > 0
  );
  const strategyLabel = grounding?.strategyLabel ?? answerPath.routeLabel;

  return (
    <div className={`copilot-grounded-answer${compact ? " is-compact" : ""}`}>
      <div className="copilot-grounded-answer-head">
        <span className="copilot-grounded-answer-badge">
          {answerModeLabel(message.answerMode)}
        </span>
        {strategyLabel ? (
          <span className="copilot-grounded-answer-strategy">
            {strategyLabel}
          </span>
        ) : null}
      </div>

      {answerSections.lead ? (
        <div className="copilot-answer-section is-primary">
          <div className="copilot-answer-section-head">
            <strong>{grounding ? "直接回答" : "助手回答"}</strong>
          </div>
          <div className="copilot-answer-lead">{answerSections.lead}</div>
          {answerPath.focusTags.length > 0 ? (
            <div className="copilot-answer-focus">
              <span className="copilot-answer-focus-label">范围</span>
              <div className="copilot-answer-focus-tags">
                {answerPath.focusTags.map((tag) => (
                  <span key={tag} className="copilot-answer-focus-tag">
                    {tag}
                  </span>
                ))}
              </div>
            </div>
          ) : null}
        </div>
      ) : null}

      {keyFindings.length > 0 ? (
        <div className="copilot-answer-section">
          <div className="copilot-answer-section-head">
            <strong>关键结论</strong>
          </div>
          <div className="copilot-answer-finding-grid">
            {keyFindings.map((item) => (
              <div key={item} className="copilot-answer-finding">
                {item}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {showAnswerPath ? (
        <div className="copilot-answer-section">
          <div className="copilot-answer-section-head">
            <strong>思考链</strong>
            <span className="copilot-answer-section-kicker">Visible answer path</span>
          </div>
          <div className="copilot-answer-path-head">
            <span className="copilot-answer-path-pill">{answerPath.routeLabel}</span>
            <span className="copilot-answer-path-pill is-muted">{answerPath.outputLabel}</span>
            {answerPath.focusTags.map((tag) => (
              <span key={tag} className="copilot-answer-path-pill is-soft">
                {tag}
              </span>
            ))}
          </div>
          <ol className="copilot-answer-path-steps">
            {answerPath.steps.map((step) => (
              <li key={step}>{step}</li>
            ))}
          </ol>
        </div>
      ) : null}

      {answerSections.reasoningNotes.length > 0 ? (
        <div className="copilot-answer-section">
          <div className="copilot-answer-section-head">
            <strong>{grounding ? "回答说明" : "补充说明"}</strong>
          </div>
          <div className="copilot-answer-notes">
            {answerSections.reasoningNotes.map((item) => (
              <p key={item} className="copilot-answer-note">
                {item}
              </p>
            ))}
          </div>
        </div>
      ) : null}

      {evidenceTables.length > 0 ? (
        <div className="copilot-answer-section">
          <div className="copilot-answer-section-head">
            <strong>数据依据</strong>
          </div>
          <div className="copilot-grounded-answer-tables">
            {evidenceTables.map((table) => (
              <div key={table.title} className="copilot-grounded-answer-table-wrap">
                <div className="copilot-grounded-answer-table-title">{table.title}</div>
                <div className="copilot-grounded-answer-scroll">
                  <table className="copilot-grounded-answer-table">
                    <thead>
                      <tr>
                        {table.columns.map((column) => (
                          <th key={column}>{column}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {table.rows.map((row, rowIndex) => (
                        <tr key={`${table.title}-${rowIndex}`}>
                          {row.map((cell, cellIndex) => (
                            <td key={`${table.title}-${rowIndex}-${cellIndex}`}>{cell}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {layers.length > 0 ? (
        <div className="copilot-answer-section">
          <div className="copilot-answer-section-head">
            <strong>数据来源层</strong>
          </div>
          <div className="copilot-grounded-answer-layers">
            {layers.map((layer) => (
              <div key={`${layer.kind}-${layer.label}`} className="copilot-grounded-answer-layer">
                <div className="copilot-grounded-answer-layer-head">
                  <span>{layer.label}</span>
                  {layer.freshness ? (
                    <span>{formatFreshness(layer.freshness)}</span>
                  ) : null}
                </div>
                <div className="copilot-grounded-answer-layer-body">
                  {layer.detail}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
