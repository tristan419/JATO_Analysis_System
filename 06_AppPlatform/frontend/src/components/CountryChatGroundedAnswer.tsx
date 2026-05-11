import type { ReactNode } from "react";

import type { CountryChatTranscriptMessage } from "../contexts/CountryChatContext";
import type { CountryChatModelUsage } from "../types/countryChat";
import {
  buildCountryChatAnswerPath,
  buildCountryChatAnswerSections,
} from "../contexts/countryChatHelpers";

const DEEPSEEK_INPUT_PRICE_PER_1M = 1.0;
const DEEPSEEK_CACHE_HIT_PRICE_PER_1M = 0.2;
const DEEPSEEK_OUTPUT_PRICE_PER_1M = 2.0;

function calcTokenCostRMB(usage: CountryChatModelUsage): {
  inputCost: number;
  cacheHitCost: number;
  outputCost: number;
  totalCost: number;
} {
  const cacheHit = usage.promptCacheHitTokens ?? 0;
  const cacheMiss = (usage.promptTokens ?? 0) - cacheHit;
  const inputCost = (cacheMiss / 1_000_000) * DEEPSEEK_INPUT_PRICE_PER_1M;
  const cacheHitCost = (cacheHit / 1_000_000) * DEEPSEEK_CACHE_HIT_PRICE_PER_1M;
  const outputCost = ((usage.completionTokens ?? 0) / 1_000_000) * DEEPSEEK_OUTPUT_PRICE_PER_1M;
  return {
    inputCost,
    cacheHitCost,
    outputCost,
    totalCost: inputCost + cacheHitCost + outputCost,
  };
}

function TokenCostSummary({ usage }: { usage: CountryChatModelUsage }) {
  const cost = calcTokenCostRMB(usage);
  return (
    <span className="copilot-answer-section-kicker" style={{ marginLeft: 8 }}>
      {usage.totalTokens?.toLocaleString()} tokens · ¥{cost.totalCost.toFixed(4)}
    </span>
  );
}

function TokenCostDetail({ usage }: { usage: CountryChatModelUsage }) {
  const cost = calcTokenCostRMB(usage);
  const cacheHit = usage.promptCacheHitTokens ?? 0;
  const cacheMiss = (usage.promptTokens ?? 0) - cacheHit;
  return (
    <div style={{ padding: "6px 0", fontSize: 12, color: "#64748b", lineHeight: 1.8 }}>
      <div>
        Prompt: {usage.promptTokens?.toLocaleString() ?? 0} tokens
        {cacheHit > 0 ? ` (cache hit ${cacheHit.toLocaleString()})` : ""}
        <span style={{ marginLeft: 8 }}>¥{cost.inputCost.toFixed(4)}</span>
        {cost.cacheHitCost > 0 ? <span style={{ marginLeft: 4 }}>+ cache ¥{cost.cacheHitCost.toFixed(4)}</span> : null}
      </div>
      <div>
        Completion: {usage.completionTokens?.toLocaleString() ?? 0} tokens
        <span style={{ marginLeft: 8 }}>¥{cost.outputCost.toFixed(4)}</span>
      </div>
      <div style={{ fontWeight: 600, color: "#334155", marginTop: 2 }}>
        合计: {usage.totalTokens?.toLocaleString() ?? 0} tokens · ¥{cost.totalCost.toFixed(4)}
      </div>
    </div>
  );
}

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
      return "证据回答";
    default:
      return "综合回答";
  }
}

function confidenceLabel(value: string | null | undefined): string {
  switch (value) {
    case "high":
      return "高可信";
    case "medium":
      return "中可信";
    case "low":
      return "低可信";
    default:
      return "可信度";
  }
}

function sufficiencyLabel(value: string | null | undefined): string {
  switch (value) {
    case "strong":
      return "证据充分";
    case "partial":
      return "证据部分充分";
    case "thin":
      return "证据偏薄";
    default:
      return "证据状态";
  }
}

function sourceStatusLabel(value: string | null | undefined): string {
  switch (value) {
    case "ready":
      return "已就绪";
    case "prefetched":
      return "已预取";
    case "prefetch":
      return "待预取";
    case "planned":
      return "待补充";
    default:
      return String(value ?? "").trim() || "未知";
  }
}

function isHttpUrl(value: string): boolean {
  return /^https?:\/\/\S+$/i.test(value.trim());
}

function renderEvidenceCell(cell: string, column: string): ReactNode {
  const text = String(cell ?? "").trim();
  if (!text) {
    return "-";
  }
  if (isHttpUrl(text)) {
    return (
      <a
        className="copilot-evidence-link"
        href={text}
        target="_blank"
        rel="noreferrer"
      >
        {column === "链接" ? "打开来源" : text}
      </a>
    );
  }
  return cell;
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
  const fallbackAnswerPath = buildCountryChatAnswerPath({
    country: message.country ?? message.contextSnapshot?.country,
    intentRoute: message.intentRoute,
    answerMode: message.answerMode,
    layers: grounding?.layers,
    extractedParams: message.extractedParams,
  });
  const answerPath = grounding?.answerPath?.steps?.length
    ? {
      ...fallbackAnswerPath,
      steps: grounding.answerPath.steps,
    }
    : fallbackAnswerPath;
  const answerSections = buildCountryChatAnswerSections({
    content: message.content,
    summary: grounding?.summary,
    reasoningNotes: grounding?.reasoningNotes,
  });
  const keyFindings = grounding?.keyFindings ?? [];
  const layers = grounding?.layers ?? [];
  const trust = grounding?.trust;
  const executionPlan = message.executionPlan;
  const executionSources = compact
    ? executionPlan?.sourcePlan?.slice(0, 4) ?? []
    : executionPlan?.sourcePlan ?? [];
  const prefetchedToolNames = executionPlan?.prefetchedToolNames ?? [];
  const allowedToolNames = executionPlan?.allowedToolNames ?? [];
  const showAnswerPath = isAssistant && (
    answerPath.focusTags.length > 0
    || answerPath.steps.length > 0
    || Boolean(grounding?.answerPath?.routeTrigger)
    || Boolean(message.intentRoute)
    || Boolean(message.answerMode)
    || layers.length > 0
  );
  const strategyLabel = grounding?.strategyLabel ?? answerPath.routeLabel;
  const visibleSteps = Array.from(
    new Set(answerPath.steps.map((step) => step.trim()).filter(Boolean)),
  );

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
            <strong>回答路径</strong>
            <span className="copilot-answer-section-kicker">Evidence path</span>
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
          {grounding?.answerPath?.routeTrigger ? (
            <p className="copilot-answer-note">{grounding.answerPath.routeTrigger}</p>
          ) : null}
          <ol className="copilot-answer-path-steps">
            {visibleSteps.map((step) => (
              <li key={step}>{step}</li>
            ))}
          </ol>
        </div>
      ) : null}

      {trust ? (
        <details className="copilot-answer-section is-collapsible">
          <summary className="copilot-answer-section-head">
            <strong>可信度</strong>
            <span className="copilot-answer-section-kicker">Trust layer</span>
          </summary>
          <div className="copilot-answer-path-head">
            <span className="copilot-answer-path-pill">
              {confidenceLabel(trust.confidence)}
            </span>
            <span className="copilot-answer-path-pill is-muted">
              {sufficiencyLabel(trust.evidenceSufficiency)}
            </span>
            <span className="copilot-answer-path-pill is-soft">
              证据分 {trust.evidenceScore}
            </span>
          </div>
          {trust.sourceCoverage ? (
            <div className="copilot-answer-note">
              必需来源命中 {trust.sourceCoverage.requiredReady}/{trust.sourceCoverage.requiredTotal}
              ，预取 {trust.sourceCoverage.prefetchedCount} 个来源。
            </div>
          ) : null}
          {trust.routeRationale ? (
            <p className="copilot-answer-note">{trust.routeRationale}</p>
          ) : null}
          {trust.missingFacts.length > 0 ? (
            <ul className="copilot-trust-missing-list">
              {trust.missingFacts.slice(0, compact ? 2 : 3).map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          ) : null}
        </details>
      ) : null}

      {executionSources.length > 0 ? (
        <details className="copilot-answer-section is-collapsible">
          <summary className="copilot-answer-section-head">
            <strong>执行计划</strong>
            <span className="copilot-answer-section-kicker">Planner</span>
          </summary>
          <div className="copilot-answer-path-head">
            {executionPlan?.orchestrationMode ? (
              <span className="copilot-answer-path-pill">
                {executionPlan.orchestrationMode}
              </span>
            ) : null}
            {executionPlan?.answerStrategy ? (
              <span className="copilot-answer-path-pill is-muted">
                {executionPlan.answerStrategy}
              </span>
            ) : null}
            {prefetchedToolNames.length > 0 ? (
              <span className="copilot-answer-path-pill is-soft">
                已预取 {prefetchedToolNames.join(" / ")}
              </span>
            ) : null}
          </div>
          <div className="copilot-plan-grid">
            {executionSources.map((source) => (
              <div
                key={`${source.key}-${source.status}-${source.label ?? ""}`}
                className="copilot-plan-card"
              >
                <div className="copilot-plan-card-head">
                  <strong>{source.label ?? source.key}</strong>
                  <span className={`copilot-plan-status is-${source.status ?? "unknown"}`}>
                    {sourceStatusLabel(source.status)}
                  </span>
                </div>
                <div className="copilot-plan-card-meta">
                  {source.required ? "必需来源" : "补充来源"}
                  {source.toolName ? ` · ${source.toolName}` : ""}
                </div>
                {source.reason ? (
                  <div className="copilot-plan-card-body">{source.reason}</div>
                ) : null}
              </div>
            ))}
          </div>
          {allowedToolNames.length > 0 ? (
            <div className="copilot-answer-focus">
              <span className="copilot-answer-focus-label">允许工具</span>
              <div className="copilot-answer-focus-tags">
                {allowedToolNames.map((toolName) => (
                  <span key={toolName} className="copilot-answer-focus-tag">
                    {toolName}
                  </span>
                ))}
              </div>
            </div>
          ) : null}
        </details>
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
                            <td key={`${table.title}-${rowIndex}-${cellIndex}`}>
                              {renderEvidenceCell(cell, table.columns[cellIndex] ?? "")}
                            </td>
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

      {message.contextSnapshot?.analysisMeta?.modelUsage ? (
        <details className="copilot-answer-section is-collapsible">
          <summary className="copilot-answer-section-head">
            <strong>Token 花费</strong>
            <TokenCostSummary usage={message.contextSnapshot.analysisMeta.modelUsage} />
          </summary>
          <TokenCostDetail usage={message.contextSnapshot.analysisMeta.modelUsage} />
        </details>
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
