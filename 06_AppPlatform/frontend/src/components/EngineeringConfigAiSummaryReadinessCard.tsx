import type { ReactElement } from "react";
import type { EngineeringConfigBusinessSummaryReadiness } from "../types/engineeringConfig";

type AiSummaryReadinessVariant = "drawer" | "sourceUpload";
type AiSummaryReadinessTone = "ready" | "blocked" | "unknown";
type AiSummaryReadinessMetric = {
  label: string;
  value: string;
  active: boolean;
};

type EngineeringConfigAiSummaryReadinessCardProps = {
  ariaLabel?: string;
  readiness: EngineeringConfigBusinessSummaryReadiness | null;
  error: string | null;
  variant: AiSummaryReadinessVariant;
};

function readinessIsReady(readiness: EngineeringConfigBusinessSummaryReadiness | null): boolean {
  return Boolean(readiness?.ready || readiness?.status === "ready");
}

function aiSummaryReadinessTone(readiness: EngineeringConfigBusinessSummaryReadiness | null): AiSummaryReadinessTone {
  if (!readiness) return "unknown";
  return readinessIsReady(readiness) ? "ready" : "blocked";
}

function aiSummaryReadinessTitle(
  readiness: EngineeringConfigBusinessSummaryReadiness | null,
  error: string | null,
  variant: AiSummaryReadinessVariant,
): string {
  if (error) return variant === "sourceUpload" ? "AI 摘要状态暂不可用" : "AI 摘要状态读取失败";
  if (!readiness) return "正在检查 AI 摘要";
  if (readinessIsReady(readiness)) return variant === "sourceUpload" ? "AI 摘要 runtime 已就绪" : "AI 摘要可用";
  if (readiness.status === "missing_key") return "AI 摘要未配置";
  return "AI 摘要受限";
}

function aiSummaryReadinessDescription(
  readiness: EngineeringConfigBusinessSummaryReadiness | null,
  error: string | null,
  variant: AiSummaryReadinessVariant,
): string {
  if (error) {
    return variant === "sourceUpload"
      ? "上传、Source Digest 建列、在线编辑和导出仍可使用；AI 摘要稍后回到配置对比页再检查。"
      : "配置表、来源证据和导出仍可使用；AI 结论暂时不作为当前页面依赖。";
  }
  if (!readiness) {
    return variant === "sourceUpload"
      ? "正在读取 provider 状态；这里只检查配置，不会触发 LLM 调用。"
      : "正在读取后端 provider 配置；不会触发真实 LLM 调用。";
  }
  if (readinessIsReady(readiness)) {
    return variant === "sourceUpload"
      ? "上传只保存来源文件和配置事实；AI 摘要在配置对比页按当前已选配置列实时生成。"
      : "当前对比事实会在页面内实时生成 AI 结论；readiness 只检查配置，不代表 provider 网络调用一定成功。";
  }
  if (readiness.status === "missing_key") {
    return variant === "sourceUpload"
      ? "缺少 LLM provider key；上传来源和创建可编辑配置列不受影响。"
      : "缺少 LLM provider key；用户仍可查看完整配置表、来源证据和导出。";
  }
  return readiness.message || "AI 摘要当前不可用；配置事实层不受影响。";
}

function compactReadinessUrl(value: string | null | undefined): string {
  if (!value) return "未配置";
  return value.replace(/^https?:\/\//, "").replace(/\/$/, "");
}

function runtimeReadinessValue(readiness: EngineeringConfigBusinessSummaryReadiness): string {
  if (readiness.runtimeUsed) return readiness.runtimeStatus || "使用中";
  if (readiness.runtimeUrl) return "未参与摘要";
  return readiness.runtimeStatus || "未配置";
}

function aiSummaryReadinessMetricItems(
  readiness: EngineeringConfigBusinessSummaryReadiness | null,
  variant: AiSummaryReadinessVariant,
): AiSummaryReadinessMetric[] {
  if (!readiness) {
    return variant === "sourceUpload"
      ? [
        { label: "Provider", value: "检查中", active: false },
        { label: "生成阶段", value: "Runtime", active: false },
        { label: "Digest", value: "非持久摘要", active: false },
      ]
      : [
        { label: "Provider", value: "检查中", active: false },
        { label: "生成阶段", value: "Runtime", active: false },
        { label: "持久化", value: "非 Digest", active: false },
      ];
  }
  return [
    { label: "Provider", value: readiness.provider || "未配置", active: readiness.ready },
    { label: "Model", value: readiness.model || "待配置", active: readiness.ready },
    { label: "API Base", value: compactReadinessUrl(readiness.apiBase), active: readiness.ready },
    { label: "Runtime", value: runtimeReadinessValue(readiness), active: Boolean(readiness.runtimeUsed) },
    { label: "缓存", value: `${readiness.cacheSize}/${readiness.cacheLimit}`, active: readiness.cacheSize > 0 },
    variant === "sourceUpload"
      ? { label: "Digest", value: readiness.persisted ? "持久摘要" : "非持久摘要", active: !readiness.persisted }
      : { label: "持久化", value: readiness.persisted ? "Digest" : "Runtime", active: !readiness.persisted },
  ];
}

function renderKeySourceNote(readiness: EngineeringConfigBusinessSummaryReadiness | null, className?: string): ReactElement | null {
  if (!readiness?.keySource) return null;
  const runtimeNote = readiness.runtimeUsed
    ? `Runtime：${compactReadinessUrl(readiness.runtimeUrl)}。`
    : "Compare 摘要复用 AstrBot provider 配置，但不走本地 AstrBot runtime。";
  return <small className={className}>Key：{readiness.keySource}；{runtimeNote} 不是 Source Digest pipeline 持久摘要。</small>;
}

export function EngineeringConfigAiSummaryReadinessCard({
  ariaLabel,
  readiness,
  error,
  variant,
}: EngineeringConfigAiSummaryReadinessCardProps): ReactElement {
  const metrics = aiSummaryReadinessMetricItems(readiness, variant);
  const title = aiSummaryReadinessTitle(readiness, error, variant);
  const description = aiSummaryReadinessDescription(readiness, error, variant);

  if (variant === "sourceUpload") {
    const tone = error ? "blocked" : aiSummaryReadinessTone(readiness);
    return (
      <section className={`config-source-ocr-readiness config-source-ocr-readiness--${tone}`} aria-label={ariaLabel ?? "AI 摘要运行边界"}>
        <div className="config-source-ocr-readiness__copy">
          <span className="market-scan-panel-eyebrow">AI 摘要运行边界</span>
          <strong>{title}</strong>
          <small>{description}</small>
        </div>
        <div className="config-source-ocr-readiness__metrics">
          {metrics.map((item) => (
            <span
              className={`config-source-ocr-readiness__metric ${item.active ? "is-active" : ""}`.trim()}
              key={item.label}
            >
              {item.label}<strong>{item.value}</strong>
            </span>
          ))}
        </div>
        {readiness?.keySource ? (
          <div className="config-source-ocr-readiness__warnings">
            {renderKeySourceNote(readiness)}
          </div>
        ) : null}
      </section>
    );
  }

  return (
    <div className="market-scan-field deck-panel-grid__wide comparison-drawer-view-mode" aria-label={ariaLabel ?? "AI 摘要运行状态"}>
      <span>AI 摘要</span>
      <div className="comparison-drawer-view-status">
        <strong>{title}</strong>
        <small>{description}</small>
        <div className="product-config-drawer-scope__chips" aria-label="AI 摘要 provider 和缓存状态">
          {metrics.map((item) => (
            <span className={`product-config-drawer-scope__chip ${item.active ? "is-active" : ""}`.trim()} key={item.label}>
              <small>{item.label}</small>
              <strong>{item.value}</strong>
            </span>
          ))}
        </div>
        {renderKeySourceNote(readiness, "market-scan-field-hint")}
      </div>
    </div>
  );
}
