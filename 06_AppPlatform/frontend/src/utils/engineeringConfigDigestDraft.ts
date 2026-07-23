import type { EngineeringConfigDigestDraftResult } from "../types/engineeringConfig";

export function formatEngineeringConfigDigestDraftMetrics(result: EngineeringConfigDigestDraftResult): string {
  return [
    `${result.trimCount} 配置列（新建 ${result.createdTrimCount}，复用 ${result.reusedTrimCount}）`,
    `${result.featureCount} 配置项（新建 ${result.createdFeatureCount}，复用 ${result.reusedFeatureCount}）`,
    `写入 ${result.valueRecordCount} 条值（新增 ${result.insertedValueCount}，更新 ${result.updatedValueCount}）`,
  ].join(" · ");
}

export function formatEngineeringConfigDigestDraftFeedback(
  modelName: string,
  result: EngineeringConfigDigestDraftResult,
  selectedTrimCount?: number,
): string {
  const actionLabel = typeof selectedTrimCount === "number"
    ? `已按 ${result.trimCount} 个配置列创建为可编辑配置列`
    : "已创建为可编辑配置列";
  return `${modelName} ${actionLabel}：${formatEngineeringConfigDigestDraftMetrics(result)}。`;
}
