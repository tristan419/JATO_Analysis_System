import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import { api } from "../api/client";
import type { CompareResponse, CompareRow, CompareTrimItem } from "../types/engineeringConfig";
import type {
  EngineeringConfigBusinessSummaryComposeRequest,
  EngineeringConfigBusinessSummaryEvidenceRef,
  EngineeringConfigBusinessSummaryItem,
  EngineeringConfigBusinessSummaryUsage,
} from "../types/engineeringConfig";
import {
  buildBusinessDifferenceSummary,
  deltaEvidenceTarget,
  type CategoryDeltaSummary,
  type ConfigDelta,
  type ConfigDeltaType,
  type TrimDeltaSummary,
} from "../utils/configDelta";
import { rowMatchesConfigSearch, type ConfigComparisonDeltaFilter } from "../utils/configComparisonFilters";
import type { SourceEvidenceSelection } from "./SourceEvidenceDrawer";

export type BusinessSummaryMode = "simple" | "expert";

interface BusinessSummaryPanelProps {
  data: CompareResponse;
  baseTrimId: string | null;
  categoryFilter?: string | null;
  deltaFilter?: ConfigComparisonDeltaFilter;
  mode?: BusinessSummaryMode;
  searchValue?: string;
  targetTrimFilterId?: string | null;
  factSource?: EngineeringConfigBusinessSummaryComposeRequest["factSource"];
  onShowDifferenceRows?: () => void;
  onFocusCategory?: (category: string) => void;
  onFocusDeltaType?: (filter: ConfigComparisonDeltaFilter, targetTrimId: string | null) => void;
  onFocusFeatureRow?: (row: CompareRow, targetTrimId: string | null, filter: ConfigComparisonDeltaFilter) => void;
  onFocusTargetTrim?: (trimId: string | null) => void;
  onFocusVersionStep?: (baseTrimId: string, targetTrimId: string, filter: ConfigComparisonDeltaFilter) => void;
  onLlmSummaryChange?: (summaries: EngineeringConfigBusinessSummaryItem[], usage: EngineeringConfigBusinessSummaryUsage | null) => void;
  onOpenSourceContext?: (targetTrimId: string | null) => void;
  onOpenEvidence: (selection: SourceEvidenceSelection) => void;
  llmSummaryEnabled?: boolean;
}

const DELTA_LABELS: Record<ConfigDeltaType, string> = {
  ADDED: "新增",
  REMOVED: "减少",
  SAME: "一致",
  OPTIONAL_CHANGED: "选装变化",
  VALUE_CHANGED: "值变化",
  UNKNOWN: "待确认",
};

const BUSINESS_SECTIONS: Array<{
  key: string;
  title: string;
  emptyLabel: string;
  types: ConfigDeltaType[];
}> = [
  { key: "added", title: "主要增加", emptyLabel: "暂无新增配置", types: ["ADDED"] },
  { key: "removed", title: "主要减少", emptyLabel: "暂无减少配置", types: ["REMOVED"] },
  { key: "changed", title: "值 / 选装变化", emptyLabel: "暂无值或选装变化", types: ["VALUE_CHANGED", "OPTIONAL_CHANGED"] },
  { key: "unknown", title: "待证据确认", emptyLabel: "暂无待确认差异", types: ["UNKNOWN"] },
];

const SECTION_DELTA_LIMIT = 3;
const BUSINESS_FOCUS_GROUP_LIMIT = 4;
const ALL_CONCLUSION_COPY_ID = "__all_conclusion_drafts__";
const LLM_SUMMARY_IDLE_TIMEOUT_MS = 2200;
const LLM_SUMMARY_FALLBACK_DELAY_MS = 650;
const LLM_SUMMARY_CACHE_LIMIT = 24;
const LLM_EVIDENCE_FACT_LIMIT = 40;
const LLM_UPGRADE_SIGNAL_LIMIT = 8;
const LLM_FEATURE_LIST_LIMIT = 16;
const LLM_FOCUS_GROUP_LIMIT = 8;
const LLM_CATEGORY_FACT_LIMIT = 12;

interface LlmBusinessSummaryCacheEntry {
  summaries: EngineeringConfigBusinessSummaryItem[];
  usage: EngineeringConfigBusinessSummaryUsage | null;
}

const llmBusinessSummaryCache = new Map<string, LlmBusinessSummaryCacheEntry>();

export function clearLlmBusinessSummaryCacheForTests(): void {
  llmBusinessSummaryCache.clear();
}

interface BaselineNarrativeItem {
  label: string;
  value: string;
  detail: string;
  categories?: CategoryDeltaSummary[];
}
interface BaseStorylineItem {
  key: string;
  label: string;
  value: string;
  detail: string;
  filter?: ConfigComparisonDeltaFilter;
  sourceTargetTrimId?: string | null;
}
interface SummaryScopeNote {
  label: string;
  detail: string;
}
interface TargetInsightItem {
  key: string;
  label: string;
  value: string;
  filter?: ConfigComparisonDeltaFilter;
}
type TargetContextTone = "neutral" | "ready" | "review" | "warning";
interface TargetContextItem {
  key: string;
  label: string;
  value: string;
  detail: string;
  tone: TargetContextTone;
}
interface TargetConclusionDraft {
  tone: TargetContextTone;
  statusLabel: string;
  title: string;
  body: string;
  evidenceNote: string;
  filter?: ConfigComparisonDeltaFilter;
  action?: "source";
  actionLabel?: string;
}
interface ConclusionCopyFeedback {
  trimId: string;
  message: string;
}
type LlmBusinessSummaryStatus = "idle" | "loading" | "ready" | "error";
type LlmSummarySectionKey = "mainUpgrades" | "replacementsOrReductions" | "evidenceStatus";
const COMPACT_SUMMARY_SECTION_ITEM_LIMIT = 4;
interface LlmSummarySectionRenderOptions {
  includeEvidenceStatus?: boolean;
  includeRecommendedUse?: boolean;
  inlineEvidenceActions?: boolean;
}
interface LlmBusinessSummaryState {
  status: LlmBusinessSummaryStatus;
  summaries: EngineeringConfigBusinessSummaryItem[];
  usage: EngineeringConfigBusinessSummaryUsage | null;
  error: string | null;
  cached: boolean;
}
interface LlmSummaryCopyFeedback {
  tone: "success" | "error";
  message: string;
}
interface LlmSummaryEvidenceHit {
  delta: ConfigDelta;
  selection: SourceEvidenceSelection;
  targetTrimId: string;
}
interface ConclusionStatusSummaryItem {
  key: string;
  label: string;
  count: number;
  tone: TargetContextTone;
  focusAction?: "source";
  focusFilter?: ConfigComparisonDeltaFilter;
  focusTargetTrimId: string;
  targetLabels: string[];
}
interface FocusedTargetQueue {
  currentIndex: number;
  total: number;
  currentLabel: string;
  nextTrimId: string;
  nextLabel: string;
  filterLabel: string;
}
interface TargetActionGuidance {
  tone: "ready" | "review" | "warning" | "neutral";
  label: string;
  title: string;
  detail: string;
  filter?: ConfigComparisonDeltaFilter;
  action?: "source";
  actionLabel?: string;
}
type VersionLadderTone = "base" | "ready" | "review" | "warning" | "neutral";
interface VersionLadderItem {
  key: string;
  role: string;
  trimLabel: string;
  primary: string;
  detail: string;
  evidence: string;
  tone: VersionLadderTone;
  targetTrimId?: string;
  filter?: ConfigComparisonDeltaFilter;
}
interface VersionUpgradeStepItem {
  key: string;
  stepLabel: string;
  fromTrim: CompareTrimItem;
  toTrim: CompareTrimItem;
  primary: string;
  detail: string;
  evidence: string;
  tone: VersionLadderTone;
  filter: ConfigComparisonDeltaFilter;
  actionable: boolean;
}
interface OptionSwapInsight {
  key: string;
  category: string;
  dimensionLabel: string;
  fromFeature: string;
  toFeature: string;
  fromDelta: ConfigDelta;
  toDelta: ConfigDelta;
}
interface BusinessFocusGroup {
  key: string;
  label: string;
  categories: string[];
  deltas: ConfigDelta[];
  filter: ConfigComparisonDeltaFilter;
  tone: TargetContextTone;
  countLabel: string;
  sampleLabel: string;
  evidenceLabel: string;
}
interface SimpleConclusionItem {
  key: string;
  targetTrimId: string;
  targetLabel: string;
  statusLabel: string;
  headline: string;
  detail: string;
  points: SimpleConclusionPoint[];
  evidenceNote: string;
  tone: TargetContextTone;
  filter?: ConfigComparisonDeltaFilter;
  action?: "source";
  actionLabel: string;
}
interface SimpleConclusionPoint {
  key: string;
  label: string;
  value: string;
  tone: TargetContextTone;
}
interface SimpleVersionNarrativeItem {
  key: string;
  targetTrimId: string;
  targetLabel: string;
  comparisonLabel: string;
  headline: string;
  body: string;
  evidenceLabel: string;
  tone: TargetContextTone;
  filter?: ConfigComparisonDeltaFilter;
  actionLabel?: string;
}
interface SimpleScopeBridgeItem {
  key: string;
  label: string;
  value: string;
  detail: string;
  filter?: ConfigComparisonDeltaFilter;
  targetTrimId?: string | null;
  actionLabel?: string;
}
interface ExcelGuideItem {
  key: string;
  label: string;
  value: string;
  detail: string;
  tone: TargetContextTone;
  filter?: ConfigComparisonDeltaFilter;
  targetTrimId?: string | null;
  sourceTargetTrimId?: string | null;
}
type IdleSchedulerWindow = Window & {
  requestIdleCallback?: (callback: () => void, options?: { timeout?: number }) => number;
  cancelIdleCallback?: (handle: number) => void;
};

const OPTION_SWAP_DIMENSIONS: Array<{ key: string; label: string; tokens: string[] }> = [
  { key: "audio", label: "音响系统", tokens: ["audio", "speaker", "speakers", "sound", "sony", "音响", "扬声器", "喇叭"] },
  { key: "steering-wheel", label: "方向盘配置", tokens: ["steering wheel", "方向盘"] },
  { key: "wheel", label: "轮胎 / 轮毂", tokens: ["wheel", "wheels", "rim", "rims", "tyre", "tire", "inch", "轮胎", "轮毂"] },
  { key: "seat-material", label: "座椅面料", tokens: ["seat material", "leather", "farbic", "fabric", "artificial leather", "座椅面料", "仿皮", "皮纺", "皮革", "织物"] },
  { key: "seat-adjustment", label: "座椅调节", tokens: ["seat adjustment", "manual adjustment", "power adjustment", "way adjustment", "way manual", "way power", "向调节", "手动调节", "电动调节"] },
  { key: "seat-comfort", label: "座椅舒适", tokens: ["seat heating", "seat ventilation", "heated seat", "ventilated seat", "座椅加热", "座椅通风"] },
  { key: "screen", label: "屏幕 / 显示", tokens: ["screen", "display", "cluster", "infotainment", "显示屏", "屏幕", "仪表"] },
  { key: "parking", label: "泊车辅助", tokens: ["parking", "park", "camera", "view", "摄像头", "影像", "泊车", "倒车", "环视"] },
  { key: "lighting", label: "灯光配置", tokens: ["lamp", "light", "led", "headlight", "灯", "大灯"] },
];
const BUSINESS_FOCUS_DIMENSIONS: Array<{ key: string; label: string; tokens: string[] }> = [
  { key: "drive-assist", label: "驾驶辅助", tokens: ["drive assist", "adas", "驾驶辅助", "智能驾驶", "辅助驾驶"] },
  { key: "comfort", label: "舒适便利", tokens: ["comfort", "convenient", "convenience", "舒适", "便利", "尾门", "空调"] },
  { key: "infotainment", label: "信息娱乐", tokens: ["information", "entertainment", "infotainment", "信息娱乐", "娱乐", "互联"] },
  { key: "safety", label: "安全配置", tokens: ["safety", "airbag", "安全", "气囊"] },
  { key: "exterior", label: "外饰配置", tokens: ["exterior", "外饰", "车顶", "天窗", "后视镜"] },
  { key: "interior", label: "内饰配置", tokens: ["interior", "内饰", "方向盘", "氛围灯"] },
  { key: "powertrain", label: "动力性能", tokens: ["powertrain", "engine", "motor", "battery", "performance", "动力", "发动机", "电机", "电池", "性能"] },
  { key: "dimension", label: "尺寸参数", tokens: ["dimension", "length", "width", "height", "wheelbase", "尺寸", "长度", "宽度", "高度", "轴距"] },
];

function trimLabel(trim: CompareTrimItem): string {
  return trim.trimName || trim.fullTrimName || trim.trimId;
}

function countText(label: string, count: number): string | null {
  return count > 0 ? `${label} ${count} 项` : null;
}

function deltaCountText(summary: TrimDeltaSummary): string {
  return [
    countText("新增", summary.addedCount),
    countText("减少", summary.removedCount),
    countText("值变化", summary.valueChangedCount),
    countText("选装变化", summary.optionalCount),
    countText("待确认", summary.unknownCount),
  ].filter((text): text is string => Boolean(text)).join("，") || "暂无业务差异";
}

function deltaEvidenceLabel(delta: ConfigDelta, trim: CompareTrimItem): string {
  const inferredText = delta.inferred ? "推断" : "";
  return `查看 ${trimLabel(trim)} ${delta.row.featureName} 的${DELTA_LABELS[delta.deltaType]}${inferredText}来源`;
}

function deltaEvidenceReason(delta: ConfigDelta, trim: CompareTrimItem): string {
  const baseLabel = trimLabel(delta.baseTrim);
  const targetLabel = trimLabel(delta.targetTrim);
  const evidenceLabel = trimLabel(trim);
  if (delta.targetValue?.inferred && trim.trimId === delta.targetTrim.trimId) {
    return `业务摘要优先打开 ${evidenceLabel} 的推断值，用于解释 ${targetLabel} 相比 ${baseLabel} 的${DELTA_LABELS[delta.deltaType]}差异。`;
  }
  if (delta.baseValue?.inferred && trim.trimId === delta.baseTrim.trimId) {
    return `业务摘要优先打开 ${evidenceLabel} 的推断值，用于解释 ${targetLabel} 相比 ${baseLabel} 的${DELTA_LABELS[delta.deltaType]}差异。`;
  }
  if (delta.deltaType === "REMOVED") {
    return `该差异为减少配置，优先打开基准 ${baseLabel} 的原始配置值。`;
  }
  return `该证据来自目标 ${targetLabel} 的配置值，用于解释相对 ${baseLabel} 的${DELTA_LABELS[delta.deltaType]}差异。`;
}

function categoryText(summary: TrimDeltaSummary): string {
  return summary.categorySummaries
    .slice(0, 3)
    .map((category) => `${categoryLabel(category.category)} ${category.totalDifferenceCount} 项`)
    .join(" · ") || "暂无差异大类";
}

function categoryLabel(category: string): string {
  return category.replace(/\s+/g, " ").trim() || "未分类";
}

function firstNonEmptyValue(...values: Array<string | null | undefined>): string | null {
  const value = values.find((item) => item && item.trim());
  return value ? value.trim() : null;
}

function normalizedComparableValue(value: string | null): string | null {
  if (!value) return null;
  return value.replace(/\s+/g, " ").trim().toLowerCase() || null;
}

function compareText(baseValue: string | null, targetValue: string | null): string {
  if (!baseValue && !targetValue) return "均待补";
  if (!baseValue) return `基准待补 → ${targetValue}`;
  if (!targetValue) return `${baseValue} → 目标待补`;
  if (normalizedComparableValue(baseValue) === normalizedComparableValue(targetValue)) return baseValue;
  return `${baseValue} → ${targetValue}`;
}

function valuesHaveSameMeaning(baseValue: string | null, targetValue: string | null): boolean {
  const baseNormalized = normalizedComparableValue(baseValue);
  const targetNormalized = normalizedComparableValue(targetValue);
  return Boolean(baseNormalized && targetNormalized && baseNormalized === targetNormalized);
}

function scenarioCountText(label: string, count: number): string | null {
  return count > 0 ? `${label} ${count}` : null;
}

function isOwnProductTrim(trim: CompareTrimItem): boolean {
  return trim.dataOrigin === "own_catalog" || Boolean(trim.hasMaterialNo || trim.materialNo);
}

function trimOriginLabel(trim: CompareTrimItem): string {
  if (isOwnProductTrim(trim)) return "本品";
  if (trim.dataOrigin === "external_or_scraped") return "竞品 / 外部";
  return "身份待补";
}

function trimMaterialAnchor(trim: CompareTrimItem): string {
  const materialNo = firstNonEmptyValue(trim.materialNo);
  if (materialNo) return `物料号 ${materialNo}`;
  const salesVersion = firstNonEmptyValue(trim.salesVersion, trim.profile?.configurationVersion, trim.profile?.variantVersion);
  if (salesVersion) return `Sales version ${salesVersion}`;
  const identityKey = firstNonEmptyValue(trim.identityKey, trim.vehicleCode, trim.profile?.familyIdentifier);
  if (identityKey) return `身份键 ${identityKey}`;
  return "无物料号 / sales version";
}

function trimMarketValue(trim: CompareTrimItem): string | null {
  return firstNonEmptyValue(trim.market, trim.country, trim.profile?.country);
}

function trimModelYearValue(trim: CompareTrimItem): string | null {
  return firstNonEmptyValue(trim.modelYear);
}

function trimSourceName(trim: CompareTrimItem): string | null {
  return firstNonEmptyValue(trim.sourceFileName, trim.sourceFilePath, trim.sourceUploadId);
}

function targetContextItems(baseTrim: CompareTrimItem, targetTrim: CompareTrimItem): TargetContextItem[] {
  const baseOrigin = trimOriginLabel(baseTrim);
  const targetOrigin = trimOriginLabel(targetTrim);
  const baseMarket = trimMarketValue(baseTrim);
  const targetMarket = trimMarketValue(targetTrim);
  const baseModelYear = trimModelYearValue(baseTrim);
  const targetModelYear = trimModelYearValue(targetTrim);
  const baseSource = trimSourceName(baseTrim);
  const targetSource = trimSourceName(targetTrim);
  const marketSame = valuesHaveSameMeaning(baseMarket, targetMarket);
  const modelYearSame = valuesHaveSameMeaning(baseModelYear, targetModelYear);
  const sourceSame = valuesHaveSameMeaning(baseSource, targetSource);

  const identityTone: TargetContextTone = isOwnProductTrim(baseTrim) && isOwnProductTrim(targetTrim)
    ? "ready"
    : targetOrigin === "身份待补" || baseOrigin === "身份待补"
      ? "warning"
      : "review";
  const marketTone: TargetContextTone = !baseMarket || !targetMarket || !baseModelYear || !targetModelYear
    ? "warning"
    : marketSame && modelYearSame
      ? "ready"
      : "review";
  const sourceTone: TargetContextTone = !baseSource || !targetSource
    ? "warning"
    : sourceSame
      ? "ready"
      : "review";

  return [
    {
      key: "identity",
      label: "对比身份",
      value: `${baseOrigin} → ${targetOrigin}`,
      detail: `基准 ${trimMaterialAnchor(baseTrim)}；目标 ${trimMaterialAnchor(targetTrim)}。`,
      tone: identityTone,
    },
    {
      key: "market-year",
      label: "市场 / 年款",
      value: [
        marketSame ? "同市场" : !baseMarket || !targetMarket ? "市场待补" : "跨市场",
        modelYearSame ? "同年款" : !baseModelYear || !targetModelYear ? "年款待补" : "跨年款",
      ].join(" · "),
      detail: `市场 ${compareText(baseMarket, targetMarket)}；年款 ${compareText(baseModelYear, targetModelYear)}。`,
      tone: marketTone,
    },
    {
      key: "source",
      label: "来源口径",
      value: sourceSame ? "同来源" : !baseSource || !targetSource ? "来源待补" : "跨来源",
      detail: `来源 ${compareText(baseSource, targetSource)}。`,
      tone: sourceTone,
    },
  ];
}

function targetContextActionGuard(baseTrim: CompareTrimItem, targetTrim: CompareTrimItem): TargetActionGuidance | null {
  const baseSource = trimSourceName(baseTrim);
  const targetSource = trimSourceName(targetTrim);
  const baseMarket = trimMarketValue(baseTrim);
  const targetMarket = trimMarketValue(targetTrim);
  const baseModelYear = trimModelYearValue(baseTrim);
  const targetModelYear = trimModelYearValue(targetTrim);
  const sourceSame = valuesHaveSameMeaning(baseSource, targetSource);
  const marketSame = valuesHaveSameMeaning(baseMarket, targetMarket);
  const modelYearSame = valuesHaveSameMeaning(baseModelYear, targetModelYear);
  const targetOrigin = trimOriginLabel(targetTrim);

  if (!baseSource || !targetSource) {
    return {
      tone: "warning",
      label: "补来源口径",
      title: "来源不足，先别转结论",
      detail: "基准列或目标配置列缺少来源文件，建议先关联来源文件快照，再输出确定业务话术。",
      action: "source",
      actionLabel: "打开来源入口",
    };
  }
  if (!baseMarket || !targetMarket) {
    return {
      tone: "warning",
      label: "补市场口径",
      title: "市场信息不完整",
      detail: "基准列或目标配置列缺少市场 / 国家字段，容易把不同市场配置误读为同口径差异。",
      action: "source",
      actionLabel: "补来源口径",
    };
  }
  if (!baseModelYear || !targetModelYear) {
    return {
      tone: "review",
      label: "核对年款",
      title: "确认年款 / 改款口径",
      detail: "年款或改款字段不完整，业务结论需要说明当前差异可能混入换代或改款影响。",
      action: "source",
      actionLabel: "补来源口径",
    };
  }
  if (!marketSame) {
    return {
      tone: "review",
      label: "跨市场",
      title: "按市场口径解释",
      detail: `当前是 ${baseMarket} 与 ${targetMarket} 对比，配置差异应保留市场前提，不宜直接当作版本高低差。`,
    };
  }
  if (!modelYearSame) {
    return {
      tone: "review",
      label: "跨年款",
      title: "按年款 / 改款解释",
      detail: `当前是 ${baseModelYear} 与 ${targetModelYear} 对比，配置差异可能来自改款或换代。`,
    };
  }
  if (!sourceSame) {
    return {
      tone: "review",
      label: "跨来源",
      title: "先核对来源一致性",
      detail: "同国家同车型在不同网站或文件中的配置可能不一致，结论需要保留来源口径。",
      action: "source",
      actionLabel: "打开来源入口",
    };
  }
  if (targetOrigin === "竞品 / 外部") {
    return {
      tone: "review",
      label: "竞品口径",
      title: "保留竞品来源前提",
      detail: "竞品或网上抓取数据通常没有物料号，转业务结论时应引用 sales version 和来源文件。",
      action: "source",
      actionLabel: "打开来源入口",
    };
  }
  return null;
}

function comparisonScenarioStorylineItem(baseTrim: CompareTrimItem, targetSummaries: TrimDeltaSummary[]): BaseStorylineItem {
  const targetTrims = targetSummaries.map((summary) => summary.targetTrim);
  const baseOrigin = trimOriginLabel(baseTrim);
  const ownTargetCount = targetTrims.filter((trim) => isOwnProductTrim(trim)).length;
  const externalTargetCount = targetTrims.filter((trim) => trimOriginLabel(trim) === "竞品 / 外部").length;
  const unknownIdentityCount = targetTrims.filter((trim) => trimOriginLabel(trim) === "身份待补").length;
  const baseMarket = trimMarketValue(baseTrim);
  const baseModelYear = trimModelYearValue(baseTrim);
  const baseSource = trimSourceName(baseTrim);

  const crossMarketCount = targetTrims.filter((trim) => {
    const targetMarket = trimMarketValue(trim);
    return Boolean(baseMarket && targetMarket && !valuesHaveSameMeaning(baseMarket, targetMarket));
  }).length;
  const missingMarketCount = targetTrims.filter((trim) => !baseMarket || !trimMarketValue(trim)).length;
  const crossModelYearCount = targetTrims.filter((trim) => {
    const targetModelYear = trimModelYearValue(trim);
    return Boolean(baseModelYear && targetModelYear && !valuesHaveSameMeaning(baseModelYear, targetModelYear));
  }).length;
  const missingModelYearCount = targetTrims.filter((trim) => !baseModelYear || !trimModelYearValue(trim)).length;
  const crossSourceCount = targetTrims.filter((trim) => {
    const targetSource = trimSourceName(trim);
    return Boolean(baseSource && targetSource && !valuesHaveSameMeaning(baseSource, targetSource));
  }).length;
  const missingSourceCount = targetTrims.filter((trim) => !baseSource || !trimSourceName(trim)).length;
  const sourceIssueTarget = targetTrims.find((trim) => {
    const targetSource = trimSourceName(trim);
    return !baseSource || !targetSource || !valuesHaveSameMeaning(baseSource, targetSource);
  });

  const scenarioValue = (() => {
    if (baseOrigin === "身份待补" || unknownIdentityCount > 0) return "身份待补";
    if (isOwnProductTrim(baseTrim) && ownTargetCount > 0 && externalTargetCount > 0) return "混合来源配置列";
    if (isOwnProductTrim(baseTrim) && externalTargetCount > 0) return "本品与外部配置列";
    if (isOwnProductTrim(baseTrim) && ownTargetCount === targetTrims.length) return "本品配置列";
    if (!isOwnProductTrim(baseTrim) && externalTargetCount === targetTrims.length) return "外部来源配置列";
    return "混合来源配置列";
  })();

  const riskParts = [
    scenarioCountText("竞品 / 外部", externalTargetCount),
    scenarioCountText("身份待补", unknownIdentityCount + (baseOrigin === "身份待补" ? 1 : 0)),
    scenarioCountText("跨市场", crossMarketCount),
    scenarioCountText("市场待补", missingMarketCount),
    scenarioCountText("跨年款", crossModelYearCount),
    scenarioCountText("年款待补", missingModelYearCount),
    scenarioCountText("跨来源", crossSourceCount),
    scenarioCountText("来源待补", missingSourceCount),
  ].filter((text): text is string => Boolean(text));
  const detail = riskParts.length > 0
    ? `目标 ${targetTrims.length} 个；${riskParts.join("，")}。无需先选本品 / 竞品模式；业务结论只需保留身份、市场、年款或来源前提。`
    : `目标 ${targetTrims.length} 个；身份、市场、年款和来源口径一致，可直接作为同车型配置层级对比。`;

  return {
    key: "scenario",
    label: "配置列口径",
    value: scenarioValue,
    detail,
    sourceTargetTrimId: sourceIssueTarget?.trimId,
  };
}

function scenarioGuideTone(item: BaseStorylineItem): TargetContextTone {
  if (item.value.includes("待补") || item.detail.includes("待补")) return "warning";
  if (item.value === "本品配置列" && !item.detail.includes("跨")) return "ready";
  if (item.value.includes("外部") || item.value.includes("混合") || item.detail.includes("跨")) return "review";
  return "neutral";
}

function categoryDeltaDetail(category: CategoryDeltaSummary): string {
  return [
    category.addedCount > 0 ? `新增 ${category.addedCount}` : null,
    category.removedCount > 0 ? `减少 ${category.removedCount}` : null,
    category.valueChangedCount > 0 ? `值变化 ${category.valueChangedCount}` : null,
    category.optionalCount > 0 ? `选装变化 ${category.optionalCount}` : null,
    category.unknownCount > 0 ? `待确认 ${category.unknownCount}` : null,
    category.inferredCount > 0 ? `推断 ${category.inferredCount}` : null,
  ].filter((text): text is string => Boolean(text)).join(" · ") || `${category.totalDifferenceCount} 项差异`;
}

function normalizedFeatureText(value: string): string {
  return value.replace(/\s+/g, " ").trim().toLowerCase();
}

function featureDisplayName(delta: ConfigDelta): string {
  return delta.row.featureName.replace(/\s+/g, " ").trim();
}

function optionSwapDimension(delta: ConfigDelta): { key: string; label: string } | null {
  const text = normalizedFeatureText(`${delta.row.category} ${delta.row.featureName} ${delta.row.featureCode}`);
  const dimension = OPTION_SWAP_DIMENSIONS.find((item) => (
    item.tokens.some((token) => text.includes(token.toLowerCase()))
  ));
  return dimension ? { key: dimension.key, label: dimension.label } : null;
}

function stableBusinessKey(value: string): string {
  return normalizedFeatureText(value).replace(/[^a-z0-9\u4e00-\u9fa5]+/gi, "-").replace(/^-+|-+$/g, "") || "uncategorized";
}

function businessFocusDimension(delta: ConfigDelta): { key: string; label: string } {
  const optionDimension = optionSwapDimension(delta);
  if (optionDimension) return optionDimension;
  const text = normalizedFeatureText(`${delta.row.category} ${delta.row.featureName} ${delta.row.featureCode}`);
  const focusDimension = BUSINESS_FOCUS_DIMENSIONS.find((item) => (
    item.tokens.some((token) => text.includes(token.toLowerCase()))
  ));
  if (focusDimension) return { key: focusDimension.key, label: focusDimension.label };
  return { key: `category-${stableBusinessKey(delta.row.category)}`, label: categoryLabel(delta.row.category) };
}

function focusFilterForDeltas(deltas: ConfigDelta[]): ConfigComparisonDeltaFilter {
  const deltaTypes = new Set(deltas.map((delta) => delta.deltaType));
  if (deltaTypes.size !== 1) return "DIFFERENCE";
  const [deltaType] = Array.from(deltaTypes);
  if (deltaType === "ADDED") return "ADDED";
  if (deltaType === "REMOVED") return "REMOVED";
  if (deltaType === "VALUE_CHANGED") return "VALUE_CHANGED";
  if (deltaType === "OPTIONAL_CHANGED") return "OPTIONAL_CHANGED";
  if (deltaType === "UNKNOWN") return "UNKNOWN";
  return "DIFFERENCE";
}

function businessFocusCountLabel(deltas: ConfigDelta[]): string {
  const addedCount = deltas.filter((delta) => delta.deltaType === "ADDED").length;
  const removedCount = deltas.filter((delta) => delta.deltaType === "REMOVED").length;
  const valueChangedCount = deltas.filter((delta) => delta.deltaType === "VALUE_CHANGED").length;
  const optionalCount = deltas.filter((delta) => delta.deltaType === "OPTIONAL_CHANGED").length;
  const unknownCount = deltas.filter((delta) => delta.deltaType === "UNKNOWN").length;
  return [
    countText("新增", addedCount),
    countText("减少", removedCount),
    countText("值变化", valueChangedCount),
    countText("选装变化", optionalCount),
    countText("待确认", unknownCount),
  ].filter((text): text is string => Boolean(text)).join(" · ") || `${deltas.length} 项差异`;
}

function businessFocusEvidenceLabel(categories: string[], deltas: ConfigDelta[]): string {
  const inferredCount = deltas.filter((delta) => delta.inferred).length;
  const unknownCount = deltas.filter((delta) => delta.deltaType === "UNKNOWN").length;
  const categoryTextValue = categories.length === 1
    ? `来源大类 ${categoryLabel(categories[0])}`
    : `来源 ${categories.length} 个大类`;
  return [
    categoryTextValue,
    inferredCount > 0 ? `推断 ${inferredCount}` : null,
    unknownCount > 0 ? `待确认 ${unknownCount}` : null,
  ].filter((text): text is string => Boolean(text)).join(" · ");
}

function businessFocusTone(deltas: ConfigDelta[]): TargetContextTone {
  if (deltas.some((delta) => delta.deltaType === "UNKNOWN")) return "warning";
  if (deltas.some((delta) => delta.inferred)) return "review";
  return "ready";
}

function businessFocusGroups(summary: TrimDeltaSummary): BusinessFocusGroup[] {
  const groupMap = new Map<string, { label: string; deltas: ConfigDelta[] }>();
  sortedDeltas(summary.deltas).forEach((delta) => {
    if (delta.deltaType === "SAME") return;
    const dimension = businessFocusDimension(delta);
    const existing = groupMap.get(dimension.key) ?? { label: dimension.label, deltas: [] };
    existing.deltas.push(delta);
    groupMap.set(dimension.key, existing);
  });
  return Array.from(groupMap.entries()).map(([key, group]) => {
    const categories = Array.from(new Set(group.deltas.map((delta) => delta.row.category))).sort((a, b) => (
      categoryLabel(a).localeCompare(categoryLabel(b), undefined, { numeric: true, sensitivity: "base" })
    ));
    return {
      key,
      label: group.label,
      categories,
      deltas: group.deltas,
      filter: focusFilterForDeltas(group.deltas),
      tone: businessFocusTone(group.deltas),
      countLabel: businessFocusCountLabel(group.deltas),
      sampleLabel: compactFeatureText(group.deltas, 2),
      evidenceLabel: businessFocusEvidenceLabel(categories, group.deltas),
    };
  }).sort((a, b) => {
    if (b.deltas.length !== a.deltas.length) return b.deltas.length - a.deltas.length;
    const toneRank = { warning: 0, review: 1, ready: 2, neutral: 3 };
    const toneDiff = toneRank[a.tone] - toneRank[b.tone];
    if (toneDiff !== 0) return toneDiff;
    return a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" });
  });
}

function compactBusinessFocusFeatureText(deltas: ConfigDelta[], groupLimit = 1, featureLimit = 2): string {
  const groupedDeltas = new Map<string, { label: string; deltas: ConfigDelta[] }>();
  sortedDeltas(deltas).forEach((delta) => {
    const dimension = businessFocusDimension(delta);
    const existing = groupedDeltas.get(dimension.key) ?? { label: dimension.label, deltas: [] };
    existing.deltas.push(delta);
    groupedDeltas.set(dimension.key, existing);
  });
  const groups = Array.from(groupedDeltas.values()).sort((a, b) => {
    if (b.deltas.length !== a.deltas.length) return b.deltas.length - a.deltas.length;
    return a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" });
  });
  const visibleGroups = groups.slice(0, groupLimit).map((group) => (
    `${group.label}：${compactFeatureText(group.deltas, featureLimit)}`
  ));
  const hiddenGroupText = groups.length > groupLimit ? `；+${groups.length - groupLimit} 个维度` : "";
  return `${deltas.length}项 · ${visibleGroups.join("；")}${hiddenGroupText}`;
}

function businessFocusText(summary: TrimDeltaSummary, limit = 3): string {
  const groups = businessFocusGroups(summary);
  if (groups.length === 0) return categoryText(summary);
  const visibleGroups = groups.slice(0, limit).map((group) => `${group.label} ${group.deltas.length} 项`);
  const hiddenGroupText = groups.length > limit ? ` · +${groups.length - limit} 个维度` : "";
  return `${visibleGroups.join(" · ")}${hiddenGroupText}`;
}

function aggregateBusinessFocusGroups(targetSummaries: TrimDeltaSummary[]): Array<{ label: string; count: number }> {
  const focusMap = new Map<string, { label: string; count: number }>();
  targetSummaries.forEach((summary) => {
    businessFocusGroups(summary).forEach((group) => {
      const existing = focusMap.get(group.key) ?? { label: group.label, count: 0 };
      existing.count += group.deltas.length;
      focusMap.set(group.key, existing);
    });
  });
  return Array.from(focusMap.values()).sort((a, b) => {
    if (b.count !== a.count) return b.count - a.count;
    return a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" });
  });
}

function aggregateBusinessFocusPreviewText(targetSummaries: TrimDeltaSummary[], groupLimit = 2, featureLimit = 1): string | null {
  const groupedDeltas = new Map<string, { label: string; deltas: ConfigDelta[] }>();
  targetSummaries.forEach((summary) => {
    sortedDeltas(summary.deltas).forEach((delta) => {
      if (delta.deltaType === "SAME") return;
      const dimension = businessFocusDimension(delta);
      const existing = groupedDeltas.get(dimension.key) ?? { label: dimension.label, deltas: [] };
      existing.deltas.push(delta);
      groupedDeltas.set(dimension.key, existing);
    });
  });
  const groups = Array.from(groupedDeltas.values()).sort((a, b) => {
    if (b.deltas.length !== a.deltas.length) return b.deltas.length - a.deltas.length;
    return a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" });
  });
  const visibleGroups = groups.slice(0, groupLimit).map((group) => (
    `${group.label} ${group.deltas.length}：${compactFeatureText(group.deltas, featureLimit)}`
  ));
  if (visibleGroups.length === 0) return null;
  const hiddenGroupText = groups.length > groupLimit ? `；+${groups.length - groupLimit} 个维度` : "";
  return `${visibleGroups.join("；")}${hiddenGroupText}`;
}

function optionSwapInsights(summary: TrimDeltaSummary): OptionSwapInsight[] {
  const removedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "REMOVED"));
  const addedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "ADDED"));
  const usedRemovedKeys = new Set<string>();
  const insights: OptionSwapInsight[] = [];

  addedDeltas.forEach((added) => {
    const dimension = optionSwapDimension(added);
    if (!dimension) return;
    const removed = removedDeltas.find((candidate) => {
      if (usedRemovedKeys.has(candidate.row.featureCode)) return false;
      if (candidate.row.category !== added.row.category) return false;
      const candidateDimension = optionSwapDimension(candidate);
      return candidateDimension?.key === dimension.key;
    });
    if (!removed) return;
    usedRemovedKeys.add(removed.row.featureCode);
    insights.push({
      key: `${dimension.key}-${removed.row.featureCode}-${added.row.featureCode}`,
      category: added.row.category,
      dimensionLabel: dimension.label,
      fromFeature: featureDisplayName(removed),
      toFeature: featureDisplayName(added),
      fromDelta: removed,
      toDelta: added,
    });
  });

  return insights;
}

function compactOptionSwapInsightText(insights: OptionSwapInsight[], limit = 1): string {
  const visible = insights.slice(0, limit).map((insight) => (
    `${insight.dimensionLabel}：${insight.fromFeature} → ${insight.toFeature}`
  ));
  return `${visible.join("；")}${insights.length > visible.length ? `；+${insights.length - visible.length} 条升级线索` : ""}`;
}

function optionSwapEvidenceReason(insight: OptionSwapInsight, side: "from" | "to"): string {
  const sourceLabel = side === "from" ? "旧配置" : "新配置";
  return `升级线索：${insight.dimensionLabel} 从 ${insight.fromFeature} 调整为 ${insight.toFeature}；当前打开${sourceLabel}来源。`;
}

function searchLabel(searchValue?: string): string | null {
  const normalized = searchValue?.replace(/\s+/g, " ").trim();
  if (!normalized) return null;
  const displayValue = normalized.length > 32 ? `${normalized.slice(0, 32)}...` : normalized;
  return `搜索：${displayValue}`;
}

function deltaFilterLabel(deltaFilter?: ConfigComparisonDeltaFilter): string | null {
  if (!deltaFilter || deltaFilter === "ALL") return null;
  if (deltaFilter === "DIFFERENCE") return "差异项";
  if (deltaFilter === "ADDED") return "新增配置";
  if (deltaFilter === "REMOVED") return "减少配置";
  if (deltaFilter === "VALUE_CHANGED") return "值变化";
  if (deltaFilter === "OPTIONAL_CHANGED") return "选装变化";
  if (deltaFilter === "INFERRED") return "规则推断";
  if (deltaFilter === "MISSING_SOURCE") return "来源问题";
  if (deltaFilter === "MERGED_SOURCE") return "合并格展开";
  if (deltaFilter === "UNKNOWN") return "待确认";
  if (deltaFilter === "COMMON") return "共同配置";
  return null;
}

function simpleDeltaFilterLabel(deltaFilter?: ConfigComparisonDeltaFilter): string | null {
  if (!deltaFilter || deltaFilter === "ALL") return null;
  if (deltaFilter === "DIFFERENCE") return "差异行";
  if (deltaFilter === "ADDED") return "新增配置行";
  if (deltaFilter === "REMOVED") return "减少配置行";
  if (deltaFilter === "VALUE_CHANGED") return "值变化行";
  if (deltaFilter === "OPTIONAL_CHANGED") return "选装变化行";
  if (deltaFilter === "INFERRED") return "规则推断行";
  if (deltaFilter === "MISSING_SOURCE") return "来源问题行";
  if (deltaFilter === "MERGED_SOURCE") return "合并格行";
  if (deltaFilter === "UNKNOWN") return "待确认行";
  if (deltaFilter === "COMMON") return "共同配置行";
  return null;
}

function isEvidenceDeltaFilter(deltaFilter: ConfigComparisonDeltaFilter): boolean {
  return deltaFilter === "MISSING_SOURCE" || deltaFilter === "MERGED_SOURCE";
}

function focusTargetForFilter(_filter: ConfigComparisonDeltaFilter, targetTrimId: string): string {
  return targetTrimId;
}

function scopeLabel(categoryFilter?: string | null, searchValue?: string, deltaFilter?: ConfigComparisonDeltaFilter): string | null {
  const parts = [
    deltaFilterLabel(deltaFilter),
    searchLabel(searchValue),
    categoryFilter ? categoryLabel(categoryFilter) : null,
  ].filter((part): part is string => Boolean(part));
  return parts.length > 0 ? parts.join(" · ") : null;
}

function panelScopeLabel(
  categoryFilter?: string | null,
  searchValue?: string,
  deltaFilter?: ConfigComparisonDeltaFilter,
  targetTrim?: CompareTrimItem | null,
): string | null {
  const parts = [
    deltaFilterLabel(deltaFilter),
    targetTrim ? `目标 ${trimLabel(targetTrim)}` : null,
    searchLabel(searchValue),
    categoryFilter ? categoryLabel(categoryFilter) : null,
  ].filter((part): part is string => Boolean(part));
  return parts.length > 0 ? parts.join(" · ") : null;
}

function simplePanelScopeLabel(
  categoryFilter?: string | null,
  searchValue?: string,
  deltaFilter?: ConfigComparisonDeltaFilter,
  targetTrim?: CompareTrimItem | null,
): string | null {
  const parts = [
    simpleDeltaFilterLabel(deltaFilter),
    targetTrim ? `目标 ${trimLabel(targetTrim)}` : null,
    searchLabel(searchValue),
    categoryFilter ? categoryLabel(categoryFilter) : null,
  ].filter((part): part is string => Boolean(part));
  return parts.length > 0 ? parts.join(" · ") : null;
}

function narrative(summary: TrimDeltaSummary, baseTrim: CompareTrimItem, categoryFilter?: string | null, searchValue?: string, deltaFilter?: ConfigComparisonDeltaFilter): string {
  const inferredText = summary.inferredCount > 0 ? `，其中规则推断 ${summary.inferredCount} 项` : "";
  const scopedLabel = scopeLabel(categoryFilter, searchValue, deltaFilter);
  if (deltaFilter === "MISSING_SOURCE") {
    return `${trimLabel(summary.targetTrim)} 与 ${trimLabel(baseTrim)} 在 ${scopedLabel || "当前范围"}：${summary.deltas.length} 项配置存在缺值或缺少来源证据，需要先补来源证据。`;
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return `${trimLabel(summary.targetTrim)} 与 ${trimLabel(baseTrim)} 在 ${scopedLabel || "当前范围"}：${summary.deltas.length} 项配置来自合并格展开，适合核对共通参数。`;
  }
  if (deltaFilter === "COMMON") {
    return `${trimLabel(summary.targetTrim)} 与 ${trimLabel(baseTrim)} 在 ${scopedLabel || "当前范围"}：共同配置 ${summary.deltas.length} 项。`;
  }
  if (scopedLabel) {
    return `${trimLabel(summary.targetTrim)} 相比 ${trimLabel(baseTrim)} 在 ${scopedLabel}：${deltaCountText(summary)}${inferredText}。`;
  }
  return `${trimLabel(summary.targetTrim)} 相比 ${trimLabel(baseTrim)}：${deltaCountText(summary)}，集中在 ${businessFocusText(summary)}${inferredText}。`;
}

function compactFeatureText(deltas: ConfigDelta[], limit = 2): string {
  const visible = deltas.slice(0, limit).map((delta) => delta.row.featureName);
  return `${visible.join("、")}${deltas.length > visible.length ? `、+${deltas.length - visible.length}` : ""}`;
}

function compactCategoryFeatureText(deltas: ConfigDelta[], categoryLimit = 1, featureLimit = 2): string {
  const groupedDeltas = new Map<string, ConfigDelta[]>();
  sortedDeltas(deltas).forEach((delta) => {
    groupedDeltas.set(delta.row.category, [...(groupedDeltas.get(delta.row.category) ?? []), delta]);
  });
  const groups = Array.from(groupedDeltas.entries()).sort((a, b) => {
    if (b[1].length !== a[1].length) return b[1].length - a[1].length;
    return categoryLabel(a[0]).localeCompare(categoryLabel(b[0]), undefined, { numeric: true, sensitivity: "base" });
  });
  const visibleGroups = groups.slice(0, categoryLimit).map(([category, items]) => (
    `${categoryLabel(category)}：${compactFeatureText(items, featureLimit)}`
  ));
  const hiddenGroupText = groups.length > categoryLimit ? `；+${groups.length - categoryLimit} 类` : "";
  return `${deltas.length}项 · ${visibleGroups.join("；")}${hiddenGroupText}`;
}

function compactBusinessDirectionText(summary: TrimDeltaSummary): string {
  return [
    summary.addedCount > 0 ? `新增 ${summary.addedCount}` : null,
    summary.removedCount > 0 ? `减少 ${summary.removedCount}` : null,
    summary.valueChangedCount > 0 ? `值变化 ${summary.valueChangedCount}` : null,
    summary.optionalCount > 0 ? `选装变化 ${summary.optionalCount}` : null,
    summary.unknownCount > 0 ? `待确认 ${summary.unknownCount}` : null,
  ].filter((text): text is string => Boolean(text)).join("、") || "暂无确定差异";
}

function compactBusinessFocusText(summary: TrimDeltaSummary): string {
  const focusGroups = businessFocusGroups(summary).slice(0, 2);
  if (focusGroups.length === 0) return "暂无集中维度";
  return focusGroups.map((group) => `${group.label} ${group.deltas.length}`).join("、");
}

function targetBusinessInterpretation(
  summary: TrimDeltaSummary,
  baseTrim: CompareTrimItem,
  deltaFilter: ConfigComparisonDeltaFilter,
): string {
  const targetLabel = trimLabel(summary.targetTrim);
  const baseLabel = trimLabel(baseTrim);
  if (deltaFilter === "MISSING_SOURCE") {
    return `${targetLabel} 与 ${baseLabel} 当前范围有 ${summary.deltas.length} 项配置存在缺值或缺少来源证据；先补来源或重新消化，再生成业务差异结论。`;
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return `${targetLabel} 与 ${baseLabel} 当前范围有 ${summary.deltas.length} 项配置来自合并格展开，适合核对这些共通参数是否应同步到各 trim。`;
  }
  if (deltaFilter === "COMMON") {
    const commonDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "SAME"));
    if (commonDeltas.length === 0) return `${targetLabel} 与 ${baseLabel} 在当前范围没有共同配置行。`;
    return `${targetLabel} 与 ${baseLabel} 在当前范围保持一致：${compactCategoryFeatureText(commonDeltas, 1, 1)}。点击共同配置可核对来源。`;
  }

  const addedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "ADDED"));
  const removedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "REMOVED"));
  const changedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "VALUE_CHANGED" || delta.deltaType === "OPTIONAL_CHANGED"));
  const unknownDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "UNKNOWN"));
  const swapInsights = optionSwapInsights(summary);
  const parts = [
    swapInsights.length > 0 ? `升级线索 ${compactOptionSwapInsightText(swapInsights)}` : null,
    addedDeltas.length > 0 ? `主要增加 ${compactBusinessFocusFeatureText(addedDeltas, 1, 1)}` : null,
    removedDeltas.length > 0 ? `减少 ${compactBusinessFocusFeatureText(removedDeltas, 1, 1)}` : null,
    changedDeltas.length > 0 ? `配置表达变化 ${compactBusinessFocusFeatureText(changedDeltas, 1, 1)}` : null,
    unknownDeltas.length > 0 ? `待确认 ${compactBusinessFocusFeatureText(unknownDeltas, 1, 1)}` : null,
  ].filter((part): part is string => Boolean(part));
  if (parts.length === 0) return `${targetLabel} 相比 ${baseLabel} 在当前范围没有业务差异。`;

  const evidenceTail = summary.inferredCount > 0
    ? `其中 ${summary.inferredCount} 项为规则推断，解释结论前应点开来源核对。`
    : unknownDeltas.length > 0
      ? "待确认项需要先补来源证据。"
      : "当前差异可继续从单元格追溯来源。";
  return `${targetLabel} 相比 ${baseLabel}：${parts.join("；")}。${evidenceTail}`;
}

function conclusionChangePhrase(summary: TrimDeltaSummary): string {
  const swapInsights = optionSwapInsights(summary);
  const parts = [
    swapInsights.length > 0 ? `升级线索 ${compactOptionSwapInsightText(swapInsights, 2)}` : null,
    summary.addedCount > 0 ? `增加 ${summary.addedCount} 项配置` : null,
    summary.removedCount > 0 ? `减少 ${summary.removedCount} 项配置` : null,
    summary.valueChangedCount > 0 ? `参数变化 ${summary.valueChangedCount} 项` : null,
    summary.optionalCount > 0 ? `选装变化 ${summary.optionalCount} 项` : null,
  ].filter((part): part is string => Boolean(part));
  if (parts.length === 0) return "当前范围没有可转成业务话术的确定差异";
  const strongestFocusGroup = businessFocusGroups(summary)[0] ?? null;
  const categoryTail = strongestFocusGroup
    ? `，主要集中在 ${strongestFocusGroup.label}`
    : "";
  return `${parts.join("，")}${categoryTail}`;
}

function targetConclusionDraft(summary: TrimDeltaSummary, baseTrim: CompareTrimItem, deltaFilter: ConfigComparisonDeltaFilter): TargetConclusionDraft {
  const targetLabel = trimLabel(summary.targetTrim);
  const baseLabel = trimLabel(baseTrim);

  if (deltaFilter === "MISSING_SOURCE") {
    return {
      tone: "warning",
      statusLabel: "暂不引用",
      title: "来源问题结论暂缓",
      body: `${targetLabel} 与 ${baseLabel} 当前范围有 ${summary.deltas.length} 项配置存在缺值或缺少来源证据，先补来源再写业务结论。`,
      evidenceNote: "缺值或缺来源证据的配置不能直接解释为确定增配、减配或缺配。",
      filter: "MISSING_SOURCE",
      actionLabel: "查看来源问题",
    };
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return {
      tone: "review",
      statusLabel: "核对共通项",
      title: "合并格展开需要复核",
      body: `${targetLabel} 与 ${baseLabel} 当前范围有 ${summary.deltas.length} 项来自合并格展开，适合确认共通参数是否正确同步到各 trim。`,
      evidenceNote: "合并格值应回看原始单元格和合并范围，不单看展开后的展示值。",
      filter: "MERGED_SOURCE",
      actionLabel: "查看合并格",
    };
  }
  if (deltaFilter === "COMMON") {
    return {
      tone: "neutral",
      statusLabel: "共同基线",
      title: "不生成差异话术",
      body: `${targetLabel} 与 ${baseLabel} 当前范围保持一致，可作为同车型共性配置基线。`,
      evidenceNote: "共同项仍可点开来源核对原始值、合并格和推断边界。",
      filter: "COMMON",
      actionLabel: "查看共同配置",
    };
  }
  if (summary.unknownCount > 0) {
    return {
      tone: "warning",
      statusLabel: "暂不引用",
      title: "待确认项未闭环",
      body: `${targetLabel} 相比 ${baseLabel} 仍有 ${summary.unknownCount} 项待确认；先补来源证据或重新消化来源，再输出确定配置结论。`,
      evidenceNote: "待确认项不会被自动当作无配置，也不应进入卖点或短板话术。",
      filter: "UNKNOWN",
      actionLabel: "查看待确认",
    };
  }
  if (summary.inferredCount > 0) {
    return {
      tone: "review",
      statusLabel: "需核对推断",
      title: "推断值待来源确认",
      body: `初稿：${targetLabel} 相比 ${baseLabel} ${conclusionChangePhrase(summary)}。`,
      evidenceNote: `含规则推断 ${summary.inferredCount} 项；不配备* 需回看来源后再引用。`,
      filter: "INFERRED",
      actionLabel: "查看推断",
    };
  }

  const contextGuard = targetContextActionGuard(baseTrim, summary.targetTrim);
  if (contextGuard) {
    return {
      tone: contextGuard.tone,
      statusLabel: contextGuard.tone === "warning" ? "暂不引用" : "保留口径",
      title: contextGuard.title,
      body: `初稿需保留口径：${contextGuard.detail}`,
      evidenceNote: contextGuard.tone === "warning"
        ? "身份、市场、年款或来源口径不完整时，不建议直接输出确定结论。"
        : "跨市场、跨年款、跨来源或竞品数据应在结论中保留前提。",
      filter: contextGuard.filter,
      action: contextGuard.action,
      actionLabel: contextGuard.actionLabel,
    };
  }

  if (summary.totalDifferenceCount > 0) {
    return {
      tone: "ready",
      statusLabel: "可引用初稿",
      title: "配置差异可转业务话术",
      body: `初稿：${targetLabel} 相比 ${baseLabel} ${conclusionChangePhrase(summary)}。`,
      evidenceNote: "当前差异状态明确，可继续转成版本卖点、短板或配置层级说明。",
      filter: "DIFFERENCE",
      actionLabel: "查看差异",
    };
  }

  return {
    tone: "neutral",
    statusLabel: "无差异",
    title: "当前范围没有业务差异",
    body: `${targetLabel} 与 ${baseLabel} 当前范围没有可解释差异，可作为同配置证明。`,
    evidenceNote: "可切换大类、目标配置列或差异筛选继续分析。",
  };
}

function deltaFeatureLabel(delta: ConfigDelta): string {
  return delta.row.featureName || delta.row.featureCode || "Unnamed feature";
}

function llmEvidenceKey(delta: ConfigDelta): string {
  const rowKey = delta.row.featureCode || stableBusinessKey(`${delta.row.category}-${delta.row.featureName}`);
  return `${delta.targetTrim.trimId}:${delta.deltaType}:${rowKey}`;
}

function buildLlmBusinessSummaryPayload(
  baseTrim: CompareTrimItem,
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
  versionScope: "published" | "latest",
  categoryFilter?: string | null,
  searchValue = "",
  factSource?: EngineeringConfigBusinessSummaryComposeRequest["factSource"],
): EngineeringConfigBusinessSummaryComposeRequest {
  return {
    trimIds: [baseTrim.trimId, ...targetSummaries.map((summary) => summary.targetTrim.trimId)],
    baseTrimId: baseTrim.trimId,
    versionScope,
    factSource,
    filters: {
      deltaFilter,
      category: categoryFilter ?? null,
      search: searchValue.trim() || null,
      targetTrimId: targetSummaries.length === 1 ? targetSummaries[0].targetTrim.trimId : null,
    },
  };
}

function simpleBaseConclusionText(baseTrim: CompareTrimItem, targetSummaries: TrimDeltaSummary[], deltaFilter: ConfigComparisonDeltaFilter): string {
  const targetCount = targetSummaries.length;
  const totalDifferenceCount = targetSummaries.reduce((total, item) => total + item.totalDifferenceCount, 0);
  const inferredCount = targetSummaries.reduce((total, item) => total + item.inferredCount, 0);
  const unknownCount = targetSummaries.reduce((total, item) => total + item.unknownCount, 0);
  const focusTextValue = aggregateBusinessFocusGroups(targetSummaries)
    .slice(0, 2)
    .map((focus) => `${focus.label} ${focus.count}`)
    .join("、");
  if (deltaFilter === "COMMON") {
    return `${trimLabel(baseTrim)} 是基准列；${targetCount} 个目标配置列当前有 ${scopedFeatureCount(targetSummaries)} 行共同配置，可作为同车型共性基线。`;
  }
  if (deltaFilter === "MISSING_SOURCE") {
    return `${trimLabel(baseTrim)} 是基准列；${targetCount} 个目标配置列当前有 ${scopedFeatureCount(targetSummaries)} 行来源问题配置，先补来源证据再解释差异。`;
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return `${trimLabel(baseTrim)} 是基准列；${targetCount} 个目标配置列当前有 ${scopedFeatureCount(targetSummaries)} 行合并格展开配置，适合核对共通参数。`;
  }
  const evidenceText = unknownCount > 0
    ? `，待确认 ${unknownCount} 项`
    : inferredCount > 0
      ? `，含规则推断 ${inferredCount} 项`
      : "";
  const focusText = focusTextValue ? `，主要在 ${focusTextValue}` : "";
  return `${trimLabel(baseTrim)} 是基准列；${targetCount} 个目标配置列，${differenceCountLabel(targetSummaries, totalDifferenceCount)}，${differenceFeatureScopeText(targetSummaries, affectedFeatureCount(targetSummaries), "row")}${focusText}${evidenceText}。`;
}

function simpleConclusionHeadline(summary: TrimDeltaSummary, baseTrim: CompareTrimItem, draft: TargetConclusionDraft, deltaFilter: ConfigComparisonDeltaFilter): string {
  const baseLabel = trimLabel(baseTrim);
  if (deltaFilter === "COMMON") return `与 ${baseLabel} 保持共同配置。`;
  if (deltaFilter === "MISSING_SOURCE" || deltaFilter === "MERGED_SOURCE") return `${draft.title}。`;
  if (summary.totalDifferenceCount === 0) return `与 ${baseLabel} 暂无业务差异。`;
  return `${compactBusinessDirectionText(summary)}，集中在 ${compactBusinessFocusText(summary)}。`;
}

function simpleConclusionDetail(summary: TrimDeltaSummary, deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "COMMON" || isEvidenceDeltaFilter(deltaFilter)) {
    const leadInsight = targetInsightItems(summary, deltaFilter)[0];
    return leadInsight ? `${leadInsight.label}：${leadInsight.value}` : categoryText(summary);
  }
  if (summary.totalDifferenceCount > 0) return `业务重点：${businessFocusText(summary, 2)}`;
  if (deltaFilter !== "ALL") {
    const leadInsight = targetInsightItems(summary, deltaFilter)[0];
    return leadInsight ? `${leadInsight.label}：${leadInsight.value}` : categoryText(summary);
  }
  return categoryText(summary);
}

function simpleConclusionPoints(summary: TrimDeltaSummary, deltaFilter: ConfigComparisonDeltaFilter): SimpleConclusionPoint[] {
  if (deltaFilter === "COMMON" || isEvidenceDeltaFilter(deltaFilter)) {
    return targetInsightItems(summary, deltaFilter).slice(0, 3).map((item) => ({
      key: item.key,
      label: item.label,
      value: item.value,
      tone: deltaFilter === "MISSING_SOURCE" ? "warning" : deltaFilter === "MERGED_SOURCE" ? "review" : "neutral",
    }));
  }

  const addedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "ADDED"));
  const removedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "REMOVED"));
  const changedDeltas = sortedDeltas(summary.deltas.filter((delta) => (
    delta.deltaType === "VALUE_CHANGED" || delta.deltaType === "OPTIONAL_CHANGED"
  )));
  const unknownDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "UNKNOWN"));
  const swapInsights = optionSwapInsights(summary);
  const points: SimpleConclusionPoint[] = [];

  if (swapInsights.length > 0) {
    points.push({
      key: "upgrade",
      label: "升级线索",
      value: compactOptionSwapInsightText(swapInsights, 2),
      tone: "ready",
    });
  }
  if (addedDeltas.length > 0) {
    points.push({
      key: "added",
      label: "主要增加",
      value: compactBusinessFocusFeatureText(addedDeltas, 2, 1),
      tone: addedDeltas.some((delta) => delta.inferred) ? "review" : "ready",
    });
  }
  if (removedDeltas.length > 0) {
    points.push({
      key: "removed",
      label: "主要减少",
      value: compactBusinessFocusFeatureText(removedDeltas, 2, 1),
      tone: removedDeltas.some((delta) => delta.inferred) ? "review" : "ready",
    });
  }
  if (changedDeltas.length > 0) {
    points.push({
      key: "changed",
      label: "参数 / 选装变化",
      value: compactBusinessFocusFeatureText(changedDeltas, 2, 1),
      tone: changedDeltas.some((delta) => delta.inferred) ? "review" : "ready",
    });
  }
  if (unknownDeltas.length > 0) {
    points.push({
      key: "unknown",
      label: "待确认",
      value: compactBusinessFocusFeatureText(unknownDeltas, 2, 1),
      tone: "warning",
    });
  }
  if (summary.inferredCount > 0) {
    points.push({
      key: "inferred",
      label: "证据边界",
      value: `规则推断 ${summary.inferredCount} 项，带 * 值需回看来源。`,
      tone: "review",
    });
  }

  return points.length > 0
    ? points.slice(0, 4)
    : [{
      key: "none",
      label: "业务结论",
      value: "当前范围暂无业务差异。",
      tone: "neutral",
    }];
}

function simpleConclusionItems(baseTrim: CompareTrimItem, targetSummaries: TrimDeltaSummary[], deltaFilter: ConfigComparisonDeltaFilter): SimpleConclusionItem[] {
  return targetSummaries.map((targetSummary) => {
    const draft = targetConclusionDraft(targetSummary, baseTrim, deltaFilter);
    return {
      key: targetSummary.targetTrim.trimId,
      targetTrimId: targetSummary.targetTrim.trimId,
      targetLabel: trimLabel(targetSummary.targetTrim),
      statusLabel: draft.statusLabel,
      headline: simpleConclusionHeadline(targetSummary, baseTrim, draft, deltaFilter),
      detail: simpleConclusionDetail(targetSummary, deltaFilter),
      points: simpleConclusionPoints(targetSummary, deltaFilter),
      evidenceNote: draft.evidenceNote,
      tone: draft.tone,
      filter: draft.filter,
      action: draft.action,
      actionLabel: draft.actionLabel ?? "查看范围",
    };
  });
}

function simpleVersionNarrativeActionLabel(filter: ConfigComparisonDeltaFilter): string {
  if (filter === "UNKNOWN") return "查看待确认行";
  if (filter === "INFERRED") return "查看推断行";
  if (filter === "ADDED") return "查看新增行";
  if (filter === "REMOVED") return "查看减少行";
  if (filter === "VALUE_CHANGED") return "查看参数变化行";
  if (filter === "OPTIONAL_CHANGED") return "查看选装变化行";
  return "查看该列差异";
}

function simpleVersionNarrativeBody(
  summary: TrimDeltaSummary,
  baseTrim: CompareTrimItem,
  deltaFilter: ConfigComparisonDeltaFilter,
): string {
  if (summary.totalDifferenceCount === 0) {
    return `${trimLabel(summary.targetTrim)} 与 ${trimLabel(baseTrim)} 在当前范围没有业务差异。`;
  }
  if (deltaFilter === "UNKNOWN") {
    return `${trimLabel(summary.targetTrim)} 相比 ${trimLabel(baseTrim)}：${summary.unknownCount} 项待确认，先补来源证据再转结论。`;
  }
  if (deltaFilter === "INFERRED") {
    return `${trimLabel(summary.targetTrim)} 相比 ${trimLabel(baseTrim)}：${summary.inferredCount} 项规则推断，带 * 值需回看来源。`;
  }
  const upgradeCount = optionSwapInsights(summary).length;
  const focusText = businessFocusText(summary, 2);
  const evidenceText = summary.inferredCount > 0
    ? `含规则推断 ${summary.inferredCount} 项，先回看来源`
    : summary.unknownCount > 0
      ? `待确认 ${summary.unknownCount} 项，先补来源`
      : "证据状态明确";
  return `${trimLabel(summary.targetTrim)} 相比 ${trimLabel(baseTrim)}：${[
    upgradeCount > 0 ? `识别 ${upgradeCount} 条升级线索` : null,
    `重点在 ${focusText}`,
    evidenceText,
  ].filter((part): part is string => Boolean(part)).join("；")}。`;
}

function simpleVersionNarrativeEvidenceLabel(summary: TrimDeltaSummary, draft: TargetConclusionDraft): string {
  if (summary.unknownCount > 0) return `${draft.statusLabel} · 待确认项先补来源`;
  if (summary.inferredCount > 0) return `${draft.statusLabel} · 不配备* 需回看来源`;
  if (summary.totalDifferenceCount > 0) return `${draft.statusLabel} · 可从单元格追溯来源`;
  return `${draft.statusLabel} · 当前范围无业务差异`;
}

function simpleVersionNarrativeItems(
  baseTrim: CompareTrimItem,
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
): SimpleVersionNarrativeItem[] {
  return targetSummaries.map((targetSummary) => {
    const draft = targetConclusionDraft(targetSummary, baseTrim, deltaFilter);
    const focusFilter = targetSummary.totalDifferenceCount > 0
      ? overviewFocusFilter(deltaFilter)
      : undefined;
    return {
      key: targetSummary.targetTrim.trimId,
      targetTrimId: targetSummary.targetTrim.trimId,
      targetLabel: trimLabel(targetSummary.targetTrim),
      comparisonLabel: `${trimLabel(targetSummary.targetTrim)} 相比 ${trimLabel(baseTrim)}`,
      headline: simpleConclusionHeadline(targetSummary, baseTrim, draft, deltaFilter),
      body: simpleVersionNarrativeBody(targetSummary, baseTrim, deltaFilter),
      evidenceLabel: simpleVersionNarrativeEvidenceLabel(targetSummary, draft),
      tone: draft.tone,
      filter: focusFilter,
      actionLabel: focusFilter ? simpleVersionNarrativeActionLabel(focusFilter) : undefined,
    };
  });
}

function simpleConclusionTableScopeActionLabel(item: SimpleConclusionItem): string {
  if (!item.filter) return item.actionLabel;
  if (item.actionLabel.endsWith("范围")) return item.actionLabel;
  return `${item.actionLabel}范围`;
}

function simpleConclusionTableScopeAriaLabel(item: SimpleConclusionItem): string {
  return `聚焦 ${item.targetLabel} 的表格范围：${simpleConclusionTableScopeActionLabel(item)}，${item.statusLabel}`;
}

function simpleScopeBridgeRangeValue(deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "ALL") return "全部配置行";
  if (deltaFilter === "DIFFERENCE") return "差异行";
  if (deltaFilter === "COMMON") return "共同配置行";
  if (deltaFilter === "UNKNOWN") return "待确认行";
  if (deltaFilter === "INFERRED") return "规则推断行";
  if (deltaFilter === "MISSING_SOURCE") return "来源问题行";
  if (deltaFilter === "MERGED_SOURCE") return "合并格行";
  return `${deltaFilterLabel(deltaFilter) ?? "当前范围"}行`;
}

function simpleScopeBridgeRangeDetail(deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "ALL") return "表格保留完整配置行；摘要只提炼业务差异。";
  if (deltaFilter === "COMMON") return "当前表格收窄到共同配置，用于沉淀共性基线。";
  if (deltaFilter === "UNKNOWN") return "当前表格收窄到待确认项，空值不自动当作无配置。";
  if (deltaFilter === "INFERRED") return "当前表格收窄到规则推断差异，不配备* 需回看来源。";
  if (isEvidenceDeltaFilter(deltaFilter)) return "当前表格收窄到证据核对范围，先解释来源再下结论。";
  return "当前表格已收窄到业务差异范围。";
}

function simpleScopeBridgeSummaryValue(targetSummaries: TrimDeltaSummary[], deltaFilter: ConfigComparisonDeltaFilter): string {
  const featureCount = scopedFeatureCount(targetSummaries);
  if (deltaFilter === "COMMON") return `共同配置 ${featureCount}`;
  if (deltaFilter === "MISSING_SOURCE" || deltaFilter === "MERGED_SOURCE") return `证据配置 ${featureCount}`;
  const totalDifferenceCount = targetSummaries.reduce((total, item) => total + item.totalDifferenceCount, 0);
  if (deltaFilter === "ALL") {
    return targetSummaries.length > 1 ? differenceCountLabel(targetSummaries, totalDifferenceCount) : `业务差异 ${totalDifferenceCount}`;
  }
  return `当前口径 ${totalDifferenceCount}`;
}

function simpleScopeBridgeSummaryDetail(deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "ALL") return "默认不隐藏共同项，先给完整配置基线，再给业务差异摘要。";
  if (deltaFilter === "COMMON") return "当前摘要不生成差异话术，只说明共同配置范围。";
  if (isEvidenceDeltaFilter(deltaFilter)) return "当前摘要用于证据核对，不直接输出卖点或短板。";
  return "当前摘要与表格使用同一筛选口径。";
}

function simpleScopeBridgeLeadTargets(targetSummaries: TrimDeltaSummary[], deltaFilter: ConfigComparisonDeltaFilter): TrimDeltaSummary[] {
  if (deltaFilter === "COMMON" || isEvidenceDeltaFilter(deltaFilter)) return [];
  const maxDifferenceCount = Math.max(0, ...targetSummaries.map((summary) => summary.totalDifferenceCount));
  if (maxDifferenceCount <= 0) return [];
  return targetSummaries.filter((summary) => summary.totalDifferenceCount === maxDifferenceCount);
}

function simpleScopeBridgeTargetLabel(
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
  targetTrim: CompareTrimItem | null,
): string {
  if (targetTrim) return "当前目标";
  return simpleScopeBridgeLeadTargets(targetSummaries, deltaFilter).length > 0 ? "重点目标" : "目标核对";
}

function simpleScopeBridgeTargetValue(
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
  targetTrim: CompareTrimItem | null,
): string {
  if (targetTrim) return trimLabel(targetTrim);
  const leadTargets = simpleScopeBridgeLeadTargets(targetSummaries, deltaFilter);
  if (leadTargets.length === 0) return "全部目标配置列";
  const firstLabel = trimLabel(leadTargets[0].targetTrim);
  return leadTargets.length > 1 ? `${firstLabel} 等 ${leadTargets.length} 个` : firstLabel;
}

function simpleScopeBridgeTargetDetail(
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
  targetTrim: CompareTrimItem | null,
): string {
  if (targetTrim) return "只聚焦目标配置列，不减少配置行。";
  if (deltaFilter === "COMMON") return `${targetSummaries.length} 个目标配置列一起核对共同配置。`;
  if (isEvidenceDeltaFilter(deltaFilter)) return `${targetSummaries.length} 个目标配置列一起核对来源证据。`;
  const leadTargets = simpleScopeBridgeLeadTargets(targetSummaries, deltaFilter);
  if (leadTargets.length === 0) return `${targetSummaries.length} 个目标配置列一起汇总。`;
  const leadDifferenceCount = leadTargets[0].totalDifferenceCount;
  const focusPreview = aggregateBusinessFocusPreviewText(leadTargets, 2, 1);
  const leadLabel = leadTargets.length > 1 ? `并列最多 ${leadDifferenceCount} 项` : `差异最多 ${leadDifferenceCount} 项`;
  return focusPreview ? `${leadLabel}；${focusPreview}。` : `${leadLabel}。`;
}

function simpleScopeBridgeTargetFocus(
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
  targetTrim: CompareTrimItem | null,
): { filter: ConfigComparisonDeltaFilter; targetTrimId: string; actionLabel: string } | null {
  if (deltaFilter === "COMMON" || isEvidenceDeltaFilter(deltaFilter)) return null;
  if (targetTrim) {
    const targetSummary = targetSummaries.find((summary) => summary.targetTrim.trimId === targetTrim.trimId);
    if (!targetSummary || targetSummary.totalDifferenceCount <= 0) return null;
    return {
      filter: deltaFilter === "ALL" ? "DIFFERENCE" : deltaFilter,
      targetTrimId: targetTrim.trimId,
      actionLabel: "查看当前目标差异行",
    };
  }
  const leadTarget = simpleScopeBridgeLeadTargets(targetSummaries, deltaFilter)[0];
  if (!leadTarget) return null;
  return {
    filter: deltaFilter === "ALL" ? "DIFFERENCE" : deltaFilter,
    targetTrimId: leadTarget.targetTrim.trimId,
    actionLabel: "查看重点目标差异行",
  };
}

function simpleScopeBridgeItems(
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
  targetTrim: CompareTrimItem | null,
): SimpleScopeBridgeItem[] {
  const targetFocus = simpleScopeBridgeTargetFocus(targetSummaries, deltaFilter, targetTrim);
  return [
    {
      key: "table",
      label: "表格范围",
      value: simpleScopeBridgeRangeValue(deltaFilter),
      detail: simpleScopeBridgeRangeDetail(deltaFilter),
    },
    {
      key: "summary",
      label: "差异提炼",
      value: simpleScopeBridgeSummaryValue(targetSummaries, deltaFilter),
      detail: simpleScopeBridgeSummaryDetail(deltaFilter),
    },
    {
      key: "target",
      label: "目标配置列",
      value: targetTrim ? trimLabel(targetTrim) : "全部目标配置列",
      detail: targetTrim
        ? "只聚焦目标配置列，不减少配置行。"
        : `${targetSummaries.length} 个目标配置列一起汇总。`,
    },
    {
      key: "lead-target",
      label: simpleScopeBridgeTargetLabel(targetSummaries, deltaFilter, targetTrim),
      value: simpleScopeBridgeTargetValue(targetSummaries, deltaFilter, targetTrim),
      detail: simpleScopeBridgeTargetDetail(targetSummaries, deltaFilter, targetTrim),
      filter: targetFocus?.filter,
      targetTrimId: targetFocus?.targetTrimId,
      actionLabel: targetFocus?.actionLabel,
    },
  ];
}

function conclusionDraftCopyText(targetTrim: CompareTrimItem, baseTrim: CompareTrimItem, draft: TargetConclusionDraft): string {
  return [
    `Target trim: ${trimLabel(targetTrim)}`,
    `Base trim: ${trimLabel(baseTrim)}`,
    `Status: ${draft.statusLabel}`,
    `Title: ${draft.title}`,
    `Conclusion: ${draft.body}`,
    `Evidence note: ${draft.evidenceNote}`,
  ].join("\n");
}

const CONCLUSION_STATUS_PRIORITY: Record<string, number> = {
  "可引用初稿": 1,
  "需核对推断": 2,
  "保留口径": 3,
  "核对共通项": 4,
  "暂不引用": 5,
  "共同基线": 6,
  "无差异": 7,
};

function conclusionStatusPriority(label: string): number {
  return CONCLUSION_STATUS_PRIORITY[label] ?? 99;
}

function conclusionStatusSummaryItems(
  baseTrim: CompareTrimItem,
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
): ConclusionStatusSummaryItem[] {
  const statusMap = new Map<string, ConclusionStatusSummaryItem>();
  targetSummaries.forEach((targetSummary) => {
    const draft = targetConclusionDraft(targetSummary, baseTrim, deltaFilter);
    const targetLabel = trimLabel(targetSummary.targetTrim);
    const existing = statusMap.get(draft.statusLabel);
    if (existing) {
      existing.count += 1;
      existing.targetLabels.push(targetLabel);
      return;
    }
    statusMap.set(draft.statusLabel, {
      key: draft.statusLabel,
      label: draft.statusLabel,
      count: 1,
      tone: draft.tone,
      focusAction: draft.action,
      focusFilter: draft.filter,
      focusTargetTrimId: targetSummary.targetTrim.trimId,
      targetLabels: [targetLabel],
    });
  });
  return Array.from(statusMap.values()).sort((a, b) => {
    const priorityDiff = conclusionStatusPriority(a.label) - conclusionStatusPriority(b.label);
    if (priorityDiff !== 0) return priorityDiff;
    return a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" });
  });
}

function conclusionStatusSummaryText(items: ConclusionStatusSummaryItem[]): string {
  return items.map((item) => `${item.label} ${item.count} (${item.targetLabels.join(", ")})`).join(" / ") || "无目标结论";
}

function conclusionStatusFirstActionText(items: ConclusionStatusSummaryItem[]): string {
  return items.map((item) => `${item.label}: ${item.targetLabels[0] ?? "-"}`).join(" / ") || "无目标结论";
}

function conclusionStatusActionOrderText(items: ConclusionStatusSummaryItem[]): string {
  return items.map((item) => `${item.label}: ${item.targetLabels.join(" -> ")}`).join(" / ") || "无目标结论";
}

function compactConclusionStatusTargets(item: ConclusionStatusSummaryItem, limit = 2): string {
  const visibleTargets = item.targetLabels.slice(0, limit).join("、");
  const hiddenCount = item.targetLabels.length - limit;
  return hiddenCount > 0 ? `${visibleTargets}、+${hiddenCount}` : visibleTargets;
}

function conclusionStatusActionHint(item: ConclusionStatusSummaryItem): string {
  const firstTarget = item.targetLabels[0];
  if (!firstTarget) return "查看状态";
  return item.count > 1 ? `先看 ${firstTarget} · 共 ${item.count} 个` : `先看 ${firstTarget}`;
}

function excelGuideItems(
  baseTrim: CompareTrimItem,
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
  statusItems: ConclusionStatusSummaryItem[],
): ExcelGuideItem[] {
  const totalDifferenceCount = targetSummaries.reduce((total, item) => total + item.totalDifferenceCount, 0);
  const inferredCount = targetSummaries.reduce((total, item) => total + item.inferredCount, 0);
  const unknownCount = targetSummaries.reduce((total, item) => total + item.unknownCount, 0);
  const affectedCount = affectedFeatureCount(targetSummaries);
  const scopedCount = scopedFeatureCount(targetSummaries);
  const focusTextValue = aggregateBusinessFocusGroups(targetSummaries)
    .slice(0, 2)
    .map((focus) => `${focus.label} ${focus.count}`)
    .join(" · ");
  const focusPreviewText = aggregateBusinessFocusPreviewText(targetSummaries);
  const leadStatus = statusItems[0] ?? null;
  const scenarioItem = comparisonScenarioStorylineItem(baseTrim, targetSummaries);

  const tableValue = deltaFilter === "ALL"
    ? "全部配置行"
    : simpleScopeBridgeRangeValue(deltaFilter);
  const tableDetail = deltaFilter === "ALL"
    ? "下面表格仍是完整 xlsx 配置行；这里先把差异单独提炼出来。"
    : isEvidenceDeltaFilter(deltaFilter)
      ? "当前表格收窄到证据核对行，先解释来源再转业务结论。"
      : deltaFilter === "COMMON"
        ? "当前表格收窄到共同配置行，不生成差异话术。"
        : "当前表格已经按差异口径收窄，可随时恢复全部配置行。";

  const differenceValue = (() => {
    if (deltaFilter === "COMMON") return `${scopedCount} 项共同配置`;
    if (deltaFilter === "MISSING_SOURCE") return `${scopedCount} 项来源问题`;
    if (deltaFilter === "MERGED_SOURCE") return `${scopedCount} 项合并格`;
    if (deltaFilter === "UNKNOWN") return `${unknownCount} 项待确认`;
    if (deltaFilter === "INFERRED") return `${inferredCount} 项规则推断`;
    return differenceCountLabel(targetSummaries, totalDifferenceCount);
  })();

  const differenceDetail = (() => {
    if (deltaFilter === "COMMON" || isEvidenceDeltaFilter(deltaFilter)) return tableDetail;
    const featureScopeText = differenceFeatureScopeText(targetSummaries, affectedCount, "row");
    if (deltaFilter === "ALL") return `${featureScopeText}；点击“查看差异行”可收窄到差异行，完整表格仍可恢复。`;
    return `${featureScopeText}；当前已按${simpleDeltaFilterLabel(deltaFilter) ?? "当前口径"}收窄，点“恢复全部配置行”可回到完整 xlsx 表。`;
  })();
  const differenceLabel = deltaFilter === "COMMON"
    ? "共同配置"
    : isEvidenceDeltaFilter(deltaFilter)
      ? "证据范围"
      : targetSummaries.length > 1
        ? "目标累计差异"
        : "差异行";

  const focusValue = (() => {
    if (deltaFilter === "COMMON") return "共同基线";
    if (deltaFilter === "MISSING_SOURCE") return "补来源证据";
    if (deltaFilter === "MERGED_SOURCE") return "核对合并格";
    return focusPreviewText || focusTextValue || "暂无主要差异";
  })();

  const focusDetail = (() => {
    if (deltaFilter === "COMMON") return "用于确认同车型不同 trim 的共性配置。";
    if (deltaFilter === "MISSING_SOURCE") return "缺值或缺来源不能直接转成配置卖点或短板。";
    if (deltaFilter === "MERGED_SOURCE") return "合并格展开值需要回看原始单元格和合并范围。";
    return focusTextValue ? "按业务维度归纳并列出样例配置；表格大类仍保留原始来源分类。" : "当前范围没有可归纳的业务差异。";
  })();
  const nextDetail = leadStatus
    ? (() => {
        const targetText = compactConclusionStatusTargets(leadStatus);
        const actionText = conclusionStatusActionHint(leadStatus);
        return actionText.includes(targetText) ? `${actionText}。` : `${actionText}；${targetText}。`;
      })()
    : "可切换目标列、配置大类或差异口径继续分析。";

  return [
    {
      key: "base",
      label: "基准列",
      value: trimLabel(baseTrim),
      detail: "新增、减少、参数变化都按这列来判断。",
      tone: "neutral",
    },
    {
      key: "scenario",
      label: "对比口径",
      value: scenarioItem.value,
      detail: scenarioItem.detail,
      tone: scenarioGuideTone(scenarioItem),
      sourceTargetTrimId: scenarioItem.sourceTargetTrimId,
    },
    {
      key: "table",
      label: "当前表格",
      value: tableValue,
      detail: tableDetail,
      tone: deltaFilter === "ALL" ? "neutral" : "review",
    },
    {
      key: "difference",
      label: differenceLabel,
      value: differenceValue,
      detail: differenceDetail,
      tone: unknownCount > 0 ? "warning" : inferredCount > 0 ? "review" : totalDifferenceCount > 0 ? "ready" : "neutral",
      filter: deltaFilter === "ALL" ? "DIFFERENCE" : deltaFilter,
      targetTrimId: null,
    },
    {
      key: "focus",
      label: "主要差异",
      value: focusValue,
      detail: focusDetail,
      tone: deltaFilter === "MISSING_SOURCE" ? "warning" : deltaFilter === "MERGED_SOURCE" || inferredCount > 0 ? "review" : "neutral",
    },
    {
      key: "next",
      label: "下一步",
      value: leadStatus ? leadStatus.label : "继续巡检",
      detail: nextDetail,
      tone: leadStatus?.tone ?? "neutral",
      filter: leadStatus?.focusFilter,
      targetTrimId: leadStatus?.focusTargetTrimId,
      sourceTargetTrimId: leadStatus?.focusAction === "source" ? leadStatus.focusTargetTrimId : null,
    },
  ];
}

function focusedTargetQueue(
  scopedTargetSummaries: TrimDeltaSummary[],
  targetTrimFilterId: string | null | undefined,
  deltaFilter: ConfigComparisonDeltaFilter,
): FocusedTargetQueue | null {
  if (!targetTrimFilterId) return null;
  const queue = scopedTargetSummaries.filter((targetSummary) => targetSummary.deltas.length > 0);
  const currentIndex = queue.findIndex((targetSummary) => targetSummary.targetTrim.trimId === targetTrimFilterId);
  if (currentIndex < 0 || queue.length < 2) return null;
  const nextTarget = queue[(currentIndex + 1) % queue.length];
  return {
    currentIndex: currentIndex + 1,
    total: queue.length,
    currentLabel: trimLabel(queue[currentIndex].targetTrim),
    nextTrimId: nextTarget.targetTrim.trimId,
    nextLabel: trimLabel(nextTarget.targetTrim),
    filterLabel: deltaFilterLabel(deltaFilter) ?? "当前口径",
  };
}

function allConclusionDraftCopyText(
  baseTrim: CompareTrimItem,
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
  scopedLabel: string | null,
): string {
  const scopeText = scopedLabel ? `Scope: ${scopedLabel}` : "Scope: 全部目标配置列";
  const statusItems = conclusionStatusSummaryItems(baseTrim, targetSummaries, deltaFilter);
  const statusText = conclusionStatusSummaryText(statusItems);
  const firstActionText = conclusionStatusFirstActionText(statusItems);
  const actionOrderText = conclusionStatusActionOrderText(statusItems);
  const drafts = targetSummaries.map((targetSummary, index) => {
    const draft = targetConclusionDraft(targetSummary, baseTrim, deltaFilter);
    return [`Target ${index + 1}`, conclusionDraftCopyText(targetSummary.targetTrim, baseTrim, draft)].join("\n");
  });
  return [
    "Config comparison conclusion drafts",
    `Base trim: ${trimLabel(baseTrim)}`,
    scopeText,
    `Target count: ${targetSummaries.length}`,
    `Status summary: ${statusText}`,
    `First action target: ${firstActionText}`,
    `Action order: ${actionOrderText}`,
    ...drafts.map((draft) => `---\n${draft}`),
  ].join("\n");
}

function targetActionGuidance(summary: TrimDeltaSummary, baseTrim: CompareTrimItem, deltaFilter: ConfigComparisonDeltaFilter): TargetActionGuidance {
  if (deltaFilter === "MISSING_SOURCE") {
    return {
      tone: "warning",
      label: "补来源",
      title: "先补来源证据",
      detail: `${summary.deltas.length} 项配置缺少来源证据，建议补传来源文件快照或重新消化原始文件。`,
      filter: "MISSING_SOURCE",
    };
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return {
      tone: "review",
      label: "核对合并格",
      title: "确认共通参数展开",
      detail: `${summary.deltas.length} 项配置来自合并格展开，适合核对 Excel 共通项是否正确映射到各 trim。`,
      filter: "MERGED_SOURCE",
    };
  }
  if (deltaFilter === "COMMON") {
    return {
      tone: "neutral",
      label: "共同基线",
      title: "适合沉淀基础配置",
      detail: `${summary.deltas.length} 项共同配置可作为同车型共性基线，差异话术不从这里生成。`,
      filter: "COMMON",
    };
  }
  if (summary.unknownCount > 0) {
    return {
      tone: "warning",
      label: "先补证据",
      title: "暂不直接输出确定结论",
      detail: `${summary.unknownCount} 项待确认仍缺明确配置状态，需要先补来源证据或重新消化来源。`,
      filter: "UNKNOWN",
    };
  }
  if (summary.inferredCount > 0) {
    return {
      tone: "review",
      label: "核对推断",
      title: "业务结论需带证据口径",
      detail: `${summary.inferredCount} 项为规则推断，带 * 的“不配备”不能当作 Excel 原文直接引用。`,
      filter: "INFERRED",
    };
  }
  const contextGuard = targetContextActionGuard(baseTrim, summary.targetTrim);
  if (contextGuard) return contextGuard;
  if (summary.totalDifferenceCount > 0) {
    const changeParts = [
      summary.addedCount > 0 ? `新增 ${summary.addedCount}` : null,
      summary.removedCount > 0 ? `减少 ${summary.removedCount}` : null,
      summary.valueChangedCount > 0 ? `参数变化 ${summary.valueChangedCount}` : null,
      summary.optionalCount > 0 ? `选装变化 ${summary.optionalCount}` : null,
    ].filter((part): part is string => Boolean(part));
    return {
      tone: "ready",
      label: "可转话术",
      title: "可进入版本卖点整理",
      detail: `${changeParts.join("，") || `${summary.totalDifferenceCount} 项差异`} 已有明确状态，可继续转成卖点、短板或配置层级说明。`,
      filter: "DIFFERENCE",
    };
  }
  return {
    tone: "neutral",
    label: "暂无动作",
    title: "当前范围没有业务差异",
    detail: "可保留为同配置证明，或切换差异项、目标配置列、配置大类继续分析。",
  };
}

function sortedDeltas(deltas: ConfigDelta[]): ConfigDelta[] {
  return [...deltas].sort((a, b) => {
    if (Number(b.inferred) !== Number(a.inferred)) return Number(b.inferred) - Number(a.inferred);
    const categoryRank = a.row.category.localeCompare(b.row.category, undefined, { numeric: true, sensitivity: "base" });
    if (categoryRank !== 0) return categoryRank;
    return a.row.featureName.localeCompare(b.row.featureName, undefined, { numeric: true, sensitivity: "base" });
  });
}

function sectionDeltaGroup(summary: TrimDeltaSummary, types: ConfigDeltaType[]): { deltas: ConfigDelta[]; hiddenDeltas: ConfigDelta[] } {
  const matchingDeltas = sortedDeltas(summary.deltas.filter((delta) => types.includes(delta.deltaType)));
  return {
    deltas: matchingDeltas.slice(0, SECTION_DELTA_LIMIT),
    hiddenDeltas: matchingDeltas.slice(SECTION_DELTA_LIMIT),
  };
}

function deltaMatchesFilter(delta: ConfigDelta, deltaFilter: ConfigComparisonDeltaFilter): boolean {
  if (deltaFilter === "ALL") return true;
  if (deltaFilter === "DIFFERENCE") return delta.deltaType !== "SAME";
  if (deltaFilter === "INFERRED") return delta.deltaType !== "SAME" && delta.inferred;
  if (deltaFilter === "MISSING_SOURCE") return delta.row.values.some((value) => !value?.source);
  if (deltaFilter === "MERGED_SOURCE") return delta.row.values.some((value) => {
    const source = value?.source;
    return Boolean(source?.mergedRange && source.sourceCell && source.sourceCell !== source.cell);
  });
  if (deltaFilter === "COMMON") return delta.deltaType === "SAME";
  return delta.deltaType === deltaFilter;
}

function scopedTargetSummary(summary: TrimDeltaSummary, categoryFilter?: string | null, searchValue = "", deltaFilter: ConfigComparisonDeltaFilter = "ALL"): TrimDeltaSummary {
  const deltas = summary.deltas.filter((delta) => (
    (!categoryFilter || delta.row.category === categoryFilter)
    && rowMatchesConfigSearch(delta.row, searchValue)
    && deltaMatchesFilter(delta, deltaFilter)
  ));
  if (!categoryFilter && !searchValue.trim() && deltaFilter === "ALL") return summary;
  const differenceDeltas = deltas.filter((delta) => delta.deltaType !== "SAME");
  return {
    ...summary,
    totalDifferenceCount: differenceDeltas.length,
    addedCount: differenceDeltas.filter((delta) => delta.deltaType === "ADDED").length,
    removedCount: differenceDeltas.filter((delta) => delta.deltaType === "REMOVED").length,
    optionalCount: differenceDeltas.filter((delta) => delta.deltaType === "OPTIONAL_CHANGED").length,
    valueChangedCount: differenceDeltas.filter((delta) => delta.deltaType === "VALUE_CHANGED").length,
    inferredCount: differenceDeltas.filter((delta) => delta.inferred).length,
    unknownCount: differenceDeltas.filter((delta) => delta.deltaType === "UNKNOWN").length,
    categorySummaries: differenceDeltas.length > 0
      ? Array.from(new Set(differenceDeltas.map((delta) => delta.row.category))).map((category) => {
          const categoryDeltas = differenceDeltas.filter((delta) => delta.row.category === category);
          return {
            category,
            totalDifferenceCount: categoryDeltas.length,
            addedCount: categoryDeltas.filter((delta) => delta.deltaType === "ADDED").length,
            removedCount: categoryDeltas.filter((delta) => delta.deltaType === "REMOVED").length,
            optionalCount: categoryDeltas.filter((delta) => delta.deltaType === "OPTIONAL_CHANGED").length,
            valueChangedCount: categoryDeltas.filter((delta) => delta.deltaType === "VALUE_CHANGED").length,
            inferredCount: categoryDeltas.filter((delta) => delta.inferred).length,
            unknownCount: categoryDeltas.filter((delta) => delta.deltaType === "UNKNOWN").length,
          };
        })
      : [],
    deltas,
  };
}

function affectedFeatureCount(targetSummaries: TrimDeltaSummary[]): number {
  const featureKeys = new Set<string>();
  targetSummaries.forEach((summary) => {
    summary.deltas.forEach((delta) => {
      if (delta.deltaType === "SAME") return;
      featureKeys.add(`${delta.row.category}::${delta.row.featureCode}`);
    });
  });
  return featureKeys.size;
}

function scopedFeatureCount(targetSummaries: TrimDeltaSummary[]): number {
  const featureKeys = new Set<string>();
  targetSummaries.forEach((summary) => {
    summary.deltas.forEach((delta) => {
      featureKeys.add(`${delta.row.category}::${delta.row.featureCode}`);
    });
  });
  return featureKeys.size;
}

function differenceCountLabel(targetSummaries: TrimDeltaSummary[], totalDifferenceCount: number): string {
  return targetSummaries.length > 1 ? `目标累计 ${totalDifferenceCount}` : `${totalDifferenceCount} 个差异`;
}

function differenceFeatureScopeText(targetSummaries: TrimDeltaSummary[], affectedCount: number, scopeUnit: "feature" | "row" = "feature"): string {
  if (targetSummaries.length > 1) return `表格差异行 ${affectedCount} 行`;
  return scopeUnit === "row" ? `涉及 ${affectedCount} 行配置` : `涉及 ${affectedCount} 个配置项`;
}

function featureCountForDeltas(deltas: ConfigDelta[]): number {
  const featureKeys = new Set<string>();
  deltas.forEach((delta) => {
    featureKeys.add(`${delta.row.category}::${delta.row.featureCode}`);
  });
  return featureKeys.size;
}

function aggregateCategorySummaries(targetSummaries: TrimDeltaSummary[]): CategoryDeltaSummary[] {
  const categoryMap = new Map<string, CategoryDeltaSummary>();
  targetSummaries.forEach((summary) => {
    summary.categorySummaries.forEach((categorySummary) => {
      const existing = categoryMap.get(categorySummary.category) ?? {
        category: categorySummary.category,
        totalDifferenceCount: 0,
        addedCount: 0,
        removedCount: 0,
        optionalCount: 0,
        valueChangedCount: 0,
        inferredCount: 0,
        unknownCount: 0,
      };
      existing.totalDifferenceCount += categorySummary.totalDifferenceCount;
      existing.addedCount += categorySummary.addedCount;
      existing.removedCount += categorySummary.removedCount;
      existing.optionalCount += categorySummary.optionalCount;
      existing.valueChangedCount += categorySummary.valueChangedCount;
      existing.inferredCount += categorySummary.inferredCount;
      existing.unknownCount += categorySummary.unknownCount;
      categoryMap.set(categorySummary.category, existing);
    });
  });
  return Array.from(categoryMap.values()).sort((a, b) => {
    if (b.totalDifferenceCount !== a.totalDifferenceCount) return b.totalDifferenceCount - a.totalDifferenceCount;
    return a.category.localeCompare(b.category, undefined, { numeric: true, sensitivity: "base" });
  });
}

function strongestTargetSummary(targetSummaries: TrimDeltaSummary[]): TrimDeltaSummary | null {
  return targetSummaries.reduce<TrimDeltaSummary | null>((strongest, summary) => {
    if (!strongest) return summary;
    if (summary.totalDifferenceCount !== strongest.totalDifferenceCount) {
      return summary.totalDifferenceCount > strongest.totalDifferenceCount ? summary : strongest;
    }
    return trimLabel(summary.targetTrim).localeCompare(trimLabel(strongest.targetTrim), undefined, { numeric: true, sensitivity: "base" }) < 0
      ? summary
      : strongest;
  }, null);
}

function baseStorylineItems(baseTrim: CompareTrimItem, targetSummaries: TrimDeltaSummary[], deltaFilter: ConfigComparisonDeltaFilter): BaseStorylineItem[] {
  const totalDifferenceCount = targetSummaries.reduce((total, item) => total + item.totalDifferenceCount, 0);
  const addedCount = targetSummaries.reduce((total, item) => total + item.addedCount, 0);
  const removedCount = targetSummaries.reduce((total, item) => total + item.removedCount, 0);
  const valueChangedCount = targetSummaries.reduce((total, item) => total + item.valueChangedCount, 0);
  const optionalCount = targetSummaries.reduce((total, item) => total + item.optionalCount, 0);
  const inferredCount = targetSummaries.reduce((total, item) => total + item.inferredCount, 0);
  const unknownCount = targetSummaries.reduce((total, item) => total + item.unknownCount, 0);
  const differenceDeltas = targetSummaries.flatMap((summary) => summary.deltas.filter((delta) => delta.deltaType !== "SAME"));
  const featureCount = affectedFeatureCount(targetSummaries);
  const inferredFeatureCount = featureCountForDeltas(differenceDeltas.filter((delta) => delta.inferred));
  const unknownFeatureCount = featureCountForDeltas(differenceDeltas.filter((delta) => delta.deltaType === "UNKNOWN"));
  const commonFeatureCount = scopedFeatureCount(targetSummaries);
  const focusSummaries = aggregateBusinessFocusGroups(targetSummaries);
  const directionText = [
    addedCount > 0 ? `新增 ${addedCount}` : null,
    removedCount > 0 ? `减少 ${removedCount}` : null,
    valueChangedCount > 0 ? `值变化 ${valueChangedCount}` : null,
    optionalCount > 0 ? `选装变化 ${optionalCount}` : null,
    unknownCount > 0 ? `待确认 ${unknownCount}` : null,
  ].filter((text): text is string => Boolean(text)).join(" · ");
  const focusTextValue = focusSummaries.slice(0, 2)
    .map((focus) => `${focus.label} ${focus.count}`)
    .join(" · ");
  const scenarioItem = comparisonScenarioStorylineItem(baseTrim, targetSummaries);

  if (deltaFilter === "MISSING_SOURCE" || deltaFilter === "MERGED_SOURCE") {
    const evidenceDeltaCount = targetSummaries.reduce((total, item) => total + item.deltas.length, 0);
    const evidenceFeatureCount = scopedFeatureCount(targetSummaries);
    const isMissingSource = deltaFilter === "MISSING_SOURCE";
    return [
      {
        key: "base",
        label: "基准角色",
        value: trimLabel(baseTrim),
        detail: `${targetSummaries.length} 个目标配置列的证据状态仍相对该配置核对。`,
      },
      scenarioItem,
      {
        key: "scope",
        label: isMissingSource ? "来源问题" : "合并格展开",
        value: `${evidenceFeatureCount} 项`,
        detail: `跨目标配置列共 ${evidenceDeltaCount} 条证据记录；当前口径用于核对来源，不生成差异话术。`,
        filter: deltaFilter,
      },
      {
        key: "evidence",
        label: "处理建议",
        value: isMissingSource ? "先补来源证据" : "核对共通参数",
        detail: isMissingSource
          ? "来源问题配置应优先补传来源文件快照或重新消化来源文件。"
          : "合并格展开值需要确认是否应该同步到当前配置列。",
        filter: deltaFilter,
      },
    ];
  }

  if (deltaFilter === "COMMON") {
    return [
      {
        key: "base",
        label: "基准角色",
        value: trimLabel(baseTrim),
        detail: `${targetSummaries.length} 个目标配置列的共同项均相对该配置核对。`,
      },
      scenarioItem,
      {
        key: "direction",
        label: "共同基线",
        value: `${commonFeatureCount} 项`,
        detail: "当前口径用于沉淀同车型共性配置，不生成差异结论。",
        filter: "COMMON",
      },
      {
        key: "evidence",
        label: "证据边界",
        value: "可追溯",
        detail: "共同项仍保留来源、合并格和推断字段，避免只看展示值。",
        filter: "COMMON",
      },
    ];
  }

  const evidenceItem: BaseStorylineItem = unknownCount > 0
    ? {
        key: "evidence",
        label: "证据优先",
        value: `待确认目标差异 ${unknownCount}`,
        detail: `配置行去重 ${unknownFeatureCount} 行；先补来源证据，再输出确定配置结论。`,
        filter: "UNKNOWN",
      }
    : inferredCount > 0
      ? {
          key: "evidence",
          label: "证据边界",
          value: `规则推断目标差异 ${inferredCount}`,
          detail: `配置行去重 ${inferredFeatureCount} 行；带 * 的不配备不能当作 Excel 原文。`,
          filter: "INFERRED",
        }
      : {
          key: "evidence",
          label: "证据状态",
          value: totalDifferenceCount > 0 ? "状态明确" : "暂无差异",
          detail: totalDifferenceCount > 0 ? "当前差异可继续转成版本卖点、短板或配置层级说明。" : "当前口径下没有需要解释的业务差异。",
          filter: totalDifferenceCount > 0 ? "DIFFERENCE" : undefined,
        };

  return [
      {
        key: "base",
        label: "基准角色",
        value: trimLabel(baseTrim),
        detail: `${targetSummaries.length} 个目标配置列的新增、减少和参数变化均相对该配置判断。`,
      },
      scenarioItem,
      {
        key: "direction",
      label: "业务方向",
      value: totalDifferenceCount > 0 ? `目标差异 ${totalDifferenceCount}` : "暂无业务差异",
      detail: totalDifferenceCount > 0 ? `${directionText}；配置行去重 ${featureCount} 行。` : "当前范围可作为同配置证明。",
      filter: totalDifferenceCount > 0 ? "DIFFERENCE" : undefined,
    },
    {
      key: "category",
      label: "集中维度",
      value: focusTextValue ? `重点：${focusTextValue}` : "暂无业务维度差异",
      detail: focusSummaries.length > 2
        ? `按业务维度归纳；另有 ${focusSummaries.length - 2} 个维度存在差异，表格大类仍保留来源分类。`
        : "按业务维度归纳；表格大类仍保留来源分类。",
    },
    evidenceItem,
  ];
}

function baselineNarrativeItems(baseTrim: CompareTrimItem, targetSummaries: TrimDeltaSummary[], deltaFilter: ConfigComparisonDeltaFilter): BaselineNarrativeItem[] {
  const totalDifferenceCount = targetSummaries.reduce((total, item) => total + item.totalDifferenceCount, 0);
  const inferredCount = targetSummaries.reduce((total, item) => total + item.inferredCount, 0);
  const unknownCount = targetSummaries.reduce((total, item) => total + item.unknownCount, 0);
  const visibleFeatureCount = scopedFeatureCount(targetSummaries);
  const visibleTargetCount = targetSummaries.reduce((total, item) => total + item.deltas.length, 0);
  const featureCount = affectedFeatureCount(targetSummaries);
  const categorySummaries = aggregateCategorySummaries(targetSummaries);
  const focusSummaries = aggregateBusinessFocusGroups(targetSummaries);
  const strongestTarget = strongestTargetSummary(targetSummaries);
  const strongestFocusGroup = strongestTarget ? businessFocusGroups(strongestTarget)[0] ?? null : null;
  const categoryValue = focusSummaries.slice(0, 2)
    .map((focus) => `${focus.label} ${focus.count}`)
    .join(" · ") || "暂无业务维度差异";
  const evidenceValue = inferredCount > 0
    ? `规则推断差异 ${inferredCount}`
    : unknownCount > 0
      ? `待确认 ${unknownCount}`
      : "无推断差异";
  const evidenceDetail = inferredCount > 0
    ? "按目标配置列口径统计；不配备* 不是 Excel 原文。"
    : unknownCount > 0
      ? "待确认项会保留，不自动按无配置处理。"
      : "当前范围未出现规则推断或待确认差异。";

  if (deltaFilter === "MISSING_SOURCE" || deltaFilter === "MERGED_SOURCE") {
    const evidenceFeatureCount = scopedFeatureCount(targetSummaries);
    const evidenceDeltaCount = targetSummaries.reduce((total, item) => total + item.deltas.length, 0);
    const isMissingSource = deltaFilter === "MISSING_SOURCE";
    return [
      {
        label: "基准配置",
        value: trimLabel(baseTrim),
        detail: "证据核对仍以该 trim 作为对比基准。",
      },
      {
        label: isMissingSource ? "来源问题" : "合并格展开",
        value: `${evidenceFeatureCount} 项`,
        detail: `按配置行去重；跨目标配置列共有 ${evidenceDeltaCount} 条证据记录。`,
      },
      {
        label: "覆盖目标",
        value: `${targetSummaries.length} 个 trim`,
        detail: "当前范围用于核对来源证据，不直接生成业务差异结论。",
      },
      {
        label: "处理建议",
        value: isMissingSource ? "补证据" : "核对展开",
        detail: isMissingSource
          ? "优先补齐缺值或来源证据，避免把来源问题误读成确定差异。"
          : "核对合并单元格展开是否符合原始配置表的共通项表达。",
      },
    ];
  }

  if (deltaFilter === "COMMON") {
    return [
      {
        label: "基准配置",
        value: trimLabel(baseTrim),
        detail: "共同配置均相对该 trim 判断。",
      },
      {
        label: "共同配置",
        value: `${visibleFeatureCount} 项`,
        detail: `按配置行去重；跨目标配置列共有 ${visibleTargetCount} 条一致判断。`,
      },
      {
        label: "覆盖目标",
        value: `${targetSummaries.length} 个 trim`,
        detail: "用于确认同车型不同配置之间保持一致的配置项。",
      },
      {
        label: "证据状态",
        value: "按来源追溯",
        detail: "点击表格单元格可查看原始值、合并格展开和推断说明。",
      },
    ];
  }

  return [
    {
      label: "基准配置",
      value: trimLabel(baseTrim),
      detail: "新增、减少和值变化均相对该 trim 判断。",
    },
    {
      label: "差异最大",
      value: strongestTarget && strongestTarget.totalDifferenceCount > 0
        ? `${trimLabel(strongestTarget.targetTrim)} · ${strongestTarget.totalDifferenceCount}`
        : "暂无差异",
      detail: strongestFocusGroup
        ? `集中在 ${strongestFocusGroup.label} ${strongestFocusGroup.deltas.length} 项。`
        : "当前范围没有业务差异。",
    },
    {
      label: "集中维度",
      value: categoryValue,
      detail: `累计 ${totalDifferenceCount} 个目标差异，涉及 ${featureCount} 行配置；表格大类仍保留来源分类。`,
      categories: categorySummaries.slice(0, 2),
    },
    {
      label: "证据状态",
      value: evidenceValue,
      detail: evidenceDetail,
    },
  ];
}

function baseNarrative(
  summary: ReturnType<typeof buildBusinessDifferenceSummary>,
  targetSummaries: TrimDeltaSummary[],
  categoryFilter?: string | null,
  searchValue?: string,
  deltaFilter?: ConfigComparisonDeltaFilter,
  targetTrim?: CompareTrimItem | null,
): string {
  if (!summary.baseTrim) return "";
  const totalDifferenceCount = targetSummaries.reduce((total, item) => total + item.totalDifferenceCount, 0);
  const featureCount = affectedFeatureCount(targetSummaries);
  const scopedLabel = panelScopeLabel(categoryFilter, searchValue, deltaFilter, targetTrim);
  const scopeText = scopedLabel ? `当前聚焦 ${scopedLabel}，` : "";
  if (deltaFilter === "COMMON") {
    return `${trimLabel(summary.baseTrim)} 作为基准列，${scopeText}当前对比 ${targetSummaries.length} 个目标配置列，当前范围包含 ${scopedFeatureCount(targetSummaries)} 行共同配置。`;
  }
  if (deltaFilter === "UNKNOWN") {
    return `${trimLabel(summary.baseTrim)} 作为基准列，${scopeText}当前对比 ${targetSummaries.length} 个目标配置列，当前范围包含 ${totalDifferenceCount} 个待确认项，涉及 ${featureCount} 行配置。`;
  }
  if (deltaFilter === "INFERRED") {
    return `${trimLabel(summary.baseTrim)} 作为基准列，${scopeText}当前对比 ${targetSummaries.length} 个目标配置列，当前范围包含 ${totalDifferenceCount} 个规则推断差异，涉及 ${featureCount} 行配置。`;
  }
  if (deltaFilter === "MISSING_SOURCE") {
    return `${trimLabel(summary.baseTrim)} 作为基准列，${scopeText}当前对比 ${targetSummaries.length} 个目标配置列，当前范围包含 ${scopedFeatureCount(targetSummaries)} 行来源问题配置。`;
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return `${trimLabel(summary.baseTrim)} 作为基准列，${scopeText}当前对比 ${targetSummaries.length} 个目标配置列，当前范围包含 ${scopedFeatureCount(targetSummaries)} 行合并格展开配置。`;
  }
  return `${trimLabel(summary.baseTrim)} 作为基准列，${scopeText}当前对比 ${targetSummaries.length} 个目标配置列，累计 ${totalDifferenceCount} 个目标差异，涉及 ${featureCount} 行配置。`;
}

function excelGuideTitle(scopedLabel: string | null): string {
  return scopedLabel ? `${scopedLabel} Excel 对比导读` : "Excel 配置对比导读";
}

function excelGuideScopeDetail(deltaFilter: ConfigComparisonDeltaFilter, scopedLabel: string | null): string {
  if (deltaFilter === "COMMON") return "当前表格只看共同配置行，用于核对哪些配置在这些列里保持一致。";
  if (deltaFilter === "INFERRED") return "当前表格只看规则推断行，带 * 的值需要回到来源单元格核对。";
  if (deltaFilter === "UNKNOWN") return "当前表格只看待确认行，空白不会自动当作无配置。";
  if (deltaFilter === "MISSING_SOURCE") return "当前表格只看缺值或缺来源行，先补来源再解释差异。";
  if (deltaFilter === "MERGED_SOURCE") return "当前表格只看合并格展开行，用于核对 xlsx 共通项。";
  if (scopedLabel) return "当前表格按这个口径查看；完整配置行可随时恢复。";
  return "默认展示全部 xlsx 配置行；上方只把差异先摘出来，点“查看差异行”才会收窄表格。";
}

function excelGuideNarrative(
  baseTrim: CompareTrimItem,
  targetSummaries: TrimDeltaSummary[],
  deltaFilter: ConfigComparisonDeltaFilter,
  scopedLabel: string | null,
): string {
  const totalDifferenceCount = targetSummaries.reduce((total, item) => total + item.totalDifferenceCount, 0);
  const featureCount = affectedFeatureCount(targetSummaries);
  const scopedPrefix = scopedLabel ? `当前查看 ${scopedLabel}，` : "";
  if (deltaFilter === "COMMON") {
    return `基准列 ${trimLabel(baseTrim)}；${scopedPrefix}当前对比 ${targetSummaries.length} 个目标配置列，包含 ${scopedFeatureCount(targetSummaries)} 个共同配置行。`;
  }
  if (deltaFilter === "UNKNOWN") {
    return `基准列 ${trimLabel(baseTrim)}；${scopedPrefix}当前对比 ${targetSummaries.length} 个目标配置列，包含 ${totalDifferenceCount} 个待确认行，涉及 ${featureCount} 行配置。`;
  }
  if (deltaFilter === "INFERRED") {
    return `基准列 ${trimLabel(baseTrim)}；${scopedPrefix}当前对比 ${targetSummaries.length} 个目标配置列，包含 ${totalDifferenceCount} 个规则推断行，涉及 ${featureCount} 行配置。`;
  }
  if (deltaFilter === "MISSING_SOURCE") {
    return `基准列 ${trimLabel(baseTrim)}；${scopedPrefix}当前对比 ${targetSummaries.length} 个目标配置列，包含 ${scopedFeatureCount(targetSummaries)} 个来源问题行。`;
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return `基准列 ${trimLabel(baseTrim)}；${scopedPrefix}当前对比 ${targetSummaries.length} 个目标配置列，包含 ${scopedFeatureCount(targetSummaries)} 个合并格展开行。`;
  }
  if (targetSummaries.length > 1) {
    return `基准列 ${trimLabel(baseTrim)}；${scopedPrefix}当前对比 ${targetSummaries.length} 个目标配置列，${differenceCountLabel(targetSummaries, totalDifferenceCount)}，${differenceFeatureScopeText(targetSummaries, featureCount, "row")}。`;
  }
  return `基准列 ${trimLabel(baseTrim)}；${scopedPrefix}当前对比 ${targetSummaries.length} 个目标配置列，发现 ${totalDifferenceCount} 个差异，涉及 ${featureCount} 行配置。`;
}

function targetMetricItems(targetSummary: TrimDeltaSummary, deltaFilter: ConfigComparisonDeltaFilter): Array<{ key: string; label: string; count: number }> {
  if (deltaFilter === "MISSING_SOURCE") {
    return [{ key: "missing-source", label: "来源问题", count: targetSummary.deltas.length }];
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return [{ key: "merged-source", label: "合并格展开", count: targetSummary.deltas.length }];
  }
  if (deltaFilter === "COMMON") {
    return [{ key: "common", label: "共同配置", count: targetSummary.deltas.length }];
  }
  return [
    { key: "added", label: "新增配置", count: targetSummary.addedCount },
    { key: "removed", label: "减少配置", count: targetSummary.removedCount },
    { key: "optional", label: "选装变化", count: targetSummary.optionalCount },
    { key: "value", label: "值变化", count: targetSummary.valueChangedCount },
    { key: "inferred", label: "规则推断", count: targetSummary.inferredCount },
    { key: "unknown", label: "待确认", count: targetSummary.unknownCount },
  ];
}

function commonConfigGroup(summary: TrimDeltaSummary): { deltas: ConfigDelta[]; hiddenDeltas: ConfigDelta[] } {
  const matchingDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "SAME"));
  return {
    deltas: matchingDeltas.slice(0, SECTION_DELTA_LIMIT),
    hiddenDeltas: matchingDeltas.slice(SECTION_DELTA_LIMIT),
  };
}

function targetInsightItems(summary: TrimDeltaSummary, deltaFilter: ConfigComparisonDeltaFilter): TargetInsightItem[] {
  if (deltaFilter === "MISSING_SOURCE") {
    return [{
      key: "missing-source",
      label: "证据提示",
      value: `${summary.deltas.length} 项配置存在缺值或缺来源，先补来源证据。`,
      filter: "MISSING_SOURCE",
    }];
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return [{
      key: "merged-source",
      label: "证据提示",
      value: `${summary.deltas.length} 项配置来自合并格展开，先核对共通参数。`,
      filter: "MERGED_SOURCE",
    }];
  }
  if (deltaFilter === "COMMON") {
    const commonDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "SAME"));
    return [{
      key: "common",
      label: "共同基线",
      value: commonDeltas.length > 0 ? compactCategoryFeatureText(commonDeltas) : "当前范围没有共同配置行",
      filter: "COMMON",
    }];
  }

  const addedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "ADDED"));
  const removedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "REMOVED"));
  const valueChangedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "VALUE_CHANGED"));
  const optionalChangedDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "OPTIONAL_CHANGED"));
  const unknownDeltas = sortedDeltas(summary.deltas.filter((delta) => delta.deltaType === "UNKNOWN"));
  const swapInsights = optionSwapInsights(summary);
  const items: TargetInsightItem[] = [];

  if (swapInsights.length > 0) {
    items.push({
      key: `swap-${swapInsights.map((item) => item.key).join("-")}`,
      label: "升级线索",
      value: compactOptionSwapInsightText(swapInsights),
      filter: "DIFFERENCE",
    });
  }
  if (addedDeltas.length > 0) {
    items.push({ key: "added", label: "增配重点", value: compactBusinessFocusFeatureText(addedDeltas), filter: "ADDED" });
  }
  if (removedDeltas.length > 0) {
    items.push({ key: "removed", label: "删减重点", value: compactBusinessFocusFeatureText(removedDeltas), filter: "REMOVED" });
  }
  if (valueChangedDeltas.length > 0) {
    items.push({ key: "changed", label: "参数变化", value: compactBusinessFocusFeatureText(valueChangedDeltas), filter: "VALUE_CHANGED" });
  }
  if (optionalChangedDeltas.length > 0) {
    items.push({ key: "optional", label: "选装变化", value: compactBusinessFocusFeatureText(optionalChangedDeltas), filter: "OPTIONAL_CHANGED" });
  }
  if (unknownDeltas.length > 0) {
    items.push({ key: "unknown", label: "待确认", value: compactBusinessFocusFeatureText(unknownDeltas), filter: "UNKNOWN" });
  }
  if (summary.inferredCount > 0) {
    items.push({ key: "inferred", label: "证据提示", value: `含规则推断 ${summary.inferredCount} 项，优先点开来源核对。`, filter: "INFERRED" });
  }
  if (items.length === 0) {
    items.push({ key: "none", label: "业务结论", value: "当前范围暂无业务差异。" });
  }
  return items;
}

function overviewFocusFilter(deltaFilter: ConfigComparisonDeltaFilter): ConfigComparisonDeltaFilter {
  return deltaFilter === "ALL" ? "DIFFERENCE" : deltaFilter;
}

function targetOverviewItem(summary: TrimDeltaSummary, deltaFilter: ConfigComparisonDeltaFilter, index: number): VersionLadderItem {
  const swapInsights = optionSwapInsights(summary);
  const strongestFocusGroup = businessFocusGroups(summary)[0] ?? null;
  const hasDifference = summary.totalDifferenceCount > 0;
  const primary = hasDifference ? deltaCountText(summary) : "暂无业务差异";
  const detail = swapInsights.length > 0
    ? `升级线索：${compactOptionSwapInsightText(swapInsights)}`
    : strongestFocusGroup
      ? `集中在 ${strongestFocusGroup.label} ${strongestFocusGroup.deltas.length} 项`
      : hasDifference
        ? "差异分布较分散，建议按大类继续下钻。"
        : "当前范围可作为同配置证明。";
  const evidence = summary.unknownCount > 0
    ? `待确认 ${summary.unknownCount} 项`
    : summary.inferredCount > 0
      ? `含规则推断 ${summary.inferredCount} 项`
      : hasDifference
        ? "证据状态明确"
        : "无差异证据";
  const tone: VersionLadderTone = summary.unknownCount > 0
    ? "warning"
    : summary.inferredCount > 0
      ? "review"
      : hasDifference
        ? "ready"
        : "neutral";
  return {
    key: summary.targetTrim.trimId,
    role: `Target ${index + 1}`,
    trimLabel: trimLabel(summary.targetTrim),
    primary,
    detail,
    evidence,
    tone,
    targetTrimId: summary.targetTrim.trimId,
    filter: overviewFocusFilter(deltaFilter),
  };
}

function versionLadderItems(baseTrim: CompareTrimItem, targetSummaries: TrimDeltaSummary[], deltaFilter: ConfigComparisonDeltaFilter): VersionLadderItem[] {
  const targetDifferenceCount = targetSummaries.reduce((total, item) => total + item.totalDifferenceCount, 0);
  const baseDetail = deltaFilter === "UNKNOWN"
    ? "当前基准用于定位待确认项，不把空白直接当作无配置。"
    : deltaFilter === "INFERRED"
      ? "当前基准用于核对规则推断边界，带 * 的值需要回看来源。"
      : "后续目标配置列的新增、减少和值变化均相对该配置判断。";
  return [
    {
      key: `base-${baseTrim.trimId}`,
      role: "Base",
      trimLabel: trimLabel(baseTrim),
      primary: "当前基准",
      detail: baseDetail,
      evidence: targetDifferenceCount > 0 ? `${targetSummaries.length} 个目标 · ${targetDifferenceCount} 个目标差异` : "当前范围暂无目标差异",
      tone: "base",
    },
    ...targetSummaries.map((targetSummary, index) => targetOverviewItem(targetSummary, deltaFilter, index)),
  ];
}

function versionUpgradeStepItem(
  data: CompareResponse,
  fromTrim: CompareTrimItem,
  toTrim: CompareTrimItem,
  stepIndex: number,
  categoryFilter?: string | null,
  searchValue = "",
  deltaFilter: ConfigComparisonDeltaFilter = "ALL",
): VersionUpgradeStepItem | null {
  const adjacentSummary = buildBusinessDifferenceSummary(data, fromTrim.trimId)
    .targetSummaries.find((targetSummary) => targetSummary.targetTrim.trimId === toTrim.trimId);
  if (!adjacentSummary) return null;
  const scopedSummary = scopedTargetSummary(adjacentSummary, categoryFilter, searchValue, deltaFilter);
  const swapInsights = optionSwapInsights(scopedSummary);
  const strongestFocusGroup = businessFocusGroups(scopedSummary)[0] ?? null;
  const hasDifference = scopedSummary.totalDifferenceCount > 0;
  const primary = hasDifference ? deltaCountText(scopedSummary) : "暂无相邻差异";
  const detail = swapInsights.length > 0
    ? `升级线索：${compactOptionSwapInsightText(swapInsights)}`
    : strongestFocusGroup
      ? `集中在 ${strongestFocusGroup.label} ${strongestFocusGroup.deltas.length} 项`
      : hasDifference
        ? "差异分布较分散，建议按大类继续下钻。"
        : "相邻版本在当前范围保持一致。";
  const evidence = scopedSummary.unknownCount > 0
    ? `待确认 ${scopedSummary.unknownCount} 项`
    : scopedSummary.inferredCount > 0
      ? `含规则推断 ${scopedSummary.inferredCount} 项`
      : hasDifference
        ? "证据状态明确"
        : "无差异证据";
  const tone: VersionLadderTone = scopedSummary.unknownCount > 0
    ? "warning"
    : scopedSummary.inferredCount > 0
      ? "review"
      : hasDifference
        ? "ready"
        : "neutral";
  return {
    key: `${fromTrim.trimId}-${toTrim.trimId}`,
    stepLabel: `Step ${stepIndex + 1}`,
    fromTrim,
    toTrim,
    primary,
    detail,
    evidence,
    tone,
    filter: overviewFocusFilter(deltaFilter),
    actionable: hasDifference,
  };
}

function versionUpgradeStepItems(
  data: CompareResponse,
  orderedTrims: CompareTrimItem[],
  categoryFilter?: string | null,
  searchValue = "",
  deltaFilter: ConfigComparisonDeltaFilter = "ALL",
): VersionUpgradeStepItem[] {
  if (orderedTrims.length < 3) return [];
  return orderedTrims.slice(1)
    .map((toTrim, index) => versionUpgradeStepItem(data, orderedTrims[index], toTrim, index, categoryFilter, searchValue, deltaFilter))
    .filter((item): item is VersionUpgradeStepItem => Boolean(item));
}

function summaryTitle(scopedLabel: string | null, deltaFilter: ConfigComparisonDeltaFilter): string {
  if (!scopedLabel) {
    if (deltaFilter === "ALL") return "配置业务摘要";
    if (deltaFilter === "COMMON") return "共同配置摘要";
    if (deltaFilter === "INFERRED") return "规则推断摘要";
    if (deltaFilter === "UNKNOWN") return "待确认摘要";
    if (deltaFilter === "MISSING_SOURCE") return "来源问题摘要";
    if (deltaFilter === "MERGED_SOURCE") return "合并格来源摘要";
    return "业务差异摘要";
  }
  if (deltaFilter === "ALL") return `${scopedLabel} 业务摘要`;
  return deltaFilter === "COMMON"
    || deltaFilter === "DIFFERENCE"
    || deltaFilter === "UNKNOWN"
    || deltaFilter === "INFERRED"
    || deltaFilter === "MISSING_SOURCE"
    || deltaFilter === "MERGED_SOURCE"
    ? `${scopedLabel} 摘要`
    : `${scopedLabel} 差异摘要`;
}

function summaryScopeNote(deltaFilter: ConfigComparisonDeltaFilter, scopedLabel: string | null): SummaryScopeNote {
  if (deltaFilter === "COMMON") {
    return { label: "摘要口径", detail: "当前统计共同配置行，不计入业务差异结论。" };
  }
  if (deltaFilter === "INFERRED") {
    return { label: "摘要口径", detail: "当前只统计规则推断差异，推断值需回看来源证据。" };
  }
  if (deltaFilter === "UNKNOWN") {
    return { label: "摘要口径", detail: "当前只统计待确认项，空值不会自动当成无配置。" };
  }
  if (deltaFilter === "MISSING_SOURCE") {
    return { label: "摘要口径", detail: "当前只统计缺值或缺少来源证据的配置行，先补来源再解释差异。" };
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return { label: "摘要口径", detail: "当前只统计合并格展开配置行，用于核对共通参数来源。" };
  }
  if (!scopedLabel) {
    return { label: "摘要口径", detail: "表格默认展示全部配置行；摘要只提炼其中的业务差异，查看差异项后才收窄表格。" };
  }
  if (deltaFilter === "ALL") {
    return { label: "摘要口径", detail: "摘要只统计当前范围内的业务差异；表格仍展示当前范围内全部配置行，查看差异项后才收窄。" };
  }
  return { label: "摘要口径", detail: `统计 ${scopedLabel} 范围内的业务差异。` };
}

function scheduleDeferredLlmSummary(callback: () => void, immediate: boolean): () => void {
  if (immediate) {
    callback();
    return () => undefined;
  }
  const schedulerWindow = window as IdleSchedulerWindow;
  let completed = false;
  let idleHandle: number | null = null;
  let timeoutHandle: number | null = null;
  const runOnce = (): void => {
    if (completed) return;
    completed = true;
    if (timeoutHandle !== null) window.clearTimeout(timeoutHandle);
    if (idleHandle !== null) schedulerWindow.cancelIdleCallback?.(idleHandle);
    callback();
  };
  if (schedulerWindow.requestIdleCallback) {
    idleHandle = schedulerWindow.requestIdleCallback(runOnce, { timeout: LLM_SUMMARY_IDLE_TIMEOUT_MS });
  }
  timeoutHandle = window.setTimeout(runOnce, LLM_SUMMARY_FALLBACK_DELAY_MS);
  return () => {
    completed = true;
    if (timeoutHandle !== null) window.clearTimeout(timeoutHandle);
    if (idleHandle !== null) schedulerWindow.cancelIdleCallback?.(idleHandle);
  };
}

function llmSummaryStatusMessage(
  error: string | null,
  fallbackMessage: string,
  compact: boolean,
  hasExistingSummary: boolean,
): string {
  if (!compact) return error || fallbackMessage;
  if (hasExistingSummary) return "AI 摘要刷新暂不可用；当前继续显示上一版摘要。";
  return "AI 摘要暂不可用；配置表和来源证据仍可继续查看。";
}

function llmSummaryUsageLabel(
  usage: EngineeringConfigBusinessSummaryUsage | null,
  status: LlmBusinessSummaryStatus,
  compact: boolean,
  cached: boolean,
): string {
  const cacheReused = cached || usage?.cacheHit === true;
  if (!compact) {
    return usage
      ? `${usage.model} · ${usage.status}${usage.totalTokens ? ` · ${usage.totalTokens} tokens` : ""}${cacheReused ? " · cache hit" : ""}`
      : "AstrBot LLM";
  }
  if (status === "ready" && cacheReused) return "AI 结论已复用";
  if (status === "ready") return "AI 结论已生成";
  if (status === "loading") return "正在生成";
  if (status === "error") return "AI 摘要暂不可用";
  return "AI 摘要准备中";
}

function aiSummaryRuntimeLabel(usage: EngineeringConfigBusinessSummaryUsage | null): string {
  const provider = usage?.provider?.trim();
  const model = usage?.model?.trim();
  if (provider && model) return `${provider} / ${model}`;
  if (model) return model;
  if (provider) return provider;
  return "AI";
}

function llmSummarySourceLabel(
  usage: EngineeringConfigBusinessSummaryUsage | null,
  status: LlmBusinessSummaryStatus,
  compact: boolean,
  cached: boolean,
): string {
  const cacheReused = cached || usage?.cacheHit === true;
  if (compact) {
    if (status === "ready" && cacheReused) {
      return `${aiSummaryRuntimeLabel(usage)} 运行时缓存复用；不是上传文件的持久摘要，引用前点开来源证据核对`;
    }
    if (status === "ready" && usage?.status === "ok") {
      return `由 ${aiSummaryRuntimeLabel(usage)} 运行时生成；不是上传文件的持久摘要，引用前点开来源证据核对`;
    }
    if (status === "loading") return "正在基于配置事实生成 AI 摘要";
    if (status === "error") return "AI 摘要暂不可用；仍可点开来源证据核对";
    return "AI 摘要准备中；配置表可先查看";
  }
  return `${aiSummaryRuntimeLabel(usage)} 运行时生成 · 当前对比实时生成，缓存命中会复用，不是上传文件的持久摘要`;
}

function cachedLlmBusinessSummary(requestKey: string): LlmBusinessSummaryCacheEntry | null {
  const cached = llmBusinessSummaryCache.get(requestKey);
  if (!cached) return null;
  llmBusinessSummaryCache.delete(requestKey);
  llmBusinessSummaryCache.set(requestKey, cached);
  return cached;
}

function rememberLlmBusinessSummary(
  requestKey: string,
  summaries: EngineeringConfigBusinessSummaryItem[],
  usage: EngineeringConfigBusinessSummaryUsage | null,
): void {
  if (summaries.length === 0) return;
  llmBusinessSummaryCache.delete(requestKey);
  llmBusinessSummaryCache.set(requestKey, { summaries, usage });
  while (llmBusinessSummaryCache.size > LLM_SUMMARY_CACHE_LIMIT) {
    const oldestKey = llmBusinessSummaryCache.keys().next().value;
    if (typeof oldestKey !== "string") break;
    llmBusinessSummaryCache.delete(oldestKey);
  }
}

export function clearEngineeringConfigBusinessSummaryCache(): void {
  llmBusinessSummaryCache.clear();
}

function llmSummaryComposePayload(
  payload: EngineeringConfigBusinessSummaryComposeRequest,
  forceRefresh: boolean,
): EngineeringConfigBusinessSummaryComposeRequest {
  if (!forceRefresh) return payload;
  return {
    ...payload,
    filters: {
      ...(payload.filters ?? {}),
      forceRefresh: true,
    },
  };
}

function LlmBusinessSummaryBlock({
  enabled,
  baseTrim,
  targetSummaries,
  deltaFilter,
  versionScope,
  categoryFilter,
  searchValue,
  factSource,
  compact = false,
  deterministicFallbackHidden = false,
  onOpenEvidence,
  onFocusFeatureRow,
  onSummaryChange,
  onSummaryReadyChange,
  onSummaryStatusChange,
}: {
  enabled: boolean;
  baseTrim: CompareTrimItem;
  targetSummaries: TrimDeltaSummary[];
  deltaFilter: ConfigComparisonDeltaFilter;
  versionScope: "published" | "latest";
  categoryFilter?: string | null;
  searchValue?: string;
  factSource?: EngineeringConfigBusinessSummaryComposeRequest["factSource"];
  compact?: boolean;
  deterministicFallbackHidden?: boolean;
  onOpenEvidence: (selection: SourceEvidenceSelection) => void;
  onFocusFeatureRow?: (row: CompareRow, targetTrimId: string | null, filter: ConfigComparisonDeltaFilter) => void;
  onSummaryChange?: (summaries: EngineeringConfigBusinessSummaryItem[], usage: EngineeringConfigBusinessSummaryUsage | null) => void;
  onSummaryReadyChange?: (ready: boolean) => void;
  onSummaryStatusChange?: (status: LlmBusinessSummaryStatus) => void;
}) {
  const [state, setState] = useState<LlmBusinessSummaryState>({
    status: "idle",
    summaries: [],
    usage: null,
    error: null,
    cached: false,
  });
  const [copyFeedback, setCopyFeedback] = useState<LlmSummaryCopyFeedback | null>(null);
  const [compactActionsOpen, setCompactActionsOpen] = useState(false);
  const [refreshIndex, setRefreshIndex] = useState(0);
  const [expandedCompactSummaryKeys, setExpandedCompactSummaryKeys] = useState<Set<string>>(() => new Set());
  const [collapsedCompactSummaryKeys, setCollapsedCompactSummaryKeys] = useState<Set<string>>(() => new Set());
  const readyRequestKeyRef = useRef<string | null>(null);
  const payload = useMemo(
    () => buildLlmBusinessSummaryPayload(
      baseTrim,
      targetSummaries,
      deltaFilter,
      versionScope,
      categoryFilter,
      searchValue,
      factSource,
    ),
    [baseTrim, categoryFilter, deltaFilter, factSource, searchValue, targetSummaries, versionScope],
  );
  const requestKey = useMemo(() => JSON.stringify(payload), [payload]);
  const fallbackMessage = deterministicFallbackHidden
    ? "AI 摘要暂不可用；配置表和来源证据仍可继续查看。"
    : "LLM 摘要暂不可用，继续使用规则摘要。";

  useEffect(() => {
    if (!enabled || targetSummaries.length === 0) {
      setState({ status: "idle", summaries: [], usage: null, error: null, cached: false });
      onSummaryChange?.([], null);
      onSummaryReadyChange?.(false);
      onSummaryStatusChange?.("idle");
      return;
    }
    if (refreshIndex === 0) {
      const cached = cachedLlmBusinessSummary(requestKey);
      if (cached) {
        readyRequestKeyRef.current = requestKey;
        setState({
          status: "ready",
          summaries: cached.summaries,
          usage: cached.usage,
          error: null,
          cached: true,
        });
        onSummaryChange?.(cached.summaries, cached.usage);
        onSummaryReadyChange?.(true);
        onSummaryStatusChange?.("ready");
        return;
      }
    }
    let cancelled = false;
    const hasCachedSummaryForRequest = readyRequestKeyRef.current === requestKey;
    onSummaryReadyChange?.(hasCachedSummaryForRequest);
    setState((current) => ({
      status: hasCachedSummaryForRequest || refreshIndex > 0 ? "loading" : "idle",
      summaries: hasCachedSummaryForRequest ? current.summaries : [],
      usage: hasCachedSummaryForRequest ? current.usage : null,
      error: null,
      cached: hasCachedSummaryForRequest ? current.cached : false,
    }));
    onSummaryStatusChange?.(hasCachedSummaryForRequest || refreshIndex > 0 ? "loading" : "idle");
    const runCompose = (): void => {
      if (cancelled) return;
      const composePayload = llmSummaryComposePayload(payload, refreshIndex > 0);
      setState((current) => ({
        status: "loading",
        summaries: hasCachedSummaryForRequest ? current.summaries : [],
        usage: hasCachedSummaryForRequest ? current.usage : null,
        error: null,
        cached: hasCachedSummaryForRequest ? current.cached : false,
      }));
      onSummaryStatusChange?.("loading");
      api.composeEngineeringConfigBusinessSummary(composePayload)
        .then((response) => {
          if (cancelled) return;
          const hasSummary = response.summaries.length > 0;
          if (hasSummary) {
            readyRequestKeyRef.current = requestKey;
            rememberLlmBusinessSummary(requestKey, response.summaries, response.usage);
          }
          else if (!hasCachedSummaryForRequest) readyRequestKeyRef.current = null;
          onSummaryReadyChange?.(hasSummary || hasCachedSummaryForRequest);
          if (hasSummary || !hasCachedSummaryForRequest) {
            onSummaryChange?.(response.summaries, response.usage);
          }
          onSummaryStatusChange?.(hasSummary ? "ready" : "error");
          setState((current) => {
            if (!hasSummary && hasCachedSummaryForRequest && current.summaries.length > 0) {
              return {
                status: "error",
                summaries: current.summaries,
                usage: current.usage ?? response.usage,
                error: response.usage.fallbackReason || "LLM 摘要暂不可用，继续保留上一版 AI 摘要。",
                cached: current.cached,
              };
            }
            return {
              status: hasSummary ? "ready" : "error",
              summaries: response.summaries,
              usage: response.usage,
              error: hasSummary
                ? null
                : response.usage.fallbackReason || fallbackMessage,
              cached: false,
            };
          });
        })
        .catch((error: unknown) => {
          if (cancelled) return;
          onSummaryReadyChange?.(hasCachedSummaryForRequest);
          onSummaryStatusChange?.("error");
          setState((current) => ({
            status: "error",
            summaries: hasCachedSummaryForRequest ? current.summaries : [],
            usage: hasCachedSummaryForRequest ? current.usage : null,
            error: error instanceof Error
              ? error.message
              : hasCachedSummaryForRequest
                ? "LLM 摘要刷新失败，继续保留上一版 AI 摘要。"
                : fallbackMessage,
            cached: hasCachedSummaryForRequest ? current.cached : false,
          }));
        });
    };
    const cancelSchedule = scheduleDeferredLlmSummary(runCompose, refreshIndex > 0);
    return () => {
      cancelled = true;
      cancelSchedule();
    };
  }, [enabled, fallbackMessage, onSummaryChange, onSummaryReadyChange, onSummaryStatusChange, refreshIndex, requestKey, targetSummaries.length]);

  useEffect(() => {
    if (!enabled) {
      onSummaryChange?.([], null);
      return;
    }
    onSummaryChange?.(state.summaries, state.usage);
  }, [enabled, onSummaryChange, state.summaries, state.usage]);

  useEffect(() => {
    setExpandedCompactSummaryKeys(new Set());
    setCollapsedCompactSummaryKeys(new Set());
  }, [requestKey]);

  if (!enabled) return null;

  const usageText = llmSummaryUsageLabel(state.usage, state.status, compact, state.cached);
  const sourceText = llmSummarySourceLabel(state.usage, state.status, compact, state.cached);
  const statusMessage = state.status === "error"
    ? llmSummaryStatusMessage(state.error, fallbackMessage, compact, state.summaries.length > 0)
    : null;

  function evidenceRefForSummaryItem(
    summary: EngineeringConfigBusinessSummaryItem,
    section: LlmSummarySectionKey,
    itemIndex: number,
  ): EngineeringConfigBusinessSummaryEvidenceRef | null {
    return summary.evidenceRefs?.find((ref) => (
      ref.section === section && ref.itemIndex === itemIndex && Boolean(ref.evidenceKey)
    )) ?? null;
  }

  function evidenceHitForLlmRef(
    summary: EngineeringConfigBusinessSummaryItem,
    ref: EngineeringConfigBusinessSummaryEvidenceRef,
  ): LlmSummaryEvidenceHit | null {
    const targetSummary = targetSummaryForLlmSummary(summary);
    if (!targetSummary) return null;
    const delta = targetSummary.deltas.find((candidate) => llmEvidenceKey(candidate) === ref.evidenceKey);
    if (!delta) return null;
    return llmEvidenceHitForDelta(
      delta,
      targetSummary,
      ref.reason || `AI 摘要引用了 ${deltaFeatureLabel(delta)}，用于解释 ${trimLabel(targetSummary.targetTrim)} 相对 ${trimLabel(baseTrim)} 的配置差异。`,
    );
  }

  function targetSummaryForLlmSummary(summary: EngineeringConfigBusinessSummaryItem): TrimDeltaSummary | null {
    return targetSummaries.find((item) => item.targetTrim.trimId === summary.targetTrimId)
      ?? targetSummaries.find((item) => trimLabel(item.targetTrim) === summary.targetLabel)
      ?? null;
  }

  function llmEvidenceHitForDelta(delta: ConfigDelta, targetSummary: TrimDeltaSummary, selectionReason: string): LlmSummaryEvidenceHit {
    const evidence = deltaEvidenceTarget(delta);
    return {
      delta,
      targetTrimId: targetSummary.targetTrim.trimId,
      selection: {
        row: delta.row,
        trim: evidence.trim,
        cell: evidence.cell,
        selectionReason,
      },
    };
  }

  function firstEvidenceHitForLlmSummary(summary: EngineeringConfigBusinessSummaryItem): LlmSummaryEvidenceHit | null {
    const sections: Array<{ section: LlmSummarySectionKey; items: string[] }> = [
      { section: "mainUpgrades", items: summary.mainUpgrades },
      { section: "replacementsOrReductions", items: summary.replacementsOrReductions },
      { section: "evidenceStatus", items: summary.evidenceStatus },
    ];
    for (const section of sections) {
      for (let index = 0; index < section.items.length; index += 1) {
        const ref = evidenceRefForSummaryItem(summary, section.section, index);
        const refHit = ref ? evidenceHitForLlmRef(summary, ref) : null;
        if (refHit) return refHit;
      }
    }
    return null;
  }

  function llmSummaryRowFocusFilter(delta: ConfigDelta): ConfigComparisonDeltaFilter {
    if (delta.inferred) return "INFERRED";
    return focusFilterForDeltas([delta]);
  }

  function renderSummaryListItem(
    summary: EngineeringConfigBusinessSummaryItem,
    section: LlmSummarySectionKey,
    item: string,
    itemIndex: number,
    inlineEvidenceActions = true,
  ): ReactNode {
    const displayItem = displaySummaryText(item);
    const ref = evidenceRefForSummaryItem(summary, section, itemIndex);
    const requiresEvidence = section === "mainUpgrades" || section === "replacementsOrReductions";
    const hit = ref ? evidenceHitForLlmRef(summary, ref) : null;
    if (!hit || !inlineEvidenceActions) {
      return (
        <li key={`${section}-${item}`}>
          <span>{displayItem}</span>
          {!hit && requiresEvidence ? (
            <small className="business-summary-llm-card__unsupported-evidence">
              未匹配配置证据，不可直接引用
            </small>
          ) : null}
        </li>
      );
    }
    const focusFilter = llmSummaryRowFocusFilter(hit.delta);
    return (
      <li key={`${section}-${item}`}>
        <span className="business-summary-llm-card__evidence-actions">
          {onFocusFeatureRow ? (
            <button
              className="business-summary-llm-card__evidence-button business-summary-llm-card__evidence-button--row"
              type="button"
              aria-label={`定位 AI 摘要配置行：${summary.targetLabel} ${displayItem}`}
              onClick={() => onFocusFeatureRow(hit.delta.row, hit.targetTrimId, focusFilter)}
            >
              <span>{displayItem}</span>
              <small>定位行</small>
            </button>
          ) : (
            <span>{displayItem}</span>
          )}
          <button
            className="business-summary-llm-card__evidence-button business-summary-llm-card__evidence-button--source"
            type="button"
            aria-label={`查看 AI 摘要证据：${summary.targetLabel} ${displayItem}`}
            onClick={() => onOpenEvidence(hit.selection)}
          >
            <small>查看证据</small>
          </button>
        </span>
      </li>
    );
  }

  function compactPreviewText(value: string): string {
    const normalized = displaySummaryText(value);
    return normalized.length > 72 ? `${normalized.slice(0, 72)}...` : normalized;
  }

  function displaySummaryText(value: string): string {
    return value
      .replace(/\bsource evidence\b/gi, "来源证据")
      .replace(/\bevidence\b/gi, "来源证据")
      .replace(/\bLLM\b/g, "AI")
      .replace(/核对\s+来源证据/g, "核对来源证据")
      .replace(/点开\s+来源证据/g, "点开来源证据")
      .replace(/\s+/g, " ")
      .trim();
  }

  function compactEvidencePreviewText(value: string | null): string | null {
    if (!value) return null;
    const normalized = value.replace(/\s+/g, " ").trim();
    if (/未返回|暂未返回|不可用/.test(normalized)) return "证据提示：该目标暂无 AI 摘要，先以配置表和来源证据为准。";
    if (/规则推断|inferred|不是\s*Excel\s*原文|不配备\*/i.test(normalized)) return "证据提示：含规则推断，引用前核对来源证据。";
    if (/需核对|待核对|人工核对|待确认|缺失|缺少|回看|OCR/i.test(normalized)) return "证据提示：含需核对项，引用前核对来源证据。";
    return `证据提示：${compactPreviewText(normalized)}`;
  }

  function compactSummaryToggleText(open: boolean): string {
    return open ? "收起 AI 要点" : "展开 AI 要点";
  }

  function renderSummarySection(
    summary: EngineeringConfigBusinessSummaryItem,
    section: LlmSummarySectionKey,
    label: string,
    items: string[],
    itemLimit?: number,
    inlineEvidenceActions = true,
  ): ReactNode {
    if (items.length === 0) return null;
    const visibleItems = typeof itemLimit === "number" ? items.slice(0, itemLimit) : items;
    const hiddenCount = items.length - visibleItems.length;
    return (
      <div className={`business-summary-llm-card__section ${section === "evidenceStatus" ? "is-evidence" : ""}`.trim()}>
        <small>{label}</small>
        <ul>
          {visibleItems.map((item, index) => renderSummaryListItem(summary, section, item, index, inlineEvidenceActions))}
        </ul>
        {hiddenCount > 0 ? (
          <em className="business-summary-llm-card__section-more">
            另 {hiddenCount} 项已收起；复制 AI 摘要或在下方配置表中核对完整条目。
          </em>
        ) : null}
      </div>
    );
  }

  function renderSummarySections(
    summary: EngineeringConfigBusinessSummaryItem,
    itemLimit?: number,
    options: LlmSummarySectionRenderOptions = {},
  ): ReactNode {
    const includeEvidenceStatus = options.includeEvidenceStatus ?? true;
    const includeRecommendedUse = options.includeRecommendedUse ?? true;
    const inlineEvidenceActions = options.inlineEvidenceActions ?? true;
    return (
      <>
        {renderSummarySection(summary, "mainUpgrades", "主要升级", summary.mainUpgrades, itemLimit, inlineEvidenceActions)}
        {renderSummarySection(summary, "replacementsOrReductions", "减少或替换", summary.replacementsOrReductions, itemLimit, inlineEvidenceActions)}
        {includeEvidenceStatus ? renderSummarySection(summary, "evidenceStatus", "证据状态", summary.evidenceStatus, itemLimit, inlineEvidenceActions) : null}
        {includeRecommendedUse && summary.recommendedUse ? <p>{displaySummaryText(summary.recommendedUse)}</p> : null}
      </>
    );
  }

  function summarySectionCopyText(title: string, items: string[]): string[] {
    if (items.length === 0) return [];
    return [title, ...items.map((item, index) => `${index + 1}. ${item}`)];
  }

  function summaryEvidenceRefCopyText(summary: EngineeringConfigBusinessSummaryItem): string[] {
    if (!summary.evidenceRefs || summary.evidenceRefs.length === 0) return [];
    const sectionLabels: Record<string, string> = {
      mainUpgrades: "主要升级",
      replacementsOrReductions: "减少或替换",
      evidenceStatus: "证据状态",
    };
    return [
      "证据引用:",
      ...summary.evidenceRefs.map((ref, index) => {
        const itemNumber = typeof ref.itemIndex === "number" ? ref.itemIndex + 1 : "?";
        const reason = ref.reason ? ` · ${ref.reason}` : "";
        return `${index + 1}. ${sectionLabels[ref.section] ?? ref.section} #${itemNumber} -> ${ref.evidenceKey}${reason}`;
      }),
    ];
  }

  function aiSummaryCopyText(): string {
    const lines = [
      "AI 配置对比摘要",
      `基准配置列：${trimLabel(baseTrim)}`,
      state.usage ? `AI 来源：${state.usage.provider} / ${state.usage.model}` : null,
      "",
    ].filter((line): line is string => line !== null);
    state.summaries.forEach((summary, index) => {
      lines.push(`目标配置列 ${index + 1}：${summary.targetLabel || summary.targetTrimId || "目标配置列"}`);
      lines.push(`结论：${displaySummaryText(summary.headline || "AI 摘要暂未返回")}`);
      lines.push(...summarySectionCopyText("主要升级：", summary.mainUpgrades.map(displaySummaryText)));
      lines.push(...summarySectionCopyText("减少或替换：", summary.replacementsOrReductions.map(displaySummaryText)));
      lines.push(...summarySectionCopyText("证据状态：", summary.evidenceStatus.map(displaySummaryText)));
      lines.push(...summaryEvidenceRefCopyText(summary));
      if (summary.recommendedUse) lines.push(`使用建议: ${displaySummaryText(summary.recommendedUse)}`);
      lines.push("");
    });
    return lines.join("\n").trim();
  }

  async function copyAiSummary(): Promise<void> {
    if (state.summaries.length === 0) return;
    if (!navigator.clipboard?.writeText) {
      setCopyFeedback({ tone: "error", message: "当前浏览器不支持复制，请手动选中 AI 摘要。" });
      return;
    }
    try {
      await navigator.clipboard.writeText(aiSummaryCopyText());
      setCopyFeedback({ tone: "success", message: "AI 摘要已复制。" });
    } catch (reason: unknown) {
      setCopyFeedback({
        tone: "error",
        message: reason instanceof Error ? reason.message : "复制失败，请手动选中 AI 摘要。",
      });
    }
  }

  function toggleCompactSummary(summaryKey: string, open: boolean): void {
    setExpandedCompactSummaryKeys((current) => {
      const next = new Set(current);
      if (open) next.add(summaryKey);
      else next.delete(summaryKey);
      return next;
    });
    setCollapsedCompactSummaryKeys((current) => {
      const next = new Set(current);
      if (open) next.delete(summaryKey);
      else next.add(summaryKey);
      return next;
    });
  }

  function compactSummaryOpen(summaryKey: string): boolean {
    if (expandedCompactSummaryKeys.has(summaryKey)) return true;
    if (collapsedCompactSummaryKeys.has(summaryKey)) return false;
    return false;
  }

  const blockLabel = compact ? "AI 配置对比摘要" : "AI 业务摘要";
  const blockTitle = state.status === "loading"
    ? "正在生成业务话术"
    : compact
      ? "基于配置事实生成业务结论"
      : "基于配置事实生成";

  function renderCopyAction(label = "复制 AI 摘要"): ReactNode {
    if (state.summaries.length === 0) return null;
    return (
      <button
        className="btn btn-sm btn-primary"
        type="button"
        aria-label="复制当前 AI 摘要"
        onClick={() => {
          void copyAiSummary();
        }}
      >
        {label}
      </button>
    );
  }

  function renderRefreshAction(): ReactNode {
    return (
      <button className="btn btn-sm btn-secondary" type="button" onClick={() => setRefreshIndex((value) => value + 1)}>
        重新生成
      </button>
    );
  }

  function renderActions({ includeCopy = true }: { includeCopy?: boolean } = {}): ReactNode {
    return (
      <div className="business-summary-llm__actions">
        {includeCopy ? renderCopyAction() : null}
        {renderRefreshAction()}
      </div>
    );
  }

  function renderCompactActions(): ReactNode {
    const showSummaryActions = state.summaries.length > 0 || state.status === "error";
    return (
      <div className="business-summary-llm__quick-actions">
        <a className="btn btn-sm btn-secondary" href="#config-compare-table">
          查看配置表
        </a>
        {state.summaries.length > 0 ? renderCopyAction("复制") : null}
        {showSummaryActions ? (
          <details
            className="business-summary-llm__more-actions"
            aria-label="AI 摘要操作"
            open={compactActionsOpen}
            onToggle={(event) => setCompactActionsOpen(event.currentTarget.open)}
          >
            <summary
              onClick={(event) => {
                event.preventDefault();
                setCompactActionsOpen((current) => !current);
              }}
            >
              摘要操作
            </summary>
            {compactActionsOpen ? (
              <div className="business-summary-llm__more-actions-body">
                {renderRefreshAction()}
                <small>{sourceText}</small>
              </div>
            ) : null}
          </details>
        ) : null}
      </div>
    );
  }

  return (
    <section className={`business-summary-llm ${state.status === "error" ? "is-error" : ""} ${compact ? "is-compact" : ""}`.trim()} aria-label={blockLabel}>
      <div className="business-summary-llm__head">
        <div>
          <span>{blockLabel}</span>
          <strong>{blockTitle}</strong>
          <small>{usageText}</small>
          {!compact ? <small className="business-summary-llm__source">{sourceText}</small> : null}
        </div>
        {compact ? renderCompactActions() : null}
        {!compact ? renderActions() : null}
      </div>
      {copyFeedback ? (
        <p className={`business-summary-llm__copy-feedback is-${copyFeedback.tone}`} role="status">
          {copyFeedback.message}
        </p>
      ) : null}
      {state.status === "idle" ? (
        <p className="business-summary-llm__status">AI 摘要将在首屏稳定后自动生成；下方配置表可以先查看。</p>
      ) : null}
      {state.status === "loading" ? (
        <p className="business-summary-llm__status">正在把配置差异改写成业务摘要；下方配置表可以继续查看来源证据。</p>
      ) : null}
      {state.status === "error" ? (
        <p className="business-summary-llm__status">{statusMessage}</p>
      ) : null}
      {state.summaries.length > 0 ? (
        <div className="business-summary-llm__items">
          {state.summaries.map((summary, index) => {
            const summaryKey = summary.targetTrimId || summary.targetLabel || `summary-${index}`;
            const evidencePreview = compactEvidencePreviewText(summary.evidenceStatus[0] ?? null);
            if (compact) {
              const detailsOpen = compactSummaryOpen(summaryKey);
              const quickEvidenceHit = firstEvidenceHitForLlmSummary(summary);
              return (
                <article
                  className="business-summary-llm-card is-compact is-readable"
                  key={summaryKey}
                  aria-label={`AI 结论和证据：${summary.targetLabel}`}
                >
                  <details
                    className="business-summary-llm-card__compact-details"
                    open={detailsOpen}
                    onToggle={(event) => toggleCompactSummary(summaryKey, event.currentTarget.open)}
                  >
                    <summary
                      className="business-summary-llm-card__compact-summary"
                      onClick={(event) => {
                        event.preventDefault();
                        toggleCompactSummary(summaryKey, !detailsOpen);
                      }}
                    >
                      <span>{summary.targetLabel}</span>
                      <strong>{displaySummaryText(summary.headline)}</strong>
                      {evidencePreview ? <em>{evidencePreview}</em> : null}
                      <small>{compactSummaryToggleText(detailsOpen)}</small>
                    </summary>
                    {detailsOpen ? (
                      <div className="business-summary-llm-card__details-body">
                        {renderSummarySections(summary, COMPACT_SUMMARY_SECTION_ITEM_LIMIT, {
                          includeEvidenceStatus: false,
                          includeRecommendedUse: false,
                          inlineEvidenceActions: false,
                        })}
                      </div>
                    ) : null}
                  </details>
                  {quickEvidenceHit ? (
                    <div className="business-summary-llm-card__compact-actions-row" aria-label={`${summary.targetLabel} AI 结论快捷核对`}>
                      {onFocusFeatureRow ? (
                        <button
                          className="btn btn-xs btn-secondary"
                          type="button"
                          aria-label={`定位 AI 结论配置行：${summary.targetLabel}`}
                          onClick={() => onFocusFeatureRow(
                            quickEvidenceHit.delta.row,
                            quickEvidenceHit.targetTrimId,
                            llmSummaryRowFocusFilter(quickEvidenceHit.delta),
                          )}
                        >
                          定位配置行
                        </button>
                      ) : null}
                      <button
                        className="btn btn-xs btn-secondary"
                        type="button"
                        aria-label={`核对 AI 结论证据：${summary.targetLabel}`}
                        onClick={() => onOpenEvidence(quickEvidenceHit.selection)}
                      >
                        核对证据
                      </button>
                    </div>
                  ) : null}
                </article>
              );
            }
            return (
              <article className="business-summary-llm-card" key={summaryKey}>
                <span>{summary.targetLabel}</span>
                <strong>{displaySummaryText(summary.headline)}</strong>
                {renderSummarySections(summary)}
              </article>
            );
          })}
        </div>
      ) : null}
    </section>
  );
}

export function BusinessSummaryPanel({
  data,
  baseTrimId,
  categoryFilter,
  deltaFilter = "ALL",
  mode = "expert",
  searchValue = "",
  targetTrimFilterId,
  factSource,
  onShowDifferenceRows,
  onFocusCategory,
  onFocusDeltaType,
  onFocusFeatureRow,
  onFocusTargetTrim,
  onFocusVersionStep,
  onLlmSummaryChange,
  onOpenSourceContext,
  onOpenEvidence,
  llmSummaryEnabled = false,
}: BusinessSummaryPanelProps) {
  const [copyFeedback, setCopyFeedback] = useState<ConclusionCopyFeedback | null>(null);
  const [llmSummaryReady, setLlmSummaryReady] = useState(false);
  const [llmSummaryStatus, setLlmSummaryStatus] = useState<LlmBusinessSummaryStatus>("idle");
  const [llmSummaryCount, setLlmSummaryCount] = useState(0);
  const [expertRulesExpanded, setExpertRulesExpanded] = useState(false);
  const simpleMode = mode === "simple";
  const expertAiSummaryPrimary = !simpleMode && llmSummaryEnabled && llmSummaryCount > 0;
  const aiSummaryOnlyMode = simpleMode && llmSummaryEnabled;
  const hideSimpleDeterministicBlocks = aiSummaryOnlyMode;
  const showExpertDeterministicBlocks = !simpleMode && (!expertAiSummaryPrimary || expertRulesExpanded);
  const showSimpleDeterministicBlocks = simpleMode && !hideSimpleDeterministicBlocks;
  const summary = useMemo(
    () => buildBusinessDifferenceSummary(data, baseTrimId),
    [data, baseTrimId],
  );
  const llmScopeResetKey = [
    baseTrimId ?? "",
    categoryFilter ?? "",
    deltaFilter,
    searchValue,
    targetTrimFilterId ?? "",
    data.trims.map((trim) => trim.trimId).join("|"),
    String(data.rows.length),
  ].join("\u001f");

  useEffect(() => {
    if (expertAiSummaryPrimary) return;
    setExpertRulesExpanded(false);
  }, [expertAiSummaryPrimary]);

  useEffect(() => {
    setLlmSummaryCount(0);
    setExpertRulesExpanded(false);
  }, [llmScopeResetKey]);

  const handleLlmSummaryChange = useCallback((
    summaries: EngineeringConfigBusinessSummaryItem[],
    usage: EngineeringConfigBusinessSummaryUsage | null,
  ): void => {
    setLlmSummaryCount(summaries.length);
    onLlmSummaryChange?.(summaries, usage);
  }, [onLlmSummaryChange]);

  if (!summary.baseTrim || summary.targetSummaries.length === 0) return null;
  const baseTrim = summary.baseTrim;
  const scopedTargetSummaries = summary.targetSummaries.map((targetSummary) => scopedTargetSummary(targetSummary, categoryFilter, searchValue, deltaFilter));
  const targetSummaries = targetTrimFilterId
    ? scopedTargetSummaries.filter((targetSummary) => targetSummary.targetTrim.trimId === targetTrimFilterId)
    : scopedTargetSummaries;
  if (targetSummaries.length === 0) return null;
  const activeTargetTrim = targetTrimFilterId ? targetSummaries[0]?.targetTrim ?? null : null;
  const panelScopedLabel = simpleMode
    ? simplePanelScopeLabel(categoryFilter, searchValue, deltaFilter, activeTargetTrim)
    : panelScopeLabel(categoryFilter, searchValue, deltaFilter, activeTargetTrim);
  const baseStoryline = showExpertDeterministicBlocks
    ? baseStorylineItems(baseTrim, targetSummaries, deltaFilter)
    : [];
  const baselineItems = showExpertDeterministicBlocks || showSimpleDeterministicBlocks
    ? baselineNarrativeItems(baseTrim, targetSummaries, deltaFilter)
    : [];
  const scopeNote = summaryScopeNote(deltaFilter, panelScopedLabel);
  const panelTitle = summaryTitle(panelScopedLabel, deltaFilter);
  const displayPanelTitle = simpleMode ? excelGuideTitle(panelScopedLabel) : panelTitle;
  const displayScopeLabel = simpleMode ? "表格口径" : scopeNote.label;
  const displayScopeDetail = simpleMode ? excelGuideScopeDetail(deltaFilter, panelScopedLabel) : scopeNote.detail;
  const displayNarrative = showExpertDeterministicBlocks || showSimpleDeterministicBlocks
    ? simpleMode
      ? excelGuideNarrative(baseTrim, targetSummaries, deltaFilter, panelScopedLabel)
      : baseNarrative(summary, targetSummaries, categoryFilter, searchValue, deltaFilter, activeTargetTrim)
    : "";
  const conclusionStatusItems = showExpertDeterministicBlocks || showSimpleDeterministicBlocks
    ? conclusionStatusSummaryItems(baseTrim, targetSummaries, deltaFilter)
    : [];
  const excelGuide = showSimpleDeterministicBlocks
    ? excelGuideItems(baseTrim, targetSummaries, deltaFilter, conclusionStatusItems)
    : [];
  const targetQueue = showExpertDeterministicBlocks
    ? focusedTargetQueue(scopedTargetSummaries, targetTrimFilterId, deltaFilter)
    : null;
  const showTargetOverview = showExpertDeterministicBlocks && deltaFilter !== "COMMON" && !isEvidenceDeltaFilter(deltaFilter);
  const versionLadder = showTargetOverview
    ? versionLadderItems(baseTrim, targetSummaries, deltaFilter)
    : [];
  const versionUpgradeSteps = showTargetOverview && !targetTrimFilterId
    ? versionUpgradeStepItems(data, data.trims, categoryFilter, searchValue, deltaFilter)
    : [];
  const simpleConclusions = showSimpleDeterministicBlocks
    ? simpleConclusionItems(baseTrim, targetSummaries, deltaFilter)
    : [];
  const showSimpleVersionNarrative = showSimpleDeterministicBlocks && deltaFilter !== "COMMON" && !isEvidenceDeltaFilter(deltaFilter);
  const simpleVersionNarratives = showSimpleVersionNarrative
    ? simpleVersionNarrativeItems(baseTrim, targetSummaries, deltaFilter)
    : [];
  const simpleScopeBridge = showSimpleDeterministicBlocks
    ? simpleScopeBridgeItems(targetSummaries, deltaFilter, activeTargetTrim)
    : [];
  const detailActionLabel = deltaFilter === "COMMON"
    ? "查看当前范围配置"
    : deltaFilter === "UNKNOWN"
      ? "查看待确认项"
      : deltaFilter === "INFERRED"
        ? "查看规则推断项"
        : deltaFilter === "MISSING_SOURCE"
          ? "查看来源问题"
          : deltaFilter === "MERGED_SOURCE"
            ? "查看合并格"
            : panelScopedLabel
              ? "查看当前范围差异"
              : simpleMode ? "查看差异行" : "查看差异项";
  const aiSummaryPending = simpleMode && llmSummaryEnabled && !llmSummaryReady && (llmSummaryStatus === "idle" || llmSummaryStatus === "loading");
  const sectionLabel = aiSummaryOnlyMode ? "配置摘要面板" : displayPanelTitle;
  const sectionTitle = aiSummaryOnlyMode ? "AI 配置对比摘要" : displayPanelTitle;

  function renderUpgradeInsightCard(targetSummary: TrimDeltaSummary, insight: OptionSwapInsight): ReactNode {
    const fromEvidence = deltaEvidenceTarget(insight.fromDelta);
    const toEvidence = deltaEvidenceTarget(insight.toDelta);
    const targetLabel = trimLabel(targetSummary.targetTrim);
    const fromTrimLabel = trimLabel(fromEvidence.trim);
    const toTrimLabel = trimLabel(toEvidence.trim);
    const upgradeLabel = `${insight.fromFeature} → ${insight.toFeature}`;
    return (
      <div className="business-summary-upgrade-clue" key={`${targetSummary.targetTrim.trimId}-${insight.key}`}>
        <span>{insight.dimensionLabel}</span>
        <strong>{upgradeLabel}</strong>
        <small>{categoryLabel(insight.category)}</small>
        <div className="business-summary-upgrade-actions" aria-label={`${insight.dimensionLabel} 升级线索来源`}>
          {onFocusDeltaType || onFocusCategory ? (
            <button
              type="button"
              aria-label={`聚焦 ${targetLabel} 的${insight.dimensionLabel}升级范围：${upgradeLabel}`}
              onClick={() => {
                onFocusDeltaType?.("DIFFERENCE", targetSummary.targetTrim.trimId);
                onFocusCategory?.(insight.category);
              }}
            >
              聚焦升级范围
            </button>
          ) : null}
          <button
            type="button"
            aria-label={`查看 ${fromTrimLabel} 的${insight.dimensionLabel}旧配置来源：${insight.fromFeature}`}
            onClick={() => onOpenEvidence({
              row: insight.fromDelta.row,
              trim: fromEvidence.trim,
              cell: fromEvidence.cell,
              selectionReason: optionSwapEvidenceReason(insight, "from"),
            })}
          >
            旧配置来源
          </button>
          <button
            type="button"
            aria-label={`查看 ${toTrimLabel} 的${insight.dimensionLabel}新配置来源：${insight.toFeature}`}
            onClick={() => onOpenEvidence({
              row: insight.toDelta.row,
              trim: toEvidence.trim,
              cell: toEvidence.cell,
              selectionReason: optionSwapEvidenceReason(insight, "to"),
            })}
          >
            新配置来源
          </button>
        </div>
      </div>
    );
  }

  function renderBusinessDeltaButton(targetSummary: TrimDeltaSummary, sectionKey: string, delta: ConfigDelta): ReactNode {
    const evidence = deltaEvidenceTarget(delta);
    return (
      <button
        className="business-summary-delta"
        type="button"
        aria-label={deltaEvidenceLabel(delta, evidence.trim)}
        key={`${targetSummary.targetTrim.trimId}-${sectionKey}-${delta.row.featureCode}-${delta.deltaType}`}
        onClick={() => onOpenEvidence({
          row: delta.row,
          trim: evidence.trim,
          cell: evidence.cell,
          selectionReason: deltaEvidenceReason(delta, evidence.trim),
        })}
      >
        <span>{DELTA_LABELS[delta.deltaType]}{delta.inferred ? " · 推断" : ""}</span>
        <strong>{delta.row.featureName}</strong>
        <small>{categoryLabel(delta.row.category)}</small>
      </button>
    );
  }

  function renderBusinessFocusGroup(targetSummary: TrimDeltaSummary, group: BusinessFocusGroup): ReactNode {
    const content = (
      <>
        <span>{group.label}</span>
        <strong>{group.countLabel}</strong>
        <small>{group.sampleLabel}</small>
        <em>{group.evidenceLabel}</em>
      </>
    );
    if (onFocusDeltaType) {
      return (
        <button
          className={`business-summary-focus-group is-${group.tone}`}
          type="button"
          key={group.key}
          aria-label={`聚焦 ${trimLabel(targetSummary.targetTrim)} 的业务重点：${group.label}`}
          onClick={() => onFocusDeltaType(group.filter, targetSummary.targetTrim.trimId)}
        >
          {content}
        </button>
      );
    }
    return (
      <span className={`business-summary-focus-group is-${group.tone}`} key={group.key}>
        {content}
      </span>
    );
  }

  function renderCommonDeltaButton(targetSummary: TrimDeltaSummary, delta: ConfigDelta, keySuffix: string): ReactNode {
    const evidence = deltaEvidenceTarget(delta);
    return (
      <button
        className="business-summary-delta"
        type="button"
        aria-label={`查看 ${trimLabel(targetSummary.targetTrim)} ${delta.row.featureName} 的共同配置来源`}
        key={`${targetSummary.targetTrim.trimId}-${keySuffix}-${delta.row.featureCode}`}
        onClick={() => onOpenEvidence({
          row: delta.row,
          trim: evidence.trim,
          cell: evidence.cell,
          selectionReason: `该配置在 ${trimLabel(targetSummary.targetTrim)} 与 ${trimLabel(baseTrim)} 中保持一致。`,
        })}
      >
        <span>一致</span>
        <strong>{delta.row.featureName}</strong>
        <small>{categoryLabel(delta.row.category)}</small>
      </button>
    );
  }

  function renderEvidenceDeltaButton(
    targetSummary: TrimDeltaSummary,
    filter: ConfigComparisonDeltaFilter,
    delta: ConfigDelta,
    keySuffix: string,
  ): ReactNode {
    const evidence = deltaEvidenceTarget(delta);
    const isMissingSource = filter === "MISSING_SOURCE";
    return (
      <button
        className="business-summary-delta"
        type="button"
        aria-label={`查看 ${trimLabel(targetSummary.targetTrim)} ${delta.row.featureName} 的证据来源`}
        key={`${targetSummary.targetTrim.trimId}-${keySuffix}-${delta.row.featureCode}`}
        onClick={() => onOpenEvidence({
          row: delta.row,
          trim: evidence.trim,
          cell: evidence.cell,
          selectionReason: isMissingSource
            ? `该配置存在缺值或缺少来源证据，需补来源后再解释 ${trimLabel(targetSummary.targetTrim)} 相对 ${trimLabel(baseTrim)} 的差异。`
            : `该配置来自合并格展开，需核对共通参数是否适用于 ${trimLabel(targetSummary.targetTrim)}。`,
        })}
      >
        <span>{isMissingSource ? "来源问题" : "合并格"}</span>
        <strong>{delta.row.featureName}</strong>
        <small>{categoryLabel(delta.row.category)}</small>
      </button>
    );
  }

  function renderTargetInsight(targetSummary: TrimDeltaSummary, item: TargetInsightItem): ReactNode {
    const focusFilter = item.filter;
    const targetId = focusFilter ? focusTargetForFilter(focusFilter, targetSummary.targetTrim.trimId) : null;
    const insightContent = (
      <>
        <strong>{item.label}</strong>
        <small>{item.value}</small>
      </>
    );
    return focusFilter && onFocusDeltaType ? (
      <button
        className="business-summary-target-insight"
        type="button"
        key={item.key}
        aria-label={`聚焦 ${trimLabel(targetSummary.targetTrim)} 的 ${item.label}`}
        onClick={() => onFocusDeltaType(focusFilter, targetId)}
      >
        {insightContent}
      </button>
    ) : (
      <span className="business-summary-target-insight" key={item.key}>
        {insightContent}
      </span>
    );
  }

  async function copyConclusionDraft(targetTrim: CompareTrimItem, draft: TargetConclusionDraft): Promise<void> {
    if (!navigator.clipboard?.writeText) {
      setCopyFeedback({ trimId: targetTrim.trimId, message: "当前浏览器不支持复制，请手动选中结论草稿。" });
      return;
    }
    try {
      await navigator.clipboard.writeText(conclusionDraftCopyText(targetTrim, baseTrim, draft));
      setCopyFeedback({ trimId: targetTrim.trimId, message: "结论草稿已复制。" });
    } catch (reason: unknown) {
      setCopyFeedback({
        trimId: targetTrim.trimId,
        message: reason instanceof Error ? reason.message : "复制失败，请手动选中结论草稿。",
      });
    }
  }

  async function copyAllConclusionDrafts(): Promise<void> {
    if (!navigator.clipboard?.writeText) {
      setCopyFeedback({ trimId: ALL_CONCLUSION_COPY_ID, message: "当前浏览器不支持复制，请手动复制摘要区结论。" });
      return;
    }
    try {
      await navigator.clipboard.writeText(allConclusionDraftCopyText(baseTrim, targetSummaries, deltaFilter, panelScopedLabel));
      setCopyFeedback({ trimId: ALL_CONCLUSION_COPY_ID, message: "全部结论草稿已复制。" });
    } catch (reason: unknown) {
      setCopyFeedback({
        trimId: ALL_CONCLUSION_COPY_ID,
        message: reason instanceof Error ? reason.message : "复制失败，请手动复制摘要区结论。",
      });
    }
  }

  function renderBaselineConclusion(): ReactNode {
    return (
      <div className="business-summary-baseline" aria-label="基准对比结论">
        {baselineItems.map((item) => (
          <div className="business-summary-baseline-item" key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            {item.categories && item.categories.length > 0 && onFocusCategory ? (
              <div className="business-summary-baseline-actions" aria-label={`${item.label}筛选`}>
                <span className="business-summary-baseline-actions__label">来源大类筛选</span>
                {item.categories.map((category) => (
                  <button
                    className="business-summary-baseline-action"
                    type="button"
                    key={category.category}
                    aria-label={`聚焦 ${categoryLabel(category.category)}，目标差异 ${category.totalDifferenceCount} 项`}
                    onClick={() => onFocusCategory(category.category)}
                  >
                    {categoryLabel(category.category)}
                    <strong>{category.totalDifferenceCount}</strong>
                  </button>
                ))}
              </div>
            ) : null}
            <small>{item.detail}</small>
          </div>
        ))}
      </div>
    );
  }

  return (
    <section className={`business-summary-panel ${simpleMode ? "is-simple" : "is-expert"} ${simpleMode && llmSummaryEnabled ? "has-ai-summary" : ""} ${aiSummaryOnlyMode ? "is-ai-only" : ""} ${aiSummaryPending ? "is-ai-pending" : ""}`.trim()} aria-label={sectionLabel}>
      {!aiSummaryOnlyMode ? (
      <div className="business-summary-panel__header">
        <div>
          <span className="market-scan-panel-eyebrow">{simpleMode && llmSummaryEnabled ? "AI 配置摘要" : simpleMode ? "Excel 对比导读" : "业务摘要"}</span>
          <h2>{sectionTitle}</h2>
          <div className="business-summary-scope-note" aria-label="摘要统计口径">
            <span>{displayScopeLabel}</span>
            <small>{displayScopeDetail}</small>
          </div>
          {showExpertDeterministicBlocks && conclusionStatusItems.length > 0 ? (
            <div className="business-summary-conclusion-status" aria-label="结论状态汇总">
              <span>结论状态</span>
              {conclusionStatusItems.map((item) => {
                const chipContent = (
                  <>
                    <span>{item.label}</span>
                    {" "}
                    <strong>{item.count}</strong>
                    {" "}
                    <em>{compactConclusionStatusTargets(item)}</em>
                    {" "}
                    <b>{conclusionStatusActionHint(item)}</b>
                  </>
                );
                const chipLabel = `${item.label} ${item.count}，${item.targetLabels.join("、")}`;
                const chipClassName = `business-summary-conclusion-status__chip is-${item.tone}`;
                if (item.focusFilter && onFocusDeltaType) {
                  return (
                    <button
                      aria-label={`聚焦结论状态：${chipLabel}；${conclusionStatusActionHint(item)}`}
                      className={chipClassName}
                      key={item.key}
                      title={`${item.label}: ${item.targetLabels.join(", ")}; ${conclusionStatusActionHint(item)}`}
                      type="button"
                      onClick={() => onFocusDeltaType(item.focusFilter as ConfigComparisonDeltaFilter, item.focusTargetTrimId)}
                    >
                      {chipContent}
                    </button>
                  );
                }
                if (item.focusAction === "source" && onOpenSourceContext) {
                  return (
                    <button
                      aria-label={`打开结论状态来源：${chipLabel}；${conclusionStatusActionHint(item)}`}
                      className={chipClassName}
                      key={item.key}
                      title={`${item.label}: ${item.targetLabels.join(", ")}; ${conclusionStatusActionHint(item)}`}
                      type="button"
                      onClick={() => onOpenSourceContext(item.focusTargetTrimId)}
                    >
                      {chipContent}
                    </button>
                  );
                }
                if (onFocusTargetTrim) {
                  return (
                    <button
                      aria-label={`聚焦结论状态目标：${chipLabel}；${conclusionStatusActionHint(item)}`}
                      className={chipClassName}
                      key={item.key}
                      title={`${item.label}: ${item.targetLabels.join(", ")}; ${conclusionStatusActionHint(item)}`}
                      type="button"
                      onClick={() => onFocusTargetTrim(item.focusTargetTrimId)}
                    >
                      {chipContent}
                    </button>
                  );
                }
                return (
                  <span
                    aria-label={chipLabel}
                    className={chipClassName}
                    key={item.key}
                    title={`${item.label}: ${item.targetLabels.join(", ")}; ${conclusionStatusActionHint(item)}`}
                  >
                    {chipContent}
                  </span>
                );
              })}
            </div>
            ) : null}
          {showExpertDeterministicBlocks || showSimpleDeterministicBlocks ? <p>{displayNarrative}</p> : null}
        </div>
        <div className="business-summary-panel__actions">
          {showExpertDeterministicBlocks && targetQueue && onFocusDeltaType ? (
            <div className="business-summary-target-queue" aria-label="目标处理队列">
              <span>{targetQueue.filterLabel}目标队列 {targetQueue.currentIndex}/{targetQueue.total}</span>
              <button
                className="business-summary-target-queue__button"
                type="button"
                aria-label={`切到下一个${targetQueue.filterLabel}目标：${targetQueue.nextLabel}`}
                title={`当前 ${targetQueue.currentLabel}，下一个 ${targetQueue.nextLabel}`}
                onClick={() => onFocusDeltaType(deltaFilter, targetQueue.nextTrimId)}
              >
                下一个 <strong>{targetQueue.nextLabel}</strong>
              </button>
            </div>
          ) : null}
          {expertAiSummaryPrimary ? (
            <button
              className="btn btn-sm btn-secondary"
              type="button"
              aria-label={expertRulesExpanded ? "收起高级规则诊断" : "查看高级规则诊断"}
              aria-expanded={expertRulesExpanded}
              onClick={() => setExpertRulesExpanded((current) => !current)}
            >
              {expertRulesExpanded ? "收起高级诊断" : "查看高级诊断"}
            </button>
          ) : null}
          {showExpertDeterministicBlocks && !expertAiSummaryPrimary ? (
            <button
              className="btn btn-sm btn-secondary"
              type="button"
              aria-label="复制当前摘要全部结论草稿"
              onClick={() => {
                void copyAllConclusionDrafts();
              }}
            >
              复制全部结论
            </button>
          ) : null}
          {simpleMode ? (
            <a className="btn btn-sm btn-secondary" href="#config-compare-table">
              查看配置表
            </a>
          ) : null}
          {onShowDifferenceRows ? (
            <button className="btn btn-sm btn-secondary" type="button" onClick={onShowDifferenceRows}>
              {detailActionLabel}
            </button>
          ) : (
            <a className="btn btn-sm btn-secondary" href="#config-compare-table">{detailActionLabel}</a>
          )}
          {copyFeedback?.trimId === ALL_CONCLUSION_COPY_ID ? (
            <em className="business-summary-panel__copy-feedback" role="status">
              {copyFeedback.message}
            </em>
          ) : null}
        </div>
      </div>
      ) : null}

      {llmSummaryEnabled ? (
        <LlmBusinessSummaryBlock
          enabled={llmSummaryEnabled}
          compact={simpleMode}
          baseTrim={baseTrim}
          targetSummaries={targetSummaries}
          deltaFilter={deltaFilter}
          versionScope={data.versionScope ?? "published"}
          categoryFilter={categoryFilter}
          searchValue={searchValue}
          factSource={factSource}
          deterministicFallbackHidden={hideSimpleDeterministicBlocks}
          onOpenEvidence={onOpenEvidence}
          onFocusFeatureRow={onFocusFeatureRow}
          onSummaryChange={handleLlmSummaryChange}
          onSummaryReadyChange={setLlmSummaryReady}
          onSummaryStatusChange={setLlmSummaryStatus}
        />
      ) : null}

      {expertAiSummaryPrimary && !expertRulesExpanded ? (
        <div className="business-summary-ai-detail-gate" aria-label="高级规则诊断已收起">
          <span>高级诊断已收起</span>
          <small>AI 结论是业务主视图；高级诊断、升级路径和来源样本只作为核查辅助，避免和 AI 摘要重复。</small>
        </div>
      ) : null}

      {showExpertDeterministicBlocks && expertAiSummaryPrimary ? (
        <div className="business-summary-ai-detail-gate is-expanded" aria-label="高级规则诊断说明">
          <span>高级诊断</span>
          <small>下面内容由确定性规则生成，用于排查推断、合并格、来源缺口和升级线索；对外话术以 AI 结论为准，引用前仍要点开来源证据。</small>
        </div>
      ) : null}

      {simpleMode && showSimpleDeterministicBlocks ? (
        <details className="business-summary-simple-fallback" aria-label="规则速读备用">
          <summary>
            <span>规则速读备用</span>
            <strong>{simpleConclusions.length > 0 ? `${simpleConclusions.length} 个目标配置列` : "暂无目标配置列"}</strong>
            <small>AI 摘要未启用时可展开；默认先看完整配置表和差异行。</small>
          </summary>
          <div className="business-summary-simple-fallback__content">
            <div className="business-summary-simple-priority" aria-label="Excel 首屏速读">
              <div className="business-summary-simple-scope-bridge" aria-label="简易模式表格与摘要关系">
                <div className="business-summary-simple-scope-bridge__items">
                  {simpleScopeBridge.map((item) => {
                    const bridgeContent = (
                      <>
                        <small>{item.label}</small>
                        <strong>{item.value}</strong>
                        <em>{item.detail}</em>
                      </>
                    );
                    const bridgeFilter = item.filter;
                    const bridgeTargetTrimId = item.targetTrimId;
                    if (bridgeFilter && bridgeTargetTrimId && onFocusDeltaType) {
                      return (
                        <button
                          className="business-summary-simple-scope-bridge__item"
                          key={item.key}
                          type="button"
                          aria-label={`${item.actionLabel ?? "聚焦目标配置列"}：${item.value}`}
                          onClick={() => onFocusDeltaType(bridgeFilter, bridgeTargetTrimId)}
                        >
                          {bridgeContent}
                        </button>
                      );
                    }
                    return (
                      <span className="business-summary-simple-scope-bridge__item" key={item.key}>
                        {bridgeContent}
                      </span>
                    );
                  })}
                </div>
                {deltaFilter === "ALL" && onShowDifferenceRows ? (
                  <button
                    className="business-summary-simple-scope-bridge__action"
                    type="button"
                    aria-label="从全量配置查看差异行；表格将只展示业务差异行"
                    onClick={onShowDifferenceRows}
                  >
                    查看差异行
                  </button>
                ) : null}
              </div>

              {simpleVersionNarratives.length > 0 ? (
                <div className="business-summary-simple-version-narrative" aria-label="版本差异速读">
                  <div className="business-summary-simple-version-narrative__head">
                    <span>版本差异速读</span>
                    <small>按基准列生成每个目标配置列的业务短句。</small>
                  </div>
                  <div className="business-summary-simple-version-narrative__items">
                    {simpleVersionNarratives.map((item) => {
                      const filter = item.filter;
                      const content = (
                        <>
                          <span>{item.comparisonLabel}</span>
                          <strong>{item.headline}</strong>
                          <p>{item.body}</p>
                          <em>{item.evidenceLabel}</em>
                          {item.actionLabel ? <b>{item.actionLabel}</b> : null}
                        </>
                      );
                      const className = `business-summary-simple-version-narrative__item is-${item.tone}`;
                      if (filter && onFocusDeltaType) {
                        return (
                          <button
                            className={className}
                            key={item.key}
                            type="button"
                            aria-label={`${item.actionLabel}：${item.targetLabel} 相对基准`}
                            onClick={() => onFocusDeltaType(filter, item.targetTrimId)}
                          >
                            {content}
                          </button>
                        );
                      }
                      return (
                        <span className={className} key={item.key}>
                          {content}
                        </span>
                      );
                    })}
                  </div>
                </div>
              ) : null}
            </div>

            <div className="business-summary-excel-guide" aria-label="Excel 对比导读">
              {excelGuide.map((item) => {
                const guideContent = (
                  <>
                    <span>{item.label}</span>
                    <strong>{item.value}</strong>
                    <small>{item.detail}</small>
                  </>
                );
                const className = `business-summary-excel-guide__item is-${item.tone}`;
                const sourceTargetTrimId = item.sourceTargetTrimId ?? null;
                const filter = item.filter;
                if (sourceTargetTrimId && onOpenSourceContext) {
                  return (
                    <button
                      className={className}
                      key={item.key}
                      type="button"
                      aria-label={`打开 Excel 对比导读来源：${item.label}，${item.value}`}
                      onClick={() => onOpenSourceContext(sourceTargetTrimId)}
                    >
                      {guideContent}
                    </button>
                  );
                }
                if (filter && onFocusDeltaType) {
                  return (
                    <button
                      className={className}
                      key={item.key}
                      type="button"
                      aria-label={`查看 Excel 对比导读：${item.label}，${item.value}`}
                      onClick={() => onFocusDeltaType(filter, item.targetTrimId ?? null)}
                    >
                      {guideContent}
                    </button>
                  );
                }
                return (
                  <span className={className} key={item.key}>
                    {guideContent}
                  </span>
                );
              })}
            </div>

            <details className="business-summary-simple-conclusion-details" aria-label="目标配置列结论抽屉">
              <summary>
                <span>目标结论</span>
                <strong>{simpleConclusions.length > 0 ? `${simpleConclusions.length} 个配置列` : "暂无目标配置列"}</strong>
                <small>展开查看每个目标配置列的增配、减配和证据边界；不会改变下方表格范围。</small>
              </summary>
              <div className="business-summary-simple-conclusion" aria-label="Excel 列对比结果">
                <div className="business-summary-simple-conclusion__base">
                  <span>基准列</span>
                  <strong>{trimLabel(baseTrim)}</strong>
                  <p>{simpleBaseConclusionText(baseTrim, targetSummaries, deltaFilter)}</p>
                </div>
                <div className="business-summary-simple-conclusion__targets">
                  {simpleConclusions.map((item) => {
                    const filter = item.filter;
                    const tableScopeActionLabel = simpleConclusionTableScopeActionLabel(item);
                    const content = (
                      <>
                        <span>{item.targetLabel}</span>
                        <strong>{item.headline}</strong>
                        <small>{item.detail}</small>
                        <span className="business-summary-simple-conclusion__points" aria-label={`${item.targetLabel} 业务差异要点`}>
                          {item.points.map((point) => (
                            <span className={`business-summary-simple-conclusion__point is-${point.tone}`} key={point.key}>
                              <span>{point.label}</span>
                              <small>{point.value}</small>
                            </span>
                          ))}
                        </span>
                        <em>{item.statusLabel} · {item.evidenceNote}</em>
                      </>
                    );
                    const className = `business-summary-simple-conclusion__target is-${item.tone}`;
                    if (filter && onFocusDeltaType) {
                      return (
                        <button
                          className={className}
                          type="button"
                          key={item.key}
                          aria-label={simpleConclusionTableScopeAriaLabel(item)}
                          onClick={() => onFocusDeltaType(filter, item.targetTrimId)}
                        >
                          {content}
                          <b>{tableScopeActionLabel}</b>
                        </button>
                      );
                    }
                    if (item.action === "source" && onOpenSourceContext) {
                      return (
                        <button
                          className={className}
                          type="button"
                          key={item.key}
                          aria-label={`打开 ${item.targetLabel} 的来源入口：${item.statusLabel}`}
                          onClick={() => onOpenSourceContext(item.targetTrimId)}
                        >
                          {content}
                          <b>{item.actionLabel}</b>
                        </button>
                      );
                    }
                    if (onFocusTargetTrim) {
                      return (
                        <button
                          className={className}
                          type="button"
                          key={item.key}
                          aria-label={`聚焦 ${item.targetLabel} 的目标配置列`}
                          onClick={() => onFocusTargetTrim(item.targetTrimId)}
                        >
                          {content}
                          <b>聚焦此配置列</b>
                        </button>
                      );
                    }
                    return (
                      <span className={className} key={item.key}>
                        {content}
                      </span>
                    );
                  })}
                </div>
              </div>
            </details>

            {renderBaselineConclusion()}
          </div>
        </details>
      ) : null}

      {showExpertDeterministicBlocks ? (
      <div className="business-summary-base-storyline" aria-label="基准配置列差异脉络">
        {baseStoryline.map((item) => {
          const itemFilter = item.filter;
          const sourceTargetTrimId = item.sourceTargetTrimId;
          const content = (
            <>
              <span>{item.label}</span>
              <strong>{item.value}</strong>
              <small>{item.detail}</small>
            </>
          );
          if (sourceTargetTrimId !== undefined && onOpenSourceContext) {
            return (
              <button
                className="business-summary-base-storyline-item"
                type="button"
                key={item.key}
                aria-label={`打开基准叙事来源：${item.label}`}
                onClick={() => onOpenSourceContext(sourceTargetTrimId)}
              >
                {content}
              </button>
            );
          }
          if (itemFilter && onFocusDeltaType) {
            const filter = itemFilter;
            return (
              <button
                className="business-summary-base-storyline-item"
                type="button"
                key={item.key}
                aria-label={`聚焦基准叙事：${item.label}`}
                onClick={() => onFocusDeltaType(filter, null)}
              >
                {content}
              </button>
            );
          }
          return (
            <span className="business-summary-base-storyline-item" key={item.key}>
              {content}
            </span>
          );
        })}
      </div>
      ) : null}

      {showExpertDeterministicBlocks ? renderBaselineConclusion() : null}

      {showExpertDeterministicBlocks && versionLadder.length > 0 ? (
        <div className="business-summary-target-overview" aria-label="当前基准对比速览">
          <div className="business-summary-target-overview__head">
            <span>当前基准对比速览</span>
            <small>按当前 Base 汇总各目标配置列的差异；真实版本顺序见相邻版本升级路径。</small>
          </div>
          <div className="business-summary-target-overview__items">
            {versionLadder.map((item) => {
              const filter = item.filter;
              const targetTrimId = item.targetTrimId;
              const overviewContent = (
                <>
                  <span>{item.role} · {item.trimLabel}</span>
                  <strong>{item.primary}</strong>
                  <small>{item.detail}</small>
                  <em>{item.evidence}</em>
                </>
              );
              if (onFocusDeltaType && targetTrimId && filter && filter !== "COMMON" && !isEvidenceDeltaFilter(filter)) {
                return (
                  <button
                    className={`business-summary-target-overview__item is-${item.tone}`}
                    type="button"
                    key={item.key}
                    aria-label={`聚焦 ${item.trimLabel} 的基准对比摘要`}
                    onClick={() => onFocusDeltaType(filter, targetTrimId)}
                  >
                    {overviewContent}
                  </button>
                );
              }
              return (
                <span className={`business-summary-target-overview__item is-${item.tone}`} key={item.key}>
                  {overviewContent}
                </span>
              );
            })}
          </div>
        </div>
      ) : null}

      {showExpertDeterministicBlocks && versionUpgradeSteps.length > 0 ? (
        <div className="business-summary-version-path" aria-label="相邻版本升级路径">
          <div className="business-summary-version-path__head">
            <span>相邻版本升级路径</span>
            <small>按当前阶梯顺序逐级比较，点击后会切换基准列和目标配置列。</small>
          </div>
          <div className="business-summary-version-path__items">
            {versionUpgradeSteps.map((step) => {
              const pathContent = (
                <>
                  <span>{step.stepLabel}</span>
                  <strong>{trimLabel(step.fromTrim)} → {trimLabel(step.toTrim)}</strong>
                  <small>{step.primary}</small>
                  <em>{step.detail}</em>
                  <b>{step.evidence}</b>
                </>
              );
              if (onFocusVersionStep && step.actionable) {
                return (
                  <button
                    className={`business-summary-version-path__item is-${step.tone}`}
                    type="button"
                    key={step.key}
                    aria-label={`查看 ${trimLabel(step.fromTrim)} 到 ${trimLabel(step.toTrim)} 的相邻版本差异`}
                    onClick={() => onFocusVersionStep(step.fromTrim.trimId, step.toTrim.trimId, step.filter)}
                  >
                    {pathContent}
                  </button>
                );
              }
              return (
                <span className={`business-summary-version-path__item is-${step.tone}`} key={step.key}>
                  {pathContent}
                </span>
              );
            })}
          </div>
        </div>
      ) : null}

      {showExpertDeterministicBlocks ? (
      <div className="business-summary-targets">
        {targetSummaries.map((targetSummary) => {
          const conclusionDraft = targetConclusionDraft(targetSummary, baseTrim, deltaFilter);
          const conclusionFilter = conclusionDraft.filter;
          const conclusionTargetId = conclusionFilter ? focusTargetForFilter(conclusionFilter, targetSummary.targetTrim.trimId) : null;
          const actionGuidance = targetActionGuidance(targetSummary, baseTrim, deltaFilter);
          const actionFilter = actionGuidance.filter;
          const actionSource = actionGuidance.action;
          const actionTargetId = actionFilter ? focusTargetForFilter(actionFilter, targetSummary.targetTrim.trimId) : null;
          const focusGroups = businessFocusGroups(targetSummary);
          const visibleFocusGroups = focusGroups.slice(0, BUSINESS_FOCUS_GROUP_LIMIT);
          const hiddenFocusGroups = focusGroups.slice(BUSINESS_FOCUS_GROUP_LIMIT);
          const insightItems = targetInsightItems(targetSummary, deltaFilter);
          return (
          <article className="business-summary-target" key={targetSummary.targetTrim.trimId}>
            <div className="business-summary-target__title">
              <div className="business-summary-target__title-row">
                <strong>{trimLabel(targetSummary.targetTrim)}</strong>
                {onFocusTargetTrim ? (() => {
                  const isFocusedTarget = targetTrimFilterId === targetSummary.targetTrim.trimId;
                  const targetLabel = trimLabel(targetSummary.targetTrim);
                  const focusActionLabel = deltaFilter === "COMMON"
                    ? `查看 ${targetLabel} 相对基准的差异`
                    : `从业务摘要聚焦 ${targetLabel} 差异`;
                  return (
                    <button
                      className="business-summary-target-focus"
                      type="button"
                      aria-label={isFocusedTarget ? `取消业务摘要中 ${targetLabel} 目标聚焦` : focusActionLabel}
                      aria-pressed={isFocusedTarget}
                      onClick={() => onFocusTargetTrim(isFocusedTarget ? null : targetSummary.targetTrim.trimId)}
                    >
                      {isFocusedTarget ? "取消聚焦" : deltaFilter === "COMMON" ? "查看差异" : "聚焦此配置列"}
                    </button>
                  );
                })() : null}
              </div>
              <span>{narrative(targetSummary, baseTrim, categoryFilter, searchValue, deltaFilter)}</span>
            </div>

            <div
              className="business-summary-target-brief"
              aria-label={`${trimLabel(targetSummary.targetTrim)} 业务解读`}
            >
              <span>业务解读</span>
              <p>{targetBusinessInterpretation(targetSummary, baseTrim, deltaFilter)}</p>
            </div>

            {!simpleMode && focusGroups.length > 0 ? (
              <div className="business-summary-focus-groups" aria-label={`${trimLabel(targetSummary.targetTrim)} 业务重点分组`}>
                <div className="business-summary-focus-groups__head">
                  <span>业务重点分组</span>
                  <small>{focusGroups.length} 个维度</small>
                </div>
                <div className="business-summary-focus-groups__items">
                  {visibleFocusGroups.map((group) => renderBusinessFocusGroup(targetSummary, group))}
                </div>
                {hiddenFocusGroups.length > 0 ? (
                  <details className="business-summary-focus-groups__more">
                    <summary>展开 {hiddenFocusGroups.length} 个业务重点</summary>
                    <div className="business-summary-focus-groups__items business-summary-focus-groups__items--nested">
                      {hiddenFocusGroups.map((group) => renderBusinessFocusGroup(targetSummary, group))}
                    </div>
                  </details>
                ) : null}
              </div>
            ) : null}

            {!simpleMode ? (
            <div className={`business-summary-conclusion-draft is-${conclusionDraft.tone}`} aria-label={`${trimLabel(targetSummary.targetTrim)} 结论草稿`}>
              <div className="business-summary-conclusion-draft__head">
                <span>{conclusionDraft.statusLabel}</span>
                <strong>{conclusionDraft.title}</strong>
              </div>
              <p>{conclusionDraft.body}</p>
              <small>{conclusionDraft.evidenceNote}</small>
              <div className="business-summary-conclusion-draft__actions">
                <button
                  className="business-summary-conclusion-draft__button"
                  type="button"
                  aria-label={`复制 ${trimLabel(targetSummary.targetTrim)} 的结论草稿`}
                  onClick={() => {
                    void copyConclusionDraft(targetSummary.targetTrim, conclusionDraft);
                  }}
                >
                  复制草稿
                </button>
                {conclusionFilter && onFocusDeltaType ? (
                  <button
                    className="business-summary-conclusion-draft__button"
                    type="button"
                    aria-label={`聚焦 ${trimLabel(targetSummary.targetTrim)} 的结论草稿：${conclusionDraft.statusLabel}`}
                    onClick={() => onFocusDeltaType(conclusionFilter, conclusionTargetId)}
                  >
                    {conclusionDraft.actionLabel || "查看范围"}
                  </button>
                ) : conclusionDraft.action === "source" && onOpenSourceContext ? (
                  <button
                    className="business-summary-conclusion-draft__button"
                    type="button"
                    aria-label={`打开 ${trimLabel(targetSummary.targetTrim)} 的结论草稿来源入口：${conclusionDraft.statusLabel}`}
                    onClick={() => onOpenSourceContext(targetSummary.targetTrim.trimId)}
                  >
                    {conclusionDraft.actionLabel || "打开来源入口"}
                  </button>
                ) : null}
              </div>
              {copyFeedback?.trimId === targetSummary.targetTrim.trimId ? (
                <em className="business-summary-conclusion-draft__feedback" role="status">
                  {copyFeedback.message}
                </em>
              ) : null}
            </div>
            ) : null}

            {!simpleMode ? (
            <div className="business-summary-target-context" aria-label={`${trimLabel(targetSummary.targetTrim)} 身份与来源口径`}>
              {targetContextItems(baseTrim, targetSummary.targetTrim).map((item) => (
                <span className={`business-summary-target-context__item is-${item.tone}`} key={item.key}>
                  <small>{item.label}</small>
                  <strong>{item.value}</strong>
                  <em>{item.detail}</em>
                </span>
              ))}
            </div>
            ) : null}

            {!simpleMode ? (
            <div className={`business-summary-action business-summary-action--${actionGuidance.tone}`} aria-label={`${trimLabel(targetSummary.targetTrim)} 业务动作建议`}>
              <div>
                <span>{actionGuidance.label}</span>
                <strong>{actionGuidance.title}</strong>
                <p>{actionGuidance.detail}</p>
              </div>
              {actionFilter && onFocusDeltaType ? (
                <button
                  className="business-summary-action__button"
                  type="button"
                  aria-label={`聚焦 ${trimLabel(targetSummary.targetTrim)} 的 业务动作建议：${actionGuidance.label}`}
                  onClick={() => onFocusDeltaType(actionFilter, actionTargetId)}
                >
                  {actionFilter === "UNKNOWN"
                    ? "查看待确认"
                    : actionFilter === "INFERRED"
                      ? "查看推断"
                      : actionFilter === "COMMON"
                        ? "查看共同配置"
                        : actionFilter === "MISSING_SOURCE"
                          ? "查看来源问题"
                          : actionFilter === "MERGED_SOURCE"
                            ? "查看合并格"
                            : "查看差异"}
                </button>
              ) : actionSource === "source" && onOpenSourceContext ? (
                <button
                  className="business-summary-action__button"
                  type="button"
                  aria-label={`打开 ${trimLabel(targetSummary.targetTrim)} 的来源入口：${actionGuidance.label}`}
                  onClick={() => onOpenSourceContext(targetSummary.targetTrim.trimId)}
                >
                  {actionGuidance.actionLabel || "打开来源入口"}
                </button>
              ) : null}
            </div>
            ) : null}

            {!simpleMode ? (
            <div className="business-summary-target-insights" aria-label={`${trimLabel(targetSummary.targetTrim)} 业务结论`}>
              {insightItems.map((item) => renderTargetInsight(targetSummary, item))}
            </div>
            ) : null}

            <div className="business-summary-metrics" aria-label={`${trimLabel(targetSummary.targetTrim)} ${deltaFilter === "COMMON" ? "配置统计" : isEvidenceDeltaFilter(deltaFilter) ? "证据统计" : "差异统计"}`}>
              {targetMetricItems(targetSummary, deltaFilter).map((item) => (
                <span key={item.key}><strong>{item.count}</strong>{item.label}</span>
              ))}
            </div>

            {!simpleMode && targetSummary.categorySummaries.length > 0 ? (
              <div className="business-summary-categories">
                {targetSummary.categorySummaries.slice(0, 3).map((category) => {
                  const content = (
                    <>
                      <span className="business-summary-category-name">{categoryLabel(category.category)}</span>
                      <strong>{category.totalDifferenceCount}</strong>
                      <small>{categoryDeltaDetail(category)}</small>
                    </>
                  );
                  return onFocusCategory ? (
                    <button
                      className="business-summary-category"
                      type="button"
                      key={category.category}
                      aria-label={`聚焦 ${trimLabel(targetSummary.targetTrim)} 的 ${categoryLabel(category.category)} 差异大类，目标差异 ${category.totalDifferenceCount} 项`}
                      onClick={() => onFocusCategory(category.category)}
                    >
                      {content}
                    </button>
                  ) : (
                    <span className="business-summary-category" key={category.category}>
                      {content}
                    </span>
                  );
                })}
              </div>
            ) : null}

            {!simpleMode ? (
            <details className="business-summary-detail" open={Boolean(targetTrimFilterId)}>
              <summary>
                <span className="business-summary-detail-summary-text">
                  <span>{deltaFilter === "COMMON" ? "共同配置明细" : isEvidenceDeltaFilter(deltaFilter) ? "证据明细" : "差异明细"}</span>
                  <small>展开 / 收起来源样本</small>
                </span>
                <strong>{deltaFilter === "COMMON" || isEvidenceDeltaFilter(deltaFilter) ? targetSummary.deltas.length : targetSummary.totalDifferenceCount} 项</strong>
              </summary>
              {deltaFilter === "COMMON" ? (() => {
                const { deltas, hiddenDeltas } = commonConfigGroup(targetSummary);
                return (
                  <div className="business-summary-outcomes">
                    <div className="business-summary-outcome">
                      <span>共同配置行</span>
                      {deltas.length > 0 ? (
                        <div className="business-summary-deltas">
                          {deltas.map((delta) => renderCommonDeltaButton(targetSummary, delta, "common"))}
                          {hiddenDeltas.length > 0 ? (
                            <details className="business-summary-delta-more">
                              <summary>展开 {hiddenDeltas.length} 项共同配置</summary>
                              <div className="business-summary-deltas business-summary-deltas--nested">
                                {hiddenDeltas.map((delta) => renderCommonDeltaButton(targetSummary, delta, "common-hidden"))}
                              </div>
                            </details>
                          ) : null}
                        </div>
                    ) : <small>当前范围没有共同配置行</small>}
                  </div>
                </div>
              );
            })() : isEvidenceDeltaFilter(deltaFilter) ? (() => {
              const evidenceDeltas = sortedDeltas(targetSummary.deltas);
              const visibleDeltas = evidenceDeltas.slice(0, SECTION_DELTA_LIMIT);
              const hiddenDeltas = evidenceDeltas.slice(SECTION_DELTA_LIMIT);
              const evidenceLabel = deltaFilter === "MISSING_SOURCE" ? "来源问题" : "合并格展开";
              return (
                <div className="business-summary-outcomes">
                  <div className="business-summary-outcome">
                    <span>{deltaFilter === "MISSING_SOURCE" ? "来源问题配置行" : "合并格展开配置行"}</span>
                    {visibleDeltas.length > 0 ? (
                      <div className="business-summary-deltas">
                        {visibleDeltas.map((delta) => renderEvidenceDeltaButton(targetSummary, deltaFilter, delta, "evidence"))}
                        {hiddenDeltas.length > 0 ? (
                          <details className="business-summary-delta-more">
                            <summary>展开 {hiddenDeltas.length} 项{evidenceLabel}</summary>
                            <div className="business-summary-deltas business-summary-deltas--nested">
                              {hiddenDeltas.map((delta) => renderEvidenceDeltaButton(targetSummary, deltaFilter, delta, "evidence-hidden"))}
                            </div>
                          </details>
                        ) : null}
                      </div>
                    ) : <small>{deltaFilter === "MISSING_SOURCE" ? "当前范围没有来源问题配置行" : "当前范围没有合并格展开配置行"}</small>}
                  </div>
                </div>
              );
            })() : (
              <div className="business-summary-outcomes">
                {(() => {
                  const upgradeInsights = optionSwapInsights(targetSummary);
                  if (upgradeInsights.length === 0) return null;
                  const visibleInsights = upgradeInsights.slice(0, SECTION_DELTA_LIMIT);
                  const hiddenInsights = upgradeInsights.slice(SECTION_DELTA_LIMIT);
                  return (
                    <div className="business-summary-outcome business-summary-outcome--wide">
                      <span>升级线索</span>
                      <div className="business-summary-upgrade-clues">
                        {visibleInsights.map((insight) => renderUpgradeInsightCard(targetSummary, insight))}
                        {hiddenInsights.length > 0 ? (
                          <details className="business-summary-upgrade-more">
                            <summary>展开 {hiddenInsights.length} 条升级线索</summary>
                            <div className="business-summary-upgrade-clues business-summary-upgrade-clues--nested">
                              {hiddenInsights.map((insight) => renderUpgradeInsightCard(targetSummary, insight))}
                            </div>
                          </details>
                        ) : null}
                      </div>
                    </div>
                  );
                })()}
                {BUSINESS_SECTIONS.map((section) => {
                  const { deltas, hiddenDeltas } = sectionDeltaGroup(targetSummary, section.types);
                  return (
                    <div className="business-summary-outcome" key={`${targetSummary.targetTrim.trimId}-${section.key}`}>
                      <span>{section.title}</span>
                      {deltas.length > 0 ? (
                        <div className="business-summary-deltas">
                          {deltas.map((delta) => renderBusinessDeltaButton(targetSummary, section.key, delta))}
                          {hiddenDeltas.length > 0 ? (
                            <details className="business-summary-delta-more">
                              <summary>展开 {hiddenDeltas.length} 项{section.title}</summary>
                              <div className="business-summary-deltas business-summary-deltas--nested">
                                {hiddenDeltas.map((delta) => renderBusinessDeltaButton(targetSummary, `${section.key}-hidden`, delta))}
                              </div>
                            </details>
                          ) : null}
                        </div>
                      ) : <small>{section.emptyLabel}</small>}
                    </div>
                  );
                })}
              </div>
            )}
            </details>
            ) : null}
          </article>
          );
        })}
      </div>
      ) : null}
    </section>
  );
}
